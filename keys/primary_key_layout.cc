/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "keys/primary_key_layout.hh"
#include "utils/lexicographical_compare.hh"
#include "utils/on_internal_error.hh"
#include <cstdlib>

primary_key_layout::value_type primary_key_layout::deserialize_value(managed_bytes_view key) const {
    value_type result;
    result.reserve(_types.size());
    for (managed_bytes_view c : components(key)) {
        result.emplace_back(to_bytes(c));
    }
    return result;
}

std::strong_ordering primary_key_layout::prefix_equal_compare(managed_bytes_view k1, managed_bytes_view k2) const {
    auto c1 = components(k1);
    auto c2 = components(k2);
    return prefix_equality_tri_compare(_types.begin(),
        c1.begin(), c1.end(),
        c2.begin(), c2.end(),
        ::tri_compare);
}

void primary_key_layout::validate(managed_bytes_view key) const {
    auto values = components(key);
    if (values.size() > _types.size()) {
        throw marshal_exception(fmt::format("compound::validate(): cannot have more values than types, have {} values but only {} types",
                    values.size(), _types.size()));
    }
    for (size_t i = 0; i != values.size(); ++i) {
        _types[i]->validate(values[i]);
    }
}

managed_bytes primary_key_layout::serialize_optionals(std::span<const bytes_opt> values) const {
    return serialize_value(values | std::views::transform([] (const bytes_opt& bo) -> bytes_view {
        if (!bo) {
            throw std::logic_error("attempted to create key component from empty optional");
        }
        return *bo;
    }));
}

managed_bytes primary_key_layout::serialize_optionals(std::span<const managed_bytes_opt> values) const {
    return serialize_value(values | std::views::transform([] (const managed_bytes_opt& bo) -> managed_bytes_view {
        if (!bo) {
            throw std::logic_error("attempted to create key component from empty optional");
        }
        return managed_bytes_view(*bo);
    }));
}

managed_bytes primary_key_layout::serialize_value_deep(const std::vector<data_value>& values) const {
    std::vector<bytes> partial;
    partial.reserve(values.size());
    auto i = _types.begin();
    for (auto&& component : values) {
        SCYLLA_ASSERT(i != _types.end());
        partial.push_back((*i++)->decompose(component));
    }
    return serialize_value(partial);
}

legacy_primary_key_layout::legacy_primary_key_layout(std::vector<data_type> types)
    : primary_key_layout(types, true)
    , _compound(std::move(types))
{ }

void legacy_primary_key_layout::decompose(managed_bytes_view key, primary_key_components& out) const {
    for (managed_bytes_view c : compound::components(key)) {
        out.push_back(c);
    }
}

size_t legacy_primary_key_layout::size(managed_bytes_view key) const {
    return std::distance(compound::begin(key), compound::end(key));
}

std::strong_ordering legacy_primary_key_layout::compare(managed_bytes_view k1, managed_bytes_view k2) const {
    return _compound.compare(k1, k2);
}

std::strong_ordering legacy_primary_key_layout::prefix_equal_compare(managed_bytes_view k1, managed_bytes_view k2) const {
    return prefix_equality_tri_compare(_types.begin(),
        compound::begin(k1), compound::end(k1),
        compound::begin(k2), compound::end(k2),
        ::tri_compare);
}

bool legacy_primary_key_layout::equal(managed_bytes_view k1, managed_bytes_view k2) const {
    return _compound.equal(k1, k2);
}

size_t legacy_primary_key_layout::hash(managed_bytes_view key) const {
    return _compound.hash(key);
}

managed_bytes legacy_primary_key_layout::serialize(std::span<const managed_bytes_view> components) const {
    return compound::serialize_value(components);
}

static constexpr size_t int32_value_size = sizeof(int32_t);
static constexpr size_t int32_empty_value_size = 1;

// Number of components of an int32_pk_layout key.
static size_t int32_key_size(managed_bytes_view key) {
    switch (key.size_bytes()) {
    case 0:
        return 0;
    case int32_empty_value_size:
    case int32_value_size:
        return 1;
    default:
        utils::on_internal_error(fmt::format("int32_pk_layout: invalid key size {}", key.size_bytes()));
    }
}

// Compares the only components of two int32_pk_layout keys which have one,
// with the order of the underlying int32 type: empty first.
static std::strong_ordering int32_compare_values(managed_bytes_view v1, managed_bytes_view v2) {
    const bool empty1 = v1.size_bytes() == int32_empty_value_size;
    const bool empty2 = v2.size_bytes() == int32_empty_value_size;
    if (empty1 || empty2) {
        return empty2 <=> empty1;
    }
    return read_simple<int32_t>(v1) <=> read_simple<int32_t>(v2);
}

int32_pk_layout::int32_pk_layout(std::vector<data_type> types)
    : primary_key_layout(std::move(types))
    , _reversed(_types.at(0)->is_reversed())
{ }

void int32_pk_layout::decompose(managed_bytes_view key, primary_key_components& out) const {
    switch (int32_key_size(key)) {
    case 0:
        return;
    default:
        out.push_back(key.size_bytes() == int32_value_size ? key : key.prefix(0));
    }
}

size_t int32_pk_layout::size(managed_bytes_view key) const {
    return int32_key_size(key);
}

std::strong_ordering int32_pk_layout::compare(managed_bytes_view k1, managed_bytes_view k2) const {
    const auto n1 = int32_key_size(k1);
    const auto n2 = int32_key_size(k2);
    if (!n1 || !n2) {
        return n1 <=> n2;
    }
    auto r = int32_compare_values(k1, k2);
    return _reversed ? 0 <=> r : r;
}

std::strong_ordering int32_pk_layout::prefix_equal_compare(managed_bytes_view k1, managed_bytes_view k2) const {
    if (!int32_key_size(k1) || !int32_key_size(k2)) {
        return std::strong_ordering::equal;
    }
    auto r = int32_compare_values(k1, k2);
    return _reversed ? 0 <=> r : r;
}

bool int32_pk_layout::equal(managed_bytes_view k1, managed_bytes_view k2) const {
    int32_key_size(k1);
    int32_key_size(k2);
    return k1 == k2;
}

size_t int32_pk_layout::hash(managed_bytes_view key) const {
    int32_key_size(key);
    return with_linearized(key, [] (bytes_view v) {
        return std::hash<bytes_view>()(v);
    });
}

managed_bytes int32_pk_layout::serialize(std::span<const managed_bytes_view> components) const {
    if (components.empty()) {
        return managed_bytes();
    }
    if (components.size() > 1) {
        throw marshal_exception(fmt::format("int32_pk_layout: key has {} components, expected at most 1", components.size()));
    }
    const auto& c = components[0];
    if (c.empty()) {
        static const bytes empty_value_marker(int32_empty_value_size, int8_t(0));
        return managed_bytes(bytes_view(empty_value_marker));
    }
    if (c.size_bytes() != int32_value_size) {
        throw marshal_exception(fmt::format("int32_pk_layout: int32 value has {} bytes, expected {}", c.size_bytes(), int32_value_size));
    }
    return managed_bytes(c);
}

bool int32_pk_layout_enabled() {
    static const bool enabled = [] {
        auto v = std::getenv("SCYLLA_INT32_PK_LAYOUT");
        return v && std::string_view(v) == "1";
    }();
    return enabled;
}

primary_key_layout_ptr make_primary_key_layout(std::vector<data_type> types) {
    if (int32_pk_layout_enabled() && types.size() == 1) {
        const auto& t = types[0]->is_reversed() ? types[0]->underlying_type() : types[0];
        if (t == int32_type) {
            return seastar::make_shared<int32_pk_layout>(std::move(types));
        }
    }
    return seastar::make_shared<legacy_primary_key_layout>(std::move(types));
}
