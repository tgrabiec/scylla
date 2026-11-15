/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "keys/primary_key_layout.hh"
#include "utils/lexicographical_compare.hh"

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
    : primary_key_layout(types)
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

primary_key_layout_ptr make_primary_key_layout(std::vector<data_type> types) {
    return seastar::make_shared<legacy_primary_key_layout>(std::move(types));
}
