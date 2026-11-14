/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "keys/primary_key_layout.hh"
#include "utils/lexicographical_compare.hh"

size_t primary_key_layout::size(managed_bytes_view key) const {
    return std::distance(begin(key), end(key));
}

primary_key_layout::value_type primary_key_layout::deserialize_value(managed_bytes_view key) const {
    value_type result;
    result.reserve(_types.size());
    for (managed_bytes_view c : components(key)) {
        result.emplace_back(to_bytes(c));
    }
    return result;
}

std::strong_ordering primary_key_layout::prefix_equal_compare(managed_bytes_view k1, managed_bytes_view k2) const {
    return prefix_equality_tri_compare(_types.begin(),
        begin(k1), end(k1),
        begin(k2), end(k2),
        ::tri_compare);
}

void primary_key_layout::validate(managed_bytes_view key) const {
    std::vector<managed_bytes_view> values(begin(key), end(key));
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

void legacy_primary_key_layout::first(cursor& c) const {
    c.rest = c.key;
    c.index = size_t(-1); // next() moves to index 0
    next(c);
}

void legacy_primary_key_layout::next(cursor& c) const {
    if (c.rest.empty()) {
        c.at_end = true;
        return;
    }
    auto len = read_simple<compound::size_type>(c.rest);
    if (c.rest.size() < len) {
        throw_with_backtrace<marshal_exception>(format("legacy_primary_key_layout - not enough bytes, expected {:d}, got {:d}", len, c.rest.size()));
    }
    c.current = c.rest.prefix(len);
    c.rest.remove_prefix(len);
    c.at_end = false;
    ++c.index;
}

std::strong_ordering legacy_primary_key_layout::compare(managed_bytes_view k1, managed_bytes_view k2) const {
    return _compound.compare(k1, k2);
}

bool legacy_primary_key_layout::equal(managed_bytes_view k1, managed_bytes_view k2) const {
    return _compound.equal(k1, k2);
}

size_t legacy_primary_key_layout::hash(managed_bytes_view key) const {
    return _compound.hash(key);
}

size_t legacy_primary_key_layout::serialized_size(std::span<const size_t> sizes) const {
    size_t len = 0;
    for (auto s : sizes) {
        len += sizeof(compound::size_type) + s;
    }
    if (len > std::numeric_limits<compound::size_type>::max()) {
        // Matches Cassandra's wording, see compound_type::serialize_value().
        throw exceptions::invalid_request_exception(format("Key length of {:d} is longer than maximum of {:d}",
                len, std::numeric_limits<compound::size_type>::max()));
    }
    return len;
}

void legacy_primary_key_layout::serialize(std::span<const size_t> sizes, component_source& src, managed_bytes_mutable_view out) const {
    for (size_t i = 0; i != sizes.size(); ++i) {
        write<compound::size_type>(out, compound::size_type(sizes[i]));
        src.write(i, out);
    }
}

primary_key_layout_ptr make_primary_key_layout(std::vector<data_type> types) {
    return seastar::make_shared<legacy_primary_key_layout>(std::move(types));
}
