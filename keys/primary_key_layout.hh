/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "keys/compound.hh"
#include "utils/small_vector.hh"
#include <seastar/core/shared_ptr.hh>

// Views of the components of a serialized key, pointing into the key.
//
// begin() and end() exist only on lvalues: iterators into a temporary would
// dangle. Range-for over a temporary is fine.
class primary_key_components {
    utils::small_vector<managed_bytes_view, 8> _views;
public:
    using value_type = managed_bytes_view;
    using const_iterator = utils::small_vector<managed_bytes_view, 8>::const_iterator;

    void push_back(managed_bytes_view v) {
        _views.push_back(v);
    }

    const_iterator begin() const& { return _views.begin(); }
    const_iterator end() const& { return _views.end(); }
    const_iterator begin() const&& = delete;
    const_iterator end() const&& = delete;

    size_t size() const { return _views.size(); }
    bool empty() const { return _views.empty(); }
    const managed_bytes_view& operator[](size_t i) const { return _views[i]; }

    std::span<const managed_bytes_view> span() const& { return {_views.data(), _views.size()}; }
    std::span<const managed_bytes_view> span() const&& = delete;
};

// Serialized form of a clustering key prefix: how the key frames its
// components, and how keys compare, hash and decompose. A schema owns one
// layout, made by make_primary_key_layout() from the clustering column types.
//
// Only operations on whole keys are virtual. Component access goes through
// decompose(), so iterating components costs one virtual call per key.
//
// Every layout keeps the serialized value of each component as a contiguous
// span of the key, so component views point into the key. A key with no
// components serializes to no bytes.
class primary_key_layout {
public:
    using value_type = std::vector<bytes>;
protected:
    std::vector<data_type> _types;
private:
    bool _legacy;
public:
    // legacy: whether keys are in the legacy compound format.
    explicit primary_key_layout(std::vector<data_type> types, bool legacy = false)
        : _types(std::move(types))
        , _legacy(legacy)
    { }
    virtual ~primary_key_layout() = default;

    // Whether keys are in the legacy compound format, which the IDL and
    // sstable large data records use.
    bool is_legacy() const {
        return _legacy;
    }

    const std::vector<data_type>& types() const {
        return _types;
    }

    bool is_singular() const {
        return _types.size() == 1;
    }

    // Decomposition

    // Appends views of the components of the key to out.
    virtual void decompose(managed_bytes_view key, primary_key_components& out) const = 0;

    primary_key_components components(managed_bytes_view key) const {
        primary_key_components result;
        decompose(key, result);
        return result;
    }

    // Returns the number of components of the key.
    virtual size_t size(managed_bytes_view key) const {
        return components(key).size();
    }

    value_type deserialize_value(managed_bytes_view key) const;

    // Comparison

    virtual std::strong_ordering compare(managed_bytes_view k1, managed_bytes_view k2) const = 0;

    // Like compare(), but a prefix equals every key it prefixes.
    virtual std::strong_ordering prefix_equal_compare(managed_bytes_view k1, managed_bytes_view k2) const;

    virtual bool equal(managed_bytes_view k1, managed_bytes_view k2) const = 0;

    virtual size_t hash(managed_bytes_view key) const = 0;

    bool less(managed_bytes_view k1, managed_bytes_view k2) const {
        return compare(k1, k2) < 0;
    }

    // Returns true iff the key has no missing components.
    bool is_full(managed_bytes_view key) const {
        return size(key) == _types.size();
    }

    bool is_empty(managed_bytes_view key) const {
        return key.empty();
    }

    void validate(managed_bytes_view key) const;

    // Serialization

    // Returns a key made of the given serialized components.
    virtual managed_bytes serialize(std::span<const managed_bytes_view> components) const = 0;

    // Serializes a key from a forward range of serialized components
    // (bytes, bytes_view, managed_bytes or any FragmentedView).
    template <typename RangeOfSerializedComponents>
    managed_bytes serialize_value(RangeOfSerializedComponents&& values) const;

    template <typename T>
    managed_bytes serialize_value(std::initializer_list<T> values) const {
        return serialize_value(std::span(values));
    }

    managed_bytes serialize_single(const managed_bytes& v) const {
        managed_bytes_view mv(v);
        return serialize(std::span(&mv, 1));
    }

    managed_bytes serialize_single(const bytes& v) const {
        managed_bytes_view mv{bytes_view(v)};
        return serialize(std::span(&mv, 1));
    }

    // Throws std::logic_error on a disengaged value.
    managed_bytes serialize_optionals(std::span<const bytes_opt> values) const;
    managed_bytes serialize_optionals(std::span<const managed_bytes_opt> values) const;

    managed_bytes serialize_value_deep(const std::vector<data_value>& values) const;
};

namespace detail {

// Components whose managed_bytes_view stays valid after the range yields them.
template <typename Range>
concept viewable_key_components = requires {
    requires std::is_lvalue_reference_v<std::ranges::range_reference_t<Range>>
        || std::same_as<std::ranges::range_value_t<Range>, bytes_view>
        || std::same_as<std::ranges::range_value_t<Range>, managed_bytes_view>;
    requires std::same_as<std::ranges::range_value_t<Range>, bytes>
        || std::same_as<std::ranges::range_value_t<Range>, bytes_view>
        || std::same_as<std::ranges::range_value_t<Range>, managed_bytes>
        || std::same_as<std::ranges::range_value_t<Range>, managed_bytes_view>;
};

template <typename T>
managed_bytes_view key_component_view(const T& v) {
    if constexpr (std::same_as<T, bytes> || std::same_as<T, bytes_view>) {
        return managed_bytes_view(bytes_view(v));
    } else {
        return managed_bytes_view(v);
    }
}

template <typename T>
managed_bytes copy_key_component(const T& v) {
    if constexpr (FragmentedView<T>) {
        managed_bytes b(managed_bytes::initialized_later(), v.size_bytes());
        auto out = managed_bytes_mutable_view(b);
        write_fragmented(out, v);
        return b;
    } else {
        return managed_bytes(key_component_view(v));
    }
}

}

template <typename RangeOfSerializedComponents>
managed_bytes primary_key_layout::serialize_value(RangeOfSerializedComponents&& values) const {
    utils::small_vector<managed_bytes_view, 8> views;
    if constexpr (detail::viewable_key_components<RangeOfSerializedComponents>) {
        for (auto&& v : values) {
            views.push_back(detail::key_component_view(v));
        }
        return serialize(std::span<const managed_bytes_view>(views.data(), views.size()));
    } else {
        // Views of the copies are taken once storage stops growing.
        std::vector<managed_bytes> storage;
        for (auto&& v : values) {
            storage.push_back(detail::copy_key_component(v));
        }
        for (const auto& c : storage) {
            views.push_back(managed_bytes_view(c));
        }
        return serialize(std::span<const managed_bytes_view>(views.data(), views.size()));
    }
}

// The layout of the legacy compound format:
//
//   <len(value1)><value1><len(value2)><value2>...<len(value_n)><value_n>
//
// with 2-byte big-endian lengths. Same as compound_type<allow_prefixes::yes>.
class legacy_primary_key_layout final : public primary_key_layout {
    using compound = compound_type<allow_prefixes::yes>;
    compound _compound;
public:
    explicit legacy_primary_key_layout(std::vector<data_type> types);

    void decompose(managed_bytes_view key, primary_key_components& out) const override;
    size_t size(managed_bytes_view key) const override;
    std::strong_ordering compare(managed_bytes_view k1, managed_bytes_view k2) const override;
    std::strong_ordering prefix_equal_compare(managed_bytes_view k1, managed_bytes_view k2) const override;
    bool equal(managed_bytes_view k1, managed_bytes_view k2) const override;
    size_t hash(managed_bytes_view key) const override;
    managed_bytes serialize(std::span<const managed_bytes_view> components) const override;
};

using primary_key_layout_ptr = seastar::shared_ptr<const primary_key_layout>;

// Returns the layout for clustering keys with components of the given types.
// Depends only on the types, and yields the same layout for a type and its
// reversed_type.
primary_key_layout_ptr make_primary_key_layout(std::vector<data_type> types);
