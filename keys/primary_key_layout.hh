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

class primary_key_layout;

// Position in a serialized key, owned by primary_key_layout_iterator,
// interpreted by the layout.
struct primary_key_layout_cursor {
    managed_bytes_view key;     // the whole key
    managed_bytes_view rest;    // bytes following the current component
    managed_bytes_view current; // the current component
    size_t index = 0;           // of the current component
    bool at_end = true;
};

// Forward iterator over components of a serialized key. Yields managed_bytes_view.
class primary_key_layout_iterator {
    const primary_key_layout* _layout = nullptr;
    primary_key_layout_cursor _c;
public:
    using iterator_category = std::forward_iterator_tag;
    using iterator_concept = std::forward_iterator_tag;
    using value_type = const managed_bytes_view;
    using difference_type = std::ptrdiff_t;

    struct end_tag {};

    primary_key_layout_iterator() = default;
    primary_key_layout_iterator(const primary_key_layout& layout, managed_bytes_view key);
    primary_key_layout_iterator(end_tag, const primary_key_layout& layout) : _layout(&layout) {}

    primary_key_layout_iterator& operator++();
    primary_key_layout_iterator operator++(int) {
        primary_key_layout_iterator i(*this);
        ++(*this);
        return i;
    }
    value_type operator*() const { return _c.current; }
    // Valid only for iterators over the same key.
    bool operator==(const primary_key_layout_iterator& o) const {
        return _c.at_end == o._c.at_end && (_c.at_end || _c.index == o._c.index);
    }
};

// Serialized form of a clustering key prefix: how the key frames its
// components, and how keys compare, hash and decompose. A schema owns one
// layout, made by make_primary_key_layout() from the clustering column types.
//
// Every layout keeps the serialized value of each component as a contiguous
// span of the key, so component views point into the key. A key with no
// components serializes to no bytes.
class primary_key_layout {
public:
    using value_type = std::vector<bytes>;

    using cursor = primary_key_layout_cursor;
    using iterator = primary_key_layout_iterator;

    // Value bytes of the components of a key being serialized.
    class component_source {
    public:
        // Appends the value of component idx to out. Called once per component, in order.
        virtual void write(size_t idx, managed_bytes_mutable_view& out) = 0;
    protected:
        ~component_source() = default;
    };
protected:
    std::vector<data_type> _types;
public:
    explicit primary_key_layout(std::vector<data_type> types) : _types(std::move(types)) {}
    virtual ~primary_key_layout() = default;

    const std::vector<data_type>& types() const {
        return _types;
    }

    bool is_singular() const {
        return _types.size() == 1;
    }

    // Iteration

    // Positions c at the first component of c.key, or at the end.
    virtual void first(cursor& c) const = 0;

    // Moves c to the next component, or to the end.
    virtual void next(cursor& c) const = 0;

    iterator begin(managed_bytes_view key) const {
        return iterator(*this, key);
    }

    iterator end(managed_bytes_view) const {
        return iterator(iterator::end_tag{}, *this);
    }

    // Returns a range of managed_bytes_view
    std::ranges::subrange<iterator> components(managed_bytes_view key) const {
        return {begin(key), end(key)};
    }

    // Returns the number of components of the key.
    virtual size_t size(managed_bytes_view key) const;

    virtual value_type deserialize_value(managed_bytes_view key) const;

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

    // Returns the size of a key with components of the given sizes.
    virtual size_t serialized_size(std::span<const size_t> sizes) const = 0;

    // Writes a key with components of the given sizes into out, which is
    // serialized_size(sizes) long, taking the component values from src.
    virtual void serialize(std::span<const size_t> sizes, component_source& src, managed_bytes_mutable_view out) const = 0;

    // Serializes a key from a forward range of serialized components
    // (bytes, bytes_view, managed_bytes or any FragmentedView).
    template <typename RangeOfSerializedComponents>
    managed_bytes serialize_value(RangeOfSerializedComponents&& values) const;

    template <typename T>
    managed_bytes serialize_value(std::initializer_list<T> values) const {
        return serialize_value(std::span(values));
    }

    managed_bytes serialize_single(const managed_bytes& v) const {
        return serialize_value(std::ranges::subrange(&v, 1 + &v));
    }

    managed_bytes serialize_single(const bytes& v) const {
        return serialize_value(std::ranges::subrange(&v, 1 + &v));
    }

    // Throws std::logic_error on a disengaged value.
    managed_bytes serialize_optionals(std::span<const bytes_opt> values) const;
    managed_bytes serialize_optionals(std::span<const managed_bytes_opt> values) const;

    managed_bytes serialize_value_deep(const std::vector<data_value>& values) const;
};

inline
primary_key_layout_iterator::primary_key_layout_iterator(const primary_key_layout& layout, managed_bytes_view key)
    : _layout(&layout)
{
    _c.key = key;
    _layout->first(_c);
}

inline
primary_key_layout_iterator& primary_key_layout_iterator::operator++() {
    _layout->next(_c);
    return *this;
}

static_assert(std::forward_iterator<primary_key_layout_iterator>);

namespace detail {

template <typename T>
size_t serialized_component_size(const T& v) {
    if constexpr (FragmentedView<T>) {
        return v.size_bytes();
    } else {
        return v.size();
    }
}

template <typename T>
void write_serialized_component(const T& v, managed_bytes_mutable_view& out) {
    if constexpr (FragmentedView<T>) {
        write_fragmented(out, v);
    } else if constexpr (std::same_as<T, managed_bytes>) {
        write_fragmented(out, managed_bytes_view(v));
    } else {
        write_fragmented(out, single_fragmented_view(v));
    }
}

}

template <typename RangeOfSerializedComponents>
managed_bytes primary_key_layout::serialize_value(RangeOfSerializedComponents&& values) const {
    utils::small_vector<size_t, 8> sizes;
    for (auto&& v : values) {
        sizes.push_back(detail::serialized_component_size(v));
    }

    using iterator_type = decltype(std::ranges::begin(values));
    struct source final : component_source {
        iterator_type it;
        explicit source(iterator_type it) : it(std::move(it)) {}
        void write(size_t, managed_bytes_mutable_view& out) override {
            detail::write_serialized_component(*it, out);
            ++it;
        }
    } src(std::ranges::begin(values));

    managed_bytes b(managed_bytes::initialized_later(), serialized_size(sizes));
    serialize(sizes, src, managed_bytes_mutable_view(b));
    return b;
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

    void first(cursor& c) const override;
    void next(cursor& c) const override;

    // These walk components with compound_type's non-virtual iterator.
    size_t size(managed_bytes_view key) const override;
    value_type deserialize_value(managed_bytes_view key) const override;
    std::strong_ordering prefix_equal_compare(managed_bytes_view k1, managed_bytes_view k2) const override;

    std::strong_ordering compare(managed_bytes_view k1, managed_bytes_view k2) const override;
    bool equal(managed_bytes_view k1, managed_bytes_view k2) const override;
    size_t hash(managed_bytes_view key) const override;
    size_t serialized_size(std::span<const size_t> sizes) const override;
    void serialize(std::span<const size_t> sizes, component_source& src, managed_bytes_mutable_view out) const override;
};

using primary_key_layout_ptr = seastar::shared_ptr<const primary_key_layout>;

// Returns the layout for clustering keys with components of the given types.
// Depends only on the types, and yields the same layout for a type and its
// reversed_type.
primary_key_layout_ptr make_primary_key_layout(std::vector<data_type> types);
