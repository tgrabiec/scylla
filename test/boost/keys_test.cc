/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "keys/keys.hh"
#include "schema/schema.hh"
#include "schema/schema_builder.hh"
#include "types/types.hh"
#include "utils/lexicographical_compare.hh"

#include "idl/keys.dist.hh"
#include "serializer_impl.hh"
#include "idl/keys.dist.impl.hh"

BOOST_AUTO_TEST_CASE(test_key_is_prefixed_by) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("c1", bytes_type, column_kind::partition_key)
            .with_column("c2", bytes_type, column_kind::clustering_key)
            .with_column("c3", bytes_type, column_kind::clustering_key)
            .with_column("c4", bytes_type, column_kind::clustering_key)
            .build();
    const schema& s = *s_ptr;

    auto key = clustering_key::from_exploded(s, {bytes("a"), bytes("b"), bytes("c")});

    BOOST_REQUIRE(key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a")})));
    BOOST_REQUIRE(key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a"), bytes("b")})));
    BOOST_REQUIRE(key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a"), bytes("b"), bytes("c")})));

    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes()})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("b"), bytes("c")})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a"), bytes("c"), bytes("b")})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("abc")})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("ab")})));
}

BOOST_AUTO_TEST_CASE(test_key_component_iterator) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("c1", bytes_type, column_kind::partition_key)
            .with_column("c2", bytes_type, column_kind::clustering_key)
            .with_column("c3", bytes_type, column_kind::clustering_key)
            .with_column("c4", bytes_type, column_kind::clustering_key)
            .build();
    const schema& s = *s_ptr;

    auto key = clustering_key::from_exploded(s, {bytes("a"), bytes("b"), bytes("c")});

    auto components = key.components(s);
    auto i = components.begin();
    auto end = components.end();

    BOOST_REQUIRE(i != end);
    BOOST_REQUIRE(*i == bytes_view(bytes("a")));
    ++i;

    BOOST_REQUIRE(i != end);
    BOOST_REQUIRE(*i == bytes_view(bytes("b")));
    ++i;

    BOOST_REQUIRE(i != end);
    BOOST_REQUIRE(*i == bytes_view(bytes("c")));
    ++i;

    BOOST_REQUIRE(i == end);
}

BOOST_AUTO_TEST_CASE(test_legacy_ordering_for_non_composite_key) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("c1", bytes_type, column_kind::partition_key)
            .build();
    const schema& s = *s_ptr;

    auto to_key = [&s] (sstring value) {
        return partition_key::from_single_value(s, to_bytes(value));
    };

    auto cmp = [&s] (const partition_key& k1, const partition_key& k2) {
        return k1.legacy_tri_compare(s, k2);
    };

    BOOST_REQUIRE(cmp(to_key("A"), to_key("B"))  < 0);
    BOOST_REQUIRE(cmp(to_key("AA"), to_key("B")) < 0);
    BOOST_REQUIRE(cmp(to_key("B"), to_key("AB")) > 0);
    BOOST_REQUIRE(cmp(to_key("B"), to_key("A"))  > 0);
    BOOST_REQUIRE(cmp(to_key("A"), to_key("A")) == 0);
}

BOOST_AUTO_TEST_CASE(test_legacy_ordering_for_composite_keys) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("c1", bytes_type, column_kind::partition_key)
            .with_column("c2", bytes_type, column_kind::partition_key)
            .build();
    const schema& s = *s_ptr;

    auto to_key = [&s] (sstring v1, sstring v2) {
        return partition_key::from_exploded(s, std::vector<bytes>{to_bytes(v1), to_bytes(v2)});
    };

    auto cmp = [&s] (const partition_key& k1, const partition_key& k2) {
        return k1.legacy_tri_compare(s, k2);
    };

    BOOST_REQUIRE(cmp(to_key("A", "B"), to_key("A", "B")) == 0);
    BOOST_REQUIRE(cmp(to_key("A", "B"), to_key("A", "C")) < 0);
    BOOST_REQUIRE(cmp(to_key("A", "B"), to_key("B", "B")) < 0);
    BOOST_REQUIRE(cmp(to_key("A", "C"), to_key("B", "B")) < 0);
    BOOST_REQUIRE(cmp(to_key("B", "A"), to_key("A", "A")) > 0);

    BOOST_REQUIRE(cmp(to_key("AA", "B"), to_key("B", "B")) > 0);
    BOOST_REQUIRE(cmp(to_key("A", "AA"), to_key("A", "A")) > 0);

    BOOST_REQUIRE(cmp(to_key("", "A"), to_key("A", "A")) < 0);
    BOOST_REQUIRE(cmp(to_key("A", ""), to_key("A", "A")) < 0);
}

BOOST_AUTO_TEST_CASE(test_conversions_between_view_and_wrapper) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("c1", bytes_type, column_kind::partition_key)
            .build();
    const schema& s = *s_ptr;

    auto key = partition_key::from_deeply_exploded(s, {data_value(bytes("value"))});
    partition_key_view key_view = key;

    BOOST_REQUIRE(key_view.equal(s, key));
    BOOST_REQUIRE(key.equal(s, key_view));

    partition_key key2 = key_view;

    BOOST_REQUIRE(key2.equal(s, key));
    BOOST_REQUIRE(key.equal(s, key2));

    BOOST_REQUIRE(*key.begin(s) == to_managed_bytes("value"));
}

template<typename T>
inline
T reserialize(const T& v) {
    auto buf = ser::serialize_to_buffer<bytes>(v);
    auto in = ser::as_input_stream(buf);
    return ser::deserialize(in, std::type_identity<T>());
}

BOOST_AUTO_TEST_CASE(test_serialization) {
    auto s = schema_builder(1, "ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("v", bytes_type)
            .build();

    auto pk_value = bytes("value");
    partition_key key(std::vector<bytes>({pk_value}));

    BOOST_REQUIRE(key.equal(*s, reserialize(key)));
}


BOOST_AUTO_TEST_CASE(test_from_nodetool_style_string_single_partition_key) {
    auto s1 = schema_builder(1, "", "")
            .with_column("c1", utf8_type, column_kind::partition_key)
            .with_column("c2", bytes_type, column_kind::clustering_key)
            .with_column("c3", bytes_type, column_kind::clustering_key)
            .with_column("c4", bytes_type, column_kind::clustering_key)
            .build();

    auto pk_value = bytes("value");
    partition_key key1(std::vector<bytes>({pk_value}));
    auto key2 = partition_key::from_nodetool_style_string(s1, "value");
    BOOST_REQUIRE(key1.equal(*s1, key2));

    auto pk_with_col_value = bytes("val:ue");
    partition_key key_with_col(std::vector<bytes>({pk_with_col_value}));
    BOOST_REQUIRE(key_with_col.equal(*s1, partition_key::from_nodetool_style_string(s1, "val:ue")));
}

BOOST_AUTO_TEST_CASE(test_from_nodetool_style_string_composite_partition_key) {
    auto s2 = schema_builder(1, "", "")
            .with_column("c1", utf8_type, column_kind::partition_key)
            .with_column("c2", utf8_type, column_kind::partition_key)
            .with_column("c3", bytes_type, column_kind::clustering_key)
            .with_column("c4", bytes_type, column_kind::clustering_key)
            .build();

    auto pk_value1 = bytes("value1");
    auto pk_value2 = bytes("value2");
    partition_key key3(std::vector<bytes>({pk_value1, pk_value2}));
    auto key4 = partition_key::from_nodetool_style_string(s2, "value1:value2");
    BOOST_REQUIRE(key3.equal(*s2, key4));

    BOOST_REQUIRE_THROW(partition_key::from_nodetool_style_string(s2, "value1:value2:extra"), std::invalid_argument);
    BOOST_REQUIRE_THROW(partition_key::from_nodetool_style_string(s2, "value1"), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(test_legacy_clustering_key_prefix) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck1", int32_type, column_kind::clustering_key)
            .with_column("ck2", utf8_type, column_kind::clustering_key)
            .build();
    const schema& s = *s_ptr;

    std::vector<clustering_key_prefix> keys{
        clustering_key_prefix::make_empty(s),
        clustering_key_prefix::from_exploded(s, {int32_type->decompose(7)}),
        clustering_key_prefix::from_exploded(s, {int32_type->decompose(7), utf8_type->decompose(sstring("x"))}),
        clustering_key_prefix::from_exploded(s, {bytes(), bytes()}),
    };

    for (const auto& ck : keys) {
        auto legacy = ck.to_legacy(s);
        BOOST_REQUIRE(clustering_key_prefix::from_legacy(s, legacy).equal(s, ck));
        BOOST_REQUIRE(legacy.explode() == ck.explode(s));

        // The adaptor and clustering_key_prefix share the IDL wire format.
        auto legacy_buf = ser::serialize_to_buffer<bytes>(legacy);
        BOOST_REQUIRE(legacy_buf == ser::serialize_to_buffer<bytes>(ck));
        BOOST_REQUIRE(ser::deserialize_from_buffer(legacy_buf, std::type_identity<clustering_key_prefix>()).equal(s, ck));
        auto legacy_from_wire = ser::deserialize_from_buffer(ser::serialize_to_buffer<bytes>(ck), std::type_identity<legacy_clustering_key_prefix>());
        BOOST_REQUIRE(clustering_key_prefix::from_legacy(s, legacy_from_wire).equal(s, ck));
    }
}

namespace {

// Implements only the pure virtual operations, so the generic ones run.
class minimal_primary_key_layout final : public primary_key_layout {
    compound_type<allow_prefixes::yes> _compound;
public:
    explicit minimal_primary_key_layout(std::vector<data_type> types) : primary_key_layout(types), _compound(types) {}
    void decompose(managed_bytes_view key, primary_key_components& out) const override {
        for (managed_bytes_view c : _compound.components(key)) {
            out.push_back(c);
        }
    }
    std::strong_ordering compare(managed_bytes_view k1, managed_bytes_view k2) const override { return _compound.compare(k1, k2); }
    bool equal(managed_bytes_view k1, managed_bytes_view k2) const override { return _compound.equal(k1, k2); }
    size_t hash(managed_bytes_view key) const override { return _compound.hash(key); }
    managed_bytes serialize(std::span<const managed_bytes_view> components) const override { return _compound.serialize_value(components); }
};

}

BOOST_AUTO_TEST_CASE(test_primary_key_layouts_match_compound_type) {
    std::vector<data_type> types{int32_type, reversed_type_impl::get_instance(utf8_type), bytes_type};
    legacy_primary_key_layout legacy(types);
    minimal_primary_key_layout minimal(types);
    compound_type<allow_prefixes::yes> compound(types);

    std::vector<std::vector<bytes>> keys{
        {},
        {int32_type->decompose(1)},
        {int32_type->decompose(1), utf8_type->decompose(sstring("a"))},
        {int32_type->decompose(1), utf8_type->decompose(sstring("b")), bytes("x")},
        {int32_type->decompose(2), bytes(), bytes()},
        {bytes()},
    };

    for (const primary_key_layout* layout : {static_cast<const primary_key_layout*>(&legacy), static_cast<const primary_key_layout*>(&minimal)}) {
        for (const auto& k1 : keys) {
            auto b1 = layout->serialize_value(k1);
            BOOST_REQUIRE(b1 == compound.serialize_value(k1));
            // Components yielded as temporaries are copied before serialization.
            BOOST_REQUIRE(b1 == layout->serialize_value(k1 | std::views::transform([] (const bytes& b) { return bytes(b); })));
            BOOST_REQUIRE(layout->deserialize_value(b1) == k1);
            auto components = layout->components(b1);
            BOOST_REQUIRE(std::vector<managed_bytes_view>(components.begin(), components.end()) == std::vector<managed_bytes_view>(compound.begin(b1), compound.end(b1)));
            BOOST_REQUIRE_EQUAL(layout->size(b1), k1.size());
            BOOST_REQUIRE_EQUAL(layout->is_full(b1), compound.is_full(b1));
            BOOST_REQUIRE_EQUAL(layout->hash(b1), compound.hash(b1));
            layout->validate(b1);
            for (const auto& k2 : keys) {
                auto b2 = layout->serialize_value(k2);
                BOOST_REQUIRE(layout->compare(b1, b2) == compound.compare(b1, b2));
                BOOST_REQUIRE_EQUAL(layout->equal(b1, b2), compound.equal(b1, b2));
                BOOST_REQUIRE(layout->prefix_equal_compare(b1, b2) == prefix_equality_tri_compare(types.begin(),
                    compound.begin(b1), compound.end(b1), compound.begin(b2), compound.end(b2), ::tri_compare));
            }
        }
    }

    // Layouts depend only on the types, and treat reversed types like their underlying types.
    auto unreversed = make_primary_key_layout({int32_type, utf8_type, bytes_type});
    for (const auto& k : keys) {
        BOOST_REQUIRE(unreversed->serialize_value(k) == legacy.serialize_value(k));
    }
}

BOOST_AUTO_TEST_CASE(test_int32_pk_layout_orders_like_compound_type) {
    std::vector<std::vector<bytes>> keys{
        {},
        {bytes()},
        {int32_type->decompose(std::numeric_limits<int32_t>::min())},
        {int32_type->decompose(int32_t(-5))},
        {int32_type->decompose(int32_t(0))},
        {int32_type->decompose(int32_t(7))},
        {int32_type->decompose(int32_t(256))},
        {int32_type->decompose(std::numeric_limits<int32_t>::max())},
    };

    for (const auto& type : std::vector<data_type>{int32_type, reversed_type_impl::get_instance(int32_type)}) {
        std::vector<data_type> types{type};
        int32_pk_layout layout(types);
        compound_type<allow_prefixes::yes> compound(types);

        for (const auto& k1 : keys) {
            auto b1 = layout.serialize_value(k1);
            BOOST_REQUIRE_EQUAL(b1.size(), size_t(k1.empty() ? 0 : k1[0].empty() ? 1 : 4));
            BOOST_REQUIRE(layout.deserialize_value(b1) == k1);
            BOOST_REQUIRE_EQUAL(layout.size(b1), k1.size());
            BOOST_REQUIRE_EQUAL(layout.is_full(b1), k1.size() == 1);
            layout.validate(b1);
            auto c1 = compound.serialize_value(k1);
            for (const auto& k2 : keys) {
                auto b2 = layout.serialize_value(k2);
                auto c2 = compound.serialize_value(k2);
                BOOST_REQUIRE(layout.compare(b1, b2) == compound.compare(c1, c2));
                BOOST_REQUIRE(layout.prefix_equal_compare(b1, b2) == prefix_equality_tri_compare(types.begin(),
                    compound.begin(c1), compound.end(c1), compound.begin(c2), compound.end(c2), ::tri_compare));
                BOOST_REQUIRE_EQUAL(layout.equal(b1, b2), compound.equal(c1, c2));
                if (layout.equal(b1, b2)) {
                    BOOST_REQUIRE_EQUAL(layout.hash(b1), layout.hash(b2));
                }
            }
        }

        // A key in the legacy layout is an internal error.
        auto legacy_key = compound.serialize_value(keys[3]);
        BOOST_REQUIRE_THROW(layout.size(legacy_key), std::runtime_error);
        BOOST_REQUIRE_THROW(layout.compare(legacy_key, legacy_key), std::runtime_error);
    }
}
