/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/algorithm/random_shuffle.hpp>
#include <boost/range/adaptors.hpp>
#include <seastar/util/variant_utils.hh>
#include <map>

#include "utils/btree.hh"

#include "tests/random-utils.hh"

struct item {
    int value;
    item(int v) : value(v) {}
    bool operator==(const item& other) const { return value == other.value; }
    bool operator!=(const item& other) const { return !(*this == other); }

    friend std::ostream& operator<<(std::ostream& out, const item& it) {
        return out << it.value;
    }

    static auto transformed_to_value() {
        return boost::adaptors::transformed([] (const item& i) { return i.value; });
    }
};

struct item_less_cmp {
    bool operator()(const item& i1, const item& i2) const { return i1.value < i2.value; }
    bool operator()(int i1, const item& i2) const { return i1 < i2.value; }
    bool operator()(const item& i1, int i2) const { return i1.value < i2; }
};

BOOST_AUTO_TEST_CASE(test_erase) {
    std::set<item, item_less_cmp> reference_set;
    btree<item, item_less_cmp> set;

    for (int i = 0; i < 100; ++i) {
        set.insert(item(i));
        reference_set.insert(item(i));
    }

    auto check_iterators = [&] (btree<item, item_less_cmp>::iterator i1, std::set<item, item_less_cmp>::iterator i2) {
        if (i1 == set.end()) {
            BOOST_REQUIRE(i2 == reference_set.end());
        } else {
            BOOST_REQUIRE_EQUAL(i1->value, i2->value);
        }
    };

    {
        auto i1 = set.erase(set.lower_bound(3), set.lower_bound(10));
        auto i2 = reference_set.erase(reference_set.lower_bound(3), reference_set.lower_bound(10));
        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());
        check_iterators(i1, i2);
    }

    {
        auto i1 = set.erase(set.lower_bound(0), set.lower_bound(5));
        auto i2 = reference_set.erase(reference_set.lower_bound(0), reference_set.lower_bound(5));
        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());
        check_iterators(i1, i2);
    }

    {
        auto i1 = set.erase(set.lower_bound(10), set.lower_bound(10));
        auto i2 = reference_set.erase(reference_set.lower_bound(10), reference_set.lower_bound(10));
        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());
        check_iterators(i1, i2);
    }

    {
        auto i1 = set.erase(set.lower_bound(10), set.lower_bound(11));
        auto i2 = reference_set.erase(reference_set.lower_bound(10), reference_set.lower_bound(11));
        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());
        check_iterators(i1, i2);
    }

    {
        auto i1 = set.erase(set.begin(), set.end());
        auto i2 = reference_set.erase(reference_set.begin(), reference_set.end());
        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());
        check_iterators(i1, i2);
    }
}

BOOST_AUTO_TEST_CASE(test_reverse_iteration) {
    std::set<item, item_less_cmp> reference_set;
    btree<item, item_less_cmp> set;

    BOOST_REQUIRE_EQUAL_COLLECTIONS(set.rbegin(), set.rend(), reference_set.rbegin(), reference_set.rend());

    for (int i = 0; i < 100; ++i) {
        set.insert(item(i));
        reference_set.insert(item(i));
    }

    auto i1 = std::prev(set.end());
    int i = 99;
    while (true) {
        BOOST_REQUIRE_EQUAL(i1->value, i);
        if (i == 0) {
            break;
        }
        --i1;
        --i;
    }

    BOOST_REQUIRE_EQUAL_COLLECTIONS(set.rbegin(), set.rend(), reference_set.rbegin(), reference_set.rend());
}

BOOST_AUTO_TEST_CASE(test_end_iterator_is_valid) {
    std::set<item, item_less_cmp> reference_set;
    btree<item, item_less_cmp> set;

    set.insert(item(0));
    set.insert(item(1));

    BOOST_REQUIRE(std::prev(set.end()) == set.find(1));
}

BOOST_AUTO_TEST_CASE(test_consistent_with_std_set) {
    std::set<item, item_less_cmp> reference_set;
    btree<item, item_less_cmp> set;

    BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());

    while (reference_set.size() < 1000) {
        auto value = tests::random::get_int<int>();
        std::cout << "insert " << value << "\n";
        set.insert(item(value));
        reference_set.insert(item(value));

        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());
    }

    btree<item, item_less_cmp> set2;
    set2.clone_from(set, [] (item i) { return i; });
    BOOST_REQUIRE_EQUAL_COLLECTIONS(set2.begin(), set2.end(), reference_set.begin(), reference_set.end());

    for (int i = 0; i < 100; ++i) {
        auto value = tests::random::get_int<int>();
        std::cout << "rnd check " << value << "\n";

        {
            auto in_reference = reference_set.find(item(value)) != reference_set.end();
            auto in_set = set.find(item(value)) != set.end();

            BOOST_REQUIRE_EQUAL(in_reference, in_set);
        }

        {
            auto i1 = reference_set.lower_bound(item(value));
            auto i2 = set.lower_bound(item(value));

            if (i1 == reference_set.end()) {
                BOOST_REQUIRE(i2 == set.end());
            } else {
                BOOST_REQUIRE_EQUAL(i1->value, i2->value);
            }
        }
    }

    for (auto&& k : reference_set) {
        std::cout << "check " << k.value << "\n";

        auto i = set.find(k);
        BOOST_REQUIRE(i != set.end());
        BOOST_REQUIRE_EQUAL(k.value, i->value);
    }

    auto values = boost::copy_range<std::vector<int>>(reference_set | item::transformed_to_value());
    boost::random_shuffle(values);

    for (auto&& v : values) {
        std::cout << "remove " << v << "\n";

        auto i1 = set.find(v);
        BOOST_REQUIRE(i1 != set.end());
        i1 = set.erase(i1);

        auto i2 = reference_set.erase(reference_set.find(v));

        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());

        if (i1 == set.end()) {
            BOOST_REQUIRE(i2 == reference_set.end());
        } else {
            BOOST_REQUIRE_EQUAL(*i1, *i2);
        }
    }

    for (int i = 0; i < 1000; ++i) {
        auto value = i;
        std::cout << "insert " << value << "\n";
        set.insert(item(value));
        reference_set.insert(item(value));

        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());
    }

    for (int v = 0; v < 1000; ++v) {
        std::cout << "remove " << v << "\n";

        auto i = set.find(v);
        BOOST_REQUIRE(i != set.end());
        set.erase(i);

        reference_set.erase(reference_set.find(v));

        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());
    }
}
