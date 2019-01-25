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
    bool operator<(const item& other) const { return value < other.value; }

    friend std::ostream& operator<<(std::ostream& out, const item& it) {
        return out << it.value;
    }

    static auto transformed_to_value() {
        return boost::adaptors::transformed([] (const item& i) { return i.value; });
    }
};

BOOST_AUTO_TEST_CASE(test_consistent_with_std_set) {
    std::set<item> reference_set;
    btree<item> set;

    BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());

    while (reference_set.size() < 1000) {
        auto value = tests::random::get_int<int>();
        std::cout << "insert " << value << "\n";
        set.insert(item(value));
        reference_set.insert(item(value));

        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());
    }

    for (int i = 0; i < 100; ++i) {
        auto value = tests::random::get_int<int>();
        std::cout << "rnd check " << value << "\n";

        auto in_reference = reference_set.find(item(value)) != reference_set.end();
        auto in_set = set.find(item(value)) != set.end();

        BOOST_REQUIRE_EQUAL(in_reference, in_set);
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

        auto i = set.find(v);
        BOOST_REQUIRE(i != set.end());
        i = set.erase(i);

        auto ref_i = reference_set.erase(reference_set.find(v));

        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());

        if (i == set.end()) {
            BOOST_REQUIRE(ref_i == reference_set.end());
        } else {
            BOOST_REQUIRE_EQUAL(*i, *ref_i);
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
