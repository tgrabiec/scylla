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
};

BOOST_AUTO_TEST_CASE(test_consistent_with_map) {
    std::set<item> reference_set;
    btree<item> set;

    while (reference_set.size() < 1000) {
        auto value = tests::random::get_int<int>();
        std::cout << "insert " << value << "\n";
        set.insert(item(value));
        reference_set.insert(item(value));

        BOOST_REQUIRE_EQUAL_COLLECTIONS(set.begin(), set.end(), reference_set.begin(), reference_set.end());
    }
}
