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


#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/fstream.hh>

#include <iostream>

#include "utils/cached_file.hh"
#include "tests/random-utils.hh"
#include "tests/tmpdir.hh"

#include <random>

static sstring read_to_string(cached_file::stream& s) {
    sstring b;
    while (auto buf = s.next().get0()) {
        b += sstring(buf.get(), buf.size());
    }
    return b;
}

SEASTAR_THREAD_TEST_CASE(test_reading_from_small_file) {
    tmpdir t;
    file f = open_file_dma((t.path() / "file").c_str(), open_flags::create | open_flags::rw).get0();

    auto file_contents = tests::random::get_sstring(1024);

    std::cout << file_contents << "\n";

    output_stream<char> out = make_file_output_stream(f);
    out.write(file_contents.begin(), file_contents.size()).get();
    out.flush().get();

    {
        cached_file cf(f, 0, file_contents.size());

        {
            auto s = cf.read(0, default_priority_class());
            BOOST_REQUIRE_EQUAL(file_contents, read_to_string(s));
        }

        {
            auto s = cf.read(2, default_priority_class());
            BOOST_REQUIRE_EQUAL(file_contents.substr(2), read_to_string(s));
        }

        {
            auto s = cf.read(3000, default_priority_class());
            BOOST_REQUIRE_EQUAL(sstring(), read_to_string(s));
        }
    }

    {
        size_t off = 100;
        cached_file cf(f, off, file_contents.size() - off);

        {
            auto s = cf.read(0, default_priority_class());
            BOOST_REQUIRE_EQUAL(file_contents.substr(off), read_to_string(s));
        }

        {
            auto s = cf.read(2, default_priority_class());
            BOOST_REQUIRE_EQUAL(file_contents.substr(off + 2), read_to_string(s));
        }

        {
            auto s = cf.read(3000, default_priority_class());
            BOOST_REQUIRE_EQUAL(sstring(), read_to_string(s));
        }
    }
}
