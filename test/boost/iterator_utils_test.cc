
#define BOOST_TEST_MODULE core

#include "utils/at_most.hh"
#include "to_string.hh"

#include <boost/range/adaptors.hpp>
#include <boost/test/unit_test.hpp>

#include <vector>


BOOST_AUTO_TEST_CASE(test_at_most) {
    std::vector<int> seq = {1, 2, 3};

    {
        auto v = boost::copy_range<std::vector<int>>(
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end()));
        BOOST_REQUIRE_EQUAL(v, seq);
    }

    {
        auto v = boost::copy_range<std::vector<int>>(at_most(1,
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())));
        auto expected = seq;
        expected.resize(1);
        BOOST_REQUIRE_EQUAL(v, expected);
    }

    {
        auto v = boost::copy_range<std::vector<int>>(at_most(2,
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())));
        auto expected = seq;
        expected.resize(2);
        BOOST_REQUIRE_EQUAL(v, expected);
    }

    {
        auto v = boost::copy_range<std::vector<int>>(at_most(0,
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())));
        auto expected = seq;
        expected.resize(0);
        BOOST_REQUIRE_EQUAL(v, expected);
    }

    {
        auto v = boost::copy_range<std::vector<int>>(at_most(seq.size(),
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())));
        auto expected = seq;
        BOOST_REQUIRE_EQUAL(v, expected);
    }

    {
        auto v = boost::copy_range<std::vector<int>>(at_most(5,
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())));
        auto expected = seq;
        BOOST_REQUIRE_EQUAL(v, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_at_most_filtered_is_like_at_most) {
    std::vector<int> seq = {1, 2, 3};

    {
        auto v = boost::copy_range<std::vector<int>>(
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end()));
        BOOST_REQUIRE_EQUAL(v, seq);
    }

    {
        auto v = boost::copy_range<std::vector<int>>(at_most_filtered(1,
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())));
        auto expected = seq;
        expected.resize(1);
        BOOST_REQUIRE_EQUAL(v, expected);
    }

    {
        auto v = boost::copy_range<std::vector<int>>(at_most_filtered(2,
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())));
        auto expected = seq;
        expected.resize(2);
        BOOST_REQUIRE_EQUAL(v, expected);
    }

    {
        auto v = boost::copy_range<std::vector<int>>(at_most_filtered(0,
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())));
        auto expected = seq;
        expected.resize(0);
        BOOST_REQUIRE_EQUAL(v, expected);
    }

    {
        auto v = boost::copy_range<std::vector<int>>(at_most_filtered(seq.size(),
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())));
        auto expected = seq;
        BOOST_REQUIRE_EQUAL(v, expected);
    }

    {
        auto v = boost::copy_range<std::vector<int>>(at_most_filtered(5,
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())));
        auto expected = seq;
        BOOST_REQUIRE_EQUAL(v, expected);
    }
}
BOOST_AUTO_TEST_CASE(test_at_most_filtered_visits_all) {
    std::vector<int> seq = {1, 2, 3};

    {
        std::set<int> visited;
        auto v = boost::copy_range<std::vector<int>>(at_most_filtered(1,
            boost::iterator_range<std::vector<int>::iterator>(seq.begin(), seq.end())
                | boost::adaptors::transformed([&] (int v) {
                    visited.emplace(v);
                    return v;
                })));
        auto expected = seq;
        expected.resize(1);
        BOOST_REQUIRE_EQUAL(v, expected);
        BOOST_REQUIRE_EQUAL(visited.size(), seq.size());
    }
}
