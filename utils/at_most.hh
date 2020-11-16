/*
 * Copyright (C) 2020 ScyllaDB
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

#include <cstdint>
#include <iterator>

#include <boost/range/adaptor/filtered.hpp>

namespace {

template<typename Iterator>
class at_most_iterator : public std::iterator<std::input_iterator_tag, typename Iterator::value_type> {
    Iterator _i;
    ssize_t _left;
public:
    struct end_tag {};
    using value_type = typename Iterator::value_type;
public:
    at_most_iterator(ssize_t left, Iterator i) : _i(i), _left(left) {}

    at_most_iterator& operator++() {
        --_left;
        ++_i;
        return *this;
    }

    at_most_iterator operator++(int) {
        at_most_iterator it(*this);
        operator++();
        return it;
    }

    value_type& operator*() { return *_i; }
    value_type* operator->() { return &*_i; }

    bool operator==(const at_most_iterator& o) {
        return _i == o._i || (!_left && !o._left);
    }

    bool operator!=(const at_most_iterator& o) { return !(*this == o); }
};

}

/// Constructs an iterator range which contains a subset of a given range which contains
/// at most n first elements of it.
///
/// Equivalent to boost::adaptors::sliced(0, n) but works with ranges which are not random-access.
template<typename Range>
inline
auto at_most(int n, Range r) {
    using iterator = typename Range::iterator;
    return boost::iterator_range<at_most_iterator<iterator>>(
        at_most_iterator<iterator>(n, r.begin()),
        at_most_iterator<iterator>(0, r.end()));
}

/// Like at_most() but guarantees that every element of Range is visited with its iterator.
template<typename Range>
inline
auto at_most_filtered(int n, Range r) {
    struct filter {
        mutable ssize_t n;

        bool operator()(const typename Range::value_type&) const {
            return n-- > 0;
        }
    };
    return r | boost::adaptors::filtered(filter{n});
}
