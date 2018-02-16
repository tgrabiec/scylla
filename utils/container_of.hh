/*
 * Copyright (C) 2018 ScyllaDB
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

#pragma once

#include <cstddef>

template <typename Container, typename T>
inline
std::ptrdiff_t constexpr offset_of_member(T Container::* member) {
    const char storage[sizeof(Container)] = { 0 };
    const Container* c = reinterpret_cast<const Container*>(&storage);
    return std::ptrdiff_t(&(c->*member)) - std::ptrdiff_t(c);
}

/// Returns a reference to the containing object given
/// a reference to the member.
///
/// Example:
///
///   struct S {
///       int x;
///   };
///   S s1;
///   int& x_ref = s1.x;
///   S& s2 = container_of(&S::x, x_ref);
///   assert(&s2 == &s1);
///
template <typename Container, typename T>
Container& container_of(T Container::* member, T& t) {
    return *reinterpret_cast<Container*>(reinterpret_cast<char*>(&t) - offset_of_member(member));
}
