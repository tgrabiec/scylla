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

#include <functional>
#include <seastar/core/future-util.hh>
#include <seastar/util/noncopyable_function.hh>

class coroutine final {
public:
    coroutine() = default;
    coroutine(noncopyable_function<stop_iteration()> f) : _run(std::move(f)) {}
    stop_iteration run() { return _run(); }
    explicit operator bool() const { return bool(_run); }
private:
    noncopyable_function<stop_iteration()> _run;
};

inline
coroutine make_empty_coroutine() {
    return coroutine([] { return stop_iteration::yes; });
}

template <typename Func>
inline
coroutine run_coroutine(Func f) {
    if (f() == stop_iteration::yes) {
        return make_empty_coroutine();
    }
    return coroutine(std::move(f));
}
