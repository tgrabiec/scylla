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

#pragma once

#include "utils/extremum_tracking.hh"
#include "utils/estimated_histogram.hh"

#include <seastar/core/print.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/weak_ptr.hh>
#include "seastarx.hh"

#include <chrono>
#include <iosfwd>
#include <boost/range/irange.hpp>

template<typename Func>
inline
auto duration_in_seconds(Func&& f) {
    using clk = std::chrono::steady_clock;
    auto start = clk::now();
    f();
    auto end = clk::now();
    return std::chrono::duration_cast<std::chrono::duration<float>>(end - start);
}

class perf_caliper {
    using clk = std::chrono::steady_clock;
    utils::estimated_histogram _hist{30000};
    min_max_tracker<clk::duration> _minmax;
    mutable std::deque<clk::duration> _samples;
public:
    struct started {
        clk::time_point started_at;
    };
    started start() {
        return { clk::now() };
    }
    void end(started s) {
        auto d = clk::now() - s.started_at;
        _samples.push_back(d);
        _minmax.update(d);
        _hist.add(d.count());
    }
    const utils::estimated_histogram& histogram() const {
        return _hist;
    }
    clk::duration min() const { return _minmax.min(); }
    clk::duration max() const { return _minmax.max(); }

    friend std::ostream& operator<<(std::ostream&, const perf_caliper&);
};

std::ostream& operator<<(std::ostream& out, const perf_caliper& pc) {
    auto to_ms = [] (int64_t nanos) {
        return float(nanos) / 1e6;
    };
    auto perc = [] (const std::deque<perf_caliper::clk::duration>& samples, float pos) {
        return samples[(std::max<size_t>(1, samples.size()) - 1) * pos];
    };
    auto& samples = pc._samples;
    boost::sort(samples);
    return out << sprint("{count: %d, "
                         "min: %.6f [ms], "
                         "50%%: %.6f [ms], "
                         "90%%: %.6f [ms], "
                         "99%%: %.6f [ms], "
                         "max: %.6f [ms]}",
        samples.size(),
        to_ms(pc.min().count()),
        to_ms(perc(samples, 0.5).count()),
        to_ms(perc(samples, 0.9).count()),
        to_ms(perc(samples, 0.99).count()),
        to_ms(pc.max().count()));
}
