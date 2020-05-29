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

#include "utils/cached_file.hh"

#include <seastar/core/reactor.hh>
#include <seastar/core/file.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

#include <chrono>
#include <iostream>

static seastar::logger testlog("test");

using namespace std::chrono_literals;
using namespace seastar;

void search(cached_file& cf, size_t buf_size) {
    size_t pos = cf.size() / 2;
    while (pos) {
        auto stream = cf.read(pos, default_priority_class(), buf_size);
        stream.next().get();
        pos /= 2;
    }
}

void dynamic_buf_search(cached_file& cf) {
    size_t pos = cf.size() / 2;
    while (pos) {
        auto area = pos * 2;
        size_t buf_size;
        if (area <= 128*1024) {
            buf_size = 128*1024;
        } else {
            buf_size = 4*1024;
        }
        auto stream = cf.read(pos, default_priority_class(), buf_size);
        stream.next().get();
        pos /= 2;
    }
}

int main(int argc, char** argv) {
    using clock = std::chrono::high_resolution_clock;

    app_template app;
    return app.run(argc, argv, [] {
        return seastar::async([] {
            const size_t K = 1024;
            auto f = open_file_dma("testfile.tmp", open_flags::ro).get0();

            for (auto area_size : {4 * K,
                                   16 * K,
                                   32 * K,
                                   64 * K,
                                   80 * K,
                                   96 * K,
                                   128 * K,
                                   160 * K,
                                   192 * K,
                                   256 * K,
                                   1024 * K}) {

                testlog.info("area: {}", area_size);

                for (auto buf_size : {4 * K,
                                      8 * K,
                                      16 * K,
                                      32 * K,
                                      64 * K,
                                      128 * K}) {

                    auto start = clock::now();
                    for (int i = 0; i < 100; ++i) {
                        cached_file cf(f, 0, area_size);
                        search(cf, buf_size);
                    }
                    auto end = clock::now();

                    auto duration = std::chrono::duration<double>(end - start).count();
                    testlog.info("buf: {}, time: {} [ms]", buf_size, format("{:.2f}", duration * 1000));
                }

                auto start = clock::now();
                for (int i = 0; i < 100; ++i) {
                    cached_file cf(f, 0, area_size);
                    dynamic_buf_search(cf);
                }
                auto end = clock::now();

                auto duration = std::chrono::duration<double>(end - start).count();
                testlog.info("buf: dynamic, time: {} [ms]", format("{:.2f}", duration * 1000));
            }
        });
    });
}
