/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/http/exception.hh>

#include "api/api.hh"
#include "api/utils.hh"
#include "api/api-doc/clocks_test.json.hh"
#include "clocks_test.hh"
#include "db/config.hh"

namespace api {

namespace hct = httpd::clocks_test_json;
using namespace json;
using namespace seastar::httpd;

void set_clocks_test(http_context& ctx, routes& r, db::config& cfg) {
    // Moves gc_clock, db_clock and write timestamps through
    // test_clocks_offset_seconds, whose observer applies the offset on every
    // shard. Also moves lowres_clock, which expires due timeouts.
    hct::forward_jump.set(r, [&cfg] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto seconds = require_query_param<int64_t>(*req, "seconds");
        if (seconds < 0) {
            throw httpd::bad_param_exception(fmt::format("seconds: must not be negative, got {}", seconds));
        }
        auto offset = cfg.test_clocks_offset_seconds() + seconds;
        co_await cfg.test_clocks_offset_seconds.set_value_on_all_shards(fmt::to_string(offset), utils::config_file::config_source::API);
        co_await lowres_clock::forward_jump(std::chrono::seconds(seconds));
        co_return json_void();
    });

    hct::get_offset.set(r, [&cfg] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        co_return cfg.test_clocks_offset_seconds();
    });
}

void unset_clocks_test(http_context& ctx, routes& r) {
    hct::forward_jump.unset(r);
    hct::get_offset.unset(r);
}

}
