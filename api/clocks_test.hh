/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#ifdef SCYLLA_ENABLE_ERROR_INJECTION

#pragma once

namespace db {
class config;
}

namespace seastar::httpd {
class routes;
}

namespace api {
struct http_context;

void set_clocks_test(http_context& ctx, seastar::httpd::routes& r, db::config& cfg);
void unset_clocks_test(http_context& ctx, seastar::httpd::routes& r);

}

#endif
