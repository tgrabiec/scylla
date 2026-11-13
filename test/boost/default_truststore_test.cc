/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/defer.hh>

#include "utils/http.hh"

BOOST_AUTO_TEST_SUITE(default_truststore_test)

static const std::string certs_dir = "test/resource/certs";

// Handshakes with a local server presenting a certificate signed by cacert.pem,
// verifying it with credentials set up by utils::http::set_default_trust().
static future<> handshake() {
    tls::credentials_builder b;
    co_await b.set_x509_key_file(certs_dir + "/scylla.pem", certs_dir + "/scylla.pem", tls::x509_crt_format::PEM);
    auto server = tls::listen(b.build_server_credentials(), socket_address(net::inet_address("127.0.0.1"), 0));

    auto serve = server.accept().then([] (accept_result ar) -> future<> {
        auto s = std::move(ar.connection);
        co_await tls::get_dn_information(s);
        auto out = s.output();
        co_await out.close();
    }).handle_exception([] (auto) {});

    std::exception_ptr ex;
    try {
        auto creds = make_shared<tls::certificate_credentials>();
        co_await utils::http::set_default_trust(*creds);
        auto s = co_await tls::connect(creds, server.local_address(), tls::tls_options{.server_name = "scylla"});
        co_await tls::get_dn_information(s);
        auto out = s.output();
        co_await out.close();
    } catch (...) {
        ex = std::current_exception();
    }
    server.abort_accept();
    co_await std::move(serve);
    if (ex) {
        std::rethrow_exception(ex);
    }
}

SEASTAR_TEST_CASE(test_default_truststore_file) {
    utils::http::set_default_truststore(certs_dir + "/cacert.pem");
    auto reset = defer([] () noexcept { utils::http::set_default_truststore({}); });
    co_await handshake();
}

SEASTAR_TEST_CASE(test_default_truststore_system) {
    utils::http::set_default_truststore({});
    bool failed = false;
    try {
        co_await handshake();
    } catch (...) {
        failed = true;
    }
    // The system trust store does not contain the test CA.
    BOOST_REQUIRE(failed);
}

SEASTAR_TEST_CASE(test_default_truststore_missing_file) {
    utils::http::set_default_truststore("/nonexistent/ca.pem");
    auto reset = defer([] () noexcept { utils::http::set_default_truststore({}); });
    auto creds = make_shared<tls::certificate_credentials>();
    bool failed = false;
    try {
        co_await utils::http::set_default_trust(*creds);
    } catch (...) {
        failed = true;
    }
    BOOST_REQUIRE(failed);
}

BOOST_AUTO_TEST_SUITE_END()
