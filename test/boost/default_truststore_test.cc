/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/net/tls.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include "test/lib/tmpdir.hh"
#include "utils/http.hh"

using namespace std::chrono_literals;

BOOST_AUTO_TEST_SUITE(default_truststore_test)

static const std::string certs_dir = "test/resource/certs";

static future<> serve(connected_socket s) {
    co_await tls::get_dn_information(s);
    auto out = s.output();
    co_await out.close();
}

// Accepts TLS connections presenting a certificate signed by cacert.pem.
// Serves clients from all shards on the shard which created it.
class test_server {
    server_socket _socket;
    gate _connections;
    future<> _accept_loop = make_ready_future<>();

    future<> accept_loop() {
        for (;;) {
            auto f = co_await coroutine::as_future(_socket.accept());
            if (f.failed()) {
                f.ignore_ready_future();
                co_return;
            }
            (void)with_gate(_connections, [s = f.get().connection] () mutable {
                return serve(std::move(s));
            }).handle_exception([] (auto) {});
        }
    }
public:
    explicit test_server(shared_ptr<tls::server_credentials> creds)
        : _socket(tls::listen(std::move(creds), socket_address(net::inet_address("127.0.0.1"), 0), listen_options{
                .lba = server_socket::load_balancing_algorithm::fixed,
                .fixed_cpu = this_shard_id(),
            })) {
        _accept_loop = accept_loop();
    }
    socket_address address() const {
        return _socket.local_address();
    }
    future<> stop() {
        _socket.abort_accept();
        co_await std::move(_accept_loop);
        co_await _connections.close();
    }
};

static future<> with_server(std::function<future<>(socket_address)> f) {
    tls::credentials_builder b;
    co_await b.set_x509_key_file(certs_dir + "/scylla.pem", certs_dir + "/scylla.pem", tls::x509_crt_format::PEM);
    test_server server(b.build_server_credentials());
    std::exception_ptr ex;
    try {
        co_await f(server.address());
    } catch (...) {
        ex = std::current_exception();
    }
    co_await server.stop();
    if (ex) {
        std::rethrow_exception(ex);
    }
}

static future<> handshake(shared_ptr<tls::certificate_credentials> creds, socket_address addr) {
    auto s = co_await tls::connect(creds, addr, tls::tls_options{.server_name = "scylla"});
    co_await tls::get_dn_information(s);
    auto out = s.output();
    co_await out.close();
}

static future<bool> verification_fails(shared_ptr<tls::certificate_credentials> creds, socket_address addr) {
    try {
        co_await handshake(std::move(creds), addr);
    } catch (tls::verification_error&) {
        co_return true;
    }
    co_return false;
}

// Credentials set up by utils::http::set_default_trust().
static future<shared_ptr<tls::certificate_credentials>> default_trust_creds() {
    auto creds = make_shared<tls::certificate_credentials>();
    co_await utils::http::set_default_trust(*creds);
    co_return creds;
}

SEASTAR_TEST_CASE(test_default_truststore_file) {
    utils::http::set_default_truststore(certs_dir + "/cacert.pem");
    auto reset = defer([] () noexcept { utils::http::set_default_truststore({}); });
    co_await with_server([] (socket_address addr) -> future<> {
        co_await handshake(co_await default_trust_creds(), addr);
    });
}

SEASTAR_TEST_CASE(test_default_truststore_system) {
    utils::http::set_default_truststore({});
    co_await with_server([] (socket_address addr) -> future<> {
        // The system trust store does not contain the test CA.
        BOOST_REQUIRE(co_await verification_fails(co_await default_trust_creds(), addr));
    });
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

static thread_local uint64_t generation_before_reload;

SEASTAR_TEST_CASE(test_default_truststore_reload) {
    tmpdir tmp;
    auto ca = (tmp.path() / "ca.pem").native();
    // A CA which did not sign scylla.pem.
    std::filesystem::copy_file("test/pylib/resources/scylla.crt", ca);
    co_await smp::invoke_on_all([ca] { utils::http::set_default_truststore(ca); });
    co_await utils::http::start_default_truststore_watch();

    std::exception_ptr ex;
    try {
        co_await with_server([&] (socket_address addr) -> future<> {
            // Boost assertions are not thread-safe, so shards report failures by throwing.
            co_await smp::invoke_on_all([addr] -> future<> {
                if (!co_await verification_fails(co_await utils::http::system_trust_credentials(), addr)) {
                    throw std::runtime_error(format("shard {}: verification passed before reload", this_shard_id()));
                }
                generation_before_reload = utils::http::default_trust_generation();
            });

            // Replace the file the way update-ca-trust does.
            std::filesystem::copy_file(certs_dir + "/cacert.pem", tmp.path() / "ca.pem.new");
            std::filesystem::rename(tmp.path() / "ca.pem.new", ca);

            co_await smp::invoke_on_all([addr] -> future<> {
                auto creds = co_await utils::http::system_trust_credentials();
                for (int i = 0; utils::http::default_trust_generation() == generation_before_reload; ++i) {
                    if (i == 1000) {
                        throw std::runtime_error(format("shard {}: no reload", this_shard_id()));
                    }
                    co_await sleep(10ms);
                }
                // Rebuilt in place, so existing holders see the new CAs.
                if (creds != co_await utils::http::system_trust_credentials()) {
                    throw std::runtime_error(format("shard {}: credentials replaced", this_shard_id()));
                }
                co_await handshake(creds, addr);
            });
        });
    } catch (...) {
        ex = std::current_exception();
    }
    co_await utils::http::stop_default_truststore_watch();
    co_await smp::invoke_on_all([] { utils::http::set_default_truststore({}); });
    if (ex) {
        std::rethrow_exception(ex);
    }
}

BOOST_AUTO_TEST_SUITE_END()
