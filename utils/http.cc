/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/*
 * Copyright (C) 2025-present ScyllaDB
 */

#include "http.hh"
#include "rest/client.hh"

#include <boost/regex.hpp>
#include <seastar/core/gate.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/all.hh>

static logging::logger dtlog("default_truststore");

namespace {

struct default_trust_state {
    std::string truststore;
    shared_ptr<tls::certificate_credentials> creds;
    uint64_t generation = 0;
    // Shard 0 only. Serves as the file watcher; connections use creds.
    shared_ptr<tls::certificate_credentials> watcher;
    std::optional<gate> reload_gate;
};

thread_local default_trust_state default_trust;

}

void utils::http::set_default_truststore(std::string path) {
    default_trust.truststore = std::move(path);
    default_trust.creds = nullptr;
    ++default_trust.generation;
}

future<> utils::http::set_default_trust(tls::certificate_credentials& creds) {
    if (!default_trust.truststore.empty()) {
        return creds.set_x509_trust_file(default_trust.truststore, tls::x509_crt_format::PEM);
    }
    return creds.set_system_trust();
}

future<> utils::http::set_default_trust(tls::credentials_builder& builder) {
    if (!default_trust.truststore.empty()) {
        return builder.set_x509_trust_file(default_trust.truststore, tls::x509_crt_format::PEM);
    }
    return builder.set_system_trust();
}

uint64_t utils::http::default_trust_generation() {
    return default_trust.generation;
}

future<shared_ptr<tls::certificate_credentials>> utils::http::system_trust_credentials() {
    while (!default_trust.creds) {
        auto gen = default_trust.generation;
        auto cred = make_shared<tls::certificate_credentials>();
        co_await set_default_trust(*cred);
        // Reloads rebuild only default_trust.creds, so a concurrent caller's object wins,
        // and a reload during the read may have left cred with stale contents.
        if (!default_trust.creds && gen == default_trust.generation) {
            default_trust.creds = std::move(cred);
        }
    }
    co_return default_trust.creds;
}

static future<> reload_default_trust() {
    try {
        if (default_trust.creds) {
            tls::credentials_builder b;
            co_await b.set_x509_trust_file(default_trust.truststore, tls::x509_crt_format::PEM);
            b.rebuild(*default_trust.creds);
        }
        ++default_trust.generation;
    } catch (...) {
        dtlog.warn("Failed to reload {}: {}", default_trust.truststore, std::current_exception());
    }
}

future<> utils::http::start_default_truststore_watch() {
    if (default_trust.truststore.empty()) {
        co_return;
    }
    tls::credentials_builder b;
    co_await b.set_x509_trust_file(default_trust.truststore, tls::x509_crt_format::PEM);
    default_trust.reload_gate.emplace();
    default_trust.watcher = co_await b.build_reloadable_certificate_credentials(
            [] (const tls::credentials_builder&, const std::unordered_set<sstring>& files, std::exception_ptr ep) -> future<> {
                if (!default_trust.reload_gate || default_trust.reload_gate->is_closed()) {
                    co_return;
                }
                auto holder = default_trust.reload_gate->hold();
                if (ep) {
                    dtlog.warn("Exception while reloading {}: {}", files, ep);
                    co_return;
                }
                dtlog.info("Reloading {}", files);
                co_await smp::invoke_on_all([] { return reload_default_trust(); });
            });
}

future<> utils::http::stop_default_truststore_watch() {
    if (default_trust.reload_gate) {
        co_await default_trust.reload_gate->close();
        default_trust.reload_gate.reset();
    }
    default_trust.watcher = nullptr;
}

future<> utils::http::dns_connection_factory::init_credentials() {
    if (_use_https && !_creds) {
        _creds = co_await system_trust_credentials();
    }
    if (!_use_https) {
        _creds = {};
    }
    _logger.debug("Initialized credentials, tls={}", _creds == nullptr ? "no" : "yes");
}

future<net::inet_address> utils::http::dns_connection_factory::get_address() {
    auto get_addr = [this] -> net::inet_address {
        const auto& addresses = _addr_list.value();
        return addresses[_addr_pos++ % addresses.size()];
    };

    if (_addr_list) {
        co_return get_addr();
    }
    auto units = co_await get_units(_init_semaphore, 1);
    if (!_addr_list) {
        auto hent = co_await net::dns::get_host_by_name(_host, net::inet_address::family::INET);
        _address_ttl = std::ranges::min_element(hent.addr_entries, [](const net::hostent::address_entry& lhs, const net::hostent::address_entry& rhs) {
                           return lhs.ttl < rhs.ttl;
                       })->ttl;
        if (_address_ttl.count() == 0) {
            co_return hent.addr_entries[_addr_pos++ % hent.addr_entries.size()].addr;
        }
        _addr_list = hent.addr_entries | std::views::transform(&net::hostent::address_entry::addr) | std::ranges::to<std::vector>();
        _addr_update_timer.rearm(lowres_clock::now() + _address_ttl);
    }

    co_return get_addr();
}

future<shared_ptr<tls::certificate_credentials>> utils::http::dns_connection_factory::get_creds() {
    if (!_creds_init) [[unlikely]] {
        auto units = co_await get_units(_init_semaphore, 1);
        if (!_creds_init) {
            co_await init_credentials();
            _creds_init = true;
        }
    }
    co_return _creds;
}

future<connected_socket> utils::http::dns_connection_factory::connect(net::inet_address address) {
    auto socket_addr = socket_address(address, _port);
    if (auto creds = co_await get_creds()) {
        _logger.debug("Making new HTTPS connection addr={} host={}", socket_addr, _host);
        co_return co_await tls::connect(creds, socket_addr, tls::tls_options{.server_name = _host});
    }
    _logger.debug("Making new HTTP connection addr={} host={}", socket_addr, _host);
    co_return co_await seastar::connect(socket_addr, {}, transport::TCP);
}

utils::http::dns_connection_factory::dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger, shared_ptr<tls::certificate_credentials> certs)
    : _host(std::move(host))
    , _port(port)
    , _logger(logger)
    ,_creds(std::move(certs))
    , _use_https(use_https)
    , _addr_update_timer([this] {
        if (auto units = try_get_units(_init_semaphore, 1)) {
            _addr_list.reset();
        }
    }) {
    _addr_update_timer.arm(lowres_clock::now());
}

utils::http::dns_connection_factory::dns_connection_factory(std::string uri, logging::logger& logger, shared_ptr<tls::certificate_credentials> certs) 
    : dns_connection_factory([&] {
        auto url = parse_simple_url(uri);
        if (!url.path.empty()) {
            throw std::invalid_argument("Cannot handle path in URI: " + uri);
        }
        return dns_connection_factory(url.host, url.port, url.is_https(), logger, std::move(certs));
    }())
{}

future<connected_socket> utils::http::dns_connection_factory::make(abort_source*) {
    try {
        auto address = co_await get_address();
        co_return co_await connect(address);
    } catch (...) {
        // On failure, forcefully renew address resolution and try again
        _logger.debug("Connection failed, resetting address provider and retrying: {:t}", std::current_exception());
    }
    _addr_list.reset();
    auto address = co_await get_address();
    co_return co_await connect(address);
}

future<> utils::http::dns_connection_factory::close() {
    _addr_update_timer.cancel();
    co_await get_units(_init_semaphore, 1);
}

static const char HTTPS[] = "https";

utils::http::url_info utils::http::parse_simple_url(std::string_view uri) {
    /**
     * https://en.wikipedia.org/wiki/IPv6#Addressing
     * In case a port is included with a numerical ipv6 address, 
     * the address part is encases in a "[]" wrapper, like
     * http://[2001:db8:4006:812::200e]:8080
     */
    static boost::regex simple_url(R"foo(([a-zA-Z]+):\/\/((?:\[[^\]]+\])|[^\/:]+)(:\d+)?(\/.*)?)foo");

    boost::smatch m;
    std::string tmp(uri);

    if (!boost::regex_match(tmp, m, simple_url)) {
        throw std::invalid_argument(fmt::format("Could not parse URI {}", uri));
    }

    auto scheme = m[1].str();
    auto host = m[2].str();
    auto port = m[3].str();
    auto path = m[4].str();

    bool https = (strcasecmp(scheme.c_str(), HTTPS) == 0);

    // check for numeric ipv6 address + port case
    if (host.size() > 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
    }
    return url_info {
        .scheme = std::move(scheme),
        .host = std::move(host),
        .path = std::move(path),
        .port = uint16_t(port.empty() ? (https ? 443 : 80) : std::stoi(port.substr(1)))
    };
}

bool utils::http::url_info::is_https() const {
    return strcasecmp(scheme.c_str(), HTTPS) == 0;
}

std::optional<utils::http::content_range> utils::http::parse_content_range(std::string_view value) {
    // 19 digits always fit in uint64_t, so stoull cannot throw.
    static const boost::regex pattern(R"(bytes (\d{1,19})-(\d{1,19})/(\d{1,19}))");
    boost::cmatch m;
    if (!boost::regex_match(value.begin(), value.end(), m, pattern)) {
        return std::nullopt;
    }
    const auto first = std::stoull(m[1].str());
    const auto last = std::stoull(m[2].str());
    const auto total = std::stoull(m[3].str());
    // RFC 9110 calls a range invalid when it names no bytes or runs past the end.
    if (first > last || last >= total) {
        return std::nullopt;
    }
    return content_range{first, last, total};
}
