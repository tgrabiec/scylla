/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// pyunit runner: runs pytest in-process against an embedded cql_test_env.
//
// Architecture: seastar runs on background threads (app_template in a
// std::thread), the main thread runs the Python interpreter. Bindings cross
// into the reactor via seastar::alien::submit_to() and block the Python
// thread (GIL released) until the seastar future resolves. Scylla objects
// live reactor-side; Python holds opaque handles.
//
// Usage: build/dev/test/pyunit/runner <pytest args>
//   e.g. build/dev/test/pyunit/runner test/pyunit -v

// pybind11 embeds Python.h, which must come before anything else.
#include <pybind11/embed.h>
#include <pybind11/stl.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/alien.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/future.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "test/lib/topology_builder.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "locator/tablets.hh"
#include "locator/load_sketch.hh"
#include "locator/token_metadata.hh"
#include "service/tablet_allocator.hh"
#include "utils/UUID_gen.hh"

#include <atomic>
#include <future>
#include <thread>

namespace py = pybind11;
using namespace seastar;
using namespace locator;
using namespace service;

// Test helpers linked in from test/boost/tablets_test.cc.
void mutate_tablets(cql_test_env& e, seastar::noncopyable_function<future<>(tablet_metadata&)> mutator);
void rebalance_tablets(cql_test_env& e,
                       shared_load_stats* load_stats,
                       std::unordered_set<host_id> skiplist,
                       std::function<bool(const migration_plan&)> stop,
                       bool auto_split,
                       bool use_resize_hint);

namespace {

struct bridge {
    alien::instance* alien_inst = nullptr;
    std::optional<seastar::promise<>> app_stop;

    // Reactor-side state of the current test's env.
    cql_test_env* env = nullptr;
    std::optional<seastar::promise<>> env_stop;
    seastar::future<> env_done = seastar::make_ready_future<>();
    std::vector<std::unique_ptr<topology_builder>> topos;
};

bridge B;

// Runs func in a seastar thread on shard 0, blocking the Python thread
// (GIL released) until it completes. Returns func's return value;
// rethrows its exception.
template <typename Func>
auto on_reactor(Func func) {
    py::gil_scoped_release gil;
    return alien::submit_to(*B.alien_inst, 0, [func = std::move(func)] () mutable {
        return seastar::async(std::move(func));
    }).get();
}

cql_test_env& env() {
    if (!B.env) {
        throw std::runtime_error("no cql_test_env is running; use the env fixture");
    }
    return *B.env;
}

topology_builder& topo(size_t idx) {
    return *B.topos.at(idx);
}

cql_test_config make_tablet_config() {
    cql_test_config c;
    c.db_config->tablets_mode_for_new_keyspaces(db::tablets_mode_t::mode::enabled);
    if (c.db_config->enable_tablets_by_default()) {
        c.initial_tablets = 2;
    }
    return c;
}

void start_env() {
    py::gil_scoped_release gil;
    alien::submit_to(*B.alien_inst, 0, [] {
        auto ready = make_lw_shared<seastar::promise<>>();
        auto constructed = make_lw_shared<bool>(false);
        B.env_stop.emplace();
        B.env_done = do_with_cql_env_thread([ready, constructed] (cql_test_env& e) {
            *constructed = true;
            B.env = &e;
            ready->set_value();
            B.env_stop->get_future().get();
            B.topos.clear();
            B.env = nullptr;
        }, make_tablet_config()).handle_exception([ready, constructed] (std::exception_ptr ep) {
            if (!*constructed) {
                ready->set_exception(ep);
            } else {
                testlog.error("cql_test_env teardown failed: {}", ep);
            }
        });
        return ready->get_future();
    }).get();
}

void stop_env() {
    py::gil_scoped_release gil;
    alien::submit_to(*B.alien_inst, 0, [] {
        if (B.env_stop) {
            B.env_stop->set_value();
            B.env_stop.reset();
        }
        return std::exchange(B.env_done, seastar::make_ready_future<>());
    }).get();
}

// Same CQL as do_add_keyspace() in tablets_test.cc, int-RF form.
std::string add_keyspace(const std::unordered_map<std::string, int>& dc_rf, int initial_tablets) {
    static std::atomic<int> ks_id = 0;
    auto ks_name = fmt::format("keyspace{}", ks_id.fetch_add(1));
    return on_reactor([ks_name, dc_rf, initial_tablets] {
        sstring rf_options;
        for (auto& [dc, rf] : dc_rf) {
            rf_options += fmt::format(", '{}': {}", dc, rf);
        }
        env().execute_cql(fmt::format(
            "create keyspace {} with replication = {{'class': 'NetworkTopologyStrategy'{}}}"
            " and tablets = {{'enabled': true, 'initial': {}}}",
            ks_name, rf_options, initial_tablets)).get();
        return ks_name;
    });
}

// Same as add_table() in tablets_test.cc.
table_id add_table(const std::string& ks_name) {
    return on_reactor([ks_name] {
        auto id = table_id(utils::UUID_gen::get_time_UUID());
        env().create_table([&] (std::string_view default_ks) {
            std::string_view ks = ks_name.empty() ? default_ks : std::string_view(ks_name);
            auto builder = schema_builder(this_smp_shard_count(), ks, id.to_sstring(), id)
                    .with_column("p1", utf8_type, column_kind::partition_key)
                    .with_column("r1", int32_type);
            return *builder.build();
        }).get();
        return id;
    });
}

// Python-side staging of one table's tablet map: pure data, materialized
// into a locator::tablet_map on the reactor when the update is applied.
struct py_tablet_map {
    size_t tablet_count;
    std::map<size_t, std::vector<std::pair<host_id, unsigned>>> replicas;
};

struct py_tablet_update {
    std::vector<std::pair<table_id, py_tablet_map>> staged;

    void apply() {
        on_reactor([this] {
            mutate_tablets(env(), [this] (tablet_metadata& tmeta) -> future<> {
                for (auto& [table, data] : staged) {
                    tablet_map tmap(data.tablet_count);
                    auto tid = tmap.first_tablet();
                    for (size_t i = 0; i < data.tablet_count; ++i) {
                        if (auto it = data.replicas.find(i); it != data.replicas.end()) {
                            tablet_replica_set rs;
                            for (auto& [h, shard] : it->second) {
                                rs.push_back(tablet_replica {h, shard});
                            }
                            tmap.set_tablet(tid, tablet_info {std::move(rs)});
                        }
                        if (i + 1 < data.tablet_count) {
                            tid = *tmap.next_tablet(tid);
                        }
                    }
                    tmeta.set_tablet_map(table, std::move(tmap));
                }
                return make_ready_future<>();
            });
        });
    }
};

// Eager snapshot of locator::load_sketch for all nodes; safe to read from
// the Python thread after the reactor round-trip.
struct py_load_sketch {
    std::unordered_map<host_id, std::pair<double, uint64_t>> loads; // host -> (tablets, avg per shard)

    double tablet_count(host_id h) const { return loads.at(h).first; }
    uint64_t avg_tablet_count(host_id h) const { return loads.at(h).second; }
};

py_load_sketch take_load_sketch() {
    py_load_sketch out;
    out.loads = on_reactor([] {
        auto tm = env().shared_token_metadata().local().get();
        load_sketch load(tm);
        load.populate().get();
        std::unordered_map<host_id, std::pair<double, uint64_t>> loads;
        tm->get_topology().for_each_node([&] (const locator::node& n) {
            auto h = n.host_id();
            loads[h] = {load.get_load(h), load.get_avg_tablet_count(h)};
        });
        return loads;
    });
    return out;
}

struct py_load_stats {
    size_t topo_idx;

    void set_default_tablet_sizes() {
        on_reactor([idx = topo_idx] {
            topo(idx).get_shared_load_stats().set_default_tablet_sizes(
                env().shared_token_metadata().local().get());
        });
    }
};

struct py_topology_builder {
    size_t idx;

    host_id add_node(unsigned shards) {
        return on_reactor([idx = idx, shards] {
            return topo(idx).add_node(node_state::normal, shards);
        });
    }

    std::string dc() {
        return on_reactor([idx = idx] {
            return std::string(topo(idx).dc());
        });
    }
};

struct py_env {
    void execute(const std::string& cql) {
        on_reactor([cql] {
            env().execute_cql(cql).get();
        });
    }

    py_topology_builder topology_builder() {
        auto idx = on_reactor([] {
            B.topos.push_back(std::make_unique<::topology_builder>(env()));
            return B.topos.size() - 1;
        });
        return py_topology_builder {idx};
    }

    void rebalance_tablets(std::optional<py_load_stats> stats) {
        on_reactor([stats] {
            shared_load_stats* ls = stats ? &topo(stats->topo_idx).get_shared_load_stats() : nullptr;
            ::rebalance_tablets(env(), ls, {}, nullptr, true, false);
        });
    }
};

} // anonymous namespace

PYBIND11_EMBEDDED_MODULE(scylla_test, m) {
    m.doc() = "In-process Scylla test environment (cql_test_env bridged over seastar::alien)";

    py::class_<host_id>(m, "HostId")
        .def("__repr__", [] (const host_id& h) { return fmt::format("HostId({})", h); })
        .def("__str__", [] (const host_id& h) { return fmt::format("{}", h); })
        .def("__eq__", [] (const host_id& a, const host_id& b) { return a == b; })
        .def("__hash__", [] (const host_id& h) { return std::hash<host_id>()(h); });

    py::class_<table_id>(m, "TableId")
        .def("__repr__", [] (const table_id& t) { return fmt::format("TableId({})", t); })
        .def("__str__", [] (const table_id& t) { return fmt::format("{}", t); })
        .def("__eq__", [] (const table_id& a, const table_id& b) { return a == b; })
        .def("__hash__", [] (const table_id& t) { return std::hash<table_id>()(t); });

    py::class_<py_tablet_map>(m, "TabletMap")
        .def(py::init([] (size_t tablet_count) {
            return py_tablet_map {tablet_count, {}};
        }), py::arg("tablet_count"))
        .def("__setitem__", [] (py_tablet_map& self, size_t idx,
                                std::vector<std::pair<host_id, unsigned>> replicas) {
            if (idx >= self.tablet_count) {
                throw std::out_of_range("tablet index out of range");
            }
            self.replicas[idx] = std::move(replicas);
        })
        .def("__len__", [] (const py_tablet_map& self) { return self.tablet_count; });

    py::class_<py_tablet_update>(m, "TabletMetadataUpdate")
        .def("__setitem__", [] (py_tablet_update& self, table_id table, const py_tablet_map& tmap) {
            self.staged.emplace_back(table, tmap);
        })
        .def("__enter__", [] (py::object self) { return self; })
        .def("__exit__", [] (py_tablet_update& self, py::object exc_type, py::object, py::object) {
            if (exc_type.is_none()) {
                self.apply();
            }
            return false;
        });

    py::class_<py_load_sketch>(m, "LoadSketch")
        .def("tablet_count", &py_load_sketch::tablet_count)
        .def("avg_tablet_count", &py_load_sketch::avg_tablet_count);

    py::class_<py_load_stats>(m, "LoadStats")
        .def("set_default_tablet_sizes", &py_load_stats::set_default_tablet_sizes);

    py::class_<py_topology_builder>(m, "TopologyBuilder")
        .def("add_node", &py_topology_builder::add_node, py::arg("shards") = 1)
        .def_property_readonly("dc", &py_topology_builder::dc)
        .def_property_readonly("load_stats", [] (const py_topology_builder& t) {
            return py_load_stats {t.idx};
        });

    py::class_<py_env>(m, "Env")
        .def("execute", &py_env::execute)
        .def("topology_builder", &py_env::topology_builder)
        .def("add_keyspace", [] (py_env&, std::unordered_map<std::string, int> rf, int initial_tablets) {
            return add_keyspace(rf, initial_tablets);
        }, py::arg("rf"), py::arg("initial_tablets") = 0)
        .def("add_table", [] (py_env&, const std::string& ks) {
            return add_table(ks);
        }, py::arg("keyspace"))
        .def("update_tablet_metadata", [] (py_env&) { return py_tablet_update {}; })
        .def("load_sketch", [] (py_env&) { return take_load_sketch(); })
        .def("rebalance_tablets", &py_env::rebalance_tablets, py::arg("load_stats") = std::nullopt);

    m.def("start_env", [] {
        start_env();
        return py_env {};
    });
    m.def("stop_env", &stop_env);
}

int main(int argc, char** argv) {
    std::vector<std::string> pytest_args(argv + 1, argv + argc);
    if (pytest_args.empty()) {
        fmt::print(stderr, "usage: {} <pytest args>\n", argv[0]);
        return 2;
    }

    const char* log_level = std::getenv("PYUNIT_LOG_LEVEL");
    std::vector<std::string> sargs = {
        "pyunit-runner",
        "-c2", "-m2G",
        "--overprovisioned",
        "--unsafe-bypass-fsync", "1",
        "--kernel-page-cache", "1",
        "--blocked-reactor-notify-ms", "2000000",
        "--max-networking-io-control-blocks", "1000",
        "--reactor-backend", "linux-aio",
        "--default-log-level", log_level ? log_level : "warn",
    };

    std::promise<bool> started;
    int app_rc = 0;
    std::thread reactor_thread([&] {
        std::vector<char*> av;
        for (auto& s : sargs) {
            av.push_back(s.data());
        }
        app_template app;
        app_rc = app.run(av.size(), av.data(), [&started] {
            B.alien_inst = &engine().alien();
            B.app_stop.emplace();
            started.set_value(true);
            return B.app_stop->get_future().then([] { return 0; });
        });
        if (!B.alien_inst) {
            started.set_value(false);
        }
    });

    if (!started.get_future().get()) {
        reactor_thread.join();
        fmt::print(stderr, "failed to start seastar (exit code {})\n", app_rc);
        return 1;
    }

    int rc;
    {
        py::scoped_interpreter interp;
        try {
            auto pytest = py::module_::import("pytest");
            py::list args;
            for (auto& a : pytest_args) {
                args.append(a);
            }
            rc = pytest.attr("main")(args).cast<int>();
        } catch (py::error_already_set& e) {
            fmt::print(stderr, "python error: {}\n", e.what());
            rc = 1;
        }
    }

    alien::submit_to(*B.alien_inst, 0, [] {
        if (B.env_stop) {
            // A test aborted without stopping its env; release it so teardown can run.
            B.env_stop->set_value();
            B.env_stop.reset();
        }
        return std::exchange(B.env_done, seastar::make_ready_future<>()).finally([] {
            B.app_stop->set_value();
        });
    }).get();
    reactor_thread.join();
    return rc;
}
