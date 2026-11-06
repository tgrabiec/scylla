/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include <fmt/ranges.h>
#include <limits>

#include <seastar/core/sharded.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <algorithm>
#include <numeric>
#include <ranges>

#include "locator/tablets.hh"
#include "replica/tablet_mutation_builder.hh"
#include "replica/tablets.hh"
#include "locator/tablet_replication_strategy.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0_client.hh"
#include "raft/raft.hh"
#include "cdc/generation.hh"
#include "cql3/query_processor.hh"
#include "db/system_keyspace.hh"
#include "gms/feature_service.hh"
#include "mutation/async_utils.hh"

#include "test/perf/perf.hh"
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/topology_builder.hh"
#include "service/storage_service.hh"

using namespace locator;
using namespace replica;

static seastar::abort_source aborted;

static const size_t MiB = 1 << 20;

static
cql_test_config tablet_cql_test_config(unsigned schema_commitlog_segment_size_mb) {
    cql_test_config c;
    c.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::enabled);
    // The tablet allocator scales a table's initial tablet count down when the
    // average number of tablet replicas per shard would exceed this goal (see
    // load_balancer::make_sizing_plan). The test pins each table to an exact
    // tablet count via tablet options, so raise the goal out of the way to keep
    // the count from being scaled down at creation time.
    c.db_config->tablets_per_shard_goal.set(std::numeric_limits<unsigned>::max());
    // The group0 raft command size limit is derived from the schema commitlog
    // segment size (roughly a quarter of it). Allow it to be raised so we can
    // apply tablet metadata commands several times larger than the ~32 MiB
    // default cap as a single command.
    if (schema_commitlog_segment_size_mb) {
        c.db_config->schema_commitlog_segment_size_in_mb.set(uint32_t(schema_commitlog_segment_size_mb));
    }
    return c;
}

static
future<table_id> add_table(cql_test_env& e, int tablet_count) {
    auto id = table_id(utils::UUID_gen::get_time_UUID());
    co_await e.create_table([id, tablet_count] (std::string_view ks_name) {
        return *schema_builder(this_smp_shard_count(), ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type)
                // Create the table with exactly `tablet_count` tablets so that the
                // generated tablet metadata matches the table's tablet map and
                // applying it doesn't look like a tablet split/merge. pow2_count=false
                // lets us pin an arbitrary count without power-of-2 rounding.
                .set_tablet_options({
                    {"min_tablet_count", format("{}", tablet_count)},
                    {"max_tablet_count", format("{}", tablet_count)},
                    {"pow2_count", "false"},
                })
                .build();
    });
    co_return id;
}



// Measures how long the reactor goes without coming back to a task which yields
// after every step. That gap is the longest contiguous run of whatever else is
// on the queue, i.e. the stall the code under test inflicts on its scheduling
// group. Unlike wall-clock latency it does not change when a fix only makes the
// work cheaper - it changes when the work becomes preemptible.
class preemption_probe {
    using clk = std::chrono::steady_clock;
    // Gaps at or below this are counted but not kept: the probe sees tens of
    // millions of them in a busy run, and only the long ones matter.
    static constexpr double short_gap_ms = 0.05;
    bool _stop = true;
    uint64_t _count = 0;
    std::vector<double> _long_gaps_ms;
    future<> _done = make_ready_future<>();
private:
    future<> run() {
        auto prev = clk::now();
        while (!_stop) {
            co_await seastar::yield();
            auto now = clk::now();
            auto gap = std::chrono::duration<double, std::milli>(now - prev).count();
            ++_count;
            if (gap > short_gap_ms) {
                _long_gaps_ms.push_back(gap);
            }
            prev = now;
        }
    }
public:
    void start() {
        _stop = false;
        _count = 0;
        _long_gaps_ms.clear();
        _done = run();
    }
    // Call from a seastar thread.
    void stop() {
        _stop = true;
        std::exchange(_done, make_ready_future<>()).get();
        std::sort(_long_gaps_ms.begin(), _long_gaps_ms.end());
    }
    // The rest: call after stop(). A percentile among the short gaps reads as
    // short_gap_ms, an upper bound.
    double percentile(double p) const {
        if (_count == 0) {
            return 0;
        }
        auto i = uint64_t(p * (_count - 1) + 0.5);
        auto nr_short = _count - _long_gaps_ms.size();
        return i < nr_short ? short_gap_ms : _long_gaps_ms[i - nr_short];
    }
    double max_ms() const {
        return _long_gaps_ms.empty() ? (_count ? short_gap_ms : 0) : _long_gaps_ms.back();
    }
    uint64_t count() const {
        return _count;
    }
};

static double percentile(const std::vector<double>& sorted, double p) {
    if (sorted.empty()) {
        return 0;
    }
    auto i = size_t(p * (sorted.size() - 1) + 0.5);
    return sorted[i];
}

// load_topology_state() walks every row of system.topology, so its cost grows
// with the cluster. Reports two numbers which move independently: what a call
// costs, and the longest the shard is held while making it. Making the walk
// cheaper does not make it preemptible, and vice versa.
static future<> test_topology_reload(app_template& app) {
    return do_with_cql_env_thread([&] (cql_test_env& e) {
        using clk = std::chrono::steady_clock;

        const int nr_nodes = app.configuration()["nodes"].as<int>();
        const int nr_racks = app.configuration()["racks"].as<int>();
        if (nr_racks < 1) {
            throw std::runtime_error("--racks must be at least 1");
        }
        const unsigned tokens_per_node = app.configuration()["tokens-per-node"].as<unsigned>();
        const int nr_left_nodes = app.configuration()["left-nodes"].as<int>();
        const int nr_leaving = app.configuration()["leaving-nodes"].as<int>();
        const unsigned shards_per_node = app.configuration()["shards-per-node"].as<unsigned>();
        const int tablets_per_shard = app.configuration()["tablets-per-shard"].as<int>();
        const int nr_tablet_tables = app.configuration()["tablet-tables"].as<int>();
        const int rf = app.configuration()["rf"].as<int>();
        const bool leaving_drained = app.configuration().contains("leaving-drained");
        const bool via_group0 = app.configuration().contains("reload-via-group0");
        if (nr_leaving > nr_nodes) {
            throw std::runtime_error(format("--leaving-nodes={} exceeds --nodes={}", nr_leaving, nr_nodes));
        }
        const int iterations = app.configuration()["iterations"].as<int>();

        e.get_storage_service().local().set_tablet_balancing_enabled(false).get();

        // Every node advertises everything this build supports, so that
        // decode_features() has to build a realistically sized std::set<sstring>
        // per row.
        std::set<sstring> features;
        for (auto f : e.local_db().features().supported_feature_set()) {
            features.emplace(f);
        }
        // Feature count grows with every release, so make it an axis of its own.
        const int nr_features = app.configuration()["features"].as<int>();
        while (nr_features >= 0 && std::cmp_greater(features.size(), nr_features)) {
            features.erase(std::prev(features.end()));
        }

        topology_builder topo(e);
        std::vector<locator::endpoint_dc_rack> racks{topo.rack()};
        while (std::cmp_less(racks.size(), nr_racks)) {
            racks.push_back(topo.start_new_rack());
        }
        std::vector<locator::host_id> normal_hosts;
        for (int i = 0; i < nr_nodes; ++i) {
            normal_hosts.push_back(topo.add_node(service::node_state::normal, shards_per_node, racks[i % nr_racks], tokens_per_node, features));
        }
        testlog.info("{} normal nodes over {} racks in {}", nr_nodes, nr_racks, racks.front().dc);
        // A decommission whose leave request is paused while tablets drain off
        // the node: the node stays normal with the request queued. That is the
        // topology every reload sees during the drain's tablet migrations.
        for (int i = 0; i < nr_leaving; ++i) {
            topo.add_draining_request(normal_hosts[i]);
        }
        // A cluster keeps a row for every node it has ever lost. The coordinator
        // deletes their tokens on the way out but not their supported features,
        // so that is what a left row looks like here.
        for (int i = 0; i < nr_left_nodes; ++i) {
            topo.add_node(service::node_state::left, 1, std::nullopt, 0, features);
        }

        auto& sys_ks = e.get_system_keyspace().local();

        // Verify the rows really carry the tokens and features we intend to
        // measure, otherwise the benchmark quietly measures nothing.
        size_t nr_rows = 0;
        utils::UUID cdc_gen_id;
        int64_t cdc_ranges = 0;
        {
            auto state = sys_ks.load_topology_state({}).get();
            nr_rows = state.normal_nodes.size() + state.transition_nodes.size()
                    + state.new_nodes.size() + state.left_nodes.size();
            size_t matching = 0;
            size_t total_tokens = 0;
            for (auto& [id, rs] : state.normal_nodes) {
                total_tokens += rs.ring ? rs.ring->tokens.size() : 0;
                if (rs.ring && rs.ring->tokens.size() == tokens_per_node && rs.supported_features == features) {
                    ++matching;
                }
            }
            size_t leaving = 0;
            for (auto& [id, req] : state.requests) {
                if (req == service::topology_request::leave && state.normal_nodes.contains(id)) {
                    ++leaving;
                }
            }
            testlog.info("system.topology: {} rows, {} normal nodes ({} with a pending leave request, "
                         "{} with {} tokens and {} features), {} left nodes, {} tokens total",
                         nr_rows, state.normal_nodes.size(), leaving, matching, tokens_per_node, features.size(),
                         state.left_nodes.size(), total_tokens);
            if (std::cmp_not_equal(leaving, nr_leaving)) {
                throw std::runtime_error(format("expected {} normal nodes with a pending leave request, got {}",
                                                nr_leaving, leaving));
            }
            if (std::cmp_less(state.left_nodes.size(), nr_left_nodes)) {
                throw std::runtime_error(format("only {} of {} left nodes made it into the topology",
                                                state.left_nodes.size(), nr_left_nodes));
            }
            if (nr_left_nodes) {
                // A left node named in force_load_hosts must still come back with
                // its full replica_state; only the others may be skipped.
                auto forced = *state.left_nodes.begin();
                auto forced_state = sys_ks.load_topology_state({locator::host_id(forced.uuid())}).get();
                auto it = forced_state.left_nodes_rs.find(forced);
                if (it == forced_state.left_nodes_rs.end()) {
                    throw std::runtime_error(format("forced left node {} missing from left_nodes_rs", forced));
                }
                if (!forced_state.excluded_tablet_nodes.contains(forced)) {
                    throw std::runtime_error(format("forced left node {} missing from excluded_tablet_nodes", forced));
                }
                testlog.info("forced left node {} loaded with {} features, {} left_nodes_rs entries",
                             forced, it->second.supported_features.size(), forced_state.left_nodes_rs.size());
            }
            if (std::cmp_less(matching, nr_nodes)) {
                throw std::runtime_error(format("only {} of {} added nodes have the requested tokens/features",
                                                matching, nr_nodes));
            }
            if (state.committed_cdc_generations.empty()) {
                throw std::runtime_error("no committed CDC generation, the generation query would be skipped");
            }
            cdc_gen_id = state.committed_cdc_generations.back().id;
        }

        // The test env creates a generation for its own node only. Real ones
        // hold a row per vnode, so pad it out: load_topology_state() counts
        // those rows on every call.
        {
            int ranges = app.configuration()["cdc-ranges"].as<int>();
            if (ranges < 0) {
                ranges = int(nr_rows * tokens_per_node);
            }
            auto count_ranges = [&] {
                auto rs = e.local_qp().execute_internal(format(
                        "SELECT count(range_end) as cnt FROM system.{} WHERE key = '{}' AND id = {}",
                        db::system_keyspace::CDC_GENERATIONS_V3, cdc::CDC_GENERATIONS_V3_KEY, cdc_gen_id),
                        cql3::query_processor::cache_internal::yes).get();
                return rs->one().get_as<int64_t>("cnt");
            };
            const auto have = count_ranges();
            for (int64_t i = have; i < ranges; ++i) {
                // Distinct range_end values below the ones the env generated;
                // only the row count matters to the query being measured.
                e.local_qp().execute_internal(format(
                        "INSERT INTO system.{} (key, id, range_end, ignore_msb) VALUES ('{}', {}, {}, 0)",
                        db::system_keyspace::CDC_GENERATIONS_V3, cdc::CDC_GENERATIONS_V3_KEY,
                        cdc_gen_id, std::numeric_limits<int64_t>::min() + 1 + i),
                        cql3::query_processor::cache_internal::yes).get();
            }
            cdc_ranges = count_ranges();
            testlog.info("system.cdc_generations_v3: generation {} has {} ranges", cdc_gen_id, cdc_ranges);
        }

        // Production clusters run the distributed system keyspaces with
        // NetworkTopologyStrategy, which changes how a reload recomputes their
        // replication maps.
        if (!app.configuration().contains("keep-simple-system-keyspaces")) {
            for (auto ks : {"system_distributed", "system_traces", "system_auth"}) {
                if (!e.local_db().has_keyspace(ks)) {
                    continue;
                }
                e.execute_cql(format("ALTER KEYSPACE {} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 3}}", ks)).get();
                auto type = e.local_db().find_keyspace(ks).get_replication_strategy().get_type();
                if (type != locator::replication_strategy_type::network_topology) {
                    throw std::runtime_error(format("{} is not NetworkTopologyStrategy after ALTER", ks));
                }
                testlog.info("system keyspace {}: NetworkTopologyStrategy dc1=3", ks);
            }
        }

        // Tablet metadata sized by replicas per shard. Replicas go round-robin over
        // the nodes' shards; with --leaving-drained the leaving nodes hold none, as
        // at the end of the drain, which is the worst case for has_replica_on().
        struct tablet_ref {
            table_id table;
            dht::token last_token;
            locator::tablet_replica_set replicas;
        };
        std::vector<tablet_ref> all_tablets;
        size_t nr_tablets = 0;
        if (tablets_per_shard > 0) {
            std::vector<locator::host_id> holders;
            for (int i = 0; i < nr_nodes; ++i) {
                if (!(leaving_drained && i < nr_leaving)) {
                    holders.push_back(normal_hosts[i]);
                }
            }
            if (std::cmp_less(holders.size(), rf)) {
                throw std::runtime_error(format("{} tablet-holding nodes is fewer than rf={}", holders.size(), rf));
            }
            const size_t total_tablets = holders.size() * shards_per_node * tablets_per_shard / rf;
            const int tablets_per_table = (total_tablets + nr_tablet_tables - 1) / nr_tablet_tables;
            locator::tablet_metadata tm;
            size_t slot = 0;
            for (int t = 0; t < nr_tablet_tables; ++t) {
                auto id = add_table(e, tablets_per_table).get();
                locator::tablet_map tmap(tablets_per_table);
                for (auto tid : tmap.tablet_ids()) {
                    thread::maybe_yield();
                    locator::tablet_replica_set replicas;
                    for (int k = 0; k < rf; ++k, ++slot) {
                        replicas.push_back({holders[slot % holders.size()], shard_id((slot / holders.size()) % shards_per_node)});
                    }
                    all_tablets.push_back({id, tmap.get_last_token(tid), replicas});
                    tmap.set_tablet(tid, locator::tablet_info{std::move(replicas)});
                    ++nr_tablets;
                }
                tm.set_tablet_map(id, std::move(tmap));
            }
            // Load it the way the write path does: save, then commit the tablet
            // mutations through group0 so the in-memory token metadata holds them.
            save_tablet_metadata(e.local_db(), tm, api::new_timestamp()).get();
            utils::chunked_vector<canonical_mutation> muts;
            replica::read_tablet_mutations(e.db(), [&] (canonical_mutation m) {
                muts.emplace_back(std::move(m));
            }).get();
            const auto tablets_schema = db::system_keyspace::tablets();
            service::group0_update_collector collector(
                    app.configuration()["command-max-mutation-size-mb"].as<size_t>() * MiB);
            for (auto& cm : muts) {
                collector.add(to_mutation_gently(cm, tablets_schema).get()).get();
            }
            auto collected = collector.collect().get();
            topo.modify_group0([&] (service::group0_guard&, utils::chunked_vector<canonical_mutation>& out) {
                out = std::move(collected);
            });

            auto tmptr = e.local_token_metadata_ptr();
            size_t loaded = 0;
            for (auto& [id, map] : tmptr->tablets().all_tables_ungrouped()) {
                loaded += map->tablet_count();
            }
            testlog.info("tablets: {} tables, {} tablets loaded ({} generated), {} holder nodes x {} shards, rf={}",
                         nr_tablet_tables, loaded, nr_tablets, holders.size(), shards_per_node, rf);
            if (loaded < nr_tablets) {
                throw std::runtime_error(format("only {} of {} generated tablets are in token metadata", loaded, nr_tablets));
            }
            for (int i = 0; i < nr_leaving; ++i) {
                auto host = normal_hosts[i];
                bool has = tmptr->tablets().has_replica_on(host);
                if (has == leaving_drained) {
                    throw std::runtime_error(format("has_replica_on(leaving {}) = {}, expected {}", host, has, !leaving_drained));
                }
                constexpr int n = 100;
                auto start = clk::now();
                for (int j = 0; j < n; ++j) {
                    has = tmptr->tablets().has_replica_on(host);
                }
                testlog.info("has_replica_on(leaving {}) = {}: {:.3f} ms per call", host, has,
                             std::chrono::duration<double, std::milli>(clk::now() - start).count() / n);
            }
        }
        if (via_group0 && all_tablets.empty()) {
            throw std::runtime_error("--reload-via-group0 needs --tablets-per-shard");
        }

        // One measured step: either load_topology_state() alone, or a group0 command
        // rewriting one tablet's replicas as they are, which is what a tablet
        // migration stage commits; its apply runs the full topology reload.
        size_t next_tablet = 0;
        auto step = [&] {
            if (!via_group0) {
                sys_ks.load_topology_state({}).get();
                return;
            }
            auto& t = all_tablets[next_tablet++ % all_tablets.size()];
            topo.modify_group0([&] (service::group0_guard& guard, utils::chunked_vector<canonical_mutation>& out) {
                out.emplace_back(replica::tablet_mutation_builder(guard.write_timestamp(), t.table)
                        .set_replicas(t.last_token, t.replicas)
                        .build());
            });
        };

        for (int i = 0; i < 3; ++i) {
            step();
        }

        preemption_probe probe;

        // Noise floor: what the probe sees with only the test env's own
        // background fibers running. Anything the measured run reports above
        // this is caused by the code under test.
        probe.start();
        seastar::sleep(std::chrono::seconds(2)).get();
        probe.stop();
        const auto idle_max = probe.max_ms();
        const auto idle_p999 = probe.percentile(0.999);

        // Latency without the probe, which would otherwise compete for the CPU.
        std::vector<double> lat;
        lat.reserve(iterations);
        const auto run_start = clk::now();
        for (int i = 0; i < iterations; ++i) {
            auto start = clk::now();
            step();
            lat.push_back(std::chrono::duration<double, std::milli>(clk::now() - start).count());
        }
        const auto run_ms = std::chrono::duration<double, std::milli>(clk::now() - run_start).count();

        // Stalls, in a second pass with the probe running.
        probe.start();
        for (int i = 0; i < iterations; ++i) {
            step();
        }
        probe.stop();

        const auto mean = std::accumulate(lat.begin(), lat.end(), 0.0) / lat.size();
        std::sort(lat.begin(), lat.end());

        testlog.info("load_topology_state nodes={} leaving={} left={} rows={} tokens_per_node={} features={} cdc_ranges={} iterations={} wall={:.1f}ms",
                     nr_nodes, nr_leaving, nr_left_nodes, nr_rows, tokens_per_node, features.size(), cdc_ranges, iterations, run_ms);
        testlog.info("RESULT mode={} racks={} shards={} tablets={} drained={} nodes={} leaving={} left={} rows={} tokens={} cdc_ranges={} mean={:.3f} p50={:.3f} p95={:.3f} max={:.3f} "
                     "stall_p99={:.3f} stall_max={:.3f} idle_stall_p999={:.3f} idle_stall_max={:.3f} "
                     "gaps={} [ms]",
                     via_group0 ? "group0" : "load", nr_racks, shards_per_node, nr_tablets, leaving_drained,
                     nr_nodes, nr_leaving, nr_left_nodes, nr_rows, tokens_per_node, cdc_ranges, mean, percentile(lat, 0.5), percentile(lat, 0.95),
                     lat.back(), probe.percentile(0.99), probe.max_ms(), idle_p999, idle_max, probe.count());
    }, tablet_cql_test_config(app.configuration()["schema-commitlog-segment-size-mb"].as<unsigned>()));
}

namespace perf {

int scylla_topology_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
            ("nodes", bpo::value<int>()->default_value(3), "Number of normal nodes to add to the topology.")
            ("racks", bpo::value<int>()->default_value(3), "Number of racks the normal nodes are spread over, round-robin.")
            ("tokens-per-node", bpo::value<unsigned>()->default_value(256), "Number of tokens per node.")
            ("features", bpo::value<int>()->default_value(-1),
                    "Cap the number of supported features per node (-1 = everything this build supports).")
            ("left-nodes", bpo::value<int>()->default_value(0),
                    "Number of extra system.topology rows in the 'left' state (no tokens, full feature "
                    "set), as a cluster accumulates one per node it ever lost.")
            ("leaving-nodes", bpo::value<int>()->default_value(0),
                    "Number of the --nodes nodes with a pending leave request, as during a decommission "
                    "whose request is paused while tablets drain off the node.")
            ("cdc-ranges", bpo::value<int>()->default_value(0),
                    "Pad the committed CDC generation to this many ranges (0 = leave it as the test env "
                    "created it, -1 = rows * tokens-per-node, which is what a real cluster has).")
            ("shards-per-node", bpo::value<unsigned>()->default_value(1), "Shard count of the added nodes.")
            ("tablets-per-shard", bpo::value<int>()->default_value(0),
                    "Create tablet metadata with this many tablet replicas per shard of the normal nodes "
                    "(0 = no tablets).")
            ("tablet-tables", bpo::value<int>()->default_value(16),
                    "Number of tables the --tablets-per-shard tablets are spread over.")
            ("rf", bpo::value<int>()->default_value(3), "Number of replicas per tablet.")
            ("leaving-drained", "Give the --leaving-nodes no tablet replicas, as at the end of the drain.")
            ("keep-simple-system-keyspaces", "Leave system_distributed, system_traces and system_auth on "
                    "SimpleStrategy instead of altering them to NetworkTopologyStrategy.")
            ("reload-via-group0", "Measure a group0 command rewriting one tablet, whose apply runs the full "
                    "topology reload, instead of load_topology_state() alone. Needs --tablets-per-shard.")
            ("iterations", bpo::value<int>()->default_value(200), "Number of measured calls.")
            ("command-max-mutation-size-mb", bpo::value<size_t>()->default_value(service::group0_update_collector::default_max_mutation_size / MiB),
                    "Max in-memory size of a single mutation in the group0 command loading the tablet metadata.")
            ("schema-commitlog-segment-size-mb", bpo::value<unsigned>()->default_value(0),
                    "Override schema_commitlog_segment_size_in_mb (0 = leave the default).")
            ("verbose", "Enables standard logging")
            ;
    return app.run(argc, argv, [&] {
        return seastar::async([&] {
            if (!app.configuration().contains("verbose")) {
                auto testlog_level = logging::logger_registry().get_logger_level("testlog");
                logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
                logging::logger_registry().set_logger_level("testlog", testlog_level);
            }
            auto stop_test = defer([] noexcept {
                aborted.request_abort();
            });
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            try {
                test_topology_reload(app).get();
            } catch (seastar::abort_requested_exception&) {
                // Ignore
            } catch (...) {
                on_fatal_internal_error(testlog, format("Aborting on unhandled exception: {}", std::current_exception()));
            }
        });
    });
}

} // namespace perf
