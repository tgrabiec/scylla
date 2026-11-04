/*
 * Copyright (C) 2023-present ScyllaDB
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

seastar::abort_source aborted;

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

static future<> test_basic_operations(app_template& app) {
    return do_with_cql_env_thread([&] (cql_test_env& e) {
        tablet_metadata tm;

        // Drive tablet metadata explicitly: the test pins each table's tablet
        // count via tablet options and spreads replicas across the fabricated
        // nodes built below. Disable the tablet load balancer so it doesn't
        // migrate or rebalance the tablets underneath the test once the metadata
        // is applied via group0.
        e.get_storage_service().local().set_tablet_balancing_enabled(false).get();

        int nr_tables = app.configuration()["tables"].as<int>();
        int tablets_per_table = app.configuration()["tablets-per-table"].as<int>();
        int rf = app.configuration()["rf"].as<int>();
        int nr_nodes = app.configuration()["nodes"].as<int>();
        if (nr_nodes == 0) {
            nr_nodes = rf;
        }
        if (nr_nodes < rf) {
            throw std::runtime_error(format("nodes ({}) must be >= rf ({})", nr_nodes, rf));
        }

        const auto shard_count = this_smp_shard_count();
        topology_builder topo(e);
        std::vector<host_id> nodes;
        nodes.reserve(nr_nodes);
        for (int i = 0; i < nr_nodes; ++i) {
            nodes.push_back(topo.add_node(service::node_state::normal, shard_count));
        }
        testlog.info("Added {} nodes to the topology", nodes.size());

        size_t total_tablets = 0;

        std::vector<table_id> ids;
        ids.resize(nr_tables);
        for (int i = 0; i < nr_tables; ++i) {
            ids[i] = add_table(e, tablets_per_table).get();
        }

        testlog.info("Generating tablet metadata");

        for (int i = 0; i < nr_tables; ++i) {
            tablet_map tmap(tablets_per_table);

            for (tablet_id j : tmap.tablet_ids()) {
                aborted.check();
                thread::maybe_yield();
                tablet_replica_set replicas;
                for (int k = 0; k < rf; ++k) {
                    const auto host = nodes[(total_tablets + k) % nodes.size()];
                    replicas.push_back({host, 0});
                }
                SCYLLA_ASSERT(std::cmp_equal(replicas.size(), rf));
                tmap.set_tablet(j, tablet_info{std::move(replicas)});
                ++total_tablets;
            }

            tm.set_tablet_map(ids[i], std::move(tmap));
        }

        testlog.info("Total tablet count: {}", total_tablets);

        testlog.info("Size of tablet_metadata in memory: {} KiB",
                     (tm.external_memory_usage() + sizeof(tablet_metadata)) / 1024);

        tablet_metadata tm2;
        auto time_to_copy = duration_in_seconds([&] {
            tm2 = tm.copy().get();
        });

        testlog.info("Copied in {:.6f} [ms]", time_to_copy.count() * 1000);

        auto time_to_clear = duration_in_seconds([&] {
            tm2.clear_gently().get();
        });

        testlog.info("Cleared in {:.6f} [ms]", time_to_clear.count() * 1000);

        auto time_to_save = duration_in_seconds([&] {
            save_tablet_metadata(e.local_db(), tm, api::new_timestamp()).get();
        });

        testlog.info("Saved in {:.6f} [ms]", time_to_save.count() * 1000);

        auto time_to_read = duration_in_seconds([&] {
            tm2 = read_tablet_metadata(e.local_qp()).get();
        });
        SCYLLA_ASSERT(tm == tm2);

        testlog.info("Read in {:.6f} [ms]", time_to_read.count() * 1000);

        utils::chunked_vector<canonical_mutation> muts;
        auto time_to_read_muts = duration_in_seconds([&] {
            replica::read_tablet_mutations(e.db(), [&] (canonical_mutation m) {
                muts.emplace_back(m);
            }).get();
        });

        testlog.info("Read mutations in {:.6f} [ms] {} mutations", time_to_read_muts.count() * 1000, muts.size());

        auto time_to_read_hosts = duration_in_seconds([&] {
            replica::read_required_hosts(e.local_qp()).get();
        });

        testlog.info("Read required hosts in {:.6f} [ms]", time_to_read_hosts.count() * 1000);

        auto cm_size = 0;
        for (auto&& cm : muts) {
            cm_size += cm.representation().size();
        }

        testlog.info("Size of canonical mutations: {:.6f} [MiB]", double(cm_size) / MiB);

        // Measure the group0 command size and the time to apply it when tablet
        // mutations are routed through group0_update_collector, the way the write
        // path builds and commits the command.
        {
            const size_t max_mutation_size = app.configuration()["command-max-mutation-size-mb"].as<size_t>() * MiB;
            const auto tablets_schema = db::system_keyspace::tablets();

            utils::chunked_vector<mutation> unfrozen_muts;
            unfrozen_muts.reserve(muts.size());
            for (const auto& cm : muts) {
                unfrozen_muts.push_back(to_mutation_gently(cm, tablets_schema).get());
            }

            service::group0_update_collector collector(max_mutation_size);
            utils::chunked_vector<canonical_mutation> collected;
            auto time_to_collect = duration_in_seconds([&] {
                for (auto& m : unfrozen_muts) {
                    collector.add(std::move(m)).get();
                }
                collected = collector.collect().get();
            });

            size_t collected_size = 0;
            size_t max_piece_size = 0;
            for (const auto& cm : collected) {
                collected_size += cm.representation().size();
                max_piece_size = std::max<size_t>(max_piece_size, cm.representation().size());
            }

            testlog.info("Size of group0 command via group0_update_collector (max_mutation_size={} MiB): "
                         "{:.6f} [MiB] ({} serialized mutations, max {:.6f} MiB), collected in {:.6f} [ms]",
                         max_mutation_size / MiB, double(collected_size) / MiB, collected.size(),
                         double(max_piece_size) / MiB, time_to_collect.count() * 1000);

            auto& group0_client = e.get_raft_group0_client();
            seastar::abort_source as;
            try {
                auto time_to_apply = duration_in_seconds([&] {
                    auto guard = group0_client.start_operation(as).get();
                    auto cmd = group0_client.prepare_command(
                            service::topology_change{.mutations{std::move(collected)}}, guard, "Apply whole tablet metadata");
                    group0_client.add_entry(std::move(cmd), std::move(guard), as).get();
                });
                testlog.info("Applied whole tablet metadata to group0 in {:.6f} [ms]", time_to_apply.count() * 1000);
            } catch (const raft::command_is_too_big_error& ex) {
                testlog.error("Could not apply tablet metadata to group0 as a single command: {}", ex);
            }
        }

        auto&& tablets_table = e.local_db().find_column_family(db::system_keyspace::tablets());
        testlog.info("Disk space used by system.tablets: {:.6f} [MiB]", double(tablets_table.get_stats().sstables_live_disk_space_used.on_disk) / MiB);

        locator::tablet_metadata_change_hint hint;

        // Migrate one tablet to a different shard on its first replica's node.
        // This is a valid intra-node migration (the destination host still
        // exists in the topology) and exercises the tablet_metadata_change_hint
        // / partial reload path below.
        {
            const auto last_table_id = ids.back();
            const auto& tmap = tm.get_tablet_map(last_table_id);

            auto ts = utils::UUID_gen::micros_timestamp(e.get_system_keyspace().local().get_last_group0_state_id().get()) + 1;

            const auto tb = tmap.first_tablet();
            replica::tablet_mutation_builder builder(ts++, last_table_id);
            const auto token = tmap.get_last_token(tb);

            // Move the first replica to a different shard on the same node,
            // leaving the other replicas in place.
            auto new_replicas = tmap.get_tablet_info(tb).replicas;
            const auto dst_shard = shard_id(shard_count > 1 ? (new_replicas.front().shard ^ 1) : new_replicas.front().shard);
            new_replicas.front() = tablet_replica {new_replicas.front().host, dst_shard};
            builder.set_new_replicas(token, std::move(new_replicas));
            builder.set_stage(token, tablet_transition_stage::streaming);
            builder.set_transition(token, tablet_transition_kind::migration);

            utils::chunked_vector<mutation> muts;
            muts.push_back(builder.build());
            e.local_db().apply(freeze(muts), db::no_timeout).get();
            replica::update_tablet_metadata_change_hint(hint, muts.front());
        }

        using clk = std::chrono::high_resolution_clock;

        const auto start_full_reload = clk::now();
        const auto tm_full_reload = read_tablet_metadata(e.local_qp()).get();
        const auto end_full_reload = clk::now();
        const auto full_reload_duration = std::chrono::duration<double, std::milli>(end_full_reload - start_full_reload);

        const auto start_partial_reload = clk::now();
        update_tablet_metadata(e.local_db(), e.local_qp(), tm, hint).get();
        const auto end_partial_reload = clk::now();
        const auto partial_reload_duration = std::chrono::duration<double, std::milli>(end_partial_reload - start_partial_reload);

        SCYLLA_ASSERT(tm == tm_full_reload);

        testlog.info("Tablet metadata reload:\nfull    {:>8.2f}ms\npartial {:>8.2f}ms", full_reload_duration.count(), partial_reload_duration.count());
    }, tablet_cql_test_config(app.configuration()["schema-commitlog-segment-size-mb"].as<unsigned>()));
}


// Measures how long the reactor goes without coming back to a task which yields
// after every step. That gap is the longest contiguous run of whatever else is
// on the queue, i.e. the stall the code under test inflicts on its scheduling
// group. Unlike wall-clock latency it does not change when a fix only makes the
// work cheaper - it changes when the work becomes preemptible.
class preemption_probe {
    using clk = std::chrono::steady_clock;
    bool _stop = true;
    std::vector<double> _gaps_ms;
    future<> _done = make_ready_future<>();
private:
    future<> run() {
        auto prev = clk::now();
        while (!_stop) {
            co_await seastar::yield();
            auto now = clk::now();
            _gaps_ms.push_back(std::chrono::duration<double, std::milli>(now - prev).count());
            prev = now;
        }
    }
public:
    void start() {
        _stop = false;
        _gaps_ms.clear();
        _gaps_ms.reserve(1 << 20);
        _done = run();
    }
    // Call from a seastar thread.
    void stop() {
        _stop = true;
        std::exchange(_done, make_ready_future<>()).get();
        std::sort(_gaps_ms.begin(), _gaps_ms.end());
    }
    // Call after stop().
    const std::vector<double>& gaps_ms() const {
        return _gaps_ms;
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
static future<> test_load_topology_state(app_template& app) {
    return do_with_cql_env_thread([&] (cql_test_env& e) {
        using clk = std::chrono::steady_clock;

        int nr_nodes = app.configuration()["nodes"].as<int>();
        if (nr_nodes == 0) {
            nr_nodes = 3;
        }
        const unsigned tokens_per_node = app.configuration()["tokens-per-node"].as<unsigned>();
        const int nr_left_nodes = app.configuration()["left-nodes"].as<int>();
        const int nr_leaving = app.configuration()["leaving-nodes"].as<int>();
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
        std::vector<locator::host_id> normal_hosts;
        for (int i = 0; i < nr_nodes; ++i) {
            normal_hosts.push_back(topo.add_node(service::node_state::normal, 1, std::nullopt, tokens_per_node, features));
        }
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

        for (int i = 0; i < 3; ++i) {
            sys_ks.load_topology_state({}).get();
        }

        preemption_probe probe;

        // Noise floor: what the probe sees with only the test env's own
        // background fibers running. Anything the measured run reports above
        // this is caused by the code under test.
        probe.start();
        seastar::sleep(std::chrono::seconds(2)).get();
        probe.stop();
        const auto idle_gaps = probe.gaps_ms();
        const auto idle_max = idle_gaps.back();
        const auto idle_p999 = percentile(idle_gaps, 0.999);

        std::vector<double> lat;
        lat.reserve(iterations);
        probe.start();
        const auto run_start = clk::now();
        for (int i = 0; i < iterations; ++i) {
            auto start = clk::now();
            sys_ks.load_topology_state({}).get();
            lat.push_back(std::chrono::duration<double, std::milli>(clk::now() - start).count());
        }
        const auto run_ms = std::chrono::duration<double, std::milli>(clk::now() - run_start).count();
        probe.stop();
        const auto& gaps = probe.gaps_ms();

        const auto mean = std::accumulate(lat.begin(), lat.end(), 0.0) / lat.size();
        std::sort(lat.begin(), lat.end());

        testlog.info("load_topology_state nodes={} leaving={} left={} rows={} tokens_per_node={} features={} cdc_ranges={} iterations={} wall={:.1f}ms",
                     nr_nodes, nr_leaving, nr_left_nodes, nr_rows, tokens_per_node, features.size(), cdc_ranges, iterations, run_ms);
        testlog.info("RESULT nodes={} leaving={} left={} rows={} tokens={} cdc_ranges={} mean={:.3f} p50={:.3f} p95={:.3f} max={:.3f} "
                     "stall_p99={:.3f} stall_max={:.3f} idle_stall_p999={:.3f} idle_stall_max={:.3f} "
                     "gaps={} [ms]",
                     nr_nodes, nr_leaving, nr_left_nodes, nr_rows, tokens_per_node, cdc_ranges, mean, percentile(lat, 0.5), percentile(lat, 0.95),
                     lat.back(), percentile(gaps, 0.99), gaps.back(), idle_p999, idle_max, gaps.size());
    }, tablet_cql_test_config(app.configuration()["schema-commitlog-segment-size-mb"].as<unsigned>()));
}

namespace perf {

int scylla_tablets_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
            ("tables", bpo::value<int>()->default_value(8), "Number of tables to create.")
            ("tablets-per-table", bpo::value<int>()->default_value(16384), "Number of tablets per table.")
            ("rf", bpo::value<int>()->default_value(3), "Number of replicas per tablet.")
            ("nodes", bpo::value<int>()->default_value(0),
                    "Number of nodes to add to the topology and spread tablet replicas over "
                    "(0 = use rf). Must be >= rf.")
            ("command-max-mutation-size-mb", bpo::value<size_t>()->default_value(service::group0_update_collector::default_max_mutation_size / MiB),
                    "Max in-memory size of a single mutation in the group0 command built via group0_update_collector (controls splitting).")
            ("schema-commitlog-segment-size-mb", bpo::value<unsigned>()->default_value(0),
                    "Override schema_commitlog_segment_size_in_mb (0 = leave the default). The group0 raft "
                    "command size limit is roughly a quarter of this, so raising it allows applying larger "
                    "tablet metadata commands as a single command (see SCYLLADB-2856).")
            ("load-topology-state", "Benchmark db::system_keyspace::load_topology_state() instead of "
                    "the tablet metadata operations.")
            ("tokens-per-node", bpo::value<unsigned>()->default_value(256),
                    "Number of tokens per node in the topology built by --load-topology-state.")
            ("features", bpo::value<int>()->default_value(-1),
                    "Cap the number of supported features per node (-1 = everything this build supports).")
            ("leaving-nodes", bpo::value<int>()->default_value(0),
                    "Number of the --nodes nodes with a pending leave request, as during a decommission "
                    "whose request is paused while tablets drain off the node.")
            ("left-nodes", bpo::value<int>()->default_value(0),
                    "Number of extra system.topology rows in the 'left' state (no tokens, full feature "
                    "set), as a cluster accumulates one per node it ever lost.")
            ("cdc-ranges", bpo::value<int>()->default_value(0),
                    "Pad the committed CDC generation to this many ranges (0 = leave it as the test env "
                    "created it, -1 = rows * tokens-per-node, which is what a real cluster has). "
                    "load_topology_state() counts these rows on every call.")
            ("iterations", bpo::value<int>()->default_value(200),
                    "Number of measured load_topology_state() calls.")
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
                if (app.configuration().contains("load-topology-state")) {
                    test_load_topology_state(app).get();
                } else {
                    test_basic_operations(app).get();
                }
            } catch (seastar::abort_requested_exception&) {
                // Ignore
            } catch (...) {
                on_fatal_internal_error(testlog, format("Aborting on unhandled exception: {}", std::current_exception()));
            }
        });
    });
}

} // namespace perf
