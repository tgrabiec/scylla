/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/consistency_level_type.hh"
#include "utils/assert.hh"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <json/json.h>
#include <fmt/ranges.h>

#include "test/lib/cql_test_env.hh"
#include "test/perf/perf.hh"
#include <seastar/core/app-template.hh>
#include <seastar/testing/test_runner.hh>
#include "test/lib/random_utils.hh"
#include "db/config.hh"

#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "types/map.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/tags/extension.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"
#include "locator/tablets.hh"
#include "audit/audit.hh"
#include "audit/audit_rule.hh"
#include "keys/keys.hh"
#include "dht/i_partitioner.hh"
#include "replica/database.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/metrics_api.hh>

static const sstring table_name = "cf";

static bytes make_key(uint64_t sequence) {
    bytes b(bytes::initialized_later(), sizeof(sequence));
    auto i = b.begin();
    write<uint64_t>(i, sequence);
    return b;
};

static sstring make_collection_literal(unsigned n) {
    if (n == 0) {
        return "{}";
    }
    // Fixed blob value for all cells, similar to C0..C4 column values
    static constexpr std::string_view cell_value =
        "0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a";
    sstring result = "{";
    for (unsigned i = 0; i < n; ++i) {
        if (i > 0) {
            result += ", ";
        }
        // Key is the 8-byte big-endian encoding of the cell index as a blob
        result += fmt::format("0x{:016x}: {}", i, cell_value);
    }
    result += "}";
    return result;
}

static void execute_update_for_key(cql_test_env& env, const bytes& key, unsigned collection) {
    sstring col_suffix;
    if (collection > 0) {
        col_suffix = fmt::format(", \"CC\" = {}", make_collection_literal(collection));
    }
    // Strongly consistent writes need QUORUM/LOCAL_QUORUM.
    // For eventual consistency it does not matter because there is only one node involved.
    auto qo = std::make_unique<cql3::query_options>(db::consistency_level::QUORUM, std::vector<cql3::raw_value>{}, cql3::query_options::specific_options::DEFAULT);
    env.execute_cql(fmt::format("UPDATE cf SET "
        "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
        "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
        "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
        "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
        "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27{} "
        "WHERE \"KEY\"= 0x{};", col_suffix, to_hex(key)), std::move(qo)).get();
};

static void execute_counter_update_for_key(cql_test_env& env, const bytes& key) {
    env.execute_cql(fmt::format("UPDATE cf SET "
        "\"C0\" = \"C0\" + 1,"
        "\"C1\" = \"C1\" + 2,"
        "\"C2\" = \"C2\" + 3,"
        "\"C3\" = \"C3\" + 4,"
        "\"C4\" = \"C4\" + 5 "
        "WHERE \"KEY\"= 0x{};", to_hex(key))).get();
};

struct test_config {
    enum class run_mode { read, write, del };
    run_mode mode;
    unsigned partitions;
    unsigned concurrency;
    bool query_single_key;
    unsigned duration_in_seconds;
    bool counters;
    bool flush_memtables;
    unsigned memtable_partitions = 0;
    unsigned operations_per_shard = 0;
    bool stop_on_error;
    sstring timeout;
    bool bypass_cache;
    std::optional<unsigned> initial_tablets;
    unsigned tablet_migration_batch = 0; // 0 = no migrations
    unsigned collection = 0;
    db::consistency_level consistency_level;
    bool shard_aware;
};

// Partition sequence numbers grouped by the shard that services reads for them,
// indexed by shard id. Lets a worker running on shard S pick a key owned by S,
// avoiding cross-shard hops. Each shard is later handed its own slice
// (a sharded<std::vector<uint64_t>>) so the hot path touches only NUMA-local
// memory.
using shard_sequences = std::vector<std::vector<uint64_t>>;

std::ostream& operator<<(std::ostream& os, const test_config::run_mode& m) {
    switch (m) {
        case test_config::run_mode::write: return os << "write";
        case test_config::run_mode::read: return os << "read";
        case test_config::run_mode::del: return os << "delete";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    return os << "{partitions=" << cfg.partitions
           << ", concurrency=" << cfg.concurrency
           << ", mode=" << cfg.mode
           << ", query_single_key=" << (cfg.query_single_key ? "yes" : "no")
           << ", counters=" << (cfg.counters ? "yes" : "no")
           << ", collection=" << cfg.collection
           << ", shard_aware=" << (cfg.shard_aware ? "yes" : "no")
           << "}";
}

static void create_partitions(cql_test_env& env, test_config& cfg) {
    std::cout << "Creating " << cfg.partitions << " partitions..." << std::endl;
    unsigned next_flush = (cfg.memtable_partitions > 0 ? cfg.memtable_partitions : cfg.partitions);
    for (unsigned sequence = 0; sequence < cfg.partitions; ++sequence) {
        if (cfg.counters) {
            execute_counter_update_for_key(env, make_key(sequence));
        } else {
            execute_update_for_key(env, make_key(sequence), cfg.collection);
        }
        if (sequence + 1 >= next_flush) {
            env.db().invoke_on_all(&replica::database::flush_all_memtables).get();
            next_flush += cfg.memtable_partitions;
        }
    }

    if (cfg.flush_memtables) {
        std::cout << "Flushing partitions..." << std::endl;
        env.db().invoke_on_all(&replica::database::flush_all_memtables).get();
    }
}

// Groups partition sequence numbers by their read-owning shard. The sharder is
// consulted on the local shard but reports the servicing shard for the whole
// node, so the complete table can be built in one place and then distributed.
// Returns a table with empty per-shard entries when the sequences won't be used
// (shard-awareness disabled, or a fixed single key is queried).
static shard_sequences build_shard_sequences(cql_test_env& env, test_config& cfg) {
    shard_sequences result(this_smp_shard_count());
    if (!cfg.shard_aware || cfg.query_single_key) {
        return result;
    }
    auto& cf = env.local_db().find_column_family("ks", table_name);
    auto schema = cf.schema();
    auto erm = cf.get_effective_replication_map();
    for (uint64_t seq = 0; seq < cfg.partitions; ++seq) {
        auto pk = partition_key::from_single_value(*schema, make_key(seq));
        auto shard = erm->shard_for_reads(*schema, dht::get_token(*schema, pk));
        result[shard].push_back(seq);
    }
    return result;
}

// Picks the key for the next operation on the current shard, drawing from the
// shard-local sequence numbers when shard-awareness is enabled so the query is
// serviced locally. Returns nullopt when this shard owns no partitions in
// shard-aware mode, signalling the worker to stay idle rather than issue a
// cross-shard query.
static std::optional<bytes> next_key(test_config& cfg, const std::vector<uint64_t>& shard_seqs) {
    if (cfg.query_single_key) {
        return make_key(0);
    }
    if (cfg.shard_aware) {
        if (shard_seqs.empty()) {
            return std::nullopt;
        }
        return make_key(shard_seqs[tests::random::get_int<uint64_t>(shard_seqs.size() - 1)]);
    }
    return make_key(tests::random::get_int<uint64_t>(cfg.partitions - 1));
}

static std::vector<perf_result> test_read(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
    sstring query = "select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\"";
    if (cfg.collection > 0) {
        query += ", \"CC\"";
    }
    query += " from cf where \"KEY\" = ?";
    if (cfg.bypass_cache) {
        query += " bypass cache";
    }
    if (!cfg.timeout.empty()) {
        query += " using timeout " + cfg.timeout;
    }
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, &shard_seqs, id] {
            auto key = next_key(cfg, shard_seqs.local());
            if (!key) {
                // This shard owns no partitions in shard-aware mode; idle for
                // one measurement window instead of issuing a cross-shard query.
                return seastar::sleep(std::chrono::seconds(1));
            }
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(*key))}}, cfg.consistency_level).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_write(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    sstring col_suffix;
    if (cfg.collection > 0) {
        col_suffix = fmt::format(", \"CC\" = {}", make_collection_literal(cfg.collection));
    }
    sstring query = format("UPDATE cf {}SET "
            "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
            "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
            "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
            "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
            "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27{} "
            "WHERE \"KEY\" = ?", usings, col_suffix);
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, &shard_seqs, id] {
            auto key = next_key(cfg, shard_seqs.local());
            if (!key) {
                // This shard owns no partitions in shard-aware mode; idle for
                // one measurement window instead of issuing a cross-shard query.
                return seastar::sleep(std::chrono::seconds(1));
            }
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(*key))}}, cfg.consistency_level).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_delete(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    sstring col_suffix;
    if (cfg.collection > 0) {
        col_suffix = ", \"CC\"";
    }
    sstring query = format("DELETE \"C0\", \"C1\", \"C2\", \"C3\", \"C4\"{} FROM cf {}WHERE \"KEY\" = ?", col_suffix, usings);
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, &shard_seqs, id] {
            auto key = next_key(cfg, shard_seqs.local());
            if (!key) {
                // This shard owns no partitions in shard-aware mode; idle for
                // one measurement window instead of issuing a cross-shard query.
                return seastar::sleep(std::chrono::seconds(1));
            }
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(*key))}}, cfg.consistency_level).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_counter_update(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    sstring query = format("UPDATE cf {}SET "
            "\"C0\" = \"C0\" + 1,"
            "\"C1\" = \"C1\" + 2,"
            "\"C2\" = \"C2\" + 3,"
            "\"C3\" = \"C3\" + 4,"
            "\"C4\" = \"C4\" + 5 "
            "WHERE \"KEY\" = ?", usings);
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, &shard_seqs, id] {
            auto key = next_key(cfg, shard_seqs.local());
            if (!key) {
                // This shard owns no partitions in shard-aware mode; idle for
                // one measurement window instead of issuing a cross-shard query.
                return seastar::sleep(std::chrono::seconds(1));
            }
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(*key))}}, cfg.consistency_level).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static schema_ptr make_counter_schema(std::string_view ks_name) {
    return schema_builder(this_smp_shard_count(), ks_name, "cf")
            .with_column("KEY", bytes_type, column_kind::partition_key)
            .with_column("C0", counter_type)
            .with_column("C1", counter_type)
            .with_column("C2", counter_type)
            .with_column("C3", counter_type)
            .with_column("C4", counter_type)
            .build();
}

// Snapshot of this shard's reactor_stalls histogram (microsecond buckets,
// counts task runtimes exceeding 2x task quota; unlike stall reports, not
// rate-limited).
static seastar::metrics::histogram reactor_stalls_snapshot() {
    const auto& vm = seastar::metrics::impl::get_value_map();
    auto it = vm.find("reactor_stalls");
    if (it == vm.end() || it->second.empty()) {
        return {};
    }
    return (*it->second.begin()->second)().get_histogram();
}

static std::vector<seastar::metrics::histogram> reactor_stalls_all_shards() {
    std::vector<seastar::metrics::histogram> res(this_smp_shard_count());
    for (unsigned shard = 0; shard < this_smp_shard_count(); ++shard) {
        res[shard] = smp::submit_to(shard, [] { return reactor_stalls_snapshot(); }).get();
    }
    return res;
}

struct stall_stats {
    uint64_t count = 0;
    double max_us = 0; // upper bound of the highest bucket hit
};

static stall_stats stalls_delta(const seastar::metrics::histogram& before, const seastar::metrics::histogram& after) {
    stall_stats r;
    r.count = after.sample_count - before.sample_count;
    uint64_t prev_a = 0;
    uint64_t prev_b = 0;
    for (size_t i = 0; i < after.buckets.size(); ++i) {
        // Buckets hold cumulative counts.
        uint64_t cum_b = i < before.buckets.size() ? before.buckets[i].count : 0;
        if ((after.buckets[i].count - prev_a) > (cum_b - prev_b)) {
            r.max_us = after.buckets[i].upper_bound;
        }
        prev_a = after.buckets[i].count;
        prev_b = cum_b;
    }
    // Overflow bucket: only a lower bound for max is known.
    if (!after.buckets.empty() && (after.sample_count - prev_a) > (before.sample_count - prev_b)) {
        r.max_us = after.buckets.back().upper_bound;
    }
    return r;
}

// Swaps tablets between shards in batches for the duration of the workload.
// Each move is a full intra-node migration: topology transition with global
// barriers, storage clone on the node (memtable flush, sstable hard links),
// and source-side cleanup (unlinks). Reproduces the fs-metadata and logging
// storm seen with mass sibling-colocation migrations (CUSTOMER-651).
class tablet_migrator {
    cql_test_env& _env;
    unsigned _batch;
    bool _stop = false;
    uint64_t _migrations = 0;
    size_t _next = 0; // rotates batches over all tablets
    future<> _done = make_ready_future<>();
public:
    tablet_migrator(cql_test_env& env, unsigned batch) : _env(env), _batch(batch) {}
    void start() {
        _done = seastar::async([this] { run(); });
    }
    future<> stop() {
        _stop = true;
        return std::move(_done);
    }
    uint64_t migrations() const { return _migrations; }
private:
    void run() {
        auto& ss = _env.get_storage_service().local();
        auto table = _env.local_db().find_column_family("ks", table_name).schema()->id();
        const auto shards = this_smp_shard_count();
        while (!_stop) {
            struct tablet_move {
                dht::token last_token;
                locator::tablet_replica src;
                locator::tablet_replica dst;
            };
            std::vector<tablet_move> moves;
            {
                // No yields while the map reference is alive.
                const auto& tmap = _env.local_db().get_token_metadata().tablets().get_tablet_map(table);
                const auto count = tmap.tablet_count();
                for (size_t i = 0; i < std::min<size_t>(_batch, count); ++i) {
                    auto tid = locator::tablet_id((_next + i) % count);
                    auto src = tmap.get_tablet_info(tid).replicas.front();
                    auto dst = src;
                    dst.shard = (src.shard + 1) % shards;
                    moves.push_back({tmap.get_last_token(tid), src, dst});
                }
                _next = (_next + moves.size()) % count;
            }
            parallel_for_each(moves, [&] (tablet_move& m) {
                return ss.move_tablet(table, m.last_token, m.src, m.dst);
            }).get();
            _migrations += moves.size();
        }
    }
};

static std::vector<perf_result> do_cql_test(cql_test_env& env, test_config& cfg) {
    std::cout << "Running test with config: " << cfg << std::endl;
    env.create_table([&cfg] (auto ks_name) {
        if (cfg.counters) {
            return *make_counter_schema(ks_name);
        }
        auto sb = schema_builder(this_smp_shard_count(), ks_name, "cf")
                .with_column("KEY", bytes_type, column_kind::partition_key)
                .with_column("C0", bytes_type)
                .with_column("C1", bytes_type)
                .with_column("C2", bytes_type)
                .with_column("C3", bytes_type)
                .with_column("C4", bytes_type);
        if (cfg.collection > 0) {
            sb.with_column("CC", map_type_impl::get_instance(bytes_type, bytes_type, true));
        }
        return *sb.build();
    }).get();

    std::cout << "Disabling auto compaction" << std::endl;
    env.db().invoke_on_all([] (auto& db) {
        auto& cf = db.find_column_family("ks", "cf");
        return cf.disable_auto_compaction();
    }).get();

    // Build the shard->sequences table once, then hand each shard its own slice
    // so the hot path reads only NUMA-local memory.
    auto table = build_shard_sequences(env, cfg);
    sharded<std::vector<uint64_t>> shard_seqs;
    shard_seqs.start().get();
    auto stop_shard_seqs = defer([&shard_seqs] noexcept {
        shard_seqs.stop().get();
    });
    shard_seqs.invoke_on_all([&table] (std::vector<uint64_t>& s) {
        s = table[this_shard_id()];
    }).get();

    // Populate before starting migrations so they only disturb the measured phase.
    if (cfg.mode == test_config::run_mode::read || cfg.mode == test_config::run_mode::del) {
        create_partitions(env, cfg);
    }

    std::optional<tablet_migrator> migrator;
    if (cfg.tablet_migration_batch) {
        migrator.emplace(env, cfg.tablet_migration_batch);
        migrator->start();
    }
    auto stop_migrator = defer([&migrator] noexcept {
        if (migrator) {
            try {
                migrator->stop().get();
            } catch (...) {
                fmt::print(std::cerr, "tablet migrator failed: {}\n", std::current_exception());
            }
        }
    });

    auto run = [&] {
        switch (cfg.mode) {
        case test_config::run_mode::read:
            return test_read(env, cfg, shard_seqs);
        case test_config::run_mode::write:
            if (cfg.counters) {
                return test_counter_update(env, cfg, shard_seqs);
            } else {
                return test_write(env, cfg, shard_seqs);
            }
        case test_config::run_mode::del:
            return test_delete(env, cfg, shard_seqs);
        };
        abort();
    };
    auto stalls_before = reactor_stalls_all_shards();
    auto results = run();
    auto stalls_after = reactor_stalls_all_shards();

    if (migrator) {
        stop_migrator.cancel();
        migrator->stop().get();
        std::cout << "Tablet migrations completed: " << migrator->migrations() << std::endl;
    }

    stall_stats total;
    for (unsigned shard = 0; shard < this_smp_shard_count(); ++shard) {
        auto d = stalls_delta(stalls_before[shard], stalls_after[shard]);
        total.count += d.count;
        total.max_us = std::max(total.max_us, d.max_us);
        if (d.count) {
            fmt::print("Reactor stalls on shard {}: count={} max<={:.1f}ms\n", shard, d.count, d.max_us / 1000.0);
        }
    }
    fmt::print("Reactor stalls total: count={} max<={:.1f}ms\n", total.count, total.max_us / 1000.0);
    return results;
}

void write_json_result(std::string result_file, const test_config& cfg, const aggregated_perf_results& agg) {
    Json::Value params;
    params["concurrency"] = cfg.concurrency;
    params["partitions"] = cfg.partitions;
    params["cpus"] = this_smp_shard_count();
    params["duration"] = cfg.duration_in_seconds;
    params["concurrency,partitions,cpus,duration"] = fmt::format("{},{},{},{}", cfg.concurrency, cfg.partitions, this_smp_shard_count(), cfg.duration_in_seconds);
    if (cfg.initial_tablets) {
        params["initial_tablets"] = cfg.initial_tablets.value();
    }
    if (cfg.collection > 0) {
        params["collection"] = cfg.collection;
    }

    std::string test_type;
    switch (cfg.mode) {
    case test_config::run_mode::read: test_type = "read"; break;
    case test_config::run_mode::write: test_type = "write"; break;
    case test_config::run_mode::del: test_type = "delete"; break;
    }
    if (cfg.counters) {
        test_type += "_counters";
    }

    perf::write_json_result(result_file, agg, params, test_type);
}

/// If app configuration contains the named parameter, store its value into \p store.
static void set_from_cli(const char* name, app_template& app, utils::config_file::named_value<sstring>& store) {
    const auto& cfg = app.configuration();
    auto found = cfg.find(name);
    if (found != cfg.end()) {
        store(found->second.as<std::string>());
    }
}

namespace perf {

int scylla_simple_query_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("random-seed", boost::program_options::value<unsigned>(), "Random number generator seed")
        ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
        ("write", "test write path instead of read path")
        ("delete", "test delete path instead of read path")
        ("duration", bpo::value<unsigned>()->default_value(5), "test duration in seconds")
        ("query-single-key", "test reading with a single key instead of random keys")
        ("concurrency", bpo::value<unsigned>()->default_value(100), "workers per core")
        ("operations-per-shard", bpo::value<unsigned>(), "run this many operations per shard (overrides duration)")
        ("counters", "test counters")
        ("collection", bpo::value<unsigned>()->default_value(0), "add map<text,text> collection column with N cells per row (excludes --counters)")
        ("tablets", "use tablets")
        ("strongly-consistent-tables", "use strongly consistent tables")
        ("consistency-level", bpo::value<std::string>()->default_value("QUORUM"), "consistency level used for read and write operations")
        ("initial-tablets", bpo::value<unsigned>()->default_value(128), "initial number of tablets")
        ("tablet-migration-batch", bpo::value<unsigned>()->default_value(0), "run intra-node tablet migrations concurrently with the workload, this many tablets at a time (requires --tablets and >= 2 shards)")
        ("sstable-summary-ratio", bpo::value<double>(), "Generate summary entry, so that summary file size / data file size ~= this ratio")
        ("sstable-format", bpo::value<std::string>(), "SSTable format name to use")
        ("flush", "flush memtables before test")
        ("memtable-partitions", bpo::value<unsigned>(), "apply this number of partitions to memtable, then flush")
        ("json-result", bpo::value<std::string>(), "name of the json result file")
        ("enable-cache", bpo::value<bool>()->default_value(true), "enable row cache")
        ("enable-index-cache", bpo::value<bool>()->default_value(true), "enable partition index cache")
        ("stop-on-error", bpo::value<bool>()->default_value(true), "stop after encountering the first error")
        ("timeout", bpo::value<std::string>()->default_value(""), "use timeout")
        ("bypass-cache", "use bypass cache when querying")
        ("shard-aware", bpo::value<bool>()->default_value(true), "generate keys owned by the shard issuing the query (use --shard-aware 0 to disable)")
        ("audit", bpo::value<std::string>(), "value for audit config entry")
        ("audit-keyspaces", bpo::value<std::string>(), "value for audit_keyspaces config entry")
        ("audit-tables", bpo::value<std::string>(), "value for audit_tables config entry")
        ("audit-categories", bpo::value<std::string>(), "value for audit_categories config entry")
        ("audit-unix-socket-path", bpo::value<std::string>(), "value for audit_unix_socket_path config entry")
        ("audit-rules", bpo::value<std::string>(), "JSON value for audit_rules config entry")
        ;

    set_abort_on_internal_error(true);

    return app.run(argc, argv, [&app] {
        auto conf_seed = app.configuration()["random-seed"];
        auto seed = conf_seed.empty() ? std::random_device()() : conf_seed.as<unsigned>();
        std::cout << "random-seed=" << seed << '\n';
        return smp::invoke_on_all([seed] {
            seastar::testing::local_random_engine.seed(seed + this_shard_id());
        }).then([&app] () -> future<> {
            auto ext = std::make_shared<db::extensions>();
            ext->add_schema_extension<db::tags_extension>(db::tags_extension::NAME);
            auto db_cfg = ::make_shared<db::config>(ext);

            const auto enable_cache = app.configuration()["enable-cache"].as<bool>();
            const auto enable_index_cache = app.configuration()["enable-index-cache"].as<bool>();
            std::cout << "enable-cache=" << enable_cache << '\n';
            std::cout << "enable-index-cache=" << enable_index_cache << '\n';
            db_cfg->enable_cache(enable_cache);
            db_cfg->cache_index_pages(enable_index_cache);
            if (app.configuration().contains("sstable-summary-ratio")) {
                db_cfg->sstable_summary_ratio(app.configuration()["sstable-summary-ratio"].as<double>());
            }
            std::cout << "sstable-summary-ratio=" << db_cfg->sstable_summary_ratio() << '\n';
            if (app.configuration().contains("sstable-format")) {
                db_cfg->sstable_format(app.configuration()["sstable-format"].as<std::string>());
            }
            std::cout << "sstable-format=" << db_cfg->sstable_format() << '\n';
            cql_test_config cfg(db_cfg);
            if (app.configuration()["tablet-migration-batch"].as<unsigned>()) {
                // Tablet migration streaming goes through loopback RPC.
                cfg.ms_listen = true;
            }
            if (app.configuration().contains("tablets")) {
                cfg.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::enabled);
                cfg.initial_tablets = app.configuration()["initial-tablets"].as<unsigned>();
            }
            if (app.configuration().contains("strongly-consistent-tables")) {
                cfg.db_config->experimental_features({db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES},
                                                     db::config::config_source::CommandLine);
                cfg.strongly_consistent_tables = true;
            }
            set_from_cli("audit", app, cfg.db_config->audit);
            set_from_cli("audit-keyspaces", app, cfg.db_config->audit_keyspaces);
            set_from_cli("audit-tables", app, cfg.db_config->audit_tables);
            set_from_cli("audit-categories", app, cfg.db_config->audit_categories);
            set_from_cli("audit-unix-socket-path", app, cfg.db_config->audit_unix_socket_path);
            if (app.configuration().contains("audit-rules")) {
                cfg.db_config->audit_rules(audit::parse_audit_rules_from_json(app.configuration()["audit-rules"].as<std::string>()));
            }
          return do_with_cql_env_thread([&app] (auto&& env) {
            auto cfg = test_config();
            cfg.partitions = app.configuration()["partitions"].as<unsigned>();
            cfg.duration_in_seconds = app.configuration()["duration"].as<unsigned>();
            cfg.concurrency = app.configuration()["concurrency"].as<unsigned>();
            cfg.query_single_key = app.configuration().contains("query-single-key");
            cfg.counters = app.configuration().contains("counters");
            cfg.flush_memtables = app.configuration().contains("flush");
            cfg.collection = app.configuration()["collection"].as<unsigned>();
            if (cfg.counters && cfg.collection > 0) {
                throw std::invalid_argument("--collection and --counters are mutually exclusive");
            }
            if (app.configuration().contains("tablets")) {
                cfg.initial_tablets = app.configuration()["initial-tablets"].as<unsigned>();
            }
            cfg.tablet_migration_batch = app.configuration()["tablet-migration-batch"].as<unsigned>();
            if (cfg.tablet_migration_batch) {
                if (!cfg.initial_tablets) {
                    throw std::invalid_argument("--tablet-migration-batch requires --tablets");
                }
                if (this_smp_shard_count() < 2) {
                    throw std::invalid_argument("--tablet-migration-batch requires at least 2 shards");
                }
            }
            if (app.configuration().contains("write")) {
                cfg.mode = test_config::run_mode::write;
            } else if (app.configuration().contains("delete")) {
                cfg.mode = test_config::run_mode::del;
            } else {
                cfg.mode = test_config::run_mode::read;
            };
            if (app.configuration().contains("operations-per-shard")) {
                cfg.operations_per_shard = app.configuration()["operations-per-shard"].as<unsigned>();
            }
            if (app.configuration().contains("memtable-partitions")) {
                cfg.memtable_partitions = app.configuration()["memtable-partitions"].as<unsigned>();
            }
            cfg.stop_on_error = app.configuration()["stop-on-error"].as<bool>();
            cfg.timeout = app.configuration()["timeout"].as<std::string>();
            cfg.bypass_cache = app.configuration().contains("bypass-cache");
            cfg.shard_aware = app.configuration()["shard-aware"].as<bool>();
            cfg.consistency_level = db::consistency_level_from_string(app.configuration()["consistency-level"].as<std::string>());
            audit::audit::start_audit(env.local_db().get_config(), env.get_shared_token_metadata(), env.qp(), env.migration_manager()).handle_exception([&] (auto&& e) {
                fmt::print("audit start failed: {}", e);
            }).get();
            audit::audit::start_storage(env.local_db().get_config()).get();
            auto audit_stop = defer([] noexcept {
                audit::audit::stop_audit().get();
            });
            auto audit_storage_stop = defer([] noexcept {
                audit::audit::stop_storage().get();
            });
            audit::audit::audit_instance().invoke_on_all([] (audit::audit& a) {
                a.on_role_created("tester");
            }).get();
            auto results = do_cql_test(env, cfg);
            aggregated_perf_results agg(results);
            std::cout << agg << std::endl;
            if (app.configuration().contains("json-result")) {
                write_json_result(app.configuration()["json-result"].as<std::string>(), cfg, agg);
            }
          }, std::move(cfg));
        });
    });
}

} // namespace perf
