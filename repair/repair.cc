/*
 * Copyright (C) 2015 ScyllaDB
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

#include "repair.hh"
#include "repair/row_level.hh"
#include "range_split.hh"

#include "atomic_cell_hash.hh"
#include "streaming/stream_plan.hh"
#include "streaming/stream_state.hh"
#include "streaming/stream_reason.hh"
#include "gms/inet_address.hh"
#include "service/storage_service.hh"
#include "service/priority_manager.hh"
#include "message/messaging_service.hh"
#include "sstables/sstables.hh"
#include "database.hh"
#include "hashers.hh"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/algorithm_ext.hpp>
#include <boost/range/adaptor/map.hpp>

#include <seastar/core/gate.hh>
#include <seastar/util/defer.hh>

logging::logger rlogger("repair");

template <typename T1, typename T2>
inline
static std::ostream& operator<<(std::ostream& os, const std::unordered_map<T1, T2>& v) {
    bool first = true;
    os << "{";
    for (auto&& elem : v) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << elem.first << "=" << elem.second;
    }
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& out, row_level_diff_detect_algorithm algo) {
    switch (algo) {
    case row_level_diff_detect_algorithm::send_full_set:
        return out << "send_full_set";
    };
    return out << "unknown";
}

static std::vector<sstring> list_column_families(const database& db, const sstring& keyspace) {
    std::vector<sstring> ret;
    for (auto &&e : db.get_column_families_mapping()) {
        if (e.first.first == keyspace) {
            ret.push_back(e.first.second);
        }
    }
    return ret;
}

template<typename Collection, typename T>
void remove_item(Collection& c, T& item) {
    auto it = std::find(c.begin(), c.end(), item);
    if (it != c.end()) {
        c.erase(it);
    }
}

// Return all of the neighbors with whom we share the provided range.
static std::vector<gms::inet_address> get_neighbors(database& db,
        const sstring& ksname, query::range<dht::token> range,
        const std::vector<sstring>& data_centers,
        const std::vector<sstring>& hosts) {

    keyspace& ks = db.find_keyspace(ksname);
    auto& rs = ks.get_replication_strategy();

    dht::token tok = range.end() ? range.end()->value() : dht::maximum_token();
    auto ret = rs.get_natural_endpoints(tok);
    remove_item(ret, utils::fb_utilities::get_broadcast_address());

    if (!data_centers.empty()) {
        auto dc_endpoints_map = service::get_local_storage_service().get_token_metadata().get_topology().get_datacenter_endpoints();
        std::unordered_set<gms::inet_address> dc_endpoints;
        for (const sstring& dc : data_centers) {
            auto it = dc_endpoints_map.find(dc);
            if (it == dc_endpoints_map.end()) {
                std::vector<sstring> dcs;
                for (const auto& e : dc_endpoints_map) {
                    dcs.push_back(e.first);
                }
                throw std::runtime_error(sprint("Unknown data center '%s'. "
                        "Known data centers: %s", dc, dcs));
            }
            for (const auto& endpoint : it->second) {
                dc_endpoints.insert(endpoint);
            }
        }
        // We require, like Cassandra does, that the current host must also
        // be part of the repair
        if (!dc_endpoints.count(utils::fb_utilities::get_broadcast_address())) {
            throw std::runtime_error("The current host must be part of the repair");
        }
        // The resulting list of nodes is the intersection of the nodes in the
        // listed data centers, and the (range-dependent) list of neighbors.
        std::unordered_set<gms::inet_address> neighbor_set(ret.begin(), ret.end());
        ret.clear();
        for (const auto& endpoint : dc_endpoints) {
            if (neighbor_set.count(endpoint)) {
                ret.push_back(endpoint);
            }
        }
    } else if (!hosts.empty()) {
        bool found_me = false;
        std::unordered_set<gms::inet_address> neighbor_set(ret.begin(), ret.end());
        ret.clear();
        for (const sstring& host : hosts) {
            gms::inet_address endpoint;
            try {
                endpoint = gms::inet_address(host);
            } catch(...) {
                throw std::runtime_error(format("Unknown host specified: {}", host));
            }
            if (endpoint == utils::fb_utilities::get_broadcast_address()) {
                found_me = true;
            } else if (neighbor_set.count(endpoint)) {
                ret.push_back(endpoint);
                // If same host is listed twice, don't add it again later
                neighbor_set.erase(endpoint);
            }
            // Nodes which aren't neighbors for this range are ignored.
            // This allows the user to give a list of "good" nodes, where
            // for each different range, only the subset of nodes actually
            // holding a replica of the given range is used. This,
            // however, means the user is never warned if one of the nodes
            // on the list isn't even part of the cluster.
        }
        // We require, like Cassandra does, that the current host must also
        // be listed on the "-hosts" option - even those we don't want it in
        // the returned list:
        if (!found_me) {
            throw std::runtime_error("The current host must be part of the repair");
        }
        if (ret.size() < 1) {
            auto me = utils::fb_utilities::get_broadcast_address();
            auto others = rs.get_natural_endpoints(tok);
            remove_item(others, me);
            throw std::runtime_error(sprint("Repair requires at least two "
                    "endpoints that are neighbors before it can continue, "
                    "the endpoint used for this repair is %s, other "
                    "available neighbors are %s but these neighbors were not "
                    "part of the supplied list of hosts to use during the "
                    "repair (%s).", me, others, hosts));
        }
    }

    return ret;

#if 0
    // Origin's ActiveRepairService.getNeighbors() also verifies that the
    // requested range fits into a local range
        StorageService ss = StorageService.instance;
        Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(keyspaceName);
        Range<Token> rangeSuperSet = null;
        for (Range<Token> range : ss.getLocalRanges(keyspaceName))
        {
            if (range.contains(toRepair))
            {
                rangeSuperSet = range;
                break;
            }
            else if (range.intersects(toRepair))
            {
                throw new IllegalArgumentException("Requested range intersects a local range but is not fully contained in one; this would lead to imprecise repair");
            }
        }
        if (rangeSuperSet == null || !replicaSets.containsKey(rangeSuperSet))
            return Collections.emptySet();

#endif
}

// The repair_tracker tracks ongoing repair operations and their progress.
// A repair which has already finished successfully is dropped from this
// table, but a failed repair will remain in the table forever so it can
// be queried about more than once (FIXME: reconsider this. But note that
// failed repairs should be rare anwyay).
// This object is not thread safe, and must be used by only one cpu.
class tracker {
private:
    // Each repair_start() call returns a unique int which the user can later
    // use to follow the status of this repair with repair_status().
    // We can't use the number 0 - if repair_start() returns 0, it means it
    // decide quickly that there is nothing to repair.
    int _next_repair_command = 1;
    // Note that there are no "SUCCESSFUL" entries in the "status" map:
    // Successfully-finished repairs are those with id < _next_repair_command
    // but aren't listed as running or failed the status map.
    std::unordered_map<int, repair_status> _status;
    // Used to allow shutting down repairs in progress, and waiting for them.
    seastar::gate _gate;
    // Set when the repair service is being shutdown
    std::atomic_bool _shutdown alignas(seastar::cache_line_size);
    // Map repair id into repair_info. The vector has smp::count elements, each
    // element will be accessed by only one shard.
    std::vector<std::unordered_map<int, lw_shared_ptr<repair_info>>> _repairs;
public:
    tracker() : _shutdown(false) {
    }
    void start(int id) {
        _gate.enter();
        _status[id] = repair_status::RUNNING;
    }
    void done(int id, bool succeeded) {
        if (succeeded) {
            _status.erase(id);
        } else {
            _status[id] = repair_status::FAILED;
        }
        _gate.leave();
    }
    repair_status get(int id) {
        if (id >= _next_repair_command) {
            throw std::runtime_error(format("unknown repair id {:d}", id));
        }
        auto it = _status.find(id);
        if (it == _status.end()) {
            return repair_status::SUCCESSFUL;
        } else {
            return it->second;
        }
    }
    int next_repair_command() {
        return _next_repair_command++;
    }
    future<> shutdown() {
        _shutdown.store(true, std::memory_order_relaxed);
        return _gate.close();
    }
    void check_in_shutdown() {
        if (_shutdown.load(std::memory_order_relaxed)) {
            throw std::runtime_error(format("Repair service is being shutdown"));
        }
    }
    void init_repair_info() {
        if (_repairs.size() != smp::count) {
            _repairs.resize(smp::count);
        }
    }
    void add_repair_info(int id, lw_shared_ptr<repair_info> ri) {
        init_repair_info();
        _repairs[engine().cpu_id()].emplace(id, ri);
    }
    void remove_repair_info(int id) {
        init_repair_info();
        _repairs[engine().cpu_id()].erase(id);
    }
    lw_shared_ptr<repair_info> get_repair_info(int id) {
        init_repair_info();
        auto it = _repairs[engine().cpu_id()].find(id);
        if (it != _repairs[engine().cpu_id()].end()) {
            return it->second;
        }
        return {};
    }
    std::vector<int> get_active() const {
        std::vector<int> res;
        boost::push_back(res, _status | boost::adaptors::filtered([] (auto& x) {
            return x.second == repair_status::RUNNING;
        }) | boost::adaptors::map_keys);
        return res;
    }
    size_t nr_running_repair_jobs() {
        size_t count = 0;
        if (engine().cpu_id() != 0) {
            return count;
        }
        for (auto& x : _status) {
            auto& status = x.second;
            if (status == repair_status::RUNNING) {
                count++;
            }
        }
        return count;
    }
    void abort_all_repairs() {
        init_repair_info();
        size_t count = nr_running_repair_jobs();
        for (auto& x : _repairs[engine().cpu_id()]) {
            auto& ri = x.second;
            ri->abort();
        }
        rlogger.info0("Aborted {} repair job(s)", count);
    }
};

static tracker repair_tracker;

void check_in_shutdown() {
    repair_tracker.check_in_shutdown();
}

class partition_hasher {
    const schema& _schema;
    sha256_hasher _hasher;
    partition_checksum _checksum;

    bound_view::compare _cmp;
    range_tombstone_list _rt_list;
    bool _inside_range_tombstone = false;
private:
    void consume_cell(const column_definition& col, const atomic_cell_or_collection& cell) {
        feed_hash(_hasher, col.name());
        feed_hash(_hasher, col.type->name());
        feed_hash(_hasher, cell, col);
    }

    void consume_range_tombstone_start(const range_tombstone& rt) {
        feed_hash(_hasher, rt.start, _schema);
        feed_hash(_hasher, rt.start_kind);
        feed_hash(_hasher, rt.tomb);
    }

    void consume_range_tombstone_end(const range_tombstone& rt) {
        feed_hash(_hasher, rt.end, _schema);
        feed_hash(_hasher, rt.end_kind);
    }

    void pop_rt_front() {
        auto& rt = *_rt_list.tombstones().begin();
        _rt_list.tombstones().erase(_rt_list.begin());
        current_deleter<range_tombstone>()(&rt);
    }

    void consume_range_tombstones_until(const clustering_row& cr) {
        while (!_rt_list.empty()) {
            auto it = _rt_list.begin();
            if (_inside_range_tombstone) {
                if (_cmp(it->end_bound(), cr.key())) {
                    consume_range_tombstone_end(*it);
                    _inside_range_tombstone = false;
                    pop_rt_front();
                } else {
                    break;
                }
            } else {
                if (_cmp(it->start_bound(), cr.key())) {
                    consume_range_tombstone_start(*it);
                    _inside_range_tombstone = true;
                } else {
                    break;
                }
            }
        }
    }

    void consume_range_tombstones_until_end() {
        if (_inside_range_tombstone) {
            consume_range_tombstone_end(*_rt_list.begin());
            pop_rt_front();
        }
        for (auto&& rt : _rt_list) {
            consume_range_tombstone_start(rt);
            consume_range_tombstone_end(rt);
        }
        _rt_list.clear();
        _inside_range_tombstone = false;
    }
public:
    explicit partition_hasher(const schema& s)
        : _schema(s), _cmp(s), _rt_list(s) { }

    void consume_new_partition(const dht::decorated_key& dk) {
        feed_hash(_hasher, dk.key(), _schema);
    }

    stop_iteration consume(tombstone t) {
        feed_hash(_hasher, t);
        return stop_iteration::no;
    }

    stop_iteration consume(const static_row& sr) {
        sr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
            auto&& col = _schema.static_column_at(id);
            consume_cell(col, cell);
        });
        return stop_iteration::no;
    }

    stop_iteration consume(const clustering_row& cr) {
        consume_range_tombstones_until(cr);

        feed_hash(_hasher, cr.key(), _schema);
        feed_hash(_hasher, cr.tomb());
        feed_hash(_hasher, cr.marker());
        cr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
            auto&& col = _schema.regular_column_at(id);
            consume_cell(col, cell);
        });
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        _rt_list.apply(_schema, std::move(rt));
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_partition() {
        consume_range_tombstones_until_end();

        std::array<uint8_t, 32> digest = _hasher.finalize_array();
        _hasher = { };

        _checksum.add(partition_checksum(digest));
        return stop_iteration::no;
    }

    partition_checksum consume_end_of_stream() {
        return std::move(_checksum);
    }
};

future<partition_checksum> partition_checksum::compute_legacy(flat_mutation_reader mr)
{
    auto s = mr.schema();
    return do_with(std::move(mr),
                   partition_checksum(), [] (auto& reader, auto& checksum) {
        return repeat([&reader, &checksum] () {
            return read_mutation_from_flat_mutation_reader(reader, db::no_timeout).then([&checksum] (auto mopt) {
                if (!mopt) {
                    return stop_iteration::yes;
                }
                sha256_hasher h;
                feed_hash(h, *mopt);
                std::array<uint8_t, 32> digest = h.finalize_array();
                checksum.add(partition_checksum(digest));
                return stop_iteration::no;
            });
        }).then([&checksum] {
            return checksum;
        });
    });
}

future<partition_checksum> partition_checksum::compute_streamed(flat_mutation_reader m)
{
    return do_with(std::move(m), [] (auto& m) {
        return m.consume(partition_hasher(*m.schema()), db::no_timeout);
    });
}

future<partition_checksum> partition_checksum::compute(flat_mutation_reader m, repair_checksum hash_version)
{
    switch (hash_version) {
    case repair_checksum::legacy: return compute_legacy(std::move(m));
    case repair_checksum::streamed: return compute_streamed(std::move(m));
    default: throw std::runtime_error(format("Unknown hash version: {:d}", static_cast<int>(hash_version)));
    }
}

static inline unaligned<uint64_t>& qword(std::array<uint8_t, 32>& b, int n) {
    return *unaligned_cast<uint64_t>(b.data() + 8 * n);
}
static inline const unaligned<uint64_t>& qword(const std::array<uint8_t, 32>& b, int n) {
    return *unaligned_cast<uint64_t>(b.data() + 8 * n);
}

void partition_checksum::add(const partition_checksum& other) {
     static_assert(std::tuple_size<decltype(_digest)>::value == 32, "digest size");
     // Hopefully the following trickery is faster than XOR'ing 32 separate bytes
     qword(_digest, 0) = qword(_digest, 0) ^ qword(other._digest, 0);
     qword(_digest, 1) = qword(_digest, 1) ^ qword(other._digest, 1);
     qword(_digest, 2) = qword(_digest, 2) ^ qword(other._digest, 2);
     qword(_digest, 3) = qword(_digest, 3) ^ qword(other._digest, 3);
}

bool partition_checksum::operator==(const partition_checksum& other) const {
    static_assert(std::tuple_size<decltype(_digest)>::value == 32, "digest size");
    return qword(_digest, 0) == qword(other._digest, 0) &&
           qword(_digest, 1) == qword(other._digest, 1) &&
           qword(_digest, 2) == qword(other._digest, 2) &&
           qword(_digest, 3) == qword(other._digest, 3);
}

const std::array<uint8_t, 32>& partition_checksum::digest() const {
    return _digest;
}

std::ostream& operator<<(std::ostream& out, const partition_checksum& c) {
    auto save_flags = out.flags();
    out << std::hex << std::setfill('0');
    for (auto b : c._digest) {
        out << std::setw(2) << (unsigned int)b;
    }
    out.flags(save_flags);
    return out;
}

// Calculate the checksum of the data held *on this shard* of a column family,
// in the given token range.
// All parameters to this function are constant references, and the caller
// must ensure they live as long as the future returned by this function is
// not resolved.
// FIXME: Both master and slave will typically call this on consecutive ranges
// so it would be useful to have this code cache its stopping point or have
// some object live throughout the operation. Moreover, it makes sense to to
// vary the collection of sstables used throught a long repair.
static future<partition_checksum> checksum_range_shard(database &db,
        const sstring& keyspace_name, const sstring& cf_name,
        const dht::partition_range_vector& prs, repair_checksum hash_version) {
    auto& cf = db.find_column_family(keyspace_name, cf_name);
    auto s = cf.schema();
    auto reader = cf.make_streaming_reader(s, prs, s->full_slice());
    return partition_checksum::compute(std::move(reader), hash_version);
}

// It is counter-productive to allow a large number of range checksum
// operations to proceed in parallel (on the same shard), because the read
// operation can already parallelize itself as much as needed, and doing
// multiple reads in parallel just adds a lot of memory overheads.
// So checksum_parallelism_semaphore is used to limit this parallelism,
// and should be set to 1, or another small number.
//
// Note that checksumming_parallelism_semaphore applies not just in the
// repair master, but also in the slave: The repair slave may receive many
// checksum requests in parallel, but will only work on one or a few
// (checksum_parallelism_semaphore) at once.
static thread_local semaphore checksum_parallelism_semaphore(2);

// Calculate the checksum of the data held on all shards of a column family,
// in the given token range.
// In practice, we only need to consider one or two shards which intersect the
// given "range". This is because the token ring has nodes*vnodes tokens,
// dividing the token space into nodes*vnodes ranges, with "range" being one
// of those. This number is big (vnodes = 256 by default). At the same time,
// sharding divides the token space into relatively few large ranges, one per
// thread.
// Watch out: All parameters to this function are constant references, and the
// caller must ensure they live as line as the future returned by this
// function is not resolved.
future<partition_checksum> checksum_range(seastar::sharded<database> &db,
        const sstring& keyspace, const sstring& cf,
        const ::dht::token_range& range, repair_checksum hash_version) {
    auto& schema = db.local().find_column_family(keyspace, cf).schema();
    auto shard_ranges = dht::split_range_to_shards(dht::to_partition_range(range), *schema);
    return do_with(partition_checksum(), std::move(shard_ranges), [&db, &keyspace, &cf, hash_version] (auto& result, auto& shard_ranges) {
        return parallel_for_each(shard_ranges, [&db, &keyspace, &cf, &result, hash_version] (auto& shard_range) {
            auto& shard = shard_range.first;
            auto& prs = shard_range.second;
            return db.invoke_on(shard, [keyspace, cf, prs = std::move(prs), hash_version] (database& db) mutable {
                return do_with(std::move(keyspace), std::move(cf), std::move(prs), [&db, hash_version] (auto& keyspace, auto& cf, auto& prs) {
                    return seastar::with_semaphore(checksum_parallelism_semaphore, 1, [&db, hash_version, &keyspace, &cf, &prs] {
                        return checksum_range_shard(db, keyspace, cf, prs, hash_version);
                    });
                });
            }).then([&result] (partition_checksum sum) {
                result.add(sum);
            });
        }).then([&result] {
            return make_ready_future<partition_checksum>(result);
        });
    });
}

// parallelism_semaphore limits the number of parallel ongoing checksum
// comparisons. This could mean, for example, that this number of checksum
// requests have been sent to other nodes and we are waiting for them to
// return so we can compare those to our own checksums. This limit can be
// set fairly high because the outstanding comparisons take only few
// resources. In particular, we do NOT do this number of file reads in
// parallel because file reads have large memory overhads (read buffers,
// partitions, etc.) - the number of concurrent reads is further limited
// by an additional semaphore checksum_parallelism_semaphore (see above).
//
// FIXME: This would be better of in a repair service, or even a per-shard
// repair instance holding all repair state. However, since we are anyway
// considering ditching those semaphores for a more fine grained resource-based
// solution, let's do the simplest thing here and change it later
constexpr int parallelism = 100;
static thread_local semaphore parallelism_semaphore(parallelism);

future<uint64_t> estimate_partitions(seastar::sharded<database>& db, const sstring& keyspace,
        const sstring& cf, const dht::token_range& range) {
    return db.map_reduce0(
        [keyspace, cf, range] (auto& db) {
            // FIXME: column_family should have a method to estimate the number of
            // partitions (and of course it should use cardinality estimation bitmaps,
            // not trivial sum). We shouldn't have this ugly code here...
            // FIXME: If sstables are shared, they will be accounted more than
            // once. However, shared sstables should exist for a short-time only.
            auto sstables = db.find_column_family(keyspace, cf).get_sstables();
            return boost::accumulate(*sstables, uint64_t(0),
                [&range] (uint64_t x, auto&& sst) { return x + sst->estimated_keys_for_range(range); });
        },
        uint64_t(0),
        std::plus<uint64_t>()
    );
}

repair_info::repair_info(seastar::sharded<database>& db_,
    const sstring& keyspace_,
    const dht::token_range_vector& ranges_,
    const std::vector<sstring>& cfs_,
    int id_,
    const std::vector<sstring>& data_centers_,
    const std::vector<sstring>& hosts_)
    : db(db_)
    , keyspace(keyspace_)
    , ranges(ranges_)
    , cfs(cfs_)
    , id(id_)
    , shard(engine().cpu_id())
    , data_centers(data_centers_)
    , hosts(hosts_)
    , _row_level_repair(service::get_local_storage_service().cluster_supports_row_level_repair()) {
}

future<> repair_info::do_streaming() {
    size_t ranges_in = 0;
    size_t ranges_out = 0;
    _sp_in = make_lw_shared<streaming::stream_plan>(format("repair-in-id-{:d}-shard-{:d}-index-{:d}", id, shard, sp_index), streaming::stream_reason::repair);
    _sp_out = make_lw_shared<streaming::stream_plan>(format("repair-out-id-{:d}-shard-{:d}-index-{:d}", id, shard, sp_index), streaming::stream_reason::repair);

    for (auto& x : ranges_need_repair_in) {
        auto& peer = x.first;
        for (auto& y : x.second) {
            auto& cf = y.first;
            auto& stream_ranges = y.second;
            ranges_in += stream_ranges.size();
            _sp_in->request_ranges(peer, keyspace, std::move(stream_ranges), {cf});
        }
    }
    ranges_need_repair_in.clear();
    current_sub_ranges_nr_in = 0;

    for (auto& x : ranges_need_repair_out) {
        auto& peer = x.first;
        for (auto& y : x.second) {
            auto& cf = y.first;
            auto& stream_ranges = y.second;
            ranges_out += stream_ranges.size();
            _sp_out->transfer_ranges(peer, keyspace, std::move(stream_ranges), {cf});
        }
    }
    ranges_need_repair_out.clear();
    current_sub_ranges_nr_out = 0;

    if (ranges_in || ranges_out) {
        rlogger.info("Start streaming for repair id={}, shard={}, index={}, ranges_in={}, ranges_out={}", id, shard, sp_index, ranges_in, ranges_out);
    }
    sp_index++;

    return _sp_in->execute().discard_result().then([this, sp_in = _sp_in, sp_out = _sp_out] {
        return _sp_out->execute().discard_result();
    }).handle_exception([this] (auto ep) {
        rlogger.warn("repair's stream failed: {}", ep);
        return make_exception_future(ep);
    }).finally([this] {
        _sp_in = {};
        _sp_out = {};
    });
}

void repair_info::check_failed_ranges() {
    rlogger.info("repair {} on shard {} stats: ranges_nr={}, sub_ranges_nr={}, {}",
        id, shard, ranges.size(), _sub_ranges_nr, _stats.get_stats());
    if (nr_failed_ranges) {
        rlogger.info("repair {} on shard {} failed - {} ranges failed", id, shard, nr_failed_ranges);
        throw std::runtime_error(format("repair {:d} on shard {:d} failed to do checksum for {:d} sub ranges", id, shard, nr_failed_ranges));
    } else {
        rlogger.info("repair {} on shard {} completed successfully", id, shard);
    }
}

future<> repair_info::request_transfer_ranges(const sstring& cf,
    const ::dht::token_range& range,
    const std::vector<gms::inet_address>& neighbors_in,
    const std::vector<gms::inet_address>& neighbors_out) {
    rlogger.debug("Add cf {}, range {}, current_sub_ranges_nr_in {}, current_sub_ranges_nr_out {}", cf, range, current_sub_ranges_nr_in, current_sub_ranges_nr_out);
    return seastar::with_semaphore(sp_parallelism_semaphore, 1, [this, cf, range, neighbors_in, neighbors_out] {
        for (const auto& peer : neighbors_in) {
            ranges_need_repair_in[peer][cf].emplace_back(range);
            current_sub_ranges_nr_in++;
        }
        for (const auto& peer : neighbors_out) {
            ranges_need_repair_out[peer][cf].emplace_back(range);
            current_sub_ranges_nr_out++;
        }
        if (current_sub_ranges_nr_in >= sub_ranges_to_stream || current_sub_ranges_nr_out >= sub_ranges_to_stream) {
            return do_streaming();
        }
        return make_ready_future<>();
    });
}

void repair_info::abort() {
    if (_sp_in) {
        _sp_in->abort();
    }
    if (_sp_out) {
        _sp_out->abort();
    }
    aborted = true;
}

void repair_info::check_in_abort() {
    if (aborted) {
        throw std::runtime_error(format("repair id {:d} is aborted on shard {:d}", id, shard));
    }
}

// Repair a single cf in a single local range.
// Comparable to RepairJob in Origin.
static future<> repair_cf_range(repair_info& ri,
        sstring cf, ::dht::token_range range,
        const std::vector<gms::inet_address>& neighbors) {
    if (neighbors.empty()) {
        // Nothing to do in this case...
        return make_ready_future<>();
    }

    ri.check_in_abort();
    return estimate_partitions(ri.db, ri.keyspace, cf, range).then([&ri, cf, range, &neighbors] (uint64_t estimated_partitions) {
    range_splitter ranges(range, estimated_partitions, ri.target_partitions);
    return do_with(seastar::gate(), true, std::move(cf), std::move(ranges),
        [&ri, &neighbors] (auto& completion, auto& success, const auto& cf, auto& ranges) {
        return do_until([&ranges] () { return !ranges.has_next(); },
            [&ranges, &ri, &completion, &success, &neighbors, &cf] () {
            auto range = ranges.next();
            check_in_shutdown();
            ri.check_in_abort();
            return seastar::get_units(parallelism_semaphore, 1).then([&ri, &completion, &success, &neighbors, &cf, range] (auto signal_sem) {
                auto checksum_type = service::get_local_storage_service().cluster_supports_large_partitions()
                                     ? repair_checksum::streamed : repair_checksum::legacy;

                // Ask this node, and all neighbors, to calculate checksums in
                // this range. When all are done, compare the results, and if
                // there are any differences, sync the content of this range.
                std::vector<future<partition_checksum>> checksums;
                checksums.reserve(1 + neighbors.size());
                checksums.push_back(checksum_range(ri.db, ri.keyspace, cf, range, checksum_type));
                for (auto&& neighbor : neighbors) {
                    checksums.push_back(
                            netw::get_local_messaging_service().send_repair_checksum_range(
                                    netw::msg_addr{neighbor}, ri.keyspace, cf, range, checksum_type));
                }

                completion.enter();
                auto leave = defer([&completion] { completion.leave(); });

                when_all(checksums.begin(), checksums.end()).then(
                        [&ri, &cf, range, &neighbors, &success]
                        (std::vector<future<partition_checksum>> checksums) {
                    // If only some of the replicas of this range are alive,
                    // we set success=false so repair will fail, but we can
                    // still do our best to repair available replicas.
                    std::vector<gms::inet_address> live_neighbors;
                    std::vector<partition_checksum> live_neighbors_checksum;
                    for (unsigned i = 0; i < checksums.size(); i++) {
                        if (checksums[i].failed()) {
                            rlogger.warn(
                                "Checksum of ks={}, table={}, range={} on {} failed: {}",
                                ri.keyspace, cf, range,
                                (i ? neighbors[i-1] :
                                 utils::fb_utilities::get_broadcast_address()),
                                checksums[i].get_exception());
                            success = false;
                            ri.nr_failed_ranges++;
                            // Do not break out of the loop here, so we can log
                            // (and discard) all the exceptions.
                        } else if (i > 0) {
                            live_neighbors.push_back(neighbors[i - 1]);
                            live_neighbors_checksum.push_back(checksums[i].get0());
                        }
                    }
                    if (checksums[0].failed() || live_neighbors.empty()) {
                        return make_ready_future<>();
                    }
                    // If one of the available checksums is different, repair
                    // all the neighbors which returned a checksum.
                    auto checksum0 = checksums[0].get0();
                    std::vector<gms::inet_address> live_neighbors_in(live_neighbors);
                    std::vector<gms::inet_address> live_neighbors_out(live_neighbors);

                    std::unordered_map<partition_checksum, std::vector<gms::inet_address>> checksum_map;
                    for (size_t idx = 0 ; idx < live_neighbors.size(); idx++) {
                        checksum_map[live_neighbors_checksum[idx]].emplace_back(live_neighbors[idx]);
                    }

                    auto node_reducer = [] (std::vector<gms::inet_address>& live_neighbors_in_or_out,
                            std::vector<gms::inet_address>& nodes_with_same_checksum, size_t nr_nodes_to_keep) {
                        // nodes_with_same_checksum contains two types of nodes:
                        // 1) the nodes we want to remove from live_neighbors_in_or_out.
                        // 2) the nodes, nr_nodes_to_keep in number, not to remove from
                        // live_neighbors_in_or_out
                        auto nr_nodes = nodes_with_same_checksum.size();
                        if (nr_nodes <= nr_nodes_to_keep) {
                            return;
                        }

                        if (nr_nodes_to_keep == 0) {
                            // All nodes in nodes_with_same_checksum will be removed from live_neighbors_in_or_out
                        } else if (nr_nodes_to_keep == 1) {
                            auto node_is_remote = [] (gms::inet_address ip) { return !service::get_local_storage_service().is_local_dc(ip); };
                            boost::partition(nodes_with_same_checksum, node_is_remote);
                            nodes_with_same_checksum.resize(nr_nodes - nr_nodes_to_keep);
                        } else {
                            throw std::runtime_error(format("nr_nodes_to_keep = {}, but it can only be 1 or 0", nr_nodes_to_keep));
                        }

                        // Now, nodes_with_same_checksum contains nodes we want to remove, remove it from live_neighbors_in_or_out
                        auto it = boost::range::remove_if(live_neighbors_in_or_out, [&nodes_with_same_checksum] (const auto& ip) {
                            return boost::algorithm::any_of_equal(nodes_with_same_checksum, ip);
                        });
                        live_neighbors_in_or_out.erase(it, live_neighbors_in_or_out.end());
                    };

                    // Reduce in traffic
                    for (auto& item : checksum_map) {
                        auto& sum = item.first;
                        auto nodes_with_same_checksum = item.second;
                        // If remote nodes have the same checksum, fetch only from one of them
                        size_t nr_nodes_to_fetch = 1;
                        // If remote nodes have zero checksum or have the same
                        // checksum as local checksum, do not fetch from them at all
                        if (sum == partition_checksum() || sum == checksum0) {
                            nr_nodes_to_fetch = 0;
                        }
                        // E.g.,
                        // Local  Remote1 Remote2 Remote3
                        // 5      5       5       5         : IN: 0
                        // 5      5       5       0         : IN: 0
                        // 5      5       0       0         : IN: 0
                        // 5      0       0       0         : IN: 0
                        // 0      5       5       5         : IN: 1
                        // 0      5       5       0         : IN: 1
                        // 0      5       0       0         : IN: 1
                        // 0      0       0       0         : IN: 0
                        // 3      5       5       3         : IN: 1
                        // 3      5       3       3         : IN: 1
                        // 3      3       3       3         : IN: 0
                        // 3      5       4       3         : IN: 2
                        node_reducer(live_neighbors_in, nodes_with_same_checksum, nr_nodes_to_fetch);
                    }

                    // Reduce out traffic
                    if (live_neighbors_in.empty()) {
                        for (auto& item : checksum_map) {
                            auto& sum = item.first;
                            auto nodes_with_same_checksum = item.second;
                            // Skip to send to the nodes with the same checksum as local node
                            // E.g.,
                            // Local  Remote1 Remote2 Remote3
                            // 5      5       5       5         : IN: 0  OUT: 0 SKIP_OUT: Remote1, Remote2, Remote3
                            // 5      5       5       0         : IN: 0  OUT: 1 SKIP_OUT: Remote1, Remote2
                            // 5      5       0       0         : IN: 0  OUT: 2 SKIP_OUT: Remote1
                            // 5      0       0       0         : IN: 0  OUT: 3 SKIP_OUT: None
                            // 0      0       0       0         : IN: 0  OUT: 0 SKIP_OUT: Remote1, Remote2, Remote3
                            if (sum == checksum0) {
                                size_t nr_nodes_to_send = 0;
                                node_reducer(live_neighbors_out, nodes_with_same_checksum, nr_nodes_to_send);
                            }
                        }
                    } else if (live_neighbors_in.size() == 1 && checksum0 == partition_checksum()) {
                        for (auto& item : checksum_map) {
                            auto& sum = item.first;
                            auto nodes_with_same_checksum = item.second;
                            // Skip to send to the nodes with none zero checksum
                            // E.g.,
                            // Local  Remote1 Remote2 Remote3
                            // 0      5       5       5         : IN: 1  OUT: 0 SKIP_OUT: Remote1, Remote2, Remote3
                            // 0      5       5       0         : IN: 1  OUT: 1 SKIP_OUT: Remote1, Remote2
                            // 0      5       0       0         : IN: 1  OUT: 2 SKIP_OUT: Remote1
                            if (sum != checksum0) {
                                size_t nr_nodes_to_send = 0;
                                node_reducer(live_neighbors_out, nodes_with_same_checksum, nr_nodes_to_send);
                            }
                        }
                    }
                    if (!(live_neighbors_in.empty() && live_neighbors_out.empty())) {
                        rlogger.debug("Found differing ks={}, table={}, range={} on nodes={}, in = {}, out = {}",
                                ri.keyspace, cf, range, live_neighbors, live_neighbors_in, live_neighbors_out);
                        ri.check_in_abort();
                        return ri.request_transfer_ranges(cf, range, live_neighbors_in, live_neighbors_out);
                    }
                    return make_ready_future<>();
                }).handle_exception([&ri, &success, &cf, range, leave = std::move(leave),
                        signal_sem = std::move(signal_sem)] (std::exception_ptr eptr) {
                    // Something above (e.g., request_transfer_ranges) failed. We could
                    // stop the repair immediately, or let it continue with
                    // other ranges (at the moment, we do the latter). But in
                    // any case, we need to remember that the repair failed to
                    // tell the caller.
                    success = false;
                    ri.nr_failed_ranges++;
                    rlogger.warn("Failed to sync ks={}, table={}, range={}: {}", ri.keyspace, cf, range, eptr);
                });
            });
        }).finally([&success, &completion, &ri, &cf] {
            return completion.close().then([&success, &ri, &cf] {
                if (!success) {
                    rlogger.warn("Checksum or sync of partial range failed, ks={}, table={}", ri.keyspace, cf);
                }
                // We probably want the repair contiunes even if some
                // ranges fail to do the checksum. We need to set the
                // per-repair success flag to false and report after the
                // streaming is done.
                return make_ready_future<>();
            });
        });
    });
    });
}

// Repair a single local range, multiple column families.
// Comparable to RepairSession in Origin
static future<> repair_range(repair_info& ri, const dht::token_range& range) {
    auto id = utils::UUID_gen::get_time_UUID();
    return do_with(get_neighbors(ri.db.local(), ri.keyspace, range, ri.data_centers, ri.hosts), [&ri, range, id] (const auto& neighbors) {
        rlogger.debug("[repair #{}] new session: will sync {} on range {} for {}.{}", id, neighbors, range, ri.keyspace, ri.cfs);
        return do_for_each(ri.cfs.begin(), ri.cfs.end(), [&ri, &neighbors, range] (auto&& cf) {
            ri._sub_ranges_nr++;
            if (ri.row_level_repair()) {
                return repair_cf_range_row_level(ri, cf, range, neighbors);
            } else {
                return repair_cf_range(ri, cf, range, neighbors);
            }
        });
    });
}

static dht::token_range_vector get_ranges_for_endpoint(
        database& db, sstring keyspace, gms::inet_address ep) {
    auto& rs = db.find_keyspace(keyspace).get_replication_strategy();
    return rs.get_ranges(ep);
}

static dht::token_range_vector get_local_ranges(
        database& db, sstring keyspace) {
    return get_ranges_for_endpoint(db, keyspace, utils::fb_utilities::get_broadcast_address());
}

static dht::token_range_vector get_primary_ranges_for_endpoint(
        database& db, sstring keyspace, gms::inet_address ep) {
    auto& rs = db.find_keyspace(keyspace).get_replication_strategy();
    return rs.get_primary_ranges(ep);
}

static dht::token_range_vector get_primary_ranges(
        database& db, sstring keyspace) {
    return get_primary_ranges_for_endpoint(db, keyspace,
            utils::fb_utilities::get_broadcast_address());
}

// get_primary_ranges_within_dc() is similar to get_primary_ranges(),
// but instead of each range being assigned just one primary owner
// across the entire cluster, here each range is assigned a primary
// owner in each of the clusters.
static dht::token_range_vector get_primary_ranges_within_dc(
        database& db, sstring keyspace) {
    auto& rs = db.find_keyspace(keyspace).get_replication_strategy();
    return rs.get_primary_ranges_within_dc(
            utils::fb_utilities::get_broadcast_address());
}

static sstring get_local_dc() {
    return locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(
            utils::fb_utilities::get_broadcast_address());
}

void repair_stats::add(const repair_stats& o) {
    round_nr += o.round_nr;
    round_nr_fast_path_already_synced += o.round_nr_fast_path_already_synced;
    round_nr_fast_path_same_combined_hashes += o.round_nr_fast_path_same_combined_hashes;
    round_nr_slow_path += o.round_nr_slow_path;
    rpc_call_nr += o.rpc_call_nr;
    tx_hashes_nr += o.tx_hashes_nr;
    rx_hashes_nr += o.rx_hashes_nr;
    tx_row_nr += o.tx_row_nr;
    rx_row_nr += o.rx_row_nr;
    tx_row_bytes += o.tx_row_bytes;
    rx_row_bytes += o.rx_row_bytes;
    auto add_map = [] (auto& target, auto& src) {
         for (const auto& [k, v] : src) {
             target[k] += v;
         }
    };
    add_map(row_from_disk_bytes, o.row_from_disk_bytes);
    add_map(row_from_disk_nr, o.row_from_disk_nr);
    add_map(tx_row_nr_peer, o.tx_row_nr_peer);
    add_map(rx_row_nr_peer, o.rx_row_nr_peer);
}

sstring repair_stats::get_stats() {
    std::map<gms::inet_address, float> row_from_disk_bytes_per_sec;
    std::map<gms::inet_address, float> row_from_disk_rows_per_sec;
    auto duration = std::chrono::duration_cast<std::chrono::duration<float>>(lowres_clock::now() - start_time).count();
    for (auto& x : row_from_disk_bytes) {
        if (std::fabs(duration) > FLT_EPSILON) {
            row_from_disk_bytes_per_sec[x.first] = x.second / duration / 1024 / 1024;
        } else {
            row_from_disk_bytes_per_sec[x.first] = 0;
        }
    }
    for (auto& x : row_from_disk_nr) {
        if (std::fabs(duration) > FLT_EPSILON) {
            row_from_disk_rows_per_sec[x.first] = x.second / duration;
        } else {
            row_from_disk_rows_per_sec[x.first] = 0;
        }
    }
    return format("round_nr={}, round_nr_fast_path_already_synced={}, round_nr_fast_path_same_combined_hashes={}, round_nr_slow_path={}, rpc_call_nr={}, tx_hashes_nr={}, rx_hashes_nr={}, duration={} seconds, tx_row_nr={}, rx_row_nr={}, tx_row_bytes={}, rx_row_bytes={}, row_from_disk_bytes={}, row_from_disk_nr={}, row_from_disk_bytes_per_sec={} MiB/s, row_from_disk_rows_per_sec={} Rows/s, tx_row_nr_peer={}, rx_row_nr_peer={}",
            round_nr,
            round_nr_fast_path_already_synced,
            round_nr_fast_path_same_combined_hashes,
            round_nr_slow_path,
            rpc_call_nr,
            tx_hashes_nr,
            rx_hashes_nr,
            duration,
            tx_row_nr,
            rx_row_nr,
            tx_row_bytes,
            rx_row_bytes,
            row_from_disk_bytes,
            row_from_disk_nr,
            row_from_disk_bytes_per_sec,
            row_from_disk_rows_per_sec,
            tx_row_nr_peer,
            rx_row_nr_peer);
}

struct repair_options {
    // If primary_range is true, we should perform repair only on this node's
    // primary ranges. The default of false means perform repair on all ranges
    // held by the node. primary_range=true is useful if the user plans to
    // repair all nodes.
    bool primary_range = false;
    // If ranges is not empty, it overrides the repair's default heuristics
    // for determining the list of ranges to repair. In particular, "ranges"
    // overrides the setting of "primary_range".
    dht::token_range_vector ranges;
    // If start_token and end_token are set, they define a range which is
    // intersected with the ranges actually held by this node to decide what
    // to repair.
    sstring start_token;
    sstring end_token;
    // column_families is the list of column families to repair in the given
    // keyspace. If this list is empty (the default), all the column families
    // in this keyspace are repaired
    std::vector<sstring> column_families;
    // hosts specifies the list of known good hosts to repair with this host
    // (note that this host is required to also be on this list). For each
    // range repaired, only the relevant subset of the hosts (holding a
    // replica of this range) is used.
    std::vector<sstring> hosts;
    // data_centers is used to restrict the repair to the local data center.
    // The node starting the repair must be in the data center; Issuing a
    // repair to a data center other than the named one returns an error.
    std::vector<sstring> data_centers;

    repair_options(std::unordered_map<sstring, sstring> options) {
        bool_opt(primary_range, options, PRIMARY_RANGE_KEY);
        ranges_opt(ranges, options, RANGES_KEY);
        list_opt(column_families, options, COLUMNFAMILIES_KEY);
        list_opt(hosts, options, HOSTS_KEY);
        list_opt(data_centers, options, DATACENTERS_KEY);
        // We currently do not support incremental repair. We could probably
        // ignore this option as it is just an optimization, but for now,
        // let's make it an error.
        bool incremental = false;
        bool_opt(incremental, options, INCREMENTAL_KEY);
        if (incremental) {
            throw std::runtime_error("unsupported incremental repair");
        }
        // We do not currently support the distinction between "parallel" and
        // "sequential" repair, and operate the same for both.
        // We don't currently support "dc parallel" parallelism.
        int parallelism = PARALLEL;
        int_opt(parallelism, options, PARALLELISM_KEY);
        if (parallelism != PARALLEL && parallelism != SEQUENTIAL) {
            throw std::runtime_error(format("unsupported repair parallelism: {:d}", parallelism));
        }
        string_opt(start_token, options, START_TOKEN);
        string_opt(end_token, options, END_TOKEN);

        bool trace = false;
        bool_opt(trace, options, TRACE_KEY);
        if (trace) {
            throw std::runtime_error("unsupported trace");
        }
        // Consume, ignore.
        int job_threads;
        int_opt(job_threads, options, JOB_THREADS_KEY);

        // The parsing code above removed from the map options we have parsed.
        // If anything is left there in the end, it's an unsupported option.
        if (!options.empty()) {
            throw std::runtime_error(format("unsupported repair options: {}",
                    options));
        }
    }

    static constexpr const char* PRIMARY_RANGE_KEY = "primaryRange";
    static constexpr const char* PARALLELISM_KEY = "parallelism";
    static constexpr const char* INCREMENTAL_KEY = "incremental";
    static constexpr const char* JOB_THREADS_KEY = "jobThreads";
    static constexpr const char* RANGES_KEY = "ranges";
    static constexpr const char* COLUMNFAMILIES_KEY = "columnFamilies";
    static constexpr const char* DATACENTERS_KEY = "dataCenters";
    static constexpr const char* HOSTS_KEY = "hosts";
    static constexpr const char* TRACE_KEY = "trace";
    static constexpr const char* START_TOKEN = "startToken";
    static constexpr const char* END_TOKEN = "endToken";

    // Settings of "parallelism" option. Numbers must match Cassandra's
    // RepairParallelism enum, which is used by the caller.
    enum repair_parallelism {
        SEQUENTIAL=0, PARALLEL=1, DATACENTER_AWARE=2
    };

private:
    static void bool_opt(bool& var,
            std::unordered_map<sstring, sstring>& options,
            const sstring& key) {
        auto it = options.find(key);
        if (it != options.end()) {
            // Same parsing as Boolean.parseBoolean does:
            if (boost::algorithm::iequals(it->second, "true")) {
                var = true;
            } else {
                var = false;
            }
            options.erase(it);
        }
    }

    static void int_opt(int& var,
            std::unordered_map<sstring, sstring>& options,
            const sstring& key) {
        auto it = options.find(key);
        if (it != options.end()) {
            errno = 0;
            var = strtol(it->second.c_str(), nullptr, 10);
            if (errno) {
                throw(std::runtime_error(format("cannot parse integer: '{}'", it->second)));
            }
            options.erase(it);
        }
    }

    static void string_opt(sstring& var,
            std::unordered_map<sstring, sstring>& options,
            const sstring& key) {
        auto it = options.find(key);
        if (it != options.end()) {
            var = it->second;
            options.erase(it);
        }
    }

    // A range is expressed as start_token:end token and multiple ranges can
    // be given as comma separated ranges(e.g. aaa:bbb,ccc:ddd).
    static void ranges_opt(dht::token_range_vector& var,
            std::unordered_map<sstring, sstring>& options,
                        const sstring& key) {
        auto it = options.find(key);
        if (it == options.end()) {
            return;
        }
        std::vector<sstring> range_strings;
        boost::split(range_strings, it->second, boost::algorithm::is_any_of(","));
        for (auto range : range_strings) {
            std::vector<sstring> token_strings;
            boost::split(token_strings, range, boost::algorithm::is_any_of(":"));
            if (token_strings.size() != 2) {
                throw(std::runtime_error("range must have two components "
                        "separated by ':', got '" + range + "'"));
            }
            auto tok_start = dht::global_partitioner().from_sstring(token_strings[0]);
            auto tok_end = dht::global_partitioner().from_sstring(token_strings[1]);
            auto rng = wrapping_range<dht::token>(
                    ::range<dht::token>::bound(tok_start, false),
                    ::range<dht::token>::bound(tok_end, true));
            ::compat::unwrap_into(std::move(rng), dht::token_comparator(), [&] (dht::token_range&& x) {
                var.push_back(std::move(x));
            });
        }
        options.erase(it);
    }

    // A comma-separate list of strings
    static void list_opt(std::vector<sstring>& var,
            std::unordered_map<sstring, sstring>& options,
                        const sstring& key) {
        auto it = options.find(key);
        if (it == options.end()) {
            return;
        }
        std::vector<sstring> range_strings;
        boost::split(var, it->second, boost::algorithm::is_any_of(","));
        options.erase(it);
    }
};


static thread_local semaphore ranges_parallelism_semaphore(16);

static future<> do_repair_ranges(lw_shared_ptr<repair_info> ri) {
    if (ri->row_level_repair()) {
        // repair all the ranges in limited parallelism
        return parallel_for_each(ri->ranges, [ri] (auto&& range) {
            return with_semaphore(ranges_parallelism_semaphore, 1, [ri, &range] {
                check_in_shutdown();
                ri->check_in_abort();
                ri->ranges_index++;
                rlogger.info("Repair {} out of {} ranges, id={}, shard={}, keyspace={}, table={}, range={}",
                    ri->ranges_index, ri->ranges.size(), ri->id, ri->shard, ri->keyspace, ri->cfs, range);
                return repair_range(*ri, range);
            });
        });
    } else {
        // repair all the ranges in sequence
        return do_for_each(ri->ranges, [ri] (auto&& range) {
            ri->check_in_abort();
            ri->ranges_index++;
            rlogger.info("Repair {} out of {} ranges, id={}, shard={}, keyspace={}, table={}, range={}",
                ri->ranges_index, ri->ranges.size(), ri->id, ri->shard, ri->keyspace, ri->cfs, range);
            return do_with(dht::selective_token_range_sharder(range, ri->shard), [ri] (auto& sharder) {
                return repeat([ri, &sharder] () {
                    check_in_shutdown();
                    ri->check_in_abort();
                    auto range_shard = sharder.next();
                    if (range_shard) {
                        return repair_range(*ri, *range_shard).then([] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    } else {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                });
            });
        }).then([ri] {
            // Do streaming for the remaining ranges we do not stream in
            // repair_cf_range
            ri->check_in_abort();
            return ri->do_streaming();
        });
    }
}

// repair_ranges repairs a list of token ranges, each assumed to be a token
// range for which this node holds a replica, and, importantly, each range
// is assumed to be a indivisible in the sense that all the tokens in has the
// same nodes as replicas.
static future<> repair_ranges(lw_shared_ptr<repair_info> ri) {
    repair_tracker.add_repair_info(ri->id, ri);
    return do_repair_ranges(ri).then([ri] {
        ri->check_failed_ranges();
        repair_tracker.remove_repair_info(ri->id);
        return make_ready_future<>();
    }).handle_exception([ri] (std::exception_ptr eptr) {
        rlogger.info("repair {} failed - {}", ri->id, eptr);
        repair_tracker.remove_repair_info(ri->id);
        return make_exception_future<>(std::move(eptr));
    });
}

// repair_start() can run on any cpu; It runs on cpu0 the function
// do_repair_start(). The benefit of always running that function on the same
// CPU is that it allows us to keep some state (like a list of ongoing
// repairs). It is fine to always do this on one CPU, because the function
// itself does very little (mainly tell other nodes and CPUs what to do).
static int do_repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options_map) {
    check_in_shutdown();

    repair_options options(options_map);

    // Note: Cassandra can, in some cases, decide immediately that there is
    // nothing to repair, and return 0. "nodetool repair" prints in this case
    // that "Nothing to repair for keyspace '...'". We don't have such a case
    // yet. Real ids returned by next_repair_command() will be >= 1.
    int id = repair_tracker.next_repair_command();
    rlogger.info("starting user-requested repair for keyspace {}, repair id {}, options {}", keyspace, id, options_map);
    repair_tracker.start(id);
    auto fail = defer([id] { repair_tracker.done(id, false); });

    if (!gms::get_local_gossiper().is_normal(utils::fb_utilities::get_broadcast_address())) {
        throw std::runtime_error("Node is not in NORMAL status yet!");
    }

    // If the "ranges" option is not explicitly specified, we repair all the
    // local ranges (the token ranges for which this node holds a replica of).
    // Each of these ranges may have a different set of replicas, so the
    // repair of each range is performed separately with repair_range().
    dht::token_range_vector ranges;
    if (options.ranges.size()) {
        ranges = options.ranges;
    } else if (options.primary_range) {
        rlogger.info("primary-range repair");
        // when "primary_range" option is on, neither data_centers nor hosts
        // may be set, except data_centers may contain only local DC (-local)
        if (options.data_centers.size() == 1 &&
            options.data_centers[0] == get_local_dc()) {
            ranges = get_primary_ranges_within_dc(db.local(), keyspace);
        } else if (options.data_centers.size() > 0 || options.hosts.size() > 0) {
            throw std::runtime_error("You need to run primary range repair on all nodes in the cluster.");
        } else {
            ranges = get_primary_ranges(db.local(), keyspace);
        }
    } else {
        ranges = get_local_ranges(db.local(), keyspace);
    }

    if (!options.data_centers.empty() && !options.hosts.empty()) {
        throw std::runtime_error("Cannot combine data centers and hosts options.");
    }

    if (!options.start_token.empty() || !options.end_token.empty()) {
        // Intersect the list of local ranges with the given token range,
        // dropping ranges with no intersection.
        // We don't have a range::intersect() method, but we can use
        // range::subtract() and subtract the complement range.
        std::optional<::range<dht::token>::bound> tok_start;
        std::optional<::range<dht::token>::bound> tok_end;
        if (!options.start_token.empty()) {
            tok_start = ::range<dht::token>::bound(
                dht::global_partitioner().from_sstring(options.start_token),
                true);
        }
        if (!options.end_token.empty()) {
            tok_end = ::range<dht::token>::bound(
                dht::global_partitioner().from_sstring(options.end_token),
                false);
        }
        dht::token_range given_range_complement(tok_end, tok_start);
        dht::token_range_vector intersections;
        for (const auto& range : ranges) {
            auto rs = range.subtract(given_range_complement,
                    dht::token_comparator());
            intersections.insert(intersections.end(), rs.begin(), rs.end());
        }
        ranges = std::move(intersections);
    }

    std::vector<sstring> cfs;
    if (options.column_families.size()) {
        cfs = options.column_families;
        for (auto& cf : cfs) {
            try {
                db.local().find_column_family(keyspace, cf);
            } catch(...) {
                throw std::runtime_error(format("No column family '{}' in keyspace '{}'", cf, keyspace));
            }
        }
    } else {
        cfs = list_column_families(db.local(), keyspace);
    }


    std::vector<future<>> repair_results;
    repair_results.reserve(smp::count);

    for (auto shard : boost::irange(unsigned(0), smp::count)) {
        auto f = db.invoke_on(shard, [keyspace, cfs, id, ranges,
                data_centers = options.data_centers, hosts = options.hosts] (database& localdb) mutable {
            auto ri = make_lw_shared<repair_info>(service::get_local_storage_service().db(),
                    std::move(keyspace), std::move(ranges), std::move(cfs),
                    id, std::move(data_centers), std::move(hosts));
            return repair_ranges(ri);
        });
        repair_results.push_back(std::move(f));
    }

    when_all(repair_results.begin(), repair_results.end()).then([id, fail = std::move(fail)] (std::vector<future<>> results) mutable {
        if (std::any_of(results.begin(), results.end(), [] (auto&& f) { return f.failed(); })) {
            rlogger.info("repair {} failed", id);
        } else {
            fail.cancel();
            repair_tracker.done(id, true);
            rlogger.info("repair {} completed successfully", id);
        }
        for (auto& f : results) {
            f.ignore_ready_future();
        }
        return make_ready_future<>();
    }).handle_exception([id] (std::exception_ptr eptr) {
         rlogger.info("repair {} failed: {}", id, eptr);
    });

    return id;
}

future<int> repair_start(seastar::sharded<database>& db, sstring keyspace,
        std::unordered_map<sstring, sstring> options) {
    return db.invoke_on(0, [&db, keyspace = std::move(keyspace), options = std::move(options)] (database& localdb) {
        return do_repair_start(db, std::move(keyspace), std::move(options));
    });
}

future<std::vector<int>> get_active_repairs(seastar::sharded<database>& db) {
    return db.invoke_on(0, [] (database& localdb) {
        return repair_tracker.get_active();
    });
}

future<repair_status> repair_get_status(seastar::sharded<database>& db, int id) {
    return db.invoke_on(0, [id] (database& localdb) {
        return repair_tracker.get(id);
    });
}

future<> repair_shutdown(seastar::sharded<database>& db) {
    rlogger.info("Starting shutdown of repair");
    return db.invoke_on(0, [] (database& localdb) {
        return repair_tracker.shutdown().then([] {
            return shutdown_all_row_level_repair();
        }).then([] {
            rlogger.info("Completed shutdown of repair");
        });
    });
}

future<> repair_abort_all(seastar::sharded<database>& db) {
    return db.invoke_on_all([] (database& localdb) {
        repair_tracker.abort_all_repairs();
    });
}
