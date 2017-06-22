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

#pragma once

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

#include "core/memory.hh"
#include <seastar/core/thread.hh>

#include "mutation_reader.hh"
#include "mutation_partition.hh"
#include "utils/logalloc.hh"
#include "utils/phased_barrier.hh"
#include "utils/histogram.hh"
#include "partition_version.hh"
#include "utils/estimated_histogram.hh"
#include "tracing/trace_state.hh"
#include <seastar/core/metrics_registration.hh>

namespace bi = boost::intrusive;

class row_cache;
class memtable_entry;

namespace cache {

class autoupdating_underlying_reader;
class cache_streamed_mutation;
class read_context;
class lsa_manager;

}

// Intrusive set entry which holds partition data.
//
// TODO: Make memtables use this format too.
class cache_entry {
    // We need auto_unlink<> option on the _cache_link because when entry is
    // evicted from cache via LRU we don't have a reference to the container
    // and don't want to store it with each entry. As for the _lru_link, we
    // have a global LRU, so technically we could not use auto_unlink<> on
    // _lru_link, but it's convenient to do so too. We may also want to have
    // multiple eviction spaces in the future and thus multiple LRUs.
    using lru_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
    using cache_link_type = bi::set_member_hook<bi::link_mode<bi::auto_unlink>>;

    schema_ptr _schema;
    dht::decorated_key _key;
    partition_entry _pe;
    // True when we know that there is nothing between this entry and the next one in cache
    struct {
        bool _continuous : 1;
        bool _dummy_entry : 1;
    } _flags{};
    lru_link_type _lru_link;
    cache_link_type _cache_link;
    friend class size_calculator;

public:
    friend class row_cache;
    friend class cache_tracker;

    struct dummy_entry_tag{};
    struct incomplete_tag{};

    cache_entry(dummy_entry_tag)
        : _key{dht::token(), partition_key::make_empty()}
    {
        _flags._dummy_entry = true;
    }

    // Creates an entry which is fully discontinuous, except for the partition tombstone.
    cache_entry(incomplete_tag, schema_ptr s, const dht::decorated_key& key, tombstone t)
        : cache_entry(s, key, mutation_partition::make_incomplete(*s, t))
    { }

    cache_entry(schema_ptr s, const dht::decorated_key& key, const mutation_partition& p)
        : _schema(std::move(s))
        , _key(key)
        , _pe(p)
    {
        _pe.version()->partition().ensure_last_dummy(*_schema);
    }

    cache_entry(schema_ptr s, dht::decorated_key&& key, mutation_partition&& p) noexcept
        : _schema(std::move(s))
        , _key(std::move(key))
        , _pe(std::move(p))
    {
        _pe.version()->partition().ensure_last_dummy(*_schema);
    }

    // It is assumed that pe is fully continuous
    cache_entry(schema_ptr s, dht::decorated_key&& key, partition_entry&& pe) noexcept
        : _schema(std::move(s))
        , _key(std::move(key))
        , _pe(std::move(pe))
    {
        // If we can assume that _pe is fully continuous, we don't need to check all versions
        // to determine what the continuity is.
        // This doesn't change value and doesn't invalidate iterators, so can be called even with a snapshot.
        _pe.version()->partition().ensure_last_dummy(*_schema);
    }

    cache_entry(cache_entry&&) noexcept;

    bool is_evictable() { return _lru_link.is_linked(); }
    const dht::decorated_key& key() const { return _key; }
    dht::ring_position_view position() const {
        if (is_dummy_entry()) {
            return dht::ring_position_view::max();
        }
        return _key;
    }
    const partition_entry& partition() const { return _pe; }
    partition_entry& partition() { return _pe; }
    const schema_ptr& schema() const { return _schema; }
    schema_ptr& schema() { return _schema; }
    streamed_mutation read(row_cache&, cache::read_context& reader);
    bool continuous() const { return _flags._continuous; }
    void set_continuous(bool value) { _flags._continuous = value; }

    bool is_dummy_entry() const { return _flags._dummy_entry; }

    struct compare {
        dht::ring_position_less_comparator _c;

        compare(schema_ptr s)
            : _c(*s)
        {}

        bool operator()(const dht::decorated_key& k1, const cache_entry& k2) const {
            return _c(k1, k2.position());
        }

        bool operator()(dht::ring_position_view k1, const cache_entry& k2) const {
            return _c(k1, k2.position());
        }

        bool operator()(const cache_entry& k1, const cache_entry& k2) const {
            return _c(k1.position(), k2.position());
        }

        bool operator()(const cache_entry& k1, const dht::decorated_key& k2) const {
            return _c(k1.position(), k2);
        }

        bool operator()(const cache_entry& k1, dht::ring_position_view k2) const {
            return _c(k1.position(), k2);
        }

        bool operator()(dht::ring_position_view k1, dht::ring_position_view k2) const {
            return _c(k1, k2);
        }
    };

    friend std::ostream& operator<<(std::ostream&, cache_entry&);
};

// Tracks accesses and performs eviction of cache entries.
class cache_tracker final {
public:
    using lru_type = bi::list<cache_entry,
        bi::member_hook<cache_entry, cache_entry::lru_link_type, &cache_entry::_lru_link>,
        bi::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
private:
    // We will try to evict large partition after that many normal evictions
    const uint32_t _normal_large_eviction_ratio = 1000;
    // Number of normal evictions to perform before we try to evict large partition
    uint32_t _normal_eviction_count = _normal_large_eviction_ratio;
public:
    struct stats {
        uint64_t hits;
        uint64_t misses;
        uint64_t insertions;
        uint64_t concurrent_misses_same_key;
        uint64_t merges;
        uint64_t evictions;
        uint64_t removals;
        uint64_t partitions;
        uint64_t modification_count;
        uint64_t mispopulations;
    };
private:
    stats _stats{};
    seastar::metrics::metric_groups _metrics;
    logalloc::region _region;
    lru_type _lru;
private:
    void setup_metrics();
public:
    cache_tracker();
    ~cache_tracker();
    void clear();
    void touch(cache_entry&);
    void insert(cache_entry&);
    void clear_continuity(cache_entry& ce);
    void on_erase();
    void on_merge();
    void on_hit();
    void on_miss();
    void on_miss_already_populated();
    void on_mispopulate();
    allocation_strategy& allocator();
    logalloc::region& region();
    const logalloc::region& region() const;
    uint64_t modification_count() const { return _stats.modification_count; }
    uint64_t partitions() const { return _stats.partitions; }
    const stats& get_stats() const { return _stats; }
};

// Returns a reference to shard-wide cache_tracker.
cache_tracker& global_cache_tracker();

//
// A data source which wraps another data source such that data obtained from the underlying data source
// is cached in-memory in order to serve queries faster.
//
// Cache populates itself automatically during misses.
//
// Cache represents a snapshot of the underlying mutation source. When the
// underlying mutation source changes, cache needs to be explicitly synchronized
// to the latest snapshot. This is done by calling update() or invalidate().
//
class row_cache final {
public:
    using phase_type = utils::phased_barrier::phase_type;
    using partitions_type = bi::set<cache_entry,
        bi::member_hook<cache_entry, cache_entry::cache_link_type, &cache_entry::_cache_link>,
        bi::constant_time_size<false>, // we need this to have bi::auto_unlink on hooks
        bi::compare<cache_entry::compare>>;
    friend class cache::autoupdating_underlying_reader;
    friend class single_partition_populating_reader;
    friend class cache_entry;
    friend class cache::cache_streamed_mutation;
    friend class cache::lsa_manager;
    friend class cache::read_context;
    friend class partition_range_cursor;
public:
    struct stats {
        utils::timed_rate_moving_average hits;
        utils::timed_rate_moving_average misses;
    };
private:
    cache_tracker& _tracker;
    stats _stats{};
    schema_ptr _schema;
    partitions_type _partitions; // Cached partitions are complete.

    // The snapshots used by cache are versioned. The version number of a snapshot is
    // called the "population phase", or simply "phase". Between updates, cache
    // represents the same snapshot.
    //
    // Update doesn't happen atomically. Before it completes, some entries reflect
    // the old snapshot, while others reflect the new snapshot. After update
    // completes, all entries must reflect the new snapshot. There is a race between the
    // update process and populating reads. Since after the update all entries must
    // reflect the new snapshot, reads using the old snapshot cannot be allowed to
    // insert data which will no longer be reached by the update process. The whole
    // range can be therefore divided into two sub-ranges, one which was already
    // processed by the update and one which hasn't. Each key can be assigned a
    // population phase which determines to which range it belongs, as well as which
    // snapshot it reflects. The methods snapshot_of() and phase_of() can
    // be used to determine this.
    //
    // In general, reads are allowed to populate given range only if the phase
    // of the snapshot they use matches the phase of all keys in that range
    // when the population is committed. This guarantees that the range will
    // be reached by the update process or already has been in its entirety.
    // In case of phase conflict, current solution is to give up on
    // population. Since the update process is a scan, it's sufficient to
    // check when committing the population if the start and end of the range
    // have the same phases and that it's the same phase as that of the start
    // of the range at the time when reading began.

    mutation_source _underlying;
    phase_type _underlying_phase = 0;
    mutation_source_opt _prev_snapshot;

    // Positions >= than this are using _prev_snapshot, the rest is using _underlying.
    stdx::optional<dht::ring_position> _prev_snapshot_pos;

    snapshot_source _snapshot_source;

    // There can be at most one update in progress.
    seastar::semaphore _update_sem = {1};

    logalloc::allocating_section _update_section;
    logalloc::allocating_section _populate_section;
    logalloc::allocating_section _read_section;
    mutation_reader create_underlying_reader(cache::read_context&, mutation_source&, const dht::partition_range&);
    mutation_reader make_scanning_reader(const dht::partition_range&, lw_shared_ptr<cache::read_context>);
    void on_hit();
    void on_miss();
    void upgrade_entry(cache_entry&);
    void invalidate_locked(const dht::decorated_key&);
    void invalidate_unwrapped(const dht::partition_range&);
    void clear_now() noexcept;
    static thread_local seastar::thread_scheduling_group _update_thread_scheduling_group;

    struct previous_entry_pointer {
        stdx::optional<dht::decorated_key> _key;

        previous_entry_pointer() = default; // Represents dht::ring_position_view::min()
        previous_entry_pointer(dht::decorated_key key) : _key(std::move(key)) {};

        // TODO: Currently inserting an entry to the cache increases
        // modification counter. That doesn't seem to be necessary and if we
        // didn't do that we could store iterator here to avoid key comparison
        // (not to mention avoiding lookups in just_cache_scanning_reader.
    };

    template<typename CreateEntry, typename VisitEntry>
    //requires requires(CreateEntry create, VisitEntry visit, partitions_type::iterator it) {
    //        { create(it) } -> partitions_type::iterator;
    //        { visit(it) } -> void;
    //    }
    //
    // Must be run under reclaim lock
    cache_entry& do_find_or_create_entry(const dht::decorated_key& key, const previous_entry_pointer* previous,
                                 CreateEntry&& create_entry, VisitEntry&& visit_entry);

    // Ensures that partition entry for given key exists in cache and returns a reference to it.
    // Prepares the entry for reading. "phase" must match the current phase of the entry.
    //
    // Since currently every entry has to have a complete tombstone, it has to be provided here.
    // The entry which is returned will have the tombstone applied to it.
    //
    // Must be run under reclaim lock
    cache_entry& find_or_create(const dht::decorated_key& key, tombstone t, row_cache::phase_type phase, const previous_entry_pointer* previous = nullptr);

    partitions_type::iterator partitions_end() {
        return std::prev(_partitions.end());
    }

    // Only active phases are accepted.
    // Reference valid only until next deferring point.
    mutation_source& snapshot_for_phase(phase_type);

    // Returns population phase for given position in the ring.
    // snapshot_for_phase() can be called to obtain mutation_source for given phase, but
    // only until the next deferring point.
    // Should be only called outside update().
    phase_type phase_of(dht::ring_position_view);

    struct snapshot_and_phase {
        mutation_source& snapshot;
        phase_type phase;
    };

    // Optimized version of:
    //
    //  { snapshot_for_phase(phase_of(pos)), phase_of(pos) };
    //
    snapshot_and_phase snapshot_of(dht::ring_position_view pos);

    // Merges the memtable into cache with configurable logic for handling memtable entries.
    // The Updater gets invoked for every entry in the memtable with a lower bound iterator
    // into _partitions (cache_i), and the memtable entry.
    // It is invoked inside allocating section and in the context of cache's allocator.
    // All memtable entries will be removed.
    template <typename Updater>
    future<> do_update(memtable& m, Updater func);
public:
    ~row_cache();
    row_cache(schema_ptr, snapshot_source, cache_tracker&);
    row_cache(row_cache&&) = default;
    row_cache(const row_cache&) = delete;
    row_cache& operator=(row_cache&&) = default;
public:
    // Implements mutation_source for this cache, see mutation_reader.hh
    // User needs to ensure that the row_cache object stays alive
    // as long as the reader is used.
    // The range must not wrap around.
    mutation_reader make_reader(schema_ptr,
                                const dht::partition_range& = query::full_partition_range,
                                const query::partition_slice& slice = query::full_slice,
                                const io_priority_class& = default_priority_class(),
                                tracing::trace_state_ptr trace_state = nullptr,
                                streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
                                mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no);

    const stats& stats() const { return _stats; }
public:
    // Populate cache from given mutation. The mutation must contain all
    // information there is for its partition in the underlying data sources.
    void populate(const mutation& m, const previous_entry_pointer* previous = nullptr);

    // Synchronizes cache with the underlying data source from a memtable which
    // has just been flushed to the underlying data source.
    // The memtable can be queried during the process, but must not be written.
    // After the update is complete, memtable is empty.
    future<> update(memtable&, partition_presence_checker underlying_negative);

    // Like update(), synchronizes cache with an incremental change to the underlying
    // mutation source, but instead of inserting and merging data, invalidates affected ranges.
    // Can be thought of as a more fine-grained version of invalidate(), which invalidates
    // as few elements as possible.
    future<> update_invalidating(memtable&);

    // Moves given partition to the front of LRU if present in cache.
    void touch(const dht::decorated_key&);

    // Synchronizes cache with the underlying mutation source
    // by invalidating ranges which were modified. This will force
    // them to be re-read from the underlying mutation source
    // during next read overlapping with the invalidated ranges.
    //
    // The ranges passed to invalidate() must include all
    // data which changed since last synchronization. Failure
    // to do so may result in reads seeing partial writes,
    // which would violate write atomicity.
    //
    // Guarantees that readers created after invalidate()
    // completes will see all writes from the underlying
    // mutation source made prior to the call to invalidate().
    future<> invalidate(const dht::decorated_key&);
    future<> invalidate(const dht::partition_range& = query::full_partition_range);
    future<> invalidate(dht::partition_range_vector&&);

    auto num_entries() const {
        return _partitions.size();
    }
    const cache_tracker& get_cache_tracker() const {
        return _tracker;
    }

    void set_schema(schema_ptr) noexcept;
    const schema_ptr& schema() const;

    friend std::ostream& operator<<(std::ostream&, row_cache&);

    friend class just_cache_scanning_reader;
    friend class scanning_and_populating_reader;
    friend class range_populating_reader;
    friend class cache_tracker;
    friend class mark_end_as_continuous;
};
