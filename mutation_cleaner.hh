/*
 * Copyright (C) 2018 ScyllaDB
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

#include "partition_version.hh"
#include "partition_version_list.hh"

#include "utils/logalloc.hh"

// Container for garbage partition_version objects, used for freeing them incrementally.
class mutation_cleaner final {
    using snapshot_list = boost::intrusive::slist<partition_snapshot,
        boost::intrusive::member_hook<partition_snapshot, boost::intrusive::slist_member_hook<>, &partition_snapshot::_cleaner_hook>>;
    struct worker {
        condition_variable cv;
        snapshot_list snapshots;
        logalloc::allocating_section alloc_section;
        bool done = false; // true means the worker was abandoned and cannot access the mutation_cleaner instance.
    };
private:
    logalloc::region& _region;
    cache_tracker* _tracker;
    partition_version_list _versions;
    lw_shared_ptr<worker> _worker_state;
private:
    stop_iteration merge_some(partition_snapshot& snp) noexcept;
    stop_iteration merge_some() noexcept;
    void start_worker();
public:
    mutation_cleaner(logalloc::region& r, cache_tracker* t)
        : _region(r)
        , _tracker(t)
        , _worker_state(make_lw_shared<worker>())
    {
        start_worker();
    }

    ~mutation_cleaner();

    // Frees some of the data. Returns stop_iteration::yes iff all was freed.
    // Must be invoked under owning allocator.
    stop_iteration clear_gently() noexcept;

    // Must be invoked under owning allocator.
    memory::reclaiming_result clear_some() noexcept;

    // Must be invoked under owning allocator.
    void clear() noexcept;

    // Enqueues v for destruction.
    // The object must not be part of any list, and must not be accessed externally any more.
    // In particular, it must not be attached, even indirectly, to any snapshot or partition_entry,
    // and must not be evicted from.
    // Must be invoked under owning allocator.
    void destroy_later(partition_version& v) noexcept;

    // Destroys v now or later.
    // Same requirements as destroy_later().
    // Must be invoked under owning allocator.
    void destroy_gently(partition_version& v) noexcept;

    // Transfers objects from other to this.
    // This and other must belong to the same logalloc::region, and the same cache_tracker.
    // After the call bool(other) is false.
    void merge(mutation_cleaner& other) noexcept;

    // Returns true iff contains no unfreed objects
    bool empty() const noexcept { return _versions.empty(); }

    // Forces cleaning and returns a future which resolves when there is nothing to clean.
    future<> drain();

    void merge_and_destroy(partition_snapshot& ps) noexcept {
        if (ps.slide_to_oldest() == stop_iteration::yes || merge_some(ps) == stop_iteration::yes) {
            lw_shared_ptr<partition_snapshot>::dispose(&ps);
        } else {
            // The snapshot must not be reachable by partitino_entry::read() after this,
            // which is ensured by slide_to_oldest() == stop_iteration::no.
            _worker_state->snapshots.push_front(ps);
            _worker_state->cv.signal();
        }
    }
};

inline
void mutation_cleaner::destroy_later(partition_version& v) noexcept {
    _versions.push_back(v);
}

inline
void mutation_cleaner::destroy_gently(partition_version& v) noexcept {
    if (v.clear_gently(_tracker) == stop_iteration::no) {
        destroy_later(v);
    } else {
        current_allocator().destroy(&v);
    }
}
