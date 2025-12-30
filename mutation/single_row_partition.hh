/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "schema/schema.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "mutation/partition_version.hh"
#include "mutation/tombstone.hh"
#include "reader_permit.hh"
#include "mutation_partition.hh"
#include "utils/lru.hh"

#include <seastar/util/backtrace.hh>

#include <fmt/format.h>

#include <type_traits>

/// Determines format of partition entry in memtable and row_cache.
/// For a given table, all partitions use the same format within a given scylla process.
/// Different servers may use different format.
enum class partition_format {
    // Handles large partitions with many rows well.
    // With MVCC.
    // rows entries linked into LRU, partition entry evicted when the last row is evicted.
    // Partitions may be incomplete in cache (tracks clustering range continuity).
    generic,

    // For schema without clustering columns.
    // No MVCC.
    // partition entry (single_row_partition) linked into LRU.
    // Partitions always complete in cache (no continuity tracking).
    single_row
};

inline
partition_format get_partition_format(const schema& s) {
    // The single_row format stores a single clustering-key-less row and has no place
    // for a static row. A schema may legally have static columns without a clustering
    // key at the schema/mutation level (although CQL disallows it), so such schemas
    // must use the generic format.
    return (s.clustering_key_size() || s.has_static_columns())
        ? partition_format::generic : partition_format::single_row;
}

// Always-false helper for the unreachable else-arm of an exhaustive
// `if constexpr` over partition_format, so that an unhandled format fails
// to compile rather than silently falling through.
template <partition_format>
inline constexpr bool dependent_false = false;

// A simplified mutation_partition object for schemas without a clustering key.
// LSA-managed object.
// Mutators should run under owning allocator.
class single_row_partition final : public evictable {
    schema_ptr _s;
    tombstone _partition_tombstone;
    deletable_row _row;

    // Upgrades to new_schema in place. Must run under the owning allocator.
    void upgrade_impl(schema_ptr new_schema);
public:
    using fragments_vector = utils::small_vector<mutation_fragment_v2, 3>;

    single_row_partition(schema_ptr);
    single_row_partition(schema_ptr, const mutation_partition&);
    single_row_partition(schema_ptr, mutation_partition&&);
    single_row_partition(single_row_partition&&) noexcept = default;
    single_row_partition(const single_row_partition&) = delete;

    // Memory usage excluding memory taken directly by this object.
    // Run under owning allocator. The allocation_strategy argument is part of the
    // uniform storage interface (mirrors partition_entry::external_memory_usage);
    // the single_row format's external memory does not depend on it.
    size_t external_memory_usage(allocation_strategy&) const {
        return _row.cells().external_memory_usage(*_s, column_kind::regular_column);
    }

    void on_evicted() noexcept override;
    void on_evicted_shallow() noexcept override {}

    // Frees elements of this entry.
    // Returns stop_iteration::yes iff there are no more elements to free.
    stop_iteration clear_gently(cache_tracker*) noexcept;

    deletable_row& row() { return _row; }
    const deletable_row& row() const { return _row; }
    tombstone partition_tombstone() const { return _partition_tombstone; }
    void set_partition_tombstone(tombstone t) { _partition_tombstone = t; }
    const schema_ptr& get_schema() const { return _s; }
    fragments_vector as_fragments(const dht::decorated_key& key, reader_permit, bool digest_requested = false) const;
    bool empty() const { return _row.empty() && !_partition_tombstone; }

    // No exception guarantees.
    void apply(const schema& mp_schema, const mutation_partition& mp, mutation_application_stats& app_stats);

    // No exception guarantees.
    void apply(const schema& mp_schema, mutation_partition&& mp, mutation_application_stats& app_stats);

    // Weak exception guarantees. After exception, p may be partially applied, but both this and p will
    // commute to the same value as they would, should the exception not happen.
    void apply_monotonically(single_row_partition&& p);

    // Strong exception guarantees.
    // Runs allocations and frees under the owning region's allocator,
    // so it is safe to call from contexts which only hold a reclaim lock
    // on the region (e.g. allocating_section) without the region's
    // allocator being the current allocator.
    void upgrade(logalloc::region&, schema_ptr new_schema);

    // --- Uniform partition-storage interface used by row_cache/memtable_entry ---
    // These mirror methods of partition_entry so that generic code can operate on
    // either storage format through a single `(auto&)` lambda.

    // single_row_partition has no MVCC snapshots, so it is never locked.
    bool is_locked() const { return false; }

    // Mirrors partition_entry::upgrade(region, schema, cleaner, tracker). The
    // cleaner/tracker arguments are unused; single_row upgrade needs only the region.
    void upgrade(logalloc::region& r, schema_ptr new_schema, mutation_cleaner&, cache_tracker*) {
        upgrade(r, std::move(new_schema));
    }

    // Removes this entry from the tracker (it is directly LRU-linked).
    void evict(cache_tracker&) noexcept;
    // Moves this entry to the front of the tracker's LRU.
    void touch(cache_tracker&);
    // Detaches this entry from the tracker's LRU, so it is not evicted by the reclaimer.
    void unlink_from_lru(cache_tracker&);

    // Debug printer, mirrors partition_entry::printer.
    class printer {
        const single_row_partition& _srp;
    public:
        explicit printer(const single_row_partition& srp) : _srp(srp) { }
        printer(const printer&) = delete;
        printer(printer&&) = delete;

        friend fmt::formatter<printer>;
    };
    friend fmt::formatter<printer>;
};

/// Maps a partition_format to the concrete in-memory partition storage type.
/// Used to give cache_entry/memtable_entry a statically-sized storage member,
/// so that a table pays only for the format it actually uses.
template <partition_format F> struct partition_storage_for;
template <> struct partition_storage_for<partition_format::generic> { using type = partition_entry; };
template <> struct partition_storage_for<partition_format::single_row> { using type = single_row_partition; };
template <partition_format F> using partition_storage_t = typename partition_storage_for<F>::type;

template <> struct fmt::formatter<single_row_partition::printer> {
    constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
    auto format(const single_row_partition::printer&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
