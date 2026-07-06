/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "mutation/partition_version.hh"

class schema;

/// Determines format of partition entry in memtable and row_cache.
/// For a given table, all partitions use the same format within a given scylla process.
/// Different servers may use different format.
enum class partition_format {
    // Handles large partitions with many rows well.
    // With MVCC.
    // rows entries linked into LRU, partition entry evicted when the last row is evicted.
    // Partitions may be incomplete in cache (tracks clustering range continuity).
    generic,
};

inline
partition_format get_partition_format(const schema&) {
    return partition_format::generic;
}

// Always-false helper for the unreachable else-arm of an exhaustive
// `if constexpr` over partition_format, so that an unhandled format fails
// to compile rather than silently falling through.
template <partition_format>
inline constexpr bool dependent_false = false;

/// Maps a partition_format to the concrete in-memory partition storage type.
/// Used to give cache_entry/memtable_entry a statically-sized storage member,
/// so that a table pays only for the format it actually uses.
template <partition_format F> struct partition_storage_for;
template <> struct partition_storage_for<partition_format::generic> { using type = partition_entry; };
template <partition_format F> using partition_storage_t = typename partition_storage_for<F>::type;
