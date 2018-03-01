
Introduction
------------

This document describes how eviction in row cache works. It assumes some familiarity with the mutation model and MVCC.

Cache is always paired with its underlying mutation source which it mirrors. That means that from the outside it appears as containing the same set of writes. Internally, it keeps a subset of data in memory, together with information about which parts are missing. Elements which are fully represented are called "complete". Complete ranges of elements are called "continuous".

Eviction is about removing parts of the data from memory and recording the fact that information about those parts is missing. Eviction doesn't change the set of writes represented by cache as part of its `mutation_source` interface.

Eviction granularity
--------------------

The smallest object which can be evicted, called eviction unit, is currently a single row (`rows_entry`). Eviction units are linked in an LRU owned by a `cache_tracker`. The LRU determines eviction order. The LRU is shared among many tables, currently there is one per `database`.

All `rows_entry` objects which are owned by a `cache_tracker` are assumed to be either contained in cache (in some `row_cache::partitions_type`) or
be owned by a (detached) `partition_snapshot`. When the last row from given `partition_entry` is evicted, the containing `cache_entry` is evicted from cache. `partition_snapshots` go away on their own, but eviction can make them contain no rows.

range tombstones are currently not evictable (to be fixed), they go away together with the whole partition.

`rows_entry` objects in memtables are not owned by a `cache_tracker`, they are not evictable. Data referenced by `partition_snapshots` created on non-evictable partition entries is not transferred to cache, so unevictable snapshots are not made evictable.

Maintaining snapshot consistency
--------------------------------

When removing a `rows_entry` (=r1), we need to record the fact that the range to which this row belongs is now discontinuous. For a single `mutation_partition` that would be done by going to the successor of r1 (=r2) and setting its `continuous` flag to `false`, which would indicate that the range between r1's predecessor and r2 is incomplete. With many partition versions, in order for snapshots's logical `mutation_partition` to remain correct, special constraints on version contents and merging rules must apply as described below.

When `rows_entry` is selected for eviction, we only have a reference to that object. We want to be able to evict it without having to update entries in other versions, to avoid associated overheads of locating sibling versions and lookups inside them. To support that, each `partition_version` has its own continuity, fully specified in that version, independent of continuity of other versions. Row continuity of the snapshot is a union of continuous sets from all versions. This is the **independent-continuity** rule.

We also need continuous row intervals in different versions to be non-overlapping, except for points corresponding to complete rows. A row may overlap with another row, in which case the row from the later version completely overrides the one from the older version. A later version may have a row which falls into a continuous interval in the older version. A newer version cannot have a continuous interval with no rows which covers a row in the older version. This is the **no-overlap** rule. We make use of this rule to make calculation of the union of intervals on merging easier, because merging of newer version into older has then time complexity of only O(size(new_version)). If we allowed for overlaps, we might have to walk over all entries in the old version to mark ranges between them as continuous.

Another rule is that row entries in **older versions are evicted first**, before any row in the newer version is evicted. This is needed so that we don't appear to loose writes in case we have the same row in more than one version, and the one from the newer version gets evicted first. To achieve this, we only move to the front of the LRU row entries which belong to the latest `partition_version` in given `partition_entry`. This implies that detached snapshots never update the LRU.

The above rules have consequences for range population. When populating a discontinuous range which is adjacent to an existing row entry in older version, we need to insert an entry for the bound (due to independent-continuity rule), and need to satisfy the no-overlap rule in one of the following ways:
  1) copy complete row entry from older version into the latest version
  2) insert a dummy entry in the latest version for position before(key).
  3) add support for incomplete row entries, and insert an incomplete entry in the latest version

Options (3) and (2) are more efficient than (1), but (1) is the simplest to implement, so it was chosen. With option (2) we have additional problem of cleaning up the extra dummy entries when the versions are finally merged. Option (3) makes cache reader more complicated.

Each `partition_version` always has a dummy entry at `position_in_partition::after_all_clustering_rows()`, so that its row range can be marked as fully discontinuous when all of its rows get evicted. Note that we can't remove fully evicted non-latest versions, because they may contain range tombstones and static row versions, which are needed to calculate snapshot's view on those elements. We can't merge them into newer versions in reclamation context due to no-allocation requirement, and because they could be referenced by snapshots.
