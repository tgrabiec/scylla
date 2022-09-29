/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <boost/range/adaptor/reversed.hpp>
#include "mutation_partition_v2.hh"
#include "clustering_interval_set.hh"
#include "converting_mutation_partition_applier.hh"
#include "partition_builder.hh"
#include "query-result-writer.hh"
#include "atomic_cell_hash.hh"
#include "reversibly_mergeable.hh"
#include "mutation_fragment.hh"
#include "mutation_query.hh"
#include "service/priority_manager.hh"
#include "mutation_compactor.hh"
#include "counters.hh"
#include "row_cache.hh"
#include "view_info.hh"
#include "mutation_cleaner.hh"
#include <seastar/core/execution_stage.hh>
#include "types/map.hh"
#include "compaction/compaction_garbage_collector.hh"
#include "utils/exceptions.hh"
#include "clustering_key_filter.hh"
#include "mutation_partition_view.hh"
#include "tombstone_gc.hh"

extern logging::logger mplog;

template<bool reversed>
struct reversal_traits;

template<>
struct reversal_traits<false> {
    template <typename Container>
    static auto begin(Container& c) {
        return c.begin();
    }

    template <typename Container>
    static auto end(Container& c) {
        return c.end();
    }

    template <typename Container, typename Disposer>
    static typename Container::iterator erase_and_dispose(Container& c,
        typename Container::iterator begin,
        typename Container::iterator end,
        Disposer disposer)
    {
        return c.erase_and_dispose(begin, end, std::move(disposer));
    }

    template<typename Container, typename Disposer>
    static typename Container::iterator erase_dispose_and_update_end(Container& c,
         typename Container::iterator it, Disposer&& disposer,
         typename Container::iterator&)
    {
        return c.erase_and_dispose(it, std::forward<Disposer>(disposer));
    }

    template <typename Container>
    static boost::iterator_range<typename Container::iterator> maybe_reverse(
        Container& c, boost::iterator_range<typename Container::iterator> r)
    {
        return r;
    }

    template <typename Container>
    static typename Container::iterator maybe_reverse(Container&, typename Container::iterator r) {
        return r;
    }
};

template<>
struct reversal_traits<true> {
    template <typename Container>
    static auto begin(Container& c) {
        return c.rbegin();
    }

    template <typename Container>
    static auto end(Container& c) {
        return c.rend();
    }

    template <typename Container, typename Disposer>
    static typename Container::reverse_iterator erase_and_dispose(Container& c,
        typename Container::reverse_iterator begin,
        typename Container::reverse_iterator end,
        Disposer disposer)
    {
        return typename Container::reverse_iterator(
            c.erase_and_dispose(end.base(), begin.base(), disposer)
        );
    }

    // Erases element pointed to by it and makes sure than iterator end is not
    // invalidated.
    template<typename Container, typename Disposer>
    static typename Container::reverse_iterator erase_dispose_and_update_end(Container& c,
        typename Container::reverse_iterator it, Disposer&& disposer,
        typename Container::reverse_iterator& end)
    {
        auto to_erase = std::next(it).base();
        bool update_end = end.base() == to_erase;
        auto ret = typename Container::reverse_iterator(
            c.erase_and_dispose(to_erase, std::forward<Disposer>(disposer))
        );
        if (update_end) {
            end = ret;
        }
        return ret;
    }

    template <typename Container>
    static boost::iterator_range<typename Container::reverse_iterator> maybe_reverse(
        Container& c, boost::iterator_range<typename Container::iterator> r)
    {
        using reverse_iterator = typename Container::reverse_iterator;
        return boost::make_iterator_range(reverse_iterator(r.end()), reverse_iterator(r.begin()));
    }

    template <typename Container>
    static typename Container::reverse_iterator maybe_reverse(Container&, typename Container::iterator r) {
        return typename Container::reverse_iterator(r);
    }
};

mutation_partition_v2::mutation_partition_v2(const schema& s, const mutation_partition_v2& x)
        : _tombstone(x._tombstone)
        , _static_row(s, column_kind::static_column, x._static_row)
        , _static_row_continuous(x._static_row_continuous)
        , _rows()
#ifdef SEASTAR_DEBUG
        , _schema_version(s.version())
#endif
{
#ifdef SEASTAR_DEBUG
    assert(x._schema_version == _schema_version);
#endif
    auto cloner = [&s] (const rows_entry* x) -> rows_entry* {
        return current_allocator().construct<rows_entry>(s, *x);
    };
    _rows.clone_from(x._rows, cloner, current_deleter<rows_entry>());
}

mutation_partition_v2::mutation_partition_v2(const schema& s, mutation_partition&& x)
    : _tombstone(x.partition_tombstone())
    , _static_row(std::move(x.static_row()))
    , _static_row_continuous(x.static_row_continuous())
    , _rows(std::move(x.mutable_clustered_rows()))
{
    auto&& tombstones = x.mutable_row_tombstones();
    if (!tombstones.empty()) {
        mutation_partition_v2 p(s.shared_from_this());

        for (auto&& t : tombstones) {
            range_tombstone& rt = t.tombstone();
            p.clustered_rows_entry(s, rt.position(), is_dummy::yes, is_continuous::no);
            p.clustered_rows_entry(s, rt.end_position(), is_dummy::yes, is_continuous::yes)
                .set_range_tombstone(rt.tomb);
        }

        mutation_application_stats app_stats;
        apply_monotonically(s, std::move(p), s, app_stats);
    }
}

mutation_partition_v2::mutation_partition_v2(const schema& s, const mutation_partition& x)
    : mutation_partition_v2(s, mutation_partition(s, x))
{ }

mutation_partition_v2::~mutation_partition_v2() {
    _rows.clear_and_dispose(current_deleter<rows_entry>());
}

mutation_partition_v2&
mutation_partition_v2::operator=(mutation_partition_v2&& x) noexcept {
    if (this != &x) {
        this->~mutation_partition_v2();
        new (this) mutation_partition_v2(std::move(x));
    }
    return *this;
}

void mutation_partition_v2::ensure_last_dummy(const schema& s) {
    check_schema(s);
    if (_rows.empty() || !_rows.rbegin()->is_last_dummy()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
                current_allocator().construct<rows_entry>(s, rows_entry::last_dummy_tag(), is_continuous::yes));
        _rows.insert_before(_rows.end(), std::move(e));
    }
}

struct mutation_fragment_applier {
    const schema& _s;
    mutation_partition_v2& _mp;

    void operator()(tombstone t) {
        _mp.apply(t);
    }

    void operator()(range_tombstone rt) {
        _mp.apply_row_tombstone(_s, std::move(rt));
    }

    void operator()(const static_row& sr) {
        _mp.static_row().apply(_s, column_kind::static_column, sr.cells());
    }

    void operator()(partition_start ps) {
        _mp.apply(ps.partition_tombstone());
    }

    void operator()(partition_end ps) {
    }

    void operator()(const clustering_row& cr) {
        auto temp = clustering_row(_s, cr);
        auto& dr = _mp.clustered_row(_s, std::move(temp.key()));
        dr.apply(_s, std::move(temp).as_deletable_row());
    }
};

stop_iteration mutation_partition_v2::apply_monotonically(const schema& s, mutation_partition_v2&& p, cache_tracker* tracker,
        mutation_application_stats& app_stats, is_preemptible preemptible, apply_resume& res) {
#ifdef SEASTAR_DEBUG
    assert(s.version() == _schema_version);
    assert(p._schema_version == _schema_version);
#endif
    _tombstone.apply(p._tombstone);
    app_stats.has_any_tombstones |= bool(_tombstone);
    _static_row.apply_monotonically(s, column_kind::static_column, std::move(p._static_row));
    _static_row_continuous |= p._static_row_continuous;

    rows_entry::tri_compare cmp(s);
    auto del = current_deleter<rows_entry>();

    // Erases the entry if it's safe to do so without changing the logical state of the partition.
    auto maybe_drop = [&] (rows_type::iterator i) -> rows_type::iterator {
        rows_entry& e = *i;
        auto next_i = std::next(i);

        if (!e.row().empty() || e.is_last_dummy()) {
            return next_i;
        }

        // Pass only if continuity is the same on both sides and
        // range tombstones for the intervals are the same on both sides (if intervals are continuous).
        bool next_continuous = next_i == _rows.end() || next_i->continuous();
        if (e.continuous() && next_continuous) {
            tombstone next_range_tombstone = (next_i == _rows.end() ? tombstone{} : next_i->range_tombstone());
            if (e.range_tombstone() != next_range_tombstone) {
                return next_i;
            }
        } else if (!e.continuous() && !next_continuous) {
            if (!e.dummy() && next_i->range_tombstone()) {
                return next_i;
            }
        } else {
            return next_i;
        }

        ++app_stats.rows_dropped_by_tombstones; // FIXME: it's more general than that now

        i = _rows.erase(i);
        if (tracker) {
            tracker->on_remove();
        }
        del(&e);
        return next_i;
    };

    // Compacts a given row taking into account tombstones at all levels.
    // Does not affect logical state of the partition.
    auto compact = [&] (rows_entry& e) {
        can_gc_fn never_gc = [](tombstone) { return false; };
        ++app_stats.rows_compacted_with_tombstones;
        e.row().compact_and_expire(s, _tombstone + e.range_tombstone(),
             gc_clock::time_point::min(),  // no TTL expiration
             never_gc,                     // no GC
             gc_clock::time_point::min()); // no GC
        // FIXME: Compact range tombstones
    };

    if (p._tombstone) {
        rows_type::iterator i;
        if (res._stage == apply_resume::stage::partition_tombstone_compaction) {
            i = _rows.upper_bound(res._pos, cmp);
        } else {
            i = _rows.begin();
        }
        auto prev_i = (i == _rows.begin()) ? rows_type::iterator() : std::prev(i);
        while (i != _rows.end()) {
            compact(*i);
            if (prev_i) {
                maybe_drop(prev_i);
            }
            if (preemptible && need_preempt() && i != _rows.end()) {
                res = apply_resume(apply_resume::stage::partition_tombstone_compaction, i->position());
                return stop_iteration::no;
            }
            prev_i = i;
            ++i;
        }
        if (prev_i != _rows.end()) {
            maybe_drop(prev_i);
        }
        // TODO: Drop redundant range tombstones
        p._tombstone = {};
    }

    // Inserting new entries into LRU here is generally unsafe because
    // it may violate the "older versions are evicted first" rule (see row_cache.md).
    // It could happen, that there are newer versions in the MVCC chain with the same
    // key, not involved in this merge. Inserting an entry here would put this
    // entry ahead in the LRU, and the newer entry could get evicted earlier leading
    // to apparent loss of writes.
    // To avoid this, when inserting sentinels we must use lru::add_before() so that
    // they are put right before in the same place in the LRU.

    // Note: This procedure is seemingly violating the "older versions are evicted first" rule
    // because it may move some entries from the newer version into the old version,
    // so the older version may have entries while the new version is already experiencing
    // eviction. However, the original information which was there in the old version
    // is guaranteed to be evicted prior to that, so there is no way for old information
    // to be exposed by such eviction.

    auto p_i = p._rows.begin();
    auto i = _rows.begin();
    rows_type::iterator lb_i = i;

    if (res._stage < apply_resume::stage::merging_rows) {
        res = apply_resume::merging_rows();
    }

    bool prev_compacted = false;
    bool made_progress = false;

    while (p_i != p._rows.end()) {
        rows_entry& src_e = *p_i;

        bool miss = true;
        if (i != _rows.end()) {
            auto x = cmp(*i, src_e);
            if (x < 0) {
                bool match;
                i = _rows.lower_bound(src_e, match, cmp);
                miss = !match;
            } else {
                miss = x > 0;
            }
        }

        // Invariants:
        //   i->position() >= p_i->position()

        // The block below reflects the information from interval (std::prev(p_i)->position(), p_i->position()) to _rows,
        // up to the last entry in p which has position() < p_i->position(). The remainder is reflected by the act of
        // moving p_i itself.
        // FIXME: This will be very inefficient for non-evictable snapshots.
        bool prev_interval_loaded = bool(src_e.continuous());
        // bool prev_interval_loaded = (evictable && src_e.continuous()) || (!evictable && src_e.range_tombstone());
        if (prev_interval_loaded) {
            rows_type::iterator prev_lb_i;

            // Handle resuming
            if (lb_i != _rows.end() && !res._pos.is_before_all_clustered_rows(s) && cmp(*lb_i, res._pos) <= 0) {
                lb_i = _rows.upper_bound(res._pos, cmp);
                if (lb_i != _rows.begin()) {
                    prev_lb_i = std::prev(lb_i);
                }
            } else if (p_i != p._rows.begin()) {
                // If there is a predecessor to p_i, it means the interval starts exactly at lb_i in p.
                // Increment is needed, we don't want to set attributes on the lower bound of the interval.
                prev_lb_i = lb_i;
                ++lb_i;
            } else {
                if (lb_i != _rows.begin()) {
                    prev_lb_i = std::prev(lb_i);
                }
            }

            while (lb_i != i) {
                bool compaction_worthwhile = src_e.range_tombstone() > lb_i->range_tombstone();

                // This works for both evictable and non-evictable snapshots.
                // For evictable snapshots we could replace the tombstone with newer, but due to
                // the "information monotonicity" rule, adding tombstone works too.
                lb_i->set_range_tombstone(lb_i->range_tombstone() + src_e.range_tombstone());
                lb_i->set_continuous(true);

                if (preemptible && need_preempt()) {
                    res.set_position(lb_i->position());
                    return stop_iteration::no;
                }

                if (prev_compacted && prev_lb_i) {
                    maybe_drop(prev_lb_i);
                }

                prev_compacted = false;
                if (compaction_worthwhile) {
                    compact(*lb_i);
                    prev_compacted = true;
                }

                prev_lb_i = lb_i;
                ++lb_i;
            }

            // Signal that resuming is not needed for the next interval.
            res.set_position(position_in_partition::before_all_clustered_rows());
        }

        auto next_p_i = std::next(p_i);

        // p_i will not be removed from p in this iteration when next_interval_loaded.
        // because there are attributes in the next interval which need p_i as a lower bound entry.
        // In this case, this iteration's p_i will be removed after the next interval is fully transferred.
        // FIXME: This will be very inefficient for non-evictable snapshots.
        bool next_interval_loaded = next_p_i != p._rows.end()
                && (next_p_i->continuous());
//        bool next_interval_loaded = next_p_i != p._rows.end()
//                && ((evictable && next_p_i->continuous()) || (!evictable && next_p_i->range_tombstone()));

        bool do_compact = false;
        if (miss) {
            {
                app_stats.has_any_tombstones |= bool(p_i->row().deleted_at()) || bool(p_i->range_tombstone());
                if (!next_interval_loaded) {
                    rows_type::key_grabber pi_kg(p_i);
                    lb_i = _rows.insert_before(i, std::move(pi_kg));
                } else {
                    // Need to leave p_i for the next interval
                    auto e = alloc_strategy_unique_ptr<rows_entry>(
                            current_allocator().construct<rows_entry>(s, src_e.position(), src_e.dummy(), src_e.continuous()));
                    rows_entry& re = *e;
                    lb_i = _rows.insert_before(i, std::move(e));
                    if (tracker) {
                        tracker->insert(re, src_e);
                    }
                    re.set_range_tombstone(src_e.range_tombstone());
                    re.row() = std::move(src_e.row());
                }
                if (i != _rows.end() && i->continuous()) {
                    // Cannot apply only-row range tombstone falling into a continuous range without inserting extra entry.
                    // Should not occur in practice due to the "older versions are evicted first" rule.
                    // Never occurs in non-evictable snapshots because they are continuous.
                    if (!src_e.continuous() && src_e.range_tombstone() > i->range_tombstone()) {
                        // See the "no singular tombstones" rule.
                        mplog.error("Cannot merge entry {} with rt={}, cont=0 into continuous range before {} with rt={}",
                                    src_e.position(), src_e.range_tombstone(), i->position(), i->range_tombstone());
                        abort();
                    }
                    lb_i->set_range_tombstone(src_e.range_tombstone() + i->range_tombstone());
                    lb_i->set_continuous(true);
                }
            }
        } else {
            assert(i->dummy() == src_e.dummy());
            {
                // FIXME: This can be an evictable snapshot even if !tracker, see partition_version::squashed()
                // So we need to handle continuity as if it was an evictable snapshot.
                if (i->continuous()) {
                    if (src_e.range_tombstone() > i->range_tombstone()) {
                        // Cannot apply range tombstone in such a case.
                        // Should not occur in practice due to the "older versions are evicted first" rule.
                        if (!src_e.continuous()) {
                            // See the "no singular tombstones" rule.
                            mplog.error("Cannot merge entry {} with rt={}, cont=0 into an entry which has rt={}, cont=1",
                                    src_e.position(), src_e.range_tombstone(), i->range_tombstone());
                            abort();
                        }
                        i->set_range_tombstone(i->range_tombstone() + src_e.range_tombstone());
                    }
                } else {
                    i->set_continuous(src_e.continuous());
                    // FIXME: ok?
                    i->set_range_tombstone(i->range_tombstone() + src_e.range_tombstone());
                }
            }
            if (tracker) {
                // Newer evictable versions store complete rows
                i->row() = std::move(src_e.row());
            } else {
                // Avoid row compaction if no newer range tombstone.
                do_compact = (src_e.range_tombstone() + src_e.row().deleted_at().regular()) >
                            (i->range_tombstone() + i->row().deleted_at().regular());
                memory::on_alloc_point();
                i->row().apply_monotonically(s, std::move(src_e.row()));
            }
            if (!next_interval_loaded) {
                if (tracker) {
                    tracker->on_remove();
                }
                p_i = p._rows.erase_and_dispose(p_i, del);
            }
            lb_i = i;
            app_stats.has_any_tombstones |= bool(p_i->row().deleted_at()) || p_i->range_tombstone();
            ++app_stats.row_hits;
        }
        if (p_i != p._rows.begin()) {
            p._rows.erase_and_dispose(std::prev(p_i), del);
            made_progress = true;
            assert(p_i == p._rows.begin());
        }
        if (next_interval_loaded) {
            // Make previous interval not loaded by clearing attributes.
            // This indicates we're done with it in case of preemption or retry.
            src_e.set_continuous(false);
            src_e.set_range_tombstone({});
            ++p_i;
        }
        // All operations above up to each insert_before() must be noexcept.
        if (prev_compacted && lb_i != _rows.begin()) {
            maybe_drop(std::prev(lb_i));
        }
        if (lb_i->dummy()) {
            prev_compacted = true;
        } else if (do_compact) {
            compact(*lb_i);
            prev_compacted = true;
        } else {
            prev_compacted = false;
        }
        ++app_stats.row_writes;
        if (made_progress && need_preempt() && p_i != p._rows.end()) {
            return stop_iteration::no;
        }
    }
    if (prev_compacted && lb_i != _rows.end()) {
        maybe_drop(lb_i);
    }
    return stop_iteration::yes;
}

stop_iteration mutation_partition_v2::apply_monotonically(const schema& s, mutation_partition_v2&& p, const schema& p_schema,
        mutation_application_stats& app_stats, is_preemptible preemptible, apply_resume& res) {
    if (s.version() == p_schema.version()) {
        return apply_monotonically(s, std::move(p), no_cache_tracker, app_stats, preemptible, res);
    } else {
        mutation_partition_v2 p2(s, p);
        p2.upgrade(p_schema, s);
        return apply_monotonically(s, std::move(p2), no_cache_tracker, app_stats, is_preemptible::no, res); // FIXME: make preemptible
    }
}

stop_iteration mutation_partition_v2::apply_monotonically(const schema& s, mutation_partition_v2&& p, cache_tracker *tracker,
                                                       mutation_application_stats& app_stats) {
    apply_resume res;
    return apply_monotonically(s, std::move(p), tracker, app_stats, is_preemptible::no, res);
}

stop_iteration mutation_partition_v2::apply_monotonically(const schema& s, mutation_partition_v2&& p, const schema& p_schema,
                                                       mutation_application_stats& app_stats) {
    apply_resume res;
    return apply_monotonically(s, std::move(p), p_schema, app_stats, is_preemptible::no, res);
}

void mutation_partition_v2::apply(const schema& s, const mutation_partition_v2& p, const schema& p_schema,
                               mutation_application_stats& app_stats) {
    apply_monotonically(s, mutation_partition_v2(p_schema, std::move(p)), p_schema, app_stats);
}

void mutation_partition_v2::apply(const schema& s, mutation_partition_v2&& p, mutation_application_stats& app_stats) {
    apply_monotonically(s, mutation_partition_v2(s, std::move(p)), no_cache_tracker, app_stats);
}

void
mutation_partition_v2::apply_weak(const schema& s, mutation_partition_view p,
                                  const schema& p_schema, mutation_application_stats& app_stats) {
    // FIXME: Optimize
    mutation_partition p2(p_schema.shared_from_this());
    partition_builder b(p_schema, p2);
    p.accept(p_schema, b);
    apply_monotonically(s, mutation_partition_v2(p_schema, std::move(p2)), p_schema, app_stats);
}

void mutation_partition_v2::apply_weak(const schema& s, const mutation_partition& p,
                                       const schema& p_schema, mutation_application_stats& app_stats) {
    // FIXME: Optimize
    apply_monotonically(s, mutation_partition_v2(s, p), p_schema, app_stats);
}

void mutation_partition_v2::apply_weak(const schema& s, mutation_partition&& p, mutation_application_stats& app_stats) {
    apply_monotonically(s, mutation_partition_v2(s, std::move(p)), no_cache_tracker, app_stats);
}

void
mutation_partition_v2::apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t) {
    check_schema(schema);
    assert(!prefix.is_full(schema));
    auto start = prefix;
    apply_row_tombstone(schema, range_tombstone{std::move(start), std::move(prefix), std::move(t)});
}

void
mutation_partition_v2::apply_row_tombstone(const schema& schema, range_tombstone rt) {
    check_schema(schema);
    mutation_partition mp(schema.shared_from_this());
    mp.apply_row_tombstone(schema, std::move(rt));
    mutation_application_stats stats;
    apply_weak(schema, std::move(mp), stats);
}

void
mutation_partition_v2::apply_delete(const schema& schema, const clustering_key_prefix& prefix, tombstone t) {
    check_schema(schema);
    if (prefix.is_empty(schema)) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        clustered_row(schema, prefix).apply(t);
    } else {
        apply_row_tombstone(schema, prefix, t);
    }
}

void
mutation_partition_v2::apply_delete(const schema& schema, range_tombstone rt) {
    check_schema(schema);
    if (range_tombstone::is_single_clustering_row_tombstone(schema, rt.start, rt.start_kind, rt.end, rt.end_kind)) {
        apply_delete(schema, std::move(rt.start), std::move(rt.tomb));
        return;
    }
    apply_row_tombstone(schema, std::move(rt));
}

void
mutation_partition_v2::apply_delete(const schema& schema, clustering_key&& prefix, tombstone t) {
    check_schema(schema);
    if (prefix.is_empty(schema)) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        clustered_row(schema, std::move(prefix)).apply(t);
    } else {
        apply_row_tombstone(schema, std::move(prefix), t);
    }
}

void
mutation_partition_v2::apply_delete(const schema& schema, clustering_key_prefix_view prefix, tombstone t) {
    check_schema(schema);
    if (prefix.is_empty(schema)) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        clustered_row(schema, prefix).apply(t);
    } else {
        apply_row_tombstone(schema, prefix, t);
    }
}

void
mutation_partition_v2::apply_insert(const schema& s, clustering_key_view key, api::timestamp_type created_at) {
    clustered_row(s, key).apply(row_marker(created_at));
}
void mutation_partition_v2::apply_insert(const schema& s, clustering_key_view key, api::timestamp_type created_at,
        gc_clock::duration ttl, gc_clock::time_point expiry) {
    clustered_row(s, key).apply(row_marker(created_at, ttl, expiry));
}
void mutation_partition_v2::insert_row(const schema& s, const clustering_key& key, deletable_row&& row) {
    auto e = alloc_strategy_unique_ptr<rows_entry>(
        current_allocator().construct<rows_entry>(key, std::move(row)));
    _rows.insert_before_hint(_rows.end(), std::move(e), rows_entry::tri_compare(s));
}

void mutation_partition_v2::insert_row(const schema& s, const clustering_key& key, const deletable_row& row) {
    check_schema(s);
    auto e = alloc_strategy_unique_ptr<rows_entry>(
        current_allocator().construct<rows_entry>(s, key, row));
    _rows.insert_before_hint(_rows.end(), std::move(e), rows_entry::tri_compare(s));
}

const row*
mutation_partition_v2::find_row(const schema& s, const clustering_key& key) const {
    check_schema(s);
    auto i = _rows.find(key, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        return nullptr;
    }
    return &i->row().cells();
}

deletable_row&
mutation_partition_v2::clustered_row(const schema& s, clustering_key&& key) {
    check_schema(s);
    auto i = _rows.find(key, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(std::move(key)));
        i = _rows.insert_before_hint(i, std::move(e), rows_entry::tri_compare(s)).first;
    }
    return i->row();
}

deletable_row&
mutation_partition_v2::clustered_row(const schema& s, const clustering_key& key) {
    check_schema(s);
    auto i = _rows.find(key, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(key));
        i = _rows.insert_before_hint(i, std::move(e), rows_entry::tri_compare(s)).first;
    }
    return i->row();
}

deletable_row&
mutation_partition_v2::clustered_row(const schema& s, clustering_key_view key) {
    check_schema(s);
    auto i = _rows.find(key, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(key));
        i = _rows.insert_before_hint(i, std::move(e), rows_entry::tri_compare(s)).first;
    }
    return i->row();
}

rows_entry&
mutation_partition_v2::clustered_rows_entry(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous) {
    check_schema(s);
    auto i = _rows.find(pos, rows_entry::tri_compare(s));
    if (i == _rows.end()) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(s, pos, dummy, continuous));
        i = _rows.insert_before_hint(i, std::move(e), rows_entry::tri_compare(s)).first;
    }
    return *i;
}

deletable_row&
mutation_partition_v2::clustered_row(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous) {
    return clustered_rows_entry(s, pos, dummy, continuous).row();
}

rows_entry&
mutation_partition_v2::clustered_row(const schema& s, position_in_partition_view pos, is_dummy dummy) {
    check_schema(s);
    auto cmp = rows_entry::tri_compare(s);
    auto i = _rows.lower_bound(pos, cmp);
    if (i == _rows.end() || cmp(i->position(), pos) != 0) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(s, pos, dummy, is_continuous::no));
        if (i != _rows.end()) {
            e->set_continuous(i->continuous());
            e->set_range_tombstone(i->range_tombstone());
        }
        i = _rows.insert_before_hint(i, std::move(e), rows_entry::tri_compare(s)).first;
    }
    return *i;
}

deletable_row&
mutation_partition_v2::append_clustered_row(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous) {
    check_schema(s);
    const auto cmp = rows_entry::tri_compare(s);
    auto i = _rows.end();
    if (!_rows.empty() && (cmp(*std::prev(i), pos) >= 0)) {
        throw std::runtime_error(format("mutation_partition_v2::append_clustered_row(): cannot append clustering row with key {} to the partition"
                ", last clustering row is equal or greater: {}", pos, std::prev(i)->key()));
    }
    auto e = alloc_strategy_unique_ptr<rows_entry>(current_allocator().construct<rows_entry>(s, pos, dummy, continuous));
    i = _rows.insert_before_hint(i, std::move(e), cmp).first;

    return i->row();
}

mutation_partition_v2::rows_type::const_iterator
mutation_partition_v2::lower_bound(const schema& schema, const query::clustering_range& r) const {
    check_schema(schema);
    if (!r.start()) {
        return std::cbegin(_rows);
    }
    return _rows.lower_bound(position_in_partition_view::for_range_start(r), rows_entry::tri_compare(schema));
}

mutation_partition_v2::rows_type::const_iterator
mutation_partition_v2::upper_bound(const schema& schema, const query::clustering_range& r) const {
    check_schema(schema);
    if (!r.end()) {
        return std::cend(_rows);
    }
    return _rows.lower_bound(position_in_partition_view::for_range_end(r), rows_entry::tri_compare(schema));
}

boost::iterator_range<mutation_partition_v2::rows_type::const_iterator>
mutation_partition_v2::range(const schema& schema, const query::clustering_range& r) const {
    check_schema(schema);
    return boost::make_iterator_range(lower_bound(schema, r), upper_bound(schema, r));
}

template <typename Container>
boost::iterator_range<typename Container::iterator>
unconst(Container& c, boost::iterator_range<typename Container::const_iterator> r) {
    return boost::make_iterator_range(
            c.erase(r.begin(), r.begin()),
            c.erase(r.end(), r.end())
    );
}

template <typename Container>
typename Container::iterator
unconst(Container& c, typename Container::const_iterator i) {
    return c.erase(i, i);
}

boost::iterator_range<mutation_partition_v2::rows_type::iterator>
mutation_partition_v2::range(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition_v2*>(this)->range(schema, r));
}

mutation_partition_v2::rows_type::iterator
mutation_partition_v2::lower_bound(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition_v2*>(this)->lower_bound(schema, r));
}

mutation_partition_v2::rows_type::iterator
mutation_partition_v2::upper_bound(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition_v2*>(this)->upper_bound(schema, r));
}

template<typename Func>
void mutation_partition_v2::for_each_row(const schema& schema, const query::clustering_range& row_range, bool reversed, Func&& func) const
{
    check_schema(schema);
    auto r = range(schema, row_range);
    if (!reversed) {
        for (const auto& e : r) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    } else {
        for (const auto& e : r | boost::adaptors::reversed) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    }
}

// Transforms given range of printable into a range of strings where each element
// in the original range is prefxied with given string.
template<typename RangeOfPrintable>
static auto prefixed(const sstring& prefix, const RangeOfPrintable& r) {
    return r | boost::adaptors::transformed([&] (auto&& e) { return format("{}{}", prefix, e); });
}

std::ostream&
operator<<(std::ostream& os, const mutation_partition_v2::printer& p) {
    const auto indent = "  ";

    auto& mp = p._mutation_partition;
    os << "mutation_partition_v2: {\n";
    if (mp._tombstone) {
        os << indent << "tombstone: " << mp._tombstone << ",\n";
    }

    if (!mp.static_row().empty()) {
        os << indent << "static_row: {\n";
        const auto& srow = mp.static_row().get();
        srow.for_each_cell([&] (column_id& c_id, const atomic_cell_or_collection& cell) {
            auto& column_def = p._schema.column_at(column_kind::static_column, c_id);
            os << indent << indent <<  "'" << column_def.name_as_text() 
               << "': " << atomic_cell_or_collection::printer(column_def, cell) << ",\n";
        }); 
        os << indent << "},\n";
    }

    os << indent << "rows: [\n";

    for (const auto& re : mp.clustered_rows()) {
        os << indent << indent << "{\n";

        const auto& row = re.row();
        os << indent << indent << indent << "cont: " << re.continuous() << ",\n";
        os << indent << indent << indent << "dummy: " << re.dummy() << ",\n";
        if (!row.marker().is_missing()) {
            os << indent << indent << indent << "marker: " << row.marker() << ",\n";
        }
        if (row.deleted_at()) {
            os << indent << indent << indent << "tombstone: " << row.deleted_at() << ",\n";
        }
        if (re.range_tombstone()) {
            os << indent << indent << indent << "rt: " << re.range_tombstone() << ",\n";
        }

        position_in_partition pip(re.position());
        if (pip.get_clustering_key_prefix()) {
            os << indent << indent << indent << "position: {\n";

            auto ck = *pip.get_clustering_key_prefix();
            auto type_iterator = ck.get_compound_type(p._schema)->types().begin();
            auto column_iterator = p._schema.clustering_key_columns().begin();

            os << indent << indent << indent << indent << "bound_weight: " << int32_t(pip.get_bound_weight()) << ",\n";

            for (auto&& e : ck.components(p._schema)) {
                os << indent << indent << indent << indent << "'" << column_iterator->name_as_text() 
                   << "': " << (*type_iterator)->to_string(to_bytes(e)) << ",\n";
                ++type_iterator;
                ++column_iterator;
            }

            os << indent << indent << indent << "},\n";
        }

        row.cells().for_each_cell([&] (column_id& c_id, const atomic_cell_or_collection& cell) {
            auto& column_def = p._schema.column_at(column_kind::regular_column, c_id);
            os << indent << indent << indent <<  "'" << column_def.name_as_text() 
               << "': " << atomic_cell_or_collection::printer(column_def, cell) << ",\n";
        });

        os << indent << indent << "},\n";
    }

    os << indent << "]\n}";

    return os;
}

bool mutation_partition_v2::equal(const schema& s, const mutation_partition_v2& p) const {
    return equal(s, p, s);
}

bool mutation_partition_v2::equal(const schema& this_schema, const mutation_partition_v2& p, const schema& p_schema) const {
#ifdef SEASTAR_DEBUG
    assert(_schema_version == this_schema.version());
    assert(p._schema_version == p_schema.version());
#endif
    if (_tombstone != p._tombstone) {
        return false;
    }

    if (!boost::equal(non_dummy_rows(), p.non_dummy_rows(),
        [&] (const rows_entry& e1, const rows_entry& e2) {
            return e1.equal(this_schema, e2, p_schema);
        }
    )) {
        return false;
    }

    return _static_row.equal(column_kind::static_column, this_schema, p._static_row, p_schema);
}

bool mutation_partition_v2::equal_continuity(const schema& s, const mutation_partition_v2& p) const {
    return _static_row_continuous == p._static_row_continuous
        && get_continuity(s).equals(s, p.get_continuity(s));
}

size_t mutation_partition_v2::external_memory_usage(const schema& s) const {
    check_schema(s);
    size_t sum = 0;
    sum += static_row().external_memory_usage(s, column_kind::static_column);
    sum += clustered_rows().external_memory_usage();
    for (auto& clr : clustered_rows()) {
        sum += clr.memory_usage(s);
    }

    return sum;
}

// Returns true if the mutation_partition_v2 represents no writes.
bool mutation_partition_v2::empty() const
{
    if (_tombstone.timestamp != api::missing_timestamp) {
        return false;
    }
    return !_static_row.size() && _rows.empty();
}

bool
mutation_partition_v2::is_static_row_live(const schema& s, gc_clock::time_point query_time) const {
    check_schema(s);
    return has_any_live_data(s, column_kind::static_column, static_row().get(), _tombstone, query_time);
}

uint64_t
mutation_partition_v2::row_count() const {
    return _rows.calculate_size();
}

void mutation_partition_v2::accept(const schema& s, mutation_partition_visitor& v) const {
    check_schema(s);
    v.accept_partition_tombstone(_tombstone);
    _static_row.for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        const column_definition& def = s.static_column_at(id);
        if (def.is_atomic()) {
            v.accept_static_cell(id, cell.as_atomic_cell(def));
        } else {
            v.accept_static_cell(id, cell.as_collection_mutation());
        }
    });
    std::optional<position_in_partition> prev_pos;
    for (const rows_entry& e : _rows) {
        const deletable_row& dr = e.row();
        if (e.range_tombstone()) {
            if (!e.continuous()) {
                v.accept_row_tombstone(range_tombstone(position_in_partition::before_key(e.position()),
                                                       position_in_partition::after_key(e.position()),
                                                       e.range_tombstone()));
            } else {
                v.accept_row_tombstone(range_tombstone(prev_pos ? position_in_partition::after_key(*prev_pos)
                                                                : position_in_partition::before_all_clustered_rows(),
                                                       position_in_partition::after_key(e.position()),
                                                       e.range_tombstone()));
            }
        }
        v.accept_row(e.position(), dr.deleted_at(), dr.marker(), e.dummy(), e.continuous());
        dr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
            const column_definition& def = s.regular_column_at(id);
            if (def.is_atomic()) {
                v.accept_row_cell(id, cell.as_atomic_cell(def));
            } else {
                v.accept_row_cell(id, cell.as_collection_mutation());
            }
        });
        prev_pos = e.position();
    }
}

void
mutation_partition_v2::upgrade(const schema& old_schema, const schema& new_schema) {
    // We need to copy to provide strong exception guarantees.
    mutation_partition tmp(new_schema.shared_from_this());
    tmp.set_static_row_continuous(_static_row_continuous);
    converting_mutation_partition_applier v(old_schema.get_column_mapping(), new_schema, tmp);
    accept(old_schema, v);
    *this = mutation_partition_v2(new_schema, std::move(tmp));
}

mutation_partition mutation_partition_v2::as_mutation_partition(const schema& s) const {
    mutation_partition tmp(s.shared_from_this());
    tmp.set_static_row_continuous(_static_row_continuous);
    partition_builder v(s, tmp);
    accept(s, v);
    return tmp;
}

mutation_partition_v2::mutation_partition_v2(mutation_partition_v2::incomplete_tag, const schema& s, tombstone t)
    : _tombstone(t)
    , _static_row_continuous(!s.has_static_columns())
    , _rows()
#ifdef SEASTAR_DEBUG
    , _schema_version(s.version())
#endif
{
    auto e = alloc_strategy_unique_ptr<rows_entry>(
            current_allocator().construct<rows_entry>(s, rows_entry::last_dummy_tag(), is_continuous::no));
    _rows.insert_before(_rows.end(), std::move(e));
}

bool mutation_partition_v2::is_fully_continuous() const {
    if (!_static_row_continuous) {
        return false;
    }
    for (auto&& row : _rows) {
        if (!row.continuous()) {
            return false;
        }
    }
    return true;
}

void mutation_partition_v2::make_fully_continuous() {
    _static_row_continuous = true;
    auto i = _rows.begin();
    while (i != _rows.end()) {
        i->set_continuous(true);
        ++i;
    }
}

void mutation_partition_v2::set_continuity(const schema& s, const position_range& pr, is_continuous cont) {
    auto cmp = rows_entry::tri_compare(s);

    if (cmp(pr.start(), pr.end()) >= 0) {
        return; // empty range
    }

    auto end = _rows.lower_bound(pr.end(), cmp);
    if (end == _rows.end() || cmp(pr.end(), end->position()) < 0) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
                current_allocator().construct<rows_entry>(s, pr.end(), is_dummy::yes,
                    end == _rows.end() ? is_continuous::yes : end->continuous()));
        end = _rows.insert_before(end, std::move(e));
    }

    auto i = _rows.lower_bound(pr.start(), cmp);
    if (cmp(pr.start(), i->position()) < 0) {
        auto e = alloc_strategy_unique_ptr<rows_entry>(
                current_allocator().construct<rows_entry>(s, pr.start(), is_dummy::yes, i->continuous()));
        i = _rows.insert_before(i, std::move(e));
    }

    assert(i != end);
    ++i;

    while (1) {
        i->set_continuous(cont);
        if (i == end) {
            break;
        }
        ++i;
    }
}

clustering_interval_set mutation_partition_v2::get_continuity(const schema& s, is_continuous cont) const {
    check_schema(s);
    clustering_interval_set result;
    auto i = _rows.begin();
    auto prev_pos = position_in_partition::before_all_clustered_rows();
    while (i != _rows.end()) {
        if (i->continuous() == cont) {
            result.add(s, position_range(std::move(prev_pos), position_in_partition::after_key(i->position())));
        }
        prev_pos = position_in_partition::after_key(i->position());
        ++i;
    }
    if (cont) {
        result.add(s, position_range(std::move(prev_pos), position_in_partition::after_all_clustered_rows()));
    }
    return result;
}

stop_iteration mutation_partition_v2::clear_gently(cache_tracker* tracker) noexcept {
    auto del = current_deleter<rows_entry>();
    auto i = _rows.begin();
    auto end = _rows.end();
    while (i != end) {
        if (tracker) {
            tracker->on_remove();
        }
        i = _rows.erase_and_dispose(i, del);

        // The iterator comparison below is to not defer destruction of now empty
        // mutation_partition_v2 objects. Not doing this would cause eviction to leave garbage
        // versions behind unnecessarily.
        if (need_preempt() && i != end) {
            return stop_iteration::no;
        }
    }

    return stop_iteration::yes;
}

bool
mutation_partition_v2::check_continuity(const schema& s, const position_range& r, is_continuous cont) const {
    check_schema(s);
    auto cmp = rows_entry::tri_compare(s);
    auto i = _rows.lower_bound(r.start(), cmp);
    auto end = _rows.lower_bound(r.end(), cmp);
    if (cmp(r.start(), r.end()) >= 0) {
        return bool(cont);
    }
    if (i != end) {
        if (no_clustering_row_between(s, r.start(), i->position())) {
            ++i;
        }
        while (i != end) {
            if (i->continuous() != cont) {
                return false;
            }
            ++i;
        }
        if (end != _rows.begin() && no_clustering_row_between(s, std::prev(end)->position(), r.end())) {
            return true;
        }
    }
    return (end == _rows.end() ? is_continuous::yes : end->continuous()) == cont;
}

bool
mutation_partition_v2::fully_continuous(const schema& s, const position_range& r) {
    return check_continuity(s, r, is_continuous::yes);
}

bool
mutation_partition_v2::fully_discontinuous(const schema& s, const position_range& r) {
    return check_continuity(s, r, is_continuous::no);
}
