/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "partition_version.hh"
#include "readers/flat_mutation_reader_v2.hh"
#include "readers/range_tombstone_change_merger.hh"
#include "clustering_key_filter.hh"
#include "query-request.hh"
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <any>

extern seastar::logger mplog;

template <bool Reversing, typename Accounter>
class partition_snapshot_flat_reader : public flat_mutation_reader_v2::impl, public Accounter {
    using rows_iter_type = std::conditional_t<Reversing,
          mutation_partition_v2::rows_type::const_reverse_iterator,
          mutation_partition_v2::rows_type::const_iterator>;

    struct rows_position {
        partition_snapshot::version_number_type _version;
        mutation_partition_v2::rows_type* _rows;
        rows_iter_type _position;
        rows_iter_type _end;
    };

    static rows_iter_type make_iterator(mutation_partition_v2::rows_type::const_iterator it) {
        if constexpr (Reversing) {
            return std::make_reverse_iterator(it);
        } else {
            return it;
        }
    }

    class heap_compare {
        position_in_partition::less_compare _less;
    public:
        // `s` shall be native to the query clustering order.
        explicit heap_compare(const schema& s) : _less(s) { }
        bool operator()(const rows_position& a, const rows_position& b) {
            if constexpr (Reversing) {
                // Here `reversed()` doesn't change anything, but keep it for consistency.
                return _less(b._position->position().reversed(), a._position->position().reversed());
            } else {
                return _less(b._position->position(), a._position->position());
            }
        }
    };

    struct row_info {
        mutation_fragment_v2 row;
        tombstone rt_for_row;
    };

    // Represents a subset of mutations for some clustering key range.
    //
    // The range of the interval starts at the upper bound of the previous
    // interval and its end depends on the contents of info:
    //    - position_in_partition: holds the upper bound of the interval
    //    - row_info: after_key(row_info::row.as_clustering_row().key())
    //    - monostate: upper bound is the end of the current clustering key range
    //
    // All positions in query schema domain.
    struct interval_info {
        // Applies to the whole range of the interval.
        tombstone range_tombstone;

        // monostate means no more rows (end of range).
        // position_in_partition means there is no row, it is the upper bound of the interval.
        // if row_info, the upper bound is after_key(row_info::row.as_clustering_row().key()).
        std::variant<row_info, position_in_partition, std::monostate> info;
    };

    // The part of the reader that accesses LSA memory directly and works
    // with reclamation disabled. The state is either immutable (comparators,
    // snapshot, references to region and alloc section) or dropped on any
    // allocation section retry (_clustering_rows).
    class lsa_partition_reader {
        // _query_schema can be used to retrieve the clustering key order which is used
        // for result ordering. This schema is passed from the query and is reversed iff
        // the query was reversed (i.e. `Reversing==true`).
        const schema& _query_schema;
        // _snapshot_schema is a schema that induces the same clustering key order as the
        // schema from the underlying snapshot. The schemas mentioned might differ, for
        // instance, if a query used newer version of the schema.
        const schema_ptr _snapshot_schema;
        reader_permit _permit;
        heap_compare _heap_cmp;

        partition_snapshot_ptr _snapshot;

        logalloc::region& _region;
        logalloc::allocating_section& _read_section;

        partition_snapshot::change_mark _change_mark;
        std::vector<rows_position> _clustering_rows;

        // Each partition version is a separate stream.
        // Contains tombstones for intervals between row entries,
        // does not contain tombstone for the current row itself.
        // When traversing in reverse, the tombstone for the row
        // may be different from the one for the interval before
        // the row even in the same partition version.
        range_tombstone_change_merger<partition_snapshot::version_number_type> _range_tombstones;

        range_tombstone_stream _rt_stream;

        bool _digest_requested;
    private:
        template<typename Function>
        decltype(auto) in_alloc_section(Function&& fn) {
            return _read_section.with_reclaiming_disabled(_region, [&] {
                return fn();
            });
        }

        void maybe_refresh_state(const query::clustering_range& ck_range_snapshot,
                                 const std::optional<position_in_partition>& last_row) {
            auto mark = _snapshot->get_change_mark();
            if (mark != _change_mark) {
                do_refresh_state(ck_range_snapshot, last_row);
                _change_mark = mark;
            }
        }

        // In reversing mode, upper and lower bounds still need to be executed against
        // snapshot schema and ck_range, however we need them to search from "opposite" direction.
        template<typename T, typename... Args>
        static rows_iter_type lower_bound(const T& t, Args&&... args) {
            if constexpr (Reversing) {
                return make_iterator(t.upper_bound(std::forward<Args>(args)...));
            } else {
                return make_iterator(t.lower_bound(std::forward<Args>(args)...));
            }
        }
        template<typename T, typename... Args>
        static rows_iter_type upper_bound(const T& t, Args&&... args) {
            if constexpr (Reversing) {
                return make_iterator(t.lower_bound(std::forward<Args>(args)...));
            } else {
                return make_iterator(t.upper_bound(std::forward<Args>(args)...));
            }
        }

        void do_refresh_state(const query::clustering_range& ck_range_snapshot,
                              const std::optional<position_in_partition>& last_row) {
            _clustering_rows.clear();
            _range_tombstones.clear();

            rows_entry::tri_compare rows_cmp(*_snapshot_schema);
            partition_snapshot::version_number_type version_no = 0;
            for (auto&& v: _snapshot->versions()) {
                auto cr = [&]() {
                    if (last_row) {
                        return upper_bound(v.partition().clustered_rows(), *last_row, rows_cmp);
                    } else {
                        return lower_bound(v.partition(), *_snapshot_schema, ck_range_snapshot);
                    }
                }();
                auto cr_end = upper_bound(v.partition(), *_snapshot_schema, ck_range_snapshot);

                if (cr != cr_end) {
                    _clustering_rows.emplace_back(rows_position{version_no, &v.partition().mutable_clustered_rows(), cr, cr_end});
                }

                // FIXME: Optimize apply() knowing that version_no is absent
                auto i = [&] {
                    if constexpr (Reversing) {
                        return cr.base();
                    } else {
                        return cr;
                    }
                }();
                if (i != v.partition().clustered_rows().end()) {
                    _range_tombstones.apply(version_no, i->range_tombstone());
                }

                ++version_no;
            }

            boost::range::make_heap(_clustering_rows, _heap_cmp);
        }

        // Valid if has_more_rows()
        const rows_entry& pop_clustering_row() {
            boost::range::pop_heap(_clustering_rows, _heap_cmp);
            auto& current = _clustering_rows.back();
            const rows_entry& e = *current._position;
            auto prev_rt = current._position->range_tombstone();
            current._position = std::next(current._position);
            if constexpr (Reversing) {
                _range_tombstones.apply(current._version, prev_rt);
            } else {
                if (current._position != current._rows->end()) {
                    _range_tombstones.apply(current._version, current._position->range_tombstone());
                } else {
                    _range_tombstones.apply(current._version, {});
                }
            }
            if (current._position == current._end) {
                _clustering_rows.pop_back();
            } else {
                boost::range::push_heap(_clustering_rows, _heap_cmp);
            }
            return e;
        }

        // Valid if has_more_rows()
        const rows_entry& peek_row() const {
            return *_clustering_rows.front()._position;
        }

        bool has_more_rows() const {
            return !_clustering_rows.empty();
        }

    public:
        explicit lsa_partition_reader(const schema& s, reader_permit permit, partition_snapshot_ptr snp,
                                      logalloc::region& region, logalloc::allocating_section& read_section,
                                      bool digest_requested)
                : _query_schema(s), _snapshot_schema(Reversing ? s.make_reversed() : s.shared_from_this()),
                  _permit(permit), _heap_cmp(s), _snapshot(std::move(snp)), _region(region),
                  _read_section(read_section), _rt_stream(s, permit), _digest_requested(digest_requested) {}

        void reset_state(const query::clustering_range& ck_range_snapshot) {
            do_refresh_state(ck_range_snapshot, {});
        }

        template<typename Function>
        decltype(auto) with_reserve(Function&& fn) {
            return _read_section.with_reserve(std::forward<Function>(fn));
        }

        tombstone partition_tombstone() {
            logalloc::reclaim_lock guard(_region);
            return _snapshot->partition_tombstone();
        }

        static_row get_static_row() {
            return in_alloc_section([&] {
                return _snapshot->static_row(_digest_requested);
            });
        }

        // Returns mutations for the next interval in the range.
        // If the ck_range_snapshot is the same as the one used previously last_row needs
        // to be engaged and equal the position of the row returned last time.
        // If the ck_range_snapshot is different or this is the first call to this
        // function last_row has to be disengaged.
        interval_info next_interval(const query::clustering_range& ck_range_snapshot,
                                    const std::optional<position_in_partition>& last_row) {
            return in_alloc_section([&]() -> interval_info {
                maybe_refresh_state(ck_range_snapshot, last_row);

                auto rt_before_row = _range_tombstones.peek();

                mplog.trace("next_interval(): range={}, last_row={}, rt={}", ck_range_snapshot, last_row, rt_before_row);

                if (!has_more_rows()) {
                    mplog.trace("next_interval(): done");
                    return interval_info{rt_before_row, std::monostate{}};
                }

                position_in_partition::equal_compare rows_eq(_query_schema);
                const rows_entry& e = pop_clustering_row();
                if (_digest_requested) {
                    e.row().cells().prepare_hash(_query_schema, column_kind::regular_column);
                }
                tombstone rt_for_row = e.range_tombstone();
                auto result_row = clustering_row(_query_schema, e);

                // TODO: Ideally this should be position() or position().reversed(), depending on Reversing.
                while (has_more_rows() && rows_eq(peek_row().position(), e.position())) {
                    const rows_entry& e = pop_clustering_row();
                    if (_digest_requested) {
                        e.row().cells().prepare_hash(_query_schema, column_kind::regular_column);
                    }
                    result_row.apply(_query_schema, e);
                    rt_for_row.apply(e.range_tombstone());
                }
                if (e.dummy()) {
                    mplog.trace("next_interval(): pos={}, rt={}", e.position(), rt_before_row);
                    return interval_info{rt_before_row, to_query_order(position_in_partition(e.position()))};
                }
                rt_for_row.apply(_range_tombstones.peek());
                mplog.trace("next_interval(): row, pos={}, rt={}, rt_for_row={}", e.position(), rt_before_row, rt_for_row);
                auto result = mutation_fragment_v2(_query_schema, _permit, std::move(result_row));
                return interval_info{rt_before_row, row_info{std::move(result), rt_for_row}};
            });
        }
    };
private:
    // Keeps shared pointer to the container we read mutation from to make sure
    // that its lifetime is appropriately extended.
    std::any _container_guard;

    // Each range from _ck_ranges are taken to be in snapshot clustering key
    // order, i.e. given a comparator derived from snapshot schema, for each ck_range from
    // _ck_ranges, begin(ck_range) <= end(ck_range).
    query::clustering_key_filter_ranges _ck_ranges;
    query::clustering_row_ranges::const_iterator _current_ck_range;
    query::clustering_row_ranges::const_iterator _ck_range_end;

    // Holds reversed current clustering key range, if Reversing was needed.
    std::optional<query::clustering_range> opt_reversed_range;

    std::optional<position_in_partition> _last_entry;

    // Last emitted range_tombstone_change.
    tombstone _current_tombstone;

    lsa_partition_reader _reader;
    bool _static_row_done = false;

    Accounter& accounter() {
        return *this;
    }
private:
    void push_static_row() {
        auto sr = _reader.get_static_row();
        if (!sr.empty()) {
            emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
        }
    }

    // If `Reversing`, when we pop_range_tombstone(), a reversed rt is returned (the correct
    // one in query clustering order). In order to save progress of reading from range_tombstone_list,
    // we need to save the end position of rt (as it was stored in the list). This corresponds to
    // the start position, with reversed bound weigth.
    static position_in_partition to_query_order(position_in_partition&& pos) {
        if constexpr (Reversing) {
            return pos.reversed();
        }
        return std::move(pos);
    }

    void emit_next_interval() {
        // We use the names ck_range_snapshot and ck_range_query to denote clustering order.
        // ck_range_snapshot uses the snapshot order, while ck_range_query uses the
        // query order. These two differ if the query was reversed (`Reversing==true`).
        const auto& ck_range_snapshot = *_current_ck_range;
        const auto& ck_range_query = opt_reversed_range ? *opt_reversed_range : ck_range_snapshot;

        auto lower_bound = [&] () -> position_in_partition_view {
            if (!_last_entry) {
                return position_in_partition_view::for_range_start(ck_range_query);
            } else {
                return position_in_partition_view::after_key(*_last_entry);
            }
        }();

        interval_info next = _reader.next_interval(ck_range_snapshot, _last_entry);

        if (next.range_tombstone != _current_tombstone) {
            _current_tombstone = next.range_tombstone;
            emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                range_tombstone_change(lower_bound, _current_tombstone)));
        }

        std::visit(make_visitor([&] (row_info&& info) {
            auto pos_view = info.row.as_clustering_row().position();
            _last_entry = position_in_partition(pos_view);
            if (info.rt_for_row != _current_tombstone) {
                _current_tombstone = info.rt_for_row;
                emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                    range_tombstone_change(
                        position_in_partition::before_key(info.row.as_clustering_row().key()), _current_tombstone)));
            }
            emplace_mutation_fragment(std::move(info.row));
        }, [&] (position_in_partition&& pos) {
            _last_entry = std::move(pos);
        }, [&] (std::monostate) {
            if (_current_tombstone) {
                _current_tombstone = {};
                emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                    range_tombstone_change(position_in_partition_view::for_range_end(ck_range_query), _current_tombstone)));
            }
            _last_entry = std::nullopt;
            _current_ck_range = std::next(_current_ck_range);
            fill_opt_reversed_range();
            on_new_range();
        }), std::move(next.info));
    }

    void emplace_mutation_fragment(mutation_fragment_v2&& mfopt) {
        mfopt.visit(accounter());
        push_mutation_fragment(std::move(mfopt));
    }

    void on_new_range() {
        if (_current_ck_range == _ck_range_end) {
            _end_of_stream = true;
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end()));
        } else {
            _reader.reset_state(*_current_ck_range);
        }
    }

    void fill_opt_reversed_range() {
        opt_reversed_range = std::nullopt;
        if (_current_ck_range != _ck_range_end) {
            if constexpr (Reversing) {
                opt_reversed_range = query::reverse(*_current_ck_range);
            }
        }
    }

    void do_fill_buffer() {
        while (!is_end_of_stream() && !is_buffer_full()) {
            emit_next_interval();
            if (need_preempt()) {
                break;
            }
        }
    }
public:
    template <typename... Args>
    partition_snapshot_flat_reader(schema_ptr s, reader_permit permit, dht::decorated_key dk, partition_snapshot_ptr snp,
                              query::clustering_key_filter_ranges crr, bool digest_requested,
                              logalloc::region& region, logalloc::allocating_section& read_section,
                              std::any pointer_to_container, Args&&... args)
        : impl(std::move(s), std::move(permit))
        , Accounter(std::forward<Args>(args)...)
        , _container_guard(std::move(pointer_to_container))
        , _ck_ranges(std::move(crr))
        , _current_ck_range(_ck_ranges.begin())
        , _ck_range_end(_ck_ranges.end())
        , _reader(*_schema, _permit, std::move(snp), region, read_section, digest_requested)
    {
        fill_opt_reversed_range();
        _reader.with_reserve([&] {
            push_mutation_fragment(*_schema, _permit, partition_start(std::move(dk), _reader.partition_tombstone()));
        });
    }

    virtual future<> fill_buffer() override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            _reader.with_reserve([&] {
                if (!_static_row_done) {
                    push_static_row();
                    on_new_range();
                    _static_row_done = true;
                }
                do_fill_buffer();
            });
            return make_ready_future<>();
        });
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _end_of_stream = true;
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        throw std::runtime_error("This reader can't be fast forwarded to another partition.");
    };
    virtual future<> fast_forward_to(position_range cr) override {
        throw std::runtime_error("This reader can't be fast forwarded to another position.");
    };
    virtual future<> close() noexcept override {
        return make_ready_future<>();
    }
};

template <bool Reversing, typename Accounter, typename... Args>
inline flat_mutation_reader_v2
make_partition_snapshot_flat_reader(schema_ptr s,
                                    reader_permit permit,
                                    dht::decorated_key dk,
                                    query::clustering_key_filter_ranges crr,
                                    partition_snapshot_ptr snp,
                                    bool digest_requested,
                                    logalloc::region& region,
                                    logalloc::allocating_section& read_section,
                                    std::any pointer_to_container,
                                    streamed_mutation::forwarding fwd,
                                    Args&&... args)
{
    auto res = make_flat_mutation_reader_v2<partition_snapshot_flat_reader<Reversing, Accounter>>(std::move(s), std::move(permit), std::move(dk),
            snp, std::move(crr), digest_requested, region, read_section, std::move(pointer_to_container), std::forward<Args>(args)...);
    if (fwd) {
        return make_forwardable(std::move(res)); // FIXME: optimize
    } else {
        return res;
    }
}
