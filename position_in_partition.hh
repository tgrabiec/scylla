/*
 * Copyright (C) 2017 ScyllaDB
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

#include "types.hh"
#include "keys.hh"
#include "clustering_bounds_comparator.hh"
#include "query-request.hh"

inline
lexicographical_relation relation_for_lower_bound(composite_view v) {
    switch (v.last_eoc()) {
        case composite::eoc::start:
        case composite::eoc::none:
            return lexicographical_relation::before_all_prefixed;
        case composite::eoc::end:
            return lexicographical_relation::after_all_prefixed;
        default:
            assert(0);
    }
}

inline
lexicographical_relation relation_for_upper_bound(composite_view v) {
    switch (v.last_eoc()) {
        case composite::eoc::start:
            return lexicographical_relation::before_all_prefixed;
        case composite::eoc::none:
            return lexicographical_relation::before_all_strictly_prefixed;
        case composite::eoc::end:
            return lexicographical_relation::after_all_prefixed;
        default:
            assert(0);
    }
}

class position_in_partition_view {
    friend class position_in_partition;

    int _bound_weight = 0;
    const clustering_key_prefix* _ck; // nullptr for static row
private:
    position_in_partition_view(int bound_weight, const clustering_key_prefix* ck)
        : _bound_weight(bound_weight)
        , _ck(ck)
    { }
    // Returns placement of this position_in_partition relative to *_ck,
    // or lexicographical_relation::at_prefix if !_ck.
    lexicographical_relation relation() const {
        // FIXME: Currently position_range cannot represent a range end bound which
        // includes just the prefix key or a range start which excludes just a prefix key.
        // In both cases we should return lexicographical_relation::before_all_strictly_prefixed here.
        // Refs #1446.
        if (_bound_weight <= 0) {
            return lexicographical_relation::before_all_prefixed;
        } else {
            return lexicographical_relation::after_all_prefixed;
        }
    }
public:
    struct static_row_tag_t { };
    struct clustering_row_tag_t { };
    struct range_tag_t { };
    using range_tombstone_tag_t = range_tag_t;

    position_in_partition_view(static_row_tag_t) : _ck(nullptr) { }
    position_in_partition_view(clustering_row_tag_t, const clustering_key_prefix& ck)
        : _ck(&ck) { }
    position_in_partition_view(const clustering_key_prefix& ck)
        : _ck(&ck) { }
    position_in_partition_view(range_tag_t, bound_view bv)
        : _bound_weight(weight(bv.kind)), _ck(&bv.prefix) { }

    static position_in_partition_view for_range_start(const query::clustering_range& r) {
        return {position_in_partition_view::range_tag_t(), bound_view::from_range_start(r)};
    }

    static position_in_partition_view for_range_end(const query::clustering_range& r) {
        return {position_in_partition_view::range_tag_t(), bound_view::from_range_end(r)};
    }

    static position_in_partition_view before_all_clustered_rows() {
        return {range_tag_t(), bound_view::bottom()};
    }

    static position_in_partition_view after_all_clustered_rows() {
        return {position_in_partition_view::range_tag_t(), bound_view::top()};
    }

    static position_in_partition_view for_static_row() {
        return {static_row_tag_t()};
    }

    static position_in_partition_view for_key(const clustering_key& ck) {
        return {clustering_row_tag_t(), ck};
    }

    static position_in_partition_view after_key(const clustering_key& ck) {
        return {1, &ck};
    }

    bool is_static_row() const { return !_ck; }
    bool is_clustering_row() const { return _ck && !_bound_weight; }

    // Returns true if all fragments that can be seen for given schema have
    // positions >= than this.
    bool is_before_all_fragments(const schema& s) const {
        return !_ck || (!s.has_static_columns() && _bound_weight < 0 && _ck->is_empty(s));
    }

    bool is_after_all_clustered_rows(const schema& s) const {
        return _ck && _ck->is_empty(s) && _bound_weight > 0;
    }

    // Valid when >= before_all_clustered_rows()
    const clustering_key_prefix& key() const {
        return *_ck;
    }

    // Can be called only when !is_static_row && !is_clustering_row().
    bound_view as_start_bound_view() const {
        assert(_bound_weight != 0);
        return bound_view(*_ck, _bound_weight < 0 ? bound_kind::incl_start : bound_kind::excl_start);
    }

    friend std::ostream& operator<<(std::ostream&, position_in_partition_view);
    friend bool no_clustering_row_between(const schema&, position_in_partition_view, position_in_partition_view);
};

class position_in_partition {
    int _bound_weight = 0;
    stdx::optional<clustering_key_prefix> _ck;
public:
    struct static_row_tag_t { };
    struct after_static_row_tag_t { };
    struct clustering_row_tag_t { };
    struct after_clustering_row_tag_t { };
    struct range_tag_t { };
    using range_tombstone_tag_t = range_tag_t;

    explicit position_in_partition(static_row_tag_t) { }
    position_in_partition(clustering_row_tag_t, clustering_key_prefix ck)
        : _ck(std::move(ck)) { }
    position_in_partition(after_clustering_row_tag_t, clustering_key_prefix ck)
        // FIXME: Use lexicographical_relation::before_strictly_prefixed here. Refs #1446
        : _bound_weight(1), _ck(std::move(ck)) { }
    position_in_partition(range_tag_t, bound_view bv)
        : _bound_weight(weight(bv.kind)), _ck(bv.prefix) { }
    position_in_partition(after_static_row_tag_t) :
        position_in_partition(range_tag_t(), bound_view::bottom()) { }
    explicit position_in_partition(position_in_partition_view view)
        : _bound_weight(view._bound_weight)
        {
            if (view._ck) {
                _ck = *view._ck;
            }
        }

    static position_in_partition before_all_clustered_rows() {
        return {position_in_partition::range_tag_t(), bound_view::bottom()};
    }

    static position_in_partition after_all_clustered_rows() {
        return {position_in_partition::range_tag_t(), bound_view::top()};
    }

    static position_in_partition after_key(clustering_key ck) {
        return {after_clustering_row_tag_t(), std::move(ck)};
    }

    static position_in_partition for_key(clustering_key ck) {
        return {clustering_row_tag_t(), std::move(ck)};
    }

    static position_in_partition for_range_start(const query::clustering_range&);
    static position_in_partition for_range_end(const query::clustering_range&);

    bool is_static_row() const { return !_ck; }
    bool is_clustering_row() const { return _ck && !_bound_weight; }

    bool is_after_all_clustered_rows(const schema& s) const {
        return _ck && _ck->is_empty(s) && _bound_weight > 0;
    }

    template<typename Hasher>
    void feed_hash(Hasher& hasher, const schema& s) const {
        ::feed_hash(hasher, _bound_weight);
        if (_ck) {
            ::feed_hash(hasher, true);
            _ck->feed_hash(hasher, s);
        } else {
            ::feed_hash(hasher, false);
        }
    }

    clustering_key_prefix& key() {
        return *_ck;
    }
    const clustering_key_prefix& key() const {
        return *_ck;
    }
    operator position_in_partition_view() const {
        return { _bound_weight, _ck ? &*_ck : nullptr };
    }

    // Defines total order on the union of position_and_partition and composite objects.
    //
    // The ordering is compatible with position_range (r). The following is satisfied for
    // all cells with name c included by the range:
    //
    //   r.start() <= c < r.end()
    //
    // The ordering on composites given by this is compatible with but weaker than the cell name order.
    //
    // The ordering on position_in_partition given by this is compatible but weaker than the ordering
    // given by position_in_partition::tri_compare.
    //
    class composite_tri_compare {
        const schema& _s;
    public:
        composite_tri_compare(const schema& s) : _s(s) {}

        int operator()(position_in_partition_view a, position_in_partition_view b) const {
            if (a.is_static_row() || b.is_static_row()) {
                return b.is_static_row() - a.is_static_row();
            }
            auto&& types = _s.clustering_key_type()->types();
            auto cmp = [&] (const data_type& t, bytes_view c1, bytes_view c2) { return t->compare(c1, c2); };
            return lexicographical_tri_compare(types.begin(), types.end(),
                a._ck->begin(_s), a._ck->end(_s),
                b._ck->begin(_s), b._ck->end(_s),
                cmp, a.relation(), b.relation());
        }

        int operator()(position_in_partition_view a, composite_view b) const {
            if (b.empty()) {
                return 1; // a cannot be empty.
            }
            if (a.is_static_row() || b.is_static()) {
                return b.is_static() - a.is_static_row();
            }
            auto&& types = _s.clustering_key_type()->types();
            auto b_values = b.values();
            auto cmp = [&] (const data_type& t, bytes_view c1, bytes_view c2) { return t->compare(c1, c2); };
            return lexicographical_tri_compare(types.begin(), types.end(),
                a._ck->begin(_s), a._ck->end(_s),
                b_values.begin(), b_values.end(),
                cmp, a.relation(), relation_for_lower_bound(b));
        }

        int operator()(composite_view a, position_in_partition_view b) const {
            return -(*this)(b, a);
        }

        int operator()(composite_view a, composite_view b) const {
            if (a.is_static() != b.is_static()) {
                return a.is_static() ? -1 : 1;
            }
            auto&& types = _s.clustering_key_type()->types();
            auto a_values = a.values();
            auto b_values = b.values();
            auto cmp = [&] (const data_type& t, bytes_view c1, bytes_view c2) { return t->compare(c1, c2); };
            return lexicographical_tri_compare(types.begin(), types.end(),
                a_values.begin(), a_values.end(),
                b_values.begin(), b_values.end(),
                cmp,
                relation_for_lower_bound(a),
                relation_for_lower_bound(b));
        }
    };

    // Less comparator giving the same order as composite_tri_compare.
    class composite_less_compare {
        composite_tri_compare _cmp;
    public:
        composite_less_compare(const schema& s) : _cmp(s) {}

        template<typename T, typename U>
        bool operator()(const T& a, const U& b) const {
            return _cmp(a, b) < 0;
        }
    };

    class tri_compare {
        bound_view::tri_compare _cmp;
    private:
        template<typename T, typename U>
        int compare(const T& a, const U& b) const {
            bool a_rt_weight = bool(a._ck);
            bool b_rt_weight = bool(b._ck);
            if (!a_rt_weight || !b_rt_weight) {
                return a_rt_weight - b_rt_weight;
            }
            return _cmp(*a._ck, a._bound_weight, *b._ck, b._bound_weight);
        }
    public:
        tri_compare(const schema& s) : _cmp(s) { }
        int operator()(const position_in_partition& a, const position_in_partition& b) const {
            return compare(a, b);
        }
        int operator()(const position_in_partition_view& a, const position_in_partition_view& b) const {
            return compare(a, b);
        }
        int operator()(const position_in_partition& a, const position_in_partition_view& b) const {
            return compare(a, b);
        }
        int operator()(const position_in_partition_view& a, const position_in_partition& b) const {
            return compare(a, b);
        }
    };
    class less_compare {
        tri_compare _cmp;
    public:
        less_compare(const schema& s) : _cmp(s) { }
        bool operator()(const position_in_partition& a, const position_in_partition& b) const {
            return _cmp(a, b) < 0;
        }
        bool operator()(const position_in_partition_view& a, const position_in_partition_view& b) const {
            return _cmp(a, b) < 0;
        }
        bool operator()(const position_in_partition& a, const position_in_partition_view& b) const {
            return _cmp(a, b) < 0;
        }
        bool operator()(const position_in_partition_view& a, const position_in_partition& b) const {
            return _cmp(a, b) < 0;
        }
    };
    class equal_compare {
        clustering_key_prefix::equality _equal;
        template<typename T, typename U>
        bool compare(const T& a, const U& b) const {
            bool a_rt_weight = bool(a._ck);
            bool b_rt_weight = bool(b._ck);
            return a_rt_weight == b_rt_weight
                   && (!a_rt_weight || (_equal(*a._ck, *b._ck)
                        && a._bound_weight == b._bound_weight));
        }
    public:
        equal_compare(const schema& s) : _equal(s) { }
        bool operator()(const position_in_partition& a, const position_in_partition& b) const {
            return compare(a, b);
        }
        bool operator()(const position_in_partition_view& a, const position_in_partition_view& b) const {
            return compare(a, b);
        }
        bool operator()(const position_in_partition_view& a, const position_in_partition& b) const {
            return compare(a, b);
        }
        bool operator()(const position_in_partition& a, const position_in_partition_view& b) const {
            return compare(a, b);
        }
    };
    friend std::ostream& operator<<(std::ostream&, const position_in_partition&);
};

inline
position_in_partition position_in_partition::for_range_start(const query::clustering_range& r) {
    return {position_in_partition::range_tag_t(), bound_view::from_range_start(r)};
}

inline
position_in_partition position_in_partition::for_range_end(const query::clustering_range& r) {
    return {position_in_partition::range_tag_t(), bound_view::from_range_end(r)};
}

// Returns true if and only if there can't be any clustering_row with position > a and < b.
// It is assumed that a <= b.
inline
bool no_clustering_row_between(const schema& s, position_in_partition_view a, position_in_partition_view b) {
    clustering_key_prefix::equality eq(s);
    if (a._ck && b._ck) {
        return eq(*a._ck, *b._ck) && (a._bound_weight >= 0 || b._bound_weight <= 0);
    } else {
        return !a._ck && !b._ck;
    }
}

// Includes all position_in_partition objects "p" for which: start <= p < end
// And only those.
class position_range {
private:
    position_in_partition _start;
    position_in_partition _end;
public:
    static position_range from_range(const query::clustering_range&);

    static position_range for_static_row() {
        return {
            position_in_partition(position_in_partition::static_row_tag_t()),
            position_in_partition(position_in_partition::after_static_row_tag_t())
        };
    }

    static position_range full() {
        return {
            position_in_partition(position_in_partition::static_row_tag_t()),
            position_in_partition::after_all_clustered_rows()
        };
    }

    static position_range all_clustered_rows() {
        return {
            position_in_partition::before_all_clustered_rows(),
            position_in_partition::after_all_clustered_rows()
        };
    }

    position_range(position_range&&) = default;
    position_range& operator=(position_range&&) = default;
    position_range(const position_range&) = default;
    position_range& operator=(const position_range&) = default;

    // Constructs position_range which covers the same rows as given clustering_range.
    // position_range includes a fragment if it includes position of that fragment.
    position_range(const query::clustering_range&);
    position_range(query::clustering_range&&);

    position_range(position_in_partition start, position_in_partition end)
        : _start(std::move(start))
        , _end(std::move(end))
    { }

    const position_in_partition& start() const& { return _start; }
    position_in_partition&& start() && { return std::move(_start); }
    const position_in_partition& end() const& { return _end; }
    position_in_partition&& end() && { return std::move(_end); }
    bool contains(const schema& s, position_in_partition_view pos) const;
    bool overlaps(const schema& s, position_in_partition_view start, position_in_partition_view end) const;

    friend std::ostream& operator<<(std::ostream&, const position_range&);
};

inline
bool position_range::contains(const schema& s, position_in_partition_view pos) const {
    position_in_partition::less_compare less(s);
    return !less(pos, _start) && less(pos, _end);
}

inline
bool position_range::overlaps(const schema& s, position_in_partition_view start, position_in_partition_view end) const {
    position_in_partition::less_compare less(s);
    return !less(end, _start) && less(start, _end);
}
