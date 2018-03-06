/*
 * Copyright (C) 2016 ScyllaDB
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

#include <boost/range/algorithm/heap_algorithm.hpp>
#include <seastar/util/defer.hh>

#include "partition_version.hh"
#include "partition_builder.hh"
#include "row_cache.hh"
#include "partition_snapshot_row_cursor.hh"

static void remove_or_mark_as_unique_owner(partition_version* current, cache_tracker* tracker)
{
    while (current && !current->is_referenced()) {
        auto next = current->next();
        if (tracker) {
            for (rows_entry& row : current->partition().clustered_rows()) {
                tracker->on_remove(row);
            }
        }
        current_allocator().destroy(current);
        current = next;
    }
    if (current) {
        current->back_reference().mark_as_unique_owner();
    }
}

partition_version::partition_version(partition_version&& pv) noexcept
    : anchorless_list_base_hook(std::move(pv))
    , _backref(pv._backref)
    , _partition(std::move(pv._partition))
{
    if (_backref) {
        _backref->_version = this;
    }
    pv._backref = nullptr;
}

partition_version& partition_version::operator=(partition_version&& pv) noexcept
{
    if (this != &pv) {
        this->~partition_version();
        new (this) partition_version(std::move(pv));
    }
    return *this;
}

partition_version::~partition_version()
{
    if (_backref) {
        _backref->_version = nullptr;
    }
}

size_t partition_version::size_in_allocator(allocation_strategy& allocator) const {
    return allocator.object_memory_size_in_allocator(this) +
           partition().external_memory_usage();
}

namespace {

GCC6_CONCEPT(

// A functor which transforms objects from Domain into objects from CoDomain
template<typename U, typename Domain, typename CoDomain>
concept bool Mapper() {
    return requires(U obj, const Domain& src) {
        { obj(src) } -> const CoDomain&
    };
}

// A functor which merges two objects from Domain into one. The result is stored in the first argument.
template<typename U, typename Domain>
concept bool Reducer() {
    return requires(U obj, Domain& dst, const Domain& src) {
        { obj(dst, src) } -> void;
    };
}

)

// Calculates the value of particular part of mutation_partition represented by
// the version chain starting from v.
// |map| extracts the part from each version.
// |reduce| Combines parts from the two versions.
template <typename Result, typename Map, typename Reduce>
GCC6_CONCEPT(
requires Mapper<Map, mutation_partition, Result>() && Reducer<Reduce, Result>()
)
inline Result squashed(const partition_version_ref& v, Map&& map, Reduce&& reduce) {
    const partition_version* this_v = &*v;
    partition_version* it = v->last();
    Result r = map(it->partition());
    while (it != this_v) {
        it = it->prev();
        reduce(r, map(it->partition()));
    }
    return r;
}

}

::static_row partition_snapshot::static_row(bool digest_requested) const {
    return ::static_row(::squashed<row>(version(),
                         [&] (const mutation_partition& mp) -> const row& {
                            if (digest_requested) {
                                mp.static_row().prepare_hash(*_schema, column_kind::static_column);
                            }
                            return mp.static_row();
                         },
                         [this] (row& a, const row& b) { a.apply(*_schema, column_kind::static_column, b); }));
}

bool partition_snapshot::static_row_continuous() const {
    return version()->partition().static_row_continuous();
}

tombstone partition_snapshot::partition_tombstone() const {
    return ::squashed<tombstone>(version(),
                               [] (const mutation_partition& mp) { return mp.partition_tombstone(); },
                               [] (tombstone& a, tombstone b) { a.apply(b); });
}

mutation_partition partition_snapshot::squashed() const {
    return ::squashed<mutation_partition>(version(),
                               [] (const mutation_partition& mp) -> const mutation_partition& { return mp; },
                               [this] (mutation_partition& a, const mutation_partition& b) { a.apply(*_schema, b, *_schema); });
}

tombstone partition_entry::partition_tombstone() const {
    return ::squashed<tombstone>(_version,
        [] (const mutation_partition& mp) { return mp.partition_tombstone(); },
        [] (tombstone& a, tombstone b) { a.apply(b); });
}

partition_snapshot::~partition_snapshot() {
    with_allocator(_region.allocator(), [this] {
        if (_version && _version.is_unique_owner()) {
            auto v = &*_version;
            _version = {};
            remove_or_mark_as_unique_owner(v, _tracker);
        } else if (_entry) {
            _entry->_snapshot = nullptr;
        }
    });
}

void merge_versions(const schema& s, mutation_partition& newer, mutation_partition&& older, cache_tracker* tracker) {
    older.apply_monotonically(s, std::move(newer), tracker);
    newer = std::move(older);
}

void partition_snapshot::merge_partition_versions() {
    if (_version && !_version.is_unique_owner()) {
        auto v = &*_version;
        _version = { };
        auto first_used = v;
        while (first_used->prev() && !first_used->is_referenced()) {
            first_used = first_used->prev();
        }

        auto current = first_used->next();
        while (current && !current->is_referenced()) {
            auto next = current->next();
            merge_versions(*_schema, first_used->partition(), std::move(current->partition()), _tracker);
            current_allocator().destroy(current);
            current = next;
        }
    }
}

unsigned partition_snapshot::version_count()
{
    unsigned count = 0;
    for (auto&& v : versions()) {
        (void)v;
        count++;
    }
    return count;
}

partition_entry::partition_entry(mutation_partition mp)
{
    auto new_version = current_allocator().construct<partition_version>(std::move(mp));
    _version = partition_version_ref(*new_version);
}

partition_entry::partition_entry(partition_entry::evictable_tag, const schema& s, mutation_partition&& mp)
    : partition_entry([&] {
        mp.ensure_last_dummy(s);
        return std::move(mp);
    }())
{ }

partition_entry partition_entry::make_evictable(const schema& s, mutation_partition&& mp) {
    return {evictable_tag(), s, std::move(mp)};
}

partition_entry partition_entry::make_evictable(const schema& s, const mutation_partition& mp) {
    return make_evictable(s, mutation_partition(mp));
}

partition_entry::~partition_entry() {
    if (!_version) {
        return;
    }
    if (_snapshot) {
        _snapshot->_version = std::move(_version);
        _snapshot->_version.mark_as_unique_owner();
        _snapshot->_entry = nullptr;
    } else {
        auto v = &*_version;
        _version = { };
        remove_or_mark_as_unique_owner(v, no_cache_tracker);
    }
}

void partition_entry::set_version(partition_version* new_version)
{
    if (_snapshot) {
        _snapshot->_version = std::move(_version);
        _snapshot->_entry = nullptr;
    }

    _snapshot = nullptr;
    _version = partition_version_ref(*new_version);
}

partition_version& partition_entry::add_version(const schema& s, cache_tracker* tracker) {
    // Every evictable version must have a dummy entry at the end so that
    // it can be tracked in the LRU. It is also needed to allow old versions
    // to stay around (with tombstones and static rows) after fully evicted.
    // Such versions must be fully discontinuous, and thus have a dummy at the end.
    auto new_version = tracker
                       ? current_allocator().construct<partition_version>(mutation_partition::make_incomplete(s))
                       : current_allocator().construct<partition_version>(mutation_partition(s.shared_from_this()));
    new_version->partition().set_static_row_continuous(_version->partition().static_row_continuous());
    new_version->insert_before(*_version);
    set_version(new_version);
    if (tracker) {
        tracker->insert(*new_version);
    }
    return *new_version;
}

void partition_entry::apply(const schema& s, const mutation_partition& mp, const schema& mp_schema)
{
    apply(s, mutation_partition(mp), mp_schema);
}

void partition_entry::apply(const schema& s, mutation_partition&& mp, const schema& mp_schema)
{
    if (s.version() != mp_schema.version()) {
        mp.upgrade(mp_schema, s);
    }
    auto new_version = current_allocator().construct<partition_version>(std::move(mp));
    if (!_snapshot) {
        try {
            _version->partition().apply_monotonically(s, std::move(new_version->partition()), no_cache_tracker);
            current_allocator().destroy(new_version);
            return;
        } catch (...) {
            // fall through
        }
    }
    new_version->insert_before(*_version);
    set_version(new_version);
}

void partition_entry::apply(const schema& s, mutation_partition_view mpv, const schema& mp_schema)
{
    mutation_partition mp(mp_schema.shared_from_this());
    partition_builder pb(mp_schema, mp);
    mpv.accept(mp_schema, pb);
    apply(s, std::move(mp), mp_schema);
}

// Iterates over all rows in mutation represented by partition_entry.
// It abstracts away the fact that rows may be spread across multiple versions.
class partition_entry::rows_iterator final {
    struct version {
        mutation_partition::rows_type::iterator current_row;
        mutation_partition::rows_type* rows;
        bool can_move;
        struct compare {
            const rows_entry::tri_compare& _cmp;
        public:
            explicit compare(const rows_entry::tri_compare& cmp) : _cmp(cmp) { }
            bool operator()(const version& a, const version& b) const {
                return _cmp(*a.current_row, *b.current_row) > 0;
            }
        };
    };
    const schema& _schema;
    rows_entry::tri_compare _rows_cmp;
    rows_entry::compare _rows_less_cmp;
    version::compare _version_cmp;
    std::vector<version> _heap;
    std::vector<version> _current_row;
    bool _current_row_dummy;
public:
    rows_iterator(partition_version* version, const schema& schema)
        : _schema(schema)
        , _rows_cmp(schema)
        , _rows_less_cmp(schema)
        , _version_cmp(_rows_cmp)
    {
        bool can_move = true;
        while (version) {
            can_move &= !version->is_referenced();
            auto& rows = version->partition().clustered_rows();
            if (!rows.empty()) {
                _heap.push_back({rows.begin(), &rows, can_move});
            }
            version = version->next();
        }
        boost::range::make_heap(_heap, _version_cmp);
        move_to_next_row();
    }
    bool done() const {
        return _current_row.empty();
    }
    // Return clustering key of the current row in source.
    // Valid only when !is_dummy().
    const clustering_key& key() const {
        return _current_row[0].current_row->key();
    }
    position_in_partition_view position() const {
        return _current_row[0].current_row->position();
    }
    bool is_dummy() const {
        return _current_row_dummy;
    }
    template<typename RowConsumer>
    void consume_row(RowConsumer&& consumer) {
        assert(!_current_row.empty());
        // versions in _current_row are not ordered but it is not a problem
        // due to the fact that all rows are continuous.
        for (version& v : _current_row) {
            if (!v.can_move) {
                consumer(deletable_row(v.current_row->row()));
            } else {
                consumer(std::move(v.current_row->row()));
            }
        }
    }
    void remove_current_row_when_possible() {
        assert(!_current_row.empty());
        auto deleter = current_deleter<rows_entry>();
        for (version& v : _current_row) {
            if (v.can_move) {
                v.rows->erase_and_dispose(v.current_row, deleter);
            }
        }
    }
    void move_to_next_row() {
        _current_row.clear();
        _current_row_dummy = true;
        while (!_heap.empty() &&
                (_current_row.empty() || _rows_cmp(*_current_row[0].current_row, *_heap[0].current_row) == 0)) {
            boost::range::pop_heap(_heap, _version_cmp);
            auto& curr = _heap.back();
            _current_row.push_back({curr.current_row, curr.rows, curr.can_move});
            _current_row_dummy &= bool(curr.current_row->dummy());
            ++curr.current_row;
            if (curr.current_row == curr.rows->end()) {
                _heap.pop_back();
            } else {
                boost::range::push_heap(_heap, _version_cmp);
            }
        }
    }
};

template<typename Func>
void partition_entry::with_detached_versions(Func&& func) {
    partition_version* current = &*_version;
    auto snapshot = _snapshot;
    if (snapshot) {
        snapshot->_version = std::move(_version);
        snapshot->_entry = nullptr;
        _snapshot = nullptr;
    }
    auto prev = std::exchange(_version, {});

    auto revert = defer([&] {
        if (snapshot) {
            _snapshot = snapshot;
            snapshot->_entry = this;
            _version = std::move(snapshot->_version);
        } else {
            _version = std::move(prev);
        }
    });

    func(current);
}

void partition_entry::apply_to_incomplete(const schema& s, partition_entry&& pe, const schema& pe_schema,
    logalloc::region& reg, cache_tracker& tracker)
{
    if (s.version() != pe_schema.version()) {
        partition_entry entry(pe.squashed(pe_schema.shared_from_this(), s.shared_from_this()));
        entry.with_detached_versions([&] (partition_version* v) {
            apply_to_incomplete(s, v, reg, tracker);
        });
    } else {
        pe.with_detached_versions([&](partition_version* v) {
            apply_to_incomplete(s, v, reg, tracker);
        });
    }
}

void partition_entry::apply_to_incomplete(const schema& s, partition_version* version,
        logalloc::region& reg, cache_tracker& tracker) {
    partition_version& dst = open_version(s, &tracker);
    auto snp = read(reg, s.shared_from_this(), &tracker);
    bool can_move = true;
    auto current = version;
    bool static_row_continuous = snp->static_row_continuous();
    while (current) {
        can_move &= !current->is_referenced();
        dst.partition().apply(current->partition().partition_tombstone());
        if (static_row_continuous) {
            row& static_row = dst.partition().static_row();
            if (can_move) {
                static_row.apply(s, column_kind::static_column, std::move(current->partition().static_row()));
            } else {
                static_row.apply(s, column_kind::static_column, current->partition().static_row());
            }
        }
        range_tombstone_list& tombstones = dst.partition().row_tombstones();
        if (can_move) {
            tombstones.apply_monotonically(s, std::move(current->partition().row_tombstones()));
        } else {
            tombstones.apply_monotonically(s, current->partition().row_tombstones());
        }
        current = current->next();
    }

    partition_entry::rows_iterator source(version, s);
    partition_snapshot_row_cursor cur(s, *snp);

    while (!source.done()) {
        if (!source.is_dummy()) {
            tracker.on_row_processed_from_memtable();
            auto ropt = cur.ensure_entry_if_complete(source.position());
            if (ropt) {
                rows_entry& e = ropt->row;
                source.consume_row([&] (deletable_row&& row) {
                    e.row().apply_monotonically(s, std::move(row));
                });
                if (!ropt->inserted) {
                    tracker.on_row_merged_from_memtable();
                }
            } else {
                tracker.on_row_dropped_from_memtable();
            }
        }
        source.remove_current_row_when_possible();
        source.move_to_next_row();
    }
}

mutation_partition partition_entry::squashed(schema_ptr from, schema_ptr to)
{
    mutation_partition mp(to);
    mp.set_static_row_continuous(_version->partition().static_row_continuous());
    for (auto&& v : _version->all_elements()) {
        auto older = v.partition();
        if (from->version() != to->version()) {
            older.upgrade(*from, *to);
        }
        merge_versions(*to, mp, std::move(older), no_cache_tracker);
    }
    return mp;
}

mutation_partition partition_entry::squashed(const schema& s)
{
    return squashed(s.shared_from_this(), s.shared_from_this());
}

void partition_entry::upgrade(schema_ptr from, schema_ptr to, cache_tracker* tracker)
{
    auto new_version = current_allocator().construct<partition_version>(squashed(from, to));
    auto old_version = &*_version;
    set_version(new_version);
    if (tracker) {
        tracker->insert(*new_version);
    }
    remove_or_mark_as_unique_owner(old_version, tracker);
}

lw_shared_ptr<partition_snapshot> partition_entry::read(logalloc::region& r,
    schema_ptr entry_schema, cache_tracker* tracker, partition_snapshot::phase_type phase)
{
    with_allocator(r.allocator(), [&] {
        open_version(*entry_schema, tracker, phase);
    });
    if (_snapshot) {
        return _snapshot->shared_from_this();
    } else {
        auto snp = make_lw_shared<partition_snapshot>(entry_schema, r, this, tracker, phase);
        _snapshot = snp.get();
        return snp;
    }
}

std::vector<range_tombstone>
partition_snapshot::range_tombstones(position_in_partition_view start, position_in_partition_view end)
{
    partition_version* v = &*version();
    if (!v->next()) {
        return boost::copy_range<std::vector<range_tombstone>>(
            v->partition().row_tombstones().slice(*_schema, start, end));
    }
    range_tombstone_list list(*_schema);
    while (v) {
        for (auto&& rt : v->partition().row_tombstones().slice(*_schema, start, end)) {
            list.apply(*_schema, rt);
        }
        v = v->next();
    }
    return boost::copy_range<std::vector<range_tombstone>>(list.slice(*_schema, start, end));
}

std::vector<range_tombstone>
partition_snapshot::range_tombstones()
{
    return range_tombstones(
        position_in_partition_view::before_all_clustered_rows(),
        position_in_partition_view::after_all_clustered_rows());
}

std::ostream& operator<<(std::ostream& out, const partition_entry& e) {
    out << "{";
    bool first = true;
    if (e._version) {
        const partition_version* v = &*e._version;
        while (v) {
            if (!first) {
                out << ", ";
            }
            if (v->is_referenced()) {
                out << "(*) ";
            }
            out << v->partition();
            v = v->next();
            first = false;
        }
    }
    out << "}";
    return out;
}

void partition_entry::evict(cache_tracker& tracker) noexcept {
    if (!_version) {
        return;
    }
    if (_snapshot) {
        _snapshot->_version = std::move(_version);
        _snapshot->_version.mark_as_unique_owner();
        _snapshot->_entry = nullptr;
    } else {
        auto v = &*_version;
        _version = { };
        remove_or_mark_as_unique_owner(v, &tracker);
    }
}
