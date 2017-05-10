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

static void remove_or_mark_as_unique_owner(partition_version* current)
{
    while (current && !current->is_referenced()) {
        auto next = current->next();
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

partition_snapshot::~partition_snapshot() {
    if (_version && _version.is_unique_owner()) {
        auto v = &*_version;
        _version = {};
        remove_or_mark_as_unique_owner(v);
    } else if (_entry) {
        _entry->_snapshot = nullptr;
    }
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
            try {
                first_used->partition().apply(*_schema, std::move(current->partition()));
                current_allocator().destroy(current);
            } catch (...) {
                // Set _version so that the merge can be retried.
                _version = partition_version_ref(*current);
                throw;
            }
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
        remove_or_mark_as_unique_owner(v);
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

void partition_entry::apply(const schema& s, partition_version* pv, const schema& pv_schema)
{
    if (!_snapshot) {
        _version->partition().apply(s, std::move(pv->partition()), pv_schema);
        current_allocator().destroy(pv);
    } else {
        if (s.version() != pv_schema.version()) {
            pv->partition().upgrade(pv_schema, s);
        }
        pv->insert_before(*_version);
        set_version(pv);
    }
}

void partition_entry::apply(const schema& s, const mutation_partition& mp, const schema& mp_schema)
{
    if (!_snapshot) {
        _version->partition().apply(s, mp, mp_schema);
    } else {
        mutation_partition mp1 = mp;
        if (s.version() != mp_schema.version()) {
            mp1.upgrade(mp_schema, s);
        }
        auto new_version = current_allocator().construct<partition_version>(std::move(mp1));
        new_version->insert_before(*_version);

        set_version(new_version);
    }
}

void partition_entry::apply(const schema& s, mutation_partition&& mp, const schema& mp_schema)
{
    if (!_snapshot) {
        _version->partition().apply(s, std::move(mp), mp_schema);
    } else {
        if (s.version() != mp_schema.version()) {
            apply(s, mp, mp_schema);
        } else {
            auto new_version = current_allocator().construct<partition_version>(std::move(mp));
            new_version->insert_before(*_version);

            set_version(new_version);
        }
    }
}

void partition_entry::apply(const schema& s, mutation_partition_view mpv, const schema& mp_schema)
{
    if (!_snapshot) {
        _version->partition().apply(s, mpv, mp_schema);
    } else {
        mutation_partition mp(s.shared_from_this());
        mp.apply(s, mpv, mp_schema);
        auto new_version = current_allocator().construct<partition_version>(std::move(mp));
        new_version->insert_before(*_version);

        set_version(new_version);
    }
}

void partition_entry::apply(const schema& s, partition_entry&& pe, const schema& mp_schema)
{
    auto begin = &*pe._version;
    auto snapshot = pe._snapshot;
    if (pe._snapshot) {
        pe._snapshot->_version = std::move(pe._version);
        pe._snapshot->_entry = nullptr;
        pe._snapshot = nullptr;
    }
    pe._version = { };

    auto current = begin;
    if (!current->next() && !current->is_referenced()) {
        try {
            apply(s, current, mp_schema);
        } catch (...) {
            pe._version = partition_version_ref(*current);
            throw;
        }
        return;
    }

    try {
        while (current && !current->is_referenced()) {
            auto next = current->next();
            apply(s, std::move(current->partition()), mp_schema);
            // Leave current->partition() valid (albeit empty) in case we throw later.
            current->partition() = mutation_partition(mp_schema.shared_from_this());
            current = next;
        }
        while (current) {
            auto next = current->next();
            apply(s, current->partition(), mp_schema);
            current = next;
        }
    } catch (...) {
        if (snapshot) {
            pe._snapshot = snapshot;
            snapshot->_entry = &pe;
            pe._version = std::move(snapshot->_version);
        } else {
            pe._version = partition_version_ref(*begin);
        }
        throw;
    }

    current = begin;
    while (current && !current->is_referenced()) {
        auto next = current->next();
        current_allocator().destroy(current);
        current = next;
    }
    if (current) {
        current->back_reference().mark_as_unique_owner();
    }
}

namespace {

class row_pointer final {
    rows_entry* _ptr;
public:
    row_pointer(const clustering_key& key)
        : _ptr(current_allocator().construct<rows_entry>(key))
    { }
    row_pointer(const row_pointer&) = delete;
    row_pointer& operator=(const row_pointer&) = delete;
    row_pointer(row_pointer&& o) : _ptr(o._ptr) {
        o._ptr = nullptr;
    }
    row_pointer& operator=(row_pointer&& o) {
        _ptr = o._ptr;
        o._ptr = nullptr;
        return *this;
    }
    rows_entry& operator*() const { return *_ptr; }
    rows_entry* operator->() const { return _ptr; }
    ~row_pointer() {
        if (_ptr) {
            current_deleter<rows_entry>()(_ptr);
        }
    }
    void disengage() { _ptr = nullptr; }
};


// When applying partition_entry to an incomplete partition_entry this class is used to represent
// the target incomplete partition_entry. It encapsulates the logic needed for handling multiple versions.
class apply_incomplete_target final {
    struct version {
        mutation_partition::rows_type::iterator current_row;
        mutation_partition::rows_type* rows;
        size_t version_no;

        struct compare {
            const rows_entry::tri_compare& _cmp;
        public:
            explicit compare(const rows_entry::tri_compare& cmp) : _cmp(cmp) { }
            bool operator()(const version& a, const version& b) const {
                auto res = _cmp(*a.current_row, *b.current_row);
                return res > 0 || (res == 0 && a.version_no < b.version_no);
            }
        };
    };
    const schema& _schema;
    partition_entry& _pe;
    rows_entry::tri_compare _rows_cmp;
    rows_entry::compare _rows_less_cmp;
    version::compare _version_cmp;
    std::vector<version> _heap;
    mutation_partition::rows_type::iterator _next_in_latest_version;
public:
    apply_incomplete_target(partition_entry& pe, const schema& schema)
        : _schema(schema)
        , _pe(pe)
        , _rows_cmp(schema)
        , _rows_less_cmp(schema)
        , _version_cmp(_rows_cmp)
    {
        size_t version_no = 0;
        _next_in_latest_version = pe.version()->partition().clustered_rows().begin();
        for (auto&& v : pe.version()->elements_from_this()) {
            if (!v.partition().clustered_rows().empty()) {
                _heap.push_back({v.partition().clustered_rows().begin(), &v.partition().clustered_rows(), version_no});
            }
            ++version_no;
        }
        boost::range::make_heap(_heap, _version_cmp);
    }
    // Move to the first row in target that is not smaller than |lower_bound|.
    // Returns true if the row with |lower_bound| as key should be kept or not.
    bool advance_to(const clustering_key& lower_bound) {
        while (!_heap.empty() && _rows_less_cmp(*_heap[0].current_row, lower_bound)) {
            boost::range::pop_heap(_heap, _version_cmp);
            auto& curr = _heap.back();
            curr.current_row = curr.rows->lower_bound(lower_bound, _rows_less_cmp);
            if (curr.version_no == 0) {
                _next_in_latest_version = curr.current_row;
            }
            if (curr.current_row == curr.rows->end()) {
                _heap.pop_back();
            } else {
                boost::range::push_heap(_heap, _version_cmp);
            }
        }
        return !_heap.empty() && (_heap[0].current_row->continuous() || _rows_cmp(lower_bound, *_heap[0].current_row) == 0);
    }
    // Merge data for a row into the target.
    void apply(row_pointer row) {
        assert(!_heap.empty());
        row->set_continuous(_heap[0].current_row->continuous());
        mutation_partition::rows_type& rows = _pe.version()->partition().clustered_rows();
        if (_next_in_latest_version != rows.end() && _rows_cmp(*row, *_next_in_latest_version) == 0) {
            _next_in_latest_version->apply_reversibly(_schema, *row);
        } else {
            rows.insert_before(_next_in_latest_version, *row);
            row.disengage();
        }
    }
};

} // namespace

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
    partition_version* _version;
    rows_entry::tri_compare _rows_cmp;
    rows_entry::compare _rows_less_cmp;
    version::compare _version_cmp;
    std::vector<version> _heap;
    std::vector<version> _current_row;

public:
    rows_iterator(partition_version* version, const schema& schema)
        : _schema(schema)
        , _version(version)
        , _rows_cmp(schema)
        , _rows_less_cmp(schema)
        , _version_cmp(_rows_cmp)
    {
        bool can_move = true;
        while (_version) {
            can_move &= !_version->is_referenced();
            auto& rows = _version->partition().clustered_rows();
            if (!rows.empty()) {
                _heap.push_back({rows.begin(), &rows, can_move});
            }
            _version = _version->next();
        }
        boost::range::make_heap(_heap, _version_cmp);
        move_to_next_row();
    }
    bool done() const {
        return _current_row.empty();
    }
    // Return clustering key of the current row in source.
    const clustering_key& key() const {
        return _current_row[0].current_row->key();
    }
    row_pointer extract_current_row() {
        assert(!_current_row.empty());
        row_pointer result(_current_row[0].current_row->key());
        // versions in _current_row are not ordered but it is not a problem
        // due to the fact that all rows are continouos.
        for (version& v : _current_row) {
            if (!v.can_move) {
                rows_entry copy(*v.current_row);
                result->apply_reversibly(_schema, copy);
            } else {
                result->apply_reversibly(_schema, *v.current_row);
            }
        }
        return result;
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
        while (!_heap.empty() &&
                (_current_row.empty() || _rows_cmp(*_current_row[0].current_row, *_heap[0].current_row) == 0)) {
            boost::range::pop_heap(_heap, _version_cmp);
            auto& curr = _heap.back();
            _current_row.push_back({curr.current_row, curr.rows, curr.can_move});
            ++curr.current_row;
            if (curr.current_row == curr.rows->end()) {
                _heap.pop_back();
            } else {
                boost::range::push_heap(_heap, _version_cmp);
            }
        }
    }
};

void partition_entry::apply_to_incomplete(const schema& s, partition_entry&& pe, const schema& pe_schema)
{
    bool apply_pe = true;
    if (s.version() != pe_schema.version()) {
        mutation_partition mp(s.shared_from_this());
        partition_entry entry(std::move(mp));
        for (auto&& v : pe.version()->elements_from_this()) {
            entry._version->partition().apply(s, v.partition(), pe_schema);
        }
        apply_to_incomplete(s, &*entry._version);
        apply_pe = false;
    }
    auto current = &*pe._version;
    auto snapshot = pe._snapshot;
    if (pe._snapshot) {
        pe._snapshot->_version = std::move(pe._version);
        pe._snapshot->_entry = nullptr;
        pe._snapshot = nullptr;
    }
    pe._version = { };

    if (apply_pe) {
        try {
            apply_to_incomplete(s, current);
        } catch (...) {
            if (snapshot) {
                pe._snapshot = snapshot;
                snapshot->_entry = &pe;
                pe._version = std::move(snapshot->_version);
            } else {
                pe._version = partition_version_ref(*current);
            }
            throw;
        }
    }

    while (current && !current->is_referenced()) {
        auto next = current->next();
        current_allocator().destroy(current);
        current = next;
    }
    if (current) {
        current->back_reference().mark_as_unique_owner();
    }
}

void partition_entry::apply_to_incomplete(const schema& s, partition_version* version) {
    // Make sure information about static row being cached stays unchanged.
    bool is_static_row_cached = _version->partition().is_static_row_cached();
    auto setup_static_row_cached = defer([this, is_static_row_cached] {
        _version->partition().set_static_row_cached(is_static_row_cached);
    });
    if (bool(_snapshot)) {
        /*
         * We want to always create a new version for a partition if it's being updated with data from memtable.
         * Even when such a new version turns out to be empty.
         * This is important because new reads have to have a new version to populate into.
         * If there is a snapshot then current version is used by older reads and can't be populated by new reads.
         * When new version turns out to be empty it does not mean that update bring no data.
         * That data might not have been merged into cache because of the lack of continuity information.
         * If we don't create a new version here then old reads will populate the version with stale data which
         * ignores new data that came with update.
         */
        mutation_partition mp(mutation_partition::incomplete_tag{}, s);
        mp.set_static_row_cached(is_static_row_cached);
        auto new_version = current_allocator().construct<partition_version>(std::move(mp));
        new_version->insert_before(*_version);
        set_version(new_version);
    }

    bool can_move = true;
    auto current = version;
    while (current) {
        can_move &= !current->is_referenced();
        _version->partition().apply(current->partition().partition_tombstone());
        if (is_static_row_cached) {
            row& static_row = _version->partition().static_row();
            if (can_move) {
                static_row.apply(s, column_kind::static_column, current->partition().static_row());
            } else {
                row copy(current->partition().static_row());
                static_row.apply(s, column_kind::static_column, copy);
            }
        }
        range_tombstone_list& tombstones = _version->partition().row_tombstones();
        if (can_move) {
            tombstones.apply_reversibly(s, current->partition().row_tombstones()).cancel();
        } else {
            range_tombstone_list copy(current->partition().row_tombstones());
            tombstones.apply_reversibly(s, copy).cancel();
        }
        current = current->next();
    }

    partition_entry::rows_iterator source(version, s);
    apply_incomplete_target target(*this, s);

    while (!source.done()) {
        if (target.advance_to(source.key())) {
            target.apply(source.extract_current_row());
        }
        source.remove_current_row_when_possible();
        source.move_to_next_row();
    }
}

mutation_partition partition_entry::squashed(schema_ptr from, schema_ptr to)
{
    mutation_partition mp(to);
    for (auto&& v : _version->all_elements()) {
        mp.apply(*to, v.partition(), *from);
    }
    return mp;
}

void partition_entry::upgrade(schema_ptr from, schema_ptr to)
{
    auto new_version = current_allocator().construct<partition_version>(mutation_partition(to));
    try {
        for (auto&& v : _version->all_elements()) {
            new_version->partition().apply(*to, v.partition(), *from);
        }
    } catch (...) {
        current_allocator().destroy(new_version);
        throw;
    }

    auto old_version = &*_version;
    set_version(new_version);
    remove_or_mark_as_unique_owner(old_version);
}

lw_shared_ptr<partition_snapshot>
partition_entry::read(schema_ptr entry_schema, partition_snapshot::additional_data_type data)
{
    if (_snapshot) {
        if (_snapshot->_data == data) {
            return _snapshot->shared_from_this();
        } else {
            auto new_version = current_allocator().construct<partition_version>(mutation_partition(entry_schema));
            new_version->insert_before(*_version);
            set_version(new_version);
        }
    }
    auto snp = make_lw_shared<partition_snapshot>(entry_schema, this, data);
    _snapshot = snp.get();
    return snp;
}
