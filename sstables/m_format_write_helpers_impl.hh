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

#include <functional>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/container/static_vector.hpp>

#include "encoding_stats.hh"
#include "schema.hh"
#include "mutation_fragment.hh"
#include "vint-serialization.hh"
#include "sstables/types.hh"
#include "sstables/m_format_write_helpers.hh"
#include "sstables/writer.hh"

namespace sstables {

// A helper CRTP base class for input ranges.
// Derived classes should implement the following functions:
//      bool next() const;
//          generates the next value, if possible;
//          returns true if the next value has been evaluated, false otherwise
//      explicit operator bool() const;
//          tells whether the range can produce more items
// TODO: turn description into a concept
template <typename InputRange, typename ValueType>
struct input_range_base {
private:

    InputRange& self() {
        return static_cast<InputRange&>(*this);
    }

    const InputRange& self() const {
        return static_cast<const InputRange&>(*this);
    }

public:
    // Use the same type for iterator and const_iterator
    using const_iterator = class iterator
            : public boost::iterator_facade<
                    iterator,
                    const ValueType,
                    std::input_iterator_tag,
                    const ValueType
            >
    {
    private:
        const InputRange* _range;

        friend class input_range_base;
        friend class boost::iterator_core_access;

        explicit iterator(const InputRange& range)
                : _range(range.next() ? &range : nullptr)
        {}

        void increment() {
            assert(_range);
            if (!_range->next()) {
                _range = nullptr;
            }
        }

        bool equal(iterator that) const {
            return (_range == that._range);
        }

        const ValueType dereference() const {
            assert(_range);
            return _range->get_value();
        }

    public:
        iterator() : _range{} {}

    };

    iterator begin() const { return iterator{self()}; }
    iterator end() const   { return iterator{}; }
};

struct clustering_block {
    constexpr static size_t max_block_size = 32;
    uint64_t header = 0;
    struct described_value {
        bytes_view value;
        std::reference_wrapper<const abstract_type> type;
    };
    boost::container::static_vector<described_value, clustering_block::max_block_size> values;
};

class clustering_blocks_input_range
        : public input_range_base<clustering_blocks_input_range, clustering_block> {
private:
    const schema& _schema;
    const clustering_key_prefix& _prefix;
    size_t _serialization_limit_size;
    mutable clustering_block _current_block;
    mutable uint32_t _offset = 0;

public:
    clustering_blocks_input_range(const schema& s, const clustering_key_prefix& prefix, ephemerally_full_prefix is_ephemerally_full)
            : _schema(s)
            , _prefix(prefix) {
        _serialization_limit_size = is_ephemerally_full == ephemerally_full_prefix::yes
                                    ? _schema.clustering_key_size()
                                    : _prefix.size(_schema);
    }

    bool next() const {
        if (_offset == _serialization_limit_size) {
            // No more values to encode
            return false;
        }

        // Each block contains up to max_block_size values
        auto limit = std::min(_serialization_limit_size, _offset + clustering_block::max_block_size);

        _current_block = {};
        assert (_offset % clustering_block::max_block_size == 0);
        while (_offset < limit) {
            auto shift = _offset % clustering_block::max_block_size;
            if (_offset < _prefix.size(_schema)) {
                bytes_view value = _prefix.get_component(_schema, _offset);
                if (value.empty()) {
                    _current_block.header |= (uint64_t(1) << (shift * 2));
                } else {
                    _current_block.values.push_back({value, *_prefix.get_compound_type(_schema)->types()[_offset]});
                }
            } else {
                // This (and all subsequent) values of the prefix are missing (null)
                // This branch is only ever taken for an ephemerally_full_prefix
                _current_block.header |= (uint64_t(1) << ((shift * 2) + 1));
            }
            ++_offset;
        }
        return true;
    }

    clustering_block get_value() const { return _current_block; };

    explicit operator bool() const {
        return (_offset < _serialization_limit_size);
    }
};

// Writes cell value according to its data type traits
// NOTE: this function is defined in sstables/sstables.cc
template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_cell_value(W& out, const abstract_type& type, bytes_view value);

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
static void write(W& out, const clustering_block& block) {
    write_vint(out, block.header);
    for (const auto& [value, type]: block.values) {
        write_cell_value(out, type, value);
    }
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_clustering_prefix(W& out, const schema& s,
        const clustering_key_prefix& prefix, ephemerally_full_prefix is_ephemerally_full) {
    clustering_blocks_input_range range{s, prefix, is_ephemerally_full};
    for (const auto block: range) {
        write(out, block);
    }
}

// This range generates a sequence of values that represent information
// about missing columns for SSTables 3.0 format.
class missing_columns_input_range
        : public input_range_base<missing_columns_input_range, uint64_t> {
private:
    const indexed_columns& _columns;
    const row& _row;
    mutable uint64_t _current_value = 0;
    mutable size_t _current_index = 0;
    mutable bool _large_mode_produced_size = false;

    enum class encoding_mode {
        small,
        large_encode_present,
        large_encode_missing,
    } _mode;

public:
    missing_columns_input_range(const indexed_columns& columns, const row& row)
            : _columns(columns)
            , _row(row) {

        auto row_size = _row.size();
        auto total_size = _columns.size();

        _current_index = row_size < total_size ? 0 : total_size;
        _mode = (total_size < 64)           ? encoding_mode::small :
                (row_size < total_size / 2) ? encoding_mode::large_encode_present :
                encoding_mode::large_encode_missing;
    }

    bool next() const {
        auto total_size = _columns.size();
        if (_current_index == total_size) {
            // No more values to encode
            return false;
        }

        if (_mode ==  encoding_mode::small) {
            // Set bit for every missing column
            for (const auto& element: _columns | boost::adaptors::indexed()) {
                auto cell = _row.find_cell(element.value().get().id);
                if (!cell) {
                    _current_value |= (uint64_t(1) << element.index());
                }
            }
            _current_index = total_size;
            return true;
        } else {
            // For either of large modes, output the difference between total size and row size first
            if (!_large_mode_produced_size) {
                _current_value = total_size - _row.size();
                _large_mode_produced_size = true;
                return true;
            }

            if (_mode == encoding_mode::large_encode_present) {
                while (_current_index < total_size) {
                    auto cell = _row.find_cell(_columns[_current_index].get().id);
                    if (cell) {
                        _current_value = _current_index;
                        ++_current_index;
                        return true;
                    }
                    ++_current_index;
                }
            } else {
                assert(_mode == encoding_mode::large_encode_missing);
                while (_current_index < total_size) {
                    auto cell = _row.find_cell(_columns[_current_index].get().id);
                    if (!cell) {
                        _current_value = _current_index;
                        ++_current_index;
                        return true;
                    }
                    ++_current_index;
                }
            }
        }

        return false;
    }

    uint64_t get_value() const { return _current_value; }

    explicit operator bool() const
    {
        return (_current_index < _columns.size());
    }
};

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_missing_columns(W& out, const indexed_columns& columns, const row& row) {
    for (const auto value: missing_columns_input_range{columns, row}) {
        write_vint(out, value);
    }
}

template <typename T, typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_unsigned_delta_vint(W& out, T value, T base) {
    using unsigned_type = std::make_unsigned_t<T>;
    unsigned_type delta = static_cast<unsigned_type>(value) - base;
    write_vint(out, delta);
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_delta_timestamp(W& out, api::timestamp_type timestamp, const encoding_stats& enc_stats) {
    write_unsigned_delta_vint(out, timestamp, enc_stats.min_timestamp);
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_delta_ttl(W& out, uint32_t ttl, const encoding_stats& enc_stats) {
    write_unsigned_delta_vint(out, ttl, enc_stats.min_ttl);
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_delta_local_deletion_time(W& out, uint32_t local_deletion_time, const encoding_stats& enc_stats) {
    write_unsigned_delta_vint(out, local_deletion_time, enc_stats.min_local_deletion_time);
}

template <typename W>
GCC6_CONCEPT(requires Writer<W>())
void write_delta_deletion_time(W& out, deletion_time dt, const encoding_stats& enc_stats) {
    write_delta_timestamp(out, dt.marked_for_delete_at, enc_stats);
    write_delta_local_deletion_time(out, dt.local_deletion_time, enc_stats);
}

};  // namespace sstables

