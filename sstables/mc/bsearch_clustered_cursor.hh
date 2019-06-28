/*
 * Copyright (C) 2019 ScyllaDB
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

#include "sstables/index_entry.hh"
#include "parsers.hh"
#include "schema.hh"
#include "utils/cached_file.hh"

#include <seastar/core/byteorder.hh>

namespace sstables {
namespace mc {

// Cursor implementation which does binary search over index entries.
//
// Memory complexity: O(1)
//
// Average cost of first lookup:
//
//    comparisons: O(log(N))
//    I/O:         O(log(N))
//
// N = number of index entries
//
class bsearch_clustered_cursor : public clustered_index_cursor {
    using pi_offset_type = uint32_t; // Offset into the promoted index.
    using pi_index_type = uint32_t; // promoted index block sequence number, 0 .. _blocks_count

    const schema& _s;
    const pi_index_type _blocks_count;
    const io_priority_class _pc;

    cached_file _promoted_index;

    // Points to the upper bound of the cursor.
    pi_index_type _current_idx = 0;

    // Points to the upper bound of the cursor.
    std::optional<position_in_partition> _current_pos;

    data_consumer::primitive_consumer _primitive_parser;
    clustering_parser _clustering_parser;
    promoted_index_block_parser _block_parser;
    cached_file::stream _stream;
    pi_index_type _upper_idx;
private:
    template <typename Consumer>
    static future<> consume_stream(cached_file::stream& s, Consumer& c) {
        return repeat([&] {
            return s.next().then([&] (temporary_buffer<char>&& buf) {
                if (buf.empty()) {
                    throw std::runtime_error(fmt::format("End of stream while parsing"));
                }
                return stop_iteration(c.consume(buf) == data_consumer::read_status::ready);
            });
        });
    }

    // idx must be in 0..(_blocks_count-1)
    future<pi_offset_type> get_block_offset(pi_index_type idx) {
        auto offset_entry_pos = _promoted_index.size() - (_blocks_count - idx) * sizeof(pi_offset_type);
        _stream = _promoted_index.read(offset_entry_pos, _pc);
        return _stream.next().then([this, idx] (temporary_buffer<char>&& buf) {
            if (__builtin_expect(_primitive_parser.read_32(buf) == data_consumer::read_status::ready, true)) {
                return make_ready_future<uint32_t>(_primitive_parser._u32);
            }
            return consume_stream(_stream, _primitive_parser).then([this] {
                return _primitive_parser._u32;
            });
        });
    }

    // Returns the starting position of promoted index block of index idx.
    // idx must be in 0..(_blocks_count-1)
    future<position_in_partition> get_block_start(pi_index_type idx) {
        return get_block_offset(idx).then([idx, this] (pi_offset_type offset) {
            _stream = _promoted_index.read(offset, _pc);
            return consume_stream(_stream, _clustering_parser).then([this] {
                return _clustering_parser.get_and_reset();
            });
        });
    }

    // Parses promoted index block of index idx.
    // The result of parsing can be collected from _block_parser.
    // idx must be in 0..(_blocks_count-1)
    future<> parse_block(pi_index_type idx) {
        // TODO: Cache last two blocks because advance_to() requests the predecesor
        // which may have been already parsed when scanning through the index.
        return get_block_offset(idx).then([idx, this] (pi_offset_type offset) {
            _stream = _promoted_index.read(offset, _pc);
            _block_parser.reset();
            return consume_stream(_stream, _block_parser);
        });
    }

    // Advances the cursor to the nearest block whose start position is > pos.
    //
    // upper_idx should be the index of the block which is known to have start position > pos.
    // upper_idx can be set to _blocks_count if no such entry is known.
    future<> advance_to_upper_bound(position_in_partition_view pos) {
        // Binary search over blocks.
        //
        // Post conditions:
        //
        //   pos < get_block_start(_current_idx)
        //   For each i < _current_idx: pos >= get_block_start(i)
        //
        // Invariants:
        //
        //   pos < get_block_start(_upper_idx) [*]
        //   pos >= get_block_start(_current_idx)
        //
        // [*] Assuming get_block_start(_blocks_count) == position_in_partition::after_all_clustered_rows(),
        // the algorithm never actually invokes get_block_start() for _blocks_count, which is an invalid argument.
        //
        // Eventually _current_idx will reach _upper_idx.

        _upper_idx = _blocks_count;
        return repeat([this, pos] {
            if (_current_idx >= _upper_idx) {
                if (_current_idx == _blocks_count) {
                    _current_pos = position_in_partition::after_all_clustered_rows();
                }
                sstlog.trace("mc_bsearch_clustered_cursor {}: bisecting done, current=[{}] .start={}", this, _current_idx, _current_pos);
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }

            // TODO: increase the size of I/O issued by get_block_start() as the range gets narrower.
            // The rationale is that issuing one large I/O is less expensive than issuing many small I/Os
            // and if the range is narrow the extra data surrounding the pivot point has a high chance
            // of being subseqently needed.

            auto mid = _current_idx + (_upper_idx - _current_idx) / 2;
            sstlog.trace("mc_bsearch_clustered_cursor {}: bisecting range [{}, {}], mid={}", this, _current_idx, _upper_idx, mid);
            return get_block_start(mid).then([this, mid, pos] (position_in_partition mid_pos) {
                position_in_partition::less_compare less(_s);
                sstlog.trace("mc_bsearch_clustered_cursor {}: compare with [{}] .start={}", this, mid, mid_pos);
                if (less(pos, mid_pos)) {
                    // Eventually _current_idx will reach _upper_idx, so _current_pos only needs to be
                    // updated whenever _upper_idx changes.
                    _current_pos = std::move(mid_pos);
                    _upper_idx = mid;
                } else {
                    _current_idx = mid + 1;
                }
                return stop_iteration::no;
            });
        });
    }
public:
    bsearch_clustered_cursor(const schema& s,
            reader_permit permit,
            column_values_fixed_lengths cvfl,
            cached_file f,
            io_priority_class pc,
            pi_index_type blocks_count)
        : _s(s)
        , _blocks_count(blocks_count)
        , _pc(pc)
        , _promoted_index(std::move(f))
        , _primitive_parser(permit)
        , _clustering_parser(s, permit, cvfl, true)
        , _block_parser(s, std::move(permit), std::move(cvfl))
    { }

    future<std::optional<skip_info>> advance_to(position_in_partition_view pos) override {
        position_in_partition::less_compare less(_s);

        sstlog.trace("mc_bsearch_clustered_cursor {}: advance_to({}), _current_pos={}, _current_idx={}",
            this, pos, _current_pos, _current_idx);

        if (_current_pos) {
            if (less(pos, *_current_pos)) {
                sstlog.trace("mc_bsearch_clustered_cursor {}: same block", this);
                return make_ready_future<std::optional<skip_info>>(std::nullopt);
            }
            ++_current_idx;
        }

        return advance_to_upper_bound(pos).then([this] {
            if (_current_idx == 0) {
                sstlog.trace("mc_bsearch_clustered_cursor {}: same block", this);
                return make_ready_future<std::optional<skip_info>>(std::nullopt);
            }
            return parse_block(_current_idx - 1).then([this] {
                offset_in_partition datafile_offset = _block_parser.offset();
                sstlog.trace("mc_bsearch_clustered_cursor {}: datafile_offset={}", this, datafile_offset);
                if (_current_idx < 2) {
                    return make_ready_future<std::optional<skip_info>>(
                        skip_info{datafile_offset, tombstone(), position_in_partition::before_all_clustered_rows()});
                }
                return parse_block(_current_idx - 2).then([this, datafile_offset] () -> std::optional<skip_info> {
                    if (!_block_parser.end_open_marker()) {
                        return skip_info{datafile_offset, tombstone(), position_in_partition::before_all_clustered_rows()};
                    }
                    auto tomb = tombstone(*_block_parser.end_open_marker());
                    sstlog.trace("mc_bsearch_clustered_cursor {}: tombstone={}, pos={}", this, tomb, _block_parser.end());
                    return skip_info{datafile_offset, tomb, std::move(_block_parser.end())};
                });
            });
        });
    }

    future<std::optional<offset_in_partition>> probe_upper_bound(position_in_partition_view pos) override {
        // FIXME: Implement
        return make_ready_future<std::optional<offset_in_partition>>(std::nullopt);
    }

    future<std::optional<entry_info>> next_entry() override {
        if (_current_idx == _blocks_count) {
            return make_ready_future<std::optional<entry_info>>(std::nullopt);
        }
        return parse_block(_current_idx).then([this] () -> std::optional<entry_info> {
            sstlog.trace("mc_bsearch_clustered_cursor {}: block {}: start={}, end={}, offset={}", this, _current_idx,
                _block_parser.start(), _block_parser.end(), _block_parser.offset());
            ++_current_idx;
            return entry_info{std::move(_block_parser.start()), std::move(_block_parser.end()), _block_parser.offset()};
        });
    }

    future<> close() override {
        return make_ready_future<>();
    }
};

}
}
