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

#include "reader_permit.hh"
#include "utils/div_ceil.hh"

#include <seastar/core/file.hh>

#include <map>

using namespace seastar;

/// An in-memory read-through cache of a subset of a file.
///
/// Caches contents with page granularity (4KiB).
/// Cached pages are evicted manually or when the object is destroyed.
///
/// The reason this represents a subset rather than the whole range is so that we can
/// have a page-aligned caching and an effective populate_front() which can be given
/// an unaligned buffer. If the cached_file represented the whole range, we wouldn't be
/// able to populate a range starting at an unaligned address.
///
class cached_file {
public:
    using offset_type = uint64_t;

    // The content of the underlying file (_file) is divided into pages
    // of equal size (page_size). This type is used to identify pages.
    // Pages are assigned consecutive identifiers starting from 0.
    using page_idx_type = uint64_t;

    // Must be aligned to _file.disk_read_dma_alignment(). 4K is always safe.
    static constexpr size_t page_size = 4096;
private:
    struct cached_page {
        temporary_buffer<char> buf;
        cached_page(temporary_buffer<char> buf) : buf(std::move(buf)) {}
    };

    file _file;
    reader_permit _permit;
    std::map<page_idx_type, cached_page> _cache;

    const offset_type _start;
    const offset_type _size;

    offset_type _last_page_size; // Ignores _start in case the start lies on the same page.
    page_idx_type _last_page;
private:
    // io_size must be aligned to page_size
    future<temporary_buffer<char>> get_page(page_idx_type idx, const io_priority_class& pc, size_t io_size) {
        auto i = _cache.lower_bound(idx);
        if (i != _cache.end() && i->first == idx) {
            cached_page& cp = i->second;
            return make_ready_future<temporary_buffer<char>>(cp.buf.share());
        }

        auto io_pages = io_size / page_size;

        page_idx_type leading_pages = std::min(io_pages / 2, idx); // Holds number of pages with index < idx
        if (i != _cache.begin()) {
            page_idx_type prev_idx = std::prev(i)->first;
            leading_pages = std::min(leading_pages, idx - prev_idx - 1);
        }

        const auto pages_up_to_area_end = _last_page - idx + 1;
        page_idx_type trailing_pages = std::min(div_ceil(io_pages, 2), pages_up_to_area_end); // Holds number of pages with index >= idx
        if (i != _cache.end()) {
            page_idx_type next_idx = std::next(i)->first;
            trailing_pages = std::min(trailing_pages, next_idx - idx);
        }

        auto read_idx = idx - leading_pages;
        auto buf = temporary_buffer<char>::aligned(_file.memory_dma_alignment(), (leading_pages + trailing_pages) * page_size);
        buf = make_tracked_temporary_buffer(std::move(buf), _permit);
        //sstables::sstlog.trace("get_page {} {} {} {} {} {}", idx, io_size, read_idx * page_size, buf.size(), leading_pages, trailing_pages);
        return _file.dma_read(read_idx * page_size, buf.get_write(), buf.size(), pc)
            .then([this, idx, leading_pages, read_idx, buf = std::move(buf)] (size_t size) mutable {
                buf.trim(size);
                populate_pages(read_idx, buf.share());
                buf.trim_front(leading_pages * page_size);
                buf.trim(std::min(size, page_size));
                return std::move(buf);
            });
    }

    future<temporary_buffer<char>> get_page(page_idx_type idx, const io_priority_class& pc) {
        auto i = _cache.lower_bound(idx);
        if (i != _cache.end() && i->first == idx) {
            cached_page& cp = i->second;
            return make_ready_future<temporary_buffer<char>>(cp.buf.share());
        }
        auto buf = temporary_buffer<char>::aligned(_file.memory_dma_alignment(), page_size);
        buf = make_tracked_temporary_buffer(std::move(buf), _permit);
        return _file.dma_read(idx * page_size, buf.get_write(), buf.size(), pc)
            .then([this, idx, buf = std::move(buf)] (size_t size) mutable {
                buf.trim(size);
                _cache.emplace(idx, cached_page(buf.share()));
                return std::move(buf);
            });
    }
public:
    // Generator for subsequent pages of data read from the file.
    class stream {
        cached_file* _cached_file;
        const io_priority_class* _pc;
        page_idx_type _page_idx;
        offset_type _offset_in_page;
        size_t _io_size;
    public:
        stream() = default;

        stream(cached_file& cf, const io_priority_class& pc, page_idx_type start_page, offset_type start_offset_in_page, size_t io_size)
            : _cached_file(&cf)
            , _pc(&pc)
            , _page_idx(start_page)
            , _offset_in_page(start_offset_in_page)
            , _io_size(io_size)
        { }

        // Yields the next chunk of data.
        // Returns empty buffer when end-of-stream is reached.
        // Calls must be serialized.
        future<temporary_buffer<char>> next() {
            if (_page_idx > _cached_file->_last_page || !_cached_file) {
                return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>());
            }
            return _cached_file->get_page(_page_idx, *_pc, _io_size).then([this] (temporary_buffer<char> page) {
                _io_size = page_size;
                if (_page_idx == _cached_file->_last_page) {
                    page.trim(_cached_file->_last_page_size);
                }
                page.trim_front(_offset_in_page);
                _offset_in_page = 0;
                ++_page_idx;
                return page;
            });
        }
    };

    void populate_pages(page_idx_type idx, temporary_buffer<char> buf) {
        while (buf.size() > page_size) {
            auto page_buf = buf.share();
            page_buf.trim(page_size);
            _cache.emplace(idx, cached_page(std::move(page_buf)));
            buf.trim_front(page_size);
            ++idx;
        }

        _cache.emplace(idx, cached_page(std::move(buf)));
    }
public:
    // This instance will represent a subset of f consisting of bytes from the range [start, start + size).
    // All offsets passed to public methods are relative to that subset's start.
    cached_file(file f, reader_permit permit, offset_type start, offset_type size)
        : _file(std::move(f))
        , _permit(std::move(permit))
        , _start(start)
        , _size(size)
    {
        offset_type last_byte_offset = _start + (_size ? (_size - 1) : 0);
        _last_page_size = (last_byte_offset % page_size) + (_size ? 1 : 0);
        _last_page = last_byte_offset / page_size;
    }

    // Populates cache from buf assuming that buf contains the data at the front of the area.
    void populate_front(temporary_buffer<char> buf) {
        // Align to page start. We can do this because the junk before _start won't be accessed.
        auto pad = _start % page_size;
        auto idx = _start / page_size;
        buf = temporary_buffer<char>(buf.get_write() - pad, buf.size() + pad, buf.release());

        while (buf.size() > page_size) {
            auto page_buf = buf.share();
            page_buf.trim(page_size);
            _cache.emplace(idx, cached_page(std::move(page_buf)));
            buf.trim_front(page_size);
            ++idx;
        }

        if (buf.size() == page_size) {
            _cache.emplace(idx, cached_page(std::move(buf)));
        }
    }

    // Invalidates [start, end) or less.
    //
    // Postconditions:
    //
    //   - all bytes outside [start, end) which were cached before the call will still be cached.
    //
    void invalidate_at_most(offset_type start, offset_type end) {
        auto lo_page = (_start + start - std::min<offset_type>(start, 1)) / page_size + 1;
        auto hi_page = (_start + end) / page_size;
        if (lo_page < hi_page) {
            _cache.erase(_cache.lower_bound(lo_page), _cache.lower_bound(hi_page));
        }
    }

    // Equivalent to invalidate_at_most(0, end).
    void invalidate_at_most_front(offset_type end) {
        _cache.erase(_cache.begin(), _cache.lower_bound((_start + end) / page_size));
    }

    // Returns a stream with data which starts at position pos in the area managed by this instance.
    // The stream does not do any read-ahead.
    // io_size must be a multiple of page_size.
    stream read(offset_type pos, const io_priority_class& pc, size_t io_size = page_size) {
        pos = std::min(pos, _size);
        auto global_pos = _start + pos;
        auto offset = global_pos % page_size;
        auto page_idx = global_pos / page_size;
        return stream(*this, pc, page_idx, offset, io_size);
    }

    // Number of bytes in the area managed by this instance.
    offset_type size() const {
        return _size;
    }

    size_t cached_bytes() const {
        return _cache.size() * page_size;
    }
};
