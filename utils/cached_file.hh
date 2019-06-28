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

class data_not_cached : public std::runtime_error {
public:
    data_not_cached() : std::runtime_error("data_not_cached") {}
};

/// Passing cache_only::yes means that no I/O will be made to satisfy the request.
/// If the request cannot be satisfied without doing I/O, data_not_cached exception
/// will be throws.
using cache_only = bool_class<class cache_only_tag>;

/// A read-through cache of a file.
///
/// Caches contents with page granularity (4 KiB).
/// Cached pages are evicted manually using the invalidate_*() method family, or when the object is destroyed.
///
/// Concurrent reading is allowed.
///
/// The cached_file can represent a subset of the file. The reason for this is so to satisfy
/// two requirements. One is that we have a page-aligned caching, where pages are aligned
/// relative to the start of the underlying file. This matches requirements of the seastar I/O engine
/// on I/O requests.
/// Another requirement is to have an effective way to populate the cache using an unaligned buffer
/// which starts in the middle of the file when we know that we won't need to access bytes located
/// before the buffer's position. See populate_front(). If we couldn't assume that, we wouldn't be
/// able to insert an unaligned buffer into the cache.
///
class cached_file {
public:
    // Must be aligned to _file.disk_read_dma_alignment(). 4K is always safe.
    static constexpr size_t page_size = 4096;

    // The content of the underlying file (_file) is divided into pages
    // of equal size (page_size). This type is used to identify pages.
    // Pages are assigned consecutive identifiers starting from 0.
    using page_idx_type = uint64_t;

    using offset_type = uint64_t;
private:
    struct cached_page {
        temporary_buffer<char> buf;
        explicit cached_page(temporary_buffer<char> buf) : buf(std::move(buf)) {}
    };

    file _file;
    reader_permit _permit;

    // FIXME: Use radix tree
    std::map<page_idx_type, cached_page> _cache;

    const offset_type _start;
    const offset_type _size;

    offset_type _last_page_size; // Ignores _start in case the start lies on the same page.
    page_idx_type _last_page;
private:
    future<temporary_buffer<char>> get_page(page_idx_type idx, const io_priority_class& pc, cache_only c_only) {
        auto i = _cache.lower_bound(idx);
        if (i != _cache.end() && i->first == idx) {
            cached_page& cp = i->second;
            return make_ready_future<temporary_buffer<char>>(cp.buf.share());
        }
        if (c_only) {
            throw data_not_cached();
        }
        auto buf = temporary_buffer<char>::aligned(_file.memory_dma_alignment(), page_size);
        buf = make_tracked_temporary_buffer(std::move(buf), _permit);
        return _file.dma_read(idx * page_size, buf.get_write(), buf.size(), pc)
            .then([this, idx, buf = std::move(buf)] (size_t size) mutable {
                if (buf.size() != size) [[unlikely]] {
                    if (idx != _last_page || size < _last_page_size) {
                        throw std::runtime_error("Short dma_read()");
                    }
                    buf.trim(_last_page_size);
                }
                _cache.emplace(idx, cached_page(buf.share()));
                return std::move(buf);
            });
    }
public:
    // Generator of subsequent pages of data reflecting the contents of the file.
    // Single-user.
    class stream {
        cached_file* _cached_file;
        const io_priority_class* _pc;
        page_idx_type _page_idx;
        offset_type _offset_in_page;
        cache_only _only_cache;
    public:
        stream() = default;

        stream(cached_file& cf, const io_priority_class& pc, page_idx_type start_page, offset_type start_offset_in_page,
                cache_only c_only)
            : _cached_file(&cf)
            , _pc(&pc)
            , _page_idx(start_page)
            , _offset_in_page(start_offset_in_page)
            , _only_cache(c_only)
        { }

        // Yields the next chunk of data.
        // Returns empty buffer when end-of-stream is reached.
        // Calls must be serialized.
        // This instance must be kept alive until the returned future resolves.
        future<temporary_buffer<char>> next() {
            if (_page_idx > _cached_file->_last_page || !_cached_file) {
                return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>());
            }
            return _cached_file->get_page(_page_idx, *_pc, _only_cache).then([this] (temporary_buffer<char> page) {
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
public:
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

    // Populates cache from buf assuming that buf contains the data from the front of the cached area.
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

        if (buf.size() == page_size || (idx == _last_page && buf.size() >= _last_page_size)) {
            _cache.emplace(idx, cached_page(std::move(buf)));
        }
    }

    // Invalidates [start, end) or less.
    //
    // Invariants:
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

    /// \brief Equivalent to \ref invalidate_at_most(0, end).
    void invalidate_at_most_front(offset_type end) {
        _cache.erase(_cache.begin(), _cache.lower_bound((_start + end) / page_size));
    }

    /// \brief Read from the file
    ///
    /// Returns a stream with data which starts at position pos in the area managed by this instance.
    /// Thios cached_file instance must outlive the returned stream.
    /// The stream does not do any read-ahead.
    ///
    /// \param pos The offset of the first byte to read, relative to the cached file area.
    /// \param c_only When cache_only::yes is passed, the stream will throw data_not_cached exception if request
    ///               cannot be satisfied from the cache.
    ///
    stream read(offset_type pos, const io_priority_class& pc, cache_only c_only = cache_only::no) {
        pos = std::min(pos, _size);
        auto global_pos = _start + pos;
        auto offset = global_pos % page_size;
        auto page_idx = global_pos / page_size;
        return stream(*this, pc, page_idx, offset, c_only);
    }

    /// \brief Returns the number of bytes in the area managed by this instance.
    offset_type size() const {
        return _size;
    }

    /// \brief Returns the number of bytes cached.
    size_t cached_bytes() const {
        return _cache.size() * page_size;
    }
};
