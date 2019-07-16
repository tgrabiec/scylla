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


// A file access interface with content caching.
// The underlying file is assumed to not change contents.
// File altering methods like writing, truncating, etc. are not supported and will throw.
class cached_file : public file_impl {
    friend class stream;
public:
    using offset_type = uint64_t;
    using page_idx_type = uint64_t;

    // Must be aligned to _file.disk_read_dma_alignment(). 4K is always safe.
    static const size_t page_size = 4096;
private:
    struct cached_page {
        temporary_buffer<char> buf;
        cached_page(temporary_buffer<char> buf) : buf(std::move(buf)) {}
    };
    file _file;
    reader_permit _permit;
    std::map<page_idx_type, cached_page> _cache;
    const offset_type _size;
    page_idx_type _last_page;
private:
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

    future<temporary_buffer<char>> get_page(page_idx_type idx, const io_priority_class& pc, size_t io_size) {
        auto i = _cache.lower_bound(idx);
        if (i != _cache.end() && i->first == idx) {
            cached_page& cp = i->second;
            return make_ready_future<temporary_buffer<char>>(cp.buf.share());
        }

        auto io_pages = io_size / page_size;

        page_idx_type leading_pages = 0; // Holds number of pages to read which have index < idx
        if (i != _cache.begin()) {
            page_idx_type prev_idx = std::prev(i)->first;
            leading_pages = std::min(io_pages / 2, idx - prev_idx - 1);
        }

        const auto pages_up_to_area_end = _last_page - idx + 1;
        page_idx_type trailing_pages = pages_up_to_area_end; // Holds number of pages to read with index >= idx
        if (i != _cache.end()) {
            page_idx_type next_idx = std::next(i)->first;
            trailing_pages = std::min(trailing_pages, next_idx - idx);
        }

        auto read_idx = idx - leading_pages;
        auto buf = temporary_buffer<char>::aligned(_file.memory_dma_alignment(), (leading_pages + trailing_pages) * page_size);
        buf = make_tracked_temporary_buffer(std::move(buf), _permit);
        return _file.dma_read(read_idx * page_size, buf.get_write(), buf.size(), pc).then(
                [this, leading_pages, read_idx, buf = std::move(buf)] (size_t size) mutable {
            buf.trim(size);
            populate_pages(read_idx, buf.share());
            buf.trim_front(leading_pages * page_size);
            buf.trim(std::min(size, page_size));
            return std::move(buf);
        });
    }

    void unsupported() {
        throw_with_backtrace<std::runtime_error>("unsupported operation on a cached_file");
    }
public: /* file_impl */
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        // FIXME
    }

    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        // FIXME
    }

    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        // FIXME
    }

    future<struct stat> stat() override {
        return _file.stat();
    }

    future<uint64_t> size(void) override {
        return make_ready_future<uint64_t>(_size);
    }

    future<> close() override {
        return _file.close();
    }

    std::unique_ptr<file_handle_impl> dup() {
        // FIXME: Don't loose caching properties
        return _file.dup();
    }

    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return _file.list_directory(std::move(next));
    }

    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override { unsupported(); }
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override { unsupported(); }
    future<> flush() override { unsupported(); }
    future<> truncate(uint64_t length) override { unsupported(); }
    future<> discard(uint64_t offset, uint64_t length) override { unsupported(); }
    future<> allocate(uint64_t position, uint64_t length) override { unsupported(); }
public:
    cached_file(file f, offset_type start, offset_type size)
        : _file(std::move(f))
        , _size(size)
    {
        offset_type last_byte_offset = _size ? (_size - 1) : 0;
        _last_page = last_byte_offset / page_size;
    }
};


// impl raw data source impl
// so that we can create an input stream with it
//

class file_slice {
    friend class stream;
public:
    using offset_type = uint64_t;
private:
    cached_file _file;
    const offset_type _start;
    const offset_type _size;
    offset_type _last_page_size;

public:
    // Generator for subsequent pages of data read from the file.
    class stream {
        file_slice* _f;
        const io_priority_class* _pc;
        page_idx_type _page_idx;
        offset_type _offset_in_page;
        size_t _io_size;
    public:
        stream() = default;

        stream(file_slice& f, const io_priority_class& pc, page_idx_type start_page, offset_type start_offset_in_page, size_t io_size)
            : _f(&f)
            , _pc(&pc)
            , _page_idx(start_page)
            , _offset_in_page(start_offset_in_page)
            , _io_size(io_size)
        { }

        // Yields the next chunk of data.
        // Returns empty buffer when end-of-stream is reached.
        // Calls must be serialized.
        future<temporary_buffer<char>> next() {
            if (_page_idx > _f->_last_page || !_f) {
                return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>());
            }
            return _f->get_page(_page_idx, *_pc, _io_size).then([this] (temporary_buffer<char> page) {
                _io_size = page_size;
                if (_page_idx == _f->_last_page) {
                    page.trim(_f->_last_page_size);
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
    file_slice(file f, reader_permit permit, offset_type start, offset_type size)
        : _file(std::move(f))
        , _permit(std::move(permit))
        , _start(start)
        , _size(size)
    {
        offset_type last_byte_offset = _start + (_size ? (_size - 1) : 0);
        _last_page_size = (last_byte_offset % page_size) + (_size ? 1 : 0);
        _last_page = last_byte_offset / page_size;
    }

    // Returns a stream representing the subset of the file starting at pos.
    // The stream does not do any read-ahead.
    // io_size must be a multiple of page_size.
    stream read(offset_type pos, const io_priority_class& pc, size_t io_size = page_size) {
        pos = std::min(pos, _size);
        auto global_pos = _start + pos;
        auto offset = global_pos % page_size;
        auto page_idx = global_pos / page_size;
        return stream(*this, pc, page_idx, offset, io_size);
    }

    offset_type size() const { return _size; }
};
