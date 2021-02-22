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
#include "utils/bptree.hh"
#include "utils/lru.hh"
#include "tracing/trace_state.hh"

#include <seastar/core/file.hh>

#include <map>

using namespace seastar;

/// \brief A read-through cache of a file.
///
/// Caches contents with page granularity (4 KiB).
/// Cached pages are evicted manually using the invalidate_*() method family, or when the object is destroyed.
///
/// Concurrent reading is allowed.
///
/// The object is movable but this is only allowed before readers are created.
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

    struct metrics {
        uint64_t page_hits = 0;
        uint64_t page_misses = 0;
        uint64_t page_evictions = 0;
        uint64_t page_populations = 0;
        uint64_t cached_bytes = 0;
    };
private:
    class cached_page : public evictable {
    public:
        cached_file* parent;
        page_idx_type idx;
        temporary_buffer<char> buf;
        explicit cached_page(cached_file* parent, page_idx_type idx, temporary_buffer<char> buf)
            : parent(parent)
            , idx(idx)
            , buf(std::move(buf))
        { }
        cached_page(cached_page&&) noexcept = default;
        void on_evicted() noexcept override;
    };

    struct page_idx_less_comparator {
        bool operator()(page_idx_type lhs, page_idx_type rhs) const noexcept {
            return lhs < rhs;
        }
    };

    file _file;
    sstring _file_name; // for logging / tracing
    metrics& _metrics;
    lru& _lru;

    using cache_type = bplus::tree<page_idx_type, cached_page, page_idx_less_comparator, 12, bplus::key_search::linear>;
    cache_type _cache;

    const offset_type _size;
    offset_type _cached_bytes = 0;

    offset_type _last_page_size;
    page_idx_type _last_page;
private:
    future<temporary_buffer<char>> get_page(page_idx_type idx, const io_priority_class& pc,
            tracing::trace_state_ptr trace_state) {
        auto i = _cache.lower_bound(idx);
        if (i != _cache.end() && i->idx == idx) {
            ++_metrics.page_hits;
            tracing::trace(trace_state, "page cache hit: file={}, page={}", _file_name, idx);
            cached_page& cp = *i;
            _lru.touch(cp);
            return make_ready_future<temporary_buffer<char>>(cp.buf.share());
        }
        tracing::trace(trace_state, "page cache miss: file={}, page={}", _file_name, idx);
        ++_metrics.page_misses;
        auto size = idx == _last_page ? _last_page_size : page_size;
        return _file.dma_read_exactly<char>(idx * page_size, size, pc)
            .then([this, idx] (temporary_buffer<char>&& buf) mutable {
                ++_metrics.page_populations;
                _metrics.cached_bytes += buf.size();
                _cached_bytes += buf.size();
                auto it_and_flag = _cache.emplace(idx, cached_page(this, idx, buf.share()));
                _lru.add(*it_and_flag.first);
                return std::move(buf);
            });
    }
public:
    // Generator of subsequent pages of data reflecting the contents of the file.
    // Single-user.
    class stream {
        cached_file* _cached_file;
        const io_priority_class* _pc;
        std::optional<reader_permit> _permit;
        page_idx_type _page_idx;
        offset_type _offset_in_page;
        tracing::trace_state_ptr _trace_state;
    public:
        // Creates an empty stream.
        stream()
            : _cached_file(nullptr)
            , _pc(nullptr)
        { }

        stream(cached_file& cf, const io_priority_class& pc, std::optional<reader_permit> permit, tracing::trace_state_ptr trace_state,
                page_idx_type start_page, offset_type start_offset_in_page)
            : _cached_file(&cf)
            , _pc(&pc)
            , _permit(std::move(permit))
            , _page_idx(start_page)
            , _offset_in_page(start_offset_in_page)
            , _trace_state(std::move(trace_state))
        { }

        // Yields the next chunk of data.
        // Returns empty buffer when end-of-stream is reached.
        // Calls must be serialized.
        // This instance must be kept alive until the returned future resolves.
        future<temporary_buffer<char>> next() {
            if (!_cached_file || _page_idx > _cached_file->_last_page) {
                return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>());
            }
            return _cached_file->get_page(_page_idx, *_pc, _trace_state).then([this] (temporary_buffer<char> page) {
                if (_page_idx == _cached_file->_last_page) {
                    page.trim(_cached_file->_last_page_size);
                }
                if (_permit) {
                    page = make_tracked_temporary_buffer(std::move(page), *_permit);
                }
                page.trim_front(_offset_in_page);
                _offset_in_page = 0;
                ++_page_idx;
                return page;
            });
        }
    };

    void on_evicted(cached_page& p) {
        _metrics.cached_bytes -= p.buf.size();
        _cached_bytes -= p.buf.size();
        ++_metrics.page_evictions;
    }

    size_t evict_range(cache_type::iterator start, cache_type::iterator end) noexcept {
        size_t count = 0;
        auto disposer = [] (auto* p) noexcept {};
        while (start != end) {
            ++count;
            on_evicted(*start);
            start = start.erase_and_dispose(disposer, page_idx_less_comparator());
        }
        return count;
    }
public:
    /// \brief Constructs a cached_file.
    ///
    /// The cached area will reflect subset of f from the byte range [start, start + size).
    ///
    /// \param m Metrics object which should be updated from operations on this object.
    ///          The metrics object can be shared by many cached_file instances, in which case it
    ///          will reflect the sum of operations on all cached_file instances.
    cached_file(file f, cached_file::metrics& m, lru& l, offset_type size, sstring file_name = {})
        : _file(std::move(f))
        , _file_name(std::move(file_name))
        , _metrics(m)
        , _lru(l)
        , _cache(page_idx_less_comparator())
        , _size(size)
    {
        offset_type last_byte_offset = _size ? (_size - 1) : 0;
        _last_page_size = (last_byte_offset % page_size) + (_size ? 1 : 0);
        _last_page = last_byte_offset / page_size;
    }

    cached_file(cached_file&&) = delete; // captured this
    cached_file(const cached_file&) = delete;

    ~cached_file() {
        evict_range(_cache.begin(), _cache.end());
    }

    /// \brief Invalidates [start, end) or less.
    ///
    /// Invariants:
    ///
    ///   - all bytes outside [start, end) which were cached before the call will still be cached.
    ///
    void invalidate_at_most(offset_type start, offset_type end, tracing::trace_state_ptr trace_state = {}) {
        auto lo_page = start / page_size
                       // If start is 0 then we can drop the containing page
                       // Otherwise we cannot drop the page.
                       + bool(start % page_size) * bool(start != 0);

        auto hi_page = (end) / page_size;

        if (lo_page < hi_page) {
            auto count = evict_range(_cache.lower_bound(lo_page), _cache.lower_bound(hi_page));
            if (count) {
                tracing::trace(trace_state, "page cache: evicted {} page(s) in [{}, {}), file={}", count,
                    lo_page, hi_page, _file_name);
            }
        }
    }

    /// \brief Equivalent to \ref invalidate_at_most(0, end).
    void invalidate_at_most_front(offset_type end, tracing::trace_state_ptr trace_state = {}) {
        auto hi_page = end / page_size;
        auto count = evict_range(_cache.begin(), _cache.lower_bound(hi_page));
        if (count) {
            tracing::trace(trace_state, "page cache: evicted {} page(s) in [0, {}), file={}", count,
                hi_page, _file_name);
        }
    }

    /// \brief Read from the file
    ///
    /// Returns a stream with data which starts at position pos in the area managed by this instance.
    /// This cached_file instance must outlive the returned stream.
    /// The stream does not do any read-ahead.
    ///
    /// \param pos The offset of the first byte to read, relative to the cached file area.
    /// \param permit Holds reader_permit under which returned buffers should be accounted.
    ///               When disengaged, no accounting is done.
    stream read(offset_type global_pos, const io_priority_class& pc, std::optional<reader_permit> permit, tracing::trace_state_ptr trace_state = {}) {
        if (global_pos >= _size) {
            return stream();
        }
        auto offset = global_pos % page_size;
        auto page_idx = global_pos / page_size;
        return stream(*this, pc, std::move(permit), std::move(trace_state), page_idx, offset);
    }

    /// \brief Returns the number of bytes in the area managed by this instance.
    offset_type size() const {
        return _size;
    }

    /// \brief Returns the number of bytes cached.
    size_t cached_bytes() const {
        return _cached_bytes;
    }

    /// \brief Returns the underlying file.
    file& get_file() {
        return _file;
    }
};

inline
void cached_file::cached_page::on_evicted() noexcept {
    parent->on_evicted(*this);
    cached_file::cache_type::iterator it(this);
    it.erase(page_idx_less_comparator());
}

class cached_file_impl : public file_impl {
    cached_file& _cf;
    tracing::trace_state_ptr _trace_state;
private:
    [[noreturn]] void unsupported() {
        throw_with_backtrace<std::logic_error>("unsupported operation");
    }
public:
    cached_file_impl(cached_file& cf, tracing::trace_state_ptr trace_state = {})
        : _cf(cf)
        , _trace_state(std::move(trace_state))
    {
        _memory_dma_alignment = _cf.get_file().memory_dma_alignment();
        _disk_read_dma_alignment = _cf.get_file().disk_read_dma_alignment();
        _disk_write_dma_alignment = _cf.get_file().disk_write_dma_alignment();
    }

    // unsupported
    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override { unsupported(); }
    virtual future<> flush(void) override { unsupported(); }
    virtual future<> truncate(uint64_t length) override { unsupported(); }
    virtual future<> discard(uint64_t offset, uint64_t length) override { unsupported(); }
    virtual future<> allocate(uint64_t position, uint64_t length) override { unsupported(); }
    virtual subscription<directory_entry> list_directory(std::function<future<>(directory_entry)>) override { unsupported(); }

    // delegating
    virtual future<struct stat> stat(void) override { return _cf.get_file().stat(); }
    virtual future<uint64_t> size(void) override { return _cf.get_file().size(); }
    virtual future<> close() override { return _cf.get_file().close(); }
    virtual std::unique_ptr<seastar::file_handle_impl> dup() override { return get_file_impl(_cf.get_file())->dup(); }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t size, const io_priority_class& pc) override {
        return do_with(_cf.read(offset, pc, std::nullopt, _trace_state), size, temporary_buffer<uint8_t>(),
                [this, size] (cached_file::stream& s, size_t& size_left, temporary_buffer<uint8_t>& result) {
            return repeat([this, &s, &size_left, &result, size] {
                return s.next().then([this, &size_left, &result, size] (temporary_buffer<char> buf) {
                    if (!buf) {
                        return stop_iteration::yes;
                    }
                    if (!result) {
                        if (buf.size() >= size_left) {
                            result = temporary_buffer<uint8_t>(reinterpret_cast<uint8_t*>(buf.get_write()), size_left, buf.release());
                            return stop_iteration::yes;
                        }
                        result = temporary_buffer<uint8_t>::aligned(_memory_dma_alignment, size_left);
                    }
                    size_t this_len = std::min(buf.size(), size_left);
                    std::copy(buf.begin(), buf.end(), result.get_write() + size - size_left);
                    size_left -= this_len;
                    return stop_iteration(size_left == 0);
                });
            }).then([&] {
                return std::move(result);
            });
        });
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        unsupported(); // FIXME
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        unsupported(); // FIXME
    }
};

// Creates a seastar::file object which will read through a given cached_file instance.
// The cached_file object must be kept alive as long as the file is in use.
inline
file make_cached_seastar_file(cached_file& cf) {
    return file(make_shared<cached_file_impl>(cf));
}
