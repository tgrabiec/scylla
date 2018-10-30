#include <seastar/core/iostream.hh>
#include "util/small_vector.hh"

#pragma once

// Accumulates data sent to the memory_data_sink, allowing the data
// to be examined later.
class memory_data_sink_buffers {
    using buffers_type = utils::small_vector<temporary_buffer<char>, 1>;
    buffers_type _bufs;
    size_t _size = 0;
public:
    size_t size() const { return _size; }
    buffers_type& buffers() { return _bufs; }

    void put(temporary_buffer<char>&& buf) {
        _size += buf.size();
        _bufs.emplace_back(std::move(buf));
    }

    void clear() {
        _bufs.clear();
        _size = 0;
    }
};

class memory_data_sink : public data_sink_impl {
    memory_data_sink_buffers& _bufs;
public:
    memory_data_sink(memory_data_sink_buffers& b) : _bufs(b) {}
    virtual future<> put(net::packet data)  override {
        abort();
        return make_ready_future<>();
    }
    virtual future<> put(temporary_buffer<char> buf) override {
        _bufs.put(std::move(buf));
        return make_ready_future<>();
    }
    virtual future<> flush() override {
        return make_ready_future<>();
    }
    virtual future<> close() override {
        return make_ready_future<>();
    }
};
