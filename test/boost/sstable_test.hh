
/*
 * Copyright (C) 2015 ScyllaDB
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

#include "types/map.hh"
#include "sstables/writer.hh"
#include "sstables/sstables.hh"
#include "sstables/binary_search.hh"
#include "database.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "types/set.hh"
#include "types/list.hh"
#include <seastar/core/thread.hh>
#include "sstables/index_reader.hh"
#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/tmpdir.hh"
#include <array>

constexpr auto la = sstables::sstable::version_types::la;
constexpr auto big = sstables::sstable::format_types::big;

class column_family_test {
    lw_shared_ptr<column_family> _cf;
public:
    column_family_test(lw_shared_ptr<column_family> cf) : _cf(cf) {}

    void add_sstable(sstables::shared_sstable sstable) {
        _cf->_sstables->insert(std::move(sstable));
    }

    void rebuild_sstable_list(const std::vector<sstables::shared_sstable>& new_sstables,
            const std::vector<sstables::shared_sstable>& sstables_to_remove) {
        _cf->rebuild_sstable_list(new_sstables, sstables_to_remove);
    }

    static void update_sstables_known_generation(column_family& cf, unsigned generation) {
        cf.update_sstables_known_generation(generation);
    }

    static uint64_t calculate_generation_for_new_table(column_family& cf) {
        return cf.calculate_generation_for_new_table();
    }

    static int64_t calculate_shard_from_sstable_generation(int64_t generation) {
        return column_family::calculate_shard_from_sstable_generation(generation);
    }
};

namespace sstables {

using sstable_ptr = shared_sstable;

class test {
    sstable_ptr _sst;
public:

    test(sstable_ptr s) : _sst(s) {}

    summary& _summary() {
        return _sst->_components->summary;
    }

    future<temporary_buffer<char>> data_read(uint64_t pos, size_t len) {
        return _sst->data_read(pos, len, default_priority_class());
    }

    future<index_list> read_indexes() {
        auto l = make_lw_shared<index_list>();
        return do_with(std::make_unique<index_reader>(_sst, no_reader_permit(), default_priority_class(), tracing::trace_state_ptr()),
                [this, l] (std::unique_ptr<index_reader>& ir) {
            return ir->read_partition_data().then([&, l] {
                l->push_back(std::move(ir->current_partition_entry()));
            }).then([&, l] {
                return repeat([&, l] {
                    return ir->advance_to_next_partition().then([&, l] {
                        if (ir->eof()) {
                            return stop_iteration::yes;
                        }

                        l->push_back(std::move(ir->current_partition_entry()));
                        return stop_iteration::no;
                    });
                });
            });
        }).then([l] {
            return std::move(*l);
        });
    }

    future<> read_statistics() {
        return _sst->read_statistics(default_priority_class());
    }

    statistics& get_statistics() {
        return _sst->_components->statistics;
    }

    future<> read_summary() {
        return _sst->read_summary(default_priority_class());
    }

    future<summary_entry&> read_summary_entry(size_t i) {
        return _sst->read_summary_entry(i);
    }

    summary& get_summary() {
        return _sst->_components->summary;
    }

    summary move_summary() {
        return std::move(_sst->_components->summary);
    }

    future<> read_toc() {
        return _sst->read_toc();
    }

    auto& get_components() {
        return _sst->_recognized_components;
    }

    template <typename T>
    int binary_search(const dht::i_partitioner& p, const T& entries, const key& sk) {
        return sstables::binary_search(p, entries, sk);
    }

    void change_generation_number(int64_t generation) {
        _sst->_generation = generation;
    }

    void change_dir(sstring dir) {
        _sst->_dir = dir;
    }

    void set_data_file_size(uint64_t size) {
        _sst->_data_file_size = size;
    }

    void set_data_file_write_time(db_clock::time_point wtime) {
        _sst->_data_file_write_time = wtime;
    }

    void set_run_identifier(utils::UUID identifier) {
        _sst->_run_identifier = identifier;
    }

    future<> store() {
        _sst->_recognized_components.erase(component_type::Index);
        _sst->_recognized_components.erase(component_type::Data);
        return seastar::async([sst = _sst] {
            sst->write_toc(default_priority_class());
            sst->write_statistics(default_priority_class());
            sst->write_compression(default_priority_class());
            sst->write_filter(default_priority_class());
            sst->write_summary(default_priority_class());
            sst->seal_sstable().get();
        });
    }

    // Used to create synthetic sstables for testing leveled compaction strategy.
    void set_values_for_leveled_strategy(uint64_t fake_data_size, uint32_t sstable_level, int64_t max_timestamp, sstring first_key, sstring last_key) {
        _sst->_data_file_size = fake_data_size;
        // Create a synthetic stats metadata
        stats_metadata stats = {};
        // leveled strategy sorts sstables by age using max_timestamp, let's set it to 0.
        stats.max_timestamp = max_timestamp;
        stats.sstable_level = sstable_level;
        _sst->_components->statistics.contents[metadata_type::Stats] = std::make_unique<stats_metadata>(std::move(stats));
        _sst->_components->summary.first_key.value = bytes(reinterpret_cast<const signed char*>(first_key.c_str()), first_key.size());
        _sst->_components->summary.last_key.value = bytes(reinterpret_cast<const signed char*>(last_key.c_str()), last_key.size());
        _sst->set_first_and_last_keys();
        _sst->_run_identifier = utils::make_random_uuid();
    }

    void set_values(sstring first_key, sstring last_key, stats_metadata stats) {
        // scylla component must be present for a sstable to be considered fully expired.
        _sst->_recognized_components.insert(component_type::Scylla);
        _sst->_components->statistics.contents[metadata_type::Stats] = std::make_unique<stats_metadata>(std::move(stats));
        _sst->_components->summary.first_key.value = bytes(reinterpret_cast<const signed char*>(first_key.c_str()), first_key.size());
        _sst->_components->summary.last_key.value = bytes(reinterpret_cast<const signed char*>(last_key.c_str()), last_key.size());
        _sst->set_first_and_last_keys();
        _sst->_components->statistics.contents[metadata_type::Compaction] = std::make_unique<compaction_metadata>();
        _sst->_run_identifier = utils::make_random_uuid();
    }

    void rewrite_toc_without_scylla_component() {
        _sst->_recognized_components.erase(component_type::Scylla);
        remove_file(_sst->filename(component_type::TOC)).get();
        _sst->write_toc(default_priority_class());
        _sst->seal_sstable().get();
    }

    future<> remove_component(component_type c) {
        return remove_file(_sst->filename(c));
    }

    const sstring filename(component_type c) const {
        return _sst->filename(c);
    }

    void set_shards(std::vector<unsigned> shards) {
        _sst->_shards = std::move(shards);
    }
};

inline auto replacer_fn_no_op() {
    return [](sstables::compaction_completion_desc desc) -> void {};
}

inline sstring get_test_dir(const sstring& name, const sstring& ks, const sstring& cf)
{
    return seastar::format("test/resource/sstables/{}/{}/{}-1c6ace40fad111e7b9cf000000000002", name, ks, cf);
}

inline sstring get_test_dir(const sstring& name, const schema_ptr s)
{
    return seastar::format("test/resource/sstables/{}/{}/{}-1c6ace40fad111e7b9cf000000000002", name, s->ks_name(), s->cf_name());
}

template<typename AsyncAction>
GCC6_CONCEPT( requires requires (AsyncAction aa, sstables::sstable::version_types& c) { { aa(c) } -> future<>; } )
inline
future<> for_each_sstable_version(AsyncAction action) {
    return seastar::do_for_each(all_sstable_versions, std::move(action));
}

inline schema_ptr composite_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema({}, "tests", "composite",
        // partition key
        {{"name", bytes_type}, {"col1", bytes_type}},
        // clustering key
        {},
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a composite key as pkey"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr set_schema() {
    static thread_local auto s = [] {
        auto my_set_type = set_type_impl::get_instance(bytes_type, false);
        schema_builder builder(make_lw_shared(schema({}, "tests", "set_pk",
        // partition key
        {{"ss", my_set_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a set as pkeys"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr map_schema() {
    static thread_local auto s = [] {
        auto my_map_type = map_type_impl::get_instance(bytes_type, bytes_type, false);
        schema_builder builder(make_lw_shared(schema({}, "tests", "map_pk",
        // partition key
        {{"ss", my_map_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a map as pkeys"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr list_schema() {
    static thread_local auto s = [] {
        auto my_list_type = list_type_impl::get_instance(bytes_type, false);
        schema_builder builder(make_lw_shared(schema({}, "tests", "list_pk",
        // partition key
        {{"ss", my_list_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a list as pkeys"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr uncompressed_schema(int32_t min_index_interval = 0) {
    auto uncompressed = [=] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", "uncompressed"), "ks", "uncompressed",
        // partition key
        {{"name", utf8_type}},
        // clustering key
        {},
        // regular columns
        {{"col1", utf8_type}, {"col2", int32_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Uncompressed data"
       )));
       builder.set_compressor_params(compression_parameters());
       if (min_index_interval) {
           builder.set_min_index_interval(min_index_interval);
       }
       return builder.build(schema_builder::compact_storage::no);
    }();
    return uncompressed;
}

inline sstring uncompressed_dir() {
    return get_test_dir("uncompressed", uncompressed_schema());
}

inline schema_ptr complex_schema() {
    static thread_local auto s = [] {
        auto my_list_type = list_type_impl::get_instance(bytes_type, true);
        auto my_map_type = map_type_impl::get_instance(bytes_type, bytes_type, true);
        auto my_set_type = set_type_impl::get_instance(bytes_type, true);
        auto my_fset_type = set_type_impl::get_instance(bytes_type, false);
        auto my_set_static_type = set_type_impl::get_instance(bytes_type, true);

        schema_builder builder(make_lw_shared(schema({}, "tests", "complex_schema",
        // partition key
        {{"key", bytes_type}},
        // clustering key
        {{"clust1", bytes_type}, {"clust2", bytes_type}},
        // regular columns
        {
            {"reg_set", my_set_type},
            {"reg_list", my_list_type},
            {"reg_map", my_map_type},
            {"reg_fset", my_fset_type},
            {"reg", bytes_type},
        },
        // static columns
        {{"static_obj", bytes_type}, {"static_collection", my_set_static_type}},
        // regular column name type
        bytes_type,
        // comment
        "Table with a complex schema, including collections and static keys"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

inline schema_ptr columns_schema() {
    static thread_local auto columns = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("name", "columns"), "name", "columns",
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"columnfamily_name", utf8_type}, {"column_name", utf8_type}},
        // regular columns
        {
            {"component_index", int32_type},
            {"index_name", utf8_type},
            {"index_options", utf8_type},
            {"index_type", utf8_type},
            {"type", utf8_type},
            {"validator", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "column definitions"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return columns;
}

inline schema_ptr compact_simple_dense_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema({}, "tests", "compact_simple_dense",
        // partition key
        {{"ks", bytes_type}},
        // clustering key
        {{"cl1", bytes_type}},
        // regular columns
        {{"cl2", bytes_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a compact storage, and a single clustering key"
       )));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return s;
}

inline schema_ptr compact_dense_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema({}, "tests", "compact_simple_dense",
        // partition key
        {{"ks", bytes_type}},
        // clustering key
        {{"cl1", bytes_type}, {"cl2", bytes_type}},
        // regular columns
        {{"cl3", bytes_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a compact storage, and a compound clustering key"
       )));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return s;
}

inline schema_ptr compact_sparse_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema({}, "tests", "compact_sparse",
        // partition key
        {{"ks", bytes_type}},
        // clustering key
        {},
        // regular columns
        {
            {"cl1", bytes_type},
            {"cl2", bytes_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a compact storage, but no clustering keys"
       )));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return s;
}

// This is "imported" from system_keyspace.cc. But we will copy it for two reasons:
// 1) This is private there, and for good reason.
// 2) If the schema for the peers table ever change (it does from ka to la), we want to make
//    sure we are testing the exact some one we have in our test dir.
inline schema_ptr peers_schema() {
    static thread_local auto peers = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("system", "peers"), "system", "peers",
        // partition key
        {{"peer", inet_addr_type}},
        // clustering key
        {},
        // regular columns
        {
                {"data_center", utf8_type},
                {"host_id", uuid_type},
                {"preferred_ip", inet_addr_type},
                {"rack", utf8_type},
                {"release_version", utf8_type},
                {"rpc_address", inet_addr_type},
                {"schema_version", uuid_type},
                {"tokens", set_type_impl::get_instance(utf8_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "information about known peers in the cluster"
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return peers;
}

enum class status {
    dead,
    live,
    ttl,
};

inline bool check_status_and_done(atomic_cell_view c, status expected) {
    if (expected == status::dead) {
        BOOST_REQUIRE(c.is_live() == false);
        return true;
    }
    BOOST_REQUIRE(c.is_live() == true);
    BOOST_REQUIRE(c.is_live_and_has_ttl() == (expected == status::ttl));
    return false;
}

template <status Status>
inline void match(const row& row, const schema& s, bytes col, const data_value& value, int64_t timestamp = 0, int32_t expiration = 0) {
    auto cdef = s.get_column_definition(col);

    BOOST_CHECK_NO_THROW(row.cell_at(cdef->id));
    auto c = row.cell_at(cdef->id).as_atomic_cell(*cdef);
    if (check_status_and_done(c, Status)) {
        return;
    }

    auto expected = cdef->type->decompose(value);
    auto val = c.value().linearize();
    assert(val == expected);
    BOOST_REQUIRE(c.value().linearize() == expected);
    if (timestamp) {
        BOOST_REQUIRE(c.timestamp() == timestamp);
    }
    if (expiration) {
        BOOST_REQUIRE(c.expiry() == gc_clock::time_point(gc_clock::duration(expiration)));
    }
}

inline void match_live_cell(const row& row, const schema& s, bytes col, const data_value& value) {
    match<status::live>(row, s, col, value);
}

inline void match_expiring_cell(const row& row, const schema& s, bytes col, const data_value& value, int64_t timestamp, int32_t expiration) {
    match<status::ttl>(row, s, col, value);
}

inline void match_dead_cell(const row& row, const schema& s, bytes col) {
    match<status::dead>(row, s, col, 0); // value will be ignored
}

inline void match_absent(const row& row, const schema& s, bytes col) {
    auto cdef = s.get_column_definition(col);
    BOOST_REQUIRE(row.find_cell(cdef->id) == nullptr);
}

inline collection_mutation_description
match_collection(const row& row, const schema& s, bytes col, const tombstone& t) {
    auto cdef = s.get_column_definition(col);

    BOOST_CHECK_NO_THROW(row.cell_at(cdef->id));
    auto c = row.cell_at(cdef->id).as_collection_mutation();
    return c.with_deserialized(*cdef->type, [&] (collection_mutation_view_description mut) {
        BOOST_REQUIRE(mut.tomb == t);
        return mut.materialize(*cdef->type);
    });
}

template <status Status>
inline void match_collection_element(const std::pair<bytes, atomic_cell>& element, const bytes_opt& col, const bytes_opt& expected_serialized_value) {
    if (col) {
        BOOST_REQUIRE(element.first == *col);
    }

    if (check_status_and_done(element.second, Status)) {
        return;
    }

    // For simplicity, we will have all set elements in our schema presented as
    // bytes - which serializes to itself.  Then we don't need to meddle with
    // the schema for the set type, and is enough for the purposes of this
    // test.
    if (expected_serialized_value) {
        BOOST_REQUIRE(element.second.value().linearize() == *expected_serialized_value);
    }
}

class test_setup {
    file _f;
    std::function<future<> (directory_entry de)> _walker;
    sstring _path;
    future<> _listing_done;

    static sstring& path() {
        static sstring _p = "test/resource/sstables/tests-temporary";
        return _p;
    };

public:
    test_setup(file f, sstring path)
            : _f(std::move(f))
            , _path(path)
            , _listing_done(_f.list_directory([this] (directory_entry de) { return _remove(de); }).done()) {
    }
    ~test_setup() {
        // FIXME: discarded future.
        (void)_f.close().finally([save = _f] {});
    }
protected:
    future<> _create_directory(sstring name) {
        return engine().make_directory(name);
    }

    future<> _remove(directory_entry de) {
        sstring t = _path + "/" + de.name;
        return engine().file_type(t).then([t] (std::optional<directory_entry_type> det) {
            auto f = make_ready_future<>();

            if (!det) {
                throw std::runtime_error("Can't determine file type\n");
            } else if (det == directory_entry_type::directory) {
                f = empty_test_dir(t);
            }
            return f.then([t] {
                return engine().remove_file(t);
            });
        });
    }
    future<> done() { return std::move(_listing_done); }

    static future<> empty_test_dir(sstring p = path()) {
        return engine().open_directory(p).then([p] (file f) {
            auto l = make_lw_shared<test_setup>(std::move(f), p);
            return l->done().then([l] { });
        });
    }
public:
    static future<> create_empty_test_dir(sstring p = path()) {
        return engine().make_directory(p).then_wrapped([p] (future<> f) {
            try {
                f.get();
            // it's fine if the directory exists, just shut down the exceptional future message
            } catch (std::exception& e) {}
            return empty_test_dir(p);
        });
    }

    static future<> do_with_tmp_directory(std::function<future<> (test_env&, sstring tmpdir_path)>&& fut) {
        return seastar::async([fut = std::move(fut)] {
            storage_service_for_tests ssft;
            auto tmp = tmpdir();
            test_env env;
            fut(env, tmp.path().string()).get();
        });
    }

    static future<> do_with_cloned_tmp_directory(sstring src, std::function<future<> (test_env&, sstring srcdir_path, sstring destdir_path)>&& fut) {
        return seastar::async([fut = std::move(fut), src = std::move(src)] {
            storage_service_for_tests ssft;
            auto src_dir = tmpdir();
            auto dest_dir = tmpdir();
            for (const auto& entry : std::filesystem::directory_iterator(src.c_str())) {
                std::filesystem::copy(entry.path(), src_dir.path() / entry.path().filename());
            }
            auto dest_path = dest_dir.path() / src.c_str();
            std::filesystem::create_directories(dest_path);
            test_env env;
            fut(env, src_dir.path().string(), dest_path.string()).get();
        });
    }
};
}

inline
::mutation_source as_mutation_source(sstables::shared_sstable sst) {
    return sst->as_mutation_source();
}


inline dht::decorated_key make_dkey(schema_ptr s, bytes b)
{
    auto sst_key = sstables::key::from_bytes(b);
    return dht::decorate_key(*s, sst_key.to_partition_key(*s));
}
