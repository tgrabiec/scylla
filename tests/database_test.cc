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


#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>

#include "tests/cql_test_env.hh"
#include "tests/result_set_assertions.hh"

#include "database.hh"
#include "partition_slice_builder.hh"
#include "frozen_mutation.hh"
#include "mutation_source_test.hh"
#include "schema_registry.hh"
#include "service/migration_manager.hh"
#include "make_random_string.hh"

SEASTAR_TEST_CASE(test_querying_with_limits) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
            auto& db = e.local_db();
            auto s = db.find_schema("ks", "cf");
            dht::partition_range_vector pranges;
            for (uint32_t i = 1; i <= 3; ++i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(sprint("key%d", i)));
                mutation m(s, pkey);
                m.partition().apply(tombstone(api::timestamp_type(1), gc_clock::now()));
                db.apply(s, freeze(m)).get();
            }
            for (uint32_t i = 3; i <= 8; ++i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(sprint("key%d", i)));
                mutation m(s, pkey);
                m.set_clustered_cell(clustering_key_prefix::make_empty(), "v", data_value(bytes("v1")), 1);
                db.apply(s, freeze(m)).get();
                pranges.emplace_back(dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*s, std::move(pkey))));
            }

            auto max_size = std::numeric_limits<size_t>::max();
            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), 3);
                auto result = db.query(s, cmd, query::result_options::only_result(), pranges, nullptr, max_size).get0();
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(3);
            }

            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(),
                        query::max_rows, gc_clock::now(), std::experimental::nullopt, 5);
                auto result = db.query(s, cmd, query::result_options::only_result(), pranges, nullptr, max_size).get0();
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(5);
            }

            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(),
                        query::max_rows, gc_clock::now(), std::experimental::nullopt, 3);
                auto result = db.query(s, cmd, query::result_options::only_result(), pranges, nullptr, max_size).get0();
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(3);
            }
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source) {
    do_with_cql_env([] (cql_test_env& e) {
        run_mutation_source_tests([&] (schema_ptr s, const std::vector<mutation>& partitions) -> mutation_source {
            try {
                e.local_db().find_column_family(s->ks_name(), s->cf_name());
                service::get_local_migration_manager().announce_column_family_drop(s->ks_name(), s->cf_name(), true).get();
            } catch (const no_such_column_family&) {
                // expected
            }
            service::get_local_migration_manager().announce_new_column_family(s, true).get();
            column_family& cf = e.local_db().find_column_family(s);
            for (auto&& m : partitions) {
                e.local_db().apply(cf.schema(), freeze(m)).get();
            }
            cf.flush().get();
            cf.get_row_cache().invalidate([] {}).get();
            return mutation_source([&] (schema_ptr s,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding fwd_mr) {
                return cf.make_reader(s, range, slice, pc, std::move(trace_state), fwd, fwd_mr);
            });
        });
        return make_ready_future<>();
    }).get();
}


schema_ptr make_4009_schema(cql_test_env& env) {
    env.execute_cql("CREATE TABLE ks.tbl ("
                    " field1 text,"
                    " field2 smallint,"
                    " field3 tinyint,"
                    " field4 timeuuid,"
                    " field5 smallint,"
                    " field6 smallint,"
                    " field7 inet,"
                    " field8 map<text, text>,"
                    " PRIMARY KEY ((field1, field2, field3), field4)"
                    " ) WITH CLUSTERING ORDER BY (field4 DESC)"
                    " AND bloom_filter_fp_chance = 0.1"
                    " AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}"
                    " AND comment = ''"
                    " AND compaction = {"
                    " 'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy',"
                    " 'compaction_window_unit': 'DAYS',"
                    " 'compaction_window_size': 7"
                    " }"
                    " AND compression = {"
                    " 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor',"
                    " 'chunk_length_kb': '64'"
                    " }"
                    " AND crc_check_chance = 1.0"
                    " AND dclocal_read_repair_chance = 0.1"
                    " AND default_time_to_live = 0"
                    " AND gc_grace_seconds = 864000"
                    " AND max_index_interval = 2048"
                    " AND memtable_flush_period_in_ms = 0"
                    " AND min_index_interval = 128"
                    " AND read_repair_chance = 0.0"
                    " AND speculative_retry = '99PERCENTILE';").get();
    return env.local_db().find_schema("ks", "tbl");
}

SEASTAR_THREAD_TEST_CASE(test_4009) {
    db::config cfg;
    cfg.enable_sstables_mc_format() = true;
    do_with_cql_env([] (cql_test_env& e) {
        auto s = make_4009_schema(e);
        for (int i = 0; i < 100; ++i) {
            auto pk = utils::UUID_gen::get_time_UUID();
            for (int j = 0; j < 100; ++j) {
                auto ck = utils::UUID_gen::get_time_UUID();
                e.execute_cql(
                    format("insert into ks.tbl (field1, field2, field3, field4, field5, field6, field7, field8) values"
                           " ('{}', 123, 124, {}, 1, 2, '127.0.0.1', {{'a': 'asd', '{}': '{}', '123': '123123'}});",
                        pk,
                        ck,
                        to_hex(to_bytes(make_random_string(32))),
                        to_hex(to_bytes(make_random_string(1*1024*1024))))).get();
            }
        }
        e.local_db().flush_all_memtables().get();
        auto rd = e.local_db().find_column_family(s->id()).make_streaming_reader(s, {query::full_partition_range});
        rd.consume_pausable([&] (mutation_fragment&& mf) {
            return stop_iteration::no;
        }, db::no_timeout).get();
        return make_ready_future<>();
    }, cfg).get();
}
