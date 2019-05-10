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


#include <iostream>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"
#include "tests/mutation_source_test.hh"
#include "tests/result_set_assertions.hh"
#include "service/migration_manager.hh"
#include "schema_builder.hh"
#include "schema_registry.hh"
#include "types/list.hh"
#include "types/user.hh"
#include "db/config.hh"
#include "tmpdir.hh"

SEASTAR_TEST_CASE(test_new_schema_with_no_structural_change_is_propagated) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            auto partial = schema_builder("tests", "table")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type);

            e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();

            auto old_schema = partial.build();

            service::get_local_migration_manager().announce_new_column_family(old_schema, false).get();

            auto old_table_version = e.db().local().find_schema(old_schema->id())->version();
            auto old_node_version = e.db().local().get_version();

            auto new_schema = partial.build();
            BOOST_REQUIRE_NE(new_schema->version(), old_schema->version());

            service::get_local_migration_manager().announce_column_family_update(new_schema, false, { }).get();

            BOOST_REQUIRE_NE(e.db().local().find_schema(old_schema->id())->version(), old_table_version);
            BOOST_REQUIRE_NE(e.db().local().get_version(), old_node_version);
        });
    });
}

SEASTAR_TEST_CASE(test_schema_is_updated_in_keyspace) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            auto builder = schema_builder("tests", "table")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type);

            e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();

            auto old_schema = builder.build();

            service::get_local_migration_manager().announce_new_column_family(old_schema, false).get();

            auto s = e.local_db().find_schema(old_schema->id());
            BOOST_REQUIRE_EQUAL(*old_schema, *s);
            BOOST_REQUIRE_EQUAL(864000, s->gc_grace_seconds().count());
            BOOST_REQUIRE_EQUAL(*s, *e.local_db().find_keyspace(s->ks_name()).metadata()->cf_meta_data().at(s->cf_name()));

            builder.set_gc_grace_seconds(1);
            auto new_schema = builder.build();

            service::get_local_migration_manager().announce_column_family_update(new_schema, false, { }).get();

            s = e.local_db().find_schema(old_schema->id());
            BOOST_REQUIRE_NE(*old_schema, *s);
            BOOST_REQUIRE_EQUAL(*new_schema, *s);
            BOOST_REQUIRE_EQUAL(1, s->gc_grace_seconds().count());
            BOOST_REQUIRE_EQUAL(*s, *e.local_db().find_keyspace(s->ks_name()).metadata()->cf_meta_data().at(s->cf_name()));
        });
    });
}

SEASTAR_TEST_CASE(test_tombstones_are_ignored_in_version_calculation) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();

            auto table_schema = schema_builder("ks", "table")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .build();

            service::get_local_migration_manager().announce_new_column_family(table_schema, false).get();

            auto old_table_version = e.db().local().find_schema(table_schema->id())->version();
            auto old_node_version = e.db().local().get_version();

            {
                BOOST_TEST_MESSAGE("Applying a no-op tombstone to v1 column definition");
                auto s = db::schema_tables::columns();
                auto pkey = partition_key::from_singular(*s, table_schema->ks_name());
                mutation m(s, pkey);
                auto ckey = clustering_key::from_exploded(*s, {utf8_type->decompose(table_schema->cf_name()), "v1"});
                m.partition().apply_delete(*s, ckey, tombstone(api::min_timestamp, gc_clock::now()));
                service::get_local_migration_manager().announce(std::vector<mutation>({m}), true).get();
            }

            auto new_table_version = e.db().local().find_schema(table_schema->id())->version();
            auto new_node_version = e.db().local().get_version();

            BOOST_REQUIRE_EQUAL(new_table_version, old_table_version);
            BOOST_REQUIRE_EQUAL(new_node_version, old_node_version);
        });
    });
}

SEASTAR_TEST_CASE(test_concurrent_column_addition) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();

            service::migration_manager& mm = service::get_local_migration_manager();

            auto s0 = schema_builder("ks", "table")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .build();

            auto s1 = schema_builder("ks", "table")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .with_column("v3", bytes_type)
                    .build();

            auto s2 = schema_builder("ks", "table", std::make_optional(s1->id()))
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .with_column("v2", bytes_type)
                    .build();

            mm.announce_new_column_family(s1, false).get();
            auto old_version = e.db().local().find_schema(s1->id())->version();

            // Apply s0 -> s2 change.
            {
                auto&& keyspace = e.db().local().find_keyspace(s0->ks_name()).metadata();
                auto muts = db::schema_tables::make_update_table_mutations(keyspace, s0, s2,
                    api::new_timestamp(), false);
                mm.announce(std::move(muts), true).get();
            }

            auto new_schema = e.db().local().find_schema(s1->id());

            BOOST_REQUIRE(new_schema->get_column_definition(to_bytes("v1")) != nullptr);
            BOOST_REQUIRE(new_schema->get_column_definition(to_bytes("v2")) != nullptr);
            BOOST_REQUIRE(new_schema->get_column_definition(to_bytes("v3")) != nullptr);

            BOOST_REQUIRE(new_schema->version() != old_version);
            BOOST_REQUIRE(new_schema->version() != s2->version());
        });
    });
}

SEASTAR_TEST_CASE(test_sort_type_in_update) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        service::migration_manager& mm = service::get_local_migration_manager();
        auto&& keyspace = e.db().local().find_keyspace("ks").metadata();

        auto type1 = user_type_impl::get_instance("ks", to_bytes("type1"), {}, {});
        auto muts1 = db::schema_tables::make_create_type_mutations(keyspace, type1, api::new_timestamp());

        auto type3 = user_type_impl::get_instance("ks", to_bytes("type3"), {}, {});
        auto muts3 = db::schema_tables::make_create_type_mutations(keyspace, type3, api::new_timestamp());

        // type2 must be created after type1 and type3. This tests that announce sorts them.
        auto type2 = user_type_impl::get_instance("ks", to_bytes("type2"), {"field1", "field3"}, {type1, type3});
        auto muts2 = db::schema_tables::make_create_type_mutations(keyspace, type2, api::new_timestamp());

        auto muts = muts2;
        muts.insert(muts.end(), muts1.begin(), muts1.end());
        muts.insert(muts.end(), muts3.begin(), muts3.end());
        mm.announce(std::move(muts), false).get();
    });
}

SEASTAR_TEST_CASE(test_column_is_dropped) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int);").get();
            e.execute_cql("alter table tests.table1 drop c2;").get();
            e.execute_cql("alter table tests.table1 add s1 int;").get();

            schema_ptr s = e.db().local().find_schema("tests", "table1");
            BOOST_REQUIRE(s->columns_by_name().count(to_bytes("c1")));
            BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("c2")));
            BOOST_REQUIRE(s->columns_by_name().count(to_bytes("s1")));
        });
    });
}

SEASTAR_TEST_CASE(test_static_column_is_dropped) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();
        e.execute_cql("create table tests.table1 (pk int, c1 int, c2 int static, primary key (pk, c1));").get();

        e.execute_cql("alter table tests.table1 drop c2;").get();
        e.execute_cql("alter table tests.table1 add s1 int static;").get();
        schema_ptr s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("c2")));
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("s1")));

        e.execute_cql("alter table tests.table1 drop s1;").get();
        s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("c2")));
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("s1")));
    });
}

SEASTAR_TEST_CASE(test_multiple_columns_add_and_drop) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();
        e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int, c3 int);").get();

        e.execute_cql("alter table tests.table1 drop (c2);").get();
        e.execute_cql("alter table tests.table1 add (s1 int);").get();
        schema_ptr s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("c2")));
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("c3")));
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("s1")));

        e.execute_cql("alter table tests.table1 drop (c1, c3);").get();
        e.execute_cql("alter table tests.table1 add (s2 int, s3 int);").get();
        s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("c2")));
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("c3")));
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("s1")));
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("s2")));
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("s3")));
    });
}

SEASTAR_TEST_CASE(test_multiple_static_columns_add_and_drop) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();
        e.execute_cql("create table tests.table1 (pk int, c1 int, c2 int static, c3 int, primary key(pk, c1));").get();

        e.execute_cql("alter table tests.table1 drop (c2);").get();
        e.execute_cql("alter table tests.table1 add (s1 int static);").get();
        schema_ptr s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("c2")));
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("c3")));
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("s1")));

        e.execute_cql("alter table tests.table1 drop (c3, s1);").get();
        e.execute_cql("alter table tests.table1 add (s2 int, s3 int static);").get();
        s = e.db().local().find_schema("tests", "table1");
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("c1")));
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("c2")));
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("c3")));
        BOOST_REQUIRE(!s->columns_by_name().count(to_bytes("s1")));
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("s2")));
        BOOST_REQUIRE(s->columns_by_name().count(to_bytes("s3")));
    });
}

SEASTAR_TEST_CASE(test_combined_column_add_and_drop) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            service::migration_manager& mm = service::get_local_migration_manager();

            e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();

            auto s1 = schema_builder("ks", "table1")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("v1", bytes_type)
                    .build();

            mm.announce_new_column_family(s1, false).get();

            auto&& keyspace = e.db().local().find_keyspace(s1->ks_name()).metadata();

            auto s2 = schema_builder("ks", "table1", std::make_optional(s1->id()))
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .without_column("v1", bytes_type, api::new_timestamp())
                    .build();

            // Drop v1
            {
                auto muts = db::schema_tables::make_update_table_mutations(keyspace, s1, s2,
                    api::new_timestamp(), false);
                mm.announce(std::move(muts), true).get();
            }

            // Add a new v1 and drop it
            {
                auto s3 = schema_builder("ks", "table1", std::make_optional(s1->id()))
                        .with_column("pk", bytes_type, column_kind::partition_key)
                        .with_column("v1", list_type_impl::get_instance(int32_type, true))
                        .build();

                auto s4 = schema_builder("ks", "table1", std::make_optional(s1->id()))
                        .with_column("pk", bytes_type, column_kind::partition_key)
                        .without_column("v1", list_type_impl::get_instance(int32_type, true), api::new_timestamp())
                        .build();

                auto muts = db::schema_tables::make_update_table_mutations(keyspace, s3, s4,
                    api::new_timestamp(), false);
                mm.announce(std::move(muts), true).get();
            }

            auto new_schema = e.db().local().find_schema(s1->id());
            BOOST_REQUIRE(new_schema->get_column_definition(to_bytes("v1")) == nullptr);

            assert_that_failed(e.execute_cql("alter table ks.table1 add v1 list<text>;"));
        });
    });
}

SEASTAR_TEST_CASE(test_merging_does_not_alter_tables_which_didnt_change) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            service::migration_manager& mm = service::get_local_migration_manager();

            auto&& keyspace = e.db().local().find_keyspace("ks").metadata();

            auto s0 = schema_builder("ks", "table1")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("v1", bytes_type)
                .build();

            auto find_table = [&] () -> column_family& {
                return e.db().local().find_column_family("ks", "table1");
            };

            auto muts1 = db::schema_tables::make_create_table_mutations(keyspace, s0, api::new_timestamp());
            mm.announce(muts1).get();

            auto s1 = find_table().schema();

            auto legacy_version = s1->version();

            mm.announce(muts1).get();

            BOOST_REQUIRE(s1 == find_table().schema());
            BOOST_REQUIRE_EQUAL(legacy_version, find_table().schema()->version());

            auto muts2 = muts1;
            muts2.push_back(db::schema_tables::make_scylla_tables_mutation(s0, api::new_timestamp()));
            mm.announce(muts2).get();

            BOOST_REQUIRE(s1 == find_table().schema());
            BOOST_REQUIRE_EQUAL(legacy_version, find_table().schema()->version());
        });
    });
}

class counting_migration_listener : public service::migration_listener {
public:
    int create_keyspace_count = 0;
    int create_column_family_count = 0;
    int create_user_type_count = 0;
    int create_function_count = 0;
    int create_aggregate_count = 0;
    int create_view_count = 0;
    int update_keyspace_count = 0;
    int update_column_family_count = 0;
    int columns_changed_count = 0;
    int update_user_type_count = 0;
    int update_function_count = 0;
    int update_aggregate_count = 0;
    int update_view_count = 0;
    int drop_keyspace_count = 0;
    int drop_column_family_count = 0;
    int drop_user_type_count = 0;
    int drop_function_count = 0;
    int drop_aggregate_count = 0;
    int drop_view_count = 0;
public:
    virtual void on_create_keyspace(const sstring&) override { ++create_keyspace_count; }
    virtual void on_create_column_family(const sstring&, const sstring&) override { ++create_column_family_count; }
    virtual void on_create_user_type(const sstring&, const sstring&) override { ++create_user_type_count; }
    virtual void on_create_function(const sstring&, const sstring&) override { ++create_function_count; }
    virtual void on_create_aggregate(const sstring&, const sstring&) override { ++create_aggregate_count; }
    virtual void on_create_view(const sstring&, const sstring&) override { ++create_view_count; }
    virtual void on_update_keyspace(const sstring&) override { ++update_keyspace_count; }
    virtual void on_update_column_family(const sstring&, const sstring&, bool columns_changed) override {
        ++update_column_family_count;
        columns_changed_count += int(columns_changed);
    }
    virtual void on_update_user_type(const sstring&, const sstring&) override { ++update_user_type_count; }
    virtual void on_update_function(const sstring&, const sstring&) override { ++update_function_count; }
    virtual void on_update_aggregate(const sstring&, const sstring&) override { ++update_aggregate_count; }
    virtual void on_update_view(const sstring&, const sstring&, bool) override { ++update_view_count; }
    virtual void on_drop_keyspace(const sstring&) override { ++drop_keyspace_count; }
    virtual void on_drop_column_family(const sstring&, const sstring&) override { ++drop_column_family_count; }
    virtual void on_drop_user_type(const sstring&, const sstring&) override { ++drop_user_type_count; }
    virtual void on_drop_function(const sstring&, const sstring&) override { ++drop_function_count; }
    virtual void on_drop_aggregate(const sstring&, const sstring&) override { ++drop_aggregate_count; }
    virtual void on_drop_view(const sstring&, const sstring&) override { ++drop_view_count; }
};

SEASTAR_TEST_CASE(test_notifications) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            counting_migration_listener listener;
            service::get_local_migration_manager().register_listener(&listener);
            auto listener_lease = defer([&listener] { service::get_local_migration_manager().register_listener(&listener); });

            e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();

            BOOST_REQUIRE_EQUAL(listener.create_keyspace_count, 1);

            e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int);").get();

            BOOST_REQUIRE_EQUAL(listener.create_column_family_count, 1);
            BOOST_REQUIRE_EQUAL(listener.columns_changed_count, 0);

            e.execute_cql("alter table tests.table1 drop c2;").get();

            BOOST_REQUIRE_EQUAL(listener.update_column_family_count, 1);
            BOOST_REQUIRE_EQUAL(listener.columns_changed_count, 1);

            e.execute_cql("alter table tests.table1 add s1 int;").get();

            BOOST_REQUIRE_EQUAL(listener.update_column_family_count, 2);
            BOOST_REQUIRE_EQUAL(listener.columns_changed_count, 2);

            e.execute_cql("alter table tests.table1 alter s1 type blob;").get();

            BOOST_REQUIRE_EQUAL(listener.update_column_family_count, 3);
            BOOST_REQUIRE_EQUAL(listener.columns_changed_count, 3);

            e.execute_cql("drop table tests.table1;").get();

            BOOST_REQUIRE_EQUAL(listener.drop_column_family_count, 1);

            e.execute_cql("create type tests.type1 (field1 text, field2 text);").get();

            BOOST_REQUIRE_EQUAL(listener.create_user_type_count, 1);

            e.execute_cql("drop type tests.type1;").get();

            BOOST_REQUIRE_EQUAL(listener.drop_user_type_count, 1);

            e.execute_cql("create type tests.type1 (field1 text, field2 text);").get();
            e.execute_cql("create type tests.type2 (field1 text, field2 text);").get();

            BOOST_REQUIRE_EQUAL(listener.create_user_type_count, 3);

            e.execute_cql("drop type tests.type1;").get();

            BOOST_REQUIRE_EQUAL(listener.drop_user_type_count, 2);

            e.execute_cql("alter type tests.type2 add field3 text;").get();

            BOOST_REQUIRE_EQUAL(listener.update_user_type_count, 1);

            e.execute_cql("alter type tests.type2 alter field3 type blob;").get();

            BOOST_REQUIRE_EQUAL(listener.update_user_type_count, 2);

            e.execute_cql("alter type tests.type2 rename field2 to field4 and field3 to field5;").get();

            BOOST_REQUIRE_EQUAL(listener.update_user_type_count, 3);
        });
    });
}

SEASTAR_TEST_CASE(test_prepared_statement_is_invalidated_by_schema_change) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            logging::logger_registry().set_logger_level("query_processor", logging::log_level::debug);
            e.execute_cql("create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int);").get();
            auto id = e.prepare("select * from tests.table1;").get0();

            e.execute_cql("alter table tests.table1 add s1 int;").get();

            try {
                e.execute_prepared(id, {}).get();
                BOOST_FAIL("Should have failed");
            } catch (const not_prepared_exception&) {
                // expected
            }
        });
    });
}

// We don't want schema digest to change between Scylla versions because that results in a schema disagreement
// during rolling upgrade.
SEASTAR_TEST_CASE(test_schema_digest_does_not_change) {
    using namespace db;
    using namespace db::schema_tables;

    auto tmp = tmpdir();
    const bool regenerate = false;

    sstring data_dir = "./tests/sstables/schema_digest_test";

    db::config db_cfg;
    if (regenerate) {
        db_cfg.data_file_directories({data_dir}, db::config::config_source::CommandLine);
    } else {
        fs::copy(std::string(data_dir), std::string(tmp.path().string()), fs::copy_options::recursive);
        db_cfg.data_file_directories({tmp.path().string()}, db::config::config_source::CommandLine);
    }

    return do_with_cql_env_thread([regenerate](cql_test_env& e) {
        if (regenerate) {
            // Exercise many different kinds of schema changes.
            e.execute_cql(
                "create keyspace tests with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table tests.table1 (pk int primary key, c1 int, c2 int);").get();
            e.execute_cql("create type tests.basic_info (c1 timestamp, v2 text);").get();
            e.execute_cql("create index on tests.table1 (c1);").get();
            e.execute_cql("create table ks.tbl (a int, b int, c float, PRIMARY KEY (a))").get();
            e.execute_cql(
                "create materialized view ks.tbl_view AS SELECT c FROM ks.tbl WHERE c IS NOT NULL PRIMARY KEY (c, a)").get();
            e.execute_cql(
                "create materialized view ks.tbl_view_2 AS SELECT a FROM ks.tbl WHERE a IS NOT NULL PRIMARY KEY (a)").get();
            e.execute_cql(
                "create keyspace tests2 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("drop keyspace tests2;").get();
        }

        auto expect_digest = [&] (schema_features sf, utils::UUID expected) {
            auto actual = calculate_schema_digest(service::get_storage_proxy(), sf).get0();
            if (regenerate) {
                std::cout << "Digest is " << actual << "\n";
            } else {
                BOOST_REQUIRE_EQUAL(actual, expected);
            }
        };

        auto expect_version = [&] (sstring ks_name, sstring cf_name, utils::UUID expected) {
            auto actual = e.local_db().find_column_family(ks_name, cf_name).schema()->version();
            if (regenerate) {
                std::cout << "Version of " << ks_name << "." << cf_name << " is " << actual << "\n";
            } else {
                BOOST_REQUIRE_EQUAL(actual, expected);
            }
        };

        schema_features sf = schema_features::of<schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY>();

        expect_digest(sf, utils::UUID("492719e5-0169-30b1-a15e-3447674c0c0c"));

        sf.set<schema_feature::VIEW_VIRTUAL_COLUMNS>();
        expect_digest(sf, utils::UUID("be3c0af4-417f-31d5-8e0e-4ac257ec00ad"));

        expect_digest(schema_features::full(), utils::UUID("be3c0af4-417f-31d5-8e0e-4ac257ec00ad"));

        // Causes tombstones to become expired
        // This is in order to test that schema disagreement doesn't form due to expired tombstones being collected
        // Refs https://github.com/scylladb/scylla/issues/4485
        forward_jump_clocks(std::chrono::seconds(60*60*24*31));

        expect_digest(schema_features::full(), utils::UUID("be3c0af4-417f-31d5-8e0e-4ac257ec00ad"));

        // FIXME: schema_mutations::digest() is still sensitive to expiry, so we can check versions only after forward_jump_clocks()
        // otherwise the results would not be stable.
        expect_version("tests", "table1", utils::UUID("4198e26c-f214-3888-9c49-c396eb01b8d7"));
        expect_version("ks", "tbl", utils::UUID("5c9cadec-e5df-357e-81d0-0261530af64b"));
        expect_version("ks", "tbl_view", utils::UUID("1d91ad22-ea7c-3e7f-9557-87f0f3bb94d7"));
        expect_version("ks", "tbl_view_2", utils::UUID("2dcd4a37-cbb5-399b-b3c9-8eb1398b096b"));
    }, db_cfg).then([tmp = std::move(tmp)] {});
}
