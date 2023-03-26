/*
 * Copyright (C) 2023-present-2020 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */



#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"

#include "replica/tablets.hh"
#include "locator/tablets.hh"
#include "locator/tablet_replication_strategy.hh"

using namespace locator;
using namespace replica;

static api::timestamp_type next_timestamp = api::new_timestamp();

static
void verify_tablet_metadata_persistence(cql_test_env& env, const tablet_metadata& tm) {
    save_tablet_metadata(env.local_db(), tm, next_timestamp++).get();
    auto tm2 = read_tablet_metadata(env.local_qp()).get0();
    BOOST_REQUIRE_EQUAL(tm, tm2);
}

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    c.db_config->experimental_features({
            db::experimental_features_t::feature::TABLETS,
            db::experimental_features_t::feature::RAFT
        }, db::config::config_source::CommandLine);
    return c;
}

static
future<table_id> add_table(cql_test_env& e) {
    auto id = table_id(utils::UUID_gen::get_time_UUID());
    co_await e.create_table([id] (std::string_view ks_name) {
        return *schema_builder(ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type)
                .build();
    });
    co_return id;
}

SEASTAR_TEST_CASE(test_tablet_metadata_persistence) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        auto table1 = add_table(e).get0();
        auto table2 = add_table(e).get0();

        {
            tablet_metadata tm;

            // Empty
            verify_tablet_metadata_persistence(e, tm);

            // Add table1
            {
                tablet_map tmap(1);
                tmap.set_tablet(0, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 0},
                        tablet_replica {h2, 3},
                        tablet_replica {h3, 1},
                    }
                });
                tm.set_tablet_map(table1, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm);

            // Add table2
            {
                tablet_map tmap(4);
                tmap.set_tablet(0, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 0},
                    }
                });
                tmap.set_tablet(1, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h3, 3},
                    }
                });
                tmap.set_tablet(2, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h2, 2},
                    }
                });
                tmap.set_tablet(3, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 1},
                    }
                });
                tm.set_tablet_map(table2, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm);

            // Reduce tablet count in table2
            {
                tablet_map tmap(2);
                tmap.set_tablet(0, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 0},
                    }
                });
                tmap.set_tablet(1, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h3, 3},
                    }
                });
                tm.set_tablet_map(table2, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm);

            // Reduce RF for table1, increasing tablet count
            {
                tablet_map tmap(2);
                tmap.set_tablet(0, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h3, 7},
                    }
                });
                tmap.set_tablet(1, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 3},
                    }
                });
                tm.set_tablet_map(table1, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm);

            // Reduce tablet count for table1
            {
                tablet_map tmap(1);
                tmap.set_tablet(0, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 3},
                    }
                });
                tm.set_tablet_map(table1, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm);

            // Change replica of table1
            {
                tablet_map tmap(1);
                tmap.set_tablet(0, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h3, 7},
                    }
                });
                tm.set_tablet_map(table1, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm);
        }
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_huge_tablet_metadata) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        tablet_metadata tm;

        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        const int nr_tables = 1'00;
        const int tablets_per_table = 1024;

        for (int i = 0; i < nr_tables; ++i) {
            tablet_map tmap(tablets_per_table);

            for (tablet_id j = 0; j < tablets_per_table; ++j) {
                tmap.set_tablet(j, tablet_info {
                    tablet_replica_set {{h1, 0}, {h2, 1}, {h3, 2},}
                });
            }

            auto id = add_table(e).get0();
            tm.set_tablet_map(id, std::move(tmap));
        }

        verify_tablet_metadata_persistence(e, tm);
    }, tablet_cql_test_config());
}
