/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "types/types.hh"
#include "types/tuple.hh"
#include "types/set.hh"
#include "db/system_keyspace.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "replica/database.hh"
#include "replica/tablets.hh"

namespace replica {

using namespace locator;

static thread_local auto replica_type = tuple_type_impl::get_instance({uuid_type, int32_type});
static thread_local auto replica_set_type = set_type_impl::get_instance(replica_type, false);

future<mutation>
tablet_map_to_mutation(const tablet_map& tablets, table_id id, const sstring& keyspace_name, const sstring& table_name,
                       api::timestamp_type ts) {
    auto s = db::system_keyspace::tablets();

    auto replicas_to_data_value = [&] (const tablet_replica_set& replicas) {
        std::vector<data_value> result;
        result.reserve(replicas.size());
        for (auto&& replica : replicas) {
            result.emplace_back(make_tuple_value(replica_type, {
                data_value(utils::UUID(replica.host.uuid())),
                data_value(int(replica.shard))
            }));
        }
        return result;
    };

    auto gc_now = gc_clock::now();
    auto tombstone_ts = ts - 1;

    mutation m(s, partition_key::from_exploded(*s, {
        data_value(keyspace_name).serialize_nonnull(),
        data_value(id.uuid()).serialize_nonnull()
    }));
    m.partition().apply(tombstone(tombstone_ts, gc_now));
    m.set_static_cell("tablet_count", data_value(int(tablets.tablet_count())).serialize(), ts);
    m.set_static_cell("table_name", data_value(table_name).serialize(), ts);

    tablet_id tid = 0;
    for (auto&& tablet : tablets.tablets()) {
        auto last_token = tablets.get_last_token(tid);
        auto ck = clustering_key::from_single_value(*s, data_value(dht::token::to_int64(last_token)).serialize_nonnull());
        m.set_clustered_cell(ck, "replicas", make_set_value(replica_set_type, replicas_to_data_value(tablet.replicas)), ts);
        if (auto tr_info = tablets.get_tablet_transition_info(tid)) {
            m.set_clustered_cell(ck, "new_replicas", make_set_value(replica_set_type, replicas_to_data_value(tr_info->next)), ts);
        } else {
            m.set_clustered_cell(ck, *s->get_column_definition("new_replicas"), atomic_cell::make_dead(ts, gc_now));
        }
        ++tid;
        co_await coroutine::maybe_yield();
    }
    co_return std::move(m);
}

mutation make_drop_tablet_map_mutation(const sstring& keyspace_name, table_id id, api::timestamp_type ts) {
    auto s = db::system_keyspace::tablets();
    mutation m(s, partition_key::from_exploded(*s, {
        data_value(keyspace_name).serialize_nonnull(),
        data_value(id.uuid()).serialize_nonnull()
    }));
    m.partition().apply(tombstone(ts, gc_clock::now()));
    return m;
}

static
tablet_replica_set deserialize_replica_set(cql3::untyped_result_set_row::view_type raw_value) {
    tablet_replica_set result;
    auto v = value_cast<set_type_impl::native_type>(
            replica_set_type->deserialize_value(raw_value));
    result.reserve(v.size());
    for (const data_value& replica_v : v) {
        std::vector<data_value> replica_dv = value_cast<tuple_type_impl::native_type>(replica_v);
        result.emplace_back(tablet_replica {
            host_id(value_cast<utils::UUID>(replica_dv[0])),
            shard_id(value_cast<int>(replica_dv[1]))
        });
    }
    return result;
}

future<> save_tablet_metadata(replica::database& db, const tablet_metadata& tm, api::timestamp_type ts) {
    tablet_logger.trace("Saving tablet metadata: {}", tm);
    std::vector<mutation> muts;
    for (auto&& [id, tablets] : tm.all_tables()) {
        auto s = db.find_schema(id); // FIXME: Should we ignore errors here, store names in tablet_map?
        muts.emplace_back(
                co_await tablet_map_to_mutation(tablets, id, s->ks_name(), s->cf_name(), ts));
    }
    co_await db.apply(freeze(muts), db::no_timeout);
}

future<tablet_metadata> read_tablet_metadata(cql3::query_processor& qp) {
    tablet_metadata tm;
    struct active_tablet_map {
        table_id table;
        tablet_map map;
        tablet_id tid = 0;
    };
    std::optional<active_tablet_map> current;
    co_await qp.query_internal("select * from system.tablets",
                               [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
        auto table = table_id(row.get_as<utils::UUID>("table_id"));

        if (!current || current->table != table) {
            if (current) {
                tm.set_tablet_map(current->table, std::move(current->map));
            }
            auto tablet_count = row.get_as<int>("tablet_count");
            current = active_tablet_map{table, tablet_map(tablet_count), 0};
            if (current->map.tablet_count() != tablet_count) {
                throw std::runtime_error(format("tablet count mismatch between on-disk and in-memory metadata for table {} tablet {}: {} != {}",
                                                table, current->map.tablet_count(), current->tid, tablet_count));
            }
        }

        tablet_replica_set tablet_replicas;
        if (row.has("replicas")) {
            tablet_replicas = deserialize_replica_set(row.get_view("replicas"));
        }

        tablet_replica_set new_tablet_replicas;
        if (row.has("new_replicas")) {
            new_tablet_replicas = deserialize_replica_set(row.get_view("new_replicas"));
        }

        if (!new_tablet_replicas.empty()) {
            std::unordered_set<tablet_replica> pending(new_tablet_replicas.begin(), new_tablet_replicas.end());
            for (auto&& r : tablet_replicas) {
                pending.erase(r);
            }
            current->map.set_tablet_transition_info(current->tid, tablet_transition_info{
                std::move(new_tablet_replicas), *pending.begin()});
        }

        current->map.set_tablet(current->tid, tablet_info{std::move(tablet_replicas)});

        auto persisted_last_token = dht::token::from_int64(row.get_as<int64_t>("last_token"));
        auto current_last_token = current->map.get_last_token(current->tid);
        if (current_last_token != persisted_last_token) {
            tablet_logger.debug("current tablet_map: {}", current->map);
            throw std::runtime_error(format("last_token mismatch between on-disk ({}) and in-memory ({}) tablet map for table {} tablet {}",
                                            persisted_last_token, current_last_token, table, current->tid));
        }

        ++current->tid;
        return make_ready_future<stop_iteration>(stop_iteration::no);
    });
    if (current) {
        tm.set_tablet_map(current->table, std::move(current->map));
    }
    tablet_logger.trace("Read tablet metadata: {}", tm);
    co_return std::move(tm);
}

}
