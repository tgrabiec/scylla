/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "locator/tablet_replication_strategy.hh"
#include "locator/tablets.hh"
#include "types/types.hh"
#include "types/tuple.hh"
#include "types/set.hh"
#include "utils/hash.hh"
#include "db/system_keyspace.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "replica/database.hh"

namespace locator {

seastar::logger tablet_logger("tablets");

std::ostream& operator<<(std::ostream& out, const tablet_replica& r) {
    return out << r.host << ":" << r.shard;
}

const tablet_map& tablet_metadata::get_tablet_map(table_id id) const {
    try {
        return _tablets.at(id);
    } catch (const std::out_of_range&) {
        throw std::runtime_error(format("Tablet map not found for table {}", id));
    }
}

void tablet_metadata::set_tablet_map(table_id id, tablet_map map) {
    _tablets[id] = std::move(map);
}

void tablet_metadata::drop_table(table_id id) {
    _tablets.erase(id);
}

future<> tablet_metadata::clear_gently() {
    for (auto&& [id, map] : _tablets) {
        co_await map.clear_gently();
    }
    co_return;
}

tablet_map::tablet_map()
        : tablet_map(1)
{ }

tablet_map::tablet_map(size_t tablet_count)
        : _log2_tablets(log2ceil(tablet_count)) {
    tablet_count = 1ul << _log2_tablets;
    _tablets.resize(tablet_count);
}

const tablet_info& tablet_map::get_tablet_info(tablet_id id) const {
    return _tablets[id];
}

tablet_id tablet_map::get_tablet_id(token t) const {
    return dht::compaction_group_of(_log2_tablets, t);
}

dht::token tablet_map::get_last_token(tablet_id id) const {
    return dht::last_token_of_compaction_group(_log2_tablets, id);
}

dht::token_range tablet_map::get_token_range(tablet_id id) const {
    if (id == 0) {
        return dht::token_range::make(dht::minimum_token(), get_last_token(id));
    } else {
        return dht::token_range::make(get_last_token(id - 1), get_last_token(id));
    }
}

void tablet_map::set_tablet(tablet_id id, tablet_info info) {
    _tablets[id] = std::move(info);
}

void tablet_map::set_tablet_transition_info(tablet_id id, tablet_transition_info info) {
    _transitions[id] = std::move(info);
}

future<> tablet_map::clear_gently() {
    for (auto&& info : _tablets) {
        info.replicas.clear();
        co_await coroutine::maybe_yield();
    }
    _tablets.clear();
    co_return;
}

const tablet_transition_info* tablet_map::get_tablet_transition_info(tablet_id id) const {
    auto i = _transitions.find(id);
    if (i == _transitions.end()) {
        return nullptr;
    }
    return &i->second;
}

std::ostream& operator<<(std::ostream& out, const tablet_map& r) {
    if (r.tablet_count() == 0) {
        return out << "{}";
    }
    out << "{";
    bool first = true;
    tablet_id tid = 0;
    for (auto&& tablet : r._tablets) {
        if (!first) {
            out << ",";
        }
        out << format("\n  [{}]: last_token={}, replicas={}", tid, r.get_last_token(tid), tablet.replicas);
        if (auto tr = r.get_tablet_transition_info(tid)) {
            out << format(", new_replicas={}, pending={}", tr->next, tr->pending_replica);
        }
        first = false;
        ++tid;
    }
    return out << "\n}";
}

std::ostream& operator<<(std::ostream& out, const tablet_metadata& tm) {
    out << "{";
    bool first = true;
    for (auto&& [id, map] : tm._tablets) {
        if (!first) {
            out << ",";
        }
        out << "\n  " << id << ": " << map;
        first = false;
    }
    return out << "\n}";
}

class tablet_effective_replication_map : public effective_replication_map {
    table_id _table;
private:
    gms::inet_address get_endpoint_for_host_id(host_id host) const {
        auto endpoint_opt = _tmptr->get_endpoint_for_host_id(host);
        if (!endpoint_opt) {
            on_internal_error(tablet_logger, format("Host ID {} not found in the cluster", host));
        }
        return *endpoint_opt;
    }
    inet_address_vector_replica_set to_replica_set(const tablet_replica_set& replicas) const {
        inet_address_vector_replica_set result;
        result.reserve(replicas.size());
        for (auto&& replica : replicas) {
            result.emplace_back(get_endpoint_for_host_id(replica.host));
        }
        return result;
    }
    const tablet_map& get_tablet_map() const {
        return _tmptr->tablets().get_tablet_map(_table);
    }
public:
    tablet_effective_replication_map(table_id table,
                                     replication_strategy_ptr rs,
                                     token_metadata_ptr tmptr,
                                     size_t replication_factor)
        : effective_replication_map(std::move(rs), std::move(tmptr), replication_factor)
        , _table(table)
    { }

    virtual ~tablet_effective_replication_map() = default;

    virtual inet_address_vector_replica_set get_natural_endpoints(const token& search_token) const override {
        auto&& tablets = get_tablet_map();
        auto tablet = tablets.get_tablet_id(search_token);
        auto&& replicas = tablets.get_tablet_info(tablet).replicas;
        tablet_logger.trace("get_natural_endpoints({}): table={}, tablet={}, replicas={}", search_token, _table, tablet, replicas);
        return to_replica_set(replicas);
    }

    virtual inet_address_vector_replica_set get_natural_endpoints_without_node_being_replaced(const token& search_token) const override {
        auto result = get_natural_endpoints(search_token);
        maybe_remove_node_being_replaced(*_tmptr, *_rs, result);
        return result;
    }

    virtual inet_address_vector_topology_change get_pending_endpoints(const token& search_token, const sstring& ks_name) const override {
        auto&& tablets = get_tablet_map();
        auto tablet = tablets.get_tablet_id(search_token);
        auto&& info = tablets.get_tablet_transition_info(tablet);
        if (!info) {
            return {};
        }
        tablet_logger.trace("get_pending_endpoints({}): table={}, tablet={}, replica={}",
                            search_token, _table, tablet, info->pending_replica);
        return {get_endpoint_for_host_id(info->pending_replica.host)};
    }
};

void tablet_aware_replication_strategy::validate_tablet_options(const gms::feature_service& fs,
                                                                const replication_strategy_config_options& opts) const {
    for (auto& c: opts) {
        if (c.first == "tablets") {
            if (!fs.tablets) {
                throw exceptions::configuration_exception("Tablet replication is not enabled");
            }
        }
    }
}

void tablet_aware_replication_strategy::process_tablet_options(abstract_replication_strategy& self) {
    for (auto& c: self.get_config_options()) {
        if (c.first == "tablets") {
            self._uses_tablets = true;
            mark_as_per_table(self);
        }
    }
}

std::unordered_set<sstring> tablet_aware_replication_strategy::recognized_tablet_options() const {
    std::unordered_set<sstring> opts;
    opts.insert("tablets");
    return opts;
}

effective_replication_map_ptr tablet_aware_replication_strategy::do_make_replication_map(
        table_id table, replication_strategy_ptr rs, token_metadata_ptr tm, size_t replication_factor) const {
    return seastar::make_shared<tablet_effective_replication_map>(table, std::move(rs), std::move(tm), replication_factor);
}

}