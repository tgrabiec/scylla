/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "dht/token-sharding.hh"
#include "locator/tablets.hh"
#include "locator/token_metadata.hh"

namespace locator {

/// Implements sharder object which reflects assignment of tablets of a given table to local shards.
/// Token ranges which don't have local tablets are reported to belong to shard 0.
class tablet_sharder : public dht::sharder {
    const token_metadata& _tm;
    table_id _table;
    mutable const tablet_map* _tmap = nullptr;
private:
    // Tablet map is lazily initialized to avoid exceptions during effective_replication_map construction
    // in case tablet mapping is not yet available in token metadata at the time the table is constructed.
    void ensure_tablet_map() const {
        if (!_tmap) {
            _tmap = &_tm.tablets().get_tablet_map(_table);
        }
    }

    std::optional<unsigned> get_shard(const tablet_replica_set& replicas, host_id host) const {
        for (auto&& r : replicas) {
            if (r.host == host) {
                return r.shard;
            }
        }
        return std::nullopt;
    };


    dht::shard_replica_set shard_of(tablet_id tid, host_id host, dht::replica_set_kind kind) const {
        auto* trinfo = _tmap->get_tablet_transition_info(tid);
        auto& tinfo = _tmap->get_tablet_info(tid);
        dht::shard_replica_set shards;

        auto push_from = [&] (const tablet_replica_set& replicas) {
            for (auto&& r : replicas) {
                if (r.host == host) {
                    shards.push_back(r.shard);
                }
            }
        };

        if (!trinfo) [[likely]] {
            push_from(tinfo.replicas);
        } else {
            write_replica_set_selector sel = kind == dht::replica_set_kind::for_reads
                                             ? as_write_selector(trinfo->reads) : trinfo->writes;
            switch (sel) {
                case write_replica_set_selector::both:
                    if (trinfo->pending_replica.host == host) {
                        shards.push_back(trinfo->pending_replica.shard);
                    }
                    [[fallthrough]];
                case write_replica_set_selector::previous:
                    push_from(tinfo.replicas);
                    break;
                case write_replica_set_selector::next:
                    push_from(trinfo->next);
                    break;
            }
        }

        return shards;
    }

    std::optional<shard_id> get_shard_for_read(tablet_id tid, host_id host) const {
        ensure_tablet_map();
        auto* trinfo = _tmap->get_tablet_transition_info(tid);
        auto& tinfo = _tmap->get_tablet_info(tid);

        if (!trinfo) {
            return get_shard(tinfo.replicas, host);
        }

        if (trinfo->pending_replica.host == host) {
            if (trinfo->transition == tablet_transition_kind::intranode_migration && trinfo->reads == read_replica_set_selector::previous) {
                return get_shard(tinfo.replicas, host);
            }
            return trinfo->pending_replica.shard;
        }

        return get_shard(tinfo.replicas, host);
    }
public:
    tablet_sharder(const token_metadata& tm, table_id table)
            : _tm(tm)
            , _table(table)
    { }

    virtual ~tablet_sharder() = default;

    virtual unsigned shard_of(const dht::token& t) const override {
        ensure_tablet_map();
        auto tid = _tmap->get_tablet_id(t);
        auto shard = get_shard_for_read(tid, _tm.get_my_id()).value_or(0);
        tablet_logger.trace("[{}] shard_of({}) = {}, tablet={}", _table, t, shard, tid);
        return shard;
    }

    virtual dht::shard_replica_set shard_of(const token& t, dht::replica_set_kind kind) const override {
        ensure_tablet_map();
        auto tid = _tmap->get_tablet_id(t);
        auto shards = shard_of(tid, _tm.get_my_id(), kind);
        tablet_logger.trace("[{}] shard_of({}, {}) = {}, tablet={}", _table, t, kind, shards, tid);
        return shards;
    }

    virtual std::optional<unsigned> shard_of(const token& t, dht::replica_set_selector sel) const override {
        ensure_tablet_map();
        auto tid = _tmap->get_tablet_id(t);
        auto* trinfo = _tmap->get_tablet_transition_info(tid);
        auto& tinfo = _tmap->get_tablet_info(tid);
        auto host = _tm.get_my_id();

        auto& replicas = std::invoke([&] () -> const tablet_replica_set& {
            if (!trinfo) [[likely]] {
                return tinfo.replicas;
            }
            switch (sel) {
                case dht::replica_set_selector::previous:
                    return tinfo.replicas;
                case dht::replica_set_selector::next:
                    return trinfo->next;
            }
            __builtin_unreachable();
        });

        std::optional<unsigned> res;
        for (auto&& r : replicas) {
            if (r.host == host) {
                res = r.shard;
                break;
            }
        }

        tablet_logger.trace("[{}] shard_of({}, {}) = {}, tablet={}", _table, t, sel, res, tid);
        return res;
    }

    virtual std::optional<dht::shard_and_token> next_shard(const token& t) const override {
        ensure_tablet_map();
        auto me = _tm.get_my_id();
        std::optional<tablet_id> tb = _tmap->get_tablet_id(t);
        while ((tb = _tmap->next_tablet(*tb))) {
            auto r = get_shard_for_read(*tb, me);
            auto next = _tmap->get_first_token(*tb);
            tablet_logger.trace("[{}] token_for_next_shard({}) = {{{}, {}}}, tablet={}", _table, t, next, r, *tb);
            return dht::shard_and_token{r.value_or(0), next};
        }
        tablet_logger.trace("[{}] token_for_next_shard({}) = null", _table, t);
        return std::nullopt;
    }

    virtual token token_for_next_shard(const token& t, shard_id shard, unsigned spans = 1) const override {
        ensure_tablet_map();
        auto token = t;
        while (auto s_a_t = next_shard(token)) {
            token = s_a_t->token;
            if (s_a_t->shard == shard) {
                if (--spans == 0) {
                    tablet_logger.trace("[{}] token_for_next_shard({}, {}, {}) = {}", _table, t, shard, spans, s_a_t->token);
                    return token;
                }
            }
        }
        tablet_logger.trace("[{}] token_for_next_shard({}, {}, {}) = null", _table, t, shard, spans);
        return dht::maximum_token();
    }
};

} // namespace locator
