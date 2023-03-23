/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "dht/token.hh"
#include "utils/small_vector.hh"
#include "locator/host_id.hh"
#include "schema/schema_fwd.hh"
#include "utils/chunked_vector.hh"

#include <seastar/core/reactor.hh>
#include <seastar/util/log.hh>

#include <vector>

namespace locator {

extern seastar::logger tablet_logger;

using token = dht::token;

// Identifies tablet within the scope of a single tablet_map,
// which has a scope of (table_id, token metadata version).
// Different tablets of different tables can have the same tablet_id.
// Different tablets in subsequent token metadata version can have the same tablet_id.
// When splitting a tablet, one of the new tablets (in the new token metadata version)
// will have the same tablet_id as the old one.
using tablet_id = size_t;

struct tablet_replica {
    host_id host;
    shard_id shard;
};

std::ostream& operator<<(std::ostream&, const tablet_replica&);

using tablet_replica_set = utils::small_vector<tablet_replica, 3>;

struct tablet_info {
    tablet_replica_set current;
};

// Used for storing state during topology changes.
struct tablet_transition_info {
    tablet_replica_set next;
    host_id pending_replica; // Optimization (next - current)
};

class tablet_map {
    // _tablets.size() == 1 << _x_log2_tablets
    utils::chunked_vector<tablet_info> _tablets;
    size_t _x_log2_tablets;
    std::optional<std::unordered_map<tablet_id, tablet_transition_info>> _transitions;
public:
    tablet_map(size_t tablet_count);
    tablet_id get_tablet_id(token) const;
    const tablet_info& get_tablet_info(tablet_id) const;
    const tablet_transition_info* get_tablet_transition_info(tablet_id) const;

    size_t tablet_count() const {
        return _tablets.size();
    }

    const tablet_info& get_tablet_info(token t) const {
        return get_tablet_info(get_tablet_id(t));
    }
};

// Holds information about all tablets in the cluster.
//
// When this instance is obtained via token_metadata_ptr, it is immutable
// and references obtained through this are guaranteed to remain valid
// as long as the token_metadata_ptr is held.
class tablet_metadata {
    std::unordered_map<table_id, tablet_map> _tablets;
public:
    const tablet_map& get_tablet_map(table_id id) const;
};

}
