/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "dht/token.hh"
#include "timestamp.hh"
#include "locator/host_id.hh"
#include "dht/i_partitioner_fwd.hh"
#include "schema/schema_fwd.hh"
#include "utils/chunked_vector.hh"
#include "utils/hash.hh"
#include "utils/small_vector.hh"

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

    bool operator==(const tablet_replica&) const = default;
};

using tablet_replica_set = utils::small_vector<tablet_replica, 3>;

/// Stores information about a single tablet.
struct tablet_info {
    tablet_replica_set replicas;

    bool operator==(const tablet_info&) const = default;
};

/// Used for storing tablet state transition during topology changes.
/// Describes transition of a single tablet.
struct tablet_transition_info {
    tablet_replica_set next;
    tablet_replica pending_replica; // Optimization (next - tablet_info::replicas)

    bool operator==(const tablet_transition_info&) const = default;
};

/// Stores information about tablets of a single table.
class tablet_map {
public:
    using tablet_container = utils::chunked_vector<tablet_info>;
private:
    // The implementation assumes that _tablets.size() is a power of 2:
    //
    //   _tablets.size() == 1 << _log2_tablets
    //
    tablet_container _tablets;
    size_t _log2_tablets; // log_2(_tablets.size())
    std::unordered_map<tablet_id, tablet_transition_info> _transitions;
public:
    tablet_map();

    /// Constructs a tablet map.
    /// The actual number of allocated tablets may be higher due to rounding up to the power of two.
    /// Check tablet_count().
    ///
    /// \param tablet_count The desired tablets to allocate.
    tablet_map(size_t tablet_count);

    tablet_id get_tablet_id(token) const;
    const tablet_info& get_tablet_info(tablet_id) const;
    const tablet_transition_info* get_tablet_transition_info(tablet_id) const;
    dht::token get_last_token(tablet_id id) const;
    dht::token_range get_token_range(tablet_id id) const;

    const tablet_container& tablets() const {
        return _tablets;
    }

    size_t tablet_count() const {
        return _tablets.size();
    }

    const tablet_info& get_tablet_info(token t) const {
        return get_tablet_info(get_tablet_id(t));
    }

    bool operator==(const tablet_map&) const = default;
public:
    void set_tablet(tablet_id, tablet_info);
    void set_tablet_transition_info(tablet_id, tablet_transition_info);

    // Destroys gently.
    // The tablet map is not usable after this call and should be destroyed.
    future<> clear_gently();

    friend std::ostream& operator<<(std::ostream&, const tablet_map&);
};

/// Holds information about all tablets in the cluster.
///
/// When this instance is obtained via token_metadata_ptr, it is immutable
/// (represents a snapshot) and references obtained through this are guaranteed
/// to remain valid as long as the containing token_metadata_ptr is held.
///
/// Copy constructor can be invoked across shards.
class tablet_metadata {
public:
    // FIXME: Make cheap to copy.
    // We want both immutability and cheap updates, so we should use
    // hierarchical data structure with shared pointers and copy-on-write.
    // Currently we have immutability but updates require full copy.
    //
    // Also, currently the copy constructor is invoked across shards, which precludes
    // using shared pointers. We should change that and use a foreign_ptr<> to
    // hold immutable tablet_metadata which lives on shard 0 only.
    // See storage_service::replica_to_all_cores().
    using table_to_map = std::unordered_map<table_id, tablet_map>;
private:
    table_to_map _tablets;
public:
    const tablet_map& get_tablet_map(table_id id) const;
    const table_to_map& all_tables() const { return _tablets; }
public:
    void set_tablet_map(table_id, tablet_map);
    void drop_table(table_id);
    future<> clear_gently();
public:
    bool operator==(const tablet_metadata&) const = default;
    friend std::ostream& operator<<(std::ostream&, const tablet_metadata&);
};

}

namespace std {

template<>
struct hash<locator::tablet_replica> {
    size_t operator()(const locator::tablet_replica& r) const {
        return utils::hash_combine(
                std::hash<locator::host_id>()(r.host),
                std::hash<shard_id>()(r.shard));
    }
};


}
