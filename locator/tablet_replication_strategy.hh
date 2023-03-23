/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "exceptions/exceptions.hh"
#include "locator/token_metadata.hh"

#include <seastar/core/sstring.hh>

#include <optional>
#include <set>
#include <string_view>

namespace locator {

// Trait class to allow a replication strategy to be aware of tablet replication.
class tablet_aware_replication_strategy : public per_table_replication_strategy {
protected:
    bool _uses_tablets = false;
protected: // Interface to replication strategy which extends this trait
    void validate_tablet_options(const gms::feature_service&, const replication_strategy_config_options&) const;
    void process_tablet_options(abstract_replication_strategy&);
    std::unordered_set<sstring> recognized_tablet_options() const;
    effective_replication_map_ptr do_make_replication_map(table_id,
                                                          replication_strategy_ptr,
                                                          token_metadata_ptr,
                                                          size_t replication_factor) const;
public: // Interface which extends replication strategy which inherits from this trait.
    bool uses_tablets() const { return _uses_tablets; }
};

} // namespace locator
