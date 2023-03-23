/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "exceptions/exceptions.hh"
#include "locator/token_metadata.hh"
#include "locator/tablets.hh"

#include <seastar/core/sstring.hh>

#include <optional>
#include <set>
#include <string_view>

namespace locator {

/// Trait class which allows replication strategies to work in a mode which
/// uses tablet-based replication.
///
/// Contains common logic, like parsing tablet options,
/// and creating effective_replication_map for a given table which works with
/// system's tablet_metadata.
class tablet_aware_replication_strategy : public per_table_replication_strategy {
protected:
    void validate_tablet_options(const gms::feature_service&, const replication_strategy_config_options&) const;
    void process_tablet_options(abstract_replication_strategy&);
    std::unordered_set<sstring> recognized_tablet_options() const;
    effective_replication_map_ptr do_make_replication_map(table_id,
                                                          replication_strategy_ptr,
                                                          token_metadata_ptr,
                                                          size_t replication_factor) const;

public:
    /// Generates tablet_map for a new table.
    /// Runs under group0 guard.
    virtual future<tablet_map> allocate_tablets_for_new_table(schema_ptr, token_metadata_ptr) const = 0;
};

} // namespace locator
