/*
 * Copyright (C) 2020 ScyllaDB
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
#include "progress.hh"
#include <seastar/core/coroutine.hh>

namespace raft {

bool follower_progress::is_stray_reject(const append_reply::rejected& rejected) {
    switch (state) {
    case follower_progress::state::PIPELINE:
        if (rejected.non_matching_idx <= match_idx) {
            // If rejected index is smaller that matched it means this is a stray reply
            return true;
        }
        break;
    case follower_progress::state::PROBE:
        // In the probe state the reply is only valid if it matches next_idx - 1, since only
        // one append request is outstanding.
        if (rejected.non_matching_idx != index_t(next_idx - 1)) {
            return true;
        }
        break;
    case follower_progress::state::SNAPSHOT:
        // any reject during snapshot transfer is stray one
        return true;
    default:
        assert(false);
    }
    return false;
}

void follower_progress::become_probe() {
    state = state::PROBE;
    probe_sent = false;
}

void follower_progress::become_pipeline() {
    if (state != state::PIPELINE) {
        // If a previous request was accepted, move to "pipeline" state
        // since we now know the follower's log state.
        state = state::PIPELINE;
        in_flight = 0;
    }
}

void follower_progress::become_snapshot() {
    state = state::SNAPSHOT;
}

bool follower_progress::can_send_to() {
    switch (state) {
    case state::PROBE:
        return !probe_sent;
    case state::PIPELINE:
        // allow `max_in_flight` outstanding indexes
        // FIXME: make it smarter
        return in_flight < follower_progress::max_in_flight;
    case state::SNAPSHOT:
        // In this state we are waiting
        // for a snapshot to be transferred
        // before starting to sync the log.
        return false;
    }
    assert(false);
    return false;
}

// If this is called when a tracker is just created, the current
// progress is empty and we should simply crate an instance for
// each follower.
// When switching configurations, we should preserve progress
// for existing followers, crate progress for new, and remove
// progress for non-members (to make sure we don't send noise
// messages to them).
void tracker::set_configuration(configuration configuration, index_t next_idx) {
    _configuration = std::move(configuration);
    _leader_progress = nullptr;
    // Swap out the current progress and then re-add
    // only those entries which are still present.
    progress old_progress = std::move(*this);

    auto emplace_simple_config = [&](const std::unordered_set<server_address>& config) {
        for (const auto& s : config) {
            auto newp = this->progress::find(s.id);
            if (newp != this->progress::end()) {
                // Processing joint configuration and already added
                // an entry for this id.
                continue;
            }
            auto oldp = old_progress.find(s.id);
            if (oldp != old_progress.end()) {
                newp = this->progress::emplace(s.id, std::move(oldp->second)).first;
            } else {
                newp = this->progress::emplace(s.id, follower_progress{s.id, next_idx}).first;
            }
            if (s.id == _my_id) {
                // The leader is part of the current
                // configuration.
                _leader_progress = &newp->second;
            }
        }
    };
    emplace_simple_config(_configuration.current);
    if (_configuration.is_joint()) {
        emplace_simple_config(_configuration.previous);
    }
}

// A sorted array of node match indexes used to find
// the pivot which serves as commit index of the group.
class match_vector {
    std::vector<index_t> _match;
    // How many elements in the match array have a match index
    // larger than the previous commit index.
    size_t _count = 0;
    index_t _prev_commit_idx;
public:
    explicit match_vector(index_t prev_commit_idx, size_t reserve_size)
            : _prev_commit_idx(prev_commit_idx) {
        _match.reserve(reserve_size);
    }

    void push_back(index_t match_idx) {
        if (match_idx > _prev_commit_idx) {
            _count++;
        }
        _match.push_back(match_idx);
    }
    bool committed() const {
        return _count >= _match.size()/2 + 1;
    }
    index_t commit_idx() {
        logger.trace("check committed count {} cluster size {}", _count, _match.size());
        // The index of the pivot node is selected so that all nodes
        // with a larger match index plus the pivot form a majority,
        // for example:
        // cluster size  pivot node     majority
        // 1             0              1
        // 2             0              2
        // 3             1              2
        // 4             1              3
        // 5             2              3
        //
        auto pivot = (_match.size() - 1) / 2;
        std::nth_element(_match.begin(), _match.begin() + pivot, _match.end());
        return _match[pivot];
    }
};

index_t tracker::committed(index_t prev_commit_idx) {

    match_vector current(prev_commit_idx, _configuration.current.size());

    if (_configuration.is_joint()) {
        match_vector previous(prev_commit_idx, _configuration.previous.size());

        for (const auto& [id, p] : *this) {
            if (_configuration.current.find(server_address{p.id}) != _configuration.current.end()) {
                current.push_back(p.match_idx);
            }
            if (_configuration.previous.find(server_address{p.id}) != _configuration.previous.end()) {
                previous.push_back(p.match_idx);
            }
        }
        if (!current.committed() || !previous.committed()) {
            return prev_commit_idx;
        }
        return std::min(current.commit_idx(), previous.commit_idx());
    } else {
        for (const auto& [id, p] : *this) {
            current.push_back(p.match_idx);
        }
        if (!current.committed()) {
            return prev_commit_idx;
        }
        return current.commit_idx();
    }
}

void votes::set_configuration(configuration configuration) {
    _configuration = std::move(configuration);
    _voters = _configuration.current;
    if (_configuration.is_joint()) {
        _voters.insert(_configuration.previous.begin(), _configuration.previous.end());
    }
}

void votes::register_vote(server_id from, bool granted) {
    server_address from_address{from};
    bool registered = false;

    if (_configuration.current.find(from_address) != _configuration.current.end()) {
        _current.register_vote(granted);
        registered = true;
    }
    if (_configuration.is_joint() &&
        _configuration.previous.find(from_address) != _configuration.previous.end()) {
        _previous.register_vote(granted);
        registered = true;
    }
    // Should never receive a vote not requested, unless an RPC
    // bug.
    assert(registered);
}

vote_result votes::tally_votes() const {
    if (_configuration.is_joint()) {
        auto previous_result = _previous.tally_votes(_configuration.previous.size());
        if (previous_result != vote_result::WON) {
            return previous_result;
        }
    }
    return _current.tally_votes(_configuration.current.size());
}

std::ostream& operator<<(std::ostream& os, const election_tracker& v) {
    os << "responded: " << v._responded << ", ";
    os << "granted: " << v._granted;
    return os;
}


std::ostream& operator<<(std::ostream& os, const votes& v) {
    os << "current: " << v._current << std::endl;
    os << "previous: " << v._previous << std::endl;
    return os;
}

} // end of namespace raft
