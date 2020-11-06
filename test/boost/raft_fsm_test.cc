/*
 * Copyright (c) 2020, Arm Limited and affiliates. All rights reserved.
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

#define BOOST_TEST_MODULE raft

#include <boost/test/unit_test.hpp>
#include "test/lib/log.hh"

#include "raft/fsm.hh"

using raft::term_t, raft::index_t, raft::server_id;

void election_threshold(raft::fsm& fsm) {
    for (int i = 0; i <= raft::ELECTION_TIMEOUT.count(); i++) {
        fsm.tick();
    }
}

void election_timeout(raft::fsm& fsm) {
    for (int i = 0; i <= 2 * raft::ELECTION_TIMEOUT.count(); i++) {
        fsm.tick();
    }
}

struct failure_detector: public raft::failure_detector {
    bool alive = true;
    bool is_alive(raft::server_id from) override {
        return alive;
    }
};

raft::fsm_config fsm_cfg{.append_request_threshold = 1};

BOOST_AUTO_TEST_CASE(test_election_single_node) {

    failure_detector fd;
    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};
    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    BOOST_CHECK(fsm.is_follower());

    election_timeout(fsm);

    // Immediately converts from leader to follower if quorum=1
    BOOST_CHECK(fsm.is_leader());

    auto output = fsm.get_output();

    BOOST_CHECK(output.term);
    BOOST_CHECK(output.vote);
    BOOST_CHECK(output.messages.empty());
    BOOST_CHECK(output.log_entries.empty());
    BOOST_CHECK(output.committed.empty());
    // The leader does not become candidate simply because
    // a timeout has elapsed, i.e. there are no spurious
    // elections.
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    BOOST_CHECK(!output.term);
    BOOST_CHECK(!output.vote);
    BOOST_CHECK(output.messages.empty());
    BOOST_CHECK(output.log_entries.empty());
    BOOST_CHECK(output.committed.empty());
}

// Test that adding an entry to a single-node cluster
// does not lead to RPC
BOOST_AUTO_TEST_CASE(test_single_node_is_quiet) {

    failure_detector fd;
    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};

    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    election_timeout(fsm);

    // Immediately converts from leader to follower if quorum=1
    BOOST_CHECK(fsm.is_leader());

    (void) fsm.get_output();

    fsm.add_entry(raft::command{});

    BOOST_CHECK(fsm.get_output().messages.empty());
}

BOOST_AUTO_TEST_CASE(test_election_two_nodes) {

    failure_detector fd;

    server_id id1{utils::make_random_uuid()}, id2{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2});
    raft::log log{raft::snapshot{.config = cfg}};

    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // After election timeout, a follower becomes a candidate
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    // If nothing happens, the candidate stays this way
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    auto output = fsm.get_output();
    // After a favourable reply, we become a leader (quorum is 2)
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());
    // Out of order response to the previous election is ignored
    fsm.step(id2, raft::vote_reply{output.term - term_t{1}, false});
    assert(fsm.is_leader());

    // Vote request within the election timeout is ignored
    // (avoiding disruptive leaders).
    fsm.step(id2, raft::vote_request{output.term + term_t{1}});
    BOOST_CHECK(fsm.is_leader());
    // Any message with a newer term after election timeout
    // -> immediately convert to follower
    fd.alive = false;
    election_threshold(fsm);
    // Use current_term + 2 to switch fsm to follower
    // even if it itself switched to a candidate
    fsm.step(id2, raft::vote_request{output.term + term_t{2}});
    BOOST_CHECK(fsm.is_follower());

    // Check that the candidate converts to a follower as well
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    fsm.step(id2, raft::vote_request{output.term + term_t{1}});
    BOOST_CHECK(fsm.is_follower());

    // Test that a node doesn't cast a vote if it has voted for
    // self already
    (void) fsm.get_output();
    while (fsm.is_follower()) {
        fsm.tick();
    }
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    auto msg = std::get<raft::vote_request>(output.messages.back().second);
    fsm.step(id2, std::move(msg));
    // We could figure out this round is going to a nowhere, but
    // we're not that smart and simply wait for a vote_reply.
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    auto reply = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(!reply.vote_granted);
}

BOOST_AUTO_TEST_CASE(test_election_four_nodes) {

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()},
              id4{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2, id3, id4});
    raft::log log{raft::snapshot{.config = cfg}};

    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Inform FSM about a new leader at a new term
    fsm.step(id4, raft::append_request_recv{term_t{1}, id4, index_t{1}, term_t{1}});

    (void) fsm.get_output();

    // Request a vote during the same term. Even though
    // we haven't voted, we should deny a vote because we
    // know about a leader for this term.
    fsm.step(id3, raft::vote_request{term_t{1}, index_t{1}, term_t{1}});

    auto output = fsm.get_output();
    auto reply = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(!reply.vote_granted);

    // Run out of steam for this term. Start a new one.
    fd.alive = false;
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    output = fsm.get_output();
    // Add a favourable reply, not enough for quorum
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_candidate());

    // Add another one, this adds up to quorum
    fsm.step(id3, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());
}

BOOST_AUTO_TEST_CASE(test_log_matching_rule) {

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2, id3});
    raft::log log(raft::snapshot{.idx = index_t{999}, .config = cfg});

    log.emplace_back(raft::log_entry{term_t{10}, index_t{1000}});
    log.stable_to(log.last_idx());

    raft::fsm fsm(id1, term_t{10}, server_id{}, std::move(log), fd, fsm_cfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    (void) fsm.get_output();

    fsm.step(id2, raft::vote_request{term_t{9}, index_t{1001}, term_t{11}});
    // Current term is too old - vote is not granted
    auto output = fsm.get_output();
    BOOST_CHECK(output.messages.empty());

    auto request_vote = [&](term_t term, index_t last_log_idx, term_t last_log_term) -> raft::vote_reply {
        fsm.step(id2, raft::vote_request{term, last_log_idx, last_log_term});
        auto output = fsm.get_output();
        return std::get<raft::vote_reply>(output.messages.back().second);
    };

    // Last stable index is too small - vote is not granted
    BOOST_CHECK(!request_vote(term_t{11}, index_t{999}, term_t{10}).vote_granted);
    // Last stable term is too small - vote is not granted
    BOOST_CHECK(!request_vote(term_t{12}, index_t{1002}, term_t{9}).vote_granted);
    // Last stable term and index are equal to the voter's - vote
    // is granted
    BOOST_CHECK(request_vote(term_t{13}, index_t{1000}, term_t{10}).vote_granted);
    // Last stable term is the same, index is greater to the voter's - vote
    // is granted
    BOOST_CHECK(request_vote(term_t{14}, index_t{1001}, term_t{10}).vote_granted);
    // Both term and index are greater than the voter's - vote
    // is granted
    BOOST_CHECK(request_vote(term_t{15}, index_t{1001}, term_t{11}).vote_granted);
}

BOOST_AUTO_TEST_CASE(test_confchange_add_node) {

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2});
    raft::log log(raft::snapshot{.idx = index_t{100}, .config = cfg});

    raft::fsm fsm(id1, term_t{1}, /* voted for */ server_id{}, std::move(log), fd, fsm_cfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Turn to a leader
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    raft::configuration newcfg({id1, id2, id3});
    // Suggest a confchange.
    fsm.add_entry(newcfg);
    // Can't have two confchanges in progress.
    BOOST_CHECK_THROW(fsm.add_entry(newcfg), raft::conf_change_in_progress);
    // Entered joint configuration immediately.
    BOOST_CHECK(fsm.get_configuration().is_joint());
    BOOST_CHECK(fsm.get_configuration().previous.size() == 2);
    BOOST_CHECK(fsm.get_configuration().current.size() == 3);
    output = fsm.get_output();
    // The output contains a log entry to be committed.
    // Once it's committed, it will be replicated.
    // The output must contain messages both for id2 and id3
    BOOST_CHECK(output.log_entries.size() == 1);
    // Calling get_output() again indicates the previous output
    // is handled, i.e. the log entry is committed, so now
    // the leader will replicate the confchange
    output = fsm.get_output();
    // Ensure both id2 and id3 have an append_entry for them.
    BOOST_CHECK(output.messages.size() == 2);
    auto msg = std::get<raft::append_request_send>(output.messages.back().second);
    auto idx = msg.entries.back().get().idx;
    // In order to accept a configuration change
    // we need one ACK, since there is a quorum overlap.
    // Strictly speaking the new node needs to install a snapshot,
    // first, for simplicity let's assume it's happened already.

    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    // One reply is enough to commit the joint configuration,
    // since there is a quorum overlap between the two
    // configurations.
    BOOST_CHECK(! fsm.get_configuration().is_joint());
    // Still can't have two confchanges in progress, even though
    // we left joint already, the final configuration is not
    // committed yet.
    BOOST_CHECK_THROW(fsm.add_entry(newcfg), raft::conf_change_in_progress);
    output = fsm.get_output();
    // A log entry for the final configuration
    BOOST_CHECK(output.log_entries.size() == 1);
    output = fsm.get_output();
    // AppendEntries messages for the final configuration
    BOOST_CHECK(output.messages.size() >= 1);
    msg = std::get<raft::append_request_send>(output.messages.back().second);
    idx = msg.entries.back().get().idx;
    // Ack AppendEntries for the final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK(fsm.get_configuration().current.size() == 3);
    fsm.step(id3, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    // Check that we can start a new confchange
    raft::configuration newcfg2({id1, id2});
    fsm.add_entry(newcfg);
}

BOOST_AUTO_TEST_CASE(test_confchange_remove_node) {

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2, id3});
    raft::log log(raft::snapshot{.idx = index_t{100}, .config = cfg});

    raft::fsm fsm(id1, term_t{1}, /* voted for */ server_id{}, std::move(log), fd, fsm_cfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Turn to a leader
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    raft::configuration newcfg({id1, id2});
    // Suggest a confchange.
    fsm.add_entry(newcfg);
    // Entered joint configuration immediately.
    BOOST_CHECK(fsm.get_configuration().is_joint());
    BOOST_CHECK(fsm.get_configuration().current.size() == 2);
    BOOST_CHECK(fsm.get_configuration().previous.size() == 3);
    output = fsm.get_output();
    // The output contains a log entry to be committed.
    // Once it's committed, it will be replicated.
    output = fsm.get_output();
    auto msg = std::get<raft::append_request_send>(output.messages.back().second);
    auto idx = msg.entries.back().get().idx;
    // In order to accept a configuration change
    // we need one ACK, since there is a quorum overlap.
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK(! fsm.get_configuration().is_joint());
    output = fsm.get_output();
    // A log entry for the final configuration
    BOOST_CHECK(output.log_entries.size() == 1);
    output = fsm.get_output();
    // AppendEntries messages for the final configuration
    BOOST_CHECK(output.messages.size() >= 1);
    msg = std::get<raft::append_request_send>(output.messages.back().second);
    idx = msg.entries.back().get().idx;
    // Ack AppendEntries for the final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK(fsm.get_configuration().current.size() == 2);
    BOOST_CHECK(!fsm.get_configuration().is_joint());
    // Check that we can start a new confchange
    raft::configuration newcfg2({id1, id2, id3});
    fsm.add_entry(newcfg);
}

BOOST_AUTO_TEST_CASE(test_confchange_replace_node) {

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()},
              id4{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2, id3});
    raft::log log(raft::snapshot{.idx = index_t{100}, .config = cfg});

    raft::fsm fsm(id1, term_t{1}, /* voted for */ server_id{}, std::move(log), fd, fsm_cfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Turn to a leader
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    raft::configuration newcfg({id1, id2, id4});
    // Suggest a confchange.
    fsm.add_entry(newcfg);
    // Entered joint configuration immediately.
    BOOST_CHECK(fsm.get_configuration().is_joint());
    BOOST_CHECK(fsm.get_configuration().current.size() == 3);
    BOOST_CHECK(fsm.get_configuration().previous.size() == 3);
    output = fsm.get_output();
    output = fsm.get_output();
    auto msg = std::get<raft::append_request_send>(output.messages.back().second);
    auto idx = msg.entries.back().get().idx;
    // In order to accept a configuration change
    // we need two ACK, since there is a quorum overlap.
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK(!fsm.get_configuration().is_joint());
    output = fsm.get_output();
    output = fsm.get_output();
    // AppendEntries messages for the final configuration
    BOOST_CHECK(output.messages.size() >= 1);
    msg = std::get<raft::append_request_send>(output.messages.back().second);
    idx = msg.entries.back().get().idx;
    // Ack AppendEntries for the final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK(fsm.get_configuration().current.size() == 3);
    BOOST_CHECK(!fsm.get_configuration().is_joint());
}
