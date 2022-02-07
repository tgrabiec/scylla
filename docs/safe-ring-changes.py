#!/usr/bin/env python3
import queue
import random
import threading
import uuid
from uuid import UUID
from cassandra.cluster import Cluster
from typing import *
from enum import Enum
import time

#
# Schema
#

"""

/* All tables are local tables managed by raft_group0 */

create table system.token_metadata (
   pk int,
   
   // Reflects what is currently stored in gossip protocol application states STATUS and TOKENS
   // Depends on 
   node UUID,
   "token" bigint,
   status int,
   
   // We have a single replication_stage per whole ring at the moment
   // which controls all token ranges for all tables.
   // We could make it more fine grained in the future.  
   replication_stage int static,

   primary key (pk, node, "token")
);

create table system.topology_changes (
   id UUID,
   
   // coordinator_id is set when there is an active coordinator for the topology change
   // which drives it forward.
   // We need to track it to implement failover. Old coordinators will stop when
   // they observe they are no longer set as active here.
   // Whenever the state of transaction is changed by the coordinator it should
   // atomically check if it's still active by consulting this column.
   coordinator_id UUID,

   // Used for routing messages to the active coordinator (e.g. cancel it).
   // Set when coordinator_id is set.
   coordinator_host UUID,

   state int,
   action int,
   failed bool,
   result text,
   primary key (id)
);

create table system.global_locks (
    name text primary key,
    owner UUID,
    candidate UUID
);

"""


class Timestamp:
    def __init__(self, t):
        if t is str:
            self.t = int(t)
        else:
            self.t = t


class Mutation:
    def apply(self):
        """Apply on local node"""
        pass


def new_uuid() -> UUID:
    """Generates a new UUID."""
    return uuid.uuid4()


#
# Messaging
#

Host = UUID


class RpcMessage:
    def execute(self): pass


this_node = threading.local()


class HostThread(threading.Thread):
    def __init__(self, host_id):
        super().__init__()
        self.host_id = host_id
        self.queue = queue.Queue()

    def run(self):
        this_node.rafts = dict()
        this_node.raft_gr0 = RaftGroup(0)
        this_node.rafts[0] = this_node.raft_gr0

        this_node.host_id = self.host_id
        this_node.dead_nodes = set()
        while True:
            func = self.queue.get()
            func()


host_threads: MutableMapping[Host, HostThread] = dict()


class WaitableFunc(object):
    """Allows waiting for func to complete"""

    def __init__(self, func):
        self.queue = queue.Queue()
        self.func = func

    def __call__(self):
        self.queue.put(self.func())

    def get(self):
        return self.queue.get()


def run_on_host(h: Host, func: Callable):
    if h not in host_threads:
        t = HostThread(h)
        t.start()
        host_threads[h] = t
    host_threads[h].queue.put(func)


def run_on_host_sync(h: Host, func: Callable):
    cmd = WaitableFunc(func)
    run_on_host(h, cmd)
    return cmd.get()


def send(h: Host, m: RpcMessage):
    """
    Sends a given message to host h for execution.
    Returns when the host received and executed the message.
    May fail even though the message was or will be eventually executed by the host.
    """
    run_on_host_sync(h, lambda: m.execute())


def current_node() -> Host:
    return this_node.host_id


def set_seed(h: Host):
    print('[%s]: set_seed(%s)' % (current_node(), h))
    this_node.seed = h


def seed() -> Host:
    """Returns one of the configured seed nodes."""
    return this_node.seed


#
# Raft
#

RaftCommand = RpcMessage


class RaftCommandFromLambda(RaftCommand):
    def __init__(self, fun):
        self.fun = fun

    def execute(self):
        self.fun()


class RaftTransaction(object):
    """
    execute() can access local state machine and return its modifications via RaftCommand.
    The transaction will execute (in terms of reads and writes of the state machine)
    atomically and will be linearized with other transactions in the same raft group
    when executed via RaftGroup.execute_linearized().
    """
    def execute(self, t: Timestamp) -> List[RaftCommand]:
        pass


class RaftTransactionFromLambda(RaftTransaction):
    def __init__(self, fun):
        self.fun = fun

    def execute(self, t) -> List[RaftCommand]:
        return self.fun(t)


RaftGroupId = int

now = 0


def new_time():
    global now
    now += 1
    return Timestamp(now)


class RaftGroup(object):
    """Mock implementation. No leader failover"""

    def __init__(self, gr_id: RaftGroupId):
        self.id = gr_id
        self.log = list()
        self.nodes: List[Host] = list()
        self.leader: Host = None

    def execute_linearized(self, t: RaftTransaction):
        """
        Atomically executes a given transaction assuming that all side effects of the transaction
        are in the form of the commands returned by t.execute().
        The transaction observes the state machine state at the point in time in the history
        when its result is going to be placed. Can be used to implement atomic CAS.
        """
        return run_on_host_sync(self.leader, lambda:
            (this_node.rafts[self.id].add(c) for c in t.execute(new_time())))

    def do_add_node(self, h: Host):
        self.nodes.append(h)
        if not self.leader:
            self.leader = h
        for m in self.log:
            run_on_host(h, lambda: m.execute())

    def add_node(self, n):
        self.execute_linearized(RaftTransactionFromLambda(lambda:
            [RaftCommandFromLambda(lambda: (self.do_add_node(n)))]
        ))

    def do_remove_node(self, n):
        self.nodes.remove(n)
        if self.leader == n:
            self.leader = self.nodes[0]

    def remove_node(self, n):
        self.execute_linearized(RaftTransactionFromLambda(lambda:
            [RaftCommandFromLambda(lambda: (self.do_remove_node(n)))]
        ))

    def add(self, m: RpcMessage):
        assert current_node() == self.leader
        self.log.append(m)
        for h in self.nodes:
            run_on_host_sync(h, lambda: m.execute())

    def global_read_barrier(self):
        """
        Waits for all nodes to apply commands committed prior to this call
        """
        pass

    def read_barrier(self):
        """
        Waits for current node to apply commands committed prior to this call
        """
        pass

#
# Token metadata
#


class TokenStatus(Enum):
    NORMAL = 'N'
    LEAVING = 'L'
    PENDING = 'P'


class ReplicationStage(Enum):
    use_only_old = 1,
    write_both_read_old = 2,
    write_both_read_new = 3,
    use_only_new = 4,
    cleanup = 5,
    cleanup_on_abort = 6


Token = int
min_token = 0
max_token = 100


class TokenMetadata:
    """
	Represents a ring, or a transition between two rings
	"""

    def __init__(self):
        self.tokens: MutableMapping[Host, MutableMapping[Token, TokenStatus]] = dict()
        self.replication_stage: ReplicationStage = ReplicationStage.use_only_old

    def members(self) -> Set[Host]:
        """Returns present members as well as those which are in transition"""
        return set(self.tokens.keys())

    def leaving_members(self) -> Set[Host]:
        """Returns nodes present in the old ring but not in the new ring"""
        return set(h for h, tokens in self.tokens if any(s == TokenStatus.LEAVING for t, s in tokens))

    def joining_members(self) -> Set[Host]:
        """Returns nodes absent in the old ring but present in the new ring"""
        return set(h for h, tokens in self.tokens if any(s == TokenStatus.PENDING for t, s in tokens))

    def members_in_new_ring(self) -> Set[Host]:
        """Returns nodes present in the new ring"""
        return set(h for h, tokens in self.tokens if any(s != TokenStatus.LEAVING for t, s in tokens))

    def set_stage(self, s: ReplicationStage):
        self.replication_stage = s

    def get_stage(self) -> ReplicationStage:
        return self.replication_stage

    def set_tokens(self, node: Host, tokens: Set[Token], s: TokenStatus):
        if node not in self.tokens:
            self.tokens[node] = dict()
        for t in tokens:
            self.tokens[node][t] = s

    def get_tokens(self, node: Host) -> Set[Token]:
        return set(self.tokens[node].keys())


def get_new_ring(r: TokenMetadata) -> TokenMetadata:
    """
	Given a transitional TokenMetadata, returns the one corresponding to the state after
	the transition.
	All tokens with TokenStatus.PENDING switch to TokenStatus.NORMAL.
	All tokens with TokenStatus.LEAVING are removed.
	get_stage() will return ReplicationStage.use_only_old.
	"""
    pass


def get_old_ring(r: TokenMetadata) -> TokenMetadata:
    """
	Given a transitional TokenMetadata, returns the one corresponding to the state before
	the transition.
	All tokens with TokenStatus.LEAVING switch to TokenStatus.NORMAL.
	All tokens with TokenStatus.PENDING are removed.
	get_stage() will return ReplicationStage.use_only_old.
	"""
    pass


def as_mutation(r: TokenMetadata, timestamp: Timestamp) -> Mutation:
    """
	Returns a mutation of system.token_metadata which will make it reflect a given TokenMetadata.

	For any two TokenMetadata objects r1 and r2 and t1 > t0, it holds that:

	    as_mutation(r1, t1) + as_mutation(r2, t0) = as_mutation(r1, t1)

	"""
    pass


def local_ring() -> TokenMetadata:
    """Reads local node's TokenMetadata from system.token_metadata"""
    return this_node.ring


def get_dead_nodes() -> Set[Host]:
    """Returns nodes marked as permanently dead.
    Messages received from dead nodes will be ignored.
    """
    return this_node.dead_nodes


def add_to_dead(nodes: Set[Host]):
    this_node.dead_nodes.extend(nodes)


class ReplicateTokenMetadata(RpcMessage):
    def __init__(self, m: Mutation):
        pass

    def execute(self):
        """Applies the mutation to local system.token_metadata and waits for the post-conditions to be satisfied"""
        pass


def replicate_token_metadata(m: Mutation):
    # TODO: check coordinator_id as part of the transaction so that we don't rely on the timestamp
    # trick to make this command noop on failover
    this_node.raft_gr0.add(TokenMetadataUpdateCommand(m))
    this_node.raft_gr0.global_read_barrier()


def get_stage_set_mutation(s: ReplicationStage, timestamp: Timestamp) -> Mutation:
    """Makes a mutation of system.token_metadata which changes the current ReplicationStage
	for all token ranges for all tables.
	"""
    t = TokenMetadata()
    t.set_stage(s)
    return as_mutation(t, timestamp)


#
# Topology change transaction management
#

TransactionId = UUID

# Transaction coordinator identifier.
# The coordinator's job is to advance the state machine of the transaction.
CoordinatorId = UUID


class TopologyChangeAction(Enum):
    Add = 1
    Decommission = 2
    Replace = 3


class CqlCommand(RaftCommand):
    def __init__(self, cql: str, *args):
        self.cql = cql
        self.args = args

    def execute(self):
        cql_local(self.cql, *self.args)


class MutationCommand(RaftCommand):
    """Applies a given mutation on current node"""
    def __init__(self, m: Mutation):
        self.m = m

    def execute(self):
        self.m.apply()


class TokenMetadataUpdateCommand(MutationCommand):
    """Updated token metadata and waits for local coordinator to synchronize with it"""

    def __init__(self, m: Mutation):
        super().__init__(m)

    def wait_for_sync(self):
        """Wait until all requests start using the new topology"""

    def execute(self):
        super().execute()
        self.wait_for_sync()


def make_create_topology_change_command(tx: TransactionId, action: TopologyChangeAction) -> RaftCommand:
    """Returns a command which creates a new topology change transaction record in system.topology_changes.
    """
    return CqlCommand("insert into system.topology_changes (id, state, action, failed)"
                      " values ({}, 'update_raft_group', {}, [{}], false)", tx, action)


def get_topology_change_action(tx: TransactionId) -> TopologyChangeAction:
    """Reads intent of the topology change from system.topology_changes"""
    pass


def get_topology_change_targets(tx: TransactionId) -> List[Host]:
    """Reads target nodes from system.topology_changes"""
    pass


def choose_new_tokens(r: TokenMetadata) -> Set[Token]:
    return {random.randint(min_token, max_token)}


def cql_local(query: str, *args) -> Mapping[str, object]:
    """Executes a CQL query with CL=1 on local node"""
    pass


#
# Topology change transaction state
#


def remove_transaction(tx: TransactionId):
    """Removes transaction record from system.topology_changes."""
    pass


def stream_data(tx: TransactionId, tables: Set[UUID]):
    """
	When returns, all writes to the tables ACKed prior to this call
	must be replicated to their new replica sets and visible to reads.
	"""
    pass


def stop_streaming(tx: TransactionId):
    """
	Interrupts streaming started by a given topology change.
	Streaming must not be active after this, or at least its effects must not succeed,
	so that it doesn't interfere with cleanup or user reads.
	"""
    pass


def set_stage(tx: TransactionId, stage: ReplicationStage, t: Timestamp):
    replicate_token_metadata(get_stage_set_mutation(stage, t))


#
# Distributed state machine
#


StepName = str

# The step action is passed a timestamp assigned during transition between steps.
# Re-execution of the same step gets the same timestamp.
# The timestamp is strictly monotonic across transitions for a given state
# machine.
# The action is supposed to return the name of the next step to be executed or None for terminal states.
StepAction = Callable[[TransactionId, CoordinatorId, Timestamp], StepName]


def run_state_machine(txid: TransactionId,
                      coid: CoordinatorId,
                      steps: Mapping[StepName, StepAction],
                      get_current_step: Callable[[TransactionId, CoordinatorId], Tuple[StepName, Timestamp]],
                      set_current_step: Callable[[TransactionId, CoordinatorId, StepName], None]):
    while True:
        step, t = get_current_step(txid, coid)
        new_step = steps[step](txid, coid, t)
        if not new_step:
            break
        set_current_step(txid, coid, new_step)


#
# Step definitions for topology change transactions
#

class SetStep(RaftTransaction):
    def __init__(self, tx: TransactionId, coid: CoordinatorId, step: StepName):
        self.coid = coid
        self.step = step
        self.tx = tx

    def execute(self, t: Timestamp) -> List[RaftCommand]:
        # coordinator_id comparison is needed so that failover() always preempts the previous coordinator.
        # Comparing just the previous step is not enough, since the old coordinator could still win the race
        # and take down the new coordinator.
        result = cql_local("select coordinator_id from system.topology_changes where id = {}", self.tx)
        if result['coordinator_id'] != self.coid:
            raise Exception('Preempted, another coordinator took over')
        print("[%s]: SET step=%s, tx=%s, coid=%s" % (current_node(), self.step, self.tx, self.coid))
        return [CqlCommand("update system.topology_changes set step = {} where id = {}", self.step, self.tx)]


def set_step(tx: TransactionId, coid: CoordinatorId, step: StepName):
    this_node.raft_gr0.execute_linearized(SetStep(tx, coid, step))


def read_step(tx: TransactionId, coid: CoordinatorId) -> Tuple[StepName, Timestamp]:
    this_node.raft_gr0.read_barrier()
    result = cql_local("select coordinator_id, step, timestamp(step) as t from system.topology_changes where id = {}", tx)
    if not result:
        raise Exception('Transaction no longer exists')
    if result['coordinator_id'] != coid:
        raise Exception('Coordinator preempted')
    print("[%s]: step=%s, tx=%s, coid=%s" % (current_node(), result['step'], tx, coid))
    return StepName(result['step']), Timestamp(result['t'])


def step_update_raft_group(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    for n in local_ring().joining_members():
        this_node.raft_gr0.add_node(n)
    return 'advertise_ring'


def step_advertise_ring(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    set_stage(tx, ReplicationStage.write_both_read_old, t)
    return 'streaming'


def get_all_tables() -> Set[UUID]:
    """
    Returns the set of existing tables.
    This read, as well as writes which modify this set, needs to be performed using linearizable consistency
    So that streaming doesn't miss any tables which may have already received writes.
    """
    pass


def step_streaming(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    # The read of tables has to happen after all nodes are using write_both_read_old
    # so that if there was a table created before write_both_read_old, streaming will
    # see it and replicate its writes.
    # If there is a table created after write_both_read_old, it will be replicated by
    # the means of write_both_read_old.
    tables = get_all_tables()
    stream_data(tx, tables)
    return 'after_streaming'


def step_after_streaming(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    # XXX: We don't have to check if coid is still active because set_stage() uses t
    # in its commands and will not have any effect if we were preempted and another
    # coordinator took over either by replaying the step or aborting it.
    set_stage(tx, ReplicationStage.write_both_read_new, t)
    return 'use_only_new'


def step_use_only_new(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    set_stage(tx, ReplicationStage.use_only_new, t)
    return 'mark_dead'


class RunCleanup(RpcMessage):
    def execute(self):
        """Runs nodetool cleanup on current node"""
        pass


def step_cleanup(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    for n in local_ring().members_in_new_ring():
        send(n, RunCleanup())
    return 'only_new_ring'


class MarkDead(RaftCommand):
    def __init__(self, nodes):
        self.nodes = nodes

    def execute(self):
        add_to_dead(self.nodes)


def step_mark_dead(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    for n in local_ring().leaving_members():
        this_node.raft_gr0.remove_node(n)

    # We must not let decommissioned nodes issue calls to the cluster because
    # they will not receive topology updates after they are removed from
    # membership and otherwise could corrupt global registers by using stale topology.
    # This must be done before next changes are made, so before unlocking the ring.

    this_node.raft_gr0.execute_linearized(RaftTransactionFromLambda(lambda:
        [MarkDead(local_ring().leaving_members())]))
    this_node.raft_gr0.global_read_barrier()
    return 'cleanup'


def step_only_new_ring(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    replicate_token_metadata(as_mutation(get_new_ring(local_ring()), t))
    return 'unlock'


def step_unlock(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    this_node.raft_gr0.add(UnlockRingTransaction())
    return 'done'


def step_done(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    # Delete the transaction here, or keep the record for audit
    return None


#
# Abort steps
#


def abort_after_streaming(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    set_stage(tx, ReplicationStage.write_both_read_old, t)
    return 'abort_streaming'


def abort_streaming(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    stop_streaming(tx)
    return 'abort_advertise_ring'


def abort_advertise_ring(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    replicate_token_metadata(as_mutation(get_old_ring(local_ring()), t))
    return 'abort_update_raft_group'


def abort_update_raft_group(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    for n in local_ring().joining_members():
        this_node.raft_gr0.remove_node(n)
    return 'unlock'


topology_change_state_machine = {
    'update_raft_group': step_update_raft_group,
    'advertise_ring': step_advertise_ring,
    'streaming': step_streaming,
    'after_streaming': step_after_streaming,
    'use_only_new': step_use_only_new,
    'mark_dead': step_mark_dead,
    'cleanup': step_cleanup,
    'only_new_ring': step_only_new_ring,
    'unlock': step_unlock,
    'done': step_done,

    # Abort steps
    'abort_after_streaming': abort_after_streaming,
    'abort_streaming': abort_streaming,
    'abort_advertise_ring': abort_advertise_ring,
    'abort_update_raft_group': abort_update_raft_group,
}


#
# Transaction execution
#


def failover(tx: TransactionId) -> CoordinatorId:
    """Starts a new coordinator for the transaction on current node.
    The previous coordinator will be preempted and will eventually stop.
    Call run_topology_change() to actually start executing.
    """

    coordinator_id = new_uuid()
    print("[%s]: Failover: tx=%s, coid=%s" % (current_node(), tx, coordinator_id))

    this_node.raft_gr0.execute_linearized(RaftTransactionFromLambda(lambda:
        [CqlCommand('update system.topology_changes set coordinator_id = {}, coordinator_host = {} where id = {}',
                    coordinator_id, current_node(), tx)]))

    # TODO: Interrupt existing coordinator (old coordinator_host) using RPC in the background as an optimization.
    return coordinator_id


def run_topology_change(tx: TransactionId, coid: CoordinatorId = None):
    """Takes over the task of advancing the state machine of the transaction.
    Can be invoked only on an existing member of the cluster.
    """

    if not coid:
        coid = failover(tx)
    run_state_machine(tx, coid, topology_change_state_machine, read_step, set_step)


def abort_topology_change(tx: TransactionId):
    """Aborts execution of a given topology change in a safe manner.
    Returns when topology change is undone."""

    abort_steps = {
        'after_streaming': 'abort_after_streaming',
        'streaming': 'abort_streaming',
        'advertise_ring': 'abort_advertise_ring',
    }

    coid = failover(tx)
    step, t = read_step(tx, coid)
    if step not in abort_steps:
        raise Exception('Impossible to abort at this stage, pausing')
    set_step(tx, coid, abort_steps[step])
    run_topology_change(tx, coid)


#
# Nodetool actions.
#

class LockRingTransaction(RaftTransaction):
    """
    There is a single global lock for the ring.
    This transaction takes this lock or throws if already taken.
    """
    def __init__(self, owner: str):
        self.owner = owner

    def execute(self, t: Timestamp) -> List[RaftCommand]:
        lock_name = "ring"
        result = cql_local("select owner from system.global_locks where key = {}", lock_name)
        if "owner" in result["owner"] and result["owner"]:
            raise Exception("Ring already locked by transaction %s" % (result["owner"]))
        return [CqlCommand("update system.global_locks set owner = {} where key = {}", self.owner, lock_name)]


class UnlockRingTransaction(RaftTransaction):
    def execute(self, t: Timestamp) -> List[RaftCommand]:
        lock_name = "ring"
        return [CqlCommand("update system.global_locks set owner = null where key = {}", lock_name)]


class BootstrapTransaction(RaftTransaction):
    """
    Creates a new topology change transaction which adds a single node to the cluster
    """
    def __init__(self, tx: TransactionId, n: Host):
        self.tx = tx
        self.n = n

    def execute(self, t: Timestamp) -> List[RaftCommand]:
        lock = LockRingTransaction(str(self.tx))
        lock_cmd = lock.execute(t)

        if self.n in local_ring().members():
            raise Exception("Node is already a member")

        tokens = choose_new_tokens(local_ring())
        ring_diff = TokenMetadata()
        ring_diff.set_tokens(self.n, tokens, TokenStatus.PENDING)

        # use_only_old because the new nodes are not added to raft_gr0 yet.
        ring_diff.set_stage(ReplicationStage.use_only_old)

        return [
            TokenMetadataUpdateCommand(as_mutation(ring_diff, t)),
            make_create_topology_change_command(self.tx, TopologyChangeAction.Add)
        ] + lock_cmd


class Bootstrap(RpcMessage):
    def __init__(self, node: Host):
        self.node = node

    def execute(self):
        print("[%s]: Bootstrap %s" % (current_node(), self.node))
        tx = new_uuid()
        this_node.raft_gr0.execute_linearized(BootstrapTransaction(tx, self.node))
        run_topology_change(tx)


def bootstrap():
    """Executed by the bootstrapping node, when bootstrapped the old auto-bootstrap way"""
    send(seed(), Bootstrap(current_node()))


def resume(tx: TransactionId):
    """Resumes execution of a given topology change here."""
    run_topology_change(tx)


def abort(tx: TransactionId):
    """Reverts the topology change."""
    abort_topology_change(tx)


if __name__ == "__main__":
    node1 = new_uuid()
    node2 = new_uuid()
    node3 = new_uuid()

    run_on_host(node2, lambda: (
        set_seed(node1),
        bootstrap()
    ))
