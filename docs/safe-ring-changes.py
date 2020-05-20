from typing import *
from enum import Enum
import time


#
# Schema
#


"""
/* local table */
create table system.token_metadata {
   pk int,
   node UUID,
   token bigint,
   status int,
   replication_stage int static,
   primary key (pk, node, token)
};

/* distributed, LWT-managed table */
create table system.topology_changes {
   id UUID,
   state int,
   action int,
   action_targets list<UUID>,
   participants list<UUID>,
   intent UUID,
   primary key (id)
};

/* distributed, LWT-managed table */
create table system.topology_change_intents {
   intent_id UUID,
   tx_id UUID,
   mutation blob,
   primary key (intent_id)
};

/* distributed, LWT-managed table */
create table system.global_locks {
    name text primary key,
    owner UUID,
    candidate UUID
};

"""

class UUID: pass
class Timestamp: pass
class Mutation: pass


def new_uuid() -> UUID:
    """Generates a new UUID."""
    pass


#
# Messaging
#

Host = UUID


class RpcMessage:
    def execute(self): pass


def send(h: Host, m: RpcMessage):
    """
    Sends a given message to host h for execution.
    Returns when the host received and executed the message.
    May fail even though the message was or will be eventually executed by the host.
    """
    pass


def dead() -> Set[Host]:
    """Returns nodes marked as permanently dead.
    Those nodes are guaranteed to not be currently executing any RpcMessage, nor at any
    later point in time."""


def current_node() -> Host:
    pass


def seed() -> Host:
    """Returns one of the configured seed nodes."""
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


class TokenMetadata:
    """
	Represents a ring, or a transition between two rings

	Is encoded like:
	   Mapping[Host, Mapping[Token, TokenStatus]]
	"""

    def members(self) -> Set[Host]: pass
    def set_stage(self, s: ReplicationStage): pass
    def get_stage(self) -> ReplicationStage: pass
    def set_tokens(self, node: Host, tokens: Set[Token], s: TokenStatus): pass
    def get_tokens(self, node: Host) -> Set[Token]: pass


def get_new_ring(r: TokenMetadata) -> TokenMetadata:
    """
	Given a transitional TokenMetadata, returns the one corresponding to the state before
	the transition.
	All tokens with TokenStatus.PENDING switch to TokenStatus.NORMAL.
	All tokens with TokenStatus.LEAVING are removed.
	get_stage() will return ReplicationStage.use_only_old.
	"""
    pass


def get_old_ring(r: TokenMetadata) -> TokenMetadata:
    """
	Given a transitional TokenMetadata, returns the one corresponding to the state after
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
    pass


class ReplicateTokenMetadata(RpcMessage):
    def __init__(self, m: Mutation):
        pass

    def execute(self):
        """Applies the mutation to local system.token_metadata and waits for the post-conditions to be satisfied"""
        pass


def replicate_token_metadata(nodes: Set[Host], m: Mutation):
    for node in nodes:
        send(node, ReplicateTokenMetadata(m))


def get_stage_set_mutation(s: ReplicationStage, timestamp: Timestamp) -> Mutation:
    """Makes a mutation of system.token_metadata which changes the current ReplicationStage
	for all token ranges for all tables.
	"""
    pass


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


def create_topology_change(action: TopologyChangeAction, targets: List[Host]) -> TransactionId:
    """Creates a new topology change transaction record in system.topology_changes.
    """
    pass


def get_topology_change_action(tx: TransactionId) -> TopologyChangeAction:
    """Reads intent of the topology change from system.topology_changes"""
    pass


def get_topology_change_targets(tx: TransactionId) -> List[Host]:
    """Reads target nodes from system.topology_changes"""
    pass


def choose_new_tokens(r: TokenMetadata) -> Set[Token]:
    pass


def make_new_ring(tx: TransactionId) -> TokenMetadata:
    """Creates a transitional ring according to the intent of the transaction
    """

    ring = local_ring()
    op = get_topology_change_action(tx)
    nodes = get_topology_change_targets(tx)
    if op == TopologyChangeAction.Add:
        for node in nodes:
            tokens = choose_new_tokens(ring)
            ring.set_tokens(node, tokens, TokenStatus.PENDING)
    elif op == TopologyChangeAction.Decommission:
        for node in nodes:
            ring.set_tokens(node, ring.get_tokens(node), TokenStatus.LEAVING)
    elif op == TopologyChangeAction.Replace:
        ...
    return ring


def cql_serial(query: str, *args) -> Mapping[str, object]:
    """Executes a CQL query with SERIAL consistency level"""
    pass

#
# Distributed locking
#


def try_lock(lock_name: str, owner: UUID) -> bool:
    """Acquires a mutually-exclusive lock on the ring if not already locked by someone else.
    If already locked by the one who attempts to lock, does nothing.
    Will not succeed unless prepare_for_locking() was called earlier.
    Returns true if and only if owner has the lock after the call.
    """
    result = cql_serial("update system.global_locks set owner = {} where key = {} if owner is null and candidate = {}",
                        owner, lock_name, owner)
    return result['applied'] or result['owner'] == owner


def prepare_for_locking(lock_name: str, owner: UUID):
    cql_serial("update system.global_locks set candidate = {} where key = {}", owner, lock_name)


def interrupt_lock_attempt(lock_name: str):
    """Invalidates prepare_for_locking().
    Subsequent try_lock_ring() will fail unless prepare_for_locking() is called again.
    """
    cql_serial("update system.global_locks set candidate = null where key = {}", lock_name)


def unlock(lock_name: str, owner: UUID):
    """Unlocks the ring if owner is still the lock owner. Otherwise has no effect.
    """
    cql_serial("update system.global_locks set owner = null where key = {} if owner = {}", lock_name, owner)


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


def save_intent(tx: TransactionId, coid: CoordinatorId, participants: Set[Host], token_metadata_mutation: Mutation):
    """Associates given mutation and a set of participants with a given transaction id if coid is still the coordinator.
     The association is global and access to it is linearizable."""
    pass


def read_participants(tx: TransactionId) -> Set[Host]:
    """Returns the set of participants associated with the transaction using save_intent()."""
    pass


def read_intent(tx: TransactionId) -> Mutation:
    """Returns token_metadata mutation associated with the transaction using save_intent()."""
    pass


def participants(tx: TransactionId) -> Set[Host]:
    """Returns the set of active participants of the transaction.
    It's not enough to look at the local ring because it may have some participants
    already removed in the middle of final steps, which may need to be replayed.
    """
    return read_participants(tx) - dead()


def set_stage(tx: TransactionId, stage: ReplicationStage, t: Timestamp):
    replicate_token_metadata(participants(tx), get_stage_set_mutation(stage, t))


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
                      get_current_step: Callable[TransactionId, Tuple[StepAction, Timestamp]],
                      set_current_step: Callable[TransactionId, CoordinatorId, StepName]):
    while True:
        step, t = get_current_step(txid)
        new_step = steps[step](txid, coid, t)
        if not new_step:
            break
        set_current_step(txid, coid, new_step)


#
# Step definitions for topology change transactions
#


def set_step(tx: TransactionId, coid: CoordinatorId, step: StepName):
    # coordinator_id comparison is needed so that failover() always preempts the previous coordinator.
    # Comparing just the previous step is not enough, since the old coordinator could still win the race
    # and take down the new coordinator.
    result = cql_serial("update system.topology_changes set step = {} where id = {} if coordinator_id = {}", step, tx, coid)
    if not result['applied']:
        raise Exception('Preempted, another coordinator took over')


def read_step(tx: TransactionId) -> Tuple[StepName, Timestamp]:
    result = cql_serial("select step, timestamp(step) as t from system.topology_changes where id = {}", tx)
    if not result:
        raise Exception('Transaction no longer exists')
    return result['step'], result['t']


def step_lock(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    # We need three steps for taking the lock so that locking can be reliably aborted.
    #
    # We have 2 linearizable registers:
    #    - step
    #    - lock, which is a tuple of (owner, candidate)
    #
    # The locking process loops over 3 steps:
    #   1) lock.candidate = id
    #   2) if step != 'lock': break
    #   3) lock.owner = id if lock.owner == null and lock.candidate = id
    #
    # The aborting process executes 3 steps:
    #   1a) step = 'abort'
    #   2a) lock.candidate = null
    #   3a) lock.owner = null if lock.owner == id
    #
    # We want to prove that when the abort sequence is done, the lock will not be held by this transaction.
    # After 2a is executed, the locking process will not acquire the lock unless it already managed to do so.
    # That's because:
    #   - if the locking process is before step 2 or after step 3, it will exit in step 2, because of 1a
    #   - if it's before step 3, the lock will fail because lock.candidate is nulled.
    #
    # If the locking process acquired the lock before 2a, 3a will release it.
    #
    while True:
        prepare_for_locking('ring', tx)
        if not read_step(tx)[0] == 'lock':
            raise Exception('Preempted')
        if try_lock('ring', tx):
            break
        time.sleep(10)
    return 'make_ring'


def step_make_ring(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    new_ring = make_new_ring(tx)
    save_intent(tx, coid, new_ring.members(), as_mutation(new_ring, t))
    return 'advertise_ring'


def step_advertise_ring(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    replicate_token_metadata(participants(tx), read_intent(tx))
    return 'before_streaming'


def step_before_streaming(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
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
    set_stage(tx, ReplicationStage.write_both_read_new, t)
    return 'use_only_new'


def step_use_only_new(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    set_stage(tx, ReplicationStage.use_only_new, t)
    return 'cleanup'


def step_cleanup(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    set_stage(tx, ReplicationStage.cleanup, t)
    return 'only_new_ring'


def step_only_new_ring(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    replicate_token_metadata(participants(tx), as_mutation(get_new_ring(local_ring()), t))
    return 'unlock'


def step_unlock(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    unlock('ring', tx)
    remove_transaction(tx)


def step_abort_lock(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    interrupt_lock_attempt('ring')
    return 'unlock'


#
# Abort steps
#

def step_1a(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    set_stage(tx, ReplicationStage.write_both_read_old, t)
    return '2a'


def step_2a(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    stop_streaming(tx)
    return '3a'


def step_3a(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    set_stage(tx, ReplicationStage.use_only_old, t)
    return '4a'


def step_4a(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    set_stage(tx, ReplicationStage.cleanup_on_abort, t)
    return '5a'


def step_5a(tx: TransactionId, coid: CoordinatorId, t: Timestamp):
    replicate_token_metadata(participants(tx), as_mutation(get_old_ring(local_ring()), t))
    return 'unlock'


#
# Transaction execution
#


def failover(tx: TransactionId) -> CoordinatorId:
    """Starts a new coordinator for the transaction on current node.
    The previous coordinator will be preempted and will eventually stop.
    Call run_topology_change() to actually start executing.
    """
    coordinator_id = new_uuid()
    cql_serial('update system.topology_changes set coordinator = {}, coordinator_host = {} where id = {}',
               coordinator_id, current_node(), tx)
    # TODO: Interrupt existing coordinator using RPC in the background as an optimization.
    return coordinator_id


def run_topology_change(tx: TransactionId, coid: CoordinatorId = None):
    """
    Takes over the task of advancing the state machine of the transaction.
    Can be invoked only on an existing member of the cluster.
    """
    steps = {
        'lock': step_lock,
        'make_ring': step_make_ring,
        'advertise_ring': step_advertise_ring,
        'before_streaming': step_before_streaming,
        'streaming': step_streaming,
        'after_streaming': step_after_streaming,
        'use_only_new': step_use_only_new,
        'cleanup': step_cleanup,
        'only_new_ring': step_only_new_ring,
        'unlock': step_unlock,
        'abort_lock': step_abort_lock,
        '1a': step_1a,
        '2a': step_2a,
        '3a': step_3a,
        '4a': step_4a,
        '5a': step_5a,
    }
    if not coid:
        coid = failover(tx)
    run_state_machine(tx, coid, steps, read_step, set_step)


def abort_topology_change(tx: TransactionId):
    """Aborts execution of a given topology change in a safe manner.
    Returns when topology change is undone."""
    coid = failover(tx)
    step, t = read_step(tx)
    if step == 'after_streaming':
        set_step(tx, coid, '1a')
    elif step == 'streaming':
        set_step(tx, coid, '2a')
    elif step == 'before_streaming':
        set_step(tx, coid, '4a')
    elif step == 'advertise_ring':
        set_step(tx, coid, '5a')
    elif step == "make_ring":
        set_step(tx, coid, 'unlock')
    elif step == "lock":
        set_step(tx, coid, 'abort_lock')
    else:
        raise Exception('Impossible to abort at this stage')
    run_topology_change(tx, coid)


#
# Nodetool actions.
#


def add_nodes(nodes: Set[Host]):
    assert not current_node() in nodes
    tx = create_topology_change(TopologyChangeAction.Add, list(nodes))
    run_topology_change(tx)


def decommission_nodes(nodes: Set[Host]):
    tx = create_topology_change(TopologyChangeAction.Decommission, list(nodes))
    run_topology_change(tx)


class Replace(RpcMessage):
    def __init__(self, old: Host, new: Host):
        self.old = old
        self.new = new

    def execute(self):
        tx = create_topology_change(TopologyChangeAction.Replace, [self.old, self.new])
        run_topology_change(tx)


def replace_node(old: Host):
    send(seed(), Replace(old, current_node()))


class Bootstrap(RpcMessage):
    def __init__(self, node: Host):
        self.node = node

    def execute(self):
        tx = create_topology_change(TopologyChangeAction.Add, [self.node])
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

