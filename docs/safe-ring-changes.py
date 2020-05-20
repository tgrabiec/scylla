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

"""

class UUID: pass
class Timestamp: pass
class Mutation: pass


#
# Messaging
#

Host = UUID

class RpcMessage: pass

def Call(h: Host, m: RpcMessage):
    """
    Sends a given message to host h for execution.
    Returns when the host received and executed the message.
    Assumes asynchronous, unreliable network.
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
        Call(node, ReplicateTokenMetadata(m))


def get_stage_set_mutation(s: ReplicationStage, timestamp: Timestamp) -> Mutation:
    """Makes a mutation of system.token_metadata which changes the current ReplicationStage
	for all token ranges for all tables.
	"""
    pass


#
# Topology change transaction management
#

class TopologyChangeAction(Enum):
    Add = 1
    Decommission = 2
    Replace = 3


def create_topology_change(action: TopologyChangeAction, targets: List[Host]) -> UUID:
    """Creates a new topology change transaction record in system.topology_changes.
    """
    pass


def get_topology_change_action(id: UUID) -> TopologyChangeAction:
    """Reads intent of the topology change from system.topology_changes"""
    pass


def get_topology_change_targets(id: UUID) -> List[Host]:
    """Reads target nodes from system.topology_changes"""
    pass


def choose_new_tokens(r: TokenMetadata) -> Set[Token]:
    pass


def make_new_ring(id: UUID) -> TokenMetadata:
    """Creates a transitional ring according to the intent of the transaction
    """

    ring = local_ring()
    op = get_topology_change_action(id)
    nodes = get_topology_change_targets(id)
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


def cql(query: str, *args):
    """Executes a CQL query"""
    pass


def try_lock_ring(owner):
    result = cql("update system.global_locks set owner = {} where key = 'ring' if owner is null", owner)
    return owner if result['applied'] else result['owner']


def unlock_ring(owner):
    cql("update system.global_locks set owner = null where key = 'ring' if owner = {}", owner)


def lock_ring(owner):
    while True:
        old = try_lock_ring(owner)
        if old == id:
            break
        time.sleep(10)


def remove_transaction(id: UUID):
    """Removes transaction record from system.topology_changes."""
    pass


def get_all_tables() -> Set[UUID]:
    """
	Returns the set of existing tables.
	This read, as well as writes which modify this set, needs to be performed using linearizable consistency.
	"""
    pass


def stream_data(id: UUID, tables: Set[UUID]):
    """
	When returns, all writes to the tables ACKed prior to this call
	must be replicated to their new replica sets and visible to reads.
	"""
    pass


def stop_streaming(id: UUID):
    """
	Interrupts streaming started by a given topology change.
	Streaming must not be active after this, or at least its effects must not succeed,
	so that it doesn't interfere with cleanup or user reads.
	"""
    pass


def save_intent(id: UUID, participants: Set[Host], token_metadata_mutation: Mutation):
    """Associates given mutation and a set of participants with a given transaction id.
     The association is global and access to it is linearizable."""
    pass


def read_participants(id: UUID) -> Set[Host]:
    """Returns the set of participants associated with the transaction using save_intent()."""
    pass


def read_intent(id: UUID) -> Mutation:
    """Returns token_metadata mutation associated with the transaction using save_intent()."""
    pass


def participants(id: UUID) -> Set[Host]:
    """Returns the set of active participants of the transaction.
    It's not enough to look at the local ring because it may have some participants
    already removed in the middle of some steps, which may need to be replayed.
    """
    return read_participants(id) - dead()


def set_stage(id: UUID, stage: ReplicationStage, t: Timestamp):
    replicate_token_metadata(participants(id), get_stage_set_mutation(stage, t))


#
# Distributed state machine
#


StepName = str

# The step action is passed a timestamp assigned during transition between steps.
# Re-execution of the same step gets the same timestamp.
# The timestamp is strictly monotonic across transitions for a given state
# machine.
# The action is supposed to return the name of the next step to be executed or None for terminal states.
StepAction = Callable[[UUID, Timestamp], StepName]


def run_state_machine(id: UUID,
                      steps: Mapping[StepName, StepAction],
                      get_current_step: Callable[UUID, Tuple[StepAction, Timestamp]],
                      set_current_step: Callable[UUID, StepName, StepName]):
    while True:
        step, t = get_current_step(id)
        new_step = steps[step](id, t)
        if not new_step:
            break
        set_current_step(id, step, new_step)


#
# Normal steps of the topology change state machine
#


def set_step(id: UUID, old_step: StepName, step: StepName):
    result = cql("update system.topology_changes set step = {} where id = {} if step = {}", step, id, old_step)
    if not result['applied']:
        raise Exception('Preempted, another coordinator took over')


def read_step(id: UUID) -> Tuple[StepName, Timestamp]:
    result = cql("select step, timestamp(step) as t from system.topology_changes where id = {}", id)
    if not result:
        raise Exception('Transaction no longer exists')
    return result['step'], result['t']


def step_lock(id: UUID, t: Timestamp):
    lock_ring(id)
    return 'make_ring'


def step_make_ring(id: UUID, t: Timestamp):
    new_ring = make_new_ring(id)
    save_intent(id, new_ring.members(), as_mutation(new_ring, t))
    return 'advertise_ring'


def step_advertise_ring(id: UUID, t: Timestamp):
    replicate_token_metadata(participants(id), read_intent(id))
    return 'before_streaming'


def step_before_streaming(id: UUID, t: Timestamp):
    set_stage(id, ReplicationStage.write_both_read_old, t)
    return 'streaming'


def step_streaming(id: UUID, t: Timestamp):
    # The read of tables has to happen after all nodes are using write_both_read_old
    # so that if there was a table created before write_both_read_old, streaming will
    # see it and replicate its writes.
    # If there is a table created after write_both_read_old, it will be replicated by
    # the means of write_both_read_old.
    tables = get_all_tables()
    stream_data(id, tables)
    return 'after_streaming'


def step_after_streaming(id: UUID, t: Timestamp):
    set_stage(id, ReplicationStage.write_both_read_new, t)
    return 'use_only_new'


def step_use_only_new(id: UUID, t: Timestamp):
    set_stage(id, ReplicationStage.use_only_new, t)
    return 'cleanup'


def step_cleanup(id: UUID, t: Timestamp):
    set_stage(id, ReplicationStage.cleanup, t)
    return 'only_new_ring'


def step_only_new_ring(id: UUID, t: Timestamp):
    replicate_token_metadata(participants(id), as_mutation(get_new_ring(local_ring()), t))
    return 'unlock'


def step_unlock(id: UUID, t: Timestamp):
    unlock_ring(id)
    remove_transaction(id)


#
# Abort steps
#

def step_1a(id: UUID, t: Timestamp):
    set_stage(id, ReplicationStage.write_both_read_old, t)
    return '2a'


def step_2a(id: UUID, t: Timestamp):
    stop_streaming(id)
    return '3a'


def step_3a(id: UUID, t: Timestamp):
    set_stage(id, ReplicationStage.use_only_old, t)
    return '4a'


def step_4a(id: UUID, t: Timestamp):
    set_stage(id, ReplicationStage.cleanup_on_abort, t)
    return '5a'


def step_5a(id: UUID, t: Timestamp):
    replicate_token_metadata(participants(id), as_mutation(get_old_ring(local_ring()), t))
    return 'unlock'


def run_topology_change(id: UUID):
    """
    Advances the state machine of a given transaction until either done, a step fails, or we detect
    that another process took over the state machine and advanced it.
    Can be invoked only on existing member of the cluster.
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
        '1a': step_1a,
        '2a': step_2a,
        '3a': step_3a,
        '4a': step_4a,
        '5a': step_5a,
    }
    run_state_machine(id, steps, read_step, set_step)


def abort_topology_change(id: UUID):
    """Aborts execution of a given topology change in a safe manner.
    Returns when topology change is done."""

    step, t = read_step(id)
    if step in ['unlock',
                'only_new_ring',
                'cleanup',
                'use_only_new']:
        raise Exception('Too late to abort')
    elif step == 'after_streaming':
        set_step(id, step, '1a')
    elif step == 'streaming':
        set_step(id, step, '2a')
    elif step == 'before_streaming':
        set_step(id, step, '4a')
    elif step == 'advertise_ring':
        set_step(id, step, '5a')
    elif step == 'lock' or step == "make_ring":
        set_step(id, step, 'unlock')
    run_topology_change(id)


#
# Nodetool actions.
#


def add_nodes(nodes: Set[Host]):
    assert not current_node() in nodes
    id = create_topology_change(TopologyChangeAction.Add, list(nodes))
    run_topology_change(id)


def decommission_nodes(nodes: Set[Host]):
    id = create_topology_change(TopologyChangeAction.Decommission, list(nodes))
    run_topology_change(id)


class Replace(RpcMessage):
    def __init__(self, old: Host, new: Host):
        self.old = old
        self.new = new

    def execute(self):
        id = create_topology_change(TopologyChangeAction.Replace, [self.old, self.new])
        run_topology_change(id)


def replace_node(old: Host):
    Call(seed(), Replace(old, current_node()))


class Bootstrap(RpcMessage):
    def __init__(self, node: Host):
        self.node = node

    def execute(self):
        id = create_topology_change(TopologyChangeAction.Add, [self.node])
        run_topology_change(id)


def bootstrap():
    """Executed by the bootstrapping node, when bootstrapped the old auto-bootstrap way"""
    Call(seed(), Bootstrap(current_node()))


def resume(id: UUID):
    """Resumes execution of a given topology change"""
    run_topology_change(id)


def abort(id: UUID):
    """Aborts given topology change. Will restore the topology to the initial state."""
    abort_topology_change(id)

