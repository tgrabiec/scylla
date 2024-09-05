#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import cassandra
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.cluster import ConsistencyLevel

from test.pylib.manager_client import ManagerClient
from test.pylib.util import ErrorInjectionBarrier
from test.pylib.tablets import get_tablet_replicas, get_tablet_stage
from test.topology.conftest import skip_mode
from test.topology.util import get_topology_coordinator

import pytest
import asyncio
import logging


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.xfail(reason="Monotonic reads are violated during migration")
async def test_monotonic_reads_across_migration(manager: ManagerClient):
    """
    Test that CL=QUORUM reads are monotonic across tablet migration, which means
    that if a read observes a write, all later reads will see that write too.

     1) Tablet replicas: {A, B, C}
     2) Start migrating tablet replica from C to D. New replica set: {A, B, D}
     3) Wait for streaming to finish, but stage is still not in write_both_read_new so that
        reads use the old replica set.
     4) Issue a write which will replicate only to {A, C}, B and D will fail it.
     5) Down node B
     6) Issue a read, which will use {A, C}
     7) Up node B
     8) finish the migration
     9) Down node A
    10) Issue a read, which will use {B, D}, verify it sees the write
    """

    servers = await manager.servers_add(4)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
                        " AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    tablet_token = 777 # Whatever
    tablet_replicas = await get_tablet_replicas(manager, servers[0], 'test', 'test', tablet_token)
    tablet_hosts = set([replica[0] for replica in tablet_replicas])

    host_ids = [await manager.get_host_id(server.server_id) for server in servers]
    target_host = set(host_ids).difference(tablet_hosts).pop()
    leaving_host = tablet_replicas[2][0]
    logger.info(f"hosts: {host_ids}")
    logger.info(f"replicas: {tablet_replicas}")
    logger.info(f"Leaving host: {leaving_host}")
    logger.info(f"Target host: {target_host}")

    leaving_shard = None
    for replica in tablet_replicas:
        if replica[0] == leaving_host:
            leaving_shard = replica[1]
    assert leaving_shard is not None

    coord = await get_topology_coordinator(manager)
    coord_serv = await manager.find_server_by_host_id(coord)

    host_B = None
    for host in host_ids:
        if host in tablet_hosts and host != leaving_host and host != coord:
            host_B = host
            break
    assert host_B is not None

    host_A = None
    for host in host_ids:
        if host in tablet_hosts and host != leaving_host and host != host_B:
            host_A = host
            break
    assert host_A is not None

    srv_A = await manager.find_server_by_host_id(host_A)
    srv_B = await manager.find_server_by_host_id(host_B)
    srv_C = await manager.find_server_by_host_id(leaving_host)
    src_D = await manager.find_server_by_host_id(target_host)

    barrier = ErrorInjectionBarrier(manager, coord_serv, "tablet_transition_updates", one_shot=False)
    await barrier.arm()

    migration = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, "test", "test",
                                                            leaving_host, leaving_shard, target_host, 0, tablet_token))

    while True:
        logger.info("Waiting for stage change")
        await barrier.wait()
        stage = await get_tablet_stage(manager, coord_serv, "test", "test", tablet_token)
        logger.info(f"Stage: {stage}")
        if stage == "streaming":
            break
        if stage is None:
            raise Exception("Tablet migration didn't wait for a messages")
        await barrier.release()

    logger.info(f"Disabling writes on {srv_B.ip_addr} and {src_D.ip_addr}")
    await manager.api.enable_injection(srv_B.ip_addr, "database_fail_user_writes", one_shot=False)
    await manager.api.enable_injection(src_D.ip_addr, "database_fail_user_writes", one_shot=False)

    # Table is empty
    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == 0

    wr_stmt = SimpleStatement("INSERT INTO test.test (pk, c) VALUES (1, 1);", consistency_level=ConsistencyLevel.QUORUM)
    rd_stmt = SimpleStatement("SELECT * FROM test.test;", consistency_level=ConsistencyLevel.QUORUM)

    # Issue a write which will replicate only to {A, C}
    try:
        await cql.run_async(wr_stmt)
        raise Exception("Write succeeded but it should fail")
    except cassandra.WriteFailure:
        pass # Expected

    await manager.api.disable_injection(src_D.ip_addr, "database_fail_user_writes")

    # Make read use {A, C}
    await manager.server_stop_gracefully(srv_B.server_id)

    rows = await cql.run_async(rd_stmt)
    assert len(rows) == 1

    await manager.server_start(srv_B.server_id)

    logger.info("Unblocking migration")
    await barrier.disarm()
    await migration
    # Replica set is now {A, B, D}

    # Make read use {B, D}
    await manager.server_stop_gracefully(srv_A.server_id)

    rows = await cql.run_async(rd_stmt)
    assert len(rows) == 1
