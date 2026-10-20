#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import time

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.mark.skip_mode(mode='release', reason='clocks test API is not available in release mode')
async def test_forward_jump_clocks_carries_to_new_and_restarted_servers(manager: ScyllaClusterManager):
    jump = 3600
    s0 = await manager.server_add()
    await manager.forward_jump_clocks(jump)
    s1 = await manager.server_add()
    await manager.server_restart(s0.server_id)

    cql, hosts = await manager.get_ready_cql([s0, s1])
    for server, host in zip([s0, s1], hosts):
        assert await manager.api.get_clocks_offset(server.ip_addr) == jump
        row = (await cql.run_async("SELECT toUnixTimestamp(now()) AS t FROM system.local", host=host))[0]
        assert row.t / 1000 - time.time() > jump - 60
