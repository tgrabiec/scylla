#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient
import pytest


@pytest.mark.asyncio
async def test_basic_operations(manager: ManagerClient):
    """Test that you can create a table and insert and query data"""

    servers = await manager.running_servers()

    manager.cql.execute("CREATE KEYSPACE test WITH replication = {'class': 'TabletReplicationStrategy'};")
    manager.cql.execute("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")
    manager.cql.execute("INSERT INTO test.test (pk, c) VALUES (1, 1);")
    manager.cql.execute("SELECT * FROM test.test;")

    # Restart

    # Reshard
