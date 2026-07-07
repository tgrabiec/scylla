# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""
Regression test for the `scylla cache` GDB command against the
single_row_partition storage format.

A table without clustering columns and without static columns stores its
partitions in the row cache using `single_row_partition` rather than the generic
`partition_entry` (see mutation/single_row_partition.hh and db/row_cache.hh).
`row_cache::_partitions` is a std::variant over
double_decker<cache_entry<generic>> and double_decker<cache_entry<single_row>>,
and scylla-gdb.py's `scylla cache` decodes whichever alternative is active.

This test populates the row cache with single_row partitions and checks that
`scylla cache`:
  * completes without error and without a Python traceback, and
  * prints cache entries flagged as single_row
    (cache_entry_base::_flags._single_row_partition), which proves the
    single_row storage variant is actually walked and decoded.
"""

import re
import time
import urllib.request

import pytest

from cassandra import OperationTimedOut
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    NoHostAvailable,
)
from cassandra.policies import WhiteListRoundRobinPolicy

from test.scylla_gdb.conftest import execute_gdb_command

# GDB can only introspect a Scylla built with debug info. Dev builds are
# compiled without debug symbols (configure.py: dev has
# can_have_debug_info=False), so - like the rest of this suite - run only in
# release mode. The aarch64 skip mirrors test_basic_commands.py.
pytestmark = [
    pytest.mark.skip_mode(
        mode=["dev", "debug"],
        reason="Scylla was built without debug symbols; use release mode",
    ),
    pytest.mark.skip_mode(
        mode=["dev", "debug", "release"],
        platform_key="aarch64",
        reason="GDB is broken on aarch64: https://sourceware.org/bugzilla/show_bug.cgi?id=27886",
    ),
]

KEYSPACE = "gdb_cache_test_ks"
TABLE = "single_row_tbl"
# The default test cluster runs with --smp 2, so partitions are spread over two
# shards while `scylla cache` only inspects the shard GDB is attached to. Insert
# enough partitions that the inspected shard is guaranteed to hold several.
NUM_PARTITIONS = 100


def _connect(scylla_server, timeout=90):
    """Open a CQL session, retrying until the node accepts authenticated queries."""
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy([scylla_server.rpc_address])
    )
    cluster = Cluster(
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=[scylla_server.rpc_address],
        protocol_version=4,
        auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"),
    )
    deadline = time.monotonic() + timeout
    while True:
        try:
            return cluster, cluster.connect()
        except (NoHostAvailable, OperationTimedOut):
            if time.monotonic() >= deadline:
                cluster.shutdown()
                raise
            time.sleep(1)


def _flush_keyspace(scylla_server, keyspace):
    """Flush `keyspace` via the REST API so that subsequent reads populate the
    row cache from sstables rather than being served straight from the memtable."""
    url = f"http://{scylla_server.ip_addr}:10000/storage_service/keyspace_flush/{keyspace}"
    request = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(request, timeout=60) as resp:
        assert resp.status == 200, f"keyspace flush failed: HTTP {resp.status}"


def _table_cache_section(stdout, table_name):
    """Extract the block that `scylla cache` prints for `table_name`.

    Output layout (scylla-gdb.py: scylla_cache.invoke):

        <ks>.<cf>:
          (cache_entry*) 0x... {_key=..., _flags=..., _storage=...}
          ...
        <blank line>

    The ks/cf name is printed through the sstring pretty-printer, so the header
    may be quoted (e.g. "gdb_cache_test_ks"."single_row_tbl":). Match the unique
    table name as a substring of a non-indented header line to stay robust to
    quoting, and terminate the block at the blank line the command emits after
    each table.
    """
    lines = stdout.splitlines()
    for i, line in enumerate(lines):
        if not line.startswith(" ") and line.rstrip().endswith(":") and table_name in line:
            block = []
            for entry in lines[i + 1:]:
                if not entry.strip():
                    break
                block.append(entry)
            return "\n".join(block)
    return None


def test_cache_command_walks_single_row_partitions(scylla_server, gdb_cmd):
    fqtn = f"{KEYSPACE}.{TABLE}"

    cluster, session = _connect(scylla_server)
    try:
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
        )
        # No clustering key and no static columns -> single_row_partition storage.
        session.execute(
            f"CREATE TABLE IF NOT EXISTS {fqtn} (pk int PRIMARY KEY, v int)"
        )
        insert = session.prepare(f"INSERT INTO {fqtn} (pk, v) VALUES (?, ?)")
        for pk in range(NUM_PARTITIONS):
            session.execute(insert, (pk, pk))

        _flush_keyspace(scylla_server, KEYSPACE)

        # Point-read every partition so the row cache on the owning shard is
        # populated with a cache_entry for it.
        select = session.prepare(f"SELECT pk, v FROM {fqtn} WHERE pk = ?")
        for pk in range(NUM_PARTITIONS):
            assert session.execute(select, (pk,)).one() is not None
    finally:
        cluster.shutdown()

    result = execute_gdb_command(gdb_cmd, "cache")

    assert result.returncode == 0, (
        f"`scylla cache` exited with {result.returncode}\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    # scylla-gdb.py reports Python errors as a traceback on stderr while the gdb
    # process itself may still exit 0, so check for it explicitly.
    assert "Traceback" not in result.stderr, (
        f"scylla-gdb.py raised while walking the cache:\n{result.stderr}"
    )

    section = _table_cache_section(result.stdout, TABLE)
    assert section is not None, (
        f"table {fqtn} missing from `scylla cache` output:\n{result.stdout}"
    )
    assert "(cache_entry*)" in section, (
        f"no cache entries printed for {fqtn}:\n{section}"
    )
    # Every cache_entry for a clustering-less, static-less table is a single_row
    # entry; finding the flag set proves the single_row double_decker variant was
    # traversed and cache_entry_base::_flags decoded correctly.
    assert re.search(r"_single_row_partition\s*=\s*(?:true|1)\b", section), (
        f"no single_row cache entries found for {fqtn}; the single_row storage "
        f"variant may not have been walked:\n{section}"
    )
