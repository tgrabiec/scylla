# Python rendition of test/boost/tablets_test.cc::test_load_balancing_with_empty_node.
#
# Runs against an in-process cql_test_env via the scylla_test bindings.
# No node boot, no cluster manager — the same granularity as the boost
# test, with pytest ergonomics. Run with:
#
#   tools/toolchain/dbuild build/dev/test/pyunit/runner test/pyunit -v
#
# The `env` fixture (see conftest.py) constructs cql_test_env on a background
# seastar reactor. Calls below block the Python thread until the underlying
# future resolves; the GIL is released while waiting.

from scylla_test import TabletMap


def test_load_balancing_with_empty_node(env):
    # Bootstrapping a single node: the load balancer sees it and
    # moves tablets to it.
    topo = env.topology_builder()
    host1 = topo.add_node(shards=2)
    host2 = topo.add_node(shards=2)
    host3 = topo.add_node(shards=2)

    ks = env.add_keyspace(rf={topo.dc: 1}, initial_tablets=4)
    table1 = env.add_table(ks)

    # Group0-guarded tablet metadata mutation; commits on scope exit.
    with env.update_tablet_metadata() as tmeta:
        tmap = TabletMap(tablet_count=4)
        tmap[0] = [(host1, 0), (host2, 1)]
        tmap[1] = [(host1, 0), (host2, 1)]
        tmap[2] = [(host1, 0), (host2, 0)]
        tmap[3] = [(host1, 1), (host2, 0)]
        tmeta[table1] = tmap

    load = env.load_sketch()
    assert load.tablet_count(host1) == 4
    assert load.avg_tablet_count(host1) == 2
    assert load.tablet_count(host2) == 4
    assert load.avg_tablet_count(host2) == 2
    assert load.tablet_count(host3) == 0
    assert load.avg_tablet_count(host3) == 0

    topo.load_stats.set_default_tablet_sizes()
    env.rebalance_tablets(topo.load_stats)

    load = env.load_sketch()
    for host in (host1, host2, host3):
        assert 1 < load.tablet_count(host) <= 3
        assert 0 < load.avg_tablet_count(host) <= 2
