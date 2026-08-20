# Fixture sketch. The real implementation lives in the scylla_test extension
# module: it boots seastar on background threads once per session and
# constructs a fresh cql_test_env per test (~1s in dev mode).

import pytest

import scylla_test


@pytest.fixture(scope="session")
def reactor():
    with scylla_test.reactor(smp=2, memory="2G") as r:
        yield r


@pytest.fixture
def env(reactor):
    with reactor.cql_test_env() as e:
        yield e
