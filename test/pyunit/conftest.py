# The scylla_test module is embedded in the pyunit runner
# (test/pyunit/runner.cc), which hosts the Python interpreter and runs
# seastar on background threads. Not importable under a plain python3.

import pytest

import scylla_test


@pytest.fixture
def env():
    e = scylla_test.start_env()
    try:
        yield e
    finally:
        scylla_test.stop_env()
