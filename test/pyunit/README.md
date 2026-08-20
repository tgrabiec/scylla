# pyunit — boost-granularity tests in Python (proposal demo)

`test_tablets.py` is `tablets_test.cc::test_load_balancing_with_empty_node`
converted 1:1. Nothing here runs yet — the point is to show what the test
author writes once a `scylla_test` pybind11 module exists.

## What the conversion buys

- **No relink per test edit.** Editing a boost test costs a combined_tests
  relink (minutes). Editing the Python file costs nothing; the extension
  module relinks only when the binding surface changes.
- **Less text, same test.** The scenario body shrinks ~80 → ~35 lines:
  replica sets are tuples, tablet metadata mutation is a `with` block,
  assertions are chained comparisons. The scenario reads at spec level.
- **pytest ergonomics.** Parametrization, -k selection, fixtures, plain
  asserts with introspected failure output, REPL-driven exploration of a
  live env.
- **Same coverage.** In-process cql_test_env, real load balancer, real
  tablet metadata — not a mock of the balancer, the balancer.

## How it runs (design)

The `scylla_test` extension module links the same objects as combined_tests.
It boots seastar on background threads once per pytest session
(`scylla_test.reactor()`), then constructs a cql_test_env per test. Python
calls cross into the reactor via `seastar::alien::submit_to()` and block
(GIL released) until the future resolves. Scylla objects stay reactor-side;
Python holds opaque handles.

`scylla_test.pyi` is the contract: exactly the surface this one test needs.
Each further converted test grows it a little; most later tests need no
new bindings.
