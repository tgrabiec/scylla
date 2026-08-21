# pyunit — boost-granularity tests in Python

`test_tablets.py` is `tablets_test.cc::test_load_balancing_with_empty_node`
converted 1:1, running against an in-process `cql_test_env` — the real
load balancer and tablet metadata, not a mock, and no node boot.

## Build and run

```
tools/toolchain/dbuild ninja build/dev/test/pyunit/runner
tools/toolchain/dbuild build/dev/test/pyunit/runner test/pyunit -v
```

Measured (dev mode, 16-core host): 2 tests, each with its own fresh env,
pass in ~1s of pytest time. Editing a test needs no rebuild at all; the
runner relinks only when the binding surface in runner.cc changes.

## How it works

`runner.cc` is a test executable that links like combined_tests and embeds
CPython (vendored pybind11 headers). `main()` starts seastar on a
background thread (`app_template` in a `std::thread`) and runs pytest on
the main thread. Binding calls cross into the reactor via
`seastar::alien::submit_to()`, blocking the Python thread with the GIL
released until the seastar future resolves. Scylla objects stay
reactor-side; Python holds handles.

The env fixture (conftest.py) opens a `cql_test_env` per test by parking
a seastar thread on a stop promise, so the same env object serves any
number of binding calls before teardown.

`mutate_tablets()` and `rebalance_tablets()` are not reimplemented — the
runner links `tablets_test.cc` and calls the existing helpers.

## What the conversion buys

- **No relink per test edit.** A boost test edit costs a combined_tests
  relink; editing test_tablets.py costs nothing.
- **Less text, same test.** The scenario body shrinks ~80 → ~35 lines:
  replica sets are tuples, the metadata mutation is a `with` block,
  assertions are chained comparisons with pytest's introspected failures.
- **pytest ergonomics.** Parametrization, -k selection, fixtures, and a
  path to REPL-driven exploration of a live env.

## Scope

`scylla_test.pyi` is the binding contract — exactly the surface the
converted tests need; it grows test by test. Tests that exercise the wire
protocol (native transport, auth handshakes) are out of scope: the env
executes CQL in-process.
