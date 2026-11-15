# Schema-dependent clustering_key format — prototype tracker

Branch: `ck-schema-format-audit` (tgrabiec/scylla). Base: master 38258344856.

## Goal

Let the in-memory format of `clustering_key_prefix` depend on the schema
(the clustering column types), e.g. to drop per-component length prefixes for
fixed-width types. Today every key uses the legacy compound format
`<u16 len><value>...`, which `compound_type<allow_prefixes::yes>` decodes
statically.

## Invariants the format must satisfy

1. **Stable across ALTER.** `alter_table_statement` lets a clustering column
   change type when `is_compatible_with()` holds: ascii→text, string→blob,
   timestamp↔date, frozen UDT/collection growth, and reversal. Memtable and
   cache keys outlive the schema version that encoded them. Specialising only
   fixed-width types (timestamp and date are both 8 bytes) satisfies this.
2. **Invariant under `reversed_type`.** `schema::make_reversed()` wraps every
   clustering type, and both schemas share key bytes. So does
   `get_unreversed_tri_compare()` in `statement_restrictions.cc`, which builds
   its own `compound_type` from un-reversed types.
3. **Component values stay contiguous spans of their serialized value.** Callers
   keep `managed_bytes_view`s into key components (`select_statement.cc:1153`,
   `db/view`, sstable writers). Only the framing (lengths, empty markers) may
   change.
4. **Canonical.** `utils/interval.hh:61` compares bounds by bytes, without a
   schema.
5. **Empty values representable** in fixed-width columns (a zero-sized `int`
   is legal in a clustering key).

## Audit method

`keys/keys.hh` shadows the schema-free members of `clustering_key_prefix` and
`clustering_key_prefix_view` with `[[deprecated]]` wrappers:

- `ck-noschema`: `components()`, `explode()`, `explode_fragmented()`,
  `begin()/end()`, `from_exploded(range)`, `from_exploded_view`, `from_range`,
  `vector<bytes>` / `vector<managed_bytes>` / `initializer_list` constructors.
- `ck-raw`: `representation()`, `operator managed_bytes_view`, `from_bytes()`.

Run all TUs with `-fsyntax-only` (patch the `cxx.dev` / `cxx_with_pch.dev`
rules in `build.ninja` to `-fsyntax-only $in && touch $out`), then bucket the
warnings by site. First run: 2190 TUs, 0 errors, 157 unique sites
(100 `ck-noschema`, 57 `ck-raw`) in 50 files.

Not covered: templates never instantiated with clustering keys, code compiled
out, and code that passes the *wrong* schema (MV base vs view, reversed).

Comparators (`less_compare`, `tri_compare`, `prefix_equal_tri_compare`,
`equal`, `hashing`, `is_full`) all take a schema already. Nothing outside
`keys.hh` calls the `compound_type<allow_prefixes::yes>` statics directly.

## Legacy adaptor (done)

`legacy_clustering_key_prefix` (`keys/keys.hh`) holds a key in the legacy
format, without a schema. `clustering_key_prefix::to_legacy(const schema&)`
and `from_legacy(const schema&, ...)` convert by decomposing with the schema
and re-encoding, so they stay correct once the in-memory format diverges.
`idl/keys.idl.hh` declares it with the same IDL layout as
`clustering_key_prefix` (`std::vector<bytes> explode()`). `keys_test`
`test_legacy_clustering_key_prefix` checks that the wire bytes match in both
directions.

Converted:

| Site | Use |
|---|---|
| `sstables/mx/writer.cc:1233,1277,1291` | large data records written into the sstable |
| `db/virtual_tables.cc:1917,2085` | read large data records |
| `tools/scylla-sstable/scylla-sstable.cc:1794` | dump large data records |
| `db/large_data_handler.cc:304,449` | in-memory lookups matched against on-disk records |
| `db/corrupt_data_handler.cc:92` | `system.corrupt_data.clustering_key` blob |

## primary_key_layout (in progress)

`keys/primary_key_layout.hh` defines the abstract `primary_key_layout`, and
`schema::clustering_key_layout()` holds one, made by
`make_primary_key_layout(types)` in `schema::rebuild()`. Today it always
returns `legacy_primary_key_layout`, which matches
`compound_type<allow_prefixes::yes>` byte for byte (checked in `keys_test`).

Virtual core:

- iteration: `first(cursor&)` and `next(cursor&)`. The non-virtual
  `iterator` (forward) wraps a `cursor {key, rest, current, index, at_end}`,
  so a layout can find component i from the whole key (header-based formats).
- `size()`, `deserialize_value()` (explode)
- `compare()`, `prefix_equal_compare()`, `equal()`, `hash()`
- `serialized_size(sizes)` and `serialize(sizes, component_source&, out)`.
  The non-virtual template `serialize_value(range)` collects component sizes,
  then calls `serialize()`, which pulls each value through a type-erased
  `component_source`. Any FragmentedView range serializes without copying.

The non-virtual API mirrors `compound_type` (`types()`, `begin/end/components`,
`less`, `is_full`, `is_empty`, `validate`, `serialize_single`,
`serialize_optionals`, `serialize_value_deep`), so the templates in `keys.hh`
work unchanged. `clustering_key_prefix{,_view}::compound` is now
`primary_key_layout_ptr` (`seastar::shared_ptr<const primary_key_layout>`;
`lw_shared_ptr` cannot hold a derived class), and `get_compound_type(s)`
returns the layout. The schema-free (deprecated) members decode the legacy
format explicitly through `compound_type<allow_prefixes::yes>` statics.

Switched to the layout outside `keys.hh`:
`bound_view::tri_compare` (`clustering_bounds_comparator.hh`), BTI
(`comparable_bytes_from_compound(const primary_key_layout&, ...)`), and
`get_unreversed_tri_compare()` (uses `make_primary_key_layout()`).

`schema::clustering_key_type()` / `clustering_key_prefix_type()` remain, for
legacy-format users: sstables `composite`, `tools/scylla-types`,
`tools/lua_sstable_consumer` (`validate()` of user hex input), and callers
that only need `->types()`.

Cost: one virtual call per key comparison. Generic iteration costs one per
component step, so `legacy_primary_key_layout` overrides the hot walkers
(`size`, `prefix_equal_compare`, `deserialize_value`) with `compound_type`'s
non-virtual iterator.

### Measurements

`perf_simple_query --smp 1 -m 2G --duration 10 --clustering-rows N`, release
`--disable-lto`, hal, median of 3 alternating runs. Baseline: ca0516ef14f plus
`--clustering-rows`. Partitions: 10000 (1000 for 1000 rows). Reads hit the row
cache. tps change against the baseline:

| rows | mode | generic walkers | legacy overrides | insns/op (overrides) |
|---|---|---|---|---|
| 0 | read | -2.8% | -0.9% | +142 (+0.4%) |
| 0 | write | -3.9% | -0.3% | +446 (+1.1%) |
| 10 | read | -5.4% | -2.0% | +48 (+0.1%) |
| 10 | write | -5.7% | -2.1% | +779 (+1.6%) |
| 1000 | read | -7.4% | -2.5% | -350 (-0.8%) |
| 1000 | write | -0.1% | -0.2% | -260 (-0.5%) |

Run-to-run spread is 0.1-3.5%. Before the overrides, `perf` showed `first`,
`prefix_equal_compare`, `size` and `next` at ~9.5% of cycles in the 10-row
write (2.6% for the inlined comparison in the baseline). Open: the extra ~780
insns/op in the 10-row write (likely key construction through
`serialize_value()` and its virtual `component_source`); cycles rise more
than instructions (indirect calls).

Not virtualized yet: `prefix_view_on_prefix_compound::less_compare_with_prefix`
and `is_prefixed_by()` (they iterate components generically), and
`validate()` (it iterates generically).

## IDL (todo)

The generated `serializer<clustering_key_prefix>` calls `explode()` and the
`vector<bytes>` constructor, with no schema. Fields that carry clustering keys:

- `mutation.idl.hh:98` (`deletable_row::key`), `:113/116` (range tombstone
  start/end), `:121`
- `position_in_partition.idl.hh:27` (used by `full_position`, `repair`,
  `paging_state`, `mutation`)
- `paging_state.idl.hh:40`
- `query.idl.hh:28` (result row key)
- `read_command.idl.hh:23,33` (`partition_slice` ranges)

Plan: switch these to `legacy_clustering_key_prefix` (wire-compatible), and
convert with the schema at each (de)serialization point; or thread the schema
into the deserializers.

## Raw-format consumers needing rework (todo)

- **BTI** (`sstables/trie/bti_key_translation.cc:116,125`). The code passes
  `key.representation()` to `comparable_bytes_from_compound()`
  (`types/comparable_bytes.cc:1543`), which walks it with
  `compound_type::components()` and the schema's types. It is only a
  problem through the static decoder: if `compound_type::components()`
  becomes format-aware (an instance method), it keeps working. Cleaner: pass
  the key and schema, or the `components(s)` range. If the new format is
  byte-comparable itself, BTI translation could collapse into a (near-)copy.
- **`statement_restrictions.cc:2167–2232`** (`starts_before_start`,
  `starts_before_or_at_end`, `ends_before_end`). Once two bounds compare equal
  under `prefix_equal_tri_compare` (one is a prefix of the other), the code
  compares `representation().size()` to decide whether they are truly equal
  or which one is the shorter prefix. That relies on every component adding
  bytes. A format where an empty fixed-width component costs zero bytes
  (e.g. a header bitmap) breaks it. Fix: compare component counts via the
  `cmp.prefix_type`. The keys are encoded under the real schema and decoded
  through the un-reversed `compound_type`; see invariant 2.
- `mutation/json.hh:48` (hex of raw bytes in JSON dumps),
  `tools/json_mutation_stream_parser.cc:204`,
  `tools/lua_sstable_consumer.cc:239,434`: tools that print or parse raw bytes.
  Switch to the legacy adaptor to keep the external format.
- `vector_search/hybrid_search.cc:35`: raw bytes in `serialized_primary_key`.
  Check whether they leave the node.

## Schema in scope, mechanical fix (todo)

`cdc/log.cc:1123,1520,1656,2111`, `cql3/restrictions/statement_restrictions.cc`
(2349, 2437, 2458/2459, 2509, 2546–2564, 2595), `cql3/statements/cas_request.cc:182`,
`cql3/statements/external_search/values_provider.cc:118`,
`cql3/statements/select_statement.cc:689,1155,1162,1216,1624`,
`cql3/statements/update_statement.cc:60`, `cql3/update_parameters.cc:32,75`,
`db/size_estimates_virtual_reader.cc:36`, `db/system_keyspace.cc:2634,2667`,
`db/view/build_progress_virtual_reader.hh:103`,
`db/view/view.cc:318,399,672,3547,3565`,
`mutation/mutation_partition_serializer.cc:245`, `replica/mutation_dump.cc:130`,
`sstables/kl/reader.cc:511,690,747,749`, `sstables/metadata_collector.cc:23`,
`sstables/mx/parsers.hh:82`, `sstables/mx/reader.cc:310,481,494,2065`,
`sstables/sstables.cc:1564`, `vector_search/vector_store_client.cc:158`.

Need the schema passed down one level: `cdc/log.cc:1520`
(`process_change_visitor`), `select_statement.cc:1153`
(`append_base_key_to_index_ck`), `size_estimates_virtual_reader.cc:36`,
`statement_restrictions.cc:2484` (token-index schema is fixed).

Tests: 32 sites in `test/boost` and `test/lib`, all with a schema at hand.

## Deferred

- Alternator (`executor.cc:3839,5049` assume `representation().size() - 2`;
  `executor_read.cc`, `serialization.cc`, `ttl.cc`).
