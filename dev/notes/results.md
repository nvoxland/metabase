# Native query engine: results

This is the state of the DataFusion-based query engine on the `hackathon-datafusion` branch: what it does today, how it measures against the existing JDBC path, what worked, and what isn't good enough yet. The design and the reasoning behind it are in [the design doc](datafusion-engine.md).

## Summary

The engine runs Metabase queries against Postgres and MySQL (including MariaDB) through one compiler, one planner, and one set of rewrite rules, with results streamed back as Arrow. Against the full driver test suites it is close to the JDBC path: 3 engine-attributable failures of about 3,640 Postgres tests, and a small set of known differences on MySQL. On a generated 11-million-row dataset it returns identical results to JDBC on every benchmark query, runs small queries within a millisecond of JDBC, and runs large joins and downloads about two to three times faster. Most of that speedup comes from Postgres being allowed to do more, not from DataFusion.

Much of the engine is still a prototype. Plan-level security isn't implemented, native SQL questions still run on JDBC, the packaging has never run in CI, and a growing layer of plan rewrites compensates for how DataFusion's unparser writes SQL.

## What was built

| Piece | What it does |
| --- | --- |
| Rust workspace (`engine/`) | Planner (DataFusion 55, analyze then unparse, no optimizer), Postgres and MySQL dialects, Postgres and MySQL connectors with pooling, a JDBC-bridge connector, a C ABI and a gRPC/Flight sidecar. About 11,800 lines. |
| `:datafusion` driver | Compiles MBQL to the engine's SQL dialect, executes through the engine, converts Arrow to rows, and runs sync through the engine's connection. |
| Routing | A database-local `query-engine` setting: `jdbc`, `datafusion`, or `shadow` (answer on JDBC, compare both paths in the background). |
| JDBC bridge | A Metabase-hosted Flight service over the existing JDBC pools, for warehouses without a Rust driver. |
| sqllogictest harness (`engine-slt`) | 7 files, 70 queries, run against DataFusion as the reference and against the engine on Postgres and MariaDB. |
| Benchmark (`dev.engine-bench`) | A generated e-commerce database and ten analytical queries run through both drivers with the same row cap. |

The branch is 50 commits, about 22,000 added lines across 127 files. Nothing has been pushed.

## Correctness results

| Measure | Result |
| --- | --- |
| Postgres driver suite (about 3,640 tests, `MB_QUERY_ENGINE=datafusion`) | 3 engine-attributable failures, down from 356 at the first run. 9 transform merge tests also fail, identically on plain JDBC. |
| MySQL driver suite on MariaDB (about 3,650 tests) | 8 differences by design, 2 rejected adversarial aliases, 1 catalog race that passes on a rerun, 8 unexplained API metadata failures (see below). |
| Query processor suites routed through the engine | Pass on both warehouses apart from the known failures above. |
| sqllogictest harness | All 70 queries agree with DataFusion on both warehouses. |
| Benchmark | Identical results on all ten queries, both as questions and as downloads. |

## Benchmark results

The benchmark database is generated in the local Postgres 14: 200,000 customers, 10,000 products, 2 million orders, 6 million order items, and 3 million page views, with skewed customers and growth over time. The queries are native SQL written in the intersection of the engine's dialect and Postgres, so the same text runs through both drivers. Both paths get Metabase's row cap for the way the query runs. Each number is the median of timed runs after a warm-up, reading every row, with the engine built in release mode on an Apple Silicon laptop.

### Run as a saved question (2,000-row cap)

| Query | Rows | JDBC ms | Engine ms | Engine / JDBC |
| --- | --: | --: | --: | --: |
| Point lookup (one customer's orders) | 31 | 2.6 | 3.0 | 1.15 |
| String filter on a small table | 20 | 5.1 | 5.9 | 1.15 |
| Revenue by month and category (3-way join) | 396 | 3,947 | 2,259 | 0.57 |
| Top customers per region (aggregate, then rank) | 200 | 5,803 | 2,648 | 0.46 |
| Daily orders with a 7-day moving average | 1,000 | 436 | 398 | 0.91 |
| Median discount by channel | 4 | 804 | 805 | 1.00 |
| Cohort retention (distinct customers) | 693 | 1,254 | 1,159 | 0.92 |
| Sessions by device (3M-row scan) | 3 | 2,532 | 2,609 | 1.03 |
| 50k-row extract | 2,000 | 10.6 | 10.2 | 0.96 |
| 500k-row extract | 2,000 | 4.0 | 4.0 | 1.02 |

### Run as a CSV download (1,048,575-row cap)

| Query | Rows | JDBC ms | Engine ms | Engine / JDBC |
| --- | --: | --: | --: | --: |
| Point lookup | 31 | 4.1 | 2.9 | 0.71 |
| String filter on a small table | 20 | 4.2 | 4.3 | 1.03 |
| Revenue by month and category | 396 | 4,081 | 2,204 | 0.54 |
| Top customers per region | 200 | 4,810 | 2,165 | 0.45 |
| Daily orders with a 7-day moving average | 1,000 | 393 | 377 | 0.96 |
| Median discount by channel | 4 | 742 | 744 | 1.00 |
| Cohort retention | 693 | 977 | 1,059 | 1.08 |
| Sessions by device | 3 | 2,536 | 2,583 | 1.02 |
| 50k-row extract | 50,000 | 69.7 | 34.2 | 0.49 |
| 500k-row extract | 500,000 | 743 | 235 | 0.32 |

Planning adds 0.1 to 0.8 ms per query.

### What the numbers mean

- **Small queries cost the same.** The engine adds a few tenths of a millisecond: planning, a session, and the catalog check.
- **Large joins and aggregates are about twice as fast because Postgres can run them in parallel.** The JDBC path passes the row cap to `Statement.setMaxRows`, which Postgres receives as a row-limited portal and never runs with parallel workers. On plain JDBC the revenue query takes 4.3 s with the cap and 2.5 s without it. The engine enforces the same cap by stopping the statement, which leaves the plan alone.
- **Downloads are two to three times faster because the engine streams and decodes rows more cheaply.** The first row of a 500k-row download arrives in about 8 ms on the engine.
- **Queries Postgres can't parallelize tie.** Window functions, ordered-set aggregates, and `count(DISTINCT ...)` do the same work on either path.

Both JDBC effects could be fixed in the JDBC path on its own, so the fair reading of the large-query numbers is "the engine doesn't have these two handicaps," not "DataFusion makes Postgres faster."

## What went well

- **The architecture held up.** Analyze, rewrite, unparse, and push the whole query down worked for everything the query processor's suites throw at it, including joins, nested queries, window functions, native pivot tables, time zones, and parameters. No query needed local execution.
- **One compiler serves two warehouses.** MySQL support took about a day: a dialect, a connector, per-warehouse sync SQL, and routing. The same MBQL compiles to the same engine SQL for both.
- **The sqllogictest harness paid for itself immediately.** Comparing each warehouse against DataFusion found semantic drifts the Metabase suites never exercise directly: integer division, `CONCAT` with NULLs, `STDDEV` meaning population on MySQL, `trim` rendering, casts that round instead of truncate, window frames MySQL rejects, and a `LIMIT` left outside a derived table.
- **The gap closed quickly and measurably.** The 771-test query processor measure went from 128 failures to 3, and the full Postgres suite from 356 to 3. Most early failures were type edges (`money`, `inet`, `bytea`, `uuid`, `timetz`), each fixed once.
- **Streaming Arrow works.** Results flow from the warehouse to Clojure in batches with back-pressure, and the first row arrives in milliseconds regardless of result size.
- **The native boundary is stable.** After work moved off JVM threads onto the engine's own runtime, the one JVM crash with a known cause (an unparser stack overflow early on) was fixed, and panics are contained at the C ABI. One unexplained JVM exit is listed under rough edges.
- **Six review rounds (about 60 findings) caught real problems** before the test suites did, including a cancelled statement that could leak a user's impersonation role to the next query.
- **The benchmark found a real performance bug.** DataFusion compares an `int4` column with an `int8` value by casting the column, which kept Postgres off the column's index. A 3 ms lookup took 55 ms, on every filter on an `int4` column. It's fixed, but no correctness test could have caught it.

## What is not great

### Missing pieces

- **Plan-level security isn't implemented.** The policy hook exists but does nothing. Sandboxing and impersonation still work the existing way: MBQL rewriting, plus the engine applying the impersonation role on its connection. Queries whose sandbox is itself a native question stay on JDBC.
- **Native SQL questions don't go through the engine.** Their SQL is the warehouse's dialect, so they always run on JDBC. The engine dialect has no entry point in the product yet, and the benchmark calls the drivers directly.
- **Packaging has never run in CI.** The uberjar workflow builds per-platform libraries, including musl for Alpine, but the branch has never been pushed, so none of it has run. The Alpine image in particular is unverified.
- **Sync is only partly through the engine.** Table, field, and foreign key discovery go through the engine's connection. Fingerprinting, field values, indexes on MySQL, actions, and shadow reruns still use JDBC, so a routed database holds two connection pools.

### Correctness gaps

- **Three Postgres failures remain.** Two are join aliases containing a backslash before a quote, which the engine rejects rather than risk mis-escaping. One is a nested query that relies on Postgres honoring an `ORDER BY` inside a derived table, which DataFusion's planner drops.
- **Some MySQL results differ by design, and users would notice.** Ascending sorts put NULLs last, where MySQL puts them first, and timestamps parsed from `YYYYMMDDHHMMSS` text come back as instants rather than wall-clock times. These need a product decision and release notes, not a code fix.
- **Eight MySQL API metadata tests fail only inside their partition.** They lose the test dataset's semantic types when run in the full suite and pass in isolation with the same settings. The cause is unknown.
- **A MySQL statement can't be cancelled before its first result packet.** MySQL sends column definitions only after it starts executing, and the engine doesn't hand out a stream until then, so a statement that spends its time before returning anything can only be stopped by its time limit.
- **Catalog snapshots can lag behind sync.** The engine plans against Metabase's synced metadata, cached for 10 seconds. A query that hits a missing table or column right after a sync retries once with a fresh snapshot. It works, but it's a heuristic, and one test failure in the MySQL suite is this race.

### Maintenance risk

- **A growing layer of plan rewrites compensates for the unparser.** Before unparsing, the planner merges nested projections, aliases sort keys over derived tables, keeps a `LIMIT` with its sort, drops widening casts on compared columns, emulates `NULLS FIRST`, renames aggregates per warehouse, and qualifies table names. Each fixes a real bug, and each is tested, but they interact, and several cost more than one attempt to get right. They should move upstream to DataFusion where possible.
- **Engine UDFs stand in for SQL the plan can't express.** `mb_cast`, `mb_trunc`, `mb_int_div`, `mb_from_unix`, `mb_at_time_zone` and its two directional forms, and `mb_bytes_to_text` exist because DataFusion's plan loses information or types something differently from the warehouses. Each needs a rendering in every dialect.
- **Semantic choices are scattered.** Integer conversions round, epochs are instants, and zone-less timestamps are read in the report time zone. Each was decided to match the JDBC path's behavior, but there's no single place that states them.
- **Performance regressions are invisible to the correctness suites.** The index bug passed every test for the life of the branch. The benchmark should run regularly, and on MySQL too, which it doesn't yet.

### Rough edges

- **The first download benchmark run ended with the JVM exiting and no crash log.** It didn't recur with the output captured and a larger heap, so the cause is unknown.
- **Developer setup is fiddly.** In a source checkout the loader prefers the debug build of the engine, which is far slower than release, and a bare REPL needs the application initialized before sync can read every setting. Neither is obvious.
- **Shadow mode keeps its comparisons in memory only.** There's no UI, and nothing survives a restart.

## Code comparison at the end state

The prototype hasn't deleted anything, so this compares the code the final version would have with the code it would retire, assuming DataFusion replaces query compilation and execution for every SQL driver. Postgres and MySQL were built, so their numbers are measured. The rest are extrapolated from them and should be read as estimates. Line counts are code lines, excluding blanks, comments, and tests.

### Removed and added

**Removed: about 4,100 lines.** That is every SQL driver's query-compilation overrides (3,539 lines, 763 multimethods across 17 drivers), result reading for drivers that move to native connectors (about 220 lines), and a few hundred lines of per-driver hooks in the shared compiler. The shared HoneySQL compiler stays as the single compiler to the engine dialect. The shared JDBC execution layer stays for bridged warehouses. Every driver's connection, sync, actions, uploads, and DDL code stays.

**Added:**

| Part | Estimate | Based on |
| --- | --: | --- |
| Engine core, FFI, sidecar, JDBC bridge, Clojure driver (without shadow mode and other transitional code) | ~4,800 | Measured |
| About 13 dialects, sharing families (Presto, Trino, and Athena; Spark, Databricks, and Hive; Postgres and Redshift) | 5,500–8,000 | The Postgres dialect is 1.6 times and the MySQL dialect 2.4 times the old compile code |
| Native connectors for Postgres, MySQL, ClickHouse, Snowflake, and BigQuery (Redshift reuses Postgres) | 4,700–6,200 | 1,000 to 1,230 lines per connector |
| Per-warehouse sync dispatch in Clojure | ~500 | Measured for MySQL |
| **Total** | **15,500–19,500** | |

The end state has about four times as much code as it removes. Dialects are larger than the compilers they replace because they render DataFusion's whole function surface, not just what MBQL emits, and because they carry emulations the old compilers never needed (for MySQL: `split_part`, `NULLS FIRST`, regex case sensitivity). Warehouses without a good Rust driver (SQL Server, Oracle, Vertica, Hive, Druid, H2) go through the bridge and cost only a dialect.

### What gets easier and what gets harder

Easier:

- **A construct is written once.** A new MBQL feature is one compiler change, plus a rendering only in dialects that spell it differently. Today it can mean 17 driver overrides.
- **One test suite checks every warehouse.** The sqllogictest harness compares each warehouse with DataFusion, so drift is caught in one place instead of per driver.
- **Capabilities and semantics live in one place.** A dialect's policy states what its warehouse can run, and anything else is rejected with a structured error. Today capabilities are spread across 89 feature flags and each driver's behavior.
- **Result reading is uniform.** Native connectors return Arrow, and the per-driver JDBC row readers go away.
- **Security gets one enforcement point.** Plan-level policy would cover MBQL, AI-generated SQL, and eventually native SQL.

Harder:

- **A fixed platform cost:** Rust, a native boundary, per-platform builds (musl, aarch64), and debugging across two languages.
- **Coupling to DataFusion.** DataFusion, Arrow, and Flight upgrade in lockstep every 6 to 10 weeks. The planner's rewrites depend on unparser behavior that can change in any release unless the fixes go upstream.
- **Dialects are harder to write than HoneySQL overrides.** A dialect has to preserve the plan's semantics, not just its syntax. Most of the bugs found building MySQL were semantic: integer casts, NULL ordering, time zones.
- **Connectors are per-warehouse code Metabase owns.** Session setup, cancellation, the row cap, and type conversion were written once for every JDBC driver. Each native connector now has its own.

### Why the connectors reimplemented so much

The Rust drivers aren't the problem. `tokio-postgres` is mature and widely used, and `mysql_async` is solid. They handle the wire protocol, authentication, TLS, and typed values (about 12,500 lines for Postgres and 37,000 for MySQL), and none of that was rewritten. None of the connector bugs found this week were in those libraries. They were in the code on top of them: `SET ROLE NONE` dropping MySQL's default role, `timestamptz` parameter offsets, MySQL zero dates (a single JDBC connection property handles those), and unsigned integers.

The reason is architectural. The engine executes in Rust and returns Arrow, and Metabase's JVM code can't be reused across that boundary. Here is what the 1,230-line Postgres connector does, and where the same job is done today:

| Connector part | Lines | Today's equivalent |
| --- | --: | --- |
| Connection details to client config, TLS modes | 144 | The Postgres driver's JDBC spec code, about the same size |
| Rows to Arrow | 337 | Shared JDBC row reading plus each driver's `read-column-thunk` |
| Parameters from JSON to Postgres types | 300 | Each driver's `set-parameter` methods (8 in Postgres). Parameters cross the native boundary as JSON. |
| Pool, session setup, cancel, row cap, streaming | 449 | The shared `sql_jdbc` execution and connection code (about 1,350 lines) and c3p0 |

So what was rebuilt is Metabase's own JDBC glue, once per connector, not the vendor drivers. JDBC's real advantage is a standard type model and connection-property conventions, plus decades of vendor quirk handling. The Rust clients are lower level, so the conversions are ours. Where Rust drivers are genuinely weak is the long tail (SQL Server, Oracle, Vertica, Hive, Athena), which is why those go through the bridge.

### Ways to shrink it

Each option below is estimated against the baseline of 15,500 to 19,500 added lines and about 4,100 removed.

- **ADBC drivers and a shared connector toolkit.** An ADBC driver returns Arrow and takes Arrow parameters, so row conversion and parameter binding (about half of each connector) mostly disappear, leaving about 100 lines of type adjustments. A shared toolkit would hold the generic half of each connector's main file (session lifecycle, cancellation, the row cap, streaming with back-pressure), about 600 lines once, leaving about 150 warehouse-specific lines per connector. With about 300 lines to integrate the ADBC driver manager, the five connectors come to 2,300 to 3,300 lines instead of 4,700 to 6,200. The catch: official Apache ADBC drivers cover Postgres, Snowflake, BigQuery, SQLite, and Flight SQL. MySQL, ClickHouse, and others come from the ADBC Driver Foundry under mixed licenses, or stay native and use only the toolkit. ADBC drivers are also C or Go libraries, one more native artifact per platform, with a blocking API.
- **Dialects scoped to what MBQL emits.** In the MySQL dialect, about a third of the override arms (`strpos`, `starts_with`, `ends_with`, `ln`, `trunc`, `random`, `regexp_like`, `regexp_replace`, trims with explicit characters) render functions the MBQL compiler never emits, and by a manual reading only about 40 of its 97 allowlisted functions are ones MBQL uses. Scoping would cut 20 to 30 percent of each dialect, bringing the dialects to 4,100 to 6,200 lines. The cost is the design decision that the whole DataFusion SQL surface is available: engine-dialect native SQL and AI-generated SQL would hit rejections more often. The rejection is safe, but it is a smaller product.
- **Always use the bridge.** No native connectors at all: every warehouse runs through its vendor JDBC driver behind the bridge. That removes the connectors (4,700 to 6,200 lines) and the per-warehouse sync dispatch (about 500), since bridged databases sync on their own driver. It also keeps the JDBC row readers (about 220 fewer lines removed). What remains is the single compiler, the dialects, plan-level policy, and the harness. The costs are an extra loopback hop and two Arrow conversions per query, and the bridge's performance hasn't been benchmarked. The streaming and parallel-query gains in the benchmark would also depend on how the bridge reads results.

| Scenario | Added | Removed | Net growth |
| --- | --: | --: | --: |
| Baseline | 15,500–19,500 | ~4,100 | +11,400–15,400 |
| ADBC and a shared toolkit | 13,100–16,600 | ~4,100 | +9,000–12,500 |
| Dialects scoped to MBQL | 14,100–17,700 | ~4,100 | +10,000–13,600 |
| ADBC, toolkit, and scoped dialects | 11,700–14,800 | ~4,100 | +7,600–10,700 |
| Always the bridge | 10,300–12,800 | ~3,900 | +6,400–8,900 |
| Always the bridge, with scoped dialects | 8,900–11,000 | ~3,900 | +5,000–7,100 |

Even the smallest version is larger than today's code. The case for the engine is consistency, testability, and plan-level security, not less code. Among the options, always using the bridge cuts the most code and keeps those benefits, but it leaves performance as an open question. ADBC with a shared toolkit keeps native performance and removes about half the connector code, on warehouses where an ADBC driver exists.

## Does DataFusion earn its place?

This section compares three ways to reach the same goals: the design as built (DataFusion planning and executing, with native or ADBC connectors where they exist and the JDBC bridge for the long tail), a reorganization of the existing Clojure code, and Apache Calcite planning in the JVM with execution staying on JDBC.

### What each capability needs

| Capability | Clojure refactor | Calcite and JDBC | DataFusion as built |
| --- | --- | --- | --- |
| One compiler for MBQL across warehouses | Yes: refactor the `sql.qp` overrides into a shared semantic layer (the idea behind the engine's `mb_*` functions) and a small rendering table per dialect | Yes | Yes |
| Consistent semantics across warehouses, tested | Yes, with the harness ported to Clojure and a reference such as DuckDB | Partly: its execution is too weak to serve as the reference, so it needs one too | Yes: DataFusion is the reference the harness already uses |
| Capabilities in one place, structured rejections | Yes: a per-dialect capability table in place of scattered feature flags; MBQL clauses are a closed set | Yes | Yes |
| Row cap, cancellation, parallel plans | Yes, by fixing the JDBC execution path | Yes, the same way | Yes, in each connector |
| Sandboxing for MBQL | Already done, by rewriting MBQL | Could move into the plan | Could move into the plan |
| Validating native or AI-generated SQL against the catalog | No | Yes | Yes |
| One SQL dialect for native and AI SQL, translated per warehouse | No (sqlglot on GraalPy is best-effort, slow, and prone to hanging) | Yes, about 40 dialects | Yes, 7 upstream dialects plus Metabase's own |
| Plan-level security on SQL people write: row filters, column masks, table allowlists, SELECT-only guaranteed | No | Yes | Yes |
| Arrow from the warehouse to Metabase: streaming, cheaper decoding | No | No | Yes |
| Local compute over Arrow: post-processing, caching, Parquet, uploads, bounded fallbacks when a warehouse lacks a feature | No | Weak | Yes |

The first five rows don't need a new engine. MBQL is already a structured, typed query plan, and a Clojure refactor could give it one compiler, tested semantics, and one place for capabilities. The benchmark's largest speedups also came from two JDBC-path details (`setMaxRows` blocking Postgres parallel query, and buffering whole results) that can be fixed in Clojure. Most of the engine's rewrite layer exists to correct how DataFusion's unparser writes SQL, and it wouldn't be needed there.

The next three rows need a SQL planner, and Calcite provides one as well as DataFusion does.

The last two rows are what only DataFusion provides: an Arrow data path from the warehouse, and a real engine to compute over it. The prototype realizes little of that yet. It converts Arrow back into Clojure rows at the end, and none of the post-processing, caching, or exports has moved into the engine. The download benchmark shows the data-path gain is real (a 500k-row download in 235 ms against 743 ms), but part of that gap is the JDBC path's buffering, which a Clojure fix would also remove.

### DataFusion or Calcite

[Apache Calcite](https://calcite.apache.org/) is an open-source Java library for parsing, validating, optimizing, and translating SQL. It isn't a database. It's the SQL layer other systems build on, including Apache Hive, Flink, Druid, Drill, and Phoenix. It parses SQL, validates it against a catalog you supply (names, types, implicit casts), turns it into relational algebra that can be inspected and rewritten with rules, optimizes it, and unparses a plan back to SQL for about 40 dialects. It can also execute queries through adapters, but that part is weaker.

The design doc set Calcite aside because it executes poorly, falls back to in-memory processing, and doesn't speak Arrow. That still holds for execution. It doesn't rule out Calcite as a planner in front of the existing JDBC execution, which is what the middle column above describes.

| | DataFusion | Calcite |
| --- | --- | --- |
| Runs in | Rust, behind a native boundary (FFI or a sidecar) | The JVM, as a library |
| Packaging | Native builds per platform (glibc, musl, aarch64, macOS) | A jar |
| Execution | Its own connectors, returning Arrow | The existing JDBC path, unchanged |
| Unparser dialects | 7 upstream (Postgres, MySQL, SQLite, DuckDB, BigQuery, Snowflake, default) | About 40, including SQL Server, Oracle, Vertica, Hive, Presto, ClickHouse, and Redshift |
| Local execution | Fast, Arrow-native, a real engine | Weak; fine for small results, not a compute engine |
| Semantic reference for testing | Yes: it runs queries, and the harness uses it as the reference | Only through its weaker execution |
| Tested here | The unparser, the rewrites, and both driver suites (about 3,650 tests per warehouse) | Not at all; its unparser will need its own fixes |
| Learning curve | Rust, DataFusion's plan and unparser internals | Calcite's type system and relational-algebra APIs, which are large and steep |
| Release coupling | DataFusion, Arrow, and Flight upgrade in lockstep every 6 to 10 weeks | A single library, on a slower cadence |

Neither is free. Calcite's unparser would need the same kind of dialect fixes this prototype needed for DataFusion, and its rewrites would be as intricate. DataFusion carries the native platform cost and the connector code described above.

### What decides it

- **If the goal is MBQL consistency and maintainability**, a Clojure refactor gets most of it, with no Rust, no native builds, and no connector code.
- **If the goal is also governing native and AI-generated SQL, with plan-level security**, Metabase needs a SQL planner. Calcite in front of the existing JDBC execution is the alternative to prototype against: it covers those rows without the native boundary or connectors, and with more dialects.
- **If Metabase will compute over Arrow itself** (post-processing, caching, exports, Parquet, fallbacks for missing warehouse features), DataFusion is the right foundation, and it covers everything above as well. Its costs are worth paying if that roadmap is real. If it isn't, the Arrow data path and local engine are capabilities the product wouldn't use.

## Recommended next steps

1. Push the branch and get the engine CI jobs, including the per-platform native builds and the Alpine image, running for real.
2. Decide the product questions: NULL ordering on MySQL, timestamps from parsed text, and whether the native editor gets an engine-dialect mode.
3. Fix the JDBC path's two handicaps (`setMaxRows` disabling parallel query, buffering whole results) independently of the engine, so the comparison reflects the engine itself.
4. Upstream the unparser fixes to DataFusion and retire the matching rewrites.
5. Run the benchmark on MySQL and add it to CI with a regression threshold.
6. Benchmark the bridge against native connectors, and try an ADBC driver for Postgres, before choosing the connector strategy for the remaining warehouses.
7. Decide what the engine is for: MBQL consistency only, governing native and AI SQL, or local compute over Arrow. That choice decides between a Clojure refactor, a SQL planner in the JVM, and DataFusion with native connectors.
8. Unless local compute over Arrow is on the roadmap, spike Calcite as a planner in front of the JDBC path, on the same sqllogictest files and driver suites, before committing further to either.
9. Implement plan-level policy, starting with row filters, in whichever planner wins.
