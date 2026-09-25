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

## Recommended next steps

1. Push the branch and get the engine CI jobs, including the per-platform native builds and the Alpine image, running for real.
2. Decide the product questions: NULL ordering on MySQL, timestamps from parsed text, and whether the native editor gets an engine-dialect mode.
3. Fix the JDBC path's two handicaps (`setMaxRows` disabling parallel query, buffering whole results) independently of the engine, so the comparison reflects the engine itself.
4. Upstream the unparser fixes to DataFusion and retire the matching rewrites.
5. Run the benchmark on MySQL and add it to CI with a regression threshold.
6. Implement plan-level policy, starting with row filters, which is the main reason for the engine to exist.
