## Flink 2.1 support — ready for review 🎉

Following up on my earlier comment: the Flink 2.1 adaptation is now landed on the `feat/flink-2.1-support` branch.

### What shipped
Four incremental PRs delivered Flink 2.1 support while keeping Flink 1.20 fully green:

| PR | Scope |
|----|-------|
| **PR#1 — scaffold + Source V2** | Added `lance-flink-2.1` module (JDK 17, Flink 2.1.1); rewrote `LanceSource` / `LanceAggregateSource` to **FLIP-27 `Source<RowData, LanceSplit, LanceEnumState>`** with split enumerator + reader; introduced `LanceSourceStreamBridge` so `LanceDynamicTableSource` keeps a single source tree. |
| **PR#2 — Sink V2** | Rewrote `LanceSink` to the Flink 2.1 2PC mix-in combo **`Sink<RowData>` + `SupportsCommitter<LanceCommittable>`** (note: the old `TwoPhaseCommittingSink` interface was dropped in 2.1 per FLIP-372 follow-ups). Split into `LanceSinkWriter` (precommit → Lance `Fragment.write()`) + `LanceCommitter` (commit → Lance `CommitBuilder.execute(Transaction)`) + `LanceCommittable` + serializer. Lance's own 2-step "write files → commit transaction" model is a natural fit for Flink's 2PC, giving us **real Exactly-Once** rather than the per-batch commit the old `RichSinkFunction` did. |
| **PR#3 — Catalog / Table adaptation** | Fixed 4 breaking API changes in the shared Table/Catalog layer that affect both 1.20 and 2.1: `TableSchema` removal, `CatalogTable.of(...)` → `CatalogTable.newBuilder()...build()`, `SupportsProjectionPushDown.applyProjection(int[][], DataType)` new signature, `DataStreamScanProvider.produceDataStream(ProviderContext, env)` new signature. **Zero bridge classes needed here** — the new signatures are already available in 1.20 as default/overloaded methods, so one source tree works for both. |
| **PR#4 — tests + polish** | Updated tests for the new signatures; migrated planner dependency to `flink-table-planner-loader` + `flink-table-runtime` (the plain `flink-table-planner` artifact is not published to Central for 2.x); refreshed README (badges, Prerequisites, DataStream API examples); updated GitHub Actions matrix. |

### Version matrix

| Flink | JDK | Module | Status |
|-------|-----|--------|--------|
| 1.20.x | 11 / 17 / 21 | `lance-flink-1.20` | ✅ build green |
| 2.1.x  | 17 / 21 | `lance-flink-2.1`  | ✅ build green |

Flink 1.18 / 1.19 modules were retired in this cycle (both are already end-of-life upstream).

### Architecture notes worth flagging for reviewers

1. **Shared source tree**: `src/main/java` (root) is pulled into both version modules via `build-helper-maven-plugin`. Classes that are not source-compatible across 1.x → 2.x (`LanceSource`, `LanceAggregateSource`, `LanceSink`, two tiny `*Bridge` glue classes) are `<exclude>`d in the 2.1 module's compiler config, and the 2.1 module provides same-package replacements. The vast majority of connector code (type conversion, catalog, options, pushdown, aggregate executor, …) lives in exactly one place.
2. **Why a bridge for Source/Sink mounting**: `env.addSource(SourceFunction, name)` (1.x) vs. `env.fromSource(Source, WatermarkStrategy, name)` (2.x) and `SinkFunctionProvider.of(sink)` (1.x) vs. `SinkV2Provider.of(sink)` (2.x) are not source-compatible, so `LanceDynamicTableSource` / `LanceDynamicTableSink` delegate exactly one line each through `LanceSourceStreamBridge` / `LanceSinkProviderBridge`, which each module overrides locally.

Happy to split the branch into four review-sized PRs on request, or just squash-merge as one. Thanks for the patience 🙏