# 本轮（UPDATE 实现）过程中暴露的既有缺陷

这两项都不是 `UPDATE` 引入的，也不在其修复范围内。它们是实现 `UPDATE` 时因首次有端到端
SQL 测试覆盖而暴露出来的既有问题，单独记录以免随实现一起被淡化。

---

## C1：`*ITCase` 测试在 `mvn test` 中从未被执行

**现象**

surefire 默认只匹配 `*Test` / `Test*` / `*Tests` / `*TestCase`，`*ITCase` 不在其中，
而项目未配置 failsafe 插件、也未自定义 `<includes>`。因此以下测试类从未在全量构建中运行：

```
LanceUpsertSinkITCase          9 个用例（含 1 个长期失败，见 C2）
LanceCatalogTableChangeITCase  8
LanceNamespaceCatalogITCase    ...
LanceCatalogTableITCase        ...
LanceTimeTravelITCase          4
LanceSinkConcurrencyITCase     1
CompositePkDeleteITCase        1
```

单独执行 `-Dtest='*ITCase'` 时共 86 个用例。

**影响**

此前多轮报告的「全量 N 个测试通过」均未包含这 86 个用例，其中含一个真实失败。
A6 的 `LanceCatalogTableChangeITCase` 等验收测试实际上从未在全量回归中生效。

**处置**

本轮新增的两个测试类已命名为 `*Test` 以确保执行。既有 ITCase 未改名，因为重命名会
影响 CI 中可能存在的分阶段执行约定（如故意把集成测试留给单独的 job）。

**待决策**

需要确认 CI 是否另有 `-Dtest='*ITCase'` 阶段。若没有，应二者择一：
配置 failsafe 并绑定 `verify`，或把 ITCase 纳入 surefire 的 `<includes>`。
在此之前，「全量通过」的说法应明确排除 ITCase。

---

## C2：两个 subtask 并发首写时主键元数据提交冲突

**现象**

`LanceUpsertSinkITCase#twoSubtasksConcurrentFirstWrite` 长期失败：

```
RuntimeException: Incompatible transaction: This UpdateConfig transaction is
incompatible with concurrent transaction UpdateConfig at version 2.
  at PrimaryKeyPersistence.persist(PrimaryKeyPersistence.java:73)
  at LanceUpsertSink.open(LanceUpsertSink.java:211)
```

已确认为既有缺陷：在本轮改动前的提交上 stash 验证，同样失败。

**成因**

`LanceUpsertSink.open()` 对首写执行 open-or-create，随后每个 subtask 都调用
`PrimaryKeyPersistence.persist` 写入主键元数据。该写入是一个 `UpdateConfig` 事务，
多个 subtask 并发提交时 Lance 的冲突解析器直接拒绝，而非合并。

**影响**

并行度 > 1 的表在首次写入时可能整体失败。dataset 已存在时不受影响，
因此在先建表再写入的流程中不易触发。

**候选方向**

- 仅由 subtask 0 写元数据，其余 subtask 跳过
- `persist` 捕获冲突异常后重读校验，若目标值已一致则视为成功
- 把主键元数据的写入前移到 catalog 建表阶段，使 sink 的 open 不再写 config

需要先确认 Lance 是否为 `UpdateConfig` 提供幂等或可重试的提交语义，再选方案。
