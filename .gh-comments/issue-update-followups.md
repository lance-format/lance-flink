# UPDATE 实现过程中暴露的既有缺陷（已解决）

这两项都不是 `UPDATE` 引入的。它们是实现 `UPDATE` 时因首次有端到端 SQL 测试覆盖而暴露
出来的既有问题，均已在本轮修复。保留本文件作为背景记录。

---

## C1：`*ITCase` 测试在构建中从未被执行 —— 已修复

**曾经的现象**

surefire 的默认 includes 不匹配 `*ITCase` 后缀，而项目未配置 failsafe 插件。CI 跑的是
`mvn verify`，但 `verify` 阶段的插件链里只有 surefire，没有任何 failsafe，因此 75 个
集成测试从未在构建中运行——CI 每次"通过"都不包含它们，其中还藏着一个真实失败（见 C2）。

**修复**

在 `pom.xml` 的 pluginManagement 声明 `maven-failsafe-plugin`（3.3.0，因内部镜像无 3.1.2），
并在 build/plugins 绑定 `integration-test` + `verify` 两个 goal。verify goal 是关键：
没有它，报告会生成但构建仍然通过。

failsafe 默认 includes 含 `**/*ITCase.java`，正好匹配项目约定，无需自定义。既有 ITCase
未改名，保留 Flink 生态的标准命名。

**验证**

`mvn verify` 现在每模块执行 surefire 276 + failsafe 75，三个 Flink 版本模块全绿。
移除 verify goal 曾确认失败的集成测试不会中断构建，加回后恢复拦截。

---

## C2：两个 subtask 并发首写时主键元数据提交冲突 —— 已修复

**曾经的现象**

`LanceUpsertSinkITCase#twoSubtasksConcurrentFirstWrite` 失败：

```
RuntimeException: Incompatible transaction: This UpdateConfig transaction is
incompatible with concurrent transaction UpdateConfig at version 2.
  at PrimaryKeyPersistence.persist(...)
  at LanceUpsertSink.open(...)
```

**成因**

`PrimaryKeyPersistence.persist` 无条件调用 `dataset.updateConfig` 写主键元数据。该调用
是一个版本化的 Lance 事务，不是幂等 put；多个 subtask 并发首写时同时提交，Lance 的冲突
解析器直接拒绝而非合并。原注释"Idempotent; safe to call on every open"是错误假设。

**修复**

让 `persist` 真正幂等，两道防线：

1. 写入前先比较——值已一致则完全不发起事务，稳态零写入；
2. 冲突时先 `dataset.checkoutLatest()` 把句柄推进到最新版本（`getConfig` 读的是 open 时
   的旧快照，不推进就永远看不到对端提交），再重读校验；若目标值已达成则视为成功，
   否则照常抛出。

第 2 步的句柄推进是关键：移除它测试立即复现原冲突，证明句柄陈旧才是根因。冲突异常是
Rust 层的裸 `RuntimeException` 无专用类型，因此用重读校验结果而非匹配消息来判断。

**验证**

`twoSubtasksConcurrentFirstWrite` 连续 3 次稳定通过；移除 `checkoutLatest` 立即复现原
冲突（证明测试与诊断有效）。另加 `PrimaryKeyPersistenceConcurrencyTest`（5 个单元用例），
其中一条专门钉住"容错不得吞掉真实分歧"——不同的 key list 仍必须写入。
