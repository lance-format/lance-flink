# MAP 类型支持的前置阻塞：Lance 文件格式版本

## 结论

`MAP` 映射不能只补类型转换层。Lance 的 Map **数据写入**要求文件格式 2.2+，而连接器
当前所有写入路径都使用 SDK 默认的 2.1，因此照清单补完 `LanceTypeConverter` 与
`RowDataConverter` 只会得到一个「建表成功、写入必失败」的功能——比不做更糟。

## 实测证据

spike 走生产 `mergeInsert` 路径（复用 `ArrowArrayStreamsTestAccess`，而非自建 harness）。

**默认 2.1**：schema 可建、可往返，写数据时在 Rust 编码层被拒：

```
UnsupportedOperationException: Not supported: Map data type is only supported in
Lance file format 2.2+, current version: 2.1
  at lance-encoding/src/encoder.rs:485
```

注意失败点在**写入**而不是建表。`Dataset.create(schema)` 带 Map 列完全成功，重开后
schema 也无损：`Map(false)<entries: Struct<key: Utf8 not null, value: Int(32, true)>>`。
这正是这个缺口危险的地方——任何只验证 DDL 的测试都看不出问题。

**显式 `withDataStorageVersion("2.2")`**：三种场景全部无损往返：

| 行 | 写入 | 读回 | isNull |
|---|---|---|---|
| 0 | `{"a":1,"b":2}` | `[{"key":"a","value":1},{"key":"b","value":2}]` | false |
| 1 | 空 map | `[]` | false |
| 2 | NULL | `null` | true |

第 2 行实证了 NULL map 走 `setNull` 路径正确，即上一轮 `MapVector` 分支顺序修正
（`MapVector extends ListVector`，必须先匹配）所保护的场景。

## 当前代码状态

三处 `WriteParams` 构造点均未设置存储版本，全部走默认 2.1：

- `LanceUpsertSink.java:231`（open-or-create）
- `LanceCatalog.java:595`（CREATE TABLE 物化）
- `LanceSink.java:169`（Fragment 写入，只设了 `maxRowsPerFile`）

`WriteParams.Builder#withDataStorageVersion(String)` 在 lance-core 7.0.0 存在且有效
（已实测）。

## 因此的工作拆分

**前置项：暴露存储版本配置** —— 已完成

新增 `write.data-storage-version`，接入全部三处 `WriteParams` 构造点。默认值为
`noDefaultValue()`，不硬编码 `2.1`——否则 SDK 升级默认版本时我们反而把用户钉死在旧版本。
未设置时行为与改动前完全一致。

`LanceCatalog#createTable` 的接入是必需的而非可选：版本在建表时固化，只在 sink 设置对
已建表无效。该接入点此前**没有任何测试覆盖**——禁用它全部既有套件依然全绿，因此补了
`DataStorageVersionMapITCase#createTableAppliesConfiguredVersion`，用
`Dataset#getLanceFileFormatVersion()` 对比「显式 2.2」与「未设置」两张表，并实测禁用后
该用例失败。

**主体项：MAP 类型映射** —— 待做

前置项已就位，可以开始。落地时必须在 `flinkTypeToArrowField` 遇到 MAP 而存储版本 < 2.2
时给出明确报错，而不是让用户在写入时撞 Rust 层的错。

## 过程中另外发现的问题

**`mergeInsert` 不保证行序。** 首版往返断言按位置取行，实际读回顺序是
`空 / NULL / {a,b}`，与写入顺序不同。断言已改为按 `id` 关联。这不是缺陷，但任何按位置
断言的测试都会随机失败。

**工厂与 `LanceOptions` 之间有 16 个选项重复定义**，键名完全重合（`path`、`write.mode`、
`read.*`、`index.*`、`vector.*` 等）。这是 A7「双份真相」在另一处的同类复发，规模更大。
本次按既有模式在两处都加了新选项以保持一致，未夹带重构——独立处理更安全。

## 未验证项

- 2.1 与 2.2 数据集能否在同一路径混用（时间旅行读旧版本）。
- 2.2 是否影响其他类型的编码或既有索引。
- `MULTISET`（Flink 侧是 `Map<T, Integer>`）是否同样受 2.2 限制——推测受同样限制，未实测。
