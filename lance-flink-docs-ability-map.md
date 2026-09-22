# lance-flink 官方文档能力映射表（文档规划）

> 目标：在 `lance-flink` 仓库新增 `docs/src/`，对照 `lance-spark` 的文档模板，把**已实现能力**映射到
> `index / install / config / performance / operations{ddl,dql,dml}` 结构。本文档是「范围确认」产物，
> 用于在动笔写文档正文前对齐边界。状态标注：✅ 已实现 / 🟡 部分实现 / ❌ 未实现（文档中需明确标注）。

## 1. 已注册的 Factory（SPI 权威来源）

来源：`src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`

| 类 | 标识（type） | 角色 | 文档归属 |
|---|---|---|---|
| `LanceDynamicTableFactory` | `lance` | 表工厂（Source + Sink） | operations/ddl + dql + dml |
| `LanceCatalogFactory` | `lance` | 目录型 Catalog（warehouse: local / S3） | operations/ddl（CREATE CATALOG） |
| `LanceNamespaceCatalogFactory` | `lance-namespace` | Namespace Catalog（impl: dir / rest） | operations/ddl（CREATE CATALOG） |

## 2. 能力 → 文档文件映射（总览）

| 文档文件 | 已实现能力（可写正文） | 未实现（需占位标注 / 不写） |
|---|---|---|
| `index.md` | 整体介绍、Features 列表、Quick Start | — |
| `install.md` | 多 Flink 版本 1.18/1.19/1.20、依赖坐标 | — |
| `config.md` | 全部 ConfigOption（读/写/索引/向量/S3/hadoop.*） | — |
| `operations/ddl/` | CREATE TABLE、CREATE CATALOG、database 操作 | ALTER TABLE、CREATE INDEX、分区 |
| `operations/dql/` | SELECT（列裁剪/过滤/limit/聚合下推）、向量搜索、时间旅行 | 全文搜索、混合搜索 |
| `operations/dml/` | INSERT INTO（append）、INSERT OVERWRITE、UPDATE、DELETE、主键/upsert | — |
| `performance.md` | 索引类型、向量调优参数 | benchmark 数据 |

## 3. 各文件详细内容清单

### 3.1 index.md（Welcome）✅
- Introduction：Flink 连接器读写 Lance 数据集
- Features：
  - 读写 Lance 数据集（append / overwrite）
  - 列裁剪、谓词下推、limit 下推、聚合下推
  - 向量搜索表函数（L2 / Cosine / Dot）
  - 时间旅行（read.version / read.as-of-timestamp）
  - 索引构建（IVF_PQ / IVF_HNSW_PQ / IVF_FLAT）
  - 双 Catalog（目录型 `lance` + namespace `lance-namespace`）
- Quick Start：`CREATE CATALOG` + `CREATE TABLE` + `INSERT INTO` + `SELECT` 最小示例

### 3.2 install.md ✅
- 支持版本：Flink 1.18 / 1.19 / 1.20（各自独立 artifact）
- 依赖：`org.lance:lance-core:7.0.0`、arrow 18.3.0
- JDK：11+（JNI 需 platform-specific native lib）
- 本地验证：`mvn clean verify`

### 3.3 config.md ✅（全部 ConfigOption）
按 `LanceOptions` 25 项 + 两个 CatalogFactory 选项分组：

| 分组 | 配置项 |
|---|---|
| 读（Source） | `read.batch-size` / `read.limit` / `read.columns` / `read.filter` / `read.version` / `read.as-of-timestamp` |
| 写（Sink） | `write.batch-size` / `write.mode`(append/overwrite) / `write.max-rows-per-file` |
| 索引 | `index.type` / `index.column` / `index.num-partitions` / `index.num-sub-vectors` / `index.num-bits` / `index.max-level` / `index.m` / `index.ef-construction` |
| 向量搜索 | `vector.column` / `vector.metric`(L2/Cosine/Dot) / `vector.nprobes` / `vector.ef` / `vector.refine-factor` |
| Catalog（目录型 `lance`） | `warehouse` / `default-database` / `s3-access-key` / `s3-secret-key` / `s3-region` / `s3-endpoint` / `s3-virtual-hosted-style` / `s3-allow-http` |
| Catalog（namespace `lance-namespace`） | `impl`(dir/rest) / `root` / `uri` / `default-database` |
| 通用 | `path` / `hadoop.*`（tbdsfs/hdfs 路径解析，去掉前缀注入 Hadoop Configuration） |

### 3.4 operations/ddl/
| 能力 | 状态 | 说明 |
|---|---|---|
| `CREATE CATALOG`（type=lance） | ✅ | warehouse local/S3 + S3 认证选项 |
| `CREATE CATALOG`（type=lance-namespace） | ✅ | impl=dir / rest |
| `CREATE DATABASE` / `DROP DATABASE` / `ALTER DATABASE` | ✅ | `LanceCatalog` 已实现 |
| `SHOW DATABASES` / `SHOW TABLES` | ✅ | listDatabases / listTables |
| `CREATE TABLE` | ✅ | 表工厂 `connector='lance'`，实际 dataset 在首次写入时创建 |
| `DROP TABLE` / `RENAME TABLE` | ✅ | 已实现 |
| `ALTER TABLE` | ✅ | `ADD`/`DROP`/`RENAME COLUMN` 与 `SET`/`RESET TBLPROPERTIES`（经 `TableChange`）；列类型变更仍拒绝（SDK `castTo` 为静默 no-op） |
| `CREATE INDEX` | ❌ | 无 SQL DDL，仅程序化 `LanceIndexBuilder`（index.* 配置在表工厂侧） |
| 分区（`PARTITIONED BY`） | ❌ | `LanceCatalog` 分区方法返回空 / 不支持 |

### 3.5 operations/dql/
| 能力 | 状态 | 说明 |
|---|---|---|
| `SELECT`（列裁剪） | ✅ | `SupportsProjectionPushDown` |
| `WHERE`（谓词下推） | ✅ | `SupportsFilterPushDown`，`read.filter` 亦可；标识符反引号转义，日期/时间戳/decimal 用类型化字面量；`IN`/`BETWEEN` 由 planner 展开为 `OR`/范围合取后下推；`NaN`/`Infinity`、含 `.` 的列名回退给 Flink |
| `LIMIT` | ✅ | `SupportsLimitPushDown` |
| 聚合下推 | ✅ | `SupportsAggregatePushDown` |
| 向量搜索（表函数） | ✅ | `LanceVectorSearchFunction.eval(dataset, column, vector, k, metric)`，支持 Float[]/BigDecimal[]/Double[]/float[] |
| 时间旅行（`VERSION AS OF` / `TIMESTAMP AS OF`） | ✅ | `read.version` / `read.as-of-timestamp`（经 `LanceOpener`） |
| 全文搜索 | ❌ | 未实现 |
| 混合搜索 | ❌ | 未实现 |

### 3.5.1 类型映射（`LanceTypeConverter` / `RowDataConverter`）
| 能力 | 状态 | 说明 |
|---|---|---|
| 数值 / 字符串 / 布尔 / 二进制 | ✅ | TINYINT~BIGINT、FLOAT/DOUBLE、STRING、BOOLEAN、BYTES、BINARY |
| `DATE` / `TIMESTAMP` | ✅ | Date32 / Timestamp（无时区） |
| `DECIMAL` | ✅ | Decimal128，保留 precision/scale（Flink 上限 38，无需 256 位） |
| `TIME` | ✅ | 按精度选 Time32(SECOND/MILLI) 或 Time64(MICRO/NANO)；值按 Flink 语义取毫秒 |
| `TIMESTAMP_LTZ` | ✅ | Timestamp + UTC 时区标记，借此与无时区 `TIMESTAMP` 区分，往返不塌缩 |
| `ARRAY` / `ROW` | ✅ | List / FixedSizeList、Struct |
| `MAP` / `MULTISET` | ❌ | 无 Arrow 映射，建表即拒绝 |

### 3.6 operations/dml/
| 能力 | 状态 | 说明 |
|---|---|---|
| `INSERT INTO`（append） | ✅ | `write.mode=append`（默认） |
| `INSERT OVERWRITE` | ✅ | `write.mode=overwrite`（首次写或 overwrite 模式） |
| `UPDATE` | ✅ | `SupportsRowLevelUpdate`，`UPDATED_ROWS` 模式经主键 `mergeInsert` upsert；要求声明主键 |
| `DELETE` | ✅ | 经主键 key-only `mergeInsert` + `withMatchedDelete`（PR #76 + follow-up A4） |
| 主键 / upsert | ✅ | `PRIMARY KEY NOT ENFORCED` 时声明 `+I/+U/-D`，`keyBy` 保序后走 `mergeInsert` |

### 3.7 performance.md 🟡
- 索引类型与选型：IVF_PQ / IVF_HNSW_PQ / IVF_FLAT（`LanceIndexBuilder`）
- 向量搜索调优：`vector.nprobes` / `vector.ef` / `vector.refine-factor`
- 写放大控制：`write.max-rows-per-file` / `write.batch-size`
- 暂无 benchmark 数据（占位）

## 4. 与 spark 模板的差异（需要在文档中体现）

| 项 | spark | flink |
|---|---|---|
| 数据源 API | DatasourceV2 (DSv2) | Flink Table API（DynamicTableSource/Sink + CatalogFactory） |
| SQL 语法定位 | 交互式 DDL/DML | 声明式长驻拓扑（CREATE TABLE + INSERT INTO） |
| vendors | databricks | 无（tbdsfs/hdfs 通过 `hadoop.*` 前缀支撑） |
| 全文/混合搜索 | ✅ | ❌ |
| DELETE/UPDATE | ✅ | ✅（需声明主键） |

## 5. 建议的文档落地顺序

1. `index.md` + `install.md` + `config.md`（纯描述已实现能力，零风险）
2. `operations/dql/`（SELECT / 向量搜索 / 时间旅行，能力最完整）
3. `operations/ddl/`（CREATE CATALOG / CREATE TABLE，标注 ALTER TABLE 不支持）
4. `operations/dml/`（INSERT，标注 DELETE/UPDATE 为 TODO 链接 #63/#74）
5. `performance.md`（索引与调优参数，无 benchmark）
