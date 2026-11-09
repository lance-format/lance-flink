# Config

Configuration options are grouped by area. The table connector uses `'connector' = 'lance'`; the
catalog types are `'lance'` (directory/S3) and `'lance-namespace'` (dir/rest).

## Table connector options (`connector = 'lance'`)

### Common

| Option | Required | Default | Description |
|---|---|---|---|
| `path` | ✅ | — | Path to the Lance dataset |
| `hadoop.*` | ❌ | — | Prefix for Hadoop-family filesystem config (e.g. `hadoop.tbdsfs.meta`); stripped and injected into the Hadoop `Configuration` used for path resolution |

### Read (Source)

| Option | Required | Default | Description |
|---|---|---|---|
| `read.batch-size` | ❌ | 1024 | Read batch size |
| `read.limit` | ❌ | — | Maximum rows to read (limit pushdown) |
| `read.columns` | ❌ | — | Columns to read, comma separated |
| `read.filter` | ❌ | — | SQL `WHERE`-style filter predicate |
| `read.version` | ❌ | — | Time travel: read a specific dataset version |
| `read.as-of-timestamp` | ❌ | — | Time travel: read as of an ISO-8601 timestamp (ignored when `read.version` is set) |

### Write (Sink)

| Option | Required | Default | Description |
|---|---|---|---|
| `write.batch-size` | ❌ | 1024 | Rows buffered per writer before a flush. Controls flush granularity, not disk layout: rows are materialized on `flush()` / `prepareCommit()` |
| `write.mode` | ❌ | append | `append` or `overwrite` |
| `write.max-rows-per-file` | ❌ | 1000000 | Maximum rows per data file |
| `write.memory-cap-bytes` | ❌ | -1 | Estimated in-memory buffer size that forces an early fragment materialization. `-1` means unbounded — `write()` never materializes, which removes the orphan-file window but makes memory usage grow with the checkpoint interval |

> **`write.mode = overwrite`**: the dataset is replaced **once per job**. The decision lives in the
> serial committer, which tags every version it commits with the `flink.job-id` transaction
> property: the first commit issues `Overwrite`, later checkpoint commits issue `Append`. OVERWRITE
> jobs are therefore safe with parallelism &gt; 1.

### Vector index

| Option | Required | Default | Description |
|---|---|---|---|
| `index.type` | ❌ | IVF_PQ | `IVF_PQ`, `IVF_HNSW`, or `IVF_FLAT` |
| `index.column` | ❌ | — | Vector column name to index |
| `index.num-partitions` | ❌ | 256 | IVF partition count |
| `index.num-sub-vectors` | ❌ | — | PQ sub-vector count (auto if unset) |
| `index.num-bits` | ❌ | 8 | PQ quantization bits (1–16) |
| `index.max-level` | ❌ | 7 | HNSW max level |
| `index.m` | ❌ | 16 | HNSW connections per level |
| `index.ef-construction` | ❌ | 100 | HNSW construction search width |

### Vector search

| Option | Required | Default | Description |
|---|---|---|---|
| `vector.column` | ❌ | — | Vector search column name |
| `vector.metric` | ❌ | L2 | `L2`, `Cosine`, or `Dot` |
| `vector.nprobes` | ❌ | 20 | IVF search probe count |
| `vector.ef` | ❌ | 100 | HNSW search width |
| `vector.refine-factor` | ❌ | — | Refine factor for recall |

## Primary key and changelog mode

Declare a primary key to switch the sink from append-only to upsert:

```sql
CREATE TABLE users (
    id BIGINT,
    name STRING,
    embedding ARRAY<FLOAT>,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'lance',
    'path' = '/data/users'
);
```

| Declared schema | Changelog mode accepted by the sink |
|---|---|
| no primary key | `INSERT` only |
| `PRIMARY KEY` | `INSERT`, `UPDATE_AFTER`, `DELETE` (`UPDATE_BEFORE` is dropped) |

Upserts are applied by `Dataset.mergeInsert` in the serial committer; rows are `keyBy` the primary
key so all events for one key reach the same subtask. Composite primary keys are supported.

## Catalog options (`type = 'lance'`)

Directory or S3 warehouse.

| Option | Required | Default | Description |
|---|---|---|---|
| `warehouse` | ✅ | — | Warehouse path (local or `s3://…`) |
| `default-database` | ❌ | default | Default database name |
| `s3-access-key` | ❌ | — | S3 access key ID |
| `s3-secret-key` | ❌ | — | S3 secret access key |
| `s3-region` | ❌ | — | S3 region (e.g. `us-east-1`) |
| `s3-endpoint` | ❌ | — | S3 endpoint (for S3-compatible storage like MinIO) |
| `s3-virtual-hosted-style` | ❌ | true | Virtual-hosted-style URLs |
| `s3-allow-http` | ❌ | false | Allow HTTP (default HTTPS only) |

## Namespace catalog options (`type = 'lance-namespace'`)

| Option | Required | Default | Description |
|---|---|---|---|
| `impl` | ✅ | — | Namespace implementation: `dir` or `rest` |
| `root` | ❌ | — | Root path for directory namespace |
| `uri` | ❌ | — | URI for REST namespace |
| `default-database` | ❌ | default | Default database name |

## Hadoop-family filesystem options

Any option prefixed with `hadoop.` is stripped and injected into the Hadoop `Configuration` used to
resolve the dataset path, so `hdfs://` / `tbdsfs://` / `viewfs://` locations can be reached without
a `core-site.xml` on the node:

```sql
CREATE TABLE hdfs_table (
    id BIGINT,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = 'tbdsfs:///path/to/dataset',
    'hadoop.tbdsfs.meta' = 'zk://namenode:2181/tbdsfs',
    'hadoop.fs.defaultFS' = 'tbdsfs:///'
);
```
