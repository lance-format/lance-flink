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
| `write.batch-size` | ❌ | 1024 | Write batch size |
| `write.mode` | ❌ | append | `append` or `overwrite` |
| `write.max-rows-per-file` | ❌ | 1000000 | Maximum rows per data file |
| `write.data-storage-version` | ❌ | *(SDK default)* | Lance file format version for written data files, e.g. `2.2`. A `MAP` column requires 2.2+. Fixed when the dataset is created — changing it later does not upgrade an existing dataset. |

Leaving `write.data-storage-version` unset lets the Lance SDK choose, which is
why it has no default here: pinning today's default would hold the connector
back once the SDK moves forward.

Some Arrow types are gated on this version. A `MAP` column is accepted into the
schema at `CREATE TABLE` on any version, but writing rows fails inside the Lance
encoder below 2.2 — so set the option at table creation, not after the first
write attempt.

### MAP columns

A `MAP` column requires `write.data-storage-version` to be `2.2` or newer.
`CREATE TABLE` fails fast if the option is set to something older, naming the
column. If the option is unset the connector only logs a warning, since the
effective default belongs to the Lance SDK.

Two constraints apply to the key and value types:

```sql
-- Rejected: DataTypes.MAP(STRING(), INT()) yields a nullable key, and Arrow
-- does not allow one.
CREATE TABLE t (attrs MAP<STRING, INT>) WITH (...);

-- Correct: the key is declared NOT NULL.
CREATE TABLE t (attrs MAP<STRING NOT NULL, INT>) WITH (
  'write.data-storage-version' = '2.2', ...
);
```

Keys and values support `INT`, `BIGINT`, `FLOAT`, `DOUBLE` and `STRING`. This is
narrower than what a top-level column accepts — the same limit applies to
`ARRAY` elements — and a type outside it is rejected at `CREATE TABLE` rather
than on the first write. An empty map and a `NULL` map are stored distinctly. A
`NULL` value is allowed; a `NULL` key is not.

### MULTISET columns

A `MULTISET` is stored as `MAP<element, count>`, so it carries the same 2.2
requirement, the same element type restrictions, and the same `NOT NULL` rule —
the element becomes the map key:

```sql
-- Rejected: DataTypes.MULTISET(DataTypes.STRING()) yields a nullable element.
CREATE TABLE t (tags MULTISET<STRING>) WITH (...);

-- Correct.
CREATE TABLE t (tags MULTISET<STRING NOT NULL>) WITH (
  'write.data-storage-version' = '2.2', ...
);
```

The count side is always a non-null `INT`, since an element that is present has
an occurrence count by definition.

One asymmetry is worth knowing about: a `MULTISET` column **reads back as
`MAP<element, INT>`**. An Arrow map carries nothing that separates
`MAP<T NOT NULL, INT>` from `MULTISET<T NOT NULL>`, and `MAP` is the far more
common declaration, so an untagged map resolves to `MAP`. Writes are unaffected,
and the stored data is identical either way — only the recovered type name
differs. Tagging the field with metadata would make the distinction survive, but
it would put a Flink-specific key into a schema other engines also read, so it
is deliberately not done.

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
