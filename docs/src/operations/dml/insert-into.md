# INSERT INTO

The Lance Flink sink appends rows to a Lance dataset. Write mode is controlled by
the `write.mode` option.

## Write modes

| `write.mode` | Behaviour |
|---|---|
| `append` (default) | Append rows to the existing dataset |
| `overwrite` | Replace the dataset once per job |

`overwrite` is decided by the serial committer, which tags every version it commits with the
`flink.job-id` transaction property:

- the first commit of a job issues `Overwrite` and replaces whatever the dataset held before;
- later checkpoint commits of the same job issue `Append`, so rows written by other subtasks or by
  earlier checkpoints are preserved.

A new submission gets a new job id and therefore replaces the previous result again. Because the
decision no longer happens in the writer, OVERWRITE jobs are safe with parallelism &gt; 1 — a
late-starting subtask never deletes files that a peer already materialized.

## Example

```sql
INSERT INTO vectors VALUES
    (1, 'Hello World', ARRAY[0.1, 0.2, 0.3, 0.4]);
```

## Sink options

| Option | Default | Description |
|---|---|---|
| `write.batch-size` | 1024 | Rows buffered before a flush |
| `write.mode` | `append` | `append` or `overwrite` |
| `write.max-rows-per-file` | 1000000 | Rows per data file |
| `write.memory-cap-bytes` | -1 | Buffered-bytes threshold that forces an early fragment materialization (`-1` = unbounded) |

## Primary key: upsert and delete

Declaring a primary key switches the sink to keyed upsert mode:

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

-- +I: insert
INSERT INTO users VALUES (1, 'Alice', ARRAY[0.1, 0.2]);

-- +U / -D arrive from any changelog source and are applied via mergeInsert
INSERT INTO users SELECT id, name, embedding FROM upstream_cdc;
```

| Declared schema | Accepted changelog |
|---|---|
| no primary key | `INSERT` only |
| `PRIMARY KEY` (single or composite) | `INSERT`, `UPDATE_AFTER`, `DELETE` |

Rows are `keyBy` the primary key so that every event for a key reaches the same subtask; the
committer then applies `Dataset.mergeInsert` serially.

## Statement support

| Statement | Status |
|---|---|
| `INSERT INTO` (append) | ✅ |
| `INSERT OVERWRITE` | ✅ |
| `UPDATE` | ❌ — no dedicated statement; use an upsert table with a primary key |
| `DELETE` | ❌ — no dedicated statement; use an upsert table with a primary key |

> The sink declares insert-only changelog mode unless the table declares a primary key.
