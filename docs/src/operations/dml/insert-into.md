# INSERT INTO

The Lance Flink sink writes rows to a Lance dataset. Behaviour depends on whether
the table declares a primary key.

## Write modes

| `write.mode` | Behaviour |
|---|---|
| `append` (default) | Append rows to the existing dataset |
| `overwrite` | Replace the dataset on first write |

## Example

```sql
INSERT INTO vectors VALUES
    (1, 'Hello World', ARRAY[0.1, 0.2, 0.3, 0.4]);
```

## Upsert mode

When a table declares `PRIMARY KEY (...) NOT ENFORCED`, the sink accepts a CDC
changelog stream and maps it onto Lance native operations:

| Change | Applied as |
|---|---|
| `+I` / `+U` | `mergeInsert` with update-all + insert-all |
| `-D` | key-only `mergeInsert` with matched-delete + not-matched-do-nothing |
| `-U` | dropped (the new value carries all required state) |

```sql
CREATE TABLE users (
    id BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'lance',
    'path' = '/data/users.lance'
);
```

Deletes are matched by primary-key value, so any column type the connector can
write can also serve as a primary key, including `DATE`, `TIMESTAMP`, `DECIMAL`
and `VARBINARY`.

## Sink options

| Option | Default | Description |
|---|---|---|
| `write.batch-size` | 1024 | Rows buffered before a flush |
| `write.mode` | `append` | `append` or `overwrite` |
| `write.max-rows-per-file` | 1000000 | Rows per data file |
| `arrow.allocator-max-bytes` | unlimited | Upper bound in bytes for the Arrow allocator |

### Bounding Arrow memory

Each sink, source, catalog and index builder creates its own Arrow allocator.
By default these are unbounded, which lets a single oversized batch exhaust
off-heap memory and affect other slots in the same TaskManager.

`arrow.allocator-max-bytes` caps each allocator individually:

```sql
CREATE TABLE users (
    id BIGINT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'lance',
    'path' = '/data/users.lance',
    'arrow.allocator-max-bytes' = '536870912'
);
```

The bound is per allocator instance, not a total for the job. Size it against
one component's working set — roughly `write.batch-size` times the row width,
with headroom — rather than against the TaskManager's whole off-heap budget.

## Hot primary keys

In upsert mode events are routed with `keyBy` on the primary key so that all
events for a key reach one subtask in order. This ordering is required for
correctness, but it also means a single key's throughput is capped by one
subtask.

If the key distribution is skewed, that subtask gates the whole pipeline while
its peers idle. The sink collapses repeated writes to the same key within a
checkpoint, which absorbs update-heavy skew, but not sheer volume concentrated
on one key.

Where the natural key is known to be skewed, prefer a composite primary key
including a higher-cardinality column, or salt the key:

```sql
PRIMARY KEY (tenant_id, event_id) NOT ENFORCED
```

Two-level hashing is not offered: it would break in-key ordering, which the
sink's flush sequencing depends on.

## Current limitations

| Statement | Status |
|---|---|
| `INSERT INTO` (append) | ✅ |
| `INSERT OVERWRITE` | ✅ |
| Primary key / upsert | ✅ |
| `DELETE` (via CDC changelog) | ✅ |
| `UPDATE` (standalone statement) | ❌ — not implemented |

> Standalone `UPDATE` and `DELETE` SQL statements are not supported; row-level
> changes are applied through a CDC changelog stream into a table declaring a
> primary key.
