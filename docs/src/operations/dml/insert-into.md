# INSERT INTO

The Lance Flink sink appends rows to a Lance dataset. Write mode is controlled by
the `write.mode` option.

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

## Sink options

| Option | Default | Description |
|---|---|---|
| `write.batch-size` | 1024 | Rows buffered before a flush |
| `write.mode` | `append` | `append` or `overwrite` |
| `write.max-rows-per-file` | 1000000 | Rows per data file |

## Current limitations

| Statement | Status |
|---|---|
| `INSERT INTO` (append) | ✅ |
| `INSERT OVERWRITE` | ✅ |
| `UPDATE` | ❌ — not implemented |
| `DELETE` | ❌ — in progress (see issue #63 / #74) |
| Primary key / upsert | ❌ — PK declaration and CDC changelog not yet supported |

> The sink currently declares insert-only changelog mode. CDC `UPDATE` / `DELETE`
> support is tracked in the connector roadmap.
