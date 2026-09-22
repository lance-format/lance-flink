# Time Travel

The Lance Flink connector supports reading historical versions of a dataset via
time-travel options, resolved uniformly through `LanceOpener`.

## Options

| Option | Type | Description |
|---|---|---|
| `read.version` | LONG | Read the given dataset version (highest precedence) |
| `read.as-of-timestamp` | STRING | Read as of an ISO-8601 timestamp; resolves to the newest version whose creation time is ≤ the timestamp |

When both are set, `read.version` takes precedence.

## Examples

```sql
-- Read a specific version
SELECT * FROM lance_table /*+ OPTIONS('read.version' = '3') */;

-- Read as of a timestamp (resolves to the newest version <= the timestamp)
SELECT * FROM lance_table /*+ OPTIONS('read.as-of-timestamp' = '2026-07-01T00:00:00Z') */;
```

Declaratively on the table DDL:

```sql
CREATE TABLE lance_table (
    id BIGINT,
    content STRING
) WITH (
    'connector' = 'lance',
    'path' = '/data/vectors',
    'read.version' = '3'
);
```

## Semantics

- `read.version` opens exactly that version.
- `read.as-of-timestamp` accepts any string parseable by
  `Instant.parse`, `OffsetDateTime.parse`, or `ZonedDateTime.parse`;
  a bare `yyyy-MM-ddTHH:mm:ss` is assumed UTC.
- A timestamp predating the oldest version raises a clear error.
