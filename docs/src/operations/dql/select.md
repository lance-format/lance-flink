# SELECT

The Lance Flink connector supports reading Lance datasets via Flink SQL `SELECT`.
Read optimizations are pushed down to Lance natively to reduce I/O.

## Supported pushdowns

| Ability | Interface | Notes |
|---|---|---|
| Column projection | `SupportsProjectionPushDown` | Only projected columns are read |
| Predicate (filter) | `SupportsFilterPushDown` | `WHERE` clauses are pushed to Lance |
| Limit | `SupportsLimitPushDown` | `LIMIT` is pushed down |
| Aggregate | `SupportsAggregatePushDown` | Eligible aggregates run natively |

## Example

```sql
-- Projection + filter + limit are all pushed down
SELECT id, content
FROM lance_table
WHERE id > 100
LIMIT 10;
```

## Static read options

The same read behaviour can be configured declaratively on the table DDL
without relying on planner pushdown:

```sql
CREATE TABLE lance_table (
    id BIGINT,
    content STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/data/vectors',
    'read.columns' = 'id,content',
    'read.filter' = 'id > 100',
    'read.limit' = '10'
);
```

| Option | Description |
|---|---|
| `read.columns` | Comma-separated columns to read |
| `read.filter` | SQL `WHERE`-style filter string |
| `read.limit` | Maximum rows to read |
