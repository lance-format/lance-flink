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
| `read.columns` | Comma-separated columns to read; applied when the planner pushes no projection |
| `read.filter` | SQL `WHERE`-style filter string, ANDed with any pushed predicate |
| `read.limit` | Maximum rows to read; applied when the planner pushes no `LIMIT` |

## Aggregate pushdown

`SupportsAggregatePushDown` lets global aggregates run natively in Lance instead of pulling every
row into Flink:

```sql
SELECT COUNT(*), MIN(id), MAX(id), SUM(id), AVG(id)
FROM lance_table;
```

Supported functions: `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`, `SUM`, `AVG`, `MIN`, `MAX`.

Two deliberate limits:

- **Grouped aggregates (`GROUP BY`) are not pushed down.** They are computed by Flink, so results
  are always correct; only the plan differs.
- **`SUM(DISTINCT col)` / `AVG(DISTINCT col)` are not pushed down** for the same reason.

Anything that cannot be translated falls back to Flink execution transparently.
