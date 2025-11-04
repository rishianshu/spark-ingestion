# Partition & Maintenance Configuration

This release adds opt-in controls that let you tune Iceberg partitioning, apply selective filters, and run a maintenance-only job without reading from sources. Everything is driven from configuration; existing pipelines continue to work unchanged unless you supply the new fields.

## Global defaults

`runtime.intermediate.partition_defaults` (optional)  
Applies to every table that lands in the Iceberg “intermediate” catalog unless a table overrides it.

| Field | Type | Description |
|-------|------|-------------|
| `spec` | array\<object> | Partition transforms to apply. Each entry needs `field` (column name) and optional `transform` (see table below). |
| `derived_columns` | array\<object> | Derived columns to materialise before writes. Each entry: `name` (column) and `expr` (Spark SQL expression). |
| `merge_filter` | string or `{ "expr": string }` | Optional predicate evaluated before merging into Iceberg/raw. Lets you keep maintenance windows “hot” by filtering out cold history. |

Supported transforms in `spec.transform`:

| Transform | Example | Result |
|-----------|---------|--------|
| `identity` (default) | `{ "field": "load_date" }` | Keep raw column as partition. |
| `year` / `years` | `{ "field": "event_ts", "transform": "year" }` | `years(event_ts)` |
| `month` / `months` | `{ "field": "event_ts", "transform": "month" }` | `months(event_ts)` |
| `day` / `days` | `{ "field": "event_ts", "transform": "day" }` | `days(event_ts)` |
| `bucket:<N>` | `{ "field": "order_id", "transform": "bucket:32" }` | `bucket(32, order_id)` |
| `truncate:<len>` | `{ "field": "carrier", "transform": "truncate:4" }` | `truncate(4, carrier)` |

## Table overrides

Inside each table map:

```json
{
  "schema": "dbo",
  "table": "orders",
  "mode": "incremental",
  "source_filter": [
    "status <> 'VOID'",
    "is_deleted = 0"
  ],
  "intermediate": {
    "partition": {
      "spec_mode": "replace",
      "spec": [
        { "field": "order_bucket", "transform": "bucket:32" },
        { "field": "order_month", "transform": "identity" }
      ],
      "derived_columns": [
        { "name": "order_month", "expr": "date_trunc('month', order_ts)" },
        { "name": "order_bucket", "expr": "hash(order_id)" }
      ]
    },
    "merge_filter": {
      "expr": "order_month >= date_trunc('month', add_months(current_date(), -6))"
    }
  },
  "maintenance": {
    "keep_backup": true
  }
}
```

Per-table fields:

| Field | Type | Meaning |
|-------|------|---------|
| `source_filter` | string or array\<string> | Appended to every JDBC read (`read_full`, `read_slice`, `count_between`). Use SQL predicates to prune the source before Spark pulls it. |
| `intermediate.partition.spec_mode` | `"append"` (default) \| `"replace"` | Controls whether table entries extend or replace the global spec. |
| `intermediate.partition.spec` | array\<object> | Same shape as the global spec; appended or replaced per `spec_mode`. |
| `intermediate.partition.derived_columns` | array\<object> | Additional derived columns. Follows `derived_mode` (defaults to `spec_mode`). |
| `intermediate.merge_filter` | string or `{ "expr": string }` | Predicate applied to the Iceberg target during the merge (`t` alias). Source rows are preserved; only matching target partitions are scanned/updated. Column names are matched case-insensitively and rewritten against the target alias. |

> Tip: Derived columns are materialised before filters, so predicates can reference them directly (`order_month`, `order_bucket`, etc.).

### Reloading tables

Use the CLI flag `--reload` to reset a table’s watermark and re-pull data from the source. Pass specific tables as a comma-separated list (e.g. `--reload schema.table1,schema.table2`) or omit the value/ use `*` to reload every selected table. Reloading is helpful after rebuilding from RAW to repopulate Iceberg without dropping table metadata.

## Maintenance mode

Use the new CLI flag to run maintenance without pulling from sources:

```
spark-ingestion --config conf/optiva.json \
  --only-tables dbo.orders \
  --maintenance-mode dry-run
```

Modes:

| Mode | Behaviour |
|------|-----------|
| `dry-run` | Reports partition spec, derived columns, merge filter, and any staged slices waiting to merge. |
| `apply` | Updates the Iceberg partition spec to match current config, rewrites existing data files (even when no slices are pending), and merges staged slices under the new layout. No source reads occur. |
| `rebuild-from-raw` | Rebuilds the Iceberg table from RAW data. The table is renamed to a timestamped backup, RAW is deduplicated/applied with derived columns, a fresh table is created with the configured partition spec, and the backup is dropped (unless `maintenance.keep_backup` is true). |

Additional notes:

- Full-table rewrites require primary keys if you introduce derived columns that do not already exist on the Iceberg table. Without keys the run is aborted and reports `derived_columns_require_primary_keys`.
- Result statuses can be `noop`, `rewritten` (data reloaded with the same spec), or `repartitioned` (spec changed and Iceberg files rewritten).

Both modes accept the regular `--load-date` flag; when omitted a current date is used for maintenance writes.

Outputs are printed as JSON for quick inspection, and the standard log stream is annotated with `maintenance_*` events.

### Rebuild considerations

- RAW must contain the full history you want to restore. The helper uses the table's primary keys and incremental column (plus `load_timestamp` when present) to deduplicate records before writing the new Iceberg table.
- RAW partitions are processed one at a time (e.g., `load_date=YYYY-MM-DD`) to avoid loading the full dataset in a single Spark job; total row count is reported in the maintenance output.
- The existing table is renamed to `table__backup_<timestamp>` prior to the rebuild. Set `maintenance.keep_backup: true` in the table config if you want to preserve that backup for manual inspection; otherwise it is dropped after a successful rebuild.
- If any step fails the backup is restored automatically and the rebuild reports `status: error`.
