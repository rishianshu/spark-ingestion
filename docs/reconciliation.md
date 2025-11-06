# Reconciliation Framework

The reconciliation subsystem validates downstream datasets against their upstream
sources using the same configuration model that powers ingestion.  It runs as a
separate Spark job and executes a sequence of configurable “checks” for each
table.  Checks express intent (e.g. “plain count”) and delegate query execution
to the table endpoints via the shared query-plan abstraction
(`QueryPlan`/`QueryResult`).  This keeps reconciliation agnostic of the
execution tool—Spark today, SQLAlchemy or any other engine tomorrow.

## Running reconciliation

```bash
spark3-submit \
  --jars ... \
  recon.py \
  --config conf/miniboss_oss.json \
  --only-tables itcftth.srv_cfsver_ct_main \
  --checks plain_count
```

The CLI arguments:

- `--config` – ingestion configuration file (shared with ingestion/maintenance).
- `--only-tables` – optional comma separated list of `schema.table` filters.
- `--checks` – optional comma separated list of check types or names. When
  omitted all configured checks are executed.
- `--output-json` – path where the JSON summary should be written instead of
  printing to stdout.
- `--fail-on-warn` – exit with status `2` if any check returns `warn`, `fail`
  or `error`.
- `--engine` – select the execution backend (`spark` or `sqlalchemy`).
- `--max-parallel` – override the maximum number of tables processed in parallel
  (default derived from `runtime.reconciliation_defaults.max_parallel`).

The command prints (or writes) a JSON structure with overall summary and per-check
details. For example:

```bash
spark3-submit ... recon.py --config conf/miniboss_oss.json \
  --only-tables itcftth.srv_cfsver_ct_main \
  --checks plain_count \
  --output-json /tmp/recon-results.json
```

The file at `/tmp/recon-results.json` will contain a `summary` section together
with an array of per-check results that can be archived or consumed by other
systems.

## Configuration model

Each table may define a `reconciliation` block containing a `checks` list.
Global defaults can be declared under `runtime.reconciliation_defaults.checks`
and are merged with table specific definitions.

```jsonc
{
  "runtime": {
    "reconciliation_defaults": {
      "checks": [
        { "type": "plain_count", "threshold": { "percent": 1.0 } },
        { "type": "pk_uniqueness" }
      ]
    }
  },
  "tables": [
    {
      "schema": "itcftth",
      "table": "srv_cfsver_ct_main",
      "...": "...",
      "reconciliation": {
        "checks": [
          {
            "type": "plain_count",
            "name": "daily-count",
            "threshold": { "absolute": 100, "percent": 0.5 },
            "filter": "commit_date >= date_sub(current_date(), 30)"
          }
        ]
      }
    }
  ]
}
```

Every check entry supports:

- `type` – check implementation (currently `plain_count`).
- `name` – optional friendly identifier; defaults to the type.
- `enabled` – set to `false` to skip without removing the definition.
- `filter` – optional SQL predicate applied to both source and target before
  evaluating the metric.
- `threshold.absolute` – allowed absolute difference (default `0`).
- `threshold.percent` – allowed percentage difference (`source` is the
  denominator). Only applied when the source count is non-zero.
- `group_by` – optional array of SQL expressions to group results (e.g.
  `"date_trunc('month', COMMIT_DATE)"`).
- `pk_uniqueness` checks validate that each configured primary key combination
  appears once in both source and target.

To compare distribution per month (matching the partition transform shown in the
table config above), add a grouped check:

```jsonc
{
  "runtime": {
    "reconciliation_defaults": {
      "checks": [
        {
          "type": "group_count",
          "group_by": ["date_trunc('month', COMMIT_DATE)"]
        }
      ]
    }
  }
}
```

Checks defined under `runtime.reconciliation_defaults` are evaluated before
table-level entries; duplicates are not automatically de-duplicated to keep the
configuration explicit.

Reconciliation relies on endpoints that implement the shared query-plan
capability. The JDBC source endpoints and the Iceberg sink endpoint already
support this API; adding it to new connectors lets recon reuse them without
referencing engine-specific code.

Parallel execution is controlled via `runtime.reconciliation_defaults.max_parallel`
(and can be overridden with `--max-parallel`). When using Spark the framework
falls back to sequential execution because a single Spark session cannot safely
run multiple SQL jobs concurrently from different threads.

When using the SQLAlchemy engine, provide a `runtime.sqlalchemy` block with at
least a `url` (e.g. `"sqlite:///tmp/recon.db"`). The same JDBC-oriented table
definitions are reused; endpoints translate the reconciliation query into SQL
expressions understood by SQLAlchemy.

## Extensibility

The reconciliation engine is designed to grow with new check types.  Each check
implements the `ReconCheck` interface and registers itself with the check
registry.  Upcoming additions will include grouped distribution checks,
incremental window checks, and aggregate metric comparisons.

To add a custom check, register the implementation with
`recon.checks.registry.register` and reference its `type` in table
configuration.  The engine will instantiate it automatically during the run.
