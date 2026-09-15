# dbt integration tests

Exercises the `dbt` workflow action end to end against a real dbt Core installation, a real
warehouse and a real OpenLineage collector.

| Workflow | What it proves |
| --- | --- |
| `main-0001-dbt-warehouse.hwf` | Hop loads `public.orders_source`, the action runs `dbt run` and `dbt test` against it, and the model dbt built is verified in the warehouse. Covers starting dbt, passing the warehouse credentials through the process environment (`profiles.yml` reads them with `env_var()`, they are nowhere on disk), passing a typed `--vars` value, and reading `run_results.json` back. |
| `main-0002-dbt-test-failure.hwf` | A failing dbt test fails the action, so the workflow follows the error hop - the data quality gate. The workflow passes *because* dbt failed; if the dbt test unexpectedly succeeds it aborts with an error. |
| `main-0003-dbt-lineage.hwf` | With **Emit OpenLineage** on, dbt's events land in the same Marquez namespace as the Hop workflow that started it, so both halves are one graph. |

## Requirements

dbt Core is a Python program and is not part of the Hop image, so the test runner installs it
before the tests start - see the `command` of `integration_test_dbt` in
`docker/integration-tests/integration-tests-dbt.yaml`. The versions are pinned there on purpose: an
unpinned install resolves dbt-core to whatever is newest, and a red nightly should mean Hop broke,
not that dbt's dependency tree moved. Bump them there when the supported dbt version changes.

`dbt-ol`, the wrapper used when lineage is enabled, starts plain `dbt` as a child process, so both
have to be on the `PATH` of the Hop process.

## Running it

```bash
./integration-tests/scripts/run-tests-docker.sh PROJECT_NAME=dbt
```

To run a single workflow against your own Postgres and dbt installation instead:

```bash
export HOP_CONFIG_FOLDER=<hop>/integration-tests/dbt
<hop-client>/hop-run.sh -e dev -r local \
  -f <hop>/integration-tests/dbt/main-0001-dbt-warehouse.hwf \
  -p POSTGRES_HOST=localhost -p POSTGRES_PORT=5432 \
  -p POSTGRES_DATABASE=hop_database -p POSTGRES_USER=hop_user -p POSTGRES_PASSWORD=hop_password
```

`main-0003` additionally needs a collector: point `HOP_LINEAGE_OPENLINEAGE_URL` at it, enable the
hub with `HOP_LINEAGE_ENABLED=Y` and `HOP_LINEAGE_SINK_IDS=openlineage`, and set `MARQUEZ_API` in
`dev-env-config.json` to a URL your machine can reach - it is the compose service name by default.

## The dbt project

`dbt-project/` is a two model dbt project on Postgres:

* `orders_by_country` aggregates `public.orders_source`, the table Hop loads. It is the handover
  the action exists for, and both sides name the table identically in the lineage graph.
* `failing_orders` is broken on purpose - its `country` is null, so the `not_null` test on it always
  fails. `main-0002` selects only that test; `main-0001` selects only the tests of the good model.
