<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Hop OpenLineage Lineage Sink

Apache Hop plugin that implements `ILineageSink` and forwards Hop lineage observations to an
[OpenLineage](https://openlineage.io/) collector. Primary target:
[Marquez](https://marquezproject.github.io/marquez/).

Relies on the lineage hub SPI in `hop-engine` (`org.apache.hop.lineage.spi.ILineageSink`).

> **User documentation** lives in the Hop user manual under *Technology → OpenLineage*, including
> the **dataset identity** rules another lineage producer must match to stitch into Hop's graph.
> This file covers building and hacking on the plugin itself.

## Build

Built as part of the standard Hop reactor. To build and test just this plugin:

```bash
./mvnw -pl plugins/tech/openlineage clean test
```

The plugin is **marketplace-optional**: it is not in the default Hop client and is installed from
the marketplace (or from `target/hop-tech-openlineage-*.zip`) into `plugins/tech/openlineage`.

```
plugins/tech/openlineage/hop-tech-openlineage-*.jar
plugins/tech/openlineage/lib/openlineage-java-*.jar
plugins/tech/openlineage/lib/openlineage-sql-java-*.jar
plugins/tech/openlineage/lib/{jackson-datatype-*,httpclient5,micrometer,...}-*.jar
```

OpenLineage JSON is built and delivered with the official
[`io.openlineage:openlineage-java`](https://github.com/OpenLineage/OpenLineage) client (Apache 2.0),
so the emitted events are spec-correct by construction. Jackson core/databind/annotations,
commons-lang3 and slf4j are provided by the Hop runtime; the client's remaining runtime
dependencies are bundled into `lib/`.

`openlineage-sql-java` carries prebuilt native libraries for every supported platform, which is why
the packaged plugin is ~24 MB. That is the main reason it ships as an optional plugin rather than
in the default client.

## Enable in Hop

Set these **ENGINE-scoped** variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `HOP_LINEAGE_ENABLED` | `N` | Set to `Y` to enable the lineage hub |
| `HOP_LINEAGE_SINK_IDS` | *(empty)* | Set to `openlineage` to load only this sink |
| `HOP_LINEAGE_QUEUE_CAPACITY` | `10000` | In-memory event queue size |
| `HOP_LINEAGE_BATCH_MAX` | `100` | Max events per sink batch |
| `HOP_LINEAGE_BATCH_LINGER_MS` | `250` | Batch linger time (ms) |

### Plugin variables (read in `init()`)

| Variable | Default | Description |
|----------|---------|-------------|
| `HOP_LINEAGE_OPENLINEAGE_URL` | *(required)* | Collector URL, e.g. `http://marquez-api:5000/api/v1/lineage` |
| `HOP_LINEAGE_OPENLINEAGE_NAMESPACE` | `hop` | OpenLineage **job** namespace. This is *not* the dataset namespace — see the dataset identity page |
| `HOP_LINEAGE_OPENLINEAGE_PRODUCER` | apache/hop plugin URL | OpenLineage `producer` field |
| `HOP_LINEAGE_OPENLINEAGE_API_KEY` | *(empty)* | Optional bearer token sent as `Authorization: Bearer …` |
| `HOP_LINEAGE_OPENLINEAGE_CONNECT_TIMEOUT_MS` | `5000` | HTTP connect timeout |
| `HOP_LINEAGE_OPENLINEAGE_READ_TIMEOUT_MS` | `30000` | HTTP read/response timeout |
| `HOP_LINEAGE_OPENLINEAGE_BUFFER_SIZE` | `10000` | Async outbound queue capacity |
| `HOP_LINEAGE_OPENLINEAGE_MAX_RETRIES` | `3` | Retries per event on a *transient* failure (exponential backoff) |
| `HOP_LINEAGE_OPENLINEAGE_RETRY_BACKOFF_MS` | `500` | Initial retry backoff; doubles each attempt |
| `HOP_LINEAGE_OPENLINEAGE_OVERFLOW_POLICY` | `DROP` | `DROP` = drop and count when the buffer is full; `BLOCK` = wait for capacity (bounded, see below) |
| `HOP_LINEAGE_OPENLINEAGE_ENQUEUE_TIMEOUT_MS` | `5000` | Under `BLOCK`, how long to wait for capacity before dropping |
| `HOP_LINEAGE_OPENLINEAGE_SHUTDOWN_DRAIN_MS` | `30000` | Max time to drain the queue on shutdown |
| `HOP_LINEAGE_OPENLINEAGE_METRICS_INTERVAL_MS` | `0` | If > 0, log `sent/failed/dropped/queued` counts at this interval (`0` = off) |
| `HOP_LINEAGE_OPENLINEAGE_TRUSTSTORE_PATH` | *(empty)* | Path to a trust/key store for HTTPS collectors with a custom CA/cert |
| `HOP_LINEAGE_OPENLINEAGE_TRUSTSTORE_PASSWORD` | *(empty)* | Store password (used with the trust store path) |
| `HOP_LINEAGE_OPENLINEAGE_KEYSTORE_TYPE` | `PKCS12` | Store type for the trust store (e.g. `PKCS12`, `JKS`) |
| `HOP_LINEAGE_OPENLINEAGE_SQL_PARSE` | `Y` | Parse Table Input / Execute SQL statements to recover their source/target tables |
| `HOP_LINEAGE_OPENLINEAGE_FILE_SPEC_NAMING` | `N` | Name file datasets per the OpenLineage naming spec. Off by default because it re-keys existing file datasets |

> **Proxy:** the client has no proxy setting; route through a forward proxy with the JVM
> properties `-Dhttps.proxyHost`/`-Dhttps.proxyPort` (or `http.*`) instead.

## Delivery model & limitations

- Events are buffered in an **in-memory** queue and POSTed by a **single worker thread**. This
  preserves run-event ordering (`START` before `COMPLETE`) — the reason delivery is not
  parallelised — and is the throughput ceiling of the sink.
- **Lineage never becomes a participant in the pipeline.** The engine-side emitters swallow their
  own failures, and the outbound queue **drops by default** rather than blocking. The caller of
  `enqueueAll` is the lineage hub's single dispatcher thread, and a finishing pipeline waits on a
  flush marker that same thread has to process — so blocking there for an unreachable collector
  would delay every pipeline completion, not just lineage. `BLOCK` remains available for
  deployments that would rather lose throughput than lose lineage, and is itself bounded by
  `…_ENQUEUE_TIMEOUT_MS`.
- On a hard JVM crash, undelivered events in the queue are **lost** (there is no on-disk spool).
  Normal shutdown drains the queue up to `…_SHUTDOWN_DRAIN_MS`.
- Transient failures (connection errors, 5xx, 408, 429) are retried. A 4xx rejection is **not**
  retried — the collector understood the request and refused it, so resending only spends the
  backoff. Persistent failures are logged and counted.

## Event mapping

| Hop `LineageEventKind` | OpenLineage output |
|------------------------|-------------------|
| `RUN_LIFECYCLE` | `RunEvent` — `START` / `COMPLETE` / `FAIL` |
| `FILE_IO` | `RunEvent` (`OTHER`) with physical file datasets (`dataSource.uri` + `schema` when Hop provides `FileIoContentSchema`), correlated to the **pipeline** run |
| `RELATIONAL_IO` | `RunEvent` (`OTHER`) with relational input/output datasets keyed by the database identity (`namespace` = `postgres://host:port`, `name` = `database.schema.table`), plus `schema`, `columnLineage` and `lifecycleStateChange` facets where known, and a `SQLJobFacet` for SQL-bearing statements |
| `TRANSFORM_SCHEMA` | Skipped (Hop internal row shapes between transforms, not physical datasets) |
| `HTTP_IO` | Skipped (debug log only) |

Run id comes from the subject `logChannelId` when present. Job name from `pipelineName` or
`workflowName`; transform subjects use `pipelineName/transformName` so they do not collide with the
pipeline run in Marquez.

**Parent/child runs:** transform run events carry a `ParentRunFacet` pointing at their pipeline run,
and action run events point at their workflow run, so per-transform jobs attach to the pipeline run
in the lineage graph instead of appearing as disconnected jobs.

### Which transforms produce `RELATIONAL_IO`

Transforms do not emit these events themselves. A transform's metadata **declares** its table access
via `@RelationalLineage` plus the `RDBMS_*` property annotations, and the engine derives the event
when the transform finishes (see the *Lineage* page of the developer manual). Covered today:

| Transform | Operation | Notes |
|-----------|-----------|-------|
| Table Output | write | `OVERWRITE` lifecycle when truncating. Per-row dynamic table names are reported by the transform itself, since no annotation can describe them |
| Insert/Update, Update | write | Key columns + updated value columns |
| Delete | delete | Affected table only — a delete produces no columns, so no schema facet |
| Combination Lookup/Update | write | Key columns; the generated technical key has no stream source and is omitted |
| Dimension Lookup/Update | write | Natural keys + attribute columns; generated columns (technical key, version, validity dates) are omitted |
| PostgreSQL, MySQL, Oracle, Vertica, CrateDB, MonetDB, Redshift, Snowflake bulk loaders | write | Explicit field mapping; `OVERWRITE` where the loader truncates or replaces |
| Table Input | read | Source tables recovered by parsing the `SELECT` |
| Execute SQL | exec | Source and target tables recovered by parsing the statement |

Adding a database transform to this list is a matter of annotating its metadata; there is no
emission code to write.

### Dataset identity

The `(namespace, name)` rules — and the naming another producer must match — are documented in the
user manual under *Technology → OpenLineage → Dataset identity*
(`docs/hop-user-manual/modules/ROOT/pages/technology/openlineage/dataset-identity.adoc`). Read it
before integrating a second lineage producer. It also records two known gaps: Snowflake/BigQuery
namespaces, and identifier casing.

### SQL parsing

Table Output knows its target table directly, but **Table Input** and **Execute SQL** only provide a
SQL statement. The engine therefore emits `RELATIONAL_IO` events carrying the raw SQL, and this sink
recovers the tables by parsing it with the
[OpenLineage SQL parser](https://github.com/OpenLineage/OpenLineage/tree/main/integration/sql)
(`io.openlineage:openlineage-sql-java`, a Rust core reached over JNI). The parser handles JOINs,
CTEs, subqueries and `INSERT … SELECT`, and the parsed `database.schema.table` is fed through the
same `LineageRelationalIdentity.datasetName` used for writes, so a read and a write of the same
table produce the identical dataset node.

- The native libraries are **bundled per-platform inside the jar**; no separate install.
- **Catalog qualification:** a `SELECT ... FROM schema.table` carries no catalog segment (SQL cannot
  name it), so the sink fills it from the connection's catalog (`RELATIONAL_IO.defaultCatalog`, i.e.
  `DatabaseMeta.getDatabaseName()`), and likewise its preferred schema.
- Parsing is **best-effort**: an unavailable native library, a statement the parser rejects, or a
  bare table with no schema simply yields no (or coarser) lineage — it never fails the read.
- Toggle with `HOP_LINEAGE_OPENLINEAGE_SQL_PARSE` (default `Y`).

### Column-level lineage

Emitted as the OpenLineage `columnLineage` dataset facet, from two sources:

1. **Parsed SQL** — `INSERT … SELECT` in Execute SQL yields per-field edges directly from the
   parser.
2. **The pipeline stream path** — a read registers how each of its output stream fields maps back to
   a source table column; a write resolves each target column through the transform that produced
   its stream field (`IValueMeta.getOrigin()`) to the matching read. This traces through
   pass-through transforms, and is deliberately conservative: a column whose origin is not a
   registered read (a computed field, a join with no resolvable source) yields **no** edge rather
   than a guess.

### Known gap: file column schema

Marquez **Fields** on a file dataset come from the OpenLineage `schema` facet on `FILE_IO` events.
The sink maps `FileIoLineagePayload.contentSchema` when present, but
`org.apache.hop.pipeline.transforms.file.BaseFileInputTransform` still emits file reads with
`contentSchema = null`, so delimited/CSV text inputs show **0 columns**. JSON/XML/YAML file
transforms pass a content schema of their own and are unaffected. Fixing this is an engine change
(build a `FileIoContentSchema` from `data.outputRowMeta` in `emitLineageFileRead`), not a plugin
change.

## Tests

```bash
./mvnw -pl plugins/tech/openlineage test
```

- `OpenLineageEventMapperTest` — the event → OpenLineage mapping, including the dataset identity
  rules and column lineage
- `OpenLineageHttpClientTest` — which delivery failures are retried and which are not
- `OpenLineageAsyncDispatcherTest` — overflow policy (bounded `BLOCK`, counted `DROP`) and drain
- `OpenLineageSinkTest` — the public `ILineageSink` contract (init/accept/shutdown) against a
  WireMock collector: delivery, exact request body, async non-blocking `accept`, retry on a
  transient `503`
- `RelationalSqlParserTest`, `RelationalColumnLineageCorrelatorTest` — table and column recovery

End-to-end coverage against a real Marquez lives in `integration-tests/openlineage` and runs with
`docker/integration-tests/integration-tests-openlineage.yaml`.

## License

Apache License 2.0 (same as Apache Hop).
