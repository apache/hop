# Hop Beam Spark *matched* client/cluster matrix

Generated: pending first full matched run

- Hop: `2.19.0-SNAPSHOT` · Beam: `2.74.0`
- Each cell: **Spark client pack V** = **cluster V** (Java 21 driver + cluster)
- Suite: `main-0001-test-spark-cluster`

## Results

| Client pack | Cluster | Result | Notes |
| --- | --- | --- | --- |
| 3.5.7 | 3.5.7 | **PASS** | Default pack; measured in `MATRIX-REPORT.md` |
| 3.4.4 | 3.4.4 | *not run yet* | Pack + fat jar verified (`version=3.4.4` in fat jar); cluster IT pending |
| 3.3.4 | 3.3.4 | *not run yet* | — |

## How to run

```bash
# Build client, then:
./integration-tests/scripts/run-spark-matched-matrix.sh KEEP_IMAGES=true

# Or a single matched pair:
./tools/spark-client-pack/materialize-pack.sh 3.4.4 assemblies/client/target/hop
export HOP_SPARK_CLIENT_VERSION=3.4.4 SPARK_VERSION=3.4.4
# rebuild hop-base + hop-beam images, then:
./integration-tests/scripts/run-tests-docker.sh PROJECT_NAME=spark \
  SPARK_VERSION=3.4.4 HOP_SPARK_CLIENT_VERSION=3.4.4 KEEP_IMAGES=true
```

## Tooling

| Piece | Path |
| --- | --- |
| Default pack (install) | `lib/spark-client/` |
| Versioned packs | `lib/spark-clients/<version>/` |
| Materialise pack | `tools/spark-client-pack/materialize-pack.sh` |
| Fat jar flag | `hop-conf.sh --spark-client-version=V` |
| Driver env | `HOP_SPARK_CLIENT_VERSION=V` |
