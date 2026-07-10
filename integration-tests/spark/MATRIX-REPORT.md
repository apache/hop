# Hop Beam Spark support matrix

Generated: 2026-07-10T16:30:00Z (measured on `issue-7476`)

- Hop: `2.19.0-SNAPSHOT`
- Beam: `2.74.0`
- Client Spark libraries (`spark.version`): `3.5.7` (scala 2.12)
- Driver JVM: Java 21 (Hop requirement)
- Cluster JVM: Java 21 (matches Hop bytecode in fat jar)
- Suite: `integration-tests/spark` smoke (`main-0001-test-spark-cluster`)
- Mode: Hop client submit (`BeamSparkPipelineEngine` → `spark://spark:7077` + fat jar)

## Results

| Spark version | Cluster binary | Result | Notes |
| --- | --- | --- | --- |
| 3.2.4 | `bin-hadoop3.2` | **FAIL** | Spark master does not start on Java 21 (`NoSuchMethodException: DirectByteBuffer.<init>(long,int)`) |
| 3.3.4 | `bin-hadoop3` | **FAIL** | Client Spark 3.5.7 cannot register with cluster 3.3.4 (`InvalidClassException` / `ApplicationDescription` serialVersionUID mismatch) → stopped SparkContext |
| 3.4.4 | `bin-hadoop3` | **FAIL** | Executor does not start on Java 21 (`NoSuchMethodException: DirectByteBuffer.<init>(long,int)`) |
| 3.5.7 | `bin-hadoop3` | **PASS** | Smoke pipeline finished (`main-0001-test-spark-cluster`) |

## Interpretation

1. **Hop always uses Java 21.** Spark standalone processes that execute Hop fat-jar bytecode must also run Java 21. That rules out Spark lines that cannot boot on Java 21 (observed: 3.2.x master, 3.4.x executor with the stock distribution + Temurin 21).
2. **Client-mode version lock.** Hop ships `spark-core_2.12` / `spark-streaming_2.12` at `spark.version` (**3.5.7**). Talking to a cluster on a different Spark **minor** fails at RPC serialization (`InvalidClassException`). For GUI / hop-run client submit, the cluster must be **Spark 3.5.x** (matching the client libraries).
3. **Practical support line today:** **Spark 3.5.x** (validated **3.5.7**) with Beam **2.74.0** and Hop **2.19** on **Java 21**.
4. Older Spark lines are **not** supported for Hop client-mode under Java 21 without a different deployment model (e.g. spark-submit with cluster-provided Spark and no client spark-core skew) — that path is not covered by this matrix.

## How to re-run

```bash
# Full matrix (default: 3.2.4 3.3.4 3.4.4 3.5.7)
./integration-tests/scripts/run-spark-matrix.sh KEEP_IMAGES=true

# Single version
SPARK_VERSION=3.5.7 ./integration-tests/scripts/run-tests-docker.sh PROJECT_NAME=spark KEEP_IMAGES=true

# Subset
SPARK_VERSIONS="3.5.7 3.4.4" ./integration-tests/scripts/run-spark-matrix.sh KEEP_IMAGES=true
```

## Notes

- Beam upstream docs still list Spark 3.2.x as the supported line; this matrix is **empirical** for Hop pipelines on Beam 2.74.0.
- Hive catalog workflows are deferred (`optional-0002/0003-*.hwf`) and not part of this matrix.
- Spark 3.2.x archive artifacts use `bin-hadoop3.2`; 3.3+ use `bin-hadoop3` (handled by `run-spark-matrix.sh`).
