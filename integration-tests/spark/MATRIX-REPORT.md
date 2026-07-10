# Hop Beam Spark support matrix

Generated: 2026-07-10 (issue #7476)

- Hop: `2.19.0-SNAPSHOT`
- Beam: `2.74.0`
- Default client Spark libraries (`spark.version`): `3.5.7` (scala 2.12)
- Driver JVM: **Java 21** (Hop requirement)
- Cluster JVM: **Java 21** (workers must load Hop fat-jar bytecode)
- Suite: `integration-tests/spark` smoke (`main-0001-test-spark-cluster`)
- Mode: Hop client submit (`BeamSparkPipelineEngine` → `spark://spark:7077` + fat jar)

## Results (fixed client 3.5.7 vs varying cluster)

| Spark version | Cluster binary | Result | Notes |
| --- | --- | --- | --- |
| 3.2.4 | `bin-hadoop3.2` | **FAIL** | Master does not start on Java 21 (`DirectByteBuffer.<init>(long,int)`) |
| 3.3.4 | `bin-hadoop3` | **FAIL** | Client/cluster skew (`InvalidClassException` / serialVersionUID) |
| 3.4.4 | `bin-hadoop3` | **FAIL** | Executor does not start on Java 21 (`DirectByteBuffer.<init>(long,int)`) |
| 3.5.7 | `bin-hadoop3` | **PASS** | Smoke pipeline finished |

## Results (matched client pack V + cluster V)

| Client pack | Cluster | Result | Notes |
| --- | --- | --- | --- |
| 3.5.7 | 3.5.7 | **PASS** | After driver classpath / pack wiring fix |
| 3.4.4 | 3.4.4 | **FAIL** | Same Java 21 `DirectByteBuffer` failure — matching client pack does **not** help |
| 3.3.4 | 3.3.4 | *(optional)* | Not required once 3.4 matched FAIL is explained by Java 21 |

## Conclusion

**Hop on Java 21 is effectively limited to Spark 3.5.x** for Beam client-mode submit.

1. **Java 21 floor (dominant):** Spark’s unsafe `DirectByteBuffer` reflection broke on JDK 21 until [SPARK-42369](https://issues.apache.org/jira/browse/SPARK-42369) (*Fix constructor for java.nio.DirectByteBuffer for Java 21+*), fixed in **Spark 3.5.0**. Hop requires Java 21 for the driver and for fat-jar workers, so the cluster must also run Java 21. Spark **3.4.x and earlier cannot be used** on that stack, even with a matching client pack / fat jar.
2. **Client/cluster match:** When both sides are on 3.5.x, client libraries and cluster minor must still align (default pack `3.5.7` with cluster `3.5.7` validated). Versioned packs (`tools/spark-client-pack`, `HOP_SPARK_CLIENT_VERSION`) remain useful for **3.5.x patch** alignment, not for going back to 3.4.
3. **Supported line:** **Spark 3.5.x** (validated **3.5.7**) + Beam **2.74.0** + Hop **2.19** + **Java 21**.

## How to re-run

```bash
# Default pin (recommended CI smoke)
SPARK_VERSION=3.5.7 ./integration-tests/scripts/run-tests-docker.sh PROJECT_NAME=spark KEEP_IMAGES=true

# Fixed client vs varying cluster
./integration-tests/scripts/run-spark-matrix.sh KEEP_IMAGES=true

# Matched packs (documents Java 21 ceiling on pre-3.5)
./integration-tests/scripts/run-spark-matched-matrix.sh KEEP_IMAGES=true
```

## Notes

- Beam upstream docs may still mention Spark 3.2.x; that is not Hop’s measured support on Java 21.
- Hive catalog workflows remain deferred (`optional-0002/0003-*.hwf`).
