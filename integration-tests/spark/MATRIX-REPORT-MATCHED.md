# Hop Beam Spark *matched* client/cluster matrix

Generated: 2026-07-10 (issue #7476)

> **Pin moved 3.5.7 → 3.5.8** (current patch on `dlcdn.apache.org`; 3.5.7 is now archive-only
> and throttled). Same 3.5.x line, same conclusion. The measured row still shows the last
> **3.5.7** run — re-run `run-spark-matched-matrix.sh` to refresh it for 3.5.8.

- Hop: `2.19.0-SNAPSHOT` · Beam: `2.74.0`
- Each cell: **Spark client pack V** = **cluster V** (Java 21 driver + cluster)
- Suite: `main-0001-test-spark-cluster`

## Results

| Client pack | Cluster | Result | Notes |
| --- | --- | --- | --- |
| 3.5.7 | 3.5.7 | **PASS** | Pack activated as `lib/spark-client`; smoke finished |
| 3.4.4 | 3.4.4 | **FAIL** | `NoSuchMethodException: java.nio.DirectByteBuffer.<init>(long,int)` — [SPARK-42369](https://issues.apache.org/jira/browse/SPARK-42369) fixed only from **Spark 3.5.0** |

## Interpretation

Matching the client pack to the cluster **eliminates RPC serialVersionUID skew**, but **cannot** make Spark 3.4 run on **Java 21**. Hop requires Java 21 for driver and workers (fat-jar bytecode), so **Spark ≥ 3.5.0 is the platform floor**.

Versioned packs remain valuable for:

- Aligning Hop’s client jars with a **3.5.x** cluster patch level
- Future matrix cells within 3.5.x (e.g. 3.5.7 vs 3.5.8)

They are **not** a path back to Spark 3.4 / 3.3 / 3.2 while Hop stays on Java 21.

## How to run

```bash
./integration-tests/scripts/run-spark-matched-matrix.sh KEEP_IMAGES=true
```

## Tooling

| Piece | Path |
| --- | --- |
| Default pack (install) | `lib/spark-client/` |
| Versioned packs | `lib/spark-clients/<version>/` |
| Materialise pack | `tools/spark-client-pack/materialize-pack.sh` |
| Fat jar flag | `hop-conf.sh --spark-client-version=V` |
| Driver env | `HOP_SPARK_CLIENT_VERSION=V` |
