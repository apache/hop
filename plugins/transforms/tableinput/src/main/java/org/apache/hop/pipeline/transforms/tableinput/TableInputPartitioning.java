/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.tableinput;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.IVariables;

/**
 * Shared logic for the parallel (partitioned) JDBC read of {@link TableInputMeta}.
 *
 * <p>The same four metadata properties ({@code jdbc_partition_column}, {@code jdbc_lower_bound},
 * {@code jdbc_upper_bound}, {@code jdbc_num_partitions}) drive the partitioned read on both
 * engines:
 *
 * <ul>
 *   <li>the classic Hop engine reads one disjoint range (slice) per transform copy;
 *   <li>the native Spark engine reads one disjoint range per Spark task through {@code
 *       spark.read.jdbc}.
 * </ul>
 *
 * <p>This class mirrors the partition arithmetic of the Apache Spark JDBC data source (Spark 4.1,
 * {@code org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation#columnPartition}) exactly, so
 * the two engines read exactly the same rows when given the same metadata. Partitioning only fires
 * when all four properties are set and resolvable; any missing or invalid property falls back to
 * the single full read (one partition, no predicates).
 */
public final class TableInputPartitioning {

  /** Subquery alias used as the FROM clause table name for a partitioned read. */
  public static final String SUBQUERY_ALIAS = "hop_part";

  private TableInputPartitioning() {}

  /** Effective partition count, or 0 when not configured / not resolvable to a positive number. */
  public static int resolveNumPartitions(TableInputMeta meta, IVariables variables) {
    String num = meta.getJdbcNumPartitions();
    if (num == null) {
      return 0;
    }
    num = variables.resolve(num).trim();
    if (num.isEmpty()) {
      return 0;
    }
    try {
      return Math.max(Integer.parseInt(num), 0);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /** Whether any partition option has been provided, even if incomplete. */
  public static boolean isPartitioningRequested(TableInputMeta meta) {
    return java.util.stream.Stream.of(
            meta.getJdbcPartitionColumn(),
            meta.getJdbcLowerBound(),
            meta.getJdbcUpperBound(),
            meta.getJdbcNumPartitions())
        .anyMatch(value -> value != null && !value.trim().isEmpty());
  }

  /** Whether the partitioned read should be used: all four properties set and resolvable. */
  public static boolean isPartitioningConfigured(TableInputMeta meta, IVariables variables) {
    if (meta.getJdbcPartitionColumn() == null
        || meta.getJdbcLowerBound() == null
        || meta.getJdbcUpperBound() == null) {
      return false;
    }
    String column = variables.resolve(meta.getJdbcPartitionColumn()).trim();
    String lower = variables.resolve(meta.getJdbcLowerBound()).trim();
    String upper = variables.resolve(meta.getJdbcUpperBound()).trim();
    return !column.isEmpty()
        && !lower.isEmpty()
        && !upper.isEmpty()
        && resolveNumPartitions(meta, variables) > 0;
  }

  /**
   * Computes the slice for {@code sliceIndex} (transform copy number on the classic engine, Spark
   * partition id on the native engine). Returns {@code null} when partitioning is not configured.
   *
   * @throws NumberFormatException when a bound does not parse as a long
   */
  public static TableInputPartitionSlice resolveSlice(
      TableInputMeta meta, IVariables variables, DatabaseMeta databaseMeta, int sliceIndex)
      throws NumberFormatException {
    if (!isPartitioningConfigured(meta, variables)) {
      return null;
    }
    String column = variables.resolve(meta.getJdbcPartitionColumn()).trim();
    long lower = parseBound(variables.resolve(meta.getJdbcLowerBound()));
    long upper = parseBound(variables.resolve(meta.getJdbcUpperBound()));
    int numPartitions = resolveNumPartitions(meta, variables);
    if (column.isEmpty() || numPartitions <= 0) {
      return null;
    }
    return partition(column, lower, upper, numPartitions, sliceIndex, databaseMeta);
  }

  /** Parses a bound as a long. */
  public static long parseBound(String value) throws NumberFormatException {
    String v = value.trim();
    if (v.isEmpty()) {
      throw new NumberFormatException("Empty bound");
    }
    return Long.parseLong(v);
  }

  /**
   * Exact port of {@code JDBCRelation#columnPartition} arithmetic for one partition: returns the
   * per-partition {@code WHERE} clause (already dialect-qualified) and the subquery text.
   *
   * <p>Slices are disjoint ranges {@code [lower+i*stride, lower+(i+1)*stride)} of the partition
   * column, with Spark's stride alignment, partition-count reduction, a null-inclusive FIRST
   * partition and an unbounded LAST partition. When {@code sliceIndex} is outside {@code [0,
   * effectiveNumPartitions)} the slice is an empty range (reads no rows), so extra transform copies
   * never duplicate rows.
   */
  static TableInputPartitionSlice partition(
      String column,
      long lowerBound,
      long upperBound,
      int numPartitions,
      int sliceIndex,
      DatabaseMeta databaseMeta) {

    String qualified = databaseMeta == null ? column : databaseMeta.quoteField(column);

    if (numPartitions <= 1 || lowerBound == upperBound) {
      if (sliceIndex != 0) {
        return new TableInputPartitionSlice(
            "1 = 0", column, qualified, 1, databaseMeta, sliceIndex, lowerBound, upperBound);
      }
      // single partition: no predicates (Spark fast path)
      return new TableInputPartitionSlice(
          null, column, qualified, numPartitions, databaseMeta, sliceIndex, lowerBound, upperBound);
    }
    if (lowerBound > upperBound) {
      throw new IllegalArgumentException(
          "Operation not allowed: the lower bound of partitioning column is larger than the upper "
              + "bound. Lower bound: "
              + lowerBound
              + "; Upper bound: "
              + upperBound);
    }

    long range = upperBound - lowerBound; // may overflow; handles Lower=MIN/Upper=MAX
    int effectiveNumPartitions =
        (range >= numPartitions || range < 0) ? numPartitions : (int) range;

    BigDecimal upperStride =
        BigDecimal.valueOf(upperBound)
            .divide(BigDecimal.valueOf(effectiveNumPartitions), 18, RoundingMode.HALF_EVEN);
    BigDecimal lowerStride =
        BigDecimal.valueOf(lowerBound)
            .divide(BigDecimal.valueOf(effectiveNumPartitions), 18, RoundingMode.HALF_EVEN);
    BigDecimal preciseStride = upperStride.subtract(lowerStride);
    long stride = preciseStride.longValue();

    BigDecimal lost =
        preciseStride
            .subtract(BigDecimal.valueOf(stride))
            .multiply(BigDecimal.valueOf(effectiveNumPartitions))
            .divide(BigDecimal.valueOf(stride), 0, RoundingMode.HALF_UP);
    long lostNumOfStrides = lost.longValue();
    long lowerWithAlignment = lowerBound + ((lostNumOfStrides / 2) * stride);

    String whereClause;
    if (sliceIndex < 0 || sliceIndex >= effectiveNumPartitions) {
      // out-of-range copy: empty range so it never duplicates rows
      long x = lowerWithAlignment + (long) effectiveNumPartitions * stride;
      whereClause = qualified + " >= " + x + " AND " + qualified + " < " + x;
    } else {
      long sliceLower = lowerWithAlignment + (long) sliceIndex * stride;
      long sliceUpper = sliceLower + stride;
      String lBound = sliceIndex != 0 ? qualified + " >= " + sliceLower : null;
      String uBound =
          sliceIndex != effectiveNumPartitions - 1 ? qualified + " < " + sliceUpper : null;
      if (uBound == null) {
        whereClause = lBound; // last partition: unbounded upper
      } else if (lBound == null) {
        whereClause = uBound + " or " + qualified + " is null"; // first partition: nulls included
      } else {
        whereClause = lBound + " AND " + uBound;
      }
    }

    return new TableInputPartitionSlice(
        whereClause,
        column,
        qualified,
        effectiveNumPartitions,
        databaseMeta,
        sliceIndex,
        lowerBound,
        upperBound);
  }

  /** Removes trailing semicolons and whitespace so the subquery stays a single statement. */
  public static String trimSql(String sql) {
    if (sql == null) {
      return null;
    }
    String trimmed = sql.trim();
    while (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }
    return trimmed;
  }

  /** One resolved slice of the partitioned read. */
  public static final class TableInputPartitionSlice {
    private final String whereClause;
    private final String partitionColumn;
    private final String qualifiedColumn;
    private final int numPartitions;
    private final DatabaseMeta databaseMeta;
    private final int index;
    private final long lower;
    private final long upper;

    TableInputPartitionSlice(
        String whereClause,
        String partitionColumn,
        String qualifiedColumn,
        int numPartitions,
        DatabaseMeta databaseMeta,
        int index,
        long lower,
        long upper) {
      this.whereClause = whereClause;
      this.partitionColumn = partitionColumn;
      this.qualifiedColumn = qualifiedColumn;
      this.numPartitions = numPartitions;
      this.databaseMeta = databaseMeta;
      this.index = index;
      this.lower = lower;
      this.upper = upper;
    }

    public String getWhereClause() {
      return whereClause;
    }

    public String getPartitionColumn() {
      return partitionColumn;
    }

    /** The prescribed number of partitions before count reduction (from jdbc_num_partitions). */
    public int getNumPartitions() {
      return numPartitions;
    }

    public DatabaseMeta getDatabaseMeta() {
      return databaseMeta;
    }

    public int getIndex() {
      return index;
    }

    /** The configured lower bound (as resolved). */
    public long getLower() {
      return lower;
    }

    /** The configured upper bound (as resolved). */
    public long getUpper() {
      return upper;
    }

    /**
     * Effective query for the classic engine: {@code SELECT * FROM (userSql) hop_part} plus the
     * slice {@code WHERE} clause when partitioning applies. The user SQL is wrapped at query time
     * (variables may still need resolution), so it is passed in here. The outer SELECT is required:
     * some databases (H2) reject a bare derived table as the whole statement.
     */
    public String buildQuery(String userSql) {
      String base = "SELECT * FROM ( " + userSql + " ) " + SUBQUERY_ALIAS;
      return whereClause == null ? base : base + " WHERE " + whereClause;
    }

    /**
     * The {@code dbtable} value for {@code spark.read.jdbc}: {@code (userSql) hop_part}. Spark's
     * own partitioner adds the per-task WHERE predicates (it needs {@code partitionColumn} to
     * resolve against the subquery output schema, so the partition column must appear in the SELECT
     * list). The partition column is also listed as a column alias so the subquery output exposes
     * it under its own name.
     */
    public String buildSparkDbtable(String userSql) {
      return "( " + userSql + " ) " + SUBQUERY_ALIAS;
    }
  }
}
