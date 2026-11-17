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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TableInputPartitioningTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopEnvironment.init();
  }

  /** A DatabaseMeta that forces quoting, so the expected SQL is deterministic. */
  private DatabaseMeta quotingMeta() {
    DatabaseMeta meta = new DatabaseMeta();
    meta.setQuoteAllFields(true);
    return meta;
  }

  private TableInputMeta meta(int lower, int upper, int partitions) {
    TableInputMeta meta = new TableInputMeta();
    meta.setJdbcPartitionColumn("id");
    meta.setJdbcLowerBound(Integer.toString(lower));
    meta.setJdbcUpperBound(Integer.toString(upper));
    meta.setJdbcNumPartitions(Integer.toString(partitions));
    return meta;
  }

  @Test
  void notConfiguredWhenAnyFieldMissing() {
    TableInputMeta meta = new TableInputMeta();
    meta.setJdbcPartitionColumn("id");
    meta.setJdbcLowerBound("0");
    meta.setJdbcUpperBound("100");
    // num partitions missing -> off
    assertFalse(TableInputPartitioning.isPartitioningConfigured(meta, new Variables()));
  }

  @Test
  void detectsIncompletePartitionRequest() {
    TableInputMeta meta = new TableInputMeta();
    assertFalse(TableInputPartitioning.isPartitioningRequested(meta));
    meta.setJdbcPartitionColumn("id");
    assertTrue(TableInputPartitioning.isPartitioningRequested(meta));
    assertFalse(TableInputPartitioning.isPartitioningConfigured(meta, new Variables()));
  }

  @Test
  void configuredWhenAllFieldsPresent() {
    TableInputMeta meta = meta(0, 100, 4);
    assertTrue(TableInputPartitioning.isPartitioningConfigured(meta, new Variables()));
    assertEquals(4, TableInputPartitioning.resolveNumPartitions(meta, new Variables()));
  }

  @Test
  void variablesResolved() {
    TableInputMeta meta = new TableInputMeta();
    meta.setJdbcPartitionColumn("id");
    meta.setJdbcLowerBound("${lower}");
    meta.setJdbcUpperBound("${upper}");
    meta.setJdbcNumPartitions("${n}");
    Variables vars = new Variables();
    vars.setVariable("lower", "0");
    vars.setVariable("upper", "100");
    vars.setVariable("n", "4");
    assertTrue(TableInputPartitioning.isPartitioningConfigured(meta, vars));
    TableInputPartitioning.TableInputPartitionSlice slice =
        TableInputPartitioning.resolveSlice(meta, vars, quotingMeta(), 1);
    assertEquals(
        "SELECT * FROM ( SELECT * FROM t ) hop_part WHERE \"id\" >= 25 AND \"id\" < 50",
        slice.buildQuery("SELECT * FROM t"));
  }

  @Test
  void slicesMatchSparkArithmetic() {
    // expected clauses from Spark 4.1 JDBCRelation.columnPartition port validation
    String[] expected0to100n4 = {
      "\"id\" < 25 or \"id\" is null",
      "\"id\" >= 25 AND \"id\" < 50",
      "\"id\" >= 50 AND \"id\" < 75",
      "\"id\" >= 75",
    };
    for (int i = 0; i < expected0to100n4.length; i++) {
      TableInputPartitioning.TableInputPartitionSlice slice =
          TableInputPartitioning.resolveSlice(meta(0, 100, 4), new Variables(), quotingMeta(), i);
      assertEquals(expected0to100n4[i], slice.getWhereClause(), "slice " + i);
    }

    String[] expectedMinMaxn8 = {
      "\"id\" < -6917529027641081857 or \"id\" is null",
      "\"id\" >= -6917529027641081857 AND \"id\" < -4611686018427387906",
      "\"id\" >= -4611686018427387906 AND \"id\" < -2305843009213693955",
      "\"id\" >= -2305843009213693955 AND \"id\" < -4",
      "\"id\" >= -4 AND \"id\" < 2305843009213693947",
      "\"id\" >= 2305843009213693947 AND \"id\" < 4611686018427387898",
      "\"id\" >= 4611686018427387898 AND \"id\" < 6917529027641081849",
      "\"id\" >= 6917529027641081849",
    };
    TableInputMeta meta = new TableInputMeta();
    meta.setJdbcPartitionColumn("id");
    meta.setJdbcLowerBound(Long.toString(Long.MIN_VALUE));
    meta.setJdbcUpperBound(Long.toString(Long.MAX_VALUE));
    meta.setJdbcNumPartitions("8");
    for (int i = 0; i < expectedMinMaxn8.length; i++) {
      TableInputPartitioning.TableInputPartitionSlice slice =
          TableInputPartitioning.resolveSlice(meta, new Variables(), quotingMeta(), i);
      assertEquals(expectedMinMaxn8[i], slice.getWhereClause(), "slice " + i);
    }
  }

  @Test
  void partitionCountReducedWhenRangeSmaller() {
    // range 0..2 with n=10 -> Spark reduces to 2 partitions
    TableInputPartitioning.TableInputPartitionSlice s0 =
        TableInputPartitioning.resolveSlice(meta(0, 2, 10), new Variables(), quotingMeta(), 0);
    TableInputPartitioning.TableInputPartitionSlice s1 =
        TableInputPartitioning.resolveSlice(meta(0, 2, 10), new Variables(), quotingMeta(), 1);
    assertEquals("\"id\" < 1 or \"id\" is null", s0.getWhereClause());
    assertEquals("\"id\" >= 1", s1.getWhereClause());
    // out of range (index 2 >= effective 2): empty range
    TableInputPartitioning.TableInputPartitionSlice s2 =
        TableInputPartitioning.resolveSlice(meta(0, 2, 10), new Variables(), quotingMeta(), 2);
    assertTrue(s2.getWhereClause().contains("AND"));
  }

  @Test
  void singlePartitionNoPredicates() {
    TableInputPartitioning.TableInputPartitionSlice slice =
        TableInputPartitioning.resolveSlice(meta(0, 100, 1), new Variables(), quotingMeta(), 0);
    assertNull(slice.getWhereClause());
  }

  @Test
  void lowerEqualsUpperSinglePartition() {
    TableInputPartitioning.TableInputPartitionSlice slice =
        TableInputPartitioning.resolveSlice(meta(5, 5, 3), new Variables(), quotingMeta(), 0);
    assertNull(slice.getWhereClause());
  }

  @Test
  void extraCopiesAreEmptyForSingleEffectivePartition() {
    assertEquals(
        "1 = 0",
        TableInputPartitioning.resolveSlice(meta(5, 5, 3), new Variables(), quotingMeta(), 1)
            .getWhereClause());
    assertEquals(
        "1 = 0",
        TableInputPartitioning.resolveSlice(meta(0, 100, 1), new Variables(), quotingMeta(), 1)
            .getWhereClause());
  }

  @Test
  void buildQueryWrapsUserSql() {
    TableInputPartitioning.TableInputPartitionSlice slice =
        TableInputPartitioning.resolveSlice(meta(0, 100, 4), new Variables(), quotingMeta(), 2);
    String q = slice.buildQuery("SELECT a, b FROM t WHERE c > 1");
    assertEquals(
        "SELECT * FROM ( SELECT a, b FROM t WHERE c > 1 ) hop_part WHERE \"id\" >= 50 AND \"id\" < 75",
        q);
  }

  @Test
  void buildSparkDbtableWrapsQuery() {
    TableInputPartitioning.TableInputPartitionSlice slice =
        TableInputPartitioning.resolveSlice(meta(0, 100, 4), new Variables(), quotingMeta(), 0);
    String dbtable = slice.buildSparkDbtable("SELECT id, a FROM t");
    assertEquals("( SELECT id, a FROM t ) hop_part", dbtable);
    // single-partition fast path: no predicates, still exposes the column
    TableInputPartitioning.TableInputPartitionSlice single =
        TableInputPartitioning.resolveSlice(meta(0, 100, 1), new Variables(), quotingMeta(), 0);
    assertEquals(
        "( SELECT id, a FROM t ) hop_part", single.buildSparkDbtable("SELECT id, a FROM t"));
  }

  @Test
  void trimSqlRemovesTrailingSemicolons() {
    assertEquals("SELECT 1", TableInputPartitioning.trimSql("SELECT 1;"));
    assertEquals("SELECT 1", TableInputPartitioning.trimSql("SELECT 1;;  "));
    assertNull(TableInputPartitioning.trimSql(null));
  }

  @Test
  void lowerBoundGreaterThanUpperThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableInputPartitioning.resolveSlice(
                meta(100, 0, 4), new Variables(), quotingMeta(), 0));
  }

  @Test
  void invalidBoundThrows() {
    TableInputMeta meta = meta(0, 100, 4);
    meta.setJdbcUpperBound("abc");
    // configured yes (fields all set), but resolveSlice throws on the non-numeric bound
    assertTrue(TableInputPartitioning.isPartitioningConfigured(meta, new Variables()));
    assertThrows(
        NumberFormatException.class,
        () -> TableInputPartitioning.resolveSlice(meta, new Variables(), quotingMeta(), 0));
  }
}
