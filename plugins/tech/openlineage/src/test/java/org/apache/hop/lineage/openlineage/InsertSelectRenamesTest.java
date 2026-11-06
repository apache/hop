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

package org.apache.hop.lineage.openlineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class InsertSelectRenamesTest {

  @Test
  void mapsPlainSelectColumnsOntoTheTargetList() {
    Map<String, String> rename =
        InsertSelectRenames.of(
            "INSERT INTO analytics.marts.daily_orders (order_id, customer) "
                + "SELECT o.id, o.cust FROM analytics.staging.orders o");

    assertEquals(Map.of("id", "order_id", "cust", "customer"), rename);
  }

  @Test
  void mapsGeneratedExpressionNamesByPosition() {
    // The parser names an unaliased expression _0, _1, ... in order of the expressions it could
    // resolve, whatever the position of its siblings.
    Map<String, String> rename =
        InsertSelectRenames.of(
            "INSERT INTO t (total, id) SELECT SUM(amount), id FROM s GROUP BY id");

    assertEquals(Map.of("_0", "total", "id", "id"), rename);
  }

  @Test
  void mapsExplicitAliases() {
    Map<String, String> rename =
        InsertSelectRenames.of(
            "INSERT INTO t (largest, cnt) SELECT MAX(v) AS biggest, COUNT(*) n FROM s");

    assertEquals(Map.of("biggest", "largest", "n", "cnt"), rename);
  }

  @Test
  void mapsImplicitAliases() {
    Map<String, String> rename =
        InsertSelectRenames.of(
            "INSERT INTO t (first_col, second_col) SELECT x first, y second FROM s");

    assertEquals(Map.of("first", "first_col", "second", "second_col"), rename);
  }

  @Test
  void mapsAnExpressionThatIsNotAColumn() {
    Map<String, String> rename = InsertSelectRenames.of("INSERT INTO t (c0) SELECT x + 1 FROM s");

    assertEquals(Map.of("_0", "c0"), rename);
  }

  @Test
  void handlesCaseExpressionsAndQuotedTargets() {
    Map<String, String> rename =
        InsertSelectRenames.of(
            "INSERT INTO \"t\" (\"status\", \"kind\") "
                + "SELECT CASE WHEN a > 1 THEN 1 ELSE 0 END, \"kind\" FROM s");

    assertEquals(Map.of("_0", "status", "kind", "kind"), rename);
  }

  @Test
  void ignoresStatementsItCannotReadWithCertainty() {
    // no target column list
    assertTrue(InsertSelectRenames.of("INSERT INTO t SELECT x FROM s").isEmpty());
    // a star cannot be tied to columns
    assertTrue(InsertSelectRenames.of("INSERT INTO t (a, b) SELECT * FROM s").isEmpty());
    // not a SELECT
    assertTrue(InsertSelectRenames.of("INSERT INTO t (a, b) VALUES (1, 2)").isEmpty());
    // sizes disagree
    assertTrue(InsertSelectRenames.of("INSERT INTO t (a) SELECT x, y FROM s").isEmpty());
    // several statements: the names would be ambiguous across them
    assertTrue(
        InsertSelectRenames.of(
                "INSERT INTO a (x) SELECT p FROM s; INSERT INTO b (y) SELECT q FROM r")
            .isEmpty());
    assertTrue(InsertSelectRenames.of(null).isEmpty());
    assertTrue(InsertSelectRenames.of("SELECT 1").isEmpty());
  }

  @Test
  void readsACteBeforeTheInsert() {
    Map<String, String> rename =
        InsertSelectRenames.of("WITH c AS (SELECT id FROM s) INSERT INTO t (a) SELECT x FROM c");

    assertEquals(Map.of("x", "a"), rename);
  }

  @Test
  void ignoresComments() {
    Map<String, String> rename =
        InsertSelectRenames.of(
            "-- copy\nINSERT INTO t (a, b) /* the targets */ SELECT x, y FROM s");

    assertEquals(Map.of("x", "a", "y", "b"), rename);
  }
}
