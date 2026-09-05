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
package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SqlQueryClassifierTest {

  @Test
  void selectIsAQuery() {
    assertTrue(SqlQueryClassifier.isQuery("SELECT * FROM t"));
    assertTrue(SqlQueryClassifier.isQuery("  select 1"));
  }

  @Test
  void showAndExplainAreQueries() {
    assertTrue(SqlQueryClassifier.isQuery("show annotations from service"));
    assertTrue(SqlQueryClassifier.isQuery("EXPLAIN SELECT 1"));
  }

  @Test
  void dmlAndDdlAreNotQueries() {
    assertFalse(SqlQueryClassifier.isQuery("INSERT INTO t VALUES (1)"));
    assertFalse(SqlQueryClassifier.isQuery("UPDATE t SET a = 1"));
    assertFalse(SqlQueryClassifier.isQuery("DELETE FROM t"));
    assertFalse(SqlQueryClassifier.isQuery("CREATE TABLE t (id INT)"));
  }

  @Test
  void selectIntoIsNotAQuery() {
    assertFalse(SqlQueryClassifier.isQuery("SELECT * INTO dest FROM src"));
  }

  @Test
  void withSelectIsAQuery() {
    String sql =
        """
        WITH customer_360_bv AS (
                 SELECT customer_360_bv_1.customer_hk,
                    customer_360_bv_1.cust_email
                   FROM public.customer_360_bv customer_360_bv_1
                )
         SELECT customer_hk,
            cust_email AS email
           FROM customer_360_bv
          WHERE cust_email IS NOT NULL
        """;
    assertTrue(SqlQueryClassifier.isQuery(sql));
  }

  @Test
  void withRecursiveSelectIsAQuery() {
    assertTrue(
        SqlQueryClassifier.isQuery(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 3) SELECT * FROM t"));
  }

  @Test
  void multipleCtesThenSelectIsAQuery() {
    assertTrue(
        SqlQueryClassifier.isQuery(
            "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) SELECT * FROM a"));
  }

  @Test
  void withInsertIsNotAQuery() {
    assertFalse(
        SqlQueryClassifier.isQuery("WITH s AS (SELECT 1 AS id) INSERT INTO t SELECT * FROM s"));
  }

  @Test
  void withUpdateIsNotAQuery() {
    assertFalse(SqlQueryClassifier.isQuery("WITH s AS (SELECT 1 AS id) UPDATE t SET a = 1"));
  }

  @Test
  void leadingCommentDoesNotHideWithSelect() {
    assertTrue(SqlQueryClassifier.isQuery("-- run me\nWITH s AS (SELECT 1) SELECT * FROM s"));
  }

  @Test
  void leftoverWhereIsNotAnExecutableStatement() {
    assertFalse(SqlQueryClassifier.isExecutableStatement("WHERE x = 1"));
    assertFalse(SqlQueryClassifier.isExecutableStatement("AND y = 2"));
    assertFalse(SqlQueryClassifier.isExecutableStatement("ORDER BY id"));
    assertFalse(SqlQueryClassifier.isExecutableStatement(""));
  }

  @Test
  void queriesAndDmlAreExecutableStatements() {
    assertTrue(SqlQueryClassifier.isExecutableStatement("SELECT * FROM t"));
    assertTrue(SqlQueryClassifier.isExecutableStatement("WITH s AS (SELECT 1) SELECT * FROM s"));
    assertTrue(SqlQueryClassifier.isExecutableStatement("INSERT INTO t VALUES (1)"));
    assertTrue(SqlQueryClassifier.isExecutableStatement("CREATE TABLE t (id INT)"));
    assertTrue(SqlQueryClassifier.isExecutableStatement("-- comment\nUPDATE t SET a = 1"));
  }

  @Test
  void scriptStatementsMarkWithSelectAsQuery() {
    ConcreteBaseDatabaseMeta meta = new ConcreteBaseDatabaseMeta();
    var statements =
        meta.getSqlScriptStatements(
            "WITH s AS (SELECT 1 AS x) SELECT * FROM s;\nINSERT INTO t VALUES (1);");
    assertTrue(statements.get(0).isQuery());
    assertFalse(statements.get(1).isQuery());
  }
}
