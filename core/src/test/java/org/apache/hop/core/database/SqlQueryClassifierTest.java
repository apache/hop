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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    assertFalse(SqlQueryClassifier.isQuery("SELECT id FROM customers INTO @last_id"));
    assertFalse(
        SqlQueryClassifier.isQuery("SELECT id, name FROM customers INTO OUTFILE '/tmp/c.csv'"));
    assertFalse(SqlQueryClassifier.isQuery("WITH s AS (SELECT 1) SELECT * FROM s INTO @x"));
  }

  @Test
  void intoInsideAStringOrSubqueryDoesNotHideASelect() {
    assertTrue(SqlQueryClassifier.isQuery("SELECT * FROM t WHERE note = 'insert into x'"));
    assertTrue(SqlQueryClassifier.isQuery("SELECT * FROM (SELECT * INTO dest FROM src) s"));
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

  @Test
  void statementVerbIsTheFirstKeyword() {
    assertEquals("MERGE", SqlQueryClassifier.statementVerb("MERGE INTO t USING s ON t.id = s.id"));
    assertEquals(
        "insert".toUpperCase(), SqlQueryClassifier.statementVerb("insert into t values (1)"));
    assertEquals("UPDATE", SqlQueryClassifier.statementVerb("  \n/* c */ UPDATE t SET a = 1"));
    assertEquals("DELETE", SqlQueryClassifier.statementVerb("-- comment\nDELETE FROM t"));
  }

  @Test
  void statementVerbSkipsTheCteList() {
    assertEquals(
        "UPDATE",
        SqlQueryClassifier.statementVerb(
            "WITH s AS (SELECT id FROM src WHERE id IN (2,3)) UPDATE t SET a = 1 FROM s"));
    assertEquals(
        "MERGE",
        SqlQueryClassifier.statementVerb(
            "WITH RECURSIVE a AS (SELECT 1), b (x) AS NOT MATERIALIZED (SELECT 2) "
                + "MERGE INTO t USING b ON t.id = b.x WHEN MATCHED THEN DELETE"));
    assertEquals(
        "SELECT", SqlQueryClassifier.statementVerb("WITH s AS (SELECT 1) SELECT * FROM s"));
  }

  @Test
  void statementVerbIsNullWithoutAKeyword() {
    assertNull(SqlQueryClassifier.statementVerb(null));
    assertNull(SqlQueryClassifier.statementVerb("   "));
    assertNull(SqlQueryClassifier.statementVerb("(SELECT 1)"));
  }

  @Test
  void schemaChangesOnTablesAndViewsAreDetected() {
    assertTrue(SqlQueryClassifier.isSchemaChange("ALTER TABLE t ADD COLUMN c INT"));
    assertTrue(SqlQueryClassifier.isSchemaChange("create table t (id int)"));
    assertTrue(SqlQueryClassifier.isSchemaChange("DROP TABLE t"));
    assertTrue(SqlQueryClassifier.isSchemaChange("RENAME TABLE a TO b"));
  }

  @Test
  void schemaChangesAreDetectedPastModifiersAndTrivia() {
    // The old check was a startsWith() on the upper-cased statement, so all of these were missed.
    assertTrue(SqlQueryClassifier.isSchemaChange("\n\t ALTER TABLE t ADD COLUMN c INT"));
    assertTrue(SqlQueryClassifier.isSchemaChange("-- fix the layout\nALTER TABLE t DROP COLUMN c"));
    assertTrue(SqlQueryClassifier.isSchemaChange("/* ticket 42 */ DROP TABLE IF EXISTS t"));
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE TABLE IF NOT EXISTS t (id int)"));
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE OR REPLACE VIEW v AS SELECT * FROM t"));
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE OR ALTER VIEW v AS SELECT * FROM t"));
    assertTrue(SqlQueryClassifier.isSchemaChange("ALTER VIEW v AS SELECT * FROM t"));
    assertTrue(SqlQueryClassifier.isSchemaChange("DROP VIEW v"));
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE MATERIALIZED VIEW v AS SELECT 1"));
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE GLOBAL TEMPORARY TABLE t (id int)"));
    assertTrue(SqlQueryClassifier.isSchemaChange("DROP SYNONYM s"));
  }

  @Test
  void dialectSpecificModifiersAreSchemaChanges() {
    // Oracle, DBMS_METADATA emits the FORCE EDITIONABLE form verbatim
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE OR REPLACE FORCE VIEW v AS SELECT 1"));
    assertTrue(
        SqlQueryClassifier.isSchemaChange(
            "CREATE OR REPLACE FORCE EDITIONABLE VIEW v AS SELECT 1 FROM dual"));
    assertTrue(
        SqlQueryClassifier.isSchemaChange("CREATE OR REPLACE EDITIONABLE VIEW v AS SELECT 1"));
    assertTrue(
        SqlQueryClassifier.isSchemaChange("CREATE OR REPLACE NONEDITIONABLE VIEW v AS SELECT 1"));
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE PUBLIC SYNONYM s FOR t"));
    assertTrue(SqlQueryClassifier.isSchemaChange("DROP PUBLIC SYNONYM s"));
    // PostgreSQL
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE RECURSIVE VIEW v (a) AS SELECT 1"));
    // Snowflake
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE OR REPLACE TRANSIENT TABLE t (id int)"));
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE SECURE VIEW v AS SELECT 1"));
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE OR REPLACE DYNAMIC TABLE t AS SELECT 1"));
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE HYBRID TABLE t (id int PRIMARY KEY)"));
    // Teradata
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE MULTISET TABLE t (id int)"));
    assertTrue(SqlQueryClassifier.isSchemaChange("CREATE VOLATILE TABLE t (id int)"));
    // Oracle forms outside the DBMS_METADATA default
    assertTrue(
        SqlQueryClassifier.isSchemaChange(
            "CREATE OR REPLACE NOFORCE EDITIONABLE VIEW v AS SELECT 1 FROM dual"));
    assertTrue(
        SqlQueryClassifier.isSchemaChange(
            "CREATE OR REPLACE EDITIONING VIEW v AS SELECT a FROM t"));
  }

  @Test
  void mysqlViewDefinitionsAreSchemaChanges() {
    // The text SHOW CREATE VIEW returns and mysqldump writes, verbatim
    assertTrue(
        SqlQueryClassifier.isSchemaChange(
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v`"
                + " AS select `t`.`id` AS `id` from `t`"));
    assertTrue(
        SqlQueryClassifier.isSchemaChange(
            "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = 'app'@'%' SQL SECURITY INVOKER VIEW v"
                + " AS SELECT 1"));
    assertTrue(
        SqlQueryClassifier.isSchemaChange(
            "CREATE DEFINER=CURRENT_USER() SQL SECURITY INVOKER VIEW v AS SELECT 1"));
    assertTrue(SqlQueryClassifier.isSchemaChange("ALTER ALGORITHM=TEMPTABLE VIEW v AS SELECT 1"));
    // A modifier with a value still has to be followed by a table or a view
    assertFalse(
        SqlQueryClassifier.isSchemaChange(
            "CREATE DEFINER=`root`@`localhost` TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET"
                + " @x = 1"));
    assertFalse(
        SqlQueryClassifier.isSchemaChange(
            "CREATE DEFINER=`root`@`localhost` PROCEDURE p() SELECT 1"));
  }

  @Test
  void otherStatementsAreNotSchemaChanges() {
    assertFalse(SqlQueryClassifier.isSchemaChange(null));
    assertFalse(SqlQueryClassifier.isSchemaChange("   "));
    assertFalse(SqlQueryClassifier.isSchemaChange("SELECT * FROM t"));
    assertFalse(SqlQueryClassifier.isSchemaChange("INSERT INTO t VALUES (1)"));
    assertFalse(SqlQueryClassifier.isSchemaChange("DELETE FROM t"));
    // TRUNCATE removes rows, it does not change the layout
    assertFalse(SqlQueryClassifier.isSchemaChange("TRUNCATE t"));
    assertFalse(SqlQueryClassifier.isSchemaChange("TRUNCATE TABLE t"));
    assertFalse(SqlQueryClassifier.isSchemaChange("CREATE INDEX i ON t (a)"));
    assertFalse(SqlQueryClassifier.isSchemaChange("CREATE SEQUENCE s"));
    assertFalse(SqlQueryClassifier.isSchemaChange("ALTER SESSION SET x = 1"));
    assertFalse(SqlQueryClassifier.isSchemaChange("DROP INDEX i"));
  }
}
