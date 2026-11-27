/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.databases.duckdb;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.junit.dml.RealDriverDmlScenario;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Runs the declared INSERT of several rows against the embedded DuckDB driver through a multi-row
 * batch, and checks the table and the verdict of every row.
 */
class DuckDBDmlTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
  }

  private static DatabaseMeta databaseMeta() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new DuckDBDatabaseMeta());
    return databaseMeta;
  }

  private static Connection connect(String name) throws Exception {
    return DriverManager.getConnection("jdbc:duckdb:");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5, 100})
  void theDeclaredInsertWorks(int rowsPerStatement) throws Exception {
    RealDriverDmlScenario.assertDeclared(databaseMeta());
    try (Connection connection = connect("dmltest" + rowsPerStatement)) {
      List<String> statements =
          RealDriverDmlScenario.run(connection, databaseMeta(), "DML_TEST", rowsPerStatement);
      statements.forEach(sql -> System.out.println("duckdb x" + rowsPerStatement + ": " + sql));
    }
  }

  @Test
  void theLimitsHold() throws Exception {
    try (Connection connection = connect("dmllimits")) {
      System.out.println(
          "duckdb "
              + RealDriverDmlScenario.runAtLimits(connection, databaseMeta(), "DML_LIMITS", 5000));
    }
  }

  /**
   * A table of 1000 columns, 999 of them VARCHAR(20), in statements of 100 and of 1000 rows: the
   * statement and its values grow with the width of the table, and nothing caps them but the
   * dialect's limits.
   */
  @ParameterizedTest
  @ValueSource(ints = {100, 1000})
  void aWideTableIsWrittenInStatementsOfSeveralRows(int rowsPerStatement) throws Exception {
    try (Connection connection = connect("dmlwide" + rowsPerStatement)) {
      System.out.println(
          "duckdb x"
              + rowsPerStatement
              + " "
              + RealDriverDmlScenario.runWide(
                  connection,
                  databaseMeta(),
                  "DML_WIDE",
                  999,
                  "VARCHAR(20)",
                  20,
                  rowsPerStatement));
    }
  }

  /** A dialect that extends this one talks to another server and declares its own. */
  @Test
  void aDialectThatExtendsThisOneDeclaresNothing() {
    assertNull(new DuckDBDatabaseMeta() {}.getDmlSyntax());
  }

  /**
   * Times 100,000 rows, one row per statement against 100. Run with {@code
   * -Dhop.dml.benchmark=true}.
   */
  @Test
  @EnabledIfSystemProperty(named = "hop.dml.benchmark", matches = "true")
  void measure() throws Exception {
    try (Connection connection = connect("dmlbench")) {
      RealDriverDmlScenario.measure(connection, databaseMeta(), "DML_BENCH", 100_000, 100, 1000)
          .forEach(line -> System.out.println("duckdb " + line));
    }
  }
}
