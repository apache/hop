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

package org.apache.hop.databases.postgresql;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.junit.dml.RealDriverCastScenario;
import org.apache.hop.junit.dml.RealDriverCastScenario.Case;
import org.apache.hop.junit.dml.RealDriverCastScenario.Outcome;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Writes values that don't fit their column (too long, too precise) through the upserts of the
 * PostgreSQL dialect (the MERGE, whose parameters are cast, and ON CONFLICT), and checks that each
 * does what the plain one-row INSERT or UPDATE does on the same column: store the same value, match
 * the same rows, or refuse the row. Needs Docker; run with {@code -Dit.test=PostgreSqlCastIT}.
 */
class PostgreSqlCastIT {

  private static PostgreSQLContainer<?> container;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    container = new PostgreSQLContainer<>("postgres:18");
    container.start();
  }

  @AfterAll
  static void stopContainer() {
    if (container != null) {
      container.stop();
    }
  }

  private static Connection connect() throws Exception {
    return DriverManager.getConnection(
        container.getJdbcUrl(), container.getUsername(), container.getPassword());
  }

  private static DatabaseMeta databaseMeta() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new PostgreSqlDatabaseMeta());
    return databaseMeta;
  }

  private static Map<String, Outcome> run(Case kase) throws Exception {
    try (Connection connection = connect()) {
      Map<String, Outcome> outcomes =
          RealDriverCastScenario.run(connection, databaseMeta(), "cast_test", kase);
      RealDriverCastScenario.print("postgres " + kase, outcomes);
      return outcomes;
    }
  }

  private static final Timestamp NANOS = Timestamp.valueOf("2026-01-02 03:04:05.123456789");
  private static final Timestamp WHOLE = Timestamp.valueOf("2026-01-02 03:04:05");

  /** Values every form writes as the plain statements do. */
  static Stream<Case> likePlain() {
    return Stream.of(
        new Case("varchar(5) exactly 5", "VARCHAR(5)", "abcde", "abc", "abcde"),
        new Case("varchar(5) trailing spaces", "VARCHAR(5)", "abc    ", "abc", "abc"),
        new Case("varchar(5) 5 multibyte", "VARCHAR(5)", "ééééé", "abc", "ééééé"),
        new Case("char(5) shorter", "CHAR(5)", "abc", "xy", "abc"),
        new Case("varchar unbounded", "VARCHAR", "x".repeat(10_000), "abc", null),
        new Case("text", "TEXT", "x".repeat(10_000), "abc", null),
        new Case(
            "numeric(5,2) fits", "NUMERIC(5,2)", new BigDecimal("123.45"), BigDecimal.ONE, null),
        new Case(
            "numeric(5,2) more scale rounds up",
            "NUMERIC(5,2)",
            new BigDecimal("1.235"),
            BigDecimal.ONE,
            null),
        new Case(
            "numeric(5,2) rounds into overflow",
            "NUMERIC(5,2)",
            new BigDecimal("999.995"),
            BigDecimal.ONE,
            null),
        new Case(
            "numeric(5,2) too many integer digits",
            "NUMERIC(5,2)",
            new BigDecimal("1234.5"),
            BigDecimal.ONE,
            null),
        new Case(
            "numeric unconstrained",
            "NUMERIC",
            new BigDecimal("12345678901234567890.123456789"),
            BigDecimal.ONE,
            null),
        new Case("integer overflow", "INTEGER", 3_000_000_000L, 1L, null),
        new Case("timestamp(3) nanos", "TIMESTAMP(3)", NANOS, WHOLE, null),
        new Case("timestamp nanos", "TIMESTAMP", NANOS, WHOLE, null),
        new Case("timestamptz(0) nanos", "TIMESTAMPTZ(0)", NANOS, WHOLE, null),
        new Case("bytea", "BYTEA", new byte[] {1, 2, 3, 4, 5}, new byte[] {1}, null),
        new Case("boolean", "BOOLEAN", Boolean.TRUE, Boolean.FALSE, null),
        new Case("bit(3) 4", "BIT(3)", "1010", "000", null, RealDriverCastScenario.OTHER),
        new Case("varbit(5) 7", "VARBIT(5)", "1010101", "0", null, RealDriverCastScenario.OTHER),
        new Case(
            "uuid",
            "UUID",
            "0f8fad5b-d9cb-469f-a165-70867728950e",
            "7c9e6679-7425-40de-944b-e07fc1f90ae7",
            null,
            RealDriverCastScenario.OTHER),
        new Case(
            "uuid invalid",
            "UUID",
            "not-a-uuid",
            "7c9e6679-7425-40de-944b-e07fc1f90ae7",
            null,
            RealDriverCastScenario.OTHER),
        new Case("jsonb", "JSONB", "{\"a\": 1}", "{}", null, RealDriverCastScenario.OTHER),
        new Case("jsonb invalid", "JSONB", "{", "{}", null, RealDriverCastScenario.OTHER),
        new Case("json", "JSON", "{\"a\":  1}", "{}", null, RealDriverCastScenario.OTHER));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("likePlain")
  void behavesLikeThePlainStatement(Case kase) throws Exception {
    RealDriverCastScenario.assertSameAsPlain("postgres " + kase, run(kase));
  }

  /**
   * Strings longer than their column. A cast to {@code varchar(5)} or {@code bpchar(5)} cut them to
   * five characters without an error, so the MERGE stored the cut value and matched the row holding
   * it; the casts have no length now.
   */
  static Stream<Case> overlongStrings() {
    return Stream.of(
        new Case("varchar(5) 6", "VARCHAR(5)", "abcdef", "abc", "abcde"),
        new Case("varchar(5) 7", "VARCHAR(5)", "abcdefg", "abc", "abcde"),
        new Case("varchar(5) 6 multibyte", "VARCHAR(5)", "éééééé", "abc", "ééééé"),
        new Case("varchar(5) 6 emoji", "VARCHAR(5)", "😀😀😀😀😀😀", "abc", "😀😀😀😀😀"),
        new Case("char(5) 6", "CHAR(5)", "abcdef", "xy", "abcde"),
        new Case("char(5) 6 multibyte", "CHAR(5)", "éééééé", "xy", "ééééé"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("overlongStrings")
  void overlongStringIsRefusedAsByThePlainStatement(Case kase) throws Exception {
    RealDriverCastScenario.assertSameAsPlain("postgres " + kase, run(kase));
  }

  /**
   * Keys with more decimals than their column. A cast to {@code numeric(5,2)} rounded them before
   * the comparison, so 1.234 matched the row holding 1.23; the casts have no precision now.
   */
  static Stream<Case> overPreciseKeys() {
    return Stream.of(
        new Case(
            "numeric(5,2) key rounds up",
            "NUMERIC(5,2)",
            new BigDecimal("1.235"),
            BigDecimal.ONE,
            new BigDecimal("1.24")),
        new Case(
            "numeric(5,2) key rounds down",
            "NUMERIC(5,2)",
            new BigDecimal("1.234"),
            BigDecimal.ONE,
            new BigDecimal("1.23")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("overPreciseKeys")
  void overPreciseKeyIsComparedAsByThePlainStatement(Case kase) throws Exception {
    RealDriverCastScenario.assertSameAsPlain("postgres " + kase, run(kase));
  }

  /**
   * A {@code bit(3)} column: the cast was {@code CAST(? AS bit)}, which is {@code bit(1)} and
   * refused every value of three bits; it is {@code varbit} now.
   */
  @Test
  void bitStringIsStoredAsByThePlainStatement() throws Exception {
    Case kase = new Case("bit(3)", "BIT(3)", "101", "000", null, RealDriverCastScenario.OTHER);
    RealDriverCastScenario.assertSameAsPlain("postgres " + kase, run(kase));
  }
}
