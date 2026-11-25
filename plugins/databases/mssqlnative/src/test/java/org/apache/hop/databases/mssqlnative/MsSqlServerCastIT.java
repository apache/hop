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

package org.apache.hop.databases.mssqlnative;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.databases.mssql.MsSqlServerDatabaseMeta;
import org.apache.hop.junit.dml.RealDriverCastScenario;
import org.apache.hop.junit.dml.RealDriverCastScenario.Case;
import org.apache.hop.junit.dml.RealDriverCastScenario.Outcome;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Writes values that don't fit their column (too long, too precise) through the MERGE of the SQL
 * Server dialects, whose parameters are cast, and checks that it does what the plain one-row INSERT
 * or UPDATE does on the same column: store the same value, match the same rows, or refuse the row.
 * Runs both SQL Server dialects against SQL Server 2022. Needs Docker (the image is amd64 only, so
 * it runs emulated on ARM); run with {@code -Dhop.it.dml=true}.
 */
@EnabledIfSystemProperty(named = "hop.it.dml", matches = "true")
class MsSqlServerCastIT {

  private static final String PASSWORD = "Hop_Merge_2026!";
  private static GenericContainer<?> container;

  @BeforeAll
  static void startContainer() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    container =
        new GenericContainer<>("mcr.microsoft.com/mssql/server:2022-latest")
            .withCreateContainerCmdModifier(cmd -> cmd.withPlatform("linux/amd64"))
            .withEnv("ACCEPT_EULA", "Y")
            .withEnv("MSSQL_SA_PASSWORD", PASSWORD)
            .withExposedPorts(1433)
            .waitingFor(
                Wait.forLogMessage(".*SQL Server is now ready for client connections.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(5)));
    container.start();
  }

  @AfterAll
  static void stopContainer() {
    if (container != null) {
      container.stop();
    }
  }

  private static Connection connect() throws Exception {
    String url =
        "jdbc:sqlserver://"
            + container.getHost()
            + ":"
            + container.getMappedPort(1433)
            + ";encrypt=false;trustServerCertificate=true";
    // The server logs "ready" a moment before it accepts logins.
    for (int attempt = 0; ; attempt++) {
      try {
        return DriverManager.getConnection(url, "sa", PASSWORD);
      } catch (SQLException e) {
        if (attempt > 30) {
          throw e;
        }
        Thread.sleep(1000);
      }
    }
  }

  private static DatabaseMeta databaseMeta(boolean nativeDialect) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(
        nativeDialect ? new MsSqlServerNativeDatabaseMeta() : new MsSqlServerDatabaseMeta());
    return databaseMeta;
  }

  private static Map<String, Outcome> run(Case kase, boolean nativeDialect) throws Exception {
    try (Connection connection = connect()) {
      Map<String, Outcome> outcomes =
          RealDriverCastScenario.run(connection, databaseMeta(nativeDialect), "CAST_TEST", kase);
      RealDriverCastScenario.print(
          "sqlserver" + (nativeDialect ? " native " : " ") + kase, outcomes);
      return outcomes;
    }
  }

  private static final Timestamp NANOS = Timestamp.valueOf("2026-01-02 03:04:05.123456789");
  private static final Timestamp WHOLE = Timestamp.valueOf("2026-01-02 03:04:05");
  private static final byte[] FOUR = {1, 2, 3, 4};
  private static final byte[] FIVE = {1, 2, 3, 4, 5};

  /** Values every form writes as the plain statements do. */
  static Stream<Case> likePlain() {
    return Stream.of(
        new Case("varchar(5) exactly 5", "VARCHAR(5)", "abcde", "abc", "abcde"),
        new Case("varchar(5) trailing spaces", "VARCHAR(5)", "abc    ", "abc", "abc"),
        new Case("varchar(5) 5 accented", "VARCHAR(5)", "ééééé", "abc", "ééééé"),
        new Case("varchar(5) not in code page", "VARCHAR(5)", "日本語", "abc", null),
        new Case("nvarchar(5) 5 multibyte", "NVARCHAR(5)", "日本語日本", "abc", "日本語日本"),
        new Case("char(5) shorter", "CHAR(5)", "abc", "xy", "abc"),
        new Case("varchar(max)", "VARCHAR(MAX)", "x".repeat(10_000), "abc", null),
        new Case("nvarchar(max)", "NVARCHAR(MAX)", "x".repeat(10_000), "abc", null),
        new Case(
            "decimal(5,2) fits", "DECIMAL(5,2)", new BigDecimal("123.45"), BigDecimal.ONE, null),
        new Case(
            "decimal(5,2) more scale rounds up",
            "DECIMAL(5,2)",
            new BigDecimal("1.235"),
            BigDecimal.ONE,
            null),
        new Case(
            "decimal(5,2) rounds into overflow",
            "DECIMAL(5,2)",
            new BigDecimal("999.995"),
            BigDecimal.ONE,
            null),
        new Case(
            "decimal(5,2) too many integer digits",
            "DECIMAL(5,2)",
            new BigDecimal("1234.5"),
            BigDecimal.ONE,
            null),
        new Case(
            "numeric(10,2) of another scale",
            "NUMERIC(10,2)",
            new BigDecimal("2.25"),
            BigDecimal.ONE,
            null),
        new Case("int overflow", "INT", 3_000_000_000L, 1L, null),
        new Case("datetime2(3) nanos", "DATETIME2(3)", NANOS, WHOLE, null),
        new Case("datetime2 nanos", "DATETIME2", NANOS, WHOLE, null),
        new Case("datetime nanos", "DATETIME", NANOS, WHOLE, null),
        new Case("varbinary(4) exactly 4", "VARBINARY(4)", FOUR, new byte[] {9}, FOUR),
        new Case("binary(4) shorter", "BINARY(4)", new byte[] {1, 2}, FOUR, null),
        new Case("varbinary(max)", "VARBINARY(MAX)", new byte[10_000], new byte[] {9}, null),
        new Case("bit", "BIT", Boolean.TRUE, Boolean.FALSE, null),
        new Case(
            "uniqueidentifier",
            "UNIQUEIDENTIFIER",
            "0F8FAD5B-D9CB-469F-A165-70867728950E",
            "7C9E6679-7425-40DE-944B-E07FC1F90AE7",
            null),
        new Case(
            "uniqueidentifier invalid",
            "UNIQUEIDENTIFIER",
            "not-a-uuid",
            "7C9E6679-7425-40DE-944B-E07FC1F90AE7",
            null));
  }

  static Stream<Arguments> likePlainWithDialects() {
    return withDialects(likePlain().map(Arguments::of));
  }

  private static Stream<Arguments> withDialects(Stream<Arguments> arguments) {
    return arguments.flatMap(
        a -> Stream.of(true, false).map(nativeDialect -> withDialect(a, nativeDialect)));
  }

  private static Arguments withDialect(Arguments arguments, boolean nativeDialect) {
    Object[] values = Arrays.copyOf(arguments.get(), arguments.get().length + 1);
    values[values.length - 1] = nativeDialect;
    return Arguments.of(values);
  }

  @ParameterizedTest(name = "{0}, native {1}")
  @MethodSource("likePlainWithDialects")
  void behavesLikeThePlainStatement(Case kase, boolean nativeDialect) throws Exception {
    RealDriverCastScenario.assertSameAsPlain("sqlserver " + kase, run(kase, nativeDialect));
  }

  /**
   * Strings and bytes longer than their column. A cast to {@code varchar(5)}, {@code nvarchar(5)},
   * {@code char(5)}, {@code nchar(5)}, {@code varbinary(4)}, {@code binary(4)} or {@code sysname}
   * cut them without an error, so the MERGE stored the cut value and matched the row holding it;
   * the casts are to {@code (max)} types now.
   */
  static Stream<Arguments> overlongValues() {
    return withDialects(
        Stream.of(
                new Case("varchar(5) 6", "VARCHAR(5)", "abcdef", "abc", "abcde"),
                new Case("varchar(5) 7", "VARCHAR(5)", "abcdefg", "abc", "abcde"),
                new Case("varchar(5) 6 accented", "VARCHAR(5)", "éééééé", "abc", "ééééé"),
                new Case("nvarchar(5) 6 multibyte", "NVARCHAR(5)", "日本語日本語", "abc", "日本語日本"),
                // Six UTF-16 code units: the cast cut the third emoji in half.
                new Case("nvarchar(5) 3 emoji", "NVARCHAR(5)", "😀😀😀", "abc", null),
                new Case("char(5) 6", "CHAR(5)", "abcdef", "xy", "abcde"),
                new Case("nchar(5) 6", "NCHAR(5)", "日本語日本語", "xy", "日本語日本"),
                new Case("varbinary(4) 5", "VARBINARY(4)", FIVE, new byte[] {9}, FOUR),
                new Case("binary(4) 5", "BINARY(4)", FIVE, FOUR, FOUR),
                new Case("sysname 129", "SYSNAME", "x".repeat(129), "abc", "x".repeat(128)))
            .map(Arguments::of));
  }

  @ParameterizedTest(name = "{0}, native {1}")
  @MethodSource("overlongValues")
  void overlongValueIsRefusedAsByThePlainStatement(Case kase, boolean nativeDialect)
      throws Exception {
    RealDriverCastScenario.assertSameAsPlain("sqlserver " + kase, run(kase, nativeDialect));
  }

  /**
   * Keys with more decimals than their column. A cast to {@code decimal(5,2)} rounded them before
   * the comparison, so 1.234 matched the row holding 1.23; the cast is to {@code decimal(38,35)}
   * now.
   */
  static Stream<Arguments> overPreciseKeys() {
    return withDialects(
        Stream.of(
                new Case(
                    "decimal(5,2) key rounds up",
                    "DECIMAL(5,2)",
                    new BigDecimal("1.235"),
                    BigDecimal.ONE,
                    new BigDecimal("1.24")),
                new Case(
                    "decimal(5,2) key rounds down",
                    "DECIMAL(5,2)",
                    new BigDecimal("1.234"),
                    BigDecimal.ONE,
                    new BigDecimal("1.23")))
            .map(Arguments::of));
  }

  @ParameterizedTest(name = "{0}, native {1}")
  @MethodSource("overPreciseKeys")
  void overPreciseKeyIsComparedAsByThePlainStatement(Case kase, boolean nativeDialect)
      throws Exception {
    RealDriverCastScenario.assertSameAsPlain("sqlserver " + kase, run(kase, nativeDialect));
  }
}
