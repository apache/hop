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

package org.apache.hop.databases.db2;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.junit.batch.DownloadedJdbcDriver;
import org.apache.hop.junit.dml.RealDriverCastScenario;
import org.apache.hop.junit.dml.RealDriverCastScenario.Case;
import org.apache.hop.junit.dml.RealDriverCastScenario.Outcome;
import org.apache.hop.junit.merge.RealDriverMergeScenario;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Runs the MERGE the DB2 dialect declares against a DB2 Community server, and writes values that
 * don't fit their column (too long, too precise) through it, whose parameters are cast, to check
 * that it does what the plain one-row INSERT or UPDATE does on the same column: store the same
 * value, match the same rows, or refuse the row.
 *
 * <p>The IBM JDBC driver is not open source: it is downloaded as Hop downloads it, never a build
 * dependency. Needs Docker; the DB2 Community image is amd64 only and takes several minutes to
 * start, longer emulated on ARM, so it runs on request only: {@code -Dhop.db2.it=true}, or against
 * a running server with {@code -Dhop.db2.url=jdbc:db2://host:port/testdb} (user db2inst1, password
 * {@value #PASSWORD}).
 */
class DB2MergeIT {

  private static final String PASSWORD = "Hop_Db2_2026";
  private static GenericContainer<?> container;
  private static String url;
  private static DownloadedJdbcDriver driver;

  @BeforeAll
  static void startContainer() throws Exception {
    url = System.getProperty("hop.db2.url");
    boolean asked = url != null && !url.isBlank() || Boolean.getBoolean("hop.db2.it");
    assumeTrue(asked, "DB2 runs on request: -Dhop.db2.it=true or -Dhop.db2.url=...");
    HopClientEnvironment.init();
    DB2DatabaseMeta dialect = new DB2DatabaseMeta();
    driver =
        DownloadedJdbcDriver.load(
            dialect.getDriverDownload(),
            dialect.getDriverClass(),
            Path.of("target", "jdbc-drivers"));
    if (url != null && !url.isBlank()) {
      return;
    }
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    container =
        new GenericContainer<>("icr.io/db2_community/db2:latest")
            .withCreateContainerCmdModifier(cmd -> cmd.withPlatform("linux/amd64"))
            .withPrivilegedMode(true)
            .withEnv("LICENSE", "accept")
            .withEnv("DB2INST1_PASSWORD", PASSWORD)
            .withEnv("DBNAME", "testdb")
            .withEnv("ARCHIVE_LOGS", "false")
            .withEnv("AUTOCONFIG", "false")
            .withExposedPorts(50000)
            .waitingFor(
                Wait.forLogMessage(".*Setup has completed.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(20)));
    container.start();
    url = "jdbc:db2://" + container.getHost() + ":" + container.getMappedPort(50000) + "/testdb";
  }

  @AfterAll
  static void stopContainer() {
    if (container != null) {
      container.stop();
    }
  }

  private static Connection connect() throws Exception {
    for (int attempt = 0; ; attempt++) {
      try {
        return driver.connect(url, "db2inst1", PASSWORD);
      } catch (SQLException e) {
        if (attempt > 30) {
          throw e;
        }
        Thread.sleep(2000);
      }
    }
  }

  private static DatabaseMeta databaseMeta() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new DB2DatabaseMeta());
    return databaseMeta;
  }

  private static Map<String, Outcome> run(Case kase) throws Exception {
    try (Connection connection = connect()) {
      Map<String, Outcome> outcomes =
          RealDriverCastScenario.run(connection, databaseMeta(), "CAST_TEST", kase);
      RealDriverCastScenario.print("db2 " + kase, outcomes);
      return outcomes;
    }
  }

  @Test
  void theDeclaredMergeWorks() throws Exception {
    try (Connection connection = connect()) {
      System.out.println("DB2 " + connection.getMetaData().getDatabaseProductVersion());
      RealDriverMergeScenario.Report report =
          RealDriverMergeScenario.run(connection, databaseMeta(), "MERGE_TEST");
      System.out.println("DB2 MERGE update counts: " + report);

      // "Update only changed rows" with a column DB2 can't compare.
      for (String type : new String[] {"CLOB(1M)", "DBCLOB(1M)"}) {
        RealDriverMergeScenario.uncomparableColumn(
            connection, databaseMeta(), "MERGE_UNCOMPARABLE", UpsertForm.MERGE, type, "one", "two");
      }
    }
  }

  /** Every value: the DB2 casts give each the plain statement's outcome. */
  static Stream<Case> likePlain() {
    Timestamp nanos = Timestamp.valueOf("2026-01-02 03:04:05.123456789");
    Timestamp good = Timestamp.valueOf("2026-01-02 03:04:05");
    byte[] four = {1, 2, 3, 4};
    byte[] five = {1, 2, 3, 4, 5};
    return Stream.of(
        new Case("varchar(5) exactly 5", "VARCHAR(5)", "abcde", "abc", "abcde"),
        new Case("varchar(5) 6", "VARCHAR(5)", "abcdef", "abc", "abcde"),
        new Case("varchar(5) 7", "VARCHAR(5)", "abcdefg", "abc", "abcde"),
        new Case("varchar(5) trailing spaces", "VARCHAR(5)", "abc    ", "abc", "abc"),
        new Case("varchar(5) 6 bytes", "VARCHAR(5)", "ééé", "abc", null),
        new Case("char(5) shorter", "CHAR(5)", "abc", "xy", "abc"),
        new Case("char(5) 6", "CHAR(5)", "abcdef", "xy", "abcde"),
        new Case("vargraphic(5) 6", "VARGRAPHIC(5)", "日本語日本語", "abc", "日本語日本"),
        new Case("graphic(5) 6", "GRAPHIC(5)", "日本語日本語", "abc", "日本語日本"),
        new Case("varchar(32672)", "VARCHAR(32672)", "x".repeat(10_000), "abc", null),
        new Case("clob", "CLOB(1M)", "x".repeat(10_000), "abc", null),
        // CAST(? AS CLOB) would mean CLOB(1M), and refuse what the column takes.
        new Case("clob(10M) 2 MB", "CLOB(10M)", "x".repeat(2_000_000), "abc", null),
        new Case(
            "decimal(5,2) fits", "DECIMAL(5,2)", new BigDecimal("123.45"), BigDecimal.ONE, null),
        new Case(
            "decimal(5,2) more scale",
            "DECIMAL(5,2)",
            new BigDecimal("1.235"),
            BigDecimal.ONE,
            new BigDecimal("1.23")),
        new Case(
            "decimal(5,2) more scale 2",
            "DECIMAL(5,2)",
            new BigDecimal("1.239"),
            BigDecimal.ONE,
            new BigDecimal("1.23")),
        new Case(
            "decimal(5,2) too many integer digits",
            "DECIMAL(5,2)",
            new BigDecimal("1234.5"),
            BigDecimal.ONE,
            null),
        new Case("integer overflow", "INTEGER", 3_000_000_000L, 1L, null),
        new Case("timestamp(3) nanos", "TIMESTAMP(3)", nanos, good, null),
        new Case("timestamp nanos", "TIMESTAMP", nanos, good, null),
        new Case("varbinary(4) 5", "VARBINARY(4)", five, new byte[] {9}, four),
        new Case("binary(4) 5", "BINARY(4)", five, four, four),
        new Case("varchar(4) for bit data 5", "VARCHAR(4) FOR BIT DATA", five, four, four),
        new Case("char(4) for bit data 5", "CHAR(4) FOR BIT DATA", five, four, four),
        new Case("boolean", "BOOLEAN", Boolean.TRUE, Boolean.FALSE, null));
  }

  /**
   * DB2 types a parameter marker in {@code CAST(? AS VARCHAR(5))} as VARCHAR(5) and refuses a
   * longer value with SQLCODE -302, as it does for the plain statement, so the casts with their
   * lengths cut nothing.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("likePlain")
  void behavesLikeThePlainStatement(Case kase) throws Exception {
    RealDriverCastScenario.assertSameAsPlain("db2 " + kase, run(kase));
  }
}
