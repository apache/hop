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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.PGConnection;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Savepoints of several transforms on the one connection of a transactional pipeline, on a real
 * PostgreSQL. A savepoint belongs to the transaction, so without {@link
 * org.apache.hop.core.database.SharedConnectionLock} one transform's release removes the savepoint
 * of another ("savepoint does not exist", then "current transaction is aborted"), and one
 * transform's rollback undoes the rows another transform already wrote.
 */
class PostgreSqlSharedConnectionSavepointIT {

  private static final int ROWS = 3_000;
  private static final long TIMEOUT_SECONDS = 120;

  private static PostgreSQLContainer<?> container;

  private final List<Database> opened = new ArrayList<>();
  private String group;
  private ExecutorService threads;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    container = new PostgreSQLContainer<>("postgres:16-alpine");
    container.start();
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  @BeforeEach
  void setUp() {
    group = "it-" + UUID.randomUUID();
    threads = Executors.newCachedThreadPool();
  }

  @AfterEach
  void tearDown() throws Exception {
    threads.shutdownNow();
    for (Database db : opened) {
      db.closeConnectionOnly();
    }
    opened.clear();
  }

  private static DatabaseMeta databaseMeta() {
    return new DatabaseMeta(
        "pg",
        "POSTGRESQL",
        "Native",
        container.getHost(),
        container.getDatabaseName(),
        Integer.toString(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
        container.getUsername(),
        container.getPassword());
  }

  /** A transform's database on the connection of the transactional pipeline. */
  private Database shared(String name) throws HopException {
    Database db = new Database(new LoggingObject(name), new Variables(), databaseMeta());
    db.setConnectionGroup(group);
    db.connect();
    db.setCommit(100);
    opened.add(db);
    return db;
  }

  /** A database on a connection of its own. */
  private Database own() throws HopException {
    Database db = new Database(new LoggingObject("own"), new Variables(), databaseMeta());
    db.connect();
    opened.add(db);
    return db;
  }

  private void createTable(String table, String check) throws HopException {
    try (Database db = own()) {
      db.execStatement("DROP TABLE IF EXISTS " + table);
      db.execStatement(
          "CREATE TABLE "
              + table
              + " (id BIGINT PRIMARY KEY"
              + (check == null ? "" : ", CHECK (" + check + ")")
              + ")");
    }
  }

  private long count(String table) throws HopException {
    try (Database db = own()) {
      RowMetaAndData row = db.getOneRow("SELECT count(*) FROM " + table);
      return row.getRowMeta().getInteger(row.getData(), 0);
    }
  }

  /** Ends the transaction of the pipeline, as LocalPipelineEngine does when it finished. */
  private static void commitThePipeline(Database owner) throws HopException {
    owner.commit(true);
  }

  private static IRowMeta idRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    return rowMeta;
  }

  /** One transform: inserts ROWS rows, each behind a savepoint; returns the rows that failed. */
  private CompletableFuture<Integer> transformWithErrorHop(Database db, String table) {
    return CompletableFuture.supplyAsync(
        () -> {
          int failed = 0;
          try {
            PreparedStatement insert = db.prepareSql("INSERT INTO " + table + " VALUES (?)");
            IRowMeta rowMeta = idRowMeta();
            for (long id = 1; id <= ROWS; id++) {
              Object[] row = {id};
              try {
                db.runInSavepoint(
                    () -> {
                      db.setValues(rowMeta, row, insert);
                      db.insertRow(insert, false, false);
                      return null;
                    });
              } catch (HopException e) {
                failed++;
              }
            }
          } catch (HopException e) {
            throw new IllegalStateException(e);
          }
          return failed;
        },
        threads);
  }

  /** One transform without an error hop: plain inserts, no savepoint. Fails on any error. */
  private CompletableFuture<Void> transformWithoutErrorHop(Database db, String table) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            PreparedStatement insert = db.prepareSql("INSERT INTO " + table + " VALUES (?)");
            IRowMeta rowMeta = idRowMeta();
            for (long id = 1; id <= ROWS; id++) {
              db.setValues(rowMeta, new Object[] {id}, insert);
              db.insertRow(insert, false, false);
            }
          } catch (HopException e) {
            throw new IllegalStateException(e);
          }
        },
        threads);
  }

  @Test
  void theTransformsOfAGroupShareOneWrappedConnection() throws Exception {
    Database first = shared("a");
    Database second = shared("b");

    assertSame(first.getConnection(), second.getConnection());
    assertNotNull(first.getSharedConnectionLock());
    assertSame(first.getSharedConnectionLock(), second.getSharedConnectionLock());
    assertFalse(first.getConnection() instanceof PGConnection);

    // PGBulkLoader reaches the COPY API through unwrap.
    PGConnection driver = first.getConnection().unwrap(PGConnection.class);
    assertNotNull(driver.getCopyAPI());
    assertNotSame(driver, first.getConnection());
  }

  /** Outside a transactional pipeline nothing changes: the driver's own connection, no lock. */
  @Test
  void aConnectionOfItsOwnIsNotWrapped() throws Exception {
    Database db = own();
    assertNull(db.getSharedConnectionLock());
    assertTrue(db.getConnection() instanceof PGConnection);
  }

  @Test
  void aFailedRowLeavesTheSharedTransactionUsable() throws Exception {
    createTable("it_sp_single", null);
    Database db = shared("a");
    IRowMeta rowMeta = idRowMeta();
    PreparedStatement insert = db.prepareSql("INSERT INTO it_sp_single VALUES (?)");
    for (long id : new long[] {1, 2, 2, 3}) {
      try {
        db.runInSavepoint(
            () -> {
              db.setValues(rowMeta, new Object[] {id}, insert);
              db.insertRow(insert, false, false);
              return null;
            });
      } catch (HopException e) {
        // the duplicate 2
      }
    }
    commitThePipeline(db);
    assertEquals(3, count("it_sp_single"));
  }

  /**
   * The reproduction of the review of #9106: transforms with an error hop on one connection, each
   * releasing its own savepoints while the others run. Every good row must be written and only the
   * bad rows may fail.
   *
   * @param transforms how many transforms share the connection
   */
  @ParameterizedTest(name = "{0} transforms")
  @ValueSource(ints = {2, 4, 8})
  void transformsWithErrorHopsKeepEveryGoodRow(int transforms) throws Exception {
    List<CompletableFuture<Integer>> running = new ArrayList<>();
    Database owner = null;
    for (int t = 0; t < transforms; t++) {
      String table = "it_sp_t" + t;
      // Every 101st row breaks the check constraint.
      createTable(table, "id % 101 <> 0");
      Database db = shared("t" + t);
      if (owner == null) {
        owner = db;
      }
      running.add(transformWithErrorHop(db, table));
    }
    for (CompletableFuture<Integer> transform : running) {
      assertEquals(ROWS / 101, transform.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
    commitThePipeline(owner);
    for (int t = 0; t < transforms; t++) {
      assertEquals(ROWS - ROWS / 101, count("it_sp_t" + t), "rows of transform " + t);
    }
  }

  /**
   * A transform without an error hop writes on the shared connection next to one with savepoints.
   * Without the lock, its rows that land inside the other transform's savepoint are undone when
   * that transform rolls a bad row back, while they were already passed on as written.
   */
  @Test
  void aTransformWithoutErrorHopLosesNoRows() throws Exception {
    createTable("it_sp_bad", "id % 7 <> 0");
    createTable("it_sp_plain", null);
    Database withErrorHop = shared("with error hop");
    Database withoutErrorHop = shared("without error hop");

    CompletableFuture<Integer> errorHop = transformWithErrorHop(withErrorHop, "it_sp_bad");
    CompletableFuture<Void> plain = transformWithoutErrorHop(withoutErrorHop, "it_sp_plain");
    assertEquals(ROWS / 7, errorHop.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    plain.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    commitThePipeline(withErrorHop);

    assertEquals(ROWS, count("it_sp_plain"));
    assertEquals(ROWS - ROWS / 7, count("it_sp_bad"));
  }

  /**
   * The same case made deterministic: the other transform tries to write while a savepoint section
   * is open, and that section then fails and rolls back. The other transform's row is written after
   * the rollback, so it survives.
   */
  @Test
  void aRowWrittenDuringAnotherTransformsSectionWaitsAndSurvives() throws Exception {
    createTable("it_sp_a", null);
    createTable("it_sp_b", null);
    Database a = shared("a");
    Database b = shared("b");
    IRowMeta rowMeta = idRowMeta();
    PreparedStatement insertA = a.prepareSql("INSERT INTO it_sp_a VALUES (?)");
    PreparedStatement insertB = b.prepareSql("INSERT INTO it_sp_b VALUES (?)");

    CountDownLatch sectionOpen = new CountDownLatch(1);
    AtomicInteger bWroteDuringSection = new AtomicInteger();
    ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<>();
    CompletableFuture<Void> transformB =
        CompletableFuture.runAsync(
            () -> {
              try {
                sectionOpen.await();
                b.setValues(rowMeta, new Object[] {1L}, insertB);
                b.insertRow(insertB, false, false);
                events.add("b wrote");
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            },
            threads);

    assertThrows(
        HopException.class,
        () ->
            a.runInSavepoint(
                () -> {
                  a.setValues(rowMeta, new Object[] {1L}, insertA);
                  a.insertRow(insertA, false, false);
                  sectionOpen.countDown();
                  LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
                  if (!events.isEmpty()) {
                    bWroteDuringSection.incrementAndGet();
                  }
                  // The same id again: the row fails and is rolled back to its savepoint.
                  a.insertRow(insertA, false, false);
                  return null;
                }));
    events.add("a rolled back");

    transformB.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    commitThePipeline(a);

    assertEquals(0, bWroteDuringSection.get(), "b wrote inside a's savepoint section");
    assertEquals(0, count("it_sp_a"));
    assertEquals(1, count("it_sp_b"), "b's row was undone by a's rollback");
  }

  /** Transforms waiting for each other's sections must not hang, also with hundreds of rows. */
  @Test
  void manyShortSectionsDoNotStarveAPlainWriter() throws Exception {
    createTable("it_sp_many_a", "id % 3 <> 0");
    createTable("it_sp_many_b", "id % 5 <> 0");
    createTable("it_sp_many_plain", null);
    Database a = shared("a");
    Database b = shared("b");
    Database plain = shared("plain");

    CompletableFuture<Integer> transformA = transformWithErrorHop(a, "it_sp_many_a");
    CompletableFuture<Integer> transformB = transformWithErrorHop(b, "it_sp_many_b");
    CompletableFuture<Void> transformPlain = transformWithoutErrorHop(plain, "it_sp_many_plain");

    assertEquals(ROWS / 3, transformA.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals(ROWS / 5, transformB.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    transformPlain.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    commitThePipeline(a);

    assertEquals(ROWS - ROWS / 3, count("it_sp_many_a"));
    assertEquals(ROWS - ROWS / 5, count("it_sp_many_b"));
    assertEquals(ROWS, count("it_sp_many_plain"));
  }
}
