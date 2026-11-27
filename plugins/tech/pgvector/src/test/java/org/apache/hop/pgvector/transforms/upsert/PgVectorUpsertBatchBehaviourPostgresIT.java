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

package org.apache.hop.pgvector.transforms.upsert;

import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.BAD;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.EMBEDDING;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.WRONG_DIMENSION;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.eventsInInputOrder;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.names;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.namesWithout;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.row;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.rows;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.tableStoredExcept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.Result;
import org.apache.hop.pipeline.engines.local.transaction.SharedConnectionRun;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Pins what the pgvector upsert does on PostgreSQL with batches and an error hop, where a failed
 * statement aborts the transaction until it is rolled back.
 *
 * <p>One input row is one {@code INSERT ... ON CONFLICT (id) DO UPDATE} statement. With "Delete
 * document before upsert" the first row of every document runs a {@code DELETE ... WHERE
 * document_id = ?} before its own statement, once per document and run. Without an error hop the
 * statements go out in JDBC batches of the commit size, and a failed batch is rolled back and stops
 * the transform. With an error hop batching is off: every row runs on its own behind a savepoint,
 * so a refused row (PGVECTORUPSERT002) only undoes its own statement, and the transform commits
 * every commit size rows. A row that fails before it reaches the database (no id, an embedding that
 * is no JSON array, a failed delete) goes to the error hop with PGVECTORUPSERT001.
 *
 * <p>Since #9768 PostgreSQL's validated batch failure contract keeps batching on with an error hop,
 * see PgVectorUpsertRecoveredBatchPostgresIT. A test whose name says legacy pins the handling as it
 * was, which HOP_DATABASE_BATCH_RECOVERY=LEGACY still gives; the others hold on the default path
 * and with LEGACY alike. A test whose name says Buggy pins behaviour that is wrong.
 */
class PgVectorUpsertBatchBehaviourPostgresIT {

  static final String TABLE = "chunks";
  static final String CODE_BEFORE_THE_DATABASE = "PGVECTORUPSERT001";
  static final String CODE_REFUSED = "PGVECTORUPSERT002";

  private static PostgreSQLContainer<?> container;
  private DatabaseMeta db;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    SharedConnectionRun.registerConnectionGroupExtensionPoint();
    container = startPgvector();
  }

  static PostgreSQLContainer<?> startPgvector() throws Exception {
    PostgreSQLContainer<?> pgvector =
        new PostgreSQLContainer<>(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"));
    pgvector.start();
    // On a busy host the first connection right after the start is now and then reset during the
    // SSL handshake, while the container settles.
    for (int attempt = 1; ; attempt++) {
      try {
        PgVectorUpsertRun.execute(databaseMeta(pgvector), "CREATE EXTENSION IF NOT EXISTS vector");
        return pgvector;
      } catch (HopDatabaseException e) {
        if (attempt == 5) {
          throw e;
        }
        Thread.sleep(1_000);
      }
    }
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  static DatabaseMeta databaseMeta(PostgreSQLContainer<?> pgvector) {
    return new DatabaseMeta(
        PgVectorUpsertRun.CONNECTION,
        "POSTGRESQL",
        "Native",
        pgvector.getHost(),
        pgvector.getDatabaseName(),
        Integer.toString(pgvector.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
        pgvector.getUsername(),
        pgvector.getPassword());
  }

  @BeforeEach
  void createTable() throws Exception {
    db = databaseMeta(container);
    PgVectorUpsertRun.createTable(db, TABLE);
  }

  private PgVectorUpsertRun upsert() {
    return PgVectorUpsertRun.on(db, TABLE);
  }

  /** The default path, and HOP_DATABASE_BATCH_RECOVERY=LEGACY. */
  static Stream<String> recoveries() {
    return Stream.of(null, "LEGACY");
  }

  static List<Long> counters(long read, long written, long rejected) {
    return List.of(read, written, rejected, 0L, 0L);
  }

  static void assertDescriptionsContain(Result r, String... texts) {
    List<Object> descriptions = r.errorDescriptions();
    assertEquals(texts.length, descriptions.size(), descriptions.toString());
    for (int i = 0; i < texts.length; i++) {
      assertTrue(
          ((String) descriptions.get(i)).contains(texts[i]),
          "error row " + i + ": " + descriptions.get(i));
    }
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: with an error hop batching is
   * off. Every row runs on its own behind a savepoint and is passed on right after it, before the
   * commit that includes it, at any commit size; only the refused rows go to the error hop.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 3, 100})
  void errorHop_legacy_Buggy_aSavepointPerRowAndEveryRowIsPassedOnBeforeItsCommit(int commitSize)
      throws Exception {
    Result r =
        upsert()
            .recovery("LEGACY")
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertTrue(r.useSafePoints);
    assertFalse(r.recoveredBatches);
    assertTrue(r.loggedDetailed("Batch support: batch failure contract UNKNOWN (from VARIABLE)"));
    assertEquals(0, r.errorCount);
    assertEquals(namesWithout(10, 3, 7), r.outputNames());
    assertEquals(names(3, 7), r.errorNames());
    assertEquals(eventsInInputOrder(10, 3, 7), r.events);
    assertEquals(List.of(CODE_REFUSED, CODE_REFUSED), r.errorCodes());
    assertDescriptionsContain(r, "expected 2 dimensions, not 3", "expected 2 dimensions, not 3");
    assertEquals(List.of(0, 1, 2, 2, 3, 4, 5, 5, 6, 7, 8), r.outputSizeAtRead);
    assertEquals(tableStoredExcept(10, 3, 7), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(10, 8, 2), r.counters());
    assertEquals(namesWithout(10, 3, 7), r.passedOnBeforeTheirCommit());
    assertEquals(List.of(), r.rejectedButCommitted());
  }

  /**
   * Buggy: commit size 0 is one transaction for the run, committed when the transform is disposed.
   * With an error hop every row runs behind a savepoint and is passed on long before that commit.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void errorHop_commitSizeZero_Buggy_oneTransactionThatIsCommittedAfterEveryRowIsPassedOn(
      String recovery) throws Exception {
    Result r =
        upsert().recovery(recovery).errorHop(true).commitSize(0).watchCommits().run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertTrue(r.useSafePoints);
    assertFalse(r.recoveredBatches);
    assertEquals(eventsInInputOrder(10, 3, 7), r.events);
    assertEquals(List.of(CODE_REFUSED, CODE_REFUSED), r.errorCodes());
    assertEquals(tableStoredExcept(10, 3, 7), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(10, 8, 2), r.counters());
    assertEquals(namesWithout(10, 3, 7), r.passedOnBeforeTheirCommit());
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: a stop before the commit rolls
   * back the rows the transform already passed on, so they are on the main output but not in the
   * table.
   */
  @Test
  void errorHop_legacy_Buggy_aStopRollsBackTheRowsThatWereAlreadyPassedOn() throws Exception {
    Result r =
        upsert()
            .recovery("LEGACY")
            .errorHop(true)
            .commitSize(100)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 5) {
                    transform.stopAll();
                  }
                })
            .run(rows(10));

    assertTrue(r.stopped);
    assertEquals(namesWithout(5), r.outputNames());
    assertEquals(List.of(), r.errorNames());
    assertEquals(List.of(), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(5, 5, 0), r.counters());
  }

  /**
   * Rows that store nothing (no content), and rows that fail before they reach the database (no
   * document id to build a key from, an embedding that is no JSON array), take their place between
   * the rows the database stores or refuses. The database refuses a vector of the wrong size, a
   * value its check constraint forbids and a value that is too long.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void errorHop_rowsThatAreNotStoredKeepTheirPlace(String recovery) throws Exception {
    Result r = upsert().recovery(recovery).errorHop(true).commitSize(3).run(mixedRows());

    assertEquals(0, r.errorCount);
    assertEquals(mixedRowsEvents(), r.events);
    assertEquals(
        List.of(
            CODE_BEFORE_THE_DATABASE,
            CODE_REFUSED,
            CODE_BEFORE_THE_DATABASE,
            CODE_REFUSED,
            CODE_REFUSED),
        r.errorCodes());
    assertDescriptionsContain(
        r,
        "No row ID could be built",
        "expected 2 dimensions, not 3",
        "Error preparing the pgvector row with id doc_6",
        "violates check constraint",
        "value too long for type character varying(20)");
    assertEquals(List.of("doc_1:r1", "doc_5:r5", "doc_9:r9"), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(9, 4, 5), r.counters());
  }

  /** doc_1..doc_9; see {@link #errorHop_rowsThatAreNotStoredKeepTheirPlace(String)}. */
  static List<Object[]> mixedRows() {
    List<Object[]> rows = new ArrayList<>();
    rows.add(row("doc", 1, "r1", EMBEDDING));
    rows.add(row("doc", 2, "", EMBEDDING));
    rows.add(row(null, 3, "r3", EMBEDDING));
    rows.add(row("doc", 4, "r4", WRONG_DIMENSION));
    rows.add(row("doc", 5, "r5", EMBEDDING));
    rows.add(row("doc", 6, "r6", "not json"));
    rows.add(row("doc", 7, BAD, EMBEDDING));
    rows.add(row("doc", 8, "much longer than twenty characters", EMBEDDING));
    rows.add(row("doc", 9, "r9", EMBEDDING));
    return rows;
  }

  static List<String> mixedRowsEvents() {
    return List.of(
        "out:doc_1",
        "out:doc_2",
        "err:null_3",
        "err:doc_4",
        "out:doc_5",
        "err:doc_6",
        "err:doc_7",
        "err:doc_8",
        "out:doc_9");
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: with "Delete document before
   * upsert" the delete of a document stays when its first row, the one that ran the delete, is
   * refused. The row is reported as refused, but half of what it did is in the table: the earlier
   * chunks of doc-a are gone and nothing replaces them.
   */
  @Test
  void errorHop_deleteBeforeUpsert_legacy_Buggy_aRefusedRowKeepsTheDeleteOfItsDocument()
      throws Exception {
    PgVectorUpsertRun.seed(db, TABLE, "doc-a", 3);
    PgVectorUpsertRun.seed(db, TABLE, "doc-b", 1);

    Result r =
        upsert()
            .recovery("LEGACY")
            .errorHop(true)
            .deleteDocumentBeforeUpsert()
            .run(List.of(row("doc-a", 0, "a0", WRONG_DIMENSION), row("doc-b", 0, "b0", EMBEDDING)));

    assertEquals(List.of("err:doc-a_0", "out:doc-b_0"), r.events);
    assertEquals(List.of(CODE_REFUSED), r.errorCodes());
    assertEquals(List.of("doc-b_0:b0"), PgVectorUpsertRun.table(db, TABLE));
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: the same for a row that fails
   * after its delete ran but before its own statement, here on an embedding that is no JSON array.
   */
  @Test
  void errorHop_deleteBeforeUpsert_legacy_Buggy_aRowThatCannotBeBoundKeepsTheDeleteOfItsDocument()
      throws Exception {
    PgVectorUpsertRun.seed(db, TABLE, "doc-a", 3);

    Result r =
        upsert()
            .recovery("LEGACY")
            .errorHop(true)
            .deleteDocumentBeforeUpsert()
            .run(List.of(row("doc-a", 0, "a0", "not json"), row("doc-b", 0, "b0", EMBEDDING)));

    assertEquals(List.of("err:doc-a_0", "out:doc-b_0"), r.events);
    assertEquals(List.of(CODE_BEFORE_THE_DATABASE), r.errorCodes());
    assertDescriptionsContain(r, "Error preparing the pgvector row with id doc-a_0");
    assertEquals(List.of("doc-b_0:b0"), PgVectorUpsertRun.table(db, TABLE));
  }

  /**
   * A document is replaced by those of its new chunks the database stores: its earlier chunks are
   * deleted once, before the first new one, and a refused chunk in the middle only takes itself.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void errorHop_deleteBeforeUpsert_aDocumentIsReplacedByTheChunksThatAreStored(String recovery)
      throws Exception {
    PgVectorUpsertRun.seed(db, TABLE, "doc-a", 5);
    PgVectorUpsertRun.seed(db, TABLE, "doc-b", 2);

    Result r =
        upsert()
            .recovery(recovery)
            .errorHop(true)
            .commitSize(3)
            .deleteDocumentBeforeUpsert()
            .run(
                List.of(
                    row("doc-a", 0, "a0", EMBEDDING),
                    row("doc-a", 1, "a1", WRONG_DIMENSION),
                    row("doc-a", 2, "a2", EMBEDDING),
                    row("doc-b", 0, "b0", EMBEDDING)));

    assertEquals(List.of("out:doc-a_0", "err:doc-a_1", "out:doc-a_2", "out:doc-b_0"), r.events);
    assertEquals(List.of(CODE_REFUSED), r.errorCodes());
    assertEquals(
        List.of("doc-a_0:a0", "doc-a_2:a2", "doc-b_0:b0"), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(4, 3, 1), r.counters());
  }

  /** The first chunk of a document is refused: the document is still replaced by the others. */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void errorHop_deleteBeforeUpsert_aDocumentWhoseFirstChunkIsRefusedIsReplacedByTheOthers(
      String recovery) throws Exception {
    PgVectorUpsertRun.seed(db, TABLE, "doc-a", 5);

    Result r =
        upsert()
            .recovery(recovery)
            .errorHop(true)
            .commitSize(3)
            .deleteDocumentBeforeUpsert()
            .run(
                List.of(
                    row("doc-a", 0, "a0", WRONG_DIMENSION),
                    row("doc-a", 1, "a1", EMBEDDING),
                    row("doc-a", 2, "a2", EMBEDDING)));

    assertEquals(List.of("err:doc-a_0", "out:doc-a_1", "out:doc-a_2"), r.events);
    assertEquals(List.of("doc-a_1:a1", "doc-a_2:a2"), PgVectorUpsertRun.table(db, TABLE));
  }

  /**
   * A delete the database refuses (a trigger here) only rejects the rows of its document, each of
   * which tries the delete again; the documents after it are replaced as usual.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void errorHop_deleteBeforeUpsert_aRefusedDeleteOnlyRejectsTheRowsOfItsDocument(String recovery)
      throws Exception {
    refuseDeletesOf("doc-guard");
    PgVectorUpsertRun.seed(db, TABLE, "doc-guard", 1);
    PgVectorUpsertRun.seed(db, TABLE, "doc-ok", 1);

    Result r =
        upsert()
            .recovery(recovery)
            .errorHop(true)
            .commitSize(3)
            .deleteDocumentBeforeUpsert()
            .run(
                List.of(
                    row("doc-guard", 1, "g1", EMBEDDING),
                    row("doc-guard", 2, "g2", EMBEDDING),
                    row("doc-ok", 1, "o1", EMBEDDING)));

    assertEquals(List.of("err:doc-guard_1", "err:doc-guard_2", "out:doc-ok_1"), r.events);
    assertEquals(List.of(CODE_BEFORE_THE_DATABASE, CODE_BEFORE_THE_DATABASE), r.errorCodes());
    assertDescriptionsContain(
        r,
        "Error deleting existing rows for document doc-guard",
        "Error deleting existing rows for document doc-guard");
    assertTrue(((String) r.errorDescriptions().get(0)).contains("refused by design"));
    assertEquals(List.of("doc-guard_0:old", "doc-ok_1:o1"), PgVectorUpsertRun.table(db, TABLE));
  }

  /** A trigger on the table that refuses to delete the rows of the given document. */
  void refuseDeletesOf(String document) throws Exception {
    PgVectorUpsertRun.execute(
        db,
        "CREATE OR REPLACE FUNCTION hop_it_refuse_delete() RETURNS trigger AS $$ BEGIN"
            + " IF OLD.document_id = '"
            + document
            + "' THEN RAISE EXCEPTION 'deletes of % are refused by design', OLD.document_id;"
            + " END IF; RETURN OLD; END; $$ LANGUAGE plpgsql",
        "CREATE TRIGGER hop_it_refuse_delete BEFORE DELETE ON "
            + TABLE
            + " FOR EACH ROW EXECUTE FUNCTION hop_it_refuse_delete()");
  }

  /**
   * Without an error hop the rows go out in batches and are passed on once their batch is
   * committed. A failed batch is rolled back and stops the transform.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void noErrorHop_aFailedBatchIsRolledBackAndStopsTheTransform(String recovery) throws Exception {
    Result r =
        upsert().recovery(recovery).errorHop(false).commitSize(4).watchCommits().run(rows(10, 7));

    assertTrue(r.batchMode);
    assertFalse(r.useSafePoints);
    assertFalse(r.recoveredBatches);
    assertNotNull(r.thrown);
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertEquals(namesWithout(4), r.outputNames());
    assertEquals(List.of(0, 0, 0, 0, 4, 4, 4, 4), r.outputSizeAtRead);
    assertEquals(List.of(), r.passedOnBeforeTheirCommit());
    assertEquals(tableStoredExcept(4), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(8, 4, 0), r.counters());
  }

  /**
   * Buggy: without an error hop a row that stores nothing (no content) is passed on at once, ahead
   * of the rows before it that wait in the batch.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void noErrorHop_Buggy_aRowThatStoresNothingOvertakesTheBatch(String recovery) throws Exception {
    List<Object[]> rows = rows(5);
    rows.set(2, row("doc", 3, "", EMBEDDING));

    Result r = upsert().recovery(recovery).errorHop(false).commitSize(100).run(rows);

    assertEquals(0, r.errorCount);
    assertEquals(
        List.of("out:doc_3", "out:doc_1", "out:doc_2", "out:doc_4", "out:doc_5"), r.events);
    assertEquals(tableStoredExcept(5, 3), PgVectorUpsertRun.table(db, TABLE));
  }

  /**
   * In a transactional pipeline the transform shares the connection: batching stays off with an
   * error hop, every row runs behind a savepoint, and the pipeline commits at the end.
   */
  @ParameterizedTest(name = "recovery {0}")
  @MethodSource("recoveries")
  void transactionalPipeline_errorHop_aSavepointPerRow(String recovery) throws Exception {
    Result r =
        upsert()
            .recovery(recovery)
            .errorHop(true)
            .commitSize(4)
            .connectionGroup("pgvector-" + UUID.randomUUID())
            .watchCommits()
            .run(rows(10, 3, 7));

    assertFalse(r.batchMode);
    assertTrue(r.useSafePoints);
    assertFalse(r.recoveredBatches);
    assertEquals(0, r.errorCount);
    assertEquals(eventsInInputOrder(10, 3, 7), r.events);
    assertEquals(Collections.nCopies(2, CODE_REFUSED), r.errorCodes());
    assertEquals(namesWithout(10, 3, 7), r.passedOnBeforeTheirCommit());
    assertEquals(tableStoredExcept(10, 3, 7), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(10, 8, 2), r.counters());
  }
}
