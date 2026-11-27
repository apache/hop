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

import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertBatchBehaviourPostgresIT.CODE_BEFORE_THE_DATABASE;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertBatchBehaviourPostgresIT.CODE_REFUSED;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertBatchBehaviourPostgresIT.TABLE;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertBatchBehaviourPostgresIT.assertDescriptionsContain;
import static org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertBatchBehaviourPostgresIT.counters;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * The pgvector upsert on PostgreSQL, whose contract LOSES_WHOLE_BATCH is validated: a failed batch
 * keeps none of its rows and aborts the transaction. With an error hop the upserts are still
 * batched, without a savepoint per row, and a failed batch is recovered: every row reaches exactly
 * one stream, in input order, after the commit that includes it.
 */
class PgVectorUpsertRecoveredBatchPostgresIT {

  /** A dialect that extends PostgreSQL, as Greenplum, Redshift and CockroachDB do. */
  @DatabaseMetaPlugin(
      type = "POSTGRESQL_PGVECTOR_LOOKALIKE",
      typeDescription = "PostgreSQL look-alike")
  public static class PostgreSqlLookAlikeDatabaseMeta extends PostgreSqlDatabaseMeta {}

  private static PostgreSQLContainer<?> container;
  private DatabaseMeta db;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlLookAlikeDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    container = PgVectorUpsertBatchBehaviourPostgresIT.startPgvector();
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  @BeforeEach
  void createTable() throws Exception {
    db = PgVectorUpsertBatchBehaviourPostgresIT.databaseMeta(container);
    PgVectorUpsertRun.createTable(db, TABLE);
  }

  /** A connection of the given type, with the given batch failure contract set on it. */
  private static DatabaseMeta connection(String type, String contract) {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            PgVectorUpsertRun.CONNECTION,
            type,
            "Native",
            container.getHost(),
            container.getDatabaseName(),
            Integer.toString(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
            container.getUsername(),
            container.getPassword());
    if (contract != null) {
      databaseMeta.setBatchFailureContractOverride(contract);
    }
    return databaseMeta;
  }

  private PgVectorUpsertRun upsert() {
    return PgVectorUpsertRun.on(db, TABLE);
  }

  /** Every row passed on after the commit that includes it, and no refused row committed. */
  static void assertPassedOnAfterTheirCommit(Result r) {
    assertEquals(List.of(), r.passedOnBeforeTheirCommit());
    assertEquals(List.of(), r.rejectedButCommitted());
  }

  /** Recovered batches: batching on with the error hop, and no savepoint per row. */
  static void assertRecovered(Result r) {
    assertTrue(r.recoveredBatches);
    assertFalse(r.useSafePoints);
    assertFalse(r.batchMode);
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] badSets = {
      {}, {1}, {3}, {5}, {6}, {10}, {3, 4}, {2, 7}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}
    };
    for (int commitSize : new int[] {1, 3, 5, 100}) {
      for (long[] bad : badSets) {
        arguments.add(Arguments.of(commitSize, bad));
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "commit size {0}, bad rows {1}")
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(int commitSize, long[] bad) throws Exception {
    Result r = upsert().errorHop(true).commitSize(commitSize).watchCommits().run(rows(10, bad));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(namesWithout(10, bad), r.outputNames());
    assertEquals(names(bad), r.errorNames());
    assertEquals(eventsInInputOrder(10, bad), r.events);
    assertEquals(Collections.nCopies(bad.length, CODE_REFUSED), r.errorCodes());
    assertTrue(
        r.errorDescriptions().stream()
            .allMatch(d -> ((String) d).contains("expected 2 dimensions, not 3")),
        r.errorDescriptions().toString());
    assertEquals(tableStoredExcept(10, bad), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(10, 10 - bad.length, bad.length), r.counters());
    assertPassedOnAfterTheirCommit(r);
    assertRecovered(r);
  }

  /** The rows of a batch leave together, once it has run and is committed. */
  @Test
  void theRowsOfABatchLeaveTogether() throws Exception {
    Result r = upsert().errorHop(true).commitSize(4).run(rows(10, 3, 7));

    assertEquals(List.of(0, 0, 0, 0, 3, 3, 3, 3, 6, 6, 6), r.outputSizeAtRead);
    assertEquals(List.of(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2), r.errorSizeAtRead);
    assertEquals(eventsInInputOrder(10, 3, 7), r.events);
    assertTrue(r.loggedDetailed("Batch support: batch failure contract LOSES_WHOLE_BATCH"));
    assertRecovered(r);
  }

  /** The last batch holds fewer rows than the commit size: it runs at the end of the input. */
  @Test
  void thePartialLastBatchRunsAtTheEndOfTheInput() throws Exception {
    Result r = upsert().errorHop(true).commitSize(4).watchCommits().run(rows(10, 10));

    // 1-4 and 5-8 leave at their commit; 9 and 10 only once the input ends
    assertEquals(List.of(0, 0, 0, 0, 4, 4, 4, 4, 8, 8, 8), r.outputSizeAtRead);
    assertEquals(eventsInInputOrder(10, 10), r.events);
    assertEquals(tableStoredExcept(10, 10), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(10, 9, 1), r.counters());
    assertPassedOnAfterTheirCommit(r);
  }

  /**
   * A row that stores nothing and a row that fails before the database wait behind the batch rows
   * that came in before them, and leave in their place once the batch is committed.
   */
  @Test
  void rowsOutsideTheBatchKeepTheirPlace() throws Exception {
    Result r =
        upsert()
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(PgVectorUpsertBatchBehaviourPostgresIT.mixedRows());

    assertEquals(PgVectorUpsertBatchBehaviourPostgresIT.mixedRowsEvents(), r.events);
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
    // Rows 2 and 3 wait for the batch of row 1, then leave with it
    assertEquals(List.of(0, 0, 0, 2, 2, 2, 3, 3, 3, 4), r.outputSizeAtRead);
    assertPassedOnAfterTheirCommit(r);
    assertRecovered(r);
  }

  /** Larger batches with a few bad rows, as in a real load. */
  @Test
  void largerBatches() throws Exception {
    long[] bad = {1, 97, 194, 500, 501, 1_000, 1_999, 2_000};

    Result r = upsert().errorHop(true).commitSize(500).run(rows(2_000, bad));

    assertEquals(namesWithout(2_000, bad), r.outputNames());
    assertEquals(names(bad), r.errorNames());
    assertEquals(eventsInInputOrder(2_000, bad), r.events);
    assertEquals(tableStoredExcept(2_000, bad), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(2_000, 1_992, 8), r.counters());
    assertRecovered(r);
  }

  /** A stop drops the rows that wait in the batch: neither written, nor passed on. */
  @Test
  void aStopDropsTheRowsThatWaitInTheBatch() throws Exception {
    Result r =
        upsert()
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
    assertEquals(List.of(), r.events);
    assertEquals(List.of(), PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(5, 0, 0), r.counters());
    assertTrue(r.loggedError("5 rows of the batch were not written"), r.errorLog.toString());
    assertRecovered(r);
  }

  /** So does a failure: what was committed before stays, and was passed on. */
  @Test
  void aFailureDropsTheRowsThatWaitInTheBatch() throws Exception {
    Result r =
        upsert()
            .errorHop(true)
            .commitSize(4)
            .afterRow(
                (processed, transform) -> {
                  if (processed == 6) {
                    transform.setErrors(1);
                    transform.stopAll();
                  }
                })
            .run(rows(10, 2));

    assertEquals(List.of("out:doc_1", "err:doc_2", "out:doc_3", "out:doc_4"), r.events);
    assertEquals(tableStoredExcept(4, 2), PgVectorUpsertRun.table(db, TABLE));
    assertTrue(r.loggedError("2 rows of the batch were not written"), r.errorLog.toString());
  }

  /** The single threaded engine ends an iteration with batchComplete: the batch runs there. */
  @Test
  void batchCompleteRunsTheBatch() throws Exception {
    List<List<String>> tableAtBatchComplete = new ArrayList<>();
    List<Integer> outputAtBatchComplete = new ArrayList<>();
    Result r =
        upsert()
            .errorHop(true)
            .commitSize(100)
            .watchCommits()
            .afterRow(
                (processed, transform) -> {
                  if (processed == 3) {
                    transform.batchComplete();
                    tableAtBatchComplete.add(PgVectorUpsertRun.table(db, TABLE));
                    outputAtBatchComplete.add(transform.getLinesWritten() > 0 ? 1 : 0);
                  }
                })
            .run(rows(5, 2));

    assertEquals(List.of(tableStoredExcept(3, 2)), tableAtBatchComplete);
    assertEquals(List.of(1), outputAtBatchComplete);
    assertEquals(eventsInInputOrder(5, 2), r.events);
    assertEquals(tableStoredExcept(5, 2), PgVectorUpsertRun.table(db, TABLE));
    assertPassedOnAfterTheirCommit(r);
  }

  /** HOP_DATABASE_BATCH_RECOVERY=LEGACY goes back to a savepoint per row, batching off. */
  @Test
  void theLegacyVariableGoesBackToASavepointPerRow() throws Exception {
    Result r = upsert().recovery("LEGACY").errorHop(true).commitSize(4).run(rows(10, 3, 7));

    assertFalse(r.recoveredBatches);
    assertTrue(r.useSafePoints);
    assertEquals(eventsInInputOrder(10, 3, 7), r.events);
  }

  /** "As before" on the connection does the same, for that connection. */
  @Test
  void asBeforeOnTheConnectionGoesBackToASavepointPerRow() throws Exception {
    db = connection("POSTGRESQL", "UNKNOWN");

    Result r = upsert().errorHop(true).commitSize(4).watchCommits().run(rows(10, 3, 7));

    assertFalse(r.recoveredBatches);
    assertTrue(r.useSafePoints);
    assertTrue(r.loggedDetailed("batch failure contract UNKNOWN (from CONNECTION)"));
    assertEquals(eventsInInputOrder(10, 3, 7), r.events);
    // Today's single-row path, Buggy: every row leaves before its commit
    assertEquals(namesWithout(10, 3, 7), r.passedOnBeforeTheirCommit());
  }

  /** Commit size 0, one transaction for the run, keeps a savepoint per row. */
  @Test
  void commitSizeZeroKeepsASavepointPerRow() throws Exception {
    Result r = upsert().errorHop(true).commitSize(0).run(rows(10, 3, 7));

    assertFalse(r.recoveredBatches);
    assertTrue(r.useSafePoints);
    assertEquals(eventsInInputOrder(10, 3, 7), r.events);
  }

  /** Without an error hop the rows are batched as they always were. */
  @Test
  void withoutAnErrorHopTheBatchesStayAsTheyWere() throws Exception {
    Result r = upsert().errorHop(false).commitSize(4).run(rows(10));

    assertFalse(r.recoveredBatches);
    assertTrue(r.batchMode);
    assertTrue(r.loggedDetailed("batch failure contract LOSES_WHOLE_BATCH (from DIALECT)"));
    assertEquals(tableStoredExcept(10), PgVectorUpsertRun.table(db, TABLE));
  }

  /**
   * A dialect that extends PostgreSQL declares no contract and keeps a savepoint per row, unless
   * the contract is set on the connection.
   */
  @Test
  void aLookAlikeRecoversBatchesWithTheContractOnTheConnection() throws Exception {
    db = connection("POSTGRESQL_PGVECTOR_LOOKALIKE", null);
    Result before = upsert().errorHop(true).commitSize(4).run(rows(10, 3));
    assertFalse(before.recoveredBatches);
    assertTrue(before.useSafePoints);
    assertTrue(before.loggedDetailed("batch failure contract UNKNOWN (from NOT_DECLARED)"));

    PgVectorUpsertRun.createTable(db, TABLE);
    db = connection("POSTGRESQL_PGVECTOR_LOOKALIKE", "LOSES_WHOLE_BATCH");
    Result r = upsert().errorHop(true).commitSize(4).watchCommits().run(rows(10, 3, 7));

    assertEquals(eventsInInputOrder(10, 3, 7), r.events);
    assertEquals(tableStoredExcept(10, 3, 7), PgVectorUpsertRun.table(db, TABLE));
    assertPassedOnAfterTheirCommit(r);
    assertRecovered(r);
  }

  // Delete document before upsert

  /**
   * The first row of a document runs its delete and its upsert together and commits both, or, when
   * the database refuses the row, neither: doc-a keeps its earlier chunks when its only new chunk
   * is refused.
   */
  @Test
  void deleteBeforeUpsert_aRefusedFirstChunkLeavesItsDocumentAsItWas() throws Exception {
    PgVectorUpsertRun.seed(db, TABLE, "doc-a", 3);
    PgVectorUpsertRun.seed(db, TABLE, "doc-b", 1);

    Result r =
        upsert()
            .errorHop(true)
            .deleteDocumentBeforeUpsert()
            .watchCommits()
            .run(List.of(row("doc-a", 0, "a0", WRONG_DIMENSION), row("doc-b", 0, "b0", EMBEDDING)));

    assertEquals(List.of("err:doc-a_0", "out:doc-b_0"), r.events);
    assertEquals(List.of(CODE_REFUSED), r.errorCodes());
    assertDescriptionsContain(r, "expected 2 dimensions, not 3");
    assertEquals(
        List.of("doc-a_0:old", "doc-a_1:old", "doc-a_2:old", "doc-b_0:b0"),
        PgVectorUpsertRun.table(db, TABLE));
    assertPassedOnAfterTheirCommit(r);
    assertRecovered(r);
  }

  /** A row that can't be bound fails before its delete runs: its document stays as it was. */
  @Test
  void deleteBeforeUpsert_aRowThatCannotBeBoundLeavesItsDocumentAsItWas() throws Exception {
    PgVectorUpsertRun.seed(db, TABLE, "doc-a", 3);

    Result r =
        upsert()
            .errorHop(true)
            .deleteDocumentBeforeUpsert()
            .run(List.of(row("doc-a", 0, "a0", "not json"), row("doc-b", 0, "b0", EMBEDDING)));

    assertEquals(List.of("err:doc-a_0", "out:doc-b_0"), r.events);
    assertEquals(List.of(CODE_BEFORE_THE_DATABASE), r.errorCodes());
    assertDescriptionsContain(r, "Error preparing the pgvector row with id doc-a_0");
    assertEquals(
        List.of("doc-a_0:old", "doc-a_1:old", "doc-a_2:old", "doc-b_0:b0"),
        PgVectorUpsertRun.table(db, TABLE));
  }

  /**
   * Documents of several chunks, with refused chunks first, in the middle and last: each document
   * is replaced by the chunks that are stored, every row leaves in its place after its commit.
   */
  @Test
  void deleteBeforeUpsert_documentsAreReplacedByTheChunksThatAreStored() throws Exception {
    PgVectorUpsertRun.seed(db, TABLE, "doc-a", 6);
    PgVectorUpsertRun.seed(db, TABLE, "doc-b", 2);
    PgVectorUpsertRun.seed(db, TABLE, "doc-c", 2);

    Result r =
        upsert()
            .errorHop(true)
            .commitSize(2)
            .deleteDocumentBeforeUpsert()
            .watchCommits()
            .run(
                List.of(
                    row("doc-a", 0, "a0", WRONG_DIMENSION),
                    row("doc-a", 1, "a1", EMBEDDING),
                    row("doc-a", 2, "a2", WRONG_DIMENSION),
                    row("doc-a", 3, "a3", EMBEDDING),
                    row("doc-a", 4, "a4", EMBEDDING),
                    row("doc-b", 0, "b0", EMBEDDING),
                    row("doc-b", 1, "b1", WRONG_DIMENSION),
                    row("doc-c", 0, "c0", EMBEDDING)));

    assertEquals(
        List.of(
            "err:doc-a_0",
            "out:doc-a_1",
            "err:doc-a_2",
            "out:doc-a_3",
            "out:doc-a_4",
            "out:doc-b_0",
            "err:doc-b_1",
            "out:doc-c_0"),
        r.events);
    assertEquals(Collections.nCopies(3, CODE_REFUSED), r.errorCodes());
    assertEquals(
        List.of("doc-a_1:a1", "doc-a_3:a3", "doc-a_4:a4", "doc-b_0:b0", "doc-c_0:c0"),
        PgVectorUpsertRun.table(db, TABLE));
    assertEquals(counters(8, 5, 3), r.counters());
    assertPassedOnAfterTheirCommit(r);
    assertRecovered(r);
  }

  /**
   * A delete the database refuses is rolled back with the row; every row of its document tries it
   * again, and the documents after it are replaced as usual.
   */
  @Test
  void deleteBeforeUpsert_aRefusedDeleteOnlyRejectsTheRowsOfItsDocument() throws Exception {
    PgVectorUpsertRun.execute(
        db,
        "CREATE OR REPLACE FUNCTION hop_it_refuse_delete() RETURNS trigger AS $$ BEGIN"
            + " IF OLD.document_id = 'doc-guard' THEN"
            + " RAISE EXCEPTION 'deletes of % are refused by design', OLD.document_id;"
            + " END IF; RETURN OLD; END; $$ LANGUAGE plpgsql",
        "CREATE TRIGGER hop_it_refuse_delete BEFORE DELETE ON "
            + TABLE
            + " FOR EACH ROW EXECUTE FUNCTION hop_it_refuse_delete()");
    PgVectorUpsertRun.seed(db, TABLE, "doc-guard", 1);
    PgVectorUpsertRun.seed(db, TABLE, "doc-ok", 1);

    Result r =
        upsert()
            .errorHop(true)
            .commitSize(3)
            .deleteDocumentBeforeUpsert()
            .run(
                List.of(
                    row("doc-ok", 1, "o1", EMBEDDING),
                    row("doc-guard", 1, "g1", EMBEDDING),
                    row("doc-guard", 2, "g2", EMBEDDING),
                    row("doc-ok", 2, "o2", EMBEDDING)));

    assertEquals(
        List.of("out:doc-ok_1", "err:doc-guard_1", "err:doc-guard_2", "out:doc-ok_2"), r.events);
    assertEquals(List.of(CODE_BEFORE_THE_DATABASE, CODE_BEFORE_THE_DATABASE), r.errorCodes());
    assertDescriptionsContain(
        r,
        "Error deleting existing rows for document doc-guard",
        "Error deleting existing rows for document doc-guard");
    assertTrue(((String) r.errorDescriptions().get(0)).contains("refused by design"));
    assertEquals(
        List.of("doc-guard_0:old", "doc-ok_1:o1", "doc-ok_2:o2"),
        PgVectorUpsertRun.table(db, TABLE));
    assertRecovered(r);
  }
}
