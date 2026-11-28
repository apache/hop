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
package org.apache.hop.pgvector.transforms.upsert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.batch.FakeBatchDatabase;
import org.apache.hop.junit.batch.FakeBatchDatabase.Behaviour;
import org.apache.hop.pgvector.util.PgVectorSchemaBuilder;
import org.apache.hop.pgvector.util.PgVectorTableColumn;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Runs the upsert through processRow, batchComplete and dispose against an in-memory JDBC
 * connection that fails batches the way the drivers of each batch failure contract do, and checks
 * what the transform reports against what the database really committed.
 *
 * <p>The fake stores parameter 1 of every statement as its key: the id ({@code <document>_<chunk>})
 * of an upsert, and the document id of a delete. So its ledger holds exactly the rows and the
 * deletes that were committed.
 */
class PgVectorUpsertRecoveredBatchTest {

  private static final String CODE_BEFORE_THE_DATABASE = "PGVECTORUPSERT001";
  private static final String CODE_REFUSED = "PGVECTORUPSERT002";
  private static final String CODE_NOT_ATTEMPTED = "PGVECTORUPSERT003";

  private TransformMockHelper<PgVectorUpsertMeta, PgVectorUpsertData> helper;
  private PgVectorUpsertData data;
  private FakeBatchDatabase fake;

  /** "out:" or "err:" with the row's id, and for an error its code, in the order rows left. */
  private final List<String> events = new ArrayList<>();

  /** The output rows that were not committed when they were passed on. */
  private final List<String> passedBeforeTheirCommit = new ArrayList<>();

  /** The error rows that were committed when they were sent to the error hop. */
  private final List<String> rejectedButCommitted = new ArrayList<>();

  // How the next transform is set up
  private boolean errorHop = true;
  private int commitSize = 4;
  private BatchFailureContract declared = BatchFailureContract.LOSES_WHOLE_BATCH;
  private String recovery;
  private final Map<String, String> attributes = new HashMap<>();
  private boolean deleteDocumentBeforeUpsert;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "PgVectorUpsert", PgVectorUpsertMeta.class, PgVectorUpsertData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    Object[][] contracts = {
      {BatchFailureContract.LOSES_WHOLE_BATCH, Behaviour.LOSES_WHOLE_BATCH},
      {BatchFailureContract.MARKS_FAILED_ROWS, Behaviour.MARKS_FAILED_ROWS},
      {BatchFailureContract.KEEPS_PREFIX, Behaviour.KEEPS_PREFIX}
    };
    int[][] badSets = {{}, {0}, {3}, {9}, {2, 3}, {0, 4, 9}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}};
    for (Object[] contract : contracts) {
      for (int commitSize : new int[] {1, 4, 100}) {
        for (int[] bad : badSets) {
          arguments.add(Arguments.of(contract[0], contract[1], commitSize, bad));
        }
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "{0}, commit size {2}, bad rows {3}")
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(
      BatchFailureContract contract, Behaviour behaviour, int commitSize, int[] bad)
      throws Exception {
    declared = contract;
    this.commitSize = commitSize;
    String[] badIds = new String[bad.length];
    for (int i = 0; i < bad.length; i++) {
      badIds[i] = "doc_" + bad[i];
    }
    fake = new FakeBatchDatabase(behaviour).failOn((Object[]) badIds);
    PgVectorUpsert transform = transform(rows("doc", 10));

    drain(transform);
    transform.dispose();

    List<String> expected = new ArrayList<>();
    List<Object> stored = new ArrayList<>();
    for (int chunk = 0; chunk < 10; chunk++) {
      String id = "doc_" + chunk;
      if (List.of(badIds).contains(id)) {
        expected.add("err:" + id + ":" + CODE_REFUSED);
      } else {
        expected.add("out:" + id);
        stored.add(id);
      }
    }
    assertTrue(data.recoveredBatches);
    assertFalse(data.useSafePoints);
    assertEquals(expected, events);
    assertEquals(stored, sorted(fake.committed()));
    assertEquals(List.of(), passedBeforeTheirCommit);
    assertEquals(List.of(), rejectedButCommitted);
  }

  /** Without failures: one batch per commit size rows, no row on its own, no rollback. */
  @Test
  void aCleanRunSendsOneBatchPerCommitSize() throws Exception {
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH);
    PgVectorUpsert transform = transform(rows("doc", 10));

    drain(transform);
    transform.dispose();

    assertEquals(3, fake.getExecuteBatchCalls(), "4 + 4 + the partial last batch of 2");
    assertEquals(0, fake.getExecuteUpdateCalls());
    assertEquals(0, fake.getRollbacks());
    assertEquals(10, fake.committed().size());
    assertEquals(List.of(), passedBeforeTheirCommit);
  }

  /** A failed batch of 100 is worked out by bisecting it, not by a round trip per row. */
  @Test
  void aFailedBatchIsWorkedOutInAFewRoundTrips() throws Exception {
    commitSize = 100;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn("doc_63");
    PgVectorUpsert transform = transform(rows("doc", 100));

    drain(transform);
    transform.dispose();

    assertEquals(99, fake.committed().size());
    assertTrue(
        fake.getExecutions() <= 1 + 2 * 8,
        "about 2 log2(100) round trips: " + fake.getExecutions());
  }

  /**
   * Rows that store nothing, and rows that fail before they reach the database, wait behind the
   * batch rows that came in before them and leave in their place.
   */
  @Test
  void rowsOutsideTheBatchKeepTheirPlace() throws Exception {
    commitSize = 100;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn("doc_5");
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"doc", "1", "one", "[1,2]"});
    rows.add(new Object[] {"doc", "2", "", "[1,2]"});
    rows.add(new Object[] {null, "3", "three", "[1,2]"});
    rows.add(new Object[] {"doc", "x", "four", "[1,2]"});
    rows.add(new Object[] {"doc", "5", "five", "[1,2]"});
    rows.add(new Object[] {"doc", "6", "six", "not json"});
    rows.add(new Object[] {"doc", "7", "seven", "[1,2]"});
    PgVectorUpsert transform = transform(rows);

    drain(transform);
    transform.dispose();

    assertEquals(
        List.of(
            "out:doc_1",
            "out:doc_2",
            "err:null_3:" + CODE_BEFORE_THE_DATABASE,
            "err:doc_x:" + CODE_BEFORE_THE_DATABASE,
            "err:doc_5:" + CODE_REFUSED,
            "err:doc_6:" + CODE_BEFORE_THE_DATABASE,
            "out:doc_7"),
        events);
    assertEquals(List.of("doc_1", "doc_7"), sorted(fake.committed()));
  }

  /** The rows that wait behind a batch count towards the commit size, so memory stays bounded. */
  @Test
  void rowsThatWaitBehindTheBatchCountTowardsTheCommitSize() throws Exception {
    commitSize = 3;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"doc", "1", "one", "[1,2]"});
    rows.add(new Object[] {"doc", "2", "", "[1,2]"});
    rows.add(new Object[] {"doc", "3", "", "[1,2]"});
    rows.add(new Object[] {"doc", "4", "four", "[1,2]"});
    PgVectorUpsert transform = transform(rows);

    for (int i = 0; i < 3; i++) {
      assertTrue(transform.processRow());
    }
    assertEquals(List.of("out:doc_1", "out:doc_2", "out:doc_3"), events);
    assertTrue(data.pendingRows.isEmpty());
    drain(transform);
    transform.dispose();
    assertEquals(2, fake.getExecuteBatchCalls());
  }

  /** The single threaded engine ends an iteration with batchComplete: the batch runs there. */
  @Test
  void batchCompleteRunsTheBatch() throws Exception {
    commitSize = 10;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn("doc_1");
    PgVectorUpsert transform = transform(rows("doc", 5));

    for (int i = 0; i < 3; i++) {
      assertTrue(transform.processRow());
    }
    assertEquals(List.of(), events, "nothing leaves before its commit");
    transform.batchComplete();

    assertEquals(List.of("out:doc_0", "err:doc_1:" + CODE_REFUSED, "out:doc_2"), events);
    assertEquals(List.of("doc_0", "doc_2"), sorted(fake.committed()));
  }

  @Test
  void disposeAfterAStopDropsTheRowsThatWaitInTheBatch() throws Exception {
    commitSize = 10;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH);
    PgVectorUpsert transform = transform(rows("doc", 5));

    for (int i = 0; i < 5; i++) {
      assertTrue(transform.processRow());
    }
    doReturn(true).when(transform).isStopped();
    transform.dispose();

    assertEquals(List.of(), events, "rows that were never committed are not passed on");
    assertTrue(fake.committed().isEmpty());
    assertEquals(0, fake.getExecutions(), "the batch is never sent");
    verify(helper.iLogChannel).logError(contains("5 rows of the batch were not written"));
  }

  @Test
  void disposeAfterAnErrorDropsTheRowsThatWaitInTheBatch() throws Exception {
    commitSize = 10;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH);
    PgVectorUpsert transform = transform(rows("doc", 5));

    for (int i = 0; i < 5; i++) {
      assertTrue(transform.processRow());
    }
    transform.setErrors(1);
    transform.dispose();

    assertEquals(List.of(), events);
    assertTrue(fake.committed().isEmpty());
    assertEquals(0, fake.getExecutions());
  }

  /**
   * A connection without transactions whose driver throws without update counts: nothing says which
   * rows of the batch ran, and they can't be undone or retried, so the batch reports every row as
   * not attempted, and each gets the code of its own for that (the fake did write rows 0 and 1, as
   * such a driver may).
   */
  @Test
  void rowsTheDatabaseNeverRanGetACodeOfTheirOwn() throws Exception {
    declared = BatchFailureContract.KEEPS_PREFIX;
    fake = new FakeBatchDatabase(Behaviour.PARTIAL_WITHOUT_COUNTS, false).failOn("doc_2");
    PgVectorUpsert transform = transform(rows("doc", 4));

    drain(transform);
    transform.dispose();

    assertEquals(
        List.of(
            "err:doc_0:" + CODE_NOT_ATTEMPTED,
            "err:doc_1:" + CODE_NOT_ATTEMPTED,
            "err:doc_2:" + CODE_NOT_ATTEMPTED,
            "err:doc_3:" + CODE_NOT_ATTEMPTED),
        events);
  }

  // The cases that keep the handling as it was

  @Test
  void theLegacyVariableKeepsASavepointPerRow() throws Exception {
    recovery = BatchSupport.RECOVERY_LEGACY;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn("doc_2");
    PgVectorUpsert transform = transform(rows("doc", 5));

    drain(transform);
    transform.dispose();

    assertSavepointPerRow();
  }

  @Test
  void asBeforeOnTheConnectionKeepsASavepointPerRow() throws Exception {
    attributes.put(BatchSupport.ATTRIBUTE_BATCH_FAILURE_CONTRACT, "UNKNOWN");
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn("doc_2");
    PgVectorUpsert transform = transform(rows("doc", 5));

    drain(transform);
    transform.dispose();

    assertSavepointPerRow();
  }

  @Test
  void aDialectWithoutAContractKeepsASavepointPerRow() throws Exception {
    declared = null;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn("doc_2");
    PgVectorUpsert transform = transform(rows("doc", 5));

    drain(transform);
    transform.dispose();

    assertSavepointPerRow();
  }

  @Test
  void commitSizeZeroKeepsASavepointPerRow() throws Exception {
    commitSize = 0;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn("doc_2");
    PgVectorUpsert transform = transform(rows("doc", 5));

    drain(transform);
    transform.dispose();

    assertSavepointPerRow();
  }

  /** The single-row path as it was, Buggy: every row leaves before the commit that includes it. */
  private void assertSavepointPerRow() {
    assertFalse(data.recoveredBatches);
    assertTrue(data.useSafePoints);
    assertEquals(0, fake.getExecuteBatchCalls());
    assertEquals(
        List.of("out:doc_0", "out:doc_1", "err:doc_2:" + CODE_REFUSED, "out:doc_3", "out:doc_4"),
        events);
    assertEquals(List.of("doc_0", "doc_1", "doc_3", "doc_4"), passedBeforeTheirCommit);
  }

  @Test
  void withoutAnErrorHopTheRowsAreBatchedAsBefore() throws Exception {
    errorHop = false;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH);
    PgVectorUpsert transform = transform(rows("doc", 5));

    drain(transform);
    transform.dispose();

    assertFalse(data.recoveredBatches);
    assertTrue(data.batchMode);
    assertEquals(2, fake.getExecuteBatchCalls());
    assertEquals(5, fake.committed().size());
  }

  // Delete document before upsert

  /**
   * The delete and the upsert of a document's first row are committed together, or rolled back
   * together: a refused first row takes its delete with it, and the next row deletes again.
   */
  @Test
  void deleteBeforeUpsert_aRefusedFirstRowTakesItsDeleteWithIt() throws Exception {
    deleteDocumentBeforeUpsert = true;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn("doc-a_0");
    List<Object[]> rows = new ArrayList<>(rows("doc-a", 2));
    rows.addAll(rows("doc-b", 1));
    PgVectorUpsert transform = transform(rows);

    drain(transform);
    transform.dispose();

    assertEquals(List.of("err:doc-a_0:" + CODE_REFUSED, "out:doc-a_1", "out:doc-b_0"), events);
    assertEquals(List.of("doc-a", "doc-a_1", "doc-b", "doc-b_0"), List.copyOf(fake.committed()));
    assertEquals(List.of(), passedBeforeTheirCommit);
  }

  /** The batch before a new document is committed before its delete runs. */
  @Test
  void deleteBeforeUpsert_theRowsBeforeADocumentAreCommittedBeforeItsDelete() throws Exception {
    deleteDocumentBeforeUpsert = true;
    commitSize = 10;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn("doc-a_2");
    List<Object[]> rows = new ArrayList<>(rows("doc-a", 3));
    rows.addAll(rows("doc-b", 2));
    PgVectorUpsert transform = transform(rows);

    drain(transform);
    transform.dispose();

    assertEquals(
        List.of(
            "out:doc-a_0",
            "out:doc-a_1",
            "err:doc-a_2:" + CODE_REFUSED,
            "out:doc-b_0",
            "out:doc-b_1"),
        events);
    assertEquals(
        List.of("doc-a", "doc-a_0", "doc-a_1", "doc-b", "doc-b_0", "doc-b_1"),
        List.copyOf(fake.committed()));
    assertEquals(List.of(), passedBeforeTheirCommit);
  }

  /** A refused delete rejects its row only; every row of its document tries again. */
  @Test
  void deleteBeforeUpsert_aRefusedDeleteRejectsItsRow() throws Exception {
    deleteDocumentBeforeUpsert = true;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH).failOn("doc-a");
    List<Object[]> rows = new ArrayList<>(rows("doc-a", 2));
    rows.addAll(rows("doc-b", 2));
    PgVectorUpsert transform = transform(rows);

    drain(transform);
    transform.dispose();

    assertEquals(
        List.of(
            "err:doc-a_0:" + CODE_BEFORE_THE_DATABASE,
            "err:doc-a_1:" + CODE_BEFORE_THE_DATABASE,
            "out:doc-b_0",
            "out:doc-b_1"),
        events);
    assertEquals(List.of("doc-b", "doc-b_0", "doc-b_1"), List.copyOf(fake.committed()));
  }

  /** Above the tracked documents the row is rejected before anything runs. */
  @Test
  void deleteBeforeUpsert_refusesToDeleteADocumentOnceTheTrackedSetIsFull() throws Exception {
    deleteDocumentBeforeUpsert = true;
    fake = new FakeBatchDatabase(Behaviour.LOSES_WHOLE_BATCH);
    PgVectorUpsert transform = transform(rows("doc-new", 1));
    for (int i = 0; i < 1_000_000; i++) {
      data.deletedDocuments.add("filler-" + i);
    }

    drain(transform);
    transform.dispose();

    assertEquals(List.of("err:doc-new_0:" + CODE_BEFORE_THE_DATABASE), events);
    assertEquals(0, fake.getExecutions());
  }

  // Harness

  /**
   * The transform on the fake connection, set up the way openDatabase() does it, minus connecting.
   */
  private PgVectorUpsert transform(List<Object[]> rows) throws Exception {
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(errorHop);

    PgVectorUpsertMeta meta = new PgVectorUpsertMeta();
    meta.setDefault();
    meta.setDocumentIdField("document_id");
    meta.setChunkIndexField("chunk_index");
    meta.setContentField("content");
    meta.setEmbeddingField("embedding");
    meta.setCreateTableIfMissing(false);
    meta.setCommitSize(String.valueOf(commitSize));
    meta.setDeleteDocumentBeforeUpsert(deleteDocumentBeforeUpsert);

    IDatabase dialect = mock(IDatabase.class);
    when(dialect.isSupportsBatchUpdates()).thenReturn(true);
    when(dialect.getBatchFailureContract()).thenReturn(declared);
    when(dialect.getAttributes()).thenReturn(attributes);
    // A dialect that can't tell which entry of a failed batch failed answers -1.
    when(dialect.getFailedBatchEntry(any())).thenReturn(-1);
    when(dialect.isUseSafePoints()).thenReturn(true);
    when(dialect.isReleaseSavepoint()).thenReturn(true);
    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getIDatabase()).thenReturn(dialect);
    when(databaseMeta.getName()).thenReturn("fake");
    when(databaseMeta.supportsBatchUpdates()).thenReturn(true);

    Database database = new Database(null, new Variables(), databaseMeta);
    database.setConnection(fake.connection());

    data = new PgVectorUpsertData();
    data.inputRowMeta = inputRowMeta();
    data.documentIdFieldIndex = 0;
    data.chunkIndexFieldIndex = 1;
    data.contentFieldIndex = 2;
    data.embeddingFieldIndex = 3;
    data.tableColumns = PgVectorSchemaBuilder.tableColumns(meta);
    int parameterIndex = 1;
    for (PgVectorTableColumn column : data.tableColumns) {
      int streamIndex =
          switch (column.name()) {
            case "document_id" -> 0;
            case "chunk_index" -> 1;
            case "content" -> 2;
            case "embedding" -> 3;
            default -> -1;
          };
      data.mappingBindings.add(
          new PgVectorUpsertData.MappingBinding(streamIndex, parameterIndex++, column));
    }
    data.database = database;

    PgVectorUpsert transform =
        spy(
            new PgVectorUpsert(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    transform.init();
    if (recovery != null) {
      transform.setVariable("HOP_DATABASE_BATCH_RECOVERY", recovery);
    }
    transform.setInputRowMeta(data.inputRowMeta);
    // What openDatabase() does, minus connecting
    transform.configureCommitStrategy(dialect);
    database.setCommit(data.commitSize);
    data.insertStatement = fake.connection().prepareStatement("UPSERT");
    if (deleteDocumentBeforeUpsert) {
      data.deleteStatement = fake.connection().prepareStatement("DELETE");
    }
    transform.configureRecoveredBatches();
    transform.first = false;

    Iterator<Object[]> iterator = rows.iterator();
    doAnswer(invocation -> iterator.hasNext() ? iterator.next() : null).when(transform).getRow();
    doAnswer(
            invocation -> {
              Object[] row = invocation.getArgument(1);
              String id = id(row);
              events.add("out:" + id);
              if (stores(row) && !fake.committed().contains(id)) {
                passedBeforeTheirCommit.add(id);
              }
              return null;
            })
        .when(transform)
        .putRow(any(IRowMeta.class), any(Object[].class));
    doAnswer(
            invocation -> {
              Object[] row = invocation.getArgument(1);
              String id = id(row);
              events.add("err:" + id + ":" + invocation.getArgument(5));
              if (fake.committed().contains(id)) {
                rejectedButCommitted.add(id);
              }
              return null;
            })
        .when(transform)
        .putError(any(), any(), anyLong(), any(), any(), any());
    return transform;
  }

  private static void drain(PgVectorUpsert transform) throws HopException {
    while (transform.processRow()) {
      // drain
    }
  }

  /** Chunks 0..count-1 of a document. */
  private static List<Object[]> rows(String document, int count) {
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      rows.add(new Object[] {document, String.valueOf(i), "chunk " + i, "[1,2]"});
    }
    return rows;
  }

  private static String id(Object[] row) {
    return row[0] + "_" + row[1];
  }

  private static boolean stores(Object[] row) {
    return row[2] != null && !((String) row[2]).isEmpty();
  }

  private static List<Object> sorted(Collection<Object> keys) {
    List<Object> list = new ArrayList<>(keys);
    list.sort(null);
    return list;
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("document_id"));
    inputRowMeta.addValueMeta(new ValueMetaString("chunk_index"));
    inputRowMeta.addValueMeta(new ValueMetaString("content"));
    inputRowMeta.addValueMeta(new ValueMetaString("embedding"));
    return inputRowMeta;
  }
}
