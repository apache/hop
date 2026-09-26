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
package org.apache.hop.pgvector.transforms.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pgvector.util.PgVectorSearchFilter;
import org.apache.hop.pgvector.util.VectorDistanceMetric;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PgVectorSearchTest {

  private TransformMockHelper<PgVectorSearchMeta, PgVectorSearchData> helper;
  private List<Object[]> output;
  private IRowMeta outputRowMeta;

  /** SQL prepared on the stubbed connection, in order, with the statement handed out for it. */
  private Map<String, PreparedStatement> prepared;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "PgVectorSearch", PgVectorSearchMeta.class, PgVectorSearchData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    output = new ArrayList<>();
    prepared = new LinkedHashMap<>();
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void emitsOneRowPerMatch() throws Exception {
    PgVectorSearchMeta meta = newMeta();

    run(meta, "[0.1,0.2]", 2);

    assertEquals(2, output.size());
    assertEquals("doc-0", output.get(0)[outputRowMeta.indexOfValue("match_document_id")]);
    assertEquals(0L, output.get(0)[outputRowMeta.indexOfValue("match_chunk_index")]);
    assertEquals("doc-1", output.get(1)[outputRowMeta.indexOfValue("match_document_id")]);
  }

  /** Default behaviour follows Hop's Database Lookup: the row survives with empty match fields. */
  @Test
  void passesTheRowThroughWithEmptyMatchFieldsWhenNothingMatches() throws Exception {
    PgVectorSearchMeta meta = newMeta();

    run(meta, "[0.1,0.2]", 0);

    assertEquals(1, output.size());
    Object[] row = output.get(0);
    assertEquals("q1", row[outputRowMeta.indexOfValue("marker")]);
    assertNull(row[outputRowMeta.indexOfValue("match_id")]);
    assertNull(row[outputRowMeta.indexOfValue("match_content")]);
    assertNull(row[outputRowMeta.indexOfValue("match_score")]);
  }

  @Test
  void dropsTheRowWhenNothingMatchesAndEatingIsEnabled() throws Exception {
    PgVectorSearchMeta meta = newMeta();
    meta.setEatingRowOnNoMatch(true);

    run(meta, "[0.1,0.2]", 0);

    assertEquals(0, output.size());
  }

  /** An empty query vector is treated as "no match", not as a failure. */
  @Test
  void treatsAnEmptyEmbeddingAsNoMatch() throws Exception {
    PgVectorSearchMeta meta = newMeta();

    run(meta, "", 0);

    assertEquals(1, output.size());
    assertNull(output.get(0)[outputRowMeta.indexOfValue("match_id")]);
  }

  @Test
  void dropsAnEmptyEmbeddingRowWhenEatingIsEnabled() throws Exception {
    PgVectorSearchMeta meta = newMeta();
    meta.setEatingRowOnNoMatch(true);

    run(meta, "", 0);

    assertEquals(0, output.size());
  }

  @Test
  void skipsMatchesBelowTheMinimumScore() throws Exception {
    PgVectorSearchMeta meta = newMeta();
    meta.setMinScore("0.95");

    // The stub returns descending scores 0.9, 0.8 - both below the threshold.
    run(meta, "[0.1,0.2]", 2);

    assertEquals(1, output.size(), "no match survives the filter, so the row passes through empty");
    assertNull(output.get(0)[outputRowMeta.indexOfValue("match_id")]);
  }

  /** Without "skip if empty" an empty filter value is still bound, as it always has been. */
  @Test
  void bindsAnEmptyFilterValueWhenSkippingIsOff() throws Exception {
    runFiltered(false, new Object[] {"[0.1,0.2]", "q1", "acme", null});

    assertEquals(1, prepared.size());
    String sql = prepared.keySet().iterator().next();
    assertTrue(sql.contains("WHERE \"tenant\" = ? AND \"category\" = ?"), sql);
    PreparedStatement statement = prepared.get(sql);
    verify(statement).setString(2, "acme");
    verify(statement).setString(3, null);
    verify(statement).setInt(5, 5);
  }

  @Test
  void leavesAnEmptySkippableFilterOutOfTheQuery() throws Exception {
    runFiltered(true, new Object[] {"[0.1,0.2]", "q1", "acme", ""});

    String sql = lastPreparedSql();
    assertTrue(sql.contains("WHERE \"tenant\" = ? ORDER BY"), sql);
    assertFalse(sql.contains("\"category\""), sql);
    PreparedStatement statement = prepared.get(sql);
    verify(statement).setString(2, "acme");
    verify(statement).setString(3, "[0.1,0.2]");
    verify(statement).setInt(4, 5);
    assertEquals(1, output.size());
    assertEquals("doc-0", output.get(0)[outputRowMeta.indexOfValue("match_document_id")]);
  }

  @Test
  void appliesASkippableFilterWhenItHasAValue() throws Exception {
    runFiltered(true, new Object[] {"[0.1,0.2]", "q1", "acme", "manuals"});

    assertEquals(1, prepared.size(), "only the statement with every filter is needed");
    String sql = lastPreparedSql();
    assertTrue(sql.contains("WHERE \"tenant\" = ? AND \"category\" = ?"), sql);
    PreparedStatement statement = prepared.get(sql);
    verify(statement).setString(2, "acme");
    verify(statement).setString(3, "manuals");
  }

  /** Skipping only applies to filters that ask for it: an empty required filter still binds. */
  @Test
  void neverSkipsAFilterThatIsNotMarkedSkippable() throws Exception {
    runFiltered(true, new Object[] {"[0.1,0.2]", "q1", null, null});

    String sql = lastPreparedSql();
    assertTrue(sql.contains("WHERE \"tenant\" = ? ORDER BY"), sql);
    verify(prepared.get(sql)).setString(2, null);
  }

  /** Each combination of filters is prepared once and reused by later rows. */
  @Test
  void preparesEachFilterCombinationOnce() throws Exception {
    runFiltered(
        true,
        new Object[] {"[0.1,0.2]", "q1", "acme", "manuals"},
        new Object[] {"[0.1,0.2]", "q2", "acme", null},
        new Object[] {"[0.1,0.2]", "q3", "acme", "faq"},
        new Object[] {"[0.1,0.2]", "q4", "acme", ""});

    assertEquals(2, prepared.size());
    assertEquals(4, output.size());
  }

  private String lastPreparedSql() {
    return new ArrayList<>(prepared.keySet()).get(prepared.size() - 1);
  }

  private static PgVectorSearchMeta newMeta() {
    PgVectorSearchMeta meta = new PgVectorSearchMeta();
    meta.setDefault();
    meta.setConnection("pgvector-local");
    meta.setEmbeddingField("embedding");
    return meta;
  }

  private void run(PgVectorSearchMeta meta, String embedding, int matchCount) throws Exception {
    PgVectorSearchData data = newData(meta, new String[0]);
    data.searchStatements.put(new BitSet(), stubStatement(matchCount));
    process(meta, data, List.<Object[]>of(new Object[] {embedding, "q1"}));
  }

  /**
   * Runs rows of {@code embedding, marker, tenant, category} through a transform filtering on
   * tenant (always applied) and category (skipped when empty, if {@code skipEmptyCategory}), with
   * statements prepared on a stubbed connection.
   */
  private void runFiltered(boolean skipEmptyCategory, Object[]... rows) throws Exception {
    PgVectorSearchMeta meta = newMeta();
    meta.setFilters(
        List.of(
            new PgVectorSearchFilter("tenant", "tenant"),
            new PgVectorSearchFilter("category", "category", skipEmptyCategory)));
    PgVectorSearchData data = newData(meta, new String[] {"tenant", "category"});
    for (int i = 0; i < meta.getFilters().size(); i++) {
      data.filterBindings.add(
          new PgVectorSearchData.FilterBinding(2 + i, meta.getFilters().get(i)));
    }
    data.metric = VectorDistanceMetric.COSINE;
    data.qualifiedTable = "\"public\".\"chunks\"";
    Connection connection = mock(Connection.class);
    when(connection.prepareStatement(anyString()))
        .thenAnswer(
            invocation -> {
              PreparedStatement statement = stubStatement(1);
              prepared.put(invocation.getArgument(0), statement);
              return statement;
            });
    data.database = mock(Database.class);
    when(data.database.getConnection()).thenReturn(connection);

    process(meta, data, List.of(rows));
  }

  private PgVectorSearchData newData(PgVectorSearchMeta meta, String[] extraFields)
      throws Exception {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("embedding"));
    inputRowMeta.addValueMeta(new ValueMetaString("marker"));
    for (String field : extraFields) {
      inputRowMeta.addValueMeta(new ValueMetaString(field));
    }

    PgVectorSearchData data = new PgVectorSearchData();
    data.inputRowMeta = inputRowMeta;
    data.outputRowMeta = inputRowMeta.clone();
    meta.getFields(data.outputRowMeta, "search", null, null, new Variables(), null);
    data.embeddingFieldIndex = 0;
    data.resultIdFieldIndex = data.outputRowMeta.indexOfValue("match_id");
    data.resultDocumentIdFieldIndex = data.outputRowMeta.indexOfValue("match_document_id");
    data.resultChunkIndexFieldIndex = data.outputRowMeta.indexOfValue("match_chunk_index");
    data.resultContentFieldIndex = data.outputRowMeta.indexOfValue("match_content");
    data.resultScoreFieldIndex = data.outputRowMeta.indexOfValue("match_score");
    data.filterBindings = new ArrayList<>();
    return data;
  }

  private void process(PgVectorSearchMeta meta, PgVectorSearchData data, List<Object[]> input)
      throws Exception {
    PgVectorSearch transform =
        spy(
            new PgVectorSearch(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    transform.init();
    transform.setInputRowMeta(data.inputRowMeta);
    // The database is already stubbed, so skip the one-time setup branch in processRow().
    transform.first = false;

    Iterator<Object[]> rows = input.iterator();
    doAnswer(invocation -> rows.hasNext() ? rows.next() : null).when(transform).getRow();
    doAnswer(
            invocation -> {
              outputRowMeta = invocation.getArgument(0);
              output.add(invocation.getArgument(1));
              return null;
            })
        .when(transform)
        .putRow(any(IRowMeta.class), any(Object[].class));

    while (transform.processRow()) {
      // drain
    }
  }

  /** A PreparedStatement whose ResultSet yields {@code matchCount} descending-score rows. */
  private static PreparedStatement stubStatement(int matchCount) throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    int[] cursor = {-1};
    when(resultSet.next()).thenAnswer(invocation -> ++cursor[0] < matchCount);
    when(resultSet.getString("id")).thenAnswer(invocation -> "id-" + cursor[0]);
    when(resultSet.getString("document_id")).thenAnswer(invocation -> "doc-" + cursor[0]);
    when(resultSet.getLong("chunk_index")).thenAnswer(invocation -> (long) cursor[0]);
    when(resultSet.wasNull()).thenReturn(false);
    when(resultSet.getString("content")).thenAnswer(invocation -> "content-" + cursor[0]);
    when(resultSet.getDouble("similarity")).thenAnswer(invocation -> 0.9 - (0.1 * cursor[0]));

    PreparedStatement statement = mock(PreparedStatement.class);
    when(statement.executeQuery()).thenReturn(resultSet);
    doAnswer(invocation -> null).when(statement).setString(anyInt(), anyString());
    doAnswer(invocation -> null).when(statement).setInt(anyInt(), anyInt());
    return statement;
  }
}
