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

package org.apache.hop.pipeline.transforms.groupby;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the {@link GroupBy} transform worker. */
class GroupByTest {

  private TransformMockHelper<GroupByMeta, GroupByData> mockHelper;
  private GroupByMeta meta;
  private GroupByData data;
  private GroupBy groupBy;
  private IRowMeta inputRowMeta;

  @BeforeAll
  static void setUpClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws HopException {
    mockHelper = new TransformMockHelper<>("Group By", GroupByMeta.class, GroupByData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);

    meta = new GroupByMeta();
    meta.setDefault();
    meta.getGroupingFields().add(new GroupingField("grp"));
    meta.getAggregations()
        .add(
            new Aggregation(
                "sum_amount",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_SUM),
                null));
    meta.getAggregations()
        .add(
            new Aggregation(
                "cnt",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_COUNT_ALL),
                null));
    meta.getAggregations()
        .add(
            new Aggregation(
                "avg_amount",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_AVERAGE),
                null));

    data = new GroupByData();
    inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("grp"));
    inputRowMeta.addValueMeta(new ValueMetaNumber("amount"));

    groupBy = createGroupBy(meta, data);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void initPreparesBuffer() {
    assertTrue(groupBy.init());
    assertNotNullBuffer();
  }

  @Test
  void processRowAggregatesSingleGroup() throws Exception {
    List<Object[]> rows =
        runPipeline(new Object[] {"A", 10.0}, new Object[] {"A", 20.0}, new Object[] {"A", 30.0});

    assertEquals(1, rows.size());
    Object[] result = rows.getFirst();
    assertEquals("A", result[0]);
    assertEquals(60.0, ((Number) result[1]).doubleValue(), 1e-9);
    assertEquals(3L, ((Number) result[2]).longValue());
    assertEquals(20.0, ((Number) result[3]).doubleValue(), 1e-9);
  }

  @Test
  void processRowAggregatesMultipleGroups() throws Exception {
    List<Object[]> rows =
        runPipeline(
            new Object[] {"A", 10.0},
            new Object[] {"A", 20.0},
            new Object[] {"B", 5.0},
            new Object[] {"B", 15.0},
            new Object[] {"C", 100.0});

    assertEquals(3, rows.size());

    assertEquals("A", rows.getFirst()[0]);
    assertEquals(30.0, ((Number) rows.get(0)[1]).doubleValue(), 1e-9);
    assertEquals(2L, ((Number) rows.get(0)[2]).longValue());

    assertEquals("B", rows.get(1)[0]);
    assertEquals(20.0, ((Number) rows.get(1)[1]).doubleValue(), 1e-9);
    assertEquals(2L, ((Number) rows.get(1)[2]).longValue());

    assertEquals("C", rows.get(2)[0]);
    assertEquals(100.0, ((Number) rows.get(2)[1]).doubleValue(), 1e-9);
    assertEquals(1L, ((Number) rows.get(2)[2]).longValue());
  }

  @Test
  void processRowWithNoInputAndAlwaysGiveBackRow() throws Exception {
    meta.setAlwaysGivingBackOneRow(true);
    meta.getGroupingFields().clear();
    meta.getAggregations().clear();
    meta.getAggregations()
        .add(
            new Aggregation(
                "cnt",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_COUNT_ANY),
                null));

    data = new GroupByData();
    groupBy = createGroupBy(meta, data);

    QueueRowSet input = new QueueRowSet();
    input.setDone();
    groupBy.addRowSetToInputRowSets(input);
    groupBy.setInputRowMeta(inputRowMeta);

    QueueRowSet output = new QueueRowSet();
    groupBy.addRowSetToOutputRowSets(output);

    assertFalse(groupBy.processRow());

    List<Object[]> rows = drain(output);
    assertEquals(1, rows.size());
    assertEquals(0L, ((Number) rows.getFirst()[0]).longValue());
  }

  @Test
  void processRowWithNoInputAndNoAlwaysGiveBack() throws Exception {
    meta.setAlwaysGivingBackOneRow(false);

    QueueRowSet input = new QueueRowSet();
    input.setDone();
    groupBy.addRowSetToInputRowSets(input);

    QueueRowSet output = new QueueRowSet();
    groupBy.addRowSetToOutputRowSets(output);

    assertFalse(groupBy.processRow());
    assertTrue(drain(output).isEmpty());
  }

  @Test
  void processRowPassAllRowsKeepsInputAndAddsAggregates() throws Exception {
    meta.setPassAllRows(true);
    meta.getAggregations().clear();
    meta.getAggregations()
        .add(
            new Aggregation(
                "sum_amount",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_SUM),
                null));

    data = new GroupByData();
    groupBy = createGroupBy(meta, data);

    List<Object[]> rows =
        runPipeline(new Object[] {"A", 10.0}, new Object[] {"A", 20.0}, new Object[] {"B", 5.0});

    assertEquals(3, rows.size());

    // Input fields are preserved; aggregate is appended.
    assertEquals("A", rows.getFirst()[0]);
    assertEquals(10.0, ((Number) rows.get(0)[1]).doubleValue(), 1e-9);
    assertEquals(30.0, ((Number) rows.get(0)[2]).doubleValue(), 1e-9);

    assertEquals("A", rows.get(1)[0]);
    assertEquals(20.0, ((Number) rows.get(1)[1]).doubleValue(), 1e-9);
    assertEquals(30.0, ((Number) rows.get(1)[2]).doubleValue(), 1e-9);

    assertEquals("B", rows.get(2)[0]);
    assertEquals(5.0, ((Number) rows.get(2)[1]).doubleValue(), 1e-9);
    assertEquals(5.0, ((Number) rows.get(2)[2]).doubleValue(), 1e-9);
  }

  @Test
  void sameGroupComparesConfiguredGroupFields() throws Exception {
    data.inputRowMeta = inputRowMeta;
    data.groupnrs = new int[] {0};

    assertTrue(groupBy.sameGroup(new Object[] {"A", 1.0}, new Object[] {"A", 2.0}));
    assertFalse(groupBy.sameGroup(new Object[] {"A", 1.0}, new Object[] {"B", 1.0}));
  }

  @Test
  void calcAggregateUpdatesSumAndCount() throws Exception {
    data.inputRowMeta = inputRowMeta;
    data.subjectnrs = new int[] {1, 1};
    data.counts = new long[2];
    data.mean = new double[2];
    data.previousSums = new Object[0];
    data.previousAvgSum = new Object[0];
    data.previousAvgCount = new long[0];
    data.movingAvgWindows = new ArrayDeque[2];

    meta.getAggregations().clear();
    meta.getAggregations()
        .add(
            new Aggregation(
                "sum_amount",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_SUM),
                null));
    meta.getAggregations()
        .add(
            new Aggregation(
                "cnt",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_COUNT_ALL),
                null));

    groupBy.newAggregate(new Object[] {"A", 10.0});
    groupBy.calcAggregate(new Object[] {"A", 10.0});
    groupBy.calcAggregate(new Object[] {"A", 20.0});

    Object[] result = groupBy.getAggregateResult();
    assertEquals(30.0, ((Number) result[0]).doubleValue(), 1e-9);
    assertEquals(2L, ((Number) result[1]).longValue());
  }

  @Test
  void batchCompleteFlushesCurrentGroup() throws Exception {
    QueueRowSet input = new QueueRowSet();
    input.putRow(inputRowMeta, new Object[] {"A", 10.0});
    input.putRow(inputRowMeta, new Object[] {"A", 20.0});
    // Intentionally not done yet: batchComplete should flush the open group.
    groupBy.addRowSetToInputRowSets(input);

    QueueRowSet output = new QueueRowSet();
    groupBy.addRowSetToOutputRowSets(output);

    assertTrue(groupBy.processRow());
    assertTrue(groupBy.processRow());
    assertTrue(drain(output).isEmpty());

    groupBy.batchComplete();

    List<Object[]> rows = drain(output);
    assertEquals(1, rows.size());
    assertEquals("A", rows.getFirst()[0]);
    assertEquals(30.0, ((Number) rows.getFirst()[1]).doubleValue(), 1e-9);
    assertEquals(2L, ((Number) rows.getFirst()[2]).longValue());
    assertTrue(data.newBatch);
  }

  private GroupBy createGroupBy(GroupByMeta transformMeta, GroupByData transformData) {
    GroupBy transform =
        new GroupBy(
            mockHelper.transformMeta,
            transformMeta,
            transformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline) {
          @Override
          public String resolve(String str) {
            return str;
          }
        };
    transform.init();
    return transform;
  }

  private List<Object[]> runPipeline(Object[]... inputRows) throws HopException {
    QueueRowSet input = new QueueRowSet();
    for (Object[] inputRow : inputRows) {
      input.putRow(inputRowMeta, inputRow);
    }
    input.setDone();
    groupBy.addRowSetToInputRowSets(input);

    QueueRowSet output = new QueueRowSet();
    groupBy.addRowSetToOutputRowSets(output);

    while (groupBy.processRow()) {
      // drain all input
    }
    return drain(output);
  }

  private List<Object[]> drain(QueueRowSet output) {
    List<Object[]> rows = new ArrayList<>();
    Object[] row;
    while ((row = output.getRowImmediate()) != null) {
      rows.add(row);
    }
    return rows;
  }

  private void assertNotNullBuffer() {
    assertNotNull(data.bufferList);
  }
}
