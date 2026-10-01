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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for Group By ignore_aggregate / field_ignore. */
class IgnoreAggregateTest {

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
    meta.setAggregateIgnored(true);
    meta.setAggregateIgnoredField("skip");
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

    data = new GroupByData();
    inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("grp"));
    inputRowMeta.addValueMeta(new ValueMetaNumber("amount"));
    inputRowMeta.addValueMeta(new ValueMetaBoolean("skip"));

    groupBy =
        new GroupBy(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline) {
          @Override
          public String resolve(String str) {
            return str;
          }
        };
    groupBy.init();
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void isRowAggregateIgnoredRespectsBooleanField() throws Exception {
    data.inputRowMeta = inputRowMeta;
    data.aggregateIgnoredFieldIndex = 2;

    assertFalse(groupBy.isRowAggregateIgnored(new Object[] {"A", 10.0, Boolean.FALSE}));
    assertTrue(groupBy.isRowAggregateIgnored(new Object[] {"A", 100.0, Boolean.TRUE}));
    assertFalse(groupBy.isRowAggregateIgnored(new Object[] {"A", 10.0, null}));
  }

  @Test
  void isRowAggregateIgnoredDisabledWhenIndexUnset() throws Exception {
    data.inputRowMeta = inputRowMeta;
    data.aggregateIgnoredFieldIndex = -1;

    assertFalse(groupBy.isRowAggregateIgnored(new Object[] {"A", 100.0, Boolean.TRUE}));
  }

  @Test
  void processRowSkipsIgnoredRowsInAggregation() throws Exception {
    List<Object[]> rows =
        runPipeline(
            new Object[] {"A", 10.0, Boolean.FALSE},
            new Object[] {"A", 20.0, Boolean.FALSE},
            new Object[] {"A", 30.0, Boolean.FALSE},
            new Object[] {"A", 100.0, Boolean.TRUE});

    assertEquals(1, rows.size());
    Object[] result = rows.getFirst();
    assertEquals("A", result[0]);
    assertEquals(60.0, ((Number) result[1]).doubleValue(), 1e-9);
    assertEquals(3L, ((Number) result[2]).longValue());
  }

  @Test
  void processRowIncludesAllRowsWhenIgnoreDisabled() throws Exception {
    meta.setAggregateIgnored(false);
    meta.setAggregateIgnoredField(null);

    List<Object[]> rows =
        runPipeline(
            new Object[] {"A", 10.0, Boolean.FALSE},
            new Object[] {"A", 20.0, Boolean.FALSE},
            new Object[] {"A", 30.0, Boolean.FALSE},
            new Object[] {"A", 100.0, Boolean.TRUE});

    assertEquals(1, rows.size());
    Object[] result = rows.getFirst();
    assertEquals(160.0, ((Number) result[1]).doubleValue(), 1e-9);
    assertEquals(4L, ((Number) result[2]).longValue());
  }

  @Test
  void processRowSkipsIgnoredRowsPerGroup() throws Exception {
    List<Object[]> rows =
        runPipeline(
            new Object[] {"A", 10.0, Boolean.FALSE},
            new Object[] {"A", 20.0, Boolean.FALSE},
            new Object[] {"A", 30.0, Boolean.FALSE},
            new Object[] {"A", 100.0, Boolean.TRUE},
            new Object[] {"B", 5.0, Boolean.FALSE},
            new Object[] {"B", 50.0, Boolean.TRUE});

    assertEquals(2, rows.size());

    Object[] groupA = rows.getFirst();
    assertEquals("A", groupA[0]);
    assertEquals(60.0, ((Number) groupA[1]).doubleValue(), 1e-9);
    assertEquals(3L, ((Number) groupA[2]).longValue());

    Object[] groupB = rows.get(1);
    assertEquals("B", groupB[0]);
    assertEquals(5.0, ((Number) groupB[1]).doubleValue(), 1e-9);
    assertEquals(1L, ((Number) groupB[2]).longValue());
  }

  @Test
  void processRowIgnoresLastRowWhenFlagged() throws Exception {
    List<Object[]> rows =
        runPipeline(
            new Object[] {"A", 10.0, Boolean.FALSE},
            new Object[] {"A", 20.0, Boolean.FALSE},
            new Object[] {"A", 100.0, Boolean.TRUE});

    assertEquals(1, rows.size());
    Object[] result = rows.getFirst();
    assertEquals(30.0, ((Number) result[1]).doubleValue(), 1e-9);
    assertEquals(2L, ((Number) result[2]).longValue());
  }

  @Test
  void processRowWithOnlyIgnoredRowsStillEmitsGroup() throws Exception {
    List<Object[]> rows =
        runPipeline(
            new Object[] {"A", 100.0, Boolean.TRUE}, new Object[] {"A", 200.0, Boolean.TRUE});

    assertEquals(1, rows.size());
    Object[] result = rows.getFirst();
    assertEquals("A", result[0]);
    assertNull(result[1]); // sum stays null when every row is ignored
    assertEquals(0L, ((Number) result[2]).longValue());
  }

  @Test
  void processRowSkipsFirstRowWhenFlaggedForMinMaxFirstLast() throws Exception {
    configureAggregations(
        Aggregation.TYPE_GROUP_MIN,
        Aggregation.TYPE_GROUP_MAX,
        Aggregation.TYPE_GROUP_FIRST,
        Aggregation.TYPE_GROUP_LAST,
        Aggregation.TYPE_GROUP_FIRST_INCL_NULL,
        Aggregation.TYPE_GROUP_LAST_INCL_NULL);

    // Group A: ignored first row holds extreme values; valid rows are 10 and 20
    // Group B: ignored first row holds extremes; valid rows are 10 and 20
    List<Object[]> rows =
        runPipeline(
            new Object[] {"A", 999.0, Boolean.TRUE},
            new Object[] {"A", 10.0, Boolean.FALSE},
            new Object[] {"A", 20.0, Boolean.FALSE},
            new Object[] {"B", 1.0, Boolean.TRUE},
            new Object[] {"B", 10.0, Boolean.FALSE},
            new Object[] {"B", 20.0, Boolean.FALSE});

    assertEquals(2, rows.size());

    Object[] groupA = rows.getFirst();
    assertEquals("A", groupA[0]);
    assertEquals(10.0, ((Number) groupA[1]).doubleValue(), 1e-9); // min
    assertEquals(20.0, ((Number) groupA[2]).doubleValue(), 1e-9); // max
    assertEquals(10.0, ((Number) groupA[3]).doubleValue(), 1e-9); // first
    assertEquals(20.0, ((Number) groupA[4]).doubleValue(), 1e-9); // last
    assertEquals(10.0, ((Number) groupA[5]).doubleValue(), 1e-9); // first incl null
    assertEquals(20.0, ((Number) groupA[6]).doubleValue(), 1e-9); // last incl null

    Object[] groupB = rows.get(1);
    assertEquals("B", groupB[0]);
    assertEquals(10.0, ((Number) groupB[1]).doubleValue(), 1e-9); // min (not 1.0)
    assertEquals(20.0, ((Number) groupB[2]).doubleValue(), 1e-9); // max
    assertEquals(10.0, ((Number) groupB[3]).doubleValue(), 1e-9); // first (not 1.0)
    assertEquals(20.0, ((Number) groupB[4]).doubleValue(), 1e-9); // last
    assertEquals(10.0, ((Number) groupB[5]).doubleValue(), 1e-9); // first incl null
    assertEquals(20.0, ((Number) groupB[6]).doubleValue(), 1e-9); // last incl null
  }

  @Test
  void processRowAllIgnoredLeavesMinMaxFirstNull() throws Exception {
    configureAggregations(
        Aggregation.TYPE_GROUP_MIN,
        Aggregation.TYPE_GROUP_MAX,
        Aggregation.TYPE_GROUP_FIRST,
        Aggregation.TYPE_GROUP_FIRST_INCL_NULL,
        Aggregation.TYPE_GROUP_COUNT_ALL);

    List<Object[]> rows =
        runPipeline(
            new Object[] {"A", 100.0, Boolean.TRUE}, new Object[] {"A", 200.0, Boolean.TRUE});

    assertEquals(1, rows.size());
    Object[] result = rows.getFirst();
    assertEquals("A", result[0]);
    assertNull(result[1]); // min
    assertNull(result[2]); // max
    assertNull(result[3]); // first
    assertNull(result[4]); // first incl null
    assertEquals(0L, ((Number) result[5]).longValue()); // count
  }

  @Test
  void processRowPassAllRowsSkipsIgnoredInCumulativeSum() throws Exception {
    meta.setPassAllRows(true);
    meta.getAggregations().clear();
    meta.getAggregations()
        .add(
            new Aggregation(
                "cum_sum",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_CUMULATIVE_SUM),
                null));
    meta.getAggregations()
        .add(
            new Aggregation(
                "sum_amount",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_SUM),
                null));

    List<Object[]> rows =
        runPipeline(
            new Object[] {"A", 10.0, Boolean.FALSE},
            new Object[] {"A", 100.0, Boolean.TRUE},
            new Object[] {"A", 20.0, Boolean.FALSE});

    assertEquals(3, rows.size());

    // output: grp, amount, skip, cum_sum, sum_amount
    assertEquals(10.0, ((Number) rows.get(0)[3]).doubleValue(), 1e-9);
    assertEquals(10.0, ((Number) rows.get(1)[3]).doubleValue(), 1e-9); // ignored: carry previous
    assertEquals(30.0, ((Number) rows.get(2)[3]).doubleValue(), 1e-9);

    assertEquals(30.0, ((Number) rows.get(0)[4]).doubleValue(), 1e-9); // group sum on every row
    assertEquals(30.0, ((Number) rows.get(1)[4]).doubleValue(), 1e-9);
    assertEquals(30.0, ((Number) rows.get(2)[4]).doubleValue(), 1e-9);
  }

  @Test
  void processRowPassAllRowsSkipsIgnoredInMovingAverage() throws Exception {
    meta.setPassAllRows(true);
    meta.getAggregations().clear();
    meta.getAggregations()
        .add(
            new Aggregation(
                "mov_avg",
                "amount",
                Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_MOVING_AVERAGE),
                "2",
                null));

    List<Object[]> rows =
        runPipeline(
            new Object[] {"A", 10.0, Boolean.FALSE},
            new Object[] {"A", 100.0, Boolean.TRUE},
            new Object[] {"A", 20.0, Boolean.FALSE});

    assertEquals(3, rows.size());
    // window size 2: after first valid row incomplete; ignored does not enter window;
    // after second valid row window is [10, 20] -> 15
    assertNull(rows.get(0)[3]);
    assertNull(rows.get(1)[3]); // ignored: still incomplete window
    assertEquals(15.0, ((Number) rows.get(2)[3]).doubleValue(), 1e-9);
  }

  @Test
  void processRowFailsWhenIgnoreEnabledButFieldBlank() throws Exception {
    meta.setAggregateIgnoredField("");

    QueueRowSet input = new QueueRowSet();
    input.putRow(inputRowMeta, new Object[] {"A", 10.0, Boolean.FALSE});
    input.setDone();
    groupBy.addRowSetToInputRowSets(input);
    groupBy.addRowSetToOutputRowSets(new QueueRowSet());

    assertFalse(groupBy.processRow());
    assertTrue(groupBy.getErrors() > 0);
  }

  private void configureAggregations(int... types) {
    meta.getAggregations().clear();
    for (int i = 0; i < types.length; i++) {
      meta.getAggregations()
          .add(
              new Aggregation(
                  "agg" + i, "amount", Aggregation.getTypeDescLongFromCode(types[i]), null));
    }
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

    List<Object[]> rows = new ArrayList<>();
    Object[] row;
    while ((row = output.getRowImmediate()) != null) {
      rows.add(row);
    }
    return rows;
  }
}
