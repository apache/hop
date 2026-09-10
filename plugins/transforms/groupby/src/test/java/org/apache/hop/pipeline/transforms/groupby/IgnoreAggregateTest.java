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
    assertEquals(0L, ((Number) result[2]).longValue());
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
