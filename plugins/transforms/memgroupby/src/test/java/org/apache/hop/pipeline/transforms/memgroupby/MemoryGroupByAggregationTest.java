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

package org.apache.hop.pipeline.transforms.memgroupby;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.TreeBasedTable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class MemoryGroupByAggregationTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private Variables variables;
  private Map<String, MemoryGroupByMeta.GroupType> aggregates;

  public static final String TRANSFORM_NAME = "testTransform";
  private static final ImmutableMap<String, MemoryGroupByMeta.GroupType> default_aggregates;

  static {
    default_aggregates =
        ImmutableMap.<String, MemoryGroupByMeta.GroupType>builder()
            .put("min", MemoryGroupByMeta.GroupType.Minimum)
            .put("max", MemoryGroupByMeta.GroupType.Maximum)
            .put("sum", MemoryGroupByMeta.GroupType.Sum)
            .put("ave", MemoryGroupByMeta.GroupType.Average)
            .put("count", MemoryGroupByMeta.GroupType.CountAll)
            .put("count_any", MemoryGroupByMeta.GroupType.CountAny)
            .put("count_distinct", MemoryGroupByMeta.GroupType.CountDistinct)
            .build();
  }

  private RowMeta rowMeta;
  private TreeBasedTable<Integer, Integer, Optional<Object>> data;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    rowMeta = new RowMeta();
    data = TreeBasedTable.create();
    variables = new Variables();
    variables.setVariable("test", "test");
    aggregates = Maps.newHashMap(default_aggregates);
  }

  @Test
  void testDefault() throws Exception {
    addColumn(new ValueMetaInteger("intg"), 0L, 1L, 1L, 10L);
    addColumn(new ValueMetaInteger("nul"));
    addColumn(new ValueMetaInteger("mix1"), -1L, 2L);
    addColumn(new ValueMetaInteger("mix2"), null, 7L);
    addColumn(new ValueMetaNumber("mix3"), -1.0, 2.5);
    addColumn(new ValueMetaDate("date1"), new Date(1L), new Date(2L));

    RowMetaAndData output = runTransform();

    assertEquals(0, output.getInteger("intg_min"));
    assertEquals(10, output.getInteger("intg_max"));
    assertEquals(12, output.getInteger("intg_sum"));
    assertEquals(3, output.getInteger("intg_ave"));
    assertEquals(4, output.getInteger("intg_count"));
    assertEquals(4, output.getInteger("intg_count_any"));
    assertEquals(3, output.getInteger("intg_count_distinct"));

    assertNull(output.getInteger("nul_min"));
    assertNull(output.getInteger("nul_max"));
    assertNull(output.getInteger("nul_sum"));
    assertNull(output.getInteger("nul_ave"));
    assertEquals(0, output.getInteger("nul_count"));
    assertEquals(4, output.getInteger("nul_count_any"));
    assertEquals(0, output.getInteger("nul_count_distinct"));

    assertEquals(2, output.getInteger("mix1_max"));
    assertEquals(-1, output.getInteger("mix1_min"));
    assertEquals(1, output.getInteger("mix1_sum"));
    assertEquals(0, output.getInteger("mix1_ave"));
    assertEquals(2, output.getInteger("mix1_count"));
    assertEquals(4, output.getInteger("mix1_count_any"));
    assertEquals(2, output.getInteger("mix1_count_distinct"));

    assertEquals(7, output.getInteger("mix2_max"));
    assertEquals(7, output.getInteger("mix2_min"));
    assertEquals(7, output.getInteger("mix2_sum"));
    assertEquals(7.0, output.getNumber("mix2_ave", Double.NaN));
    assertEquals(1, output.getInteger("mix2_count"));
    assertEquals(4, output.getInteger("mix2_count_any"));
    assertEquals(1, output.getInteger("mix2_count_distinct"));

    assertEquals(2.5, output.getNumber("mix3_max", Double.NaN));
    assertEquals(-1.0, output.getNumber("mix3_min", Double.NaN));
    assertEquals(1.5, output.getNumber("mix3_sum", Double.NaN));
    assertEquals(0.75, output.getNumber("mix3_ave", Double.NaN));
    assertEquals(2, output.getInteger("mix3_count"));
    assertEquals(4, output.getInteger("mix3_count_any"));
    assertEquals(2, output.getInteger("mix3_count_distinct"));

    assertEquals(1.0, output.getNumber("date1_min", Double.NaN));
    assertEquals(2.0, output.getNumber("date1_max", Double.NaN));
    assertEquals(3.0, output.getNumber("date1_sum", Double.NaN));
    assertEquals(1.5, output.getNumber("date1_ave", Double.NaN));
    assertEquals(2, output.getInteger("date1_count"));
    assertEquals(4, output.getInteger("date1_count_any"));
    assertEquals(2, output.getInteger("date1_count_distinct"));
  }

  @Test
  void testNullMin() throws Exception {
    variables.setVariable(Const.HOP_AGGREGATION_MIN_NULL_IS_VALUED, "Y");

    addColumn(new ValueMetaInteger("intg"), null, 0L, 1L, -1L);
    addColumn(new ValueMetaString("str"), "A", null, "B", null);

    aggregates = Maps.toMap(List.of("min", "max"), Functions.forMap(default_aggregates));

    RowMetaAndData output = runTransform();

    assertNull(output.getInteger("intg_min"));
    assertEquals(1, output.getInteger("intg_max"));

    assertNull(output.getString("str_min", null));
    assertEquals("B", output.getString("str_max", "invalid"));
  }

  @Test
  void testNullsAreZeroCompatible() throws Exception {
    variables.setVariable(Const.HOP_AGGREGATION_ALL_NULLS_ARE_ZERO, "Y");

    addColumn(new ValueMetaInteger("nul"));
    addColumn(new ValueMetaInteger("both"), -2L, 0L, null, 10L);

    RowMetaAndData output = runTransform();

    assertEquals(0, output.getInteger("nul_min"));
    assertEquals(0, output.getInteger("nul_max"));
    assertEquals(0, output.getInteger("nul_sum"));
    assertEquals(0, output.getInteger("nul_ave"));
    assertEquals(0, output.getInteger("nul_count"));
    assertEquals(4, output.getInteger("nul_count_any"));
    assertEquals(0, output.getInteger("nul_count_distinct"));

    assertEquals(10, output.getInteger("both_max"));
    assertEquals(-2, output.getInteger("both_min"));
    assertEquals(8, output.getInteger("both_sum"));
    assertEquals(2, output.getInteger("both_ave"));
    assertEquals(3, output.getInteger("both_count"));
    assertEquals(4, output.getInteger("both_count_any"));
    assertEquals(3, output.getInteger("both_count_distinct"));
  }

  @Test
  void testNullsAreZeroDefault() throws Exception {
    variables.setVariable(Const.HOP_AGGREGATION_ALL_NULLS_ARE_ZERO, "Y");

    addColumn(new ValueMetaInteger("nul"));
    addColumn(new ValueMetaInteger("both"), -2L, 0L, null, 10L);
    addColumn(new ValueMetaNumber("both_num"), -2.0, 0.0, null, 10.0);

    RowMetaAndData output = runTransform();

    assertEquals(0, output.getInteger("nul_min"));
    assertEquals(0, output.getInteger("nul_max"));
    assertEquals(0, output.getInteger("nul_sum"));
    assertEquals(0, output.getInteger("nul_ave"));
    assertEquals(0, output.getInteger("nul_count"));
    assertEquals(4, output.getInteger("nul_count_any"));
    assertEquals(0, output.getInteger("nul_count_distinct"));

    assertEquals(10, output.getInteger("both_max"));
    assertEquals(-2, output.getInteger("both_min"));
    assertEquals(8, output.getInteger("both_sum"));
    assertEquals(2, output.getInteger("both_ave"));
    assertEquals(3, output.getInteger("both_count"));
    assertEquals(4, output.getInteger("both_count_any"));
    assertEquals(3, output.getInteger("both_count_distinct"));

    assertEquals(10.0, output.getNumber("both_num_max", Double.NaN));
    assertEquals(-2.0, output.getNumber("both_num_min", Double.NaN));
    assertEquals(8.0, output.getNumber("both_num_sum", Double.NaN));
    assertEquals(2.666666, output.getNumber("both_num_ave", Double.NaN), 0.000001 /* delta */);
    assertEquals(3, output.getInteger("both_num_count"));
    assertEquals(4, output.getInteger("both_num_count_any"));
    assertEquals(3, output.getInteger("both_num_count_distinct"));
  }

  @Test
  void testSqlCompatible() throws Exception {
    addColumn(new ValueMetaInteger("value"), null, -2L, null, 0L, null, 10L, null, null, 0L, null);

    RowMetaAndData output = runTransform();

    assertEquals(10, output.getInteger("value_max"));
    assertEquals(-2, output.getInteger("value_min"));
    assertEquals(8, output.getInteger("value_sum"));
    assertEquals(2, output.getInteger("value_ave"));
    assertEquals(4, output.getInteger("value_count"));
    assertEquals(10, output.getInteger("value_count_any"));
    assertEquals(3, output.getInteger("value_count_distinct"));
  }

  private RowMetaAndData runTransform() throws HopException {
    // Allocate meta
    List<String> aggKeys = ImmutableList.copyOf(aggregates.keySet());
    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    for (int i = 0; i < rowMeta.size(); i++) {
      String name = rowMeta.getValueMeta(i).getName();
      for (int j = 0; j < aggKeys.size(); j++) {
        String aggKey = aggKeys.get(j);
        GAggregate aggregate = new GAggregate();
        aggregate.setField(name + "_" + aggKey);
        aggregate.setSubject(name);
        aggregate.setType(aggregates.get(aggKey));
        meta.getAggregates().add(aggregate);
      }
    }

    MemoryGroupByData data = new MemoryGroupByData();
    data.map = Maps.newHashMap();

    TransformMeta transformMeta = new TransformMeta(TRANSFORM_NAME, meta);
    PipelineMeta pipelineMeta = Mockito.mock(PipelineMeta.class);
    Pipeline pipeline = Mockito.spy(new LocalPipelineEngine());
    Mockito.when(pipelineMeta.findTransform(TRANSFORM_NAME)).thenReturn(transformMeta);

    // Spy on transform, regrettable but we need to easily inject rows
    MemoryGroupBy transform =
        spy(new MemoryGroupBy(transformMeta, meta, data, 0, pipelineMeta, pipeline));
    transform.copyFrom(variables);
    doNothing().when(transform).putRow((IRowMeta) any(), (Object[]) any());
    doNothing().when(transform).setOutputDone();

    // Process rows
    doReturn(rowMeta).when(transform).getInputRowMeta();
    for (Object[] row : getRows()) {
      doReturn(row).when(transform).getRow();
      assertTrue(transform.processRow());
    }
    verify(transform, never()).putRow((IRowMeta) any(), (Object[]) any());

    // Mark stop
    doReturn(null).when(transform).getRow();
    while (transform.processRow()) {
      // Run transform
    }
    verify(transform).setOutputDone();

    // Collect output
    ArgumentCaptor<IRowMeta> rowMetaCaptor = ArgumentCaptor.forClass(IRowMeta.class);
    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
    verify(transform).putRow(rowMetaCaptor.capture(), rowCaptor.capture());

    return new RowMetaAndData(rowMetaCaptor.getValue(), rowCaptor.getValue());
  }

  private void addColumn(IValueMeta meta, Object... values) {
    int column = rowMeta.size();

    rowMeta.addValueMeta(meta);
    for (int row = 0; row < values.length; row++) {
      data.put(row, column, Optional.fromNullable(values[row]));
    }
  }

  private Iterable<Object[]> getRows() {
    if (data.isEmpty()) {
      return new java.util.HashSet<>();
    }

    Range<Integer> rows = Range.closed(0, data.rowMap().lastKey());

    return FluentIterable.from(ContiguousSet.create(rows, DiscreteDomain.integers()))
        .transform(Functions.forMap(data.rowMap(), ImmutableMap.<Integer, Optional<Object>>of()))
        .transform(
            input -> {
              Object[] row = new Object[rowMeta.size()];
              for (Map.Entry<Integer, Optional<Object>> entry : input.entrySet()) {
                row[entry.getKey()] = entry.getValue().orNull();
              }
              return row;
            });
  }
}
