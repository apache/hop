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

package org.apache.hop.pipeline.transforms.tableoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.RelationalWriteColumn;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Covers the lineage observation Table Output contributes: the per-column provenance it hands to a
 * sink, and the bookkeeping it does on the per-row path for the dynamic table-name mode.
 */
class TableOutputLineageTest {

  private TransformMockHelper<TableOutputMeta, TableOutputData> mockHelper;
  private TableOutputMeta meta;
  private TableOutputData data;
  private TableOutput transform;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("Table output", TableOutputMeta.class, TableOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    meta = mockHelper.iTransformMeta;
    data = new TableOutputData();
    transform =
        spy(
            new TableOutput(
                mockHelper.transformMeta,
                meta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  /**
   * With an explicit field mapping, each target column records the stream field it came from and
   * the transform that produced that field — the hook a sink follows back to the source column.
   */
  @Test
  void buildWriteColumns_mapsStreamFieldsToTargetColumnsWithOrigin() {
    when(meta.isSpecifyFields()).thenReturn(true);
    when(meta.getFields())
        .thenReturn(
            List.of(
                new TableOutputField("order_id", "id"),
                new TableOutputField("order_amount", "amount")));
    doReturn(rowMeta("Read orders_source", "id", "amount")).when(transform).getInputRowMeta();

    List<RelationalWriteColumn> columns = transform.buildWriteColumns();

    assertEquals(2, columns.size());
    assertEquals("order_id", columns.get(0).getTargetColumn());
    assertEquals("id", columns.get(0).getStreamField());
    assertEquals("Read orders_source", columns.get(0).getOriginTransform());
    assertEquals("order_amount", columns.get(1).getTargetColumn());
    assertEquals("amount", columns.get(1).getStreamField());
  }

  /**
   * Without an explicit mapping the whole input row is written 1:1, so names match on both sides.
   */
  @Test
  void buildWriteColumns_mapsWholeRowWhenFieldsAreNotSpecified() {
    when(meta.isSpecifyFields()).thenReturn(false);
    doReturn(rowMeta("Read orders_source", "id", "amount")).when(transform).getInputRowMeta();

    List<RelationalWriteColumn> columns = transform.buildWriteColumns();

    assertEquals(2, columns.size());
    assertEquals("id", columns.get(0).getTargetColumn());
    assertEquals("id", columns.get(0).getStreamField());
    assertEquals("Read orders_source", columns.get(0).getOriginTransform());
  }

  /** A target column with no matching stream field still maps, just without a resolvable origin. */
  @Test
  void buildWriteColumns_leavesOriginUnsetForUnresolvableStreamFields() {
    when(meta.isSpecifyFields()).thenReturn(true);
    when(meta.getFields()).thenReturn(List.of(new TableOutputField("order_id", "not_in_stream")));
    doReturn(rowMeta("Read orders_source", "id")).when(transform).getInputRowMeta();

    List<RelationalWriteColumn> columns = transform.buildWriteColumns();

    assertEquals(1, columns.size());
    assertEquals("order_id", columns.get(0).getTargetColumn());
    assertNull(columns.get(0).getOriginTransform());
  }

  /** No input row shape means no column lineage — never a failure. */
  @Test
  void buildWriteColumns_isEmptyWithoutAnInputRowShape() {
    doReturn(null).when(transform).getInputRowMeta();
    assertTrue(transform.buildWriteColumns().isEmpty());
  }

  /**
   * The dynamic-table bookkeeping sits on the per-row path, so with lineage off (the default) it
   * must not accumulate anything at all.
   */
  @Test
  void recordDynamicLineageTarget_doesNothingWhenLineageIsDisabled() {
    try (MockedStatic<LineageHub> hub = lineageEnabled(false)) {
      transform.recordDynamicLineageTarget("orders_2026_01");
    }

    assertTrue(data.dynamicTablesWritten.isEmpty());
  }

  /** With lineage on, distinct targets are collected — and blanks are ignored. */
  @Test
  void recordDynamicLineageTarget_collectsDistinctTargetsWhenEnabled() {
    try (MockedStatic<LineageHub> hub = lineageEnabled(true)) {
      transform.recordDynamicLineageTarget("orders_2026_01");
      transform.recordDynamicLineageTarget("orders_2026_02");
      transform.recordDynamicLineageTarget("orders_2026_01");
      transform.recordDynamicLineageTarget(null);
      transform.recordDynamicLineageTarget("");
    }

    assertEquals(2, data.dynamicTablesWritten.size());
    assertTrue(data.dynamicTablesWritten.contains("orders_2026_01"));
    assertFalse(data.dynamicLineageTruncated);
  }

  /**
   * The table name comes from a row field, so its cardinality is unbounded; the set must stop
   * growing rather than track every value a long run produces.
   */
  @Test
  void recordDynamicLineageTarget_stopsAccumulatingAtTheCap() {
    try (MockedStatic<LineageHub> hub = lineageEnabled(true)) {
      for (int i = 0; i < 1200; i++) {
        transform.recordDynamicLineageTarget("orders_" + i);
      }
    }

    assertEquals(1000, data.dynamicTablesWritten.size());
    assertTrue(data.dynamicLineageTruncated);
  }

  /**
   * Stubs the hub's on/off switch directly. Driving it through {@code HOP_LINEAGE_ENABLED} would
   * not be reliable here: {@code Variables.initializeFrom} applies HopConfig's described variables
   * *after* system properties, so the variable's declared default silently wins as soon as another
   * test in the same JVM has initialised the Hop environment.
   */
  private static MockedStatic<LineageHub> lineageEnabled(boolean enabled) {
    LineageHub hub = org.mockito.Mockito.mock(LineageHub.class);
    org.mockito.Mockito.when(hub.isEnabled()).thenReturn(enabled);
    MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class);
    staticHub.when(LineageHub::getInstance).thenReturn(hub);
    return staticHub;
  }

  private static IRowMeta rowMeta(String origin, String... fields) {
    RowMeta rowMeta = new RowMeta();
    for (String field : fields) {
      ValueMetaString valueMeta = new ValueMetaString(field);
      valueMeta.setOrigin(origin);
      rowMeta.addValueMeta(valueMeta);
    }
    return rowMeta;
  }
}
