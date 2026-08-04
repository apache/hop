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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Covers the one piece of relational lineage Table Output still owns: the per-row bookkeeping for
 * the dynamic table-name mode. Everything static — the target table and its column mapping — is
 * declared on the metadata and derived by {@code LineageMetadataWalker}.
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
