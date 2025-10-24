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

package org.apache.hop.pipeline.transforms.abort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class AbortTest {
  private TransformMockHelper<AbortMeta, AbortData> transformMockHelper;
  private Abort abort;

  @BeforeEach
  void setup() {
    transformMockHelper = new TransformMockHelper<>("ABORT TEST", AbortMeta.class, AbortData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);

    abort =
        new Abort(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  void testAbortDoesntAbortWithoutInputRow() throws HopException {
    abort.processRow();
    abort.addRowSetToInputRowSets(transformMockHelper.getMockInputRowSet());
    assertFalse(abort.isStopped());
    abort.processRow();
    verify(transformMockHelper.pipeline, never()).stopAll();
    assertFalse(abort.isStopped());
  }

  @Test
  void testAbortAbortsWithInputRow() throws HopException {
    abort.processRow();
    abort.addRowSetToInputRowSets(transformMockHelper.getMockInputRowSet(new Object[] {}));
    assertFalse(abort.isStopped());
    abort.processRow();
    verify(transformMockHelper.pipeline, times(1)).stopAll();
    assertTrue(abort.isStopped());
  }

  @Test
  void testAbortWithError() throws HopException {
    when(transformMockHelper.iTransformMeta.isSafeStop()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.isAbortWithError()).thenReturn(true);
    abort.processRow();
    abort.addRowSetToInputRowSets(transformMockHelper.getMockInputRowSet(new Object[] {}));
    abort.processRow();
    assertEquals(1L, abort.getErrors());
    verify(transformMockHelper.pipeline).stopAll();
  }

  @Test
  void testInitWithValidThreshold() {
    when(transformMockHelper.iTransformMeta.getRowThreshold()).thenReturn("10");
    boolean result = abort.init();

    assertTrue(result);
  }

  @Test
  void testInitWithInvalidThreshold() {
    try (MockedStatic<Const> constMock = mockStatic(Const.class)) {
      when(transformMockHelper.iTransformMeta.getRowThreshold()).thenReturn("abc");
      constMock.when(() -> Const.toInt("abc", -1)).thenReturn(-1);

      boolean result = abort.init();
      // init() returns true even if threshold invalid
      assertTrue(result);
    }
  }

  @Test
  void testProcessRowStopsAfterThreshold() throws Exception {
    when(transformMockHelper.iTransformMeta.getRowThreshold()).thenReturn("1");
    when(transformMockHelper.iTransformMeta.isAbortWithError()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.isAlwaysLogRows()).thenReturn(false);

    // Prepare transform
    boolean result = abort.init();
    assertTrue(result);

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("No"));
    abort.setInputRowMeta(inputRowMeta);

    // First row — should continue
    abort = spy(abort);
    doReturn(new Object[] {1000L}).doReturn(null).when(abort).getRow();

    boolean firstResult = abort.processRow();
    assertTrue(firstResult);

    // Second call — should detect no more rows
    boolean secondResult = abort.processRow();
    assertFalse(secondResult);
  }

  @Test
  void testProcessRowTriggersAbortCondition() throws Exception {
    when(transformMockHelper.iTransformMeta.getRowThreshold()).thenReturn("0");
    when(transformMockHelper.iTransformMeta.isAbortWithError()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.getMessage()).thenReturn("Abort now!");
    when(transformMockHelper.iTransformMeta.isAlwaysLogRows()).thenReturn(false);

    abort.init();

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("No"));
    abort.setInputRowMeta(inputRowMeta);

    // First row — should continue
    abort = spy(abort);
    doReturn(new Object[] {1000L}).doReturn(null).when(abort).getRow();

    abort.processRow();

    // Verify abort behavior
    verify(transformMockHelper.iTransformMeta, atLeastOnce()).isAbortWithError();
    verify(abort, atLeastOnce()).stopAll();
  }

  @Test
  void testProcessRowLogsRowWhenAlwaysLogRowsEnabled() throws Exception {
    when(transformMockHelper.iTransformMeta.getRowThreshold()).thenReturn("10");
    when(transformMockHelper.iTransformMeta.isAlwaysLogRows()).thenReturn(true);

    abort.init();

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("No"));
    abort.setInputRowMeta(inputRowMeta);

    // First row — should continue
    abort = spy(abort);
    doReturn(new Object[] {1000L}).doReturn(null).when(abort).getRow();

    // Should log at minimal level
    abort.processRow();
    verify(transformMockHelper.iTransformMeta).isAlwaysLogRows();
  }
}
