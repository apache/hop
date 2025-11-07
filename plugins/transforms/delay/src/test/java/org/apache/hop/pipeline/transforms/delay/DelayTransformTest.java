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

package org.apache.hop.pipeline.transforms.delay;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class DelayTransformTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<DelayMeta, DelayData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper = new TransformMockHelper<>("Delay", DelayMeta.class, DelayData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void processRowTreatsNullTimeoutFieldAsZero() throws Exception {
    DelayMeta meta = new DelayMeta();
    meta.setTimeout("5");
    meta.setTimeoutField("timeoutField");

    DelayData data = new DelayData();
    Delay delay = createTransform(meta, data);
    assertTrue(delay.init());

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("timeoutField"));
    Object[] rowData = new Object[] {null};

    BlockingRowSet input = createInputRowSet(rowMeta, rowData);
    BlockingRowSet output = createOutputRowSet(rowMeta);

    delay.addRowSetToInputRowSets(input);
    delay.addRowSetToOutputRowSets(output);
    delay.setInputRowMeta(rowMeta);

    assertTrue(delay.processRow());
    Object[] result = output.getRowWait(1, TimeUnit.SECONDS);
    assertArrayEquals(rowData, result);
    assertEquals(0L, data.timeout);

    assertFalse(delay.processRow());
  }

  @Test
  void processRowUsesStringTimeoutField() throws Exception {
    DelayMeta meta = new DelayMeta();
    meta.setTimeoutField("timeoutField");
    meta.setScaleTimeCode(0); // milliseconds

    DelayData data = new DelayData();
    Delay delay = createTransform(meta, data);
    assertTrue(delay.init());

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("timeoutField"));
    Object[] rowData = new Object[] {"2"};

    BlockingRowSet input = createInputRowSet(rowMeta, rowData);
    BlockingRowSet output = createOutputRowSet(rowMeta);

    delay.addRowSetToInputRowSets(input);
    delay.addRowSetToOutputRowSets(output);
    delay.setInputRowMeta(rowMeta);

    assertTrue(delay.processRow());
    Object[] result = output.getRowWait(1, TimeUnit.SECONDS);
    assertArrayEquals(rowData, result);
    assertEquals(2L, data.timeout);

    assertFalse(delay.processRow());
  }

  @Test
  void processRowTreatsEmptyScaleFieldAsSeconds() throws Exception {
    DelayMeta meta = new DelayMeta();
    meta.setTimeoutField("timeout");
    meta.setScaleTimeFromField(true);
    meta.setScaleTimeField("scale");

    DelayData data = new DelayData();
    Delay delay = createTransform(meta, data);
    assertTrue(delay.init());

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("timeout"));
    rowMeta.addValueMeta(new ValueMetaString("scale"));
    Object[] rowData = new Object[] {null, ""};

    BlockingRowSet input = createInputRowSet(rowMeta, rowData);
    BlockingRowSet output = createOutputRowSet(rowMeta);

    delay.addRowSetToInputRowSets(input);
    delay.addRowSetToOutputRowSets(output);
    delay.setInputRowMeta(rowMeta);

    assertTrue(delay.processRow());
    Object[] result = output.getRowWait(1, TimeUnit.SECONDS);
    assertArrayEquals(rowData, result);
    assertEquals(0L, data.timeout);
    assertEquals(1000L, data.Multiple); // seconds

    assertFalse(delay.processRow());
  }

  @Test
  void processRowWithUnknownScaleThrows() throws Exception {
    DelayMeta meta = new DelayMeta();
    meta.setTimeoutField("timeout");
    meta.setScaleTimeFromField(true);
    meta.setScaleTimeField("scale");

    DelayData data = new DelayData();
    Delay delay = createTransform(meta, data);
    assertTrue(delay.init());

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("timeout"));
    rowMeta.addValueMeta(new ValueMetaString("scale"));
    Object[] rowData = new Object[] {1L, "weeks"};

    BlockingRowSet input = createInputRowSet(rowMeta, rowData);
    delay.addRowSetToInputRowSets(input);
    delay.setInputRowMeta(rowMeta);

    HopException exception = assertThrows(HopException.class, delay::processRow);
    String expectedMessage =
        BaseMessages.getString(
            DelayMeta.class, "Delay.Log.ScaleTimeFieldInvalid", meta.getScaleTimeField(), "weeks");
    assertEquals(expectedMessage.trim(), exception.getMessage().trim());
  }

  @Test
  void processRowWithNegativeTimeoutThrows() throws Exception {
    DelayMeta meta = new DelayMeta();
    meta.setTimeoutField("timeout");

    DelayData data = new DelayData();
    Delay delay = createTransform(meta, data);
    assertTrue(delay.init());

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("timeout"));
    Object[] rowData = new Object[] {-1L};

    BlockingRowSet input = createInputRowSet(rowMeta, rowData);
    delay.addRowSetToInputRowSets(input);
    delay.setInputRowMeta(rowMeta);

    HopException exception = assertThrows(HopException.class, delay::processRow);
    String expectedMessage =
        BaseMessages.getString(
            DelayMeta.class, "Delay.Log.TimeoutFieldNegative", meta.getTimeoutField(), -1L);
    assertEquals(expectedMessage.trim(), exception.getMessage().trim());
  }

  private Delay createTransform(DelayMeta meta, DelayData data) {
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    return new Delay(
        mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  private BlockingRowSet createInputRowSet(IRowMeta rowMeta, Object[]... rows) {
    BlockingRowSet rowSet = new BlockingRowSet(10);
    rowSet.setThreadNameFromToCopy("previous", 0, "Delay", 0);
    rowSet.setRowMeta(rowMeta);
    for (Object[] row : rows) {
      rowSet.putRow(rowMeta, row);
    }
    rowSet.setDone();
    return rowSet;
  }

  private BlockingRowSet createOutputRowSet(IRowMeta rowMeta) {
    BlockingRowSet rowSet = new BlockingRowSet(10);
    rowSet.setThreadNameFromToCopy("Delay", 0, "next", 0);
    rowSet.setRowMeta(rowMeta);
    return rowSet;
  }
}
