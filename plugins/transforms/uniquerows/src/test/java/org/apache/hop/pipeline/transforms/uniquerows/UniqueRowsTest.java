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

package org.apache.hop.pipeline.transforms.uniquerows;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class UniqueRowsTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<UniqueRowsMeta, UniqueRowsData> transformMockHelper;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    transformMockHelper =
        new TransformMockHelper<>("UniqueRows", UniqueRowsMeta.class, UniqueRowsData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
    when(transformMockHelper.transformMeta.getName()).thenReturn("UniqueRows");
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  void testInitWithErrorHandling() {
    // Setup error handling
    when(transformMockHelper.transformMeta.getTransformErrorMeta())
        .thenReturn(mock(org.apache.hop.pipeline.transform.TransformErrorMeta.class));
    when(transformMockHelper.iTransformMeta.supportsErrorHandling()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.isRejectDuplicateRow()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.getErrorDescription())
        .thenReturn("Duplicate row error");

    UniqueRows uniqueRows =
        new UniqueRows(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    boolean result = uniqueRows.init();

    assertTrue(result);
    assertTrue(transformMockHelper.iTransformData.sendDuplicateRows);
  }

  @Test
  void testInitWithoutErrorHandling() {
    // Setup without error handling
    when(transformMockHelper.transformMeta.getTransformErrorMeta()).thenReturn(null);
    when(transformMockHelper.iTransformMeta.supportsErrorHandling()).thenReturn(false);

    UniqueRows uniqueRows =
        new UniqueRows(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    boolean result = uniqueRows.init();

    assertTrue(result);
    assertFalse(transformMockHelper.iTransformData.sendDuplicateRows);
  }

  @Test
  void testProcessRowWithNoCompareFields() throws Exception {
    // Setup meta with no compare fields
    when(transformMockHelper.iTransformMeta.getCompareFields()).thenReturn(new ArrayList<>());
    when(transformMockHelper.iTransformMeta.isCountRows()).thenReturn(false);

    UniqueRows uniqueRows =
        new UniqueRows(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    assertTrue(uniqueRows.init());

    // Setup input row meta
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    inputRowMeta.addValueMeta(new ValueMetaInteger("age"));
    uniqueRows.setInputRowMeta(inputRowMeta);

    // Test first row - should not output anything yet
    uniqueRows = spy(uniqueRows);
    doReturn(new Object[] {"Alice", 30L}).when(uniqueRows).getRow();
    assertTrue(uniqueRows.processRow());
    // First row is stored but not output yet

    // Test second row (different) - should output previous row
    doReturn(new Object[] {"Bob", 25L}).when(uniqueRows).getRow();
    assertTrue(uniqueRows.processRow());
    verify(uniqueRows).putRow(any(IRowMeta.class), any(Object[].class));
  }

  @Test
  void testProcessRowWithCompareFields() throws Exception {
    // Setup meta with compare fields
    List<UniqueField> compareFields = new ArrayList<>();
    compareFields.add(new UniqueField("name", false));
    when(transformMockHelper.iTransformMeta.getCompareFields()).thenReturn(compareFields);
    when(transformMockHelper.iTransformMeta.isCountRows()).thenReturn(false);

    UniqueRows uniqueRows =
        new UniqueRows(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    assertTrue(uniqueRows.init());

    // Setup input row meta
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    inputRowMeta.addValueMeta(new ValueMetaInteger("age"));
    uniqueRows.setInputRowMeta(inputRowMeta);

    // Test first row - should not output anything yet
    uniqueRows = spy(uniqueRows);
    doReturn(new Object[] {"Alice", 30L}).when(uniqueRows).getRow();
    assertTrue(uniqueRows.processRow());
    // First row is stored but not output yet

    // Test second row (different name) - should output previous row
    doReturn(new Object[] {"Bob", 25L}).when(uniqueRows).getRow();
    assertTrue(uniqueRows.processRow());
    verify(uniqueRows).putRow(any(IRowMeta.class), any(Object[].class));
  }

  @Test
  void testProcessRowWithFieldNotFound() throws Exception {
    // Setup meta with a field that doesn't exist
    List<UniqueField> compareFields = new ArrayList<>();
    compareFields.add(new UniqueField("nonexistent", false));
    when(transformMockHelper.iTransformMeta.getCompareFields()).thenReturn(compareFields);

    UniqueRows uniqueRows =
        new UniqueRows(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    assertTrue(uniqueRows.init());

    // Setup input row meta
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    uniqueRows.setInputRowMeta(inputRowMeta);

    // Spy on the transform to mock getRow()
    uniqueRows = spy(uniqueRows);
    doReturn(new Object[] {"Alice"}).when(uniqueRows).getRow();

    // Execute processRow - should fail due to field not found
    boolean result = uniqueRows.processRow();

    assertFalse(result);
    // Verify that errors were set
    assertTrue(uniqueRows.getErrors() > 0);
  }

  @Test
  void testBatchComplete() throws Exception {
    when(transformMockHelper.iTransformMeta.getCompareFields()).thenReturn(new ArrayList<>());
    when(transformMockHelper.iTransformMeta.isCountRows()).thenReturn(false);

    UniqueRows uniqueRows =
        new UniqueRows(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    assertTrue(uniqueRows.init());

    // Setup input row meta
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    uniqueRows.setInputRowMeta(inputRowMeta);

    // Set up previous row in data
    transformMockHelper.iTransformData.previous = new Object[] {"Alice"};
    transformMockHelper.iTransformData.outputRowMeta = inputRowMeta;
    transformMockHelper.iTransformData.counter = 2;

    // Spy on the transform to mock putRow
    uniqueRows = spy(uniqueRows);

    // Call batchComplete
    uniqueRows.batchComplete();

    // Verify that putRow was called and previous was cleared
    verify(uniqueRows).putRow(any(IRowMeta.class), any(Object[].class));
    assertNull(transformMockHelper.iTransformData.previous);
  }

  @Test
  void testBatchCompleteWithNoPreviousRow() throws Exception {
    when(transformMockHelper.iTransformMeta.getCompareFields()).thenReturn(new ArrayList<>());

    UniqueRows uniqueRows =
        new UniqueRows(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    assertTrue(uniqueRows.init());

    // Ensure no previous row
    transformMockHelper.iTransformData.previous = null;

    // Spy on the transform to mock putRow
    uniqueRows = spy(uniqueRows);

    // Call batchComplete
    uniqueRows.batchComplete();

    // Verify that putRow was NOT called
    verify(uniqueRows, org.mockito.Mockito.never())
        .putRow(any(IRowMeta.class), any(Object[].class));
  }

  @Test
  void testProcessRowWithNullInput() throws Exception {
    when(transformMockHelper.iTransformMeta.getCompareFields()).thenReturn(new ArrayList<>());
    when(transformMockHelper.iTransformMeta.isCountRows()).thenReturn(false);

    UniqueRows uniqueRows =
        new UniqueRows(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    assertTrue(uniqueRows.init());

    // Setup input row meta
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    uniqueRows.setInputRowMeta(inputRowMeta);

    // Set up previous row in data
    transformMockHelper.iTransformData.previous = new Object[] {"Alice"};
    transformMockHelper.iTransformData.outputRowMeta = inputRowMeta;
    transformMockHelper.iTransformData.counter = 1;

    // Spy on the transform to mock getRow() to return null
    uniqueRows = spy(uniqueRows);
    doReturn(null).when(uniqueRows).getRow();

    // Execute processRow - should handle null input
    boolean result = uniqueRows.processRow();

    assertFalse(result);
    // Verify that the previous row was output
    verify(uniqueRows).putRow(any(IRowMeta.class), any(Object[].class));
  }
}
