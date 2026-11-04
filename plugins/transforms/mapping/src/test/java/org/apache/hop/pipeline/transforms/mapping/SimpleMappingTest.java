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
package org.apache.hop.pipeline.transforms.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.Const;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.input.MappingInput;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.output.MappingOutput;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SimpleMappingTest {

  private static final String MAPPING_INPUT_TRANSFORM_NAME = "MAPPING_INPUT_TRANSFORM_NAME";

  private static final String MAPPING_OUTPUT_TRANSFORM_NAME = "MAPPING_OUTPUT_TRANSFORM_NAME";

  private TransformMockHelper<SimpleMappingMeta, SimpleMappingData> transformMockHelper;

  // Using real SimpleMappingData object
  private SimpleMappingData simpleMpData = new SimpleMappingData();

  private SimpleMapping smp;

  @BeforeEach
  void setup() throws Exception {
    transformMockHelper =
        new TransformMockHelper<>(
            "SIMPLE_MAPPING_TEST", SimpleMappingMeta.class, SimpleMappingData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);

    // Mock for MappingInput
    MappingInput mpInputMock = mock(MappingInput.class);
    when(mpInputMock.getTransformName()).thenReturn(MAPPING_INPUT_TRANSFORM_NAME);

    // Mock for MappingOutput
    MappingOutput mpOutputMock = mock(MappingOutput.class);
    when(mpOutputMock.getTransformName()).thenReturn(MAPPING_OUTPUT_TRANSFORM_NAME);

    // Mock for RowDataInputMapper
    RowDataInputMapper rdInputMpMock = mock(RowDataInputMapper.class);
    IRowMeta rwMetaInMock = mock(RowMeta.class);
    doReturn(Boolean.TRUE).when(rdInputMpMock).putRow(rwMetaInMock, new Object[] {});

    // Mock for RowProducer
    RowProducer rProducerMock = mock(RowProducer.class);
    when(rProducerMock.putRow(any(IRowMeta.class), any(Object[].class), anyBoolean()))
        .thenReturn(true);

    // Mock for MappingIODefinition
    MappingIODefinition mpIODefMock = mock(MappingIODefinition.class);

    // Reuse transformMockHelper.pipeline as the mapping (sub-)pipeline so the verifies below
    // observe the same mock. Stub startThreads() to a no-op so SimpleMapping.processRow does
    // not crash on the mock's null internal transforms list.
    doNothing().when(transformMockHelper.pipeline).startThreads();

    // Set up real SimpleMappingData with some mocked elements
    simpleMpData.mappingInput = mpInputMock;
    simpleMpData.mappingOutput = mpOutputMock;
    simpleMpData.rowDataInputMapper = rdInputMpMock;
    simpleMpData.mappingPipeline = (LocalPipelineEngine) transformMockHelper.pipeline;

    Mockito.doReturn(mpOutputMock)
        .when(transformMockHelper.pipeline)
        .getTransform(MAPPING_OUTPUT_TRANSFORM_NAME, 0);
    Mockito.doReturn(rProducerMock)
        .when(transformMockHelper.pipeline)
        .addRowProducer(MAPPING_INPUT_TRANSFORM_NAME, 0);

    when(transformMockHelper.iTransformMeta.getInputMapping()).thenReturn(mpIODefMock);
  }

  @AfterEach
  void cleanUp() {
    transformMockHelper.cleanUp();
  }

  @Test
  void testTransformSetUpAsWasStarted_AtProcessingFirstRow() throws HopException {

    smp =
        new SimpleMapping(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            simpleMpData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    smp.addRowSetToInputRowSets(transformMockHelper.getMockInputRowSet(new Object[] {}));
    assertTrue(smp.processRow(), "First processRow should return true");
    assertFalse(smp.first, "The transform is processing not in first");
    assertTrue(smp.getData().wasStarted, "The transform was started");
  }

  @Test
  void testTransformShouldProcessError_WhenMappingPipelineHasError() throws HopException {

    // Set Up TransMock to return the error
    int errorCount = 1;
    when(transformMockHelper.pipeline.getErrors()).thenReturn(errorCount);

    // The transform has been already finished
    when(transformMockHelper.pipeline.isFinished()).thenReturn(Boolean.TRUE);
    // The transform was started
    simpleMpData.wasStarted = true;

    smp =
        new SimpleMapping(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            simpleMpData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    // mappingPipeline already set by @BeforeEach; dispose path under test (not full init)

    smp.dispose();
    verify(transformMockHelper.pipeline, times(1)).isFinished();
    verify(transformMockHelper.pipeline, never()).waitUntilFinished();
    verify(transformMockHelper.pipeline, never())
        .addActiveSubPipeline(anyString(), any(Pipeline.class));
    verify(transformMockHelper.pipeline, never()).getActiveSubPipeline(anyString());
    verify(transformMockHelper.pipeline, times(1)).getErrors();
    assertEquals(smp.getErrors(), errorCount, "The transform contains the errors");
  }

  @Test
  void testTransformShouldStopProcessingInput_IfUnderlyingTransitionIsStopped() throws Exception {

    MappingInput mappingInput = mock(MappingInput.class);
    when(mappingInput.getTransformName()).thenReturn(MAPPING_INPUT_TRANSFORM_NAME);
    transformMockHelper.iTransformData.mappingInput = mappingInput;

    RowProducer rowProducer = mock(RowProducer.class);
    when(rowProducer.putRow(any(IRowMeta.class), any(Object[].class), anyBoolean()))
        .thenReturn(true);

    ITransform transform = mock(ITransform.class);

    LocalPipelineEngine mappingPipeline = mock(LocalPipelineEngine.class);
    when(mappingPipeline.addRowProducer(anyString(), anyInt())).thenReturn(rowProducer);
    when(mappingPipeline.getTransform(anyString(), anyInt())).thenReturn(transform);
    when(mappingPipeline.isFinishedOrStopped()).thenReturn(Boolean.FALSE).thenReturn(Boolean.TRUE);
    transformMockHelper.iTransformData.mappingPipeline = mappingPipeline;

    MappingOutput mappingOutput = mock(MappingOutput.class);
    when(mappingOutput.getTransformName()).thenReturn(MAPPING_OUTPUT_TRANSFORM_NAME);
    transformMockHelper.iTransformData.mappingOutput = mappingOutput;

    smp =
        new SimpleMapping(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    // Use pre-mocked mappingPipeline from iTransformData (full init needs a real mapping .hpl)
    smp.addRowSetToInputRowSets(transformMockHelper.getMockInputRowSet(new Object[] {}));
    smp.addRowSetToInputRowSets(transformMockHelper.getMockInputRowSet(new Object[] {}));

    assertTrue(smp.processRow());
    assertFalse(smp.processRow());
  }

  @Test
  void testDispose() throws HopException {

    // Set Up TransMock to return the error
    when(transformMockHelper.pipeline.getErrors()).thenReturn(0);

    // The transform has been already finished
    when(transformMockHelper.pipeline.isFinished()).thenReturn(Boolean.FALSE);
    // The transform was started
    simpleMpData.wasStarted = true;

    // mappingPipeline is already set up by @BeforeEach (do not call init — needs a real .hpl)
    smp =
        new SimpleMapping(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            simpleMpData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    smp.dispose();
    verify(transformMockHelper.pipeline, times(1)).isFinished();
    verify(transformMockHelper.pipeline, times(1)).waitUntilFinished();
  }

  @Test
  void disposeWithNullMappingPipelineDoesNotNpe() {
    // Failed init leaves mappingPipeline null; Pipeline.prepareExecution still calls dispose().
    simpleMpData.mappingPipeline = null;
    simpleMpData.wasStarted = false;
    smp =
        new SimpleMapping(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            simpleMpData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    smp.dispose();
  }

  @Test
  void testProcessRowWhenNoInputIsTrue() throws HopException {
    when(transformMockHelper.iTransformMeta.isNoInput()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.isNoOutput()).thenReturn(false);
    simpleMpData.mappingInput = null;

    smp =
        new SimpleMapping(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            simpleMpData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    assertFalse(smp.processRow(), "processRow should finish in one call when noInput is true");
    assertTrue(smp.getData().wasStarted, "The transform was started");
    verify(transformMockHelper.pipeline, never()).addRowProducer(anyString(), anyInt());
    verify(transformMockHelper.pipeline, times(1)).startThreads();
    verify(transformMockHelper.pipeline, times(1)).waitUntilFinished();
  }

  @Test
  void testProcessRowWhenNoOutputIsTrue() throws HopException {
    when(transformMockHelper.iTransformMeta.isNoInput()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.isNoOutput()).thenReturn(true);
    simpleMpData.mappingOutput = null;

    smp =
        new SimpleMapping(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            simpleMpData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    smp.addRowSetToInputRowSets(transformMockHelper.getMockInputRowSet(new Object[] {}));

    assertTrue(smp.processRow(), "First processRow should return true for input row");
    assertFalse(smp.processRow(), "Second processRow should finish after input is exhausted");
    verify(transformMockHelper.pipeline, times(1)).addRowProducer(MAPPING_INPUT_TRANSFORM_NAME, 0);
    verify(transformMockHelper.pipeline, never()).getTransform(MAPPING_OUTPUT_TRANSFORM_NAME, 0);
    verify(transformMockHelper.pipeline, times(1)).startThreads();
    verify(transformMockHelper.pipeline, times(1)).waitUntilFinished();
  }

  @Test
  void testProcessRowWhenNoInputAndNoOutput() throws HopException {
    when(transformMockHelper.iTransformMeta.isNoInput()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.isNoOutput()).thenReturn(true);
    simpleMpData.mappingInput = null;
    simpleMpData.mappingOutput = null;

    smp = newSimpleMapping();

    assertFalse(smp.processRow(), "A standalone mapping finishes in one call");
    assertTrue(smp.getData().wasStarted);
    verify(transformMockHelper.pipeline, never()).addRowProducer(anyString(), anyInt());
    verify(transformMockHelper.pipeline, never()).getTransform(anyString(), anyInt());
    verify(transformMockHelper.pipeline, times(1)).startThreads();
    verify(transformMockHelper.pipeline, times(1)).waitUntilFinished();
  }

  @Test
  void testProcessRowNoInputDrivesSingleThreadedLoop() throws HopException {
    when(transformMockHelper.iTransformMeta.isNoInput()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.isNoOutput()).thenReturn(true);
    simpleMpData.mappingInput = null;
    simpleMpData.mappingOutput = null;
    SingleThreadedPipelineExecutor executor = mock(SingleThreadedPipelineExecutor.class);
    when(executor.oneIteration()).thenReturn(true, false);
    simpleMpData.executor = executor;

    smp = newSimpleMapping();

    assertFalse(smp.processRow());
    verify(executor, times(2)).oneIteration();
    verify(transformMockHelper.pipeline, never()).waitUntilFinished();
    verify(transformMockHelper.pipeline, times(1)).startThreads();
  }

  @Test
  void initRefusesIncomingHopWhenNoInputThroughTheEngine() {
    SimpleMapping mapping = mappingWithIncomingHop();

    assertFalse(mapping.init());
    assertEquals(1, mapping.getErrors());
    verify(transformMockHelper.iLogChannel).logError(contains("not reading incoming rows"));
  }

  @Test
  void initAllowsIncomingHopWhenUnconsumedMainInputIsAllowed() {
    SimpleMapping mapping = mappingWithIncomingHop();
    mapping.setVariable(Const.HOP_ALLOW_UNCONSUMED_MAIN_INPUT, "Y");

    mapping.init();

    verify(transformMockHelper.iLogChannel, never())
        .logError(contains("not reading incoming rows"));
  }

  @Test
  void initFailsWhenNoOutputStillHasAnOutgoingHop() {
    when(transformMockHelper.iTransformMeta.isNoInput()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.isNoOutput()).thenReturn(true);
    smp = newSimpleMapping();
    smp.addRowSetToOutputRowSets(transformMockHelper.getMockInputRowSet(new Object[] {}));

    assertFalse(smp.init());
    assertEquals(1, smp.getErrors());
    verify(transformMockHelper.iLogChannel).logError(contains("without output"));
  }

  private SimpleMapping mappingWithIncomingHop() {
    SimpleMappingMeta realMeta = new SimpleMappingMeta();
    realMeta.setDefault();
    realMeta.setNoInput(true);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("no-input-hop");
    TransformMeta from = new TransformMeta("from", new DummyMeta());
    TransformMeta to = new TransformMeta("mapping", realMeta);
    pipelineMeta.addTransform(from);
    pipelineMeta.addTransform(to);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(from, to));

    QueueRowSet rowSet = new QueueRowSet();
    rowSet.setThreadNameFromToCopy("from", 0, "mapping", 0);
    doReturn(rowSet)
        .when(transformMockHelper.pipeline)
        .findRowSet(any(), anyInt(), any(), anyInt());
    PipelineRunConfiguration runConfig = mock(PipelineRunConfiguration.class);
    when(runConfig.getEngineRunConfiguration())
        .thenReturn(mock(IPipelineEngineRunConfiguration.class));
    doReturn(runConfig).when(transformMockHelper.pipeline).getPipelineRunConfiguration();

    return new SimpleMapping(
        to, realMeta, new SimpleMappingData(), 0, pipelineMeta, transformMockHelper.pipeline);
  }

  private SimpleMapping newSimpleMapping() {
    return new SimpleMapping(
        transformMockHelper.transformMeta,
        transformMockHelper.iTransformMeta,
        simpleMpData,
        0,
        transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline);
  }
}
