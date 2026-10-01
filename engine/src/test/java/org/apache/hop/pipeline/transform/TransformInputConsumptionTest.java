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
package org.apache.hop.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.TestUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.transform.transforms.NonConsumingSourceMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TransformInputConsumptionTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<NonConsumingSourceMeta, DummyData> mockHelper;

  @BeforeEach
  void setUp() throws HopException {
    TestUtil.registerTestPluginTypes();
    mockHelper = new TransformMockHelper<>("to", NonConsumingSourceMeta.class, DummyData.class);
    lenient()
        .when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    lenient().when(mockHelper.iLogChannel.isDebug()).thenReturn(false);
    lenient().doReturn(null).when(mockHelper.pipeline).findRowSet(any(), anyInt(), any(), anyInt());
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void dummyConsumesMainInputByDefault() {
    DummyMeta meta = new DummyMeta();
    assertTrue(meta.consumesMainInput());
    assertFalse(meta.canStartWithoutInput());
  }

  @Test
  void nonConsumingSourceDoesNotAcceptMainInput() {
    NonConsumingSourceMeta meta = new NonConsumingSourceMeta();
    assertFalse(meta.consumesMainInput());
    assertTrue(meta.canStartWithoutInput());
    assertFalse(meta.getTransformIOMeta().isInputAcceptor());
  }

  @Test
  void isDisallowedMainInputHopDetectsHopIntoNonConsumer() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    assertTrue(pipelineMeta.isDisallowedMainInputHop(pipelineMeta.getPipelineHop(0)));
  }

  @Test
  void isDisallowedMainInputHopAllowsDummyToDummy() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta from = new TransformMeta("from", new DummyMeta());
    TransformMeta to = new TransformMeta("to", new DummyMeta());
    pipelineMeta.addTransform(from);
    pipelineMeta.addTransform(to);
    PipelineHopMeta hop = new PipelineHopMeta(from, to);
    pipelineMeta.addPipelineHop(hop);
    assertFalse(pipelineMeta.isDisallowedMainInputHop(hop));
  }

  @Test
  void isDisallowedMainInputHopAllowsErrorHop() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    PipelineHopMeta hop = pipelineMeta.getPipelineHop(0);
    hop.setErrorHop(true);
    assertFalse(pipelineMeta.isDisallowedMainInputHop(hop));
  }

  @Test
  void findDisallowedMainInputHopsListsOnlyMainHops() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    TransformMeta to = pipelineMeta.findTransform("to");
    assertEquals(1, pipelineMeta.findDisallowedMainInputHops(to).size());
    pipelineMeta.getPipelineHop(0).setErrorHop(true);
    assertTrue(pipelineMeta.findDisallowedMainInputHops(to).isEmpty());
  }

  @Test
  void findPreviousMainTransformsExcludesErrorHops() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    TransformMeta to = pipelineMeta.findTransform("to");
    assertEquals(1, pipelineMeta.findPreviousMainTransforms(to).size());
    assertEquals(1, pipelineMeta.findPreviousTransforms(to, false).size());
    pipelineMeta.getPipelineHop(0).setErrorHop(true);
    assertTrue(pipelineMeta.findPreviousMainTransforms(to).isEmpty());
    assertEquals(1, pipelineMeta.findPreviousTransforms(to, false).size());
  }

  @Test
  void checkTransformsCommentsOnPipelineSource() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("source-comment-test");
    pipelineMeta.addTransform(new TransformMeta("src", new NonConsumingSourceMeta()));

    List<ICheckResult> remarks = new ArrayList<>();
    pipelineMeta.checkTransforms(
        remarks, false, null, new Variables(), new MemoryMetadataProvider());
    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_COMMENT
                        && TransformSourceSupport.CHECK_CODE_PIPELINE_SOURCE.equals(
                            r.getErrorCode())),
        "expected a Verify comment that the transform is a pipeline source");
  }

  @Test
  void checkTransformsDoesNotCommentOnDummy() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("dummy-comment-test");
    pipelineMeta.addTransform(new TransformMeta("dummy", new DummyMeta()));

    List<ICheckResult> remarks = new ArrayList<>();
    pipelineMeta.checkTransforms(
        remarks, false, null, new Variables(), new MemoryMetadataProvider());
    assertFalse(
        remarks.stream()
            .anyMatch(
                r -> TransformSourceSupport.CHECK_CODE_PIPELINE_SOURCE.equals(r.getErrorCode())));
  }

  @Test
  void checkTransformsFlagsHopIntoNonConsumer() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    List<ICheckResult> remarks = new ArrayList<>();
    pipelineMeta.checkTransforms(
        remarks, false, null, new Variables(), new MemoryMetadataProvider());
    ICheckResult error =
        remarks.stream()
            .filter(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText() != null
                        && r.getText().contains("not reading rows"))
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        "expected a Verify error that the target does not consume main input"));
    assertTrue(
        error.getText().contains(Const.HOP_ALLOW_UNCONSUMED_MAIN_INPUT),
        "Verify error should name the opt-out variable without MessageFormat failing on '{HOP_...}'");
  }

  @Test
  void checkTransformsAllowsErrorHopIntoNonConsumer() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    pipelineMeta.getPipelineHop(0).setErrorHop(true);
    List<ICheckResult> remarks = new ArrayList<>();
    pipelineMeta.checkTransforms(
        remarks, false, null, new Variables(), new MemoryMetadataProvider());
    assertFalse(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText() != null
                        && r.getText().contains("not reading rows")));
  }

  @Test
  void initFailsWhenNonConsumerHasMainHop() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    stubRunConfigAndNonConsumingMeta();
    BaseTransform<NonConsumingSourceMeta, DummyData> transform =
        newTransform(pipelineMeta.findTransform("to"), pipelineMeta);

    assertFalse(transform.init());
    assertEquals(1L, transform.getErrors());
  }

  @Test
  void initAllowsErrorHopIntoNonConsumer() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    pipelineMeta.getPipelineHop(0).setErrorHop(true);
    stubRunConfigAndNonConsumingMeta();
    BaseTransform<NonConsumingSourceMeta, DummyData> transform =
        newTransform(pipelineMeta.findTransform("to"), pipelineMeta);

    assertTrue(transform.init());
    assertEquals(0L, transform.getErrors());
  }

  @Test
  void initAllowsUnconsumedMainInputWhenVariableSet() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    stubRunConfigAndNonConsumingMeta();
    BaseTransform<NonConsumingSourceMeta, DummyData> transform =
        newTransform(pipelineMeta.findTransform("to"), pipelineMeta);
    transform.setVariable(Const.HOP_ALLOW_UNCONSUMED_MAIN_INPUT, "Y");

    assertTrue(transform.init());
    assertEquals(0L, transform.getErrors());
  }

  @Test
  void setOutputDoneStopsPipelineWhenLeftoverMainInputExists() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    QueueRowSet rowSet = stubRunConfigAndNonConsumingMeta();
    rowSet.putRow(new RowMeta(), new Object[0]);
    BaseTransform<NonConsumingSourceMeta, DummyData> transform =
        newTransform(pipelineMeta.findTransform("to"), pipelineMeta);
    transform.setErrors(4);

    transform.setOutputDone();

    assertEquals(5L, transform.getErrors());
    verify(mockHelper.pipeline).stopAll();
  }

  @Test
  void setOutputDoneDoesNotStopWhenUnconsumedAllowed() {
    PipelineMeta pipelineMeta = pipelineWithHopIntoNonConsumer();
    QueueRowSet rowSet = stubRunConfigAndNonConsumingMeta();
    rowSet.putRow(new RowMeta(), new Object[0]);
    BaseTransform<NonConsumingSourceMeta, DummyData> transform =
        newTransform(pipelineMeta.findTransform("to"), pipelineMeta);
    transform.setVariable(Const.HOP_ALLOW_UNCONSUMED_MAIN_INPUT, "Y");

    transform.setOutputDone();

    assertEquals(0L, transform.getErrors());
    verify(mockHelper.pipeline, never()).stopAll();
  }

  /** Real rowset so {@link BaseTransform} dispatch does not fail before the consumption checks. */
  private QueueRowSet stubRunConfigAndNonConsumingMeta() {
    when(mockHelper.iTransformMeta.consumesMainInput()).thenReturn(false);
    PipelineRunConfiguration runConfig = mock(PipelineRunConfiguration.class);
    when(runConfig.getEngineRunConfiguration())
        .thenReturn(mock(IPipelineEngineRunConfiguration.class));
    when(mockHelper.pipeline.getPipelineRunConfiguration()).thenReturn(runConfig);
    QueueRowSet rowSet = new QueueRowSet();
    rowSet.setThreadNameFromToCopy("from", 0, "to", 0);
    lenient()
        .doReturn(rowSet)
        .when(mockHelper.pipeline)
        .findRowSet(any(), anyInt(), any(), anyInt());
    return rowSet;
  }

  private BaseTransform<NonConsumingSourceMeta, DummyData> newTransform(
      TransformMeta transformMeta, PipelineMeta pipelineMeta) {
    return new BaseTransform<>(
        transformMeta,
        mockHelper.iTransformMeta,
        mockHelper.iTransformData,
        0,
        pipelineMeta,
        mockHelper.pipeline);
  }

  private static PipelineMeta pipelineWithHopIntoNonConsumer() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("input-consumption-test");
    TransformMeta from = new TransformMeta("from", new DummyMeta());
    TransformMeta to = new TransformMeta("to", new NonConsumingSourceMeta());
    pipelineMeta.addTransform(from);
    pipelineMeta.addTransform(to);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(from, to));
    return pipelineMeta;
  }
}
