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

package org.apache.hop.pipeline.transforms.pipelineexecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.InOrder;

/** Unit tests for {@link PipelineExecutor} runtime behaviour (parameter passing, helpers). */
class PipelineExecutorTest {

  @RegisterExtension
  static final RestoreHopEngineEnvironmentExtension env =
      new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void initHop() throws HopException {
    HopEnvironment.init();
  }

  private PipelineExecutor newExecutor(PipelineExecutorMeta meta, PipelineExecutorData data) {
    TransformMeta transformMeta = mock(TransformMeta.class);
    when(transformMeta.getName()).thenReturn("pipeline_executor");
    when(transformMeta.isPartitioned()).thenReturn(false);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    when(pipelineMeta.findTransform(anyString())).thenReturn(transformMeta);
    LocalPipelineEngine pipeline = spy(new LocalPipelineEngine());
    PipelineExecutor executor =
        new PipelineExecutor(transformMeta, meta, data, 0, pipelineMeta, pipeline);
    executor.setMetadataProvider(new MemoryMetadataProvider());
    return executor;
  }

  @Test
  void getLastIncomingFieldValuesReturnsNullWhenBufferEmpty() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.groupBuffer = new ArrayList<>();
    PipelineExecutor executor = newExecutor(meta, data);

    assertNull(executor.getLastIncomingFieldValues());
  }

  @Test
  void getLastIncomingFieldValuesPreservesNullsFromLastBufferedRow() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.groupBuffer = new ArrayList<>();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("a"));
    rowMeta.addValueMeta(new ValueMetaString("b"));
    data.groupBuffer.add(new RowMetaAndData(rowMeta, new Object[] {"first", "second"}));
    data.groupBuffer.add(new RowMetaAndData(rowMeta, new Object[] {"x", null}));

    PipelineExecutor executor = newExecutor(meta, data);
    List<String> last = executor.getLastIncomingFieldValues();

    assertNotNull(last);
    assertEquals(Arrays.asList("x", null), last);
  }

  @Test
  void getLastIncomingFieldValuesUsesRowMetaStringConversion() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.groupBuffer = new ArrayList<>();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("s1"));
    rowMeta.addValueMeta(new ValueMetaDate("d1"));
    rowMeta.addValueMeta(new ValueMetaBoolean("b1"));
    Date date = new Date(1761051000000L);
    rowMeta.getValueMeta(1).setConversionMask("yyyy-MM-dd");
    data.groupBuffer.add(new RowMetaAndData(rowMeta, new Object[] {"x", date, true}));

    PipelineExecutor executor = newExecutor(meta, data);
    List<String> last = executor.getLastIncomingFieldValues();

    assertNotNull(last);
    assertEquals(
        Arrays.asList("x", rowMeta.getString(data.groupBuffer.get(0).getData(), 1), "Y"), last);
  }

  @Test
  void discardLogLinesDoesNothingWhenExecutorPipelineIsNull() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.setExecutorPipeline(null);
    PipelineExecutor executor = newExecutor(meta, data);

    assertNotNull(executor);
    executor.discardLogLines(data);
  }

  @Test
  void cleanupPreviousAttemptPipelineStopsRunningChildPipeline() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> previousChild = mock(IPipelineEngine.class);
    when(previousChild.isRunning()).thenReturn(true);
    when(previousChild.getLogChannelId()).thenReturn("previous-child-channel");
    data.setExecutorPipeline(previousChild);
    PipelineExecutor executor = newExecutor(meta, data);

    executor.cleanupPreviousAttemptPipeline(data);

    verify(previousChild).stopAll();
  }

  @Test
  void passParametersToPipelineMapsIncomingFieldToChildParameter() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(false);
    PipelineExecutorParameters param = new PipelineExecutorParameters();
    param.setVariable("MYPARAM");
    param.setField("myField");
    param.setInput("");
    meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));
    inputRowMeta.addValueMeta(new ValueMetaString("myField"));

    PipelineExecutorData data = new PipelineExecutorData();
    data.setInputRowMeta(inputRowMeta);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    when(child.listParameters()).thenReturn(new String[] {"MYPARAM"});
    when(child.getPipelineMeta()).thenReturn(mock(PipelineMeta.class));
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.passParametersToPipeline(Arrays.asList("1", "from-row"));

    verify(child, atLeastOnce()).setParameterValue("MYPARAM", "from-row");
  }

  @Test
  void passParametersToPipelineUsesStaticInputWhenFieldNotInRow() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(false);
    PipelineExecutorParameters param = new PipelineExecutorParameters();
    param.setVariable("P");
    param.setField("notInStream");
    param.setInput("static-value");
    meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));

    PipelineExecutorData data = new PipelineExecutorData();
    data.setInputRowMeta(inputRowMeta);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    when(child.listParameters()).thenReturn(new String[] {"P"});
    when(child.getPipelineMeta()).thenReturn(mock(PipelineMeta.class));
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.passParametersToPipeline(Collections.singletonList("row-id"));

    verify(child, atLeastOnce()).setParameterValue("P", "static-value");
  }

  @Test
  void passParametersToPipelineTreatsNullIncomingListAsEmpty() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(false);
    PipelineExecutorParameters param = new PipelineExecutorParameters();
    param.setVariable("P");
    param.setField("");
    param.setInput("fallback");
    meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));

    PipelineExecutorData data = new PipelineExecutorData();
    data.setInputRowMeta(inputRowMeta);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    when(child.listParameters()).thenReturn(new String[] {"P"});
    when(child.getPipelineMeta()).thenReturn(mock(PipelineMeta.class));
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.passParametersToPipeline(null);

    verify(child, atLeastOnce()).setParameterValue("P", "fallback");
  }

  /**
   * Empty field mapping must apply the child pipeline parameter default, not a sticky value left
   * from a previous PipelineExecutor row (integration test main-0003-pipeline-pipeline-executor).
   */
  @Test
  void passParametersToPipelineEmptyFieldUsesChildParameterDefaultNotStickyValue()
      throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(true);
    PipelineExecutorParameters param = new PipelineExecutorParameters();
    param.setVariable("TEST3_PARAMETER2");
    param.setField("value2");
    param.setInput("");
    meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("value2"));

    PipelineMeta childMeta = new PipelineMeta();
    childMeta.setName("child");
    childMeta.addParameterDefinition("TEST3_PARAMETER2", "default2", "with default");

    LocalPipelineEngine child = new LocalPipelineEngine(childMeta);
    child.copyParametersFromDefinitions(childMeta);
    // Simulate sticky value inherited from a previous executor iteration
    child.setVariable("TEST3_PARAMETER2", "B2");

    PipelineExecutorData data = new PipelineExecutorData();
    data.setInputRowMeta(inputRowMeta);
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.setVariable("TEST3_PARAMETER2", "B2");

    // Empty field cell (mapped from value2)
    executor.passParametersToPipeline(Collections.singletonList(""));

    assertEquals("default2", child.getVariable("TEST3_PARAMETER2"));
    // An empty field cell makes the transform itself declare the variable as empty, which is
    // where the sticky "B2" used to come back from.
    assertEquals("", executor.getVariable("TEST3_PARAMETER2"));
  }

  @Test
  void passParametersToPipelineNonEmptyFieldKeepsMappedValue() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(true);
    PipelineExecutorParameters param = new PipelineExecutorParameters();
    param.setVariable("TEST3_PARAMETER2");
    param.setField("value2");
    param.setInput("");
    meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("value2"));

    PipelineMeta childMeta = new PipelineMeta();
    childMeta.setName("child");
    childMeta.addParameterDefinition("TEST3_PARAMETER2", "default2", "with default");

    LocalPipelineEngine child = new LocalPipelineEngine(childMeta);
    child.copyParametersFromDefinitions(childMeta);
    child.setVariable("TEST3_PARAMETER2", "stale");

    PipelineExecutorData data = new PipelineExecutorData();
    data.setInputRowMeta(inputRowMeta);
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);

    executor.passParametersToPipeline(Collections.singletonList("B2"));

    assertEquals("B2", child.getVariable("TEST3_PARAMETER2"));
  }

  @Test
  void refreshRetriedParameterVariablesFromExternalScopesUsesLatestJvmValueForUnmappedParameter() {
    final String variableName = "ACCESS_TOKEN";
    final String originalProperty = System.getProperty(variableName);
    try {
      PipelineExecutorMeta meta = new PipelineExecutorMeta();
      meta.setDefault();
      meta.setInheritingAllVariables(true);
      PipelineExecutorParameters param = new PipelineExecutorParameters();
      param.setVariable(variableName);
      param.setField("");
      param.setInput("");
      meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

      PipelineExecutor executor = newExecutor(meta, new PipelineExecutorData());
      executor.setVariable(variableName, "stale-token");
      Variables parent = new Variables();
      parent.initializeFrom(null);
      parent.setVariable(variableName, "stale-parent-token");
      executor.getPipeline().initializeFrom(parent);
      parent.setVariable(variableName, null);
      System.setProperty(variableName, "mynewtoken");

      executor.refreshRetriedParameterVariablesFromExternalScopes();

      assertEquals("mynewtoken", executor.getVariable(variableName));
    } finally {
      if (originalProperty == null) {
        System.clearProperty(variableName);
      } else {
        System.setProperty(variableName, originalProperty);
      }
    }
  }

  @Test
  void refreshRetriedParameterVariablesFromExternalScopesPrefersParentPipelineValueOverJvm() {
    final String variableName = "ACCESS_TOKEN";
    final String originalProperty = System.getProperty(variableName);
    try {
      PipelineExecutorMeta meta = new PipelineExecutorMeta();
      meta.setDefault();
      meta.setInheritingAllVariables(true);
      PipelineExecutorParameters param = new PipelineExecutorParameters();
      param.setVariable(variableName);
      param.setField("");
      param.setInput("");
      meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

      PipelineExecutor executor = newExecutor(meta, new PipelineExecutorData());
      executor.setVariable(variableName, "stale-token");
      LocalWorkflowEngine parentWorkflow = new LocalWorkflowEngine();
      parentWorkflow.initializeFrom(null);
      parentWorkflow.setVariable(variableName, "parent-token");
      executor.getPipeline().initializeFrom(parentWorkflow);
      System.setProperty(variableName, "jvm-token");

      executor.refreshRetriedParameterVariablesFromExternalScopes();

      assertEquals("parent-token", executor.getVariable(variableName));
    } finally {
      if (originalProperty == null) {
        System.clearProperty(variableName);
      } else {
        System.setProperty(variableName, originalProperty);
      }
    }
  }

  @Test
  void refreshRetriedParameterVariablesFromExternalScopesRefreshesInheritedParameterNotOnGrid()
      throws HopException {
    final String variableName = "ACCESS_TOKEN";
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(true);
    meta.setParameters(new ArrayList<>());

    PipelineMeta childMeta = new PipelineMeta();
    childMeta.setName("child");
    childMeta.addParameterDefinition(variableName, "", "token");
    LocalPipelineEngine child = new LocalPipelineEngine(childMeta);
    child.copyParametersFromDefinitions(childMeta);

    PipelineExecutorData data = new PipelineExecutorData();
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.setVariable(variableName, "stale-token");
    executor.getPipeline().setVariable(variableName, "stale-copied-token");
    Variables parent = new Variables();
    parent.setVariable(variableName, "fresh-token");
    executor.getPipeline().setParentVariables(parent);

    executor.refreshRetriedParameterVariablesFromExternalScopes();
    executor.passParametersToPipeline(Collections.emptyList());

    assertEquals("fresh-token", child.getVariable(variableName));
  }

  @Test
  void refreshRetriedParameterVariablesFromExternalScopesRefreshesVariableUsedInStaticInput()
      throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(true);
    PipelineExecutorParameters param = new PipelineExecutorParameters();
    param.setVariable("P");
    param.setField("");
    param.setInput("${ACCESS_TOKEN}");
    meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

    PipelineMeta childMeta = new PipelineMeta();
    childMeta.setName("child");
    childMeta.addParameterDefinition("P", "", "mapped from static");
    LocalPipelineEngine child = new LocalPipelineEngine(childMeta);
    child.copyParametersFromDefinitions(childMeta);

    PipelineExecutorData data = new PipelineExecutorData();
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.setVariable("ACCESS_TOKEN", "stale-token");
    executor.getPipeline().setVariable("ACCESS_TOKEN", "stale-copied-token");
    Variables parent = new Variables();
    parent.setVariable("ACCESS_TOKEN", "fresh-token");
    executor.getPipeline().setParentVariables(parent);

    executor.refreshRetriedParameterVariablesFromExternalScopes();
    executor.passParametersToPipeline(Collections.emptyList());

    assertEquals("fresh-token", child.getVariable("P"));
  }

  @Test
  void collectPipelineResultsDoesNotForwardRowsWhenNoResultRowsTarget() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();

    PipelineExecutorData data = new PipelineExecutorData();
    data.setResultRowsRowSet(null);

    PipelineExecutor executor = spy(newExecutor(meta, data));
    Result result = new Result();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("x"));
    result.setRows(Collections.singletonList(new RowMetaAndData(rowMeta, new Object[] {"v"})));

    executor.collectPipelineResults(result);

    verify(executor, never()).putRowTo(any(), any(), any());
  }

  @Test
  void stopRunningDoesNothingWhenNoExecutorPipeline() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.setExecutorPipeline(null);
    PipelineExecutor executor = newExecutor(meta, data);

    assertNotNull(executor);
    executor.stopRunning();
  }

  @Test
  void stopAllDelegatesToExecutorWhenPresent() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    data.setExecutorPipeline(child);

    PipelineExecutor executor = newExecutor(meta, data);
    executor.stopAll();

    verify(child).stopAll();
  }

  @Test
  void stopRunningStopsChildPipelineWhenPresent() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    data.setExecutorPipeline(child);
    PipelineExecutor executor = newExecutor(meta, data);

    executor.stopRunning();

    verify(child).stopAll();
  }

  @Test
  void disposeClearsGroupBuffer() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    PipelineExecutorData data = new PipelineExecutorData();
    data.groupBuffer = new ArrayList<>();
    data.groupBuffer.add(new RowMetaAndData(new RowMeta(), new Object[0]));

    PipelineExecutor executor = newExecutor(meta, data);
    executor.dispose();

    assertNull(data.groupBuffer);
  }

  @Test
  void initFailsWhenStaticFilenameMissing() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setFilenameInField(false);
    meta.setFilename("");
    meta.setRunConfigurationName("local");

    PipelineExecutor executor = newExecutor(meta, new PipelineExecutorData());
    assertFalse(executor.init());
  }

  @Test
  void initSucceedsWhenFilenameComesFromField() {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setFilenameInField(true);
    meta.setFilenameField("child_path");
    meta.setFilename("/tmp/ignored-default.hpl");
    meta.setRunConfigurationName("local");

    PipelineExecutor executor = newExecutor(meta, new PipelineExecutorData());
    assertTrue(executor.init());
  }

  @Test
  void initLoadsChildPipelineFromFilesystemPath() throws URISyntaxException {
    String path =
        Paths.get(
                Objects.requireNonNull(
                        PipelineExecutorTest.class.getResource(
                            "/org/apache/hop/pipeline/transforms/pipelineexecutor/minimal-child.hpl"))
                    .toURI())
            .toAbsolutePath()
            .toString();

    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setFilenameInField(false);
    meta.setFilename(path);
    meta.setRunConfigurationName("local");

    PipelineExecutor executor = newExecutor(meta, new PipelineExecutorData());
    assertTrue(executor.init());
    assertNotNull(executor.getData().getExecutorPipelineMeta());
  }

  @Test
  void loadMappingMetaHonorsExplicitFilenameOverWrongMetaFilename()
      throws HopException, URISyntaxException {
    String goodPath =
        Paths.get(
                Objects.requireNonNull(
                        PipelineExecutorTest.class.getResource(
                            "/org/apache/hop/pipeline/transforms/pipelineexecutor/minimal-child.hpl"))
                    .toURI())
            .toAbsolutePath()
            .toString();

    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setFilename("/this/path/does/not/exist/bad-child.hpl");

    PipelineMeta loaded =
        TransformWithMappingMeta.loadMappingMeta(
            meta, goodPath, new MemoryMetadataProvider(), new Variables());

    assertNotNull(loaded);
    assertFalse(loaded.getTransforms().isEmpty());
  }

  @Test
  void executeWithRetriesRetriesFailedAttemptUntilSuccess() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setRetryAttempts("2");
    meta.setRetryDelay("0");

    PipelineExecutor executor = spy(newExecutor(meta, new PipelineExecutorData()));

    Result failed = new Result();
    failed.setResult(false);
    failed.setNrErrors(1);
    Result success = new Result();
    success.setResult(true);
    success.setNrErrors(0);

    doReturn(failed, success).when(executor).executePipelineAttempt(any(), anyLong());

    Result result = executor.executeWithRetries(Collections.emptyList());

    assertTrue(result.isResult());
    verify(executor, times(2)).executePipelineAttempt(any(), anyLong());
  }

  @Test
  void executeWithRetriesKeepsAttemptZeroParameterValuesOnRetryAttempt() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setRetryAttempts("1");
    meta.setRetryDelay("0");

    PipelineExecutorData data = new PipelineExecutorData();
    data.groupBuffer = new ArrayList<>();
    RowMeta bufferedRowMeta = new RowMeta();
    bufferedRowMeta.addValueMeta(new ValueMetaString("col1"));
    bufferedRowMeta.addValueMeta(new ValueMetaDate("col2"));
    bufferedRowMeta.addValueMeta(new ValueMetaBoolean("col3"));
    bufferedRowMeta.getValueMeta(1).setConversionMask("yyyy-MM-dd");
    data.groupBuffer.add(
        new RowMetaAndData(bufferedRowMeta, new Object[] {null, new Date(1761051000000L), true}));

    PipelineExecutor executor = spy(newExecutor(meta, data));

    Result failed = new Result();
    failed.setResult(false);
    failed.setNrErrors(1);
    Result success = new Result();
    success.setResult(true);
    success.setNrErrors(0);

    doReturn(failed, success).when(executor).executePipelineAttempt(any(), anyLong());
    Result result = executor.executeWithRetries(Arrays.asList("from-row", null, "N"));

    assertTrue(result.isResult());
    InOrder attemptOrder = inOrder(executor);
    attemptOrder
        .verify(executor)
        .executePipelineAttempt(eq(Arrays.asList("from-row", null, "N")), anyLong());
    attemptOrder
        .verify(executor)
        .executePipelineAttempt(eq(Arrays.asList("from-row", null, "N")), anyLong());
  }

  @Test
  void executeWithRetriesRefreshesFromInitializedParentBeforeRetryAttempt() throws HopException {
    final String variableName = "ACCESS_TOKEN";
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setInheritingAllVariables(true);
    meta.setRetryAttempts("1");
    meta.setRetryDelay("0");
    PipelineExecutorParameters param = new PipelineExecutorParameters();
    param.setVariable(variableName);
    param.setField("");
    param.setInput("");
    meta.setParameters(new ArrayList<>(Collections.singletonList(param)));

    PipelineMeta childMeta = new PipelineMeta();
    childMeta.setName("child");
    childMeta.addParameterDefinition(variableName, "", "token");
    LocalPipelineEngine child = new LocalPipelineEngine(childMeta);
    child.copyParametersFromDefinitions(childMeta);

    PipelineExecutorData data = new PipelineExecutorData();
    data.setExecutorPipeline(child);
    PipelineExecutor executor = spy(newExecutor(meta, data));

    LocalWorkflowEngine parentWorkflow = new LocalWorkflowEngine();
    parentWorkflow.initializeFrom(null);
    parentWorkflow.setVariable(variableName, "old-token");
    executor.getPipeline().initializeFrom(parentWorkflow);
    executor.setVariable(variableName, "old-token");

    List<String> childParameterByAttempt = new ArrayList<>();
    AtomicInteger attemptCounter = new AtomicInteger();
    doAnswer(
            invocation -> {
              int attempt = attemptCounter.getAndIncrement();
              executor.passParametersToPipeline(Collections.emptyList());
              childParameterByAttempt.add(child.getVariable(variableName));

              Result result = new Result();
              if (attempt == 0) {
                parentWorkflow.setVariable(variableName, "new-token");
                result.setResult(false);
                result.setNrErrors(1);
              } else {
                result.setResult(true);
                result.setNrErrors(0);
              }
              return result;
            })
        .when(executor)
        .executePipelineAttempt(any(), anyLong());

    Result result = executor.executeWithRetries(Collections.emptyList());

    assertTrue(result.isResult());
    assertEquals(Arrays.asList("old-token", "new-token"), childParameterByAttempt);
  }

  @Test
  void executeWithRetriesCleansUpPreviousAttemptOnlyWhenRetrying() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setRetryAttempts("1");
    meta.setRetryDelay("0");

    PipelineExecutor executor = spy(newExecutor(meta, new PipelineExecutorData()));

    Result failed = new Result();
    failed.setResult(false);
    failed.setNrErrors(1);
    Result success = new Result();
    success.setResult(true);
    success.setNrErrors(0);

    doReturn(failed, success).when(executor).executePipelineAttempt(any(), anyLong());
    doNothing().when(executor).cleanupPreviousAttemptPipeline(any());

    Result result = executor.executeWithRetries(Collections.emptyList());

    assertTrue(result.isResult());
    verify(executor, times(1)).cleanupPreviousAttemptPipeline(any());
  }

  @Test
  void executeWithRetriesDoesNotCleanUpWhenNoRetryWillRun() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setRetryAttempts("0");
    meta.setRetryDelay("0");

    PipelineExecutor executor = spy(newExecutor(meta, new PipelineExecutorData()));

    Result failed = new Result();
    failed.setResult(false);
    failed.setNrErrors(1);

    doReturn(failed).when(executor).executePipelineAttempt(any(), anyLong());
    doNothing().when(executor).cleanupPreviousAttemptPipeline(any());

    Result result = executor.executeWithRetries(Collections.emptyList());

    assertFalse(result.isResult());
    verify(executor, never()).cleanupPreviousAttemptPipeline(any());
  }

  @Test
  void executePipelineAttemptStopsCurrentChildWhenStartThreadsThrows() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();

    PipelineExecutorData data = new PipelineExecutorData();
    data.groupBuffer = new ArrayList<>();
    data.groupBuffer.add(new RowMetaAndData(new RowMeta(), new Object[0]));

    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    when(child.getLogChannelId()).thenReturn("failing-child-channel");
    when(child.isRunning()).thenReturn(true);
    doThrow(new HopException("start failed")).when(child).startThreads();

    PipelineExecutor executor = spy(newExecutor(meta, data));
    doReturn(child).when(executor).createInternalPipeline();
    doNothing().when(executor).passParametersToPipeline(any());

    Result result = executor.executePipelineAttempt(Collections.emptyList(), 0);

    assertFalse(result.isResult());
    verify(child).stopAll();
    verify(executor, never()).discardLogLines(any());
  }

  @Test
  void executeWithRetriesParsesExpandedRetryAttempts() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setRetryAttempts("1e0");
    meta.setRetryDelay("0");

    PipelineExecutor executor = spy(newExecutor(meta, new PipelineExecutorData()));

    Result failed = new Result();
    failed.setResult(false);
    failed.setNrErrors(1);

    doReturn(failed).when(executor).executePipelineAttempt(any(), anyLong());

    Result result = executor.executeWithRetries(Collections.emptyList());

    assertFalse(result.isResult());
    verify(executor, times(2)).executePipelineAttempt(any(), anyLong());
  }

  @Test
  void executeWithRetriesDoesNotRetryWhenRetriesAreDisabled() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setRetryAttempts("0");
    meta.setRetryDelay("0");

    PipelineExecutor executor = spy(newExecutor(meta, new PipelineExecutorData()));

    Result failed = new Result();
    failed.setResult(false);
    failed.setNrErrors(1);

    doReturn(failed).when(executor).executePipelineAttempt(any(), anyLong());

    Result result = executor.executeWithRetries(Collections.emptyList());

    assertFalse(result.isResult());
    verify(executor, times(1)).executePipelineAttempt(any(), anyLong());
  }

  @Test
  void executeWithRetriesUsesConfiguredStopAfterForEachAttempt() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setWaitTimeout("1");
    meta.setRetryAttempts("5");
    meta.setRetryDelay("0");

    PipelineExecutor executor = spy(newExecutor(meta, new PipelineExecutorData()));

    Result failed = new Result();
    failed.setResult(false);
    failed.setNrErrors(1);

    doReturn(failed).when(executor).executePipelineAttempt(any(), anyLong());

    Result result = executor.executeWithRetries(Collections.emptyList());

    assertFalse(result.isResult());
    verify(executor, times(6)).executePipelineAttempt(any(), eq(1L));
  }

  @Test
  void executeWithRetriesDoesNotStartAnotherAttemptAfterTransformStop() throws HopException {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setDefault();
    meta.setRetryAttempts("5");
    meta.setRetryDelay("0");

    PipelineExecutor executor = spy(newExecutor(meta, new PipelineExecutorData()));

    Result failed = new Result();
    failed.setResult(false);
    failed.setNrErrors(1);

    doReturn(failed).when(executor).executePipelineAttempt(any(), anyLong());
    doReturn(false, true).when(executor).isStopped();

    Result result = executor.executeWithRetries(Collections.emptyList());

    assertFalse(result.isResult());
    verify(executor, times(1)).executePipelineAttempt(any(), anyLong());
  }
}
