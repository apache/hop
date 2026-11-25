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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.hub.LineageConfiguration;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.hub.LineageSinkStatus;
import org.apache.hop.lineage.hub.LineageSinkTarget;
import org.apache.hop.lineage.metadata.FakeLineageSink;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.plugin.LineageSinkPlugin;
import org.apache.hop.lineage.plugin.LineageSinkPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LineageSinkRoutesTest {

  private MemoryMetadataProvider metadata;
  private IVariables variables;
  private LineageHub hub;

  @BeforeAll
  static void registerTheFakeSink() throws Exception {
    HopClientEnvironment.init();
    HopLogStore.init();
    PluginRegistry.addPluginType(LineageSinkPluginType.getInstance());
    PluginRegistry.getInstance()
        .registerPluginClass(
            FakeLineageSink.class.getName(), LineageSinkPluginType.class, LineageSinkPlugin.class);
  }

  @BeforeEach
  void setUp() throws Exception {
    metadata = new MemoryMetadataProvider();
    metadata
        .getSerializer(LineageSink.class)
        .save(FakeLineageSink.named("marquez", "${MARQUEZ_URL}"));
    metadata
        .getSerializer(LineageSink.class)
        .save(FakeLineageSink.named("datahub", "http://datahub:8080"));
    variables = new Variables();
    variables.setVariable("MARQUEZ_URL", "http://marquez:5000");
    variables.setVariable("SINK", "marquez");
    hub =
        LineageHub.newIsolatedForTesting(
            LineageConfiguration.forTesting(false, 100, 10, 50L, Set.of()), List.of());
  }

  @SuppressWarnings("unchecked")
  private IPipelineEngine<PipelineMeta> pipeline(String sinkName, ILogChannel log) {
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    Map<String, Object> extensionData = new HashMap<>();
    when(pipeline.getExtensionDataMap()).thenReturn(extensionData);
    PipelineRunConfiguration runConfiguration = new PipelineRunConfiguration();
    runConfiguration.setLineageSinkName(sinkName);
    when(pipeline.getPipelineRunConfiguration()).thenReturn(runConfiguration);
    when(pipeline.getMetadataProvider()).thenReturn(metadata);
    when(pipeline.getLogChannel()).thenReturn(log);
    when(pipeline.resolve(anyString()))
        .thenAnswer(i -> variables.resolve((String) i.getArgument(0)));
    when(pipeline.getVariable(anyString()))
        .thenAnswer(i -> variables.getVariable(i.getArgument(0)));
    return pipeline;
  }

  @SuppressWarnings("unchecked")
  private IWorkflowEngine<WorkflowMeta> workflow(String sinkName) {
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.getExtensionDataMap()).thenReturn(new HashMap<>());
    WorkflowRunConfiguration runConfiguration = new WorkflowRunConfiguration();
    runConfiguration.setLineageSinkName(sinkName);
    when(workflow.getWorkflowRunConfiguration()).thenReturn(runConfiguration);
    when(workflow.getMetadataProvider()).thenReturn(metadata);
    when(workflow.resolve(anyString()))
        .thenAnswer(i -> variables.resolve((String) i.getArgument(0)));
    when(workflow.getVariable(anyString()))
        .thenAnswer(i -> variables.getVariable(i.getArgument(0)));
    return workflow;
  }

  private static String key(String name, String url) {
    return new LineageSinkTarget(
            name, "fake-sink", Map.of("FAKE_URL", url, "FAKE_TOKEN", "s3cr3t-token"))
        .key();
  }

  @Test
  void aRunReportsToTheSinkOfItsRunConfigurationResolvedWithItsVariables() {
    Set<String> sinks = LineageSinkRoutes.of(pipeline("${SINK}", null), hub);

    assertEquals(Set.of(key("marquez", "http://marquez:5000")), sinks);
  }

  @Test
  void aRunAlsoReportsToTheSinksOfTheRunThatStartedIt() {
    IWorkflowEngine<WorkflowMeta> parent = workflow("datahub");
    IPipelineEngine<PipelineMeta> withItsOwn = pipeline("marquez", null);
    when(withItsOwn.getParentWorkflow()).thenReturn(parent);
    IPipelineEngine<PipelineMeta> withoutItsOwn = pipeline(null, null);
    when(withoutItsOwn.getParentWorkflow()).thenReturn(parent);
    IPipelineEngine<PipelineMeta> subPipeline = pipeline(null, null);
    when(subPipeline.getParentPipeline()).thenReturn(withItsOwn);

    String datahub = key("datahub", "http://datahub:8080");
    String marquez = key("marquez", "http://marquez:5000");
    assertEquals(Set.of(datahub, marquez), LineageSinkRoutes.of(withItsOwn, hub));
    assertEquals(Set.of(datahub), LineageSinkRoutes.of(withoutItsOwn, hub));
    assertEquals(Set.of(datahub, marquez), LineageSinkRoutes.of(subPipeline, hub));
  }

  @Test
  void aRunThatSelectsTheSinkOfItsParentHoldsItOnce() throws Exception {
    IWorkflowEngine<WorkflowMeta> parent = workflow("marquez");
    IPipelineEngine<PipelineMeta> child = pipeline("marquez", null);
    when(child.getParentWorkflow()).thenReturn(parent);
    String old = key("marquez", "http://marquez:5000");
    assertEquals(Set.of(old), LineageSinkRoutes.of(child, hub));
    hub.emit(
        LineageEvent.of(
            LineageEventKind.RUN_LIFECYCLE,
            LineageContext.builder().sinks(Set.of(old)).build(),
            null));
    hub.flush();

    LineageSinkRoutes.release(child.getExtensionDataMap(), hub);
    LineageSinkRoutes.release(parent.getExtensionDataMap(), hub);
    hub.flush();
    variables.setVariable("MARQUEZ_URL", "http://marquez:5001");
    LineageSinkRoutes.of(pipeline("marquez", null), hub);
    List<LineageSinkStatus> status = hub.sinkStatus();
    hub.shutdown();

    assertEquals(1, status.size(), "the version both runs used is retired: " + status);
    assertFalse(status.get(0).started(), "the edited version starts with its first event");
  }

  @Test
  void aRunWithoutExtensionDataHoldsNoSinks() {
    IPipelineEngine<PipelineMeta> noExtensionData = pipeline("marquez", null);
    when(noExtensionData.getExtensionDataMap()).thenReturn(null);
    String old = key("marquez", "http://marquez:5000");
    assertEquals(Set.of(old), LineageSinkRoutes.of(noExtensionData, hub));
    assertEquals(Set.of(old), LineageSinkRoutes.of(noExtensionData, hub));
    hub.flush();

    variables.setVariable("MARQUEZ_URL", "http://marquez:5001");
    LineageSinkRoutes.of(pipeline("marquez", null), hub);
    List<LineageSinkStatus> status = hub.sinkStatus();
    hub.shutdown();

    assertEquals(1, status.size(), "nothing holds the old version, so it is retired: " + status);
  }

  @Test
  void aUnitTestRunReportsOnlyItsResultsToItsSinks() {
    IPipelineEngine<PipelineMeta> pipeline = pipeline("marquez", null);
    variables.setVariable(LineageSinkRoutes.VAR_RUN_UNIT_TEST, "Y");

    assertEquals(Set.of(), LineageSinkRoutes.ofRun(pipeline, hub), "not its run lineage");
    assertEquals(
        Set.of(key("marquez", "http://marquez:5000")),
        LineageSinkRoutes.of(pipeline, hub),
        "its results, and held to be released when it ends");

    variables.setVariable(LineageSinkRoutes.VAR_RUN_UNIT_TEST, "N");
    assertEquals(LineageSinkRoutes.of(pipeline, hub), LineageSinkRoutes.ofRun(pipeline, hub));
  }

  // A Workflow executor in the tested pipeline runs its workflow with the test run's variables
  @Test
  void aWorkflowAUnitTestRunStartedReportsNoRunLineage() {
    IPipelineEngine<PipelineMeta> testRun = pipeline("marquez", null);
    IWorkflowEngine<WorkflowMeta> workflow = workflow(null);
    when(workflow.getParentPipeline()).thenReturn(testRun);
    variables.setVariable(LineageSinkRoutes.VAR_RUN_UNIT_TEST, "Y");

    assertEquals(Set.of(), LineageSinkRoutes.ofRun(workflow, hub), "not the test run's sinks");
    assertEquals(
        Set.of(key("marquez", "http://marquez:5000")),
        LineageSinkRoutes.of(workflow, hub),
        "held all the same, to be released when it ends");

    variables.setVariable(LineageSinkRoutes.VAR_RUN_UNIT_TEST, "N");
    assertEquals(LineageSinkRoutes.of(workflow, hub), LineageSinkRoutes.ofRun(workflow, hub));
  }

  // The Execute tests transform is the logging parent of its unit test runs, not their parent
  // pipeline: the results go to the sinks of the pipeline that runs the transform.
  @Test
  void theResultsOfAUnitTestTheExecuteTestsTransformRanGoToItsPipelinesSinks() {
    IPipelineEngine<PipelineMeta> runTests = pipeline("datahub", null);
    BaseTransform<?, ?> executeTests = mock(BaseTransform.class);
    when(executeTests.getPipeline()).thenReturn(runTests);
    IPipelineEngine<PipelineMeta> testRun = pipeline(null, null);
    when(testRun.getParent()).thenReturn(executeTests);
    variables.setVariable(LineageSinkRoutes.VAR_RUN_UNIT_TEST, "Y");

    assertEquals(Set.of(key("datahub", "http://datahub:8080")), LineageSinkRoutes.of(testRun, hub));
    assertTrue(LineageSinkRoutes.isUnitTestReported(testRun, hub));
    assertEquals(Set.of(), LineageSinkRoutes.ofRun(testRun, hub), "while its run lineage is not");
  }

  @Test
  void theResultsOfAUnitTestRunAreReportedToItsSinks() {
    IPipelineEngine<PipelineMeta> pipeline = pipeline("marquez", null);
    variables.setVariable(LineageSinkRoutes.VAR_RUN_UNIT_TEST, "Y");

    assertTrue(LineageSinkRoutes.isUnitTestReported(pipeline, hub));
    assertTrue(LineageSinkRoutes.ofRun(pipeline, hub).isEmpty(), "while its run lineage is not");
    assertFalse(LineageSinkRoutes.isUnitTestReported(pipeline(null, null), hub));
  }

  @Test
  void aRunKeepsTheSinksItStartedWith() {
    IPipelineEngine<PipelineMeta> pipeline = pipeline("marquez", null);
    Set<String> first = LineageSinkRoutes.of(pipeline, hub);

    pipeline.getPipelineRunConfiguration().setLineageSinkName("datahub");

    assertSame(first, LineageSinkRoutes.of(pipeline, hub));
  }

  @Test
  void aSinkThatDoesNotExistIsLoggedAndLeftOut() {
    ILogChannel log = mock(ILogChannel.class);

    Set<String> sinks = LineageSinkRoutes.of(pipeline("gone", log), hub);

    assertTrue(sinks.isEmpty());
    verify(log).logError(contains("'gone'"), any(Throwable.class));
    assertFalse(hub.isEnabled());
  }

  @Test
  void aRunWithoutASinkHasNone() {
    assertTrue(LineageSinkRoutes.of(pipeline(null, null), hub).isEmpty());
    assertTrue(LineageSinkRoutes.of(workflow(""), hub).isEmpty());
    assertFalse(hub.isEnabled());
  }

  @Test
  void aSinkThatFailedToStartIsLoggedOnTheRunsThatSelectIt() {
    LineageHub failing =
        LineageHub.newIsolatedForTesting(
            LineageConfiguration.forTesting(false, 100, 10, 50L, Set.of()),
            List.of(),
            target -> {
              throw new HopException("simulated: the trust store is missing");
            });
    String key = LineageSinkRoutes.of(pipeline("marquez", null), failing).iterator().next();
    failing.emit(
        LineageEvent.of(
            LineageEventKind.RUN_LIFECYCLE,
            LineageContext.builder().sinks(Set.of(key)).build(),
            null));
    failing.flush();
    ILogChannel log = mock(ILogChannel.class);

    LineageSinkRoutes.of(pipeline("marquez", log), failing);
    failing.shutdown();

    verify(log).logError(contains("trust store is missing"));
  }

  @Test
  void aRunLetsGoOfItsSinksOnce() {
    IPipelineEngine<PipelineMeta> pipeline = pipeline("marquez", null);
    LineageSinkRoutes.of(pipeline, hub);
    LineageHub spy = spy(hub);

    LineageSinkRoutes.release(pipeline.getExtensionDataMap(), spy);
    LineageSinkRoutes.release(pipeline.getExtensionDataMap(), spy);

    verify(spy, times(1)).release(any());
  }

  // A transform that starts a workflow with only itself as the logging parent: the workflow
  // inherits the sinks of the transform's pipeline, as with a parent pipeline
  @Test
  void aWorkflowATransformStartedTakesThatTransformsPipelineSinks() {
    IPipelineEngine<PipelineMeta> pipeline = pipeline("datahub", null);
    BaseTransform<?, ?> transform = mock(BaseTransform.class);
    when(transform.getPipeline()).thenReturn(pipeline);
    IWorkflowEngine<WorkflowMeta> workflow = workflow(null);
    when(workflow.getParent()).thenReturn(transform);

    assertEquals(
        Set.of(key("datahub", "http://datahub:8080")), LineageSinkRoutes.of(workflow, hub));
  }
}
