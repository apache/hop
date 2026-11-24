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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.LineageRemoteRuns.RemoteRun;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.hub.LineageConfiguration;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.hub.LineageSinkTarget;
import org.apache.hop.lineage.metadata.FakeLineageSink;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.lineage.plugin.LineageSinkPlugin;
import org.apache.hop.lineage.plugin.LineageSinkPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.www.WebResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** A run a client runs on a Hop Server, reported by the server as the client's own run. */
class LineageRemoteRunsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private MemoryMetadataProvider metadata;
  private IVariables variables;

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
        .save(FakeLineageSink.named("marquez", "http://marquez:5000"));
    metadata
        .getSerializer(LineageSink.class)
        .save(FakeLineageSink.named("datahub", "http://datahub:8080"));
    variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/project");
    variables.setVariable("SINK", "marquez");
  }

  @SuppressWarnings("unchecked")
  private IPipelineEngine<PipelineMeta> pipeline(
      String runId, String filename, String sinkName, ILogChannel log) {
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getExtensionDataMap()).thenReturn(new HashMap<>());
    when(pipeline.getLogChannelId()).thenReturn(runId);
    when(pipeline.getLogChannel()).thenReturn(log);
    PipelineMeta meta = mock(PipelineMeta.class);
    when(meta.getName()).thenReturn("orders");
    when(meta.getFilename()).thenReturn(filename);
    when(pipeline.getPipelineMeta()).thenReturn(meta);
    PipelineRunConfiguration runConfiguration = new PipelineRunConfiguration();
    runConfiguration.setLineageSinkName(sinkName);
    when(pipeline.getPipelineRunConfiguration()).thenReturn(runConfiguration);
    when(pipeline.getMetadataProvider()).thenReturn(metadata);
    when(pipeline.resolve(anyString()))
        .thenAnswer(i -> variables.resolve((String) i.getArgument(0)));
    return pipeline;
  }

  @SuppressWarnings("unchecked")
  private IWorkflowEngine<WorkflowMeta> workflow(String runId, String name, String filename) {
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.getExtensionDataMap()).thenReturn(new HashMap<>());
    when(workflow.getLogChannelId()).thenReturn(runId);
    WorkflowMeta meta = mock(WorkflowMeta.class);
    when(meta.getName()).thenReturn(name);
    when(meta.getFilename()).thenReturn(filename);
    when(workflow.getWorkflowMeta()).thenReturn(meta);
    when(workflow.getWorkflowRunConfiguration()).thenReturn(new WorkflowRunConfiguration());
    when(workflow.getMetadataProvider()).thenReturn(metadata);
    when(workflow.resolve(anyString()))
        .thenAnswer(i -> variables.resolve((String) i.getArgument(0)));
    return workflow;
  }

  /** A client's pipeline run that the action "Run orders" of workflow "main" started. */
  private IPipelineEngine<PipelineMeta> clientRun() {
    IPipelineEngine<PipelineMeta> client =
        pipeline("client-run", "/project/orders.hpl", "${SINK}", null);
    IAction action = mock(IAction.class, withSettings().extraInterfaces(ILoggingObject.class));
    when(action.getName()).thenReturn("Run orders");
    ILogChannel actionLog = mock(ILogChannel.class);
    when(actionLog.getLogChannelId()).thenReturn("action-run");
    when(action.getLogChannel()).thenReturn(actionLog);
    when(client.getParent()).thenReturn((ILoggingObject) action);
    IWorkflowEngine<WorkflowMeta> main = workflow("main-run", "main", "/project/main.hwf");
    when(client.getParentWorkflow()).thenReturn(main);
    return client;
  }

  /** The run the server starts for the client's run, from an export in its own folder. */
  private IPipelineEngine<PipelineMeta> serverRun(Map<String, String> sent, ILogChannel log) {
    IPipelineEngine<PipelineMeta> server =
        pipeline("server-run", "/tmp/export/orders.hpl", "datahub", log);
    LineageRemoteRuns.adopt(server, sent, log);
    return server;
  }

  private Map<String, String> sentBy(IPipelineEngine<PipelineMeta> client) {
    Map<String, String> sent = new HashMap<>();
    sent.put(LineageRemoteRuns.VAR_REMOTE_RUN, "{\"runId\":\"an inherited run\"}");
    LineageRemoteRuns.pass(client, sent);
    return sent;
  }

  @Test
  void theServerTakesOverTheRunTheClientPassed() {
    IPipelineEngine<PipelineMeta> server = serverRun(sentBy(clientRun()), null);

    RemoteRun remoteRun = LineageRemoteRuns.adopted(server);
    assertNotNull(remoteRun);
    assertEquals("client-run", remoteRun.runId(), "the client's, not the run it inherited");
    assertEquals("client-run", LineageRemoteRuns.runIdOf(server));
    assertEquals("/project/orders.hpl", remoteRun.filename());
    assertEquals("${PROJECT_HOME}/orders.hpl", remoteRun.portableFilename());
    assertEquals("main/Run orders", remoteRun.parent().get("parentJobName"));
    assertEquals("action-run", remoteRun.parent().get("parentRunId"));
    assertEquals("${PROJECT_HOME}/main.hwf", remoteRun.parent().get("parentFilename"));
    assertEquals(List.of("marquez"), remoteRun.sinks());
  }

  @Test
  void aServerRunWithoutAClientRunIsItsOwn() {
    IPipelineEngine<PipelineMeta> server = serverRun(Map.of("SOME", "value"), null);

    assertNull(LineageRemoteRuns.adopted(server));
    assertEquals("server-run", LineageRemoteRuns.runIdOf(server));
  }

  @Test
  void theServerAnswersWithTheRunItReports() throws Exception {
    WebResult answer =
        WebResult.fromXmlString(
            new WebResult(WebResult.STRING_OK, "added", "id-1")
                .withLineageRunId("client-run")
                .getXml());
    assertEquals("client-run", answer.getLineageRunId());
    assertEquals("id-1", answer.getId());
    assertNull(
        WebResult.fromXmlString(new WebResult(WebResult.STRING_OK, "added", "id-1").getXml())
            .getLineageRunId(),
        "a server that doesn't report the run says nothing");

    IPipelineEngine<PipelineMeta> client = clientRun();
    sentBy(client);
    LineageRemoteRuns.answered(client, null);
    assertFalse(LineageRemoteRuns.isHandedOver(client));
    LineageRemoteRuns.answered(client, "another-run");
    assertFalse(LineageRemoteRuns.isHandedOver(client));
    LineageRemoteRuns.answered(client, answer.getLineageRunId());
    assertTrue(LineageRemoteRuns.isHandedOver(client));
  }

  private static List<LineageEvent> emitted(LineageHub hub) {
    ArgumentCaptor<LineageEvent> events = ArgumentCaptor.forClass(LineageEvent.class);
    verify(hub, Mockito.atLeastOnce()).emit(events.capture());
    return events.getAllValues();
  }

  private static LineageContext lifecycle(List<LineageEvent> events) {
    return events.stream()
        .filter(e -> e.getKind() == LineageEventKind.RUN_LIFECYCLE)
        .findFirst()
        .orElseThrow()
        .getContext();
  }

  @Test
  void theServerRunReportsAsTheClientsRun() {
    IPipelineEngine<PipelineMeta> server = serverRun(sentBy(clientRun()), null);
    ITransform transform = mock(ITransform.class);
    ILogChannel transformLog = mock(ILogChannel.class);
    when(transformLog.getLogChannelId()).thenReturn("read-run");
    when(transform.getLogChannel()).thenReturn(transformLog);
    when(transform.getTransformName()).thenReturn("Read");
    when(transform.getPipeline()).thenReturn(server);
    LineageHub hub = mock(LineageHub.class);

    LineageContext transformContext;
    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRunLifecycleEmitter.emitPipeline(server, RunLifecyclePhase.STARTED, null);
      transformContext = LineageRunLifecycleEmitter.transformContextBuilder(transform).build();
    }

    LineageContext run = lifecycle(emitted(hub));
    assertEquals("client-run", run.getLogChannelId());
    assertEquals("/project/orders.hpl", run.getHopFilename());
    assertEquals("${PROJECT_HOME}/orders.hpl", run.getHopFilenamePortableKey());
    assertEquals("main/Run orders", run.getAttributes().get("parentJobName"));
    assertEquals("action-run", run.getAttributes().get("parentRunId"));

    assertEquals("read-run", transformContext.getLogChannelId());
    assertEquals("client-run", transformContext.getAttributes().get("pipelineLogChannelId"));
    assertEquals("${PROJECT_HOME}/orders.hpl", transformContext.getHopFilenamePortableKey());
  }

  @Test
  @SuppressWarnings("unchecked")
  void aRunTheServerRunStartsHasTheClientsRunAsParent() {
    IPipelineEngine<PipelineMeta> server = serverRun(sentBy(clientRun()), null);
    IPipelineEngine<PipelineMeta> child = mock(IPipelineEngine.class);
    when(child.getParentPipeline()).thenReturn(server);
    when(child.getExtensionDataMap()).thenReturn(new HashMap<>());
    when(server.getFilename()).thenReturn("/tmp/export/orders.hpl");
    LineageContext.Builder ctx = LineageContext.builder();

    LineageRunLifecycleEmitter.fillPipelineParentFields(ctx, child);

    Map<String, String> parent = ctx.build().getAttributes();
    assertEquals("client-run", parent.get("parentRunId"));
    assertEquals("orders", parent.get("parentJobName"));
    assertEquals("${PROJECT_HOME}/orders.hpl", parent.get("parentFilename"));
  }

  private static String key(String name, String url) {
    return new LineageSinkTarget(
            name, "fake-sink", Map.of("FAKE_URL", url, "FAKE_TOKEN", "s3cr3t-token"))
        .key();
  }

  @Test
  void theServerRunReportsToTheClientsSinksAndItsOwn() throws Exception {
    LineageHub hub =
        LineageHub.newIsolatedForTesting(
            LineageConfiguration.forTesting(false, 100, 10, 50L, Set.of()), List.of());
    ILogChannel log = mock(ILogChannel.class);
    Map<String, String> sent = sentBy(clientRun());
    IPipelineEngine<PipelineMeta> server =
        pipeline("server-run", "/tmp/export/orders.hpl", "datahub", log);
    // The client's sink, one the server doesn't have, and the server run's own sink once more
    ObjectNode run = (ObjectNode) MAPPER.readTree(sent.get(LineageRemoteRuns.VAR_REMOTE_RUN));
    run.putArray("sinks").add("marquez").add("missing").add("datahub");
    LineageRemoteRuns.adopt(
        server, Map.of(LineageRemoteRuns.VAR_REMOTE_RUN, MAPPER.writeValueAsString(run)), log);

    Set<String> sinks = LineageSinkRoutes.of(server, hub);

    assertEquals(
        Set.of(key("marquez", "http://marquez:5000"), key("datahub", "http://datahub:8080")),
        sinks);
    verify(log).logError(contains("'missing'"));
  }

  @Test
  void aClientRunTheServerReportsLeavesItsSinksToTheServer() {
    LineageHub hub = mock(LineageHub.class);
    when(hub.useSink(any())).thenReturn("marquez-key");
    IPipelineEngine<PipelineMeta> handedOver = clientRun();
    sentBy(handedOver);
    LineageRemoteRuns.answered(handedOver, "client-run");
    IPipelineEngine<PipelineMeta> notHandedOver = clientRun();
    sentBy(notHandedOver);
    LineageRemoteRuns.answered(notHandedOver, null);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRunLifecycleEmitter.emitPipeline(handedOver, RunLifecyclePhase.FINISHED, null);
      LineageRunLifecycleEmitter.emitPipeline(notHandedOver, RunLifecyclePhase.FINISHED, null);
    }

    List<LineageEvent> events = emitted(hub);
    assertEquals(Set.of(), events.get(0).getContext().getSinks(), "variables' sinks only");
    assertEquals(Set.of("marquez-key"), events.get(1).getContext().getSinks());
  }

  @Test
  void aRemoteWorkflowsStartWaitsForTheServersAnswer() {
    LineageHub hub = mock(LineageHub.class);
    IWorkflowEngine<WorkflowMeta> workflow = workflow("client-run", "main", "/project/main.hwf");

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRemoteRuns.submitting(workflow);
      LineageRunLifecycleEmitter.emitWorkflow(workflow, RunLifecyclePhase.STARTED, null);
      verify(hub, never()).emit(any());

      LineageRemoteRuns.pass(workflow, new HashMap<>());
      LineageRemoteRuns.answered(workflow, "client-run");
      LineageRemoteRuns.submitted(workflow);
    }

    LineageContext start = lifecycle(emitted(hub));
    assertEquals("client-run", start.getLogChannelId());
    assertEquals(Set.of(), start.getSinks(), "the server reports it to the run's sinks");
  }
}
