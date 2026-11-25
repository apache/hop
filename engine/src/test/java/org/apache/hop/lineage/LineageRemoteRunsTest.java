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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.LineageRemoteRuns.Answer;
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
import org.apache.hop.lineage.spi.ILineageSink;
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
    LineageRemoteRuns.adopt(server, server, metadata, sent, log);
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
    assertEquals(List.of(key("marquez", "http://marquez:5000")), remoteRun.sinkKeys());
  }

  @Test
  void aServerRunWithoutAClientRunIsItsOwn() {
    IPipelineEngine<PipelineMeta> server = serverRun(Map.of("SOME", "value"), null);

    assertNull(LineageRemoteRuns.adopted(server));
    assertEquals("server-run", LineageRemoteRuns.runIdOf(server));
  }

  @Test
  void theServerAnswersWithTheRunAndTheSinksItReports() throws Exception {
    WebResult answer =
        WebResult.fromXmlString(
            new WebResult(WebResult.STRING_OK, "added", "id-1")
                .withLineage(new Answer("client-run", List.of("marquez", "datahub"), true))
                .getXml());
    assertEquals("client-run", answer.getLineageRunId());
    assertEquals(List.of("marquez", "datahub"), answer.getLineageSinks(), "the sinks' keys");
    assertTrue(answer.isLineageVariableSinks());
    assertEquals("id-1", answer.getId());
    assertEquals(
        new Answer("client-run", List.of("marquez", "datahub"), true), answer.getLineageAnswer());
    WebResult none =
        WebResult.fromXmlString(new WebResult(WebResult.STRING_OK, "added", "id-1").getXml());
    assertNull(none.getLineageAnswer(), "a server that doesn't report the run says nothing");
    assertFalse(
        new WebResult(WebResult.STRING_OK, "added", "id-1").getJson().contains("lineage"),
        "nor in JSON");

    IPipelineEngine<PipelineMeta> client = clientRun();
    sentBy(client);
    LineageRemoteRuns.answered(client, null);
    assertFalse(LineageRemoteRuns.isHandedOver(client));
    LineageRemoteRuns.answered(client, new Answer("another-run", List.of("marquez"), false));
    assertFalse(LineageRemoteRuns.isHandedOver(client));
    LineageRemoteRuns.answered(client, answer.getLineageAnswer());
    assertTrue(LineageRemoteRuns.isHandedOver(client));
  }

  @Test
  void sendingTheRunAgainForgetsTheEarlierServersAnswer() {
    IPipelineEngine<PipelineMeta> client = clientRun();
    sentBy(client);
    LineageRemoteRuns.answered(client, new Answer("client-run", List.of("marquez"), true));
    assertTrue(LineageRemoteRuns.isHandedOver(client));

    sentBy(client);

    assertFalse(LineageRemoteRuns.isHandedOver(client), "until the next server answered");
    assertTrue(LineageRemoteRuns.reportsToVariableSinks(client));
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
  void theServerRunReportsToTheClientsSinksItHasTooAndItsOwn() throws Exception {
    LineageHub hub =
        LineageHub.newIsolatedForTesting(
            LineageConfiguration.forTesting(false, 100, 10, 50L, Set.of()), List.of());
    ILogChannel log = mock(ILogChannel.class);
    Map<String, String> sent = sentBy(clientRun());
    IPipelineEngine<PipelineMeta> server =
        pipeline("server-run", "/tmp/export/orders.hpl", "datahub", log);
    // Sinks the server has as well: one named like a variable, the server run's own sink
    metadata
        .getSerializer(LineageSink.class)
        .save(FakeLineageSink.named("sink-${ENV}", "http://literal:5000"));
    // And ones it doesn't report to: a type that isn't installed, a name it doesn't have, and a
    // sink of the same name that points elsewhere on the server (prod on the client, dev here)
    LineageSink notInstalled = FakeLineageSink.named("elsewhere", "http://elsewhere:5000");
    ((FakeLineageSink.Configuration) notInstalled.getSink()).setPluginId("not-installed");
    metadata.getSerializer(LineageSink.class).save(notInstalled);
    metadata
        .getSerializer(LineageSink.class)
        .save(FakeLineageSink.named("catalog", "${CATALOG_URL}"));
    variables.setVariable("CATALOG_URL", "http://dev-marquez:5000");
    String elsewhere =
        new LineageSinkTarget(
                "elsewhere",
                "not-installed",
                Map.of("FAKE_URL", "http://elsewhere:5000", "FAKE_TOKEN", "s3cr3t-token"))
            .key();
    ObjectNode run = (ObjectNode) MAPPER.readTree(sent.get(LineageRemoteRuns.VAR_REMOTE_RUN));
    run.putArray("sinkKeys")
        .add(key("marquez", "http://marquez:5000"))
        .add(key("missing", "http://missing:5000"))
        .add(elsewhere)
        .add(key("catalog", "http://prod-marquez:5000"))
        .add(key("sink-${ENV}", "http://literal:5000"))
        .add(key("datahub", "http://datahub:8080"));
    variables.setVariable("ENV", "prod");

    Answer answer =
        LineageRemoteRuns.adopt(
            server,
            server,
            metadata,
            Map.of(LineageRemoteRuns.VAR_REMOTE_RUN, MAPPER.writeValueAsString(run)),
            log,
            hub,
            new Variables());
    Set<String> sinks = LineageSinkRoutes.of(server, hub);

    List<String> accepted =
        List.of(
            key("marquez", "http://marquez:5000"),
            key("sink-${ENV}", "http://literal:5000"),
            key("datahub", "http://datahub:8080"));
    assertEquals(accepted, answer.sinkKeys());
    assertEquals(answer, LineageRemoteRuns.answerOf(server));
    assertEquals(
        Set.copyOf(accepted),
        sinks,
        "a name as the client resolved it, not resolved once more on the server");
    verify(log).logError(contains("'missing'"));
    verify(log).logError(contains("'not-installed' isn't installed"));
    verify(log).logError(contains("'catalog'"));
    verify(log, never()).logError(contains("does not exist"));
  }

  @Test
  void theServerTakesOnlyTheParentOfTheClientsRun() throws Exception {
    ObjectNode run = MAPPER.createObjectNode();
    run.put("runId", "client-run");
    run.putObject("parent").put("parentRunId", "action-run").put("lineageDefinition", "true");
    IPipelineEngine<PipelineMeta> server =
        serverRun(Map.of(LineageRemoteRuns.VAR_REMOTE_RUN, MAPPER.writeValueAsString(run)), null);

    assertEquals(Map.of("parentRunId", "action-run"), LineageRemoteRuns.adopted(server).parent());
  }

  @Test
  void aClientRunTheServerCantReadIsLoggedOnTheServer() {
    ILogChannel log = mock(ILogChannel.class);
    IPipelineEngine<PipelineMeta> server =
        serverRun(Map.of(LineageRemoteRuns.VAR_REMOTE_RUN, "{not json"), log);

    assertNull(LineageRemoteRuns.adopted(server));
    verify(log).logError(contains("Unable to read the client's run"));
  }

  @Test
  void aClientRunTheServerReportsLeavesThoseSinksToTheServer() {
    LineageHub hub = mock(LineageHub.class);
    when(hub.useSink(any())).thenAnswer(i -> ((LineageSinkTarget) i.getArgument(0)).key());
    String marquez = key("marquez", "http://marquez:5000");
    IPipelineEngine<PipelineMeta> handedOver = clientRun();
    sentBy(handedOver);
    LineageRemoteRuns.answered(handedOver, new Answer("client-run", List.of(marquez), false));
    IPipelineEngine<PipelineMeta> refused = clientRun();
    sentBy(refused);
    // The server reports the run, but couldn't use the client's sink
    LineageRemoteRuns.answered(refused, new Answer("client-run", List.of(), false));
    IPipelineEngine<PipelineMeta> notHandedOver = clientRun();
    sentBy(notHandedOver);
    LineageRemoteRuns.answered(notHandedOver, null);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRunLifecycleEmitter.emitPipeline(handedOver, RunLifecyclePhase.FINISHED, null);
      LineageRunLifecycleEmitter.emitPipeline(refused, RunLifecyclePhase.FINISHED, null);
      LineageRunLifecycleEmitter.emitPipeline(notHandedOver, RunLifecyclePhase.FINISHED, null);
      // The run let go of the sink it held, also when the server reported to it
      LineageSinkRoutes.release(handedOver);
    }

    List<LineageEvent> events = emitted(hub);
    assertEquals(Set.of(), events.get(0).getContext().getSinks(), "variables' sinks only");
    assertTrue(events.get(0).getContext().isForVariableSinks());
    assertEquals(Set.of(marquez), events.get(1).getContext().getSinks());
    assertEquals(Set.of(marquez), events.get(2).getContext().getSinks());
    verify(hub).release(Set.of(marquez));
  }

  /** A sink configured with variables, for the hub to start. */
  private static final class OpenLineageLikeSink implements ILineageSink {
    @Override
    public void accept(List<LineageEvent> events) {
      // Nothing to deliver to
    }
  }

  private static LineageHub variableSinksHub(ILineageSink... sinks) {
    return LineageHub.newIsolatedForTesting(
        LineageConfiguration.forTesting(true, 100, 10, 50L, Set.of()), List.of(sinks));
  }

  @Test
  void theVariableSinksDigestNamesWhereTheyReport() {
    LineageHub on = variableSinksHub(new OpenLineageLikeSink());
    Variables client = new Variables();
    client.setVariable("HOP_LINEAGE_OPENLINEAGE_URL", "http://marquez:5000");
    client.setVariable("HOP_LINEAGE_OPENLINEAGE_NAMESPACE", "team-a");
    client.setVariable("HOP_LINEAGE_QUEUE_CAPACITY", "500");
    Variables same = new Variables();
    same.setVariable("HOP_LINEAGE_OPENLINEAGE_URL", " http://marquez:5000 ");
    same.setVariable("HOP_LINEAGE_OPENLINEAGE_NAMESPACE", "team-a");
    Variables otherNamespace = new Variables();
    otherNamespace.setVariable("HOP_LINEAGE_OPENLINEAGE_URL", "http://marquez:5000");
    otherNamespace.setVariable("HOP_LINEAGE_OPENLINEAGE_NAMESPACE", "hop-server");
    Variables otherKey = new Variables();
    otherKey.setVariable("HOP_LINEAGE_OPENLINEAGE_URL", "http://marquez:5000");
    otherKey.setVariable("HOP_LINEAGE_OPENLINEAGE_NAMESPACE", "team-a");
    otherKey.setVariable("HOP_LINEAGE_OPENLINEAGE_API_KEY", "another-key");

    String digest = LineageRemoteRuns.variableSinksDigest(on, client);

    assertNotNull(digest);
    assertEquals(digest, LineageRemoteRuns.variableSinksDigest(on, same), "hub tuning aside");
    assertNotEquals(digest, LineageRemoteRuns.variableSinksDigest(on, otherNamespace));
    assertNotEquals(digest, LineageRemoteRuns.variableSinksDigest(on, otherKey));
    assertNotEquals(
        digest,
        LineageRemoteRuns.variableSinksDigest(
            variableSinksHub(new OpenLineageLikeSink(), batch -> {}), client),
        "another set of sinks started");
    assertFalse(digest.contains("marquez"), "the settings may hold a secret");
  }

  @Test
  void noVariableSinksThatDeliverNoDigest() {
    Variables marquez = new Variables();
    marquez.setVariable("HOP_LINEAGE_OPENLINEAGE_URL", "http://marquez:5000");
    LineageHub off =
        LineageHub.newIsolatedForTesting(
            LineageConfiguration.forTesting(false, 100, 10, 50L, Set.of()),
            List.of(new OpenLineageLikeSink()));
    ILineageSink broken =
        new ILineageSink() {
          @Override
          public void init(IVariables variables, ILogChannel log) throws HopException {
            throw new HopException("simulated: the trust store is missing");
          }

          @Override
          public void accept(List<LineageEvent> events) {
            // Never started
          }
        };

    assertNull(LineageRemoteRuns.variableSinksDigest(off, marquez), "lineage is off");
    assertNull(LineageRemoteRuns.variableSinksDigest(variableSinksHub(), marquez), "no sinks");
    assertNull(
        LineageRemoteRuns.variableSinksDigest(variableSinksHub(broken), marquez),
        "they failed to start");
  }

  @Test
  void aClientRunLeavesTheSameVariableSinksToTheServer() throws Exception {
    LineageHub on = variableSinksHub(new OpenLineageLikeSink());
    LineageHub failed =
        variableSinksHub(
            new ILineageSink() {
              @Override
              public void init(IVariables variables, ILogChannel log) throws HopException {
                throw new HopException("simulated: the trust store is missing");
              }

              @Override
              public void accept(List<LineageEvent> events) {
                // Never started
              }
            });
    Variables marquez = new Variables();
    marquez.setVariable("HOP_LINEAGE_OPENLINEAGE_URL", "http://marquez:5000");
    Variables other = new Variables();
    other.setVariable("HOP_LINEAGE_OPENLINEAGE_URL", "http://other:5000");

    ObjectNode run = MAPPER.createObjectNode().put("runId", "client-run");
    run.put("variableSinks", LineageRemoteRuns.variableSinksDigest(on, marquez));
    Map<String, String> sent =
        Map.of(LineageRemoteRuns.VAR_REMOTE_RUN, MAPPER.writeValueAsString(run));
    IPipelineEngine<PipelineMeta> server = pipeline("server-run", "/tmp/orders.hpl", null, null);
    assertTrue(
        LineageRemoteRuns.adopt(server, server, metadata, sent, null, on, marquez).variableSinks());
    assertFalse(
        LineageRemoteRuns.adopt(server, server, metadata, sent, null, on, other).variableSinks());
    assertFalse(
        LineageRemoteRuns.adopt(server, server, metadata, sent, null, failed, marquez)
            .variableSinks(),
        "a server whose sinks failed to start doesn't claim them");

    LineageHub hub = mock(LineageHub.class);
    IPipelineEngine<PipelineMeta> handedOver = clientRun();
    sentBy(handedOver);
    LineageRemoteRuns.answered(
        handedOver, new Answer("client-run", List.of(key("marquez", "http://marquez:5000")), true));
    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRunLifecycleEmitter.emitPipeline(handedOver, RunLifecyclePhase.FINISHED, null);
    }
    LineageContext event = lifecycle(emitted(hub));
    assertFalse(event.isForVariableSinks(), "the server reports it there");
    assertEquals(Set.of(), event.getSinks());
  }

  @Test
  void aWorkflowAUnitTestStartedPassesNoSinks() throws Exception {
    WorkflowRunConfiguration runConfiguration = new WorkflowRunConfiguration();
    runConfiguration.setLineageSinkName("marquez");
    IWorkflowEngine<WorkflowMeta> workflow = workflow("client-run", "main", "/project/main.hwf");
    when(workflow.getWorkflowRunConfiguration()).thenReturn(runConfiguration);
    IWorkflowEngine<WorkflowMeta> unitTest = workflow("test-run", "main", "/project/main.hwf");
    when(unitTest.getWorkflowRunConfiguration()).thenReturn(runConfiguration);
    when(unitTest.getVariable(LineageSinkRoutes.VAR_RUN_UNIT_TEST)).thenReturn("Y");

    assertEquals(List.of(key("marquez", "http://marquez:5000")), passed(workflow).sinkKeys());
    assertEquals(List.of(), passed(unitTest).sinkKeys(), "as it reports to none");
  }

  private static RemoteRun passed(IWorkflowEngine<WorkflowMeta> workflow) throws Exception {
    Map<String, String> sent = new HashMap<>();
    LineageRemoteRuns.pass(workflow, sent);
    return MAPPER.readValue(sent.get(LineageRemoteRuns.VAR_REMOTE_RUN), RemoteRun.class);
  }

  @Test
  void aRemoteWorkflowsStartWaitsForTheServersAnswerAndKeepsItsTime() throws Exception {
    LineageHub hub = mock(LineageHub.class);
    IWorkflowEngine<WorkflowMeta> workflow = workflow("client-run", "main", "/project/main.hwf");
    long sentAt;

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRemoteRuns.submitting(workflow);
      long startedAt = System.currentTimeMillis();
      LineageRunLifecycleEmitter.emitWorkflow(workflow, RunLifecyclePhase.STARTED, null);
      verify(hub, never()).emit(any());

      LineageRemoteRuns.pass(workflow, new HashMap<>());
      Thread.sleep(50);
      sentAt = System.currentTimeMillis();
      LineageRemoteRuns.answered(workflow, new Answer("client-run", List.of(), false));
      LineageRemoteRuns.submitted(workflow);

      LineageEvent start =
          emitted(hub).stream()
              .filter(e -> e.getKind() == LineageEventKind.RUN_LIFECYCLE)
              .findFirst()
              .orElseThrow();
      assertEquals("client-run", start.getContext().getLogChannelId());
      assertTrue(start.getTimestampMillis() >= startedAt);
      assertTrue(start.getTimestampMillis() < sentAt, "stamped when it started, not when sent");
    }
  }

  @Test
  void aStartThatCantBeEmittedDoesntFailTheRun() {
    LineageHub hub = mock(LineageHub.class);
    Mockito.doThrow(new IllegalStateException("simulated")).when(hub).emit(any());
    IWorkflowEngine<WorkflowMeta> workflow = workflow("client-run", "main", "/project/main.hwf");
    ILogChannel log = mock(ILogChannel.class);
    when(workflow.getLogChannel()).thenReturn(log);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRemoteRuns.submitting(workflow);
      LineageRunLifecycleEmitter.emitWorkflow(workflow, RunLifecyclePhase.STARTED, null);
      LineageRemoteRuns.submitted(workflow);
    }

    verify(log).logError(contains("start of this run"), any(IllegalStateException.class));
  }
}
