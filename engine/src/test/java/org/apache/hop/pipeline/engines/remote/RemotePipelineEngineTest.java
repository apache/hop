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

package org.apache.hop.pipeline.engines.remote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.apache.hop.www.HopServerPipelineStatus;
import org.apache.hop.www.RemoteHopServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The run configuration a remote run configuration hands the pipeline to is used on the server.
 * When that leads back to a remote run configuration the pipeline keeps being handed on and
 * registered again and again. See issue #4086.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class RemotePipelineEngineTest {

  private static final String SERVER_NAME = "a-server";
  private static final String PIPELINE_NAME = "a-pipeline";
  private static final String CONTAINER_ID = "container-id";

  /**
   * A pipeline that runs on a server adds its files to the result over there. The parent workflow
   * has to see them, or an action such as Mail has nothing to attach. The status polls only carry
   * the metrics, so the files and rows are fetched once the server reports the pipeline finished.
   * See issue #4826.
   */
  @Test
  void resultFilesAndRowsOfAFinishedRemotePipelineReachTheParent() throws Exception {
    RemoteHopServer hopServer = mock(RemoteHopServer.class);
    when(hopServer.requestPipelineStatus(any(), eq(PIPELINE_NAME), eq(CONTAINER_ID), anyInt()))
        .thenReturn(status(Pipeline.STRING_FINISHED, null));
    when(hopServer.requestPipelineStatus(
            any(), eq(PIPELINE_NAME), eq(CONTAINER_ID), anyInt(), eq(true)))
        .thenReturn(status(Pipeline.STRING_FINISHED, serverResult()));
    RemotePipelineEngine engine = engine(hopServer);

    engine.getPipelineStatus();
    Result result = engine.getResult();

    assertTrue(engine.isFinished());
    assertEquals(1, result.getResultFiles().size(), "The file added on the server is missing");
    ResultFile resultFile = result.getResultFilesList().get(0);
    assertEquals("output.txt", resultFile.getFile().getName().getBaseName());
    assertEquals(ResultFile.FILE_TYPE_GENERAL, resultFile.getType());
    assertEquals(2, result.getRows().size(), "The rows copied to the result are missing");
    assertEquals("row-2", result.getRows().get(1).getString("name", null));
    // The metrics are still the ones aggregated from the transform status list.
    assertEquals(7, result.getNrLinesWritten());
  }

  /** The full result is only asked for once, on the poll that sees the pipeline finish. */
  @Test
  void fullResultIsRequestedOnceAndNotWhileRunning() throws Exception {
    RemoteHopServer hopServer = mock(RemoteHopServer.class);
    when(hopServer.requestPipelineStatus(any(), eq(PIPELINE_NAME), eq(CONTAINER_ID), anyInt()))
        .thenReturn(status(Pipeline.STRING_RUNNING, null), status(Pipeline.STRING_FINISHED, null));
    when(hopServer.requestPipelineStatus(
            any(), eq(PIPELINE_NAME), eq(CONTAINER_ID), anyInt(), eq(true)))
        .thenReturn(status(Pipeline.STRING_FINISHED, serverResult()));
    RemotePipelineEngine engine = engine(hopServer);

    engine.getPipelineStatus();
    assertTrue(engine.isRunning());
    assertTrue(engine.getResult().getResultFiles().isEmpty());
    verify(hopServer, never())
        .requestPipelineStatus(any(), eq(PIPELINE_NAME), eq(CONTAINER_ID), anyInt(), eq(true));

    engine.getPipelineStatus();
    engine.getPipelineStatus();

    verify(hopServer, times(1))
        .requestPipelineStatus(any(), eq(PIPELINE_NAME), eq(CONTAINER_ID), anyInt(), eq(true));
    assertEquals(1, engine.getResult().getResultFiles().size());
  }

  /**
   * When the server can no longer be asked for the full result the pipeline still counts as
   * finished: an exception from the polling timer would leave the parent waiting forever.
   */
  @Test
  void unavailableFullResultStillFinishesThePipeline() throws Exception {
    RemoteHopServer hopServer = mock(RemoteHopServer.class);
    when(hopServer.requestPipelineStatus(any(), eq(PIPELINE_NAME), eq(CONTAINER_ID), anyInt()))
        .thenReturn(status(Pipeline.STRING_FINISHED, null));
    when(hopServer.requestPipelineStatus(
            any(), eq(PIPELINE_NAME), eq(CONTAINER_ID), anyInt(), eq(true)))
        .thenThrow(new HopException("server went away"));
    RemotePipelineEngine engine = engine(hopServer);

    engine.getPipelineStatus();

    assertTrue(engine.isFinished());
    assertTrue(engine.getResult().getResultFiles().isEmpty());
    assertEquals(7, engine.getResult().getNrLinesWritten());
  }

  private static RemotePipelineEngine engine(RemoteHopServer hopServer) {
    RemotePipelineEngine engine = new RemotePipelineEngine();
    engine.setLogLevel(LogLevel.BASIC);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(PIPELINE_NAME);
    engine.setPipelineMeta(pipelineMeta);
    engine.setPipelineRunConfiguration(remote("remote", "local"));
    engine.containerId = CONTAINER_ID;
    engine.hopServer = hopServer;
    return engine;
  }

  /** What the server reports: one transform that wrote 7 rows, and optionally a Result. */
  private static HopServerPipelineStatus status(String statusDescription, Result result) {
    HopServerPipelineStatus status =
        new HopServerPipelineStatus(PIPELINE_NAME, CONTAINER_ID, statusDescription);
    TransformStatus transformStatus = new TransformStatus();
    transformStatus.setTransformName("Text file output");
    transformStatus.setStatusDescription(statusDescription);
    transformStatus.setLinesWritten(7);
    status.getTransformStatusList().add(transformStatus);
    status.setResult(result);
    return status;
  }

  /** The Result as the server's pipeline built it: one general file and two result rows. */
  private static Result serverResult() throws Exception {
    Result result = new Result();
    result
        .getResultFiles()
        .put(
            "output.txt",
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                HopVfs.getFileObject("ram:///" + PIPELINE_NAME + "/output.txt"),
                PIPELINE_NAME,
                "Text file output"));
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(new RowMetaAndData(rowMeta, "row-1"));
    rows.add(new RowMetaAndData(rowMeta, "row-2"));
    result.setRows(rows);
    return result;
  }

  /** A remote run configuration that names itself never reaches a server that would run it. */
  @Test
  void runConfigurationThatRefersToItselfIsRejected() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, remote("remote", "remote"));

    HopException e = assertThrows(HopException.class, () -> prepare(metadataProvider, "remote"));

    assertTrue(e.getMessage().contains("remote -> remote"), "The chain should be reported: " + e);
  }

  /**
   * The name of the run configuration to use on the server can hold a variable, which the check has
   * to resolve before it can see that it leads back to itself.
   */
  @Test
  void runConfigurationThatRefersToItselfThroughAVariableIsRejected() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, remote("remote", "${RUN_CONFIG}"));

    RemotePipelineEngine engine = engine(metadataProvider, "remote");
    engine.setVariable("RUN_CONFIG", "remote");

    HopException e = assertThrows(HopException.class, engine::prepareExecution);

    assertTrue(e.getMessage().contains("remote -> remote"), "The chain should be reported: " + e);
  }

  /** A chain of remote run configurations that leads back to an earlier one is rejected. */
  @Test
  void cyclicalRunConfigurationChainIsRejected() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, remote("first", "second"));
    save(metadataProvider, remote("second", "first"));

    HopException e = assertThrows(HopException.class, () -> prepare(metadataProvider, "first"));

    assertTrue(
        e.getMessage().contains("first -> second -> first"), "The chain should be reported: " + e);
  }

  /**
   * A remote run configuration that hands the pipeline to a local one is what a remote run
   * configuration is meant to do, so it may not be rejected. It fails later on, when it looks for
   * the server that does not exist here.
   */
  @Test
  void runConfigurationThatLeadsToALocalOneIsAccepted() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, remote("remote", "local"));
    save(metadataProvider, local("local"));

    HopException e = assertThrows(HopException.class, () -> prepare(metadataProvider, "remote"));

    assertTrue(
        e.getMessage().contains(SERVER_NAME),
        "It should get as far as looking for the server: " + e);
  }

  /**
   * A run configuration that only exists on the server cannot be followed from here, which may not
   * be reported as a problem.
   */
  @Test
  void runConfigurationThatIsUnknownHereIsAccepted() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    save(metadataProvider, remote("remote", "only-known-on-the-server"));

    HopException e = assertThrows(HopException.class, () -> prepare(metadataProvider, "remote"));

    assertTrue(
        e.getMessage().contains(SERVER_NAME),
        "It should get as far as looking for the server: " + e);
  }

  /**
   * The server computes HOP_VERSION itself. Sending the client's version along made a pipeline on a
   * server report the version of the client that started it. See issue #8263.
   */
  @Test
  void hopVersionIsNotPassedToTheServer() {
    assertFalse(RemotePipelineEngine.isVariablePassedToRemoteServer(Const.HOP_VERSION));
    assertTrue(RemotePipelineEngine.isVariablePassedToRemoteServer("MY_VARIABLE"));
  }

  private static PipelineRunConfiguration remote(String name, String runConfigurationName) {
    RemotePipelineRunConfiguration engineConfiguration = new RemotePipelineRunConfiguration();
    engineConfiguration.setEnginePluginId("Remote");
    engineConfiguration.setHopServerName(SERVER_NAME);
    engineConfiguration.setRunConfigurationName(runConfigurationName);
    return new PipelineRunConfiguration(
        name, "", null, new ArrayList<>(), engineConfiguration, null, false);
  }

  private static PipelineRunConfiguration local(String name) {
    LocalPipelineRunConfiguration engineConfiguration = new LocalPipelineRunConfiguration();
    engineConfiguration.setEnginePluginId("Local");
    return new PipelineRunConfiguration(
        name, "", null, new ArrayList<>(), engineConfiguration, null, false);
  }

  private static void save(IHopMetadataProvider metadataProvider, PipelineRunConfiguration c)
      throws HopException {
    metadataProvider.getSerializer(PipelineRunConfiguration.class).save(c);
  }

  private static void prepare(IHopMetadataProvider metadataProvider, String name) throws Exception {
    engine(metadataProvider, name).prepareExecution();
  }

  private static RemotePipelineEngine engine(IHopMetadataProvider metadataProvider, String name)
      throws HopException {
    RemotePipelineEngine engine = new RemotePipelineEngine();
    engine.setLogLevel(LogLevel.BASIC);
    engine.setMetadataProvider(metadataProvider);
    engine.setPipelineRunConfiguration(
        metadataProvider.getSerializer(PipelineRunConfiguration.class).load(name));
    engine.setPipelineMeta(new PipelineMeta());
    return engine;
  }
}
