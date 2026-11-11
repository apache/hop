/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.execution.failure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.net.ConnectException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.failure.ExecutionFailure;
import org.apache.hop.core.failure.FailureCategory;
import org.apache.hop.core.failure.FailureComponent;
import org.apache.hop.core.failure.FailurePhase;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateBuilder;
import org.apache.hop.execution.local.FileExecutionInfoLocation;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/** Failures of transforms and pipelines, run on the local pipeline engine. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ExecutionFailurePipelineTest {

  enum Mode {
    SUCCEED,
    RUN_THROWS,
    RUN_THROWS_SECRET,
    RUN_SETS_ERRORS,
    INIT_THROWS,
    INIT_RETURNS_FALSE
  }

  @Transform(id = "ExecutionFailureTestTransform", name = "Execution failure test transform")
  public static class FailingMeta extends BaseTransformMeta<Failing, FailingData> {
    Mode mode = Mode.SUCCEED;
  }

  public static class FailingData extends BaseTransformData implements ITransformData {}

  public static class Failing extends BaseTransform<FailingMeta, FailingData> {
    public Failing(
        TransformMeta transformMeta,
        FailingMeta meta,
        FailingData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    @Override
    public boolean init() {
      switch (meta.mode) {
        case INIT_THROWS:
          throw new IllegalStateException(
              "Unable to open the file", new FileNotFoundException("/data/in/orders.csv"));
        case INIT_RETURNS_FALSE:
          logError("Unable to connect to the server", new ConnectException("Connection refused"));
          return false;
        default:
          return super.init();
      }
    }

    @Override
    public boolean processRow() throws HopException {
      switch (meta.mode) {
        case RUN_THROWS:
          throw new HopDatabaseException(
              "Error reading from the database",
              new SQLException("Connection refused", "08001", 0));
        case RUN_THROWS_SECRET:
          throw new HopDatabaseException(
              "Login failed with password "
                  + getVariable("DB_PASSWORD")
                  + " on jdbc:postgresql://scott:tiger@db/sales");
        case RUN_SETS_ERRORS:
          logError("Unexpected value", new NumberFormatException("For input string: \"abc\""));
          setErrors(1);
          stopAll();
          setOutputDone();
          return false;
        default:
          setOutputDone();
          return false;
      }
    }
  }

  @BeforeAll
  static void beforeAll() throws Exception {
    HopEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            FailingMeta.class.getName(), TransformPluginType.class, Transform.class);
  }

  private final List<ExecutionFailure> announced = new CopyOnWriteArrayList<>();

  LocalPipelineEngine pipelineWith(Mode mode) {
    FailingMeta meta = new FailingMeta();
    meta.mode = mode;
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("failure-test");
    pipelineMeta.addTransform(new TransformMeta("Failing", meta));
    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.addExecutionFailureListener(announced::add);
    return pipeline;
  }

  static void run(LocalPipelineEngine pipeline) throws HopException {
    pipeline.prepareExecution();
    pipeline.startThreads();
    pipeline.waitUntilFinished();
  }

  @Test
  void succeedingPipelineReportsNothing() throws Exception {
    LocalPipelineEngine pipeline = pipelineWith(Mode.SUCCEED);
    run(pipeline);

    assertTrue(pipeline.getFailures().isEmpty());
    assertNull(pipeline.getRunFailure());
    assertTrue(pipeline.getResult().getFailures().isEmpty());
    assertTrue(announced.isEmpty());
  }

  @Test
  void transformThatThrowsWhileRunning() throws Exception {
    LocalPipelineEngine pipeline = pipelineWith(Mode.RUN_THROWS);
    run(pipeline);

    assertEquals(1, pipeline.getFailures().size());
    ExecutionFailure transformFailure = pipeline.getFailures().get(0);
    FailureComponent component = transformFailure.getComponent();
    assertEquals(FailureComponent.KIND_TRANSFORM, component.getKind());
    assertEquals("Failing", component.getName());
    assertEquals("ExecutionFailureTestTransform", component.getPluginId());
    assertEquals(0, component.getCopyNr());
    assertEquals(FailurePhase.RUN, transformFailure.getPhase());
    assertEquals(FailureCategory.CONNECTIVITY, transformFailure.getCategory());
    assertEquals(
        HopDatabaseException.class.getName(), transformFailure.getCause().getExceptionClass());
    assertEquals(SQLException.class.getName(), transformFailure.getCause().getRootCauseClass());
    assertEquals("08001", transformFailure.getCause().getSqlState());
    assertFalse(transformFailure.getCause().isFromLog());
    assertEquals("failure-test", transformFailure.getRun().getName());
    assertEquals(pipeline.getLogChannelId(), transformFailure.getRun().getExecutionId());
    assertEquals(1L, transformFailure.getCounters().get("errors"));

    ExecutionFailure runFailure = pipeline.getRunFailure();
    assertNotNull(runFailure);
    assertNull(runFailure.getComponent());
    assertSame(transformFailure, runFailure.getCausedBy());
    assertSame(transformFailure, runFailure.getRootCause());
    assertEquals(FailureCategory.CONNECTIVITY, runFailure.getCategory());
    assertEquals(FailurePhase.RUN, runFailure.getPhase());

    // The transform first, then the pipeline, each once
    assertEquals(List.of(transformFailure, runFailure), announced);

    Result result = pipeline.getResult();
    assertEquals(List.of(runFailure), result.getFailures());
  }

  @Test
  void transformThatSetsErrorsWithoutThrowing() throws Exception {
    LocalPipelineEngine pipeline = pipelineWith(Mode.RUN_SETS_ERRORS);
    run(pipeline);

    assertEquals(1, pipeline.getFailures().size());
    ExecutionFailure failure = pipeline.getFailures().get(0);
    assertEquals(FailurePhase.RUN, failure.getPhase());
    assertTrue(failure.getCause().isFromLog());
    assertTrue(failure.getCause().getMessage().contains("Unexpected value"));
    assertEquals(NumberFormatException.class.getName(), failure.getCause().getRootCauseClass());
    assertEquals(FailureCategory.DATA_CONVERSION, failure.getCategory());
    assertSame(failure, pipeline.getRunFailure().getCausedBy());
  }

  @Test
  void transformThatThrowsInInit() {
    LocalPipelineEngine pipeline = pipelineWith(Mode.INIT_THROWS);
    assertThrows(HopException.class, pipeline::prepareExecution);

    assertEquals(1, pipeline.getFailures().size());
    ExecutionFailure failure = pipeline.getFailures().get(0);
    // Counted like an init() that returns false
    assertEquals(1L, failure.getCounters().get("errors"));
    assertEquals(FailurePhase.INIT, failure.getPhase());
    assertEquals(FailureCategory.MISSING_INPUT, failure.getCategory());
    assertEquals(FileNotFoundException.class.getName(), failure.getCause().getRootCauseClass());

    ExecutionFailure runFailure = pipeline.getRunFailure();
    assertNotNull(runFailure);
    assertEquals(FailurePhase.INIT, runFailure.getPhase());
    assertSame(failure, runFailure.getCausedBy());
    assertEquals(List.of(failure, runFailure), announced);
  }

  @Test
  void transformWhoseInitReturnsFalse() {
    LocalPipelineEngine pipeline = pipelineWith(Mode.INIT_RETURNS_FALSE);
    assertThrows(HopException.class, pipeline::prepareExecution);

    ExecutionFailure failure = pipeline.getFailures().get(0);
    assertEquals(FailurePhase.INIT, failure.getPhase());
    assertTrue(failure.getCause().isFromLog());
    assertTrue(failure.getCause().getMessage().contains("Unable to connect to the server"));
    assertEquals(FailureCategory.CONNECTIVITY, failure.getCategory());
    assertNotNull(pipeline.getRunFailure());
  }

  @Test
  void secondRunStartsWithoutTheFailuresOfTheFirst() throws Exception {
    FailingMeta meta = new FailingMeta();
    meta.mode = Mode.RUN_THROWS;
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("failure-test");
    pipelineMeta.addTransform(new TransformMeta("Failing", meta));

    LocalPipelineEngine first = new LocalPipelineEngine(pipelineMeta);
    run(first);
    assertNotNull(first.getRunFailure());

    meta.mode = Mode.SUCCEED;
    LocalPipelineEngine second = new LocalPipelineEngine(pipelineMeta);
    run(second);
    assertNull(second.getRunFailure());
    assertTrue(second.getFailures().isEmpty());
  }

  @Test
  void stateOfAFailedPipelineAndTransformCarriesTheFailure() throws Exception {
    LocalPipelineEngine pipeline = pipelineWith(Mode.RUN_THROWS);
    run(pipeline);

    ExecutionState pipelineState = ExecutionStateBuilder.fromExecutor(pipeline, -1).build();
    assertTrue(pipelineState.isFailed());
    assertSame(pipeline.getRunFailure(), pipelineState.getFailure());

    ExecutionState transformState =
        ExecutionStateBuilder.fromTransform(pipeline, pipeline.getComponents().get(0)).build();
    assertSame(pipeline.getFailures().get(0), transformState.getFailure());

    // The JSON form holds the whole chain
    String json = HopJson.newMapper().writeValueAsString(pipelineState);
    ExecutionState read = HopJson.newMapper().readValue(json, ExecutionState.class);
    assertEquals(FailureCategory.CONNECTIVITY, read.getFailure().getCategory());
    assertEquals("08001", read.getFailure().getRootCause().getCause().getSqlState());
  }

  @Test
  void stateOfASucceedingPipelineHasNoFailure() throws Exception {
    LocalPipelineEngine pipeline = pipelineWith(Mode.SUCCEED);
    run(pipeline);
    assertNull(ExecutionStateBuilder.fromExecutor(pipeline, -1).build().getFailure());
  }

  @Test
  void failedRunIsStoredAtTheLocation(@TempDir Path folder) throws Exception {
    LocalPipelineEngine pipeline = pipelineWith(Mode.RUN_THROWS);
    MemoryMetadataProvider metadataProvider = withLocation(pipeline, folder);
    run(pipeline);

    ExecutionState state = readState(folder, metadataProvider, pipeline.getLogChannelId());
    assertTrue(state.isFailed());
    assertEquals(FailureCategory.CONNECTIVITY, state.getFailure().getCategory());
    assertEquals("Failing", state.getFailure().getRootCause().getComponent().getName());
  }

  /** A pipeline that fails to initialize used to leave nothing at its location. */
  @Test
  void initFailureIsStoredAtTheLocation(@TempDir Path folder) throws Exception {
    LocalPipelineEngine pipeline = pipelineWith(Mode.INIT_THROWS);
    MemoryMetadataProvider metadataProvider = withLocation(pipeline, folder);
    assertThrows(HopException.class, pipeline::prepareExecution);

    FileExecutionInfoLocation location = new FileExecutionInfoLocation(folder.toString());
    location.initialize(new Variables(), metadataProvider);
    try {
      Execution execution = location.getExecution(pipeline.getLogChannelId());
      assertNotNull(execution, "The failed pipeline should be registered");
      ExecutionState state = location.getExecutionState(pipeline.getLogChannelId());
      assertTrue(state.isFailed());
      assertTrue(state.isFinished());
      assertEquals(FailurePhase.INIT, state.getFailure().getPhase());
      assertEquals(FailureCategory.MISSING_INPUT, state.getFailure().getCategory());
    } finally {
      location.close();
    }
  }

  private static MemoryMetadataProvider withLocation(LocalPipelineEngine pipeline, Path folder)
      throws HopException {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    ExecutionInfoLocation location =
        new ExecutionInfoLocation(
            "files",
            "",
            "100000",
            "100000",
            "",
            null,
            new FileExecutionInfoLocation(folder.toString()));
    metadataProvider.getSerializer(ExecutionInfoLocation.class).save(location);
    pipeline.setMetadataProvider(metadataProvider);
    pipeline.getPipelineRunConfiguration().setExecutionInfoLocationName("files");
    return metadataProvider;
  }

  private static ExecutionState readState(
      Path folder, MemoryMetadataProvider metadataProvider, String executionId)
      throws HopException {
    FileExecutionInfoLocation location = new FileExecutionInfoLocation(folder.toString());
    location.initialize(new Variables(), metadataProvider);
    try {
      return location.getExecutionState(executionId);
    } finally {
      location.close();
    }
  }

  @Test
  void secretsAreMaskedInTheCause() throws Exception {
    LocalPipelineEngine pipeline = pipelineWith(Mode.RUN_THROWS_SECRET);
    pipeline.setVariable("DB_PASSWORD", "s3cr3t-value");
    run(pipeline);

    var cause = pipeline.getFailures().get(0).getCause();
    assertTrue(cause.isRedacted());
    for (String text : List.of(cause.getMessage(), cause.getStackTrace())) {
      assertFalse(text.contains("s3cr3t-value"), text);
      assertFalse(text.contains("tiger"), text);
      assertTrue(text.contains("Login failed with password"), text);
    }
  }
}
