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

package org.apache.hop.beam.engines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.PipelineResult;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine;
import org.apache.hop.beam.transform.PipelineTestBase;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** A Beam run fires the pipeline extension points of the local engine, in the same order. */
class BeamPipelineEngineExtensionPointsTest extends PipelineTestBase {

  static final List<String> FIRED = new CopyOnWriteArrayList<>();

  /** Notes which extension point fired, for a run of the pipeline named "extension-points". */
  private abstract static class Recorder implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {
    private final String extensionPointId;

    Recorder(String extensionPointId) {
      this.extensionPointId = extensionPointId;
    }

    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> engine) {
      if (engine.getPipelineMeta() != null
          && "extension-points".equals(engine.getPipelineMeta().getName())) {
        FIRED.add(extensionPointId);
      }
    }
  }

  @ExtensionPoint(
      id = "BeamTestPrepareExecutionXp",
      extensionPointId = "PipelinePrepareExecution",
      description = "")
  public static class PrepareExecutionXp extends Recorder {
    public PrepareExecutionXp() {
      super("PipelinePrepareExecution");
    }
  }

  @ExtensionPoint(
      id = "BeamTestStartThreadsXp",
      extensionPointId = "PipelineStartThreads",
      description = "")
  public static class StartThreadsXp extends Recorder {
    public StartThreadsXp() {
      super("PipelineStartThreads");
    }
  }

  @ExtensionPoint(id = "BeamTestStartXp", extensionPointId = "PipelineStart", description = "")
  public static class StartXp extends Recorder {
    public StartXp() {
      super("PipelineStart");
    }
  }

  @ExtensionPoint(id = "BeamTestFinishXp", extensionPointId = "PipelineFinish", description = "")
  public static class FinishXp extends Recorder {
    public FinishXp() {
      super("PipelineFinish");
    }
  }

  @ExtensionPoint(
      id = "BeamTestCompletedXp",
      extensionPointId = "PipelineCompleted",
      description = "")
  public static class CompletedXp extends Recorder {
    public CompletedXp() {
      super("PipelineCompleted");
    }
  }

  @BeforeEach
  void registerTheExtensionPoints() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    for (Class<?> xp :
        List.of(
            PrepareExecutionXp.class,
            StartThreadsXp.class,
            StartXp.class,
            FinishXp.class,
            CompletedXp.class)) {
      registry.registerPluginClass(
          xp.getName(), ExtensionPointPluginType.class, ExtensionPoint.class);
    }
    FIRED.clear();
  }

  @Test
  void aRunFiresTheExtensionPointsOfTheLocalEngineOnceInOrder() throws Exception {
    PipelineMeta pipelineMeta =
        BeamPipelineMetaUtil.generateBeamInputOutputPipelineMeta(
            "extension-points", "INPUT", "OUTPUT", metadataProvider);

    createRunPipeline(variables, pipelineMeta);

    assertEquals(
        List.of(
            "PipelinePrepareExecution",
            "PipelineStartThreads",
            "PipelineStart",
            "PipelineFinish",
            "PipelineCompleted"),
        FIRED);
  }

  /**
   * On an async runner (Dataflow, Flink, the Spark runner) a run that fails or is stopped ends too:
   * the listeners that saw it start, and PipelineFinish and PipelineCompleted, see the end once,
   * although the refresh timer and stopAll() both see the final state.
   */
  @ParameterizedTest
  @EnumSource(
      value = PipelineResult.State.class,
      names = {"FAILED", "CANCELLED", "STOPPED"})
  void anAsyncRunThatDoesNotSucceedEndsOnce(PipelineResult.State state) throws Exception {
    AtomicInteger finished = new AtomicInteger();
    BeamPipelineEngine engine = asyncEngineEndingIn(state);
    engine.addExecutionFinishedListener(e -> finished.incrementAndGet());

    engine.evaluatePipelineStatus(); // the refresh timer
    engine.stopAll(); // and a stop from the user
    engine.evaluatePipelineStatus();

    assertEquals(1, finished.get());
    assertEquals(List.of("PipelineFinish", "PipelineCompleted"), FIRED);
    if (state == PipelineResult.State.FAILED) {
      assertEquals(1, engine.getErrors(), "one failure is one error");
      assertEquals(Pipeline.STRING_FINISHED_WITH_ERRORS, engine.getStatusDescription());
    } else {
      assertEquals(0, engine.getErrors());
      assertEquals(Pipeline.STRING_STOPPED, engine.getStatusDescription());
    }
  }

  private BeamPipelineEngine asyncEngineEndingIn(PipelineResult.State state) {
    BeamPipelineEngine engine = new BeamDirectPipelineEngine();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("extension-points");
    engine.setPipelineMeta(pipelineMeta);
    IBeamPipelineEngineRunConfiguration runConfiguration =
        mock(IBeamPipelineEngineRunConfiguration.class);
    when(runConfiguration.isRunningAsynchronous()).thenReturn(true);
    engine.setBeamEngineRunConfiguration(runConfiguration);
    PipelineResult result = mock(PipelineResult.class);
    when(result.getState()).thenReturn(state);
    when(result.waitUntilFinish(any(Duration.class))).thenReturn(state);
    engine.beamPipelineResults = result;
    engine.setRunning(true);
    return engine;
  }
}
