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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.beam.transform.PipelineTestBase;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
}
