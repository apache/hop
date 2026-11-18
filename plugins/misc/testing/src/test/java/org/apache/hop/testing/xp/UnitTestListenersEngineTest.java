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

package org.apache.hop.testing.xp;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.testing.util.DataSetConst;
import org.junit.jupiter.api.Test;

/**
 * Validating a unit test and writing rows to a data set use row listeners on the transforms of
 * Hop's local engine, like the other unit test listeners. On another engine they do nothing, even
 * with the unit test flags set: there are no rows to validate or write.
 */
class UnitTestListenersEngineTest {

  @Test
  @SuppressWarnings("unchecked")
  void validationLeavesAnotherEngineAlone() throws Exception {
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    IPipelineEngine<PipelineMeta> engine =
        unitTestEngine(mock(IPipelineEngine.class), pipelineMeta);

    new ValidatePipelineUnitTestExtensionPoint()
        .callExtensionPoint(LogChannel.GENERAL, new Variables(), engine);

    verify(pipelineMeta, never()).getMetadataProvider();
  }

  @Test
  void validationRunsOnTheLocalEngine() throws Exception {
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    IPipelineEngine<PipelineMeta> engine =
        unitTestEngine(mock(LocalPipelineEngine.class), pipelineMeta);

    new ValidatePipelineUnitTestExtensionPoint()
        .callExtensionPoint(LogChannel.GENERAL, new Variables(), engine);

    // No metadata provider here, so it stops right after looking for one
    verify(pipelineMeta).getMetadataProvider();
  }

  @Test
  @SuppressWarnings("unchecked")
  void writingToADataSetLeavesAnotherEngineAlone() throws Exception {
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    IPipelineEngine<PipelineMeta> engine =
        writeToDataSetEngine(mock(IPipelineEngine.class), pipelineMeta);

    new WriteToDataSetExtensionPoint()
        .callExtensionPoint(LogChannel.GENERAL, new Variables(), engine);

    verify(engine, never()).addExecutionFinishedListener(any());
    verify(pipelineMeta, never()).getMetadataProvider();
  }

  @Test
  void writingToADataSetRunsOnTheLocalEngine() throws Exception {
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    IPipelineEngine<PipelineMeta> engine =
        writeToDataSetEngine(mock(LocalPipelineEngine.class), pipelineMeta);

    new WriteToDataSetExtensionPoint()
        .callExtensionPoint(LogChannel.GENERAL, new Variables(), engine);

    verify(engine).addExecutionFinishedListener(any());
    verify(pipelineMeta).getMetadataProvider();
  }

  private static IPipelineEngine<PipelineMeta> unitTestEngine(
      IPipelineEngine<PipelineMeta> engine, PipelineMeta pipelineMeta) {
    when(engine.getPipelineMeta()).thenReturn(pipelineMeta);
    when(engine.getVariable(DataSetConst.VAR_RUN_UNIT_TEST)).thenReturn("Y");
    when(engine.getVariable(DataSetConst.VAR_UNIT_TEST_NAME)).thenReturn("a unit test");
    return engine;
  }

  private static IPipelineEngine<PipelineMeta> writeToDataSetEngine(
      IPipelineEngine<PipelineMeta> engine, PipelineMeta pipelineMeta) {
    when(engine.getPipelineMeta()).thenReturn(pipelineMeta);
    when(engine.getVariable(DataSetConst.VAR_WRITE_TO_DATASET)).thenReturn("Y");
    return engine;
  }
}
