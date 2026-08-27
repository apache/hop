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

package org.apache.hop.beam.transform;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.junit.jupiter.api.Test;

/**
 * Reproduction test for issue #2340: the "Write to log" transform produces no visible output in the
 * pipeline log when the pipeline is executed on the Beam Direct runner (it works fine on the local
 * runner).
 */
class WriteToLogPipelineTest extends PipelineTestBase {

  private static final String MARKER = "HOP2340WRITETOLOGMARKER";

  @Test
  void testWriteToLogAppearsInPipelineLogOnBeamDirect() throws Exception {

    PipelineMeta pipelineMeta =
        BeamPipelineMetaUtil.generateWriteToLogPipelineMeta(
            "beam-write-to-log", "INPUT", "WriteToLog", MARKER, metadataProvider);
    pipelineMeta.lookupReferencesAfterLoading();

    IPipelineEngine<PipelineMeta> pipeline =
        PipelineEngineFactory.createPipelineEngine(
            variables, NAME_RUN_CONFIG, metadataProvider, pipelineMeta);
    pipeline.execute();
    pipeline.waitUntilFinished();

    // Guard: make sure rows actually flowed through the WriteToLog transform, so an empty-input
    // regression can never masquerade as the #2340 bug.
    //
    EngineMetrics metrics = pipeline.getEngineMetrics();
    Long writeToLogRead = null;
    for (IEngineComponent component : metrics.getComponents()) {
      if ("WriteToLog".equals(component.getName())) {
        writeToLogRead = metrics.getComponentMetric(component, Pipeline.METRIC_READ);
      }
    }
    if (writeToLogRead != null) {
      assertTrue(
          writeToLogRead > 0,
          "WriteToLog should have read rows on the Beam Direct runner (read="
              + writeToLogRead
              + ")");
    }

    // What the user actually sees for this run: the log scoped to the pipeline's own log channel.
    //
    String pipelineLog =
        HopLogStore.getAppender().getBuffer(pipeline.getLogChannelId(), true).toString();

    assertTrue(
        pipelineLog.contains(MARKER),
        "The WriteToLog message '"
            + MARKER
            + "' should appear in the pipeline log when running on the Beam Direct runner, "
            + "but it was absent (issue #2340).");
  }
}
