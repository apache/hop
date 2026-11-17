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

import java.util.List;
import java.util.Set;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.LineagePayload;
import org.apache.hop.lineage.model.UnitTestLineagePayload;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Reports the results of Hop unit tests as lineage, so a catalog can show them on the datasets the
 * tested transforms write: one {@link LineageEventKind#UNIT_TEST} event per golden data set, on the
 * tested transform's run.
 */
public final class LineageUnitTestEmitter {

  private LineageUnitTestEmitter() {}

  /**
   * Reports one golden data set of a unit test that ran in {@code pipeline}. Does nothing when the
   * run observes no lineage, or the transform isn't part of it. Never throws: lineage must not
   * change the outcome of a test.
   *
   * @param pipeline the pipeline run the test ran in
   * @param tested the pipeline as it was before the test changed it, which is what the result is
   *     about: the test may have replaced the transform, and runs a copy without a file name
   * @param unitTestName the name of the unit test
   * @param transformName the transform whose rows were compared
   * @param dataSetName the golden data set
   * @param result how the comparison came out
   * @param comment what the comparison found
   */
  public static void emit(
      IPipelineEngine<PipelineMeta> pipeline,
      PipelineMeta tested,
      String unitTestName,
      String transformName,
      String dataSetName,
      UnitTestLineagePayload.Result result,
      String comment) {
    try {
      Set<String> sinks = LineageSinkRoutes.ofUnitTest(pipeline);
      if (!(pipeline instanceof Pipeline local)
          || (!LineageHub.getInstance().isEnabled() && sinks.isEmpty())) {
        return;
      }
      ITransform transform = local.getTransform(transformName, 0);
      LineageContext.Builder ctx = LineageRunLifecycleEmitter.transformContextBuilder(transform);
      if (ctx == null) {
        return;
      }
      // The run's other events reach these sinks only when it is not a unit test run
      ctx.sinks(sinks);
      PipelineMeta definition = tested != null ? tested : pipeline.getPipelineMeta();
      if (definition != null && !Utils.isEmpty(definition.getFilename())) {
        LineageRunLifecycleEmitter.fillFilenameFields(ctx, definition.getFilename(), pipeline);
      }
      LineageHub.getInstance()
          .emit(
              LineageEvent.of(
                  LineageEventKind.UNIT_TEST,
                  ctx.build(),
                  new UnitTestLineagePayload(
                      unitTestName,
                      dataSetName,
                      result,
                      comment,
                      testedDatasets(definition, pipeline, transformName))));
    } catch (Exception e) {
      if (pipeline.getLogChannel() != null) {
        pipeline.getLogChannel().logDebug("Unit test lineage skipped: " + e.getMessage());
      }
    }
  }

  /** What the tested transform declares it reads and writes; none when that can't be told. */
  private static List<LineagePayload> testedDatasets(
      PipelineMeta meta, IPipelineEngine<PipelineMeta> pipeline, String transformName) {
    TransformMeta transformMeta = meta == null ? null : meta.findTransform(transformName);
    if (transformMeta == null) {
      return List.of();
    }
    try {
      return LineageDefinitions.declaredIo(meta, transformMeta, pipeline);
    } catch (Exception e) {
      return List.of();
    }
  }
}
