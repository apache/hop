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

package org.apache.hop.ui.hopgui;

import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.EngineCompatibilityChecker;
import org.apache.hop.pipeline.engine.EngineCompatibilityChecker.Violation;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.widgets.Shell;

/**
 * Pre-flight engine-compatibility check used by the GUI run delegates. Walks the pipeline/workflow
 * against the engine selected in the run-config dialog, and — when there are UNSUPPORTED elements —
 * pops a "Run anyway / Cancel" confirmation. This is the GUI counterpart of the CLI's {@code
 * --allow-unsupported} flag: strict by default, single explicit override per run.
 *
 * <p>The check is best-effort: it instantiates a bare engine via {@link PaletteEngineFilter}'s
 * probe machinery (no setup, just for {@code supports()}). If that fails the check falls back to
 * annotation-only resolution — annotation-declared excludes still block; engine-private bans (e.g.
 * Beam's GroupBy ban) silently pass. The final, authoritative gate remains the converter at
 * execution time, just with a worse error message.
 */
public final class EngineCompatibilityRunGate {

  private EngineCompatibilityRunGate() {}

  /**
   * @return UNSUPPORTED transforms for the engine named in {@code runConfigName}. Empty list when
   *     the run config or engine id cannot be resolved (in which case the run proceeds, mirroring
   *     today's behavior).
   */
  public static List<Violation> checkPipelineForRun(
      PipelineMeta pipelineMeta, String runConfigName, IHopMetadataProvider metadataProvider) {
    String engineId = resolvePipelineEngineId(runConfigName, metadataProvider);
    if (StringUtils.isEmpty(engineId)) {
      return Collections.emptyList();
    }
    return EngineCompatibilityChecker.checkPipeline(
        pipelineMeta, engineId, PaletteEngineFilter.loadPipelineEngineProbe(engineId));
  }

  /** Workflow counterpart. */
  public static List<Violation> checkWorkflowForRun(
      WorkflowMeta workflowMeta, String runConfigName, IHopMetadataProvider metadataProvider) {
    String engineId = resolveWorkflowEngineId(runConfigName, metadataProvider);
    if (StringUtils.isEmpty(engineId)) {
      return Collections.emptyList();
    }
    return EngineCompatibilityChecker.checkWorkflow(
        workflowMeta, engineId, PaletteEngineFilter.loadWorkflowEngineProbe(engineId));
  }

  /**
   * Show the violations and ask whether to run anyway. Returns {@code true} for "Yes" (run anyway),
   * {@code false} for "No" / dialog close (cancel). Caller is responsible for not invoking this
   * with an empty list.
   */
  public static boolean confirmRunAnyway(
      Shell shell, String elementType, String engineId, List<Violation> violations) {
    String engineLabel =
        "pipeline".equals(elementType)
            ? PaletteEngineFilter.getPipelineEngineLabelForId(engineId)
            : PaletteEngineFilter.getWorkflowEngineLabelForId(engineId);
    String kind = "workflow".equals(elementType) ? "action(s)" : "transform(s)";
    String header = engineLabel + " can't run " + violations.size() + " " + kind + ". Run anyway?";
    return new EngineCompatibilityDialog(shell, header, violations).open();
  }

  private static String resolvePipelineEngineId(
      String runConfigName, IHopMetadataProvider metadataProvider) {
    if (StringUtils.isEmpty(runConfigName) || metadataProvider == null) {
      return null;
    }
    try {
      PipelineRunConfiguration runConfig =
          metadataProvider.getSerializer(PipelineRunConfiguration.class).load(runConfigName);
      if (runConfig != null && runConfig.getEngineRunConfiguration() != null) {
        return runConfig.getEngineRunConfiguration().getEnginePluginId();
      }
    } catch (Exception e) {
      LogChannel.UI.logDebug(
          "Could not resolve pipeline engine for run config '" + runConfigName + "'", e);
    }
    return null;
  }

  private static String resolveWorkflowEngineId(
      String runConfigName, IHopMetadataProvider metadataProvider) {
    if (StringUtils.isEmpty(runConfigName) || metadataProvider == null) {
      return null;
    }
    try {
      WorkflowRunConfiguration runConfig =
          metadataProvider.getSerializer(WorkflowRunConfiguration.class).load(runConfigName);
      if (runConfig != null && runConfig.getEngineRunConfiguration() != null) {
        return runConfig.getEngineRunConfiguration().getEnginePluginId();
      }
    } catch (Exception e) {
      LogChannel.UI.logDebug(
          "Could not resolve workflow engine for run config '" + runConfigName + "'", e);
    }
    return null;
  }
}
