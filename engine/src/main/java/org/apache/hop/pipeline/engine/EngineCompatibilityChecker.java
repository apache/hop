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

package org.apache.hop.pipeline.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.EngineCompatibility;
import org.apache.hop.core.plugins.EngineCompatibilityResolver;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePluginType;

/**
 * Walks a pipeline or workflow and collects every transform/action whose engine-compatibility
 * verdict resolves to UNSUPPORTED. The CLI and the run-dialog use this to decide whether to abort
 * before execution starts.
 *
 * <p>Verdicts of UNKNOWN are intentionally NOT reported — the backwards-compatible default for any
 * existing plugin (no annotations, engine with no opinion) is UNKNOWN, and the converter falls back
 * to a generic handler that works for most transforms. Reporting UNKNOWN would generate warning
 * fatigue and immediately break every existing pipeline; only authoritative refusals surface here.
 */
public final class EngineCompatibilityChecker {

  private EngineCompatibilityChecker() {}

  /** One UNSUPPORTED transform or action, ready for logging. */
  public record Violation(String elementName, String pluginId, String reason) {

    public String formatLine() {
      return reason == null || reason.isEmpty()
          ? String.format("  • %s (%s)", elementName, pluginId)
          : String.format("  • %s (%s) — %s", elementName, pluginId, reason);
    }
  }

  /**
   * @return UNSUPPORTED transforms in the order they appear in the pipeline. Empty list if the
   *     engine is null, has no plugin id, or every transform is SUPPORTED/UNKNOWN.
   */
  public static List<Violation> checkPipeline(
      PipelineMeta pipelineMeta, IPipelineEngine<?> engine) {
    if (engine == null) {
      return Collections.emptyList();
    }
    return checkPipeline(pipelineMeta, engine.getPluginId(), engine::supports);
  }

  /**
   * Lower-level overload that lets the caller supply the engine id and a {@code supports} probe
   * directly. Used by the GUI run delegates, which load a bare engine instance via {@code
   * PluginRegistry.loadClass} rather than going through {@code PipelineEngineFactory} just to ask
   * one question. {@code engineCheck} may be null — in that case only annotation declarations are
   * consulted (the {@code BeamGenericTransformHandler} fallback won't be reflected, but explicit
   * annotation excludes and the few hard bans surfaced via fragments still apply).
   */
  public static List<Violation> checkPipeline(
      PipelineMeta pipelineMeta,
      String engineId,
      Function<IPlugin, EngineCompatibility> engineCheck) {
    if (pipelineMeta == null || engineId == null) {
      return Collections.emptyList();
    }
    PluginRegistry registry = PluginRegistry.getInstance();
    List<Violation> violations = new ArrayList<>();
    for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
      String pluginId = transformMeta.getTransformPluginId();
      if (pluginId == null) {
        continue;
      }
      IPlugin plugin = registry.findPluginWithId(TransformPluginType.class, pluginId);
      if (plugin == null) {
        continue;
      }
      EngineCompatibility verdict =
          EngineCompatibilityResolver.resolve(plugin, engineId, engineCheck);
      if (verdict.isUnsupported()) {
        violations.add(new Violation(transformMeta.getName(), pluginId, verdict.getReason()));
      }
    }
    return violations;
  }

  /**
   * @return UNSUPPORTED actions in the order they appear in the workflow. Empty list if the engine
   *     is null, has no plugin id, or every action is SUPPORTED/UNKNOWN.
   */
  public static List<Violation> checkWorkflow(
      WorkflowMeta workflowMeta, IWorkflowEngine<?> engine) {
    if (engine == null) {
      return Collections.emptyList();
    }
    // IWorkflowEngine has no getPluginId(); resolve it via reverse registry lookup.
    String engineId =
        PluginRegistry.getInstance().getPluginId(WorkflowEnginePluginType.class, engine);
    return checkWorkflow(workflowMeta, engineId, engine::supports);
  }

  /** Lower-level overload mirroring {@link #checkPipeline(PipelineMeta, String, Function)}. */
  public static List<Violation> checkWorkflow(
      WorkflowMeta workflowMeta,
      String engineId,
      Function<IPlugin, EngineCompatibility> engineCheck) {
    if (workflowMeta == null || engineId == null) {
      return Collections.emptyList();
    }
    PluginRegistry registry = PluginRegistry.getInstance();
    List<Violation> violations = new ArrayList<>();
    for (ActionMeta actionMeta : workflowMeta.getActions()) {
      if (actionMeta.getAction() == null) {
        continue;
      }
      String pluginId = actionMeta.getAction().getPluginId();
      if (pluginId == null) {
        continue;
      }
      IPlugin plugin = registry.findPluginWithId(ActionPluginType.class, pluginId);
      if (plugin == null) {
        continue;
      }
      EngineCompatibility verdict =
          EngineCompatibilityResolver.resolve(plugin, engineId, engineCheck);
      if (verdict.isUnsupported()) {
        violations.add(new Violation(actionMeta.getName(), pluginId, verdict.getReason()));
      }
    }
    return violations;
  }

  /** Renders a multi-line report; the heading is the caller's responsibility. */
  public static String formatViolations(List<Violation> violations) {
    return violations.stream().map(Violation::formatLine).collect(Collectors.joining("\n"));
  }
}
