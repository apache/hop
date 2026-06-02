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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.EngineCompatibility;
import org.apache.hop.core.plugins.EngineCompatibilityResolver;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePluginType;

/**
 * Filters the pipeline-canvas and workflow-canvas context palettes by a user-selected "design"
 * engine, persisted in {@code hop-config.json}. UNSUPPORTED transforms/actions are hidden;
 * SUPPORTED and UNKNOWN stay visible.
 *
 * <p>UNKNOWN is intentionally kept visible — every legacy plugin reports UNKNOWN today (empty
 * annotation arrays, engines returning UNKNOWN by default), so hiding it would empty the palette
 * the moment any engine is selected. Only authoritative refusals (engine UNSUPPORTED, annotation
 * exclude-list match) hide an entry.
 *
 * <p>An empty stored value means "no filter" — the palette behaves exactly as it did before this
 * feature was introduced, preserving the new-user experience.
 *
 * <p>The local "Local" and remote "Remote" engines are grouped under a single {@link
 * #HOP_ENGINE_GROUP_ID Hop} entry: from a user's perspective they are the same engine surface (same
 * plugin compatibility, same transform palette) and splitting them in the combo adds noise. A
 * plugin is shown when <em>any</em> engine in the group accepts it, hidden only when <em>every</em>
 * member of the group says UNSUPPORTED.
 */
public final class PaletteEngineFilter {

  /** {@code hop-config.json} key for the pipeline palette filter. Value is the engine plugin id. */
  public static final String CONFIG_KEY_PIPELINE_DESIGN_ENGINE =
      "HopGuiPaletteDesignPipelineEngine";

  /** {@code hop-config.json} key for the workflow palette filter. */
  public static final String CONFIG_KEY_WORKFLOW_DESIGN_ENGINE =
      "HopGuiPaletteDesignWorkflowEngine";

  /** Combo entry shown to users when no engine filter is applied. Always offered first. */
  public static final String NO_FILTER_LABEL = "All engines";

  /**
   * Synthetic engine id used by the combo to represent the unified Hop-engine group (Local +
   * Remote, both pipeline and workflow). Stored verbatim in {@code hop-config.json}; when the
   * filter loads this value it expands into probes for every member of the group.
   */
  public static final String HOP_ENGINE_GROUP_ID = "Hop";

  /** Engine plugin ids collapsed into the {@link #HOP_ENGINE_GROUP_ID Hop} group. */
  private static final Set<String> HOP_PIPELINE_ENGINE_IDS = Set.of("Local", "Remote");

  private static final Set<String> HOP_WORKFLOW_ENGINE_IDS = Set.of("Local", "Remote");

  private final String engineId;
  private final List<EngineProbe> probes;

  private PaletteEngineFilter(String engineId, List<EngineProbe> probes) {
    this.engineId = engineId;
    this.probes = probes;
  }

  /** Reads the saved pipeline-design engine id and instantiates a probe for it. */
  public static PaletteEngineFilter forPipelineDesign() {
    String engineId = StringUtils.trimToEmpty(getPipelineDesignEngineId());
    return new PaletteEngineFilter(engineId, pipelineProbesFor(engineId));
  }

  /** Reads the saved workflow-design engine id and instantiates a probe for it. */
  public static PaletteEngineFilter forWorkflowDesign() {
    String engineId = StringUtils.trimToEmpty(getWorkflowDesignEngineId());
    return new PaletteEngineFilter(engineId, workflowProbesFor(engineId));
  }

  /** True iff a non-empty engine id is stored — i.e. the palette should actually filter. */
  public boolean isActive() {
    return StringUtils.isNotEmpty(engineId);
  }

  /** The engine id currently driving the filter, or empty when {@link #isActive()} is false. */
  public String getEngineId() {
    return engineId;
  }

  /**
   * @return false if every engine in the filter group refuses this plugin (palette should hide it);
   *     true for SUPPORTED or UNKNOWN verdicts on at least one group member, or when no filter is
   *     active.
   */
  public boolean isPluginAllowed(IPlugin plugin) {
    if (!isActive() || plugin == null) {
      return true;
    }
    List<String> ids = expandGroup(engineId, HOP_PIPELINE_ENGINE_IDS, HOP_WORKFLOW_ENGINE_IDS);
    for (int i = 0; i < ids.size(); i++) {
      EngineProbe probe = i < probes.size() ? probes.get(i) : null;
      EngineCompatibility verdict =
          EngineCompatibilityResolver.resolve(
              plugin, ids.get(i), probe == null ? null : probe::supports);
      if (!verdict.isUnsupported()) {
        return true;
      }
    }
    return false;
  }

  // ─── config accessors ────────────────────────────────────────────────────────

  public static String getPipelineDesignEngineId() {
    return HopConfig.readOptionString(CONFIG_KEY_PIPELINE_DESIGN_ENGINE, "");
  }

  public static void setPipelineDesignEngineId(String engineId) {
    HopConfig.getInstance()
        .saveOption(CONFIG_KEY_PIPELINE_DESIGN_ENGINE, engineId == null ? "" : engineId);
  }

  public static String getWorkflowDesignEngineId() {
    return HopConfig.readOptionString(CONFIG_KEY_WORKFLOW_DESIGN_ENGINE, "");
  }

  public static void setWorkflowDesignEngineId(String engineId) {
    HopConfig.getInstance()
        .saveOption(CONFIG_KEY_WORKFLOW_DESIGN_ENGINE, engineId == null ? "" : engineId);
  }

  // ─── labels for toolbar combos ───────────────────────────────────────────────

  /**
   * Combo entries for the pipeline-canvas filter, in display order: {@link #NO_FILTER_LABEL}, the
   * {@link #HOP_ENGINE_GROUP_ID Hop} group (when any Local/Remote pipeline engine exists), then
   * every non-Hop pipeline engine sorted alphabetically.
   */
  public static List<String> getPipelineEngineLabels() {
    return labelsFor(PipelineEnginePluginType.class, HOP_PIPELINE_ENGINE_IDS);
  }

  /**
   * Combo entries for the workflow-canvas filter, same ordering as {@link
   * #getPipelineEngineLabels()}.
   */
  public static List<String> getWorkflowEngineLabels() {
    return labelsFor(WorkflowEnginePluginType.class, HOP_WORKFLOW_ENGINE_IDS);
  }

  /**
   * {@code "All engines"} when {@code engineId} is empty; {@code "Hop"} when it is one of the
   * grouped ids or the synthetic group id itself; the plugin name otherwise.
   */
  public static String getPipelineEngineLabelForId(String engineId) {
    return labelForId(PipelineEnginePluginType.class, engineId, HOP_PIPELINE_ENGINE_IDS);
  }

  public static String getWorkflowEngineLabelForId(String engineId) {
    return labelForId(WorkflowEnginePluginType.class, engineId, HOP_WORKFLOW_ENGINE_IDS);
  }

  /**
   * {@code ""} when {@code label} is {@link #NO_FILTER_LABEL}; {@link #HOP_ENGINE_GROUP_ID} when
   * the label is the Hop group; otherwise the engine plugin id resolved from the name.
   */
  public static String getPipelineEngineIdForLabel(String label) {
    return idForLabel(PipelineEnginePluginType.class, label);
  }

  public static String getWorkflowEngineIdForLabel(String label) {
    return idForLabel(WorkflowEnginePluginType.class, label);
  }

  private static List<String> labelsFor(
      Class<? extends IPluginType> pluginType, Set<String> hopGroupIds) {
    List<String> labels = new ArrayList<>();
    labels.add(NO_FILTER_LABEL);
    try {
      List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(pluginType);

      // Hop group comes right after "All engines" — only if at least one member is registered.
      boolean hasHopMember = plugins.stream().anyMatch(p -> matchesAnyId(p, hopGroupIds));
      if (hasHopMember) {
        labels.add(HOP_ENGINE_GROUP_ID);
      }

      // Then every non-Hop engine sorted alphabetically by display name. The Beam engines all
      // share the "Beam *" prefix and therefore sort together; third-party engines slot in
      // wherever their name puts them.
      plugins.stream()
          .filter(p -> !matchesAnyId(p, hopGroupIds))
          .map(IPlugin::getName)
          .filter(StringUtils::isNotEmpty)
          .sorted(String.CASE_INSENSITIVE_ORDER)
          .forEach(labels::add);
    } catch (Exception e) {
      LogChannel.UI.logDebug("Could not list engines of type " + pluginType.getSimpleName(), e);
    }
    return labels;
  }

  private static String labelForId(
      Class<? extends IPluginType> pluginType, String engineId, Set<String> hopGroupIds) {
    if (StringUtils.isEmpty(engineId)) {
      return NO_FILTER_LABEL;
    }
    if (HOP_ENGINE_GROUP_ID.equals(engineId) || hopGroupIds.contains(engineId)) {
      return HOP_ENGINE_GROUP_ID;
    }
    try {
      IPlugin plugin = PluginRegistry.getInstance().findPluginWithId(pluginType, engineId);
      if (plugin != null && StringUtils.isNotEmpty(plugin.getName())) {
        return plugin.getName();
      }
    } catch (Exception ignored) {
      // fall through
    }
    return engineId;
  }

  private static String idForLabel(Class<? extends IPluginType> pluginType, String label) {
    if (StringUtils.isEmpty(label) || NO_FILTER_LABEL.equals(label)) {
      return "";
    }
    if (HOP_ENGINE_GROUP_ID.equals(label)) {
      return HOP_ENGINE_GROUP_ID;
    }
    try {
      List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(pluginType);
      for (IPlugin plugin : plugins) {
        if (label.equals(plugin.getName()) && plugin.getIds().length > 0) {
          return plugin.getIds()[0];
        }
      }
    } catch (Exception ignored) {
      // fall through
    }
    return "";
  }

  private static boolean matchesAnyId(IPlugin plugin, Set<String> ids) {
    if (plugin == null || plugin.getIds() == null) {
      return false;
    }
    for (String id : plugin.getIds()) {
      if (ids.contains(id)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Expands a (possibly group) engine id into the concrete plugin ids the filter must consult.
   * Single-engine values pass through unchanged; the synthetic {@link #HOP_ENGINE_GROUP_ID} fans
   * out to the union of pipeline + workflow Hop ids so that one logical "Hop" filter covers both
   * canvases that share this class.
   */
  private static List<String> expandGroup(
      String engineId, Set<String> hopPipelineIds, Set<String> hopWorkflowIds) {
    if (HOP_ENGINE_GROUP_ID.equals(engineId)) {
      Set<String> all = new LinkedHashSet<>();
      all.addAll(hopPipelineIds);
      all.addAll(hopWorkflowIds);
      return new ArrayList<>(all);
    }
    return List.of(engineId);
  }

  private static List<EngineProbe> pipelineProbesFor(String engineId) {
    return probesFor(engineId, HOP_PIPELINE_ENGINE_IDS, HOP_WORKFLOW_ENGINE_IDS, true);
  }

  private static List<EngineProbe> workflowProbesFor(String engineId) {
    return probesFor(engineId, HOP_PIPELINE_ENGINE_IDS, HOP_WORKFLOW_ENGINE_IDS, false);
  }

  private static List<EngineProbe> probesFor(
      String engineId,
      Set<String> hopPipelineIds,
      Set<String> hopWorkflowIds,
      boolean forPipeline) {
    if (StringUtils.isEmpty(engineId)) {
      return List.of();
    }
    List<String> ids = expandGroup(engineId, hopPipelineIds, hopWorkflowIds);
    List<EngineProbe> probes = new ArrayList<>(ids.size());
    for (String id : ids) {
      EngineProbe probe =
          forPipeline ? PipelineEngineProbe.tryLoad(id) : WorkflowEngineProbe.tryLoad(id);
      probes.add(probe); // intentionally allow null — isPluginAllowed falls back to annotation-only
    }
    return probes;
  }

  // ─── engine probes (one per side; isolate the cast + classloader risks) ──────

  /**
   * Loads a bare pipeline engine instance for the given id and returns its {@code supports} method
   * as a {@link java.util.function.Function} — for callers (e.g. the GUI run delegate) that want to
   * pre-flight engine compatibility before {@code PipelineEngineFactory.createPipelineEngine}
   * performs the heavier setup for the real run. Returns null when no engine can be loaded; the
   * caller should pass null to the resolver, which falls back to annotation-only checks.
   */
  public static java.util.function.Function<IPlugin, EngineCompatibility> loadPipelineEngineProbe(
      String engineId) {
    EngineProbe probe = PipelineEngineProbe.tryLoad(engineId);
    return probe == null ? null : probe::supports;
  }

  /** Workflow counterpart of {@link #loadPipelineEngineProbe(String)}. */
  public static java.util.function.Function<IPlugin, EngineCompatibility> loadWorkflowEngineProbe(
      String engineId) {
    EngineProbe probe = WorkflowEngineProbe.tryLoad(engineId);
    return probe == null ? null : probe::supports;
  }

  private interface EngineProbe {
    EngineCompatibility supports(IPlugin plugin);
  }

  private static final class PipelineEngineProbe implements EngineProbe {
    private final IPipelineEngine<?> engine;

    private PipelineEngineProbe(IPipelineEngine<?> engine) {
      this.engine = engine;
    }

    static EngineProbe tryLoad(String engineId) {
      if (StringUtils.isEmpty(engineId)) {
        return null;
      }
      try {
        IPlugin enginePlugin =
            PluginRegistry.getInstance().findPluginWithId(PipelineEnginePluginType.class, engineId);
        if (enginePlugin == null) {
          return null;
        }
        Object instance = PluginRegistry.getInstance().loadClass(enginePlugin);
        if (instance instanceof IPipelineEngine<?> pipelineEngine) {
          return new PipelineEngineProbe(pipelineEngine);
        }
      } catch (Exception e) {
        LogChannel.UI.logDebug(
            "Could not instantiate pipeline engine '" + engineId + "' for palette filtering", e);
      }
      return null;
    }

    @Override
    public EngineCompatibility supports(IPlugin plugin) {
      return engine.supports(plugin);
    }
  }

  private static final class WorkflowEngineProbe implements EngineProbe {
    private final IWorkflowEngine<?> engine;

    private WorkflowEngineProbe(IWorkflowEngine<?> engine) {
      this.engine = engine;
    }

    static EngineProbe tryLoad(String engineId) {
      if (StringUtils.isEmpty(engineId)) {
        return null;
      }
      try {
        IPlugin enginePlugin =
            PluginRegistry.getInstance().findPluginWithId(WorkflowEnginePluginType.class, engineId);
        if (enginePlugin == null) {
          return null;
        }
        Object instance = PluginRegistry.getInstance().loadClass(enginePlugin);
        if (instance instanceof IWorkflowEngine<?> workflowEngine) {
          return new WorkflowEngineProbe(workflowEngine);
        }
      } catch (Exception e) {
        LogChannel.UI.logDebug(
            "Could not instantiate workflow engine '" + engineId + "' for palette filtering", e);
      }
      return null;
    }

    @Override
    public EngineCompatibility supports(IPlugin plugin) {
      return engine.supports(plugin);
    }
  }
}
