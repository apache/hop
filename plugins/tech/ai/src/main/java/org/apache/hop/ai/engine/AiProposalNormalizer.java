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
package org.apache.hop.ai.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/**
 * Repairs the mistakes models make in proposals before they are checked, so the user does not have
 * to: a workflow type in a pipeline, {@code fromAction} where {@code fromTransform} is meant, a
 * missing plugin id that the name gives away, or a missing location. Only fixes with one clear
 * meaning are made; anything else is left for the validator to report.
 */
public final class AiProposalNormalizer {

  private static final int STEP_X = 160;
  private static final int DEFAULT_X = 100;
  private static final int DEFAULT_Y = 100;

  /** Everything that differs between pipelines and workflows. */
  private record Kind(
      Map<String, String> typeFixes,
      Map<String, String> keyFixes,
      String addType,
      String hopType,
      String pluginIdKey,
      String fromKey,
      String toKey,
      Class<? extends IPluginType> pluginType,
      Function<String, Point> locationOf) {}

  private AiProposalNormalizer() {}

  public static void forPipeline(PipelineMeta pipelineMeta, List<AiProposal> proposals) {
    Map<String, String> types = new HashMap<>();
    types.put("ADD_ACTION", "ADD_TRANSFORM");
    types.put("DELETE_ACTION", "DELETE_TRANSFORM");
    types.put("RENAME_ACTION", "RENAME_TRANSFORM");
    types.put("ADD_WORKFLOW_HOP", "ADD_PIPELINE_HOP");
    types.put("DELETE_WORKFLOW_HOP", "DELETE_PIPELINE_HOP");
    types.put("SET_ACTION_LOCATION", "SET_TRANSFORM_LOCATION");
    types.put("ADD_WORKFLOW_NOTE", "ADD_PIPELINE_NOTE");
    types.put("CONFIGURE_ACTION", "CONFIGURE_TRANSFORM");
    types.put("CLIPBOARD_ACTIONS", "CLIPBOARD_TRANSFORMS");
    types.put("REPLACE_ACTION", "REPLACE_TRANSFORM");
    types.put("ADD_HOP", "ADD_PIPELINE_HOP");
    types.put("DELETE_HOP", "DELETE_PIPELINE_HOP");
    Map<String, String> keys = new HashMap<>();
    keys.put("actionPluginId", "transformPluginId");
    keys.put("actionName", "transformName");
    keys.put("fromAction", "fromTransform");
    keys.put("toAction", "toTransform");
    commonKeys(keys, "transformPluginId", "fromTransform", "toTransform");
    normalize(
        proposals,
        new Kind(
            types,
            keys,
            "ADD_TRANSFORM",
            "ADD_PIPELINE_HOP",
            "transformPluginId",
            "fromTransform",
            "toTransform",
            TransformPluginType.class,
            name -> {
              TransformMeta transform =
                  pipelineMeta == null ? null : pipelineMeta.findTransform(name);
              return transform == null ? null : transform.getLocation();
            }),
        pipelineMeta == null
            ? List.of()
            : pipelineMeta.getTransforms().stream().map(TransformMeta::getLocation).toList(),
        pipelineMeta == null
            ? List.of()
            : pipelineMeta.getTransforms().stream().map(TransformMeta::getName).toList());
  }

  public static void forWorkflow(WorkflowMeta workflowMeta, List<AiProposal> proposals) {
    Map<String, String> types = new HashMap<>();
    types.put("ADD_TRANSFORM", "ADD_ACTION");
    types.put("DELETE_TRANSFORM", "DELETE_ACTION");
    types.put("RENAME_TRANSFORM", "RENAME_ACTION");
    types.put("ADD_PIPELINE_HOP", "ADD_WORKFLOW_HOP");
    types.put("DELETE_PIPELINE_HOP", "DELETE_WORKFLOW_HOP");
    types.put("SET_TRANSFORM_LOCATION", "SET_ACTION_LOCATION");
    types.put("ADD_PIPELINE_NOTE", "ADD_WORKFLOW_NOTE");
    types.put("CONFIGURE_TRANSFORM", "CONFIGURE_ACTION");
    types.put("CLIPBOARD_TRANSFORMS", "CLIPBOARD_ACTIONS");
    types.put("REPLACE_TRANSFORM", "REPLACE_ACTION");
    types.put("ADD_HOP", "ADD_WORKFLOW_HOP");
    types.put("DELETE_HOP", "DELETE_WORKFLOW_HOP");
    Map<String, String> keys = new HashMap<>();
    keys.put("transformPluginId", "actionPluginId");
    keys.put("transformName", "actionName");
    keys.put("fromTransform", "fromAction");
    keys.put("toTransform", "toAction");
    commonKeys(keys, "actionPluginId", "fromAction", "toAction");
    normalize(
        proposals,
        new Kind(
            types,
            keys,
            "ADD_ACTION",
            "ADD_WORKFLOW_HOP",
            "actionPluginId",
            "fromAction",
            "toAction",
            ActionPluginType.class,
            name -> {
              ActionMeta action = workflowMeta == null ? null : workflowMeta.findAction(name);
              return action == null ? null : action.getLocation();
            }),
        workflowMeta == null
            ? List.of()
            : workflowMeta.getActions().stream().map(ActionMeta::getLocation).toList(),
        workflowMeta == null
            ? List.of()
            : workflowMeta.getActions().stream().map(ActionMeta::getName).toList());
  }

  private static void commonKeys(
      Map<String, String> keys, String pluginKey, String from, String to) {
    for (String alias :
        List.of("pluginId", "plugin", "pluginType", "transformType", "actionType")) {
      keys.put(alias, pluginKey);
    }
    keys.put("from", from);
    keys.put("source", from);
    keys.put("to", to);
    keys.put("target", to);
  }

  private static void normalize(
      List<AiProposal> proposals, Kind kind, List<Point> existing, List<String> existingNames) {
    if (proposals == null) {
      return;
    }
    int rightMost = DEFAULT_X - STEP_X;
    int top = DEFAULT_Y;
    for (Point point : existing) {
      if (point != null && point.x > rightMost) {
        rightMost = point.x;
        top = point.y;
      }
    }
    // First the types and parameter names of every proposal, so the hops are readable when the
    // transforms or actions they connect are placed.
    for (AiProposal proposal : proposals) {
      if (proposal == null || Utils.isEmpty(proposal.getType())) {
        continue;
      }
      String type = proposal.getType().trim().toUpperCase(Locale.ROOT);
      proposal.setType(kind.typeFixes().getOrDefault(type, type));
      Map<String, String> parameters = proposal.getParameters();
      for (Map.Entry<String, String> fix : kind.keyFixes().entrySet()) {
        if (parameters.containsKey(fix.getKey()) && Utils.isEmpty(parameters.get(fix.getValue()))) {
          parameters.put(fix.getValue(), parameters.remove(fix.getKey()));
        }
      }
    }
    Map<String, Point> placed = new HashMap<>();
    for (AiProposal proposal : proposals) {
      if (proposal == null || !kind.addType().equals(proposal.getType())) {
        continue;
      }
      Map<String, String> parameters = proposal.getParameters();
      IPlugin plugin = findPlugin(kind.pluginType(), parameters.get(kind.pluginIdKey()));
      if (plugin == null) {
        plugin = guessPlugin(kind.pluginType(), parameters.get("name"), proposal.getDescription());
        if (plugin != null) {
          parameters.put(kind.pluginIdKey(), plugin.getIds()[0]);
          proposal.setDescription(
              Const.NVL(proposal.getDescription(), "")
                  + " (plugin "
                  + plugin.getIds()[0]
                  + ", found from the name)");
        }
      }
      if (Utils.isEmpty(parameters.get("name")) && plugin != null) {
        parameters.put("name", plugin.getName());
      }
      if (AiProposalParamSupport.parseLocation(proposal).isValid()) {
        continue;
      }
      // Place it right of what it is connected from, else right of everything.
      Point from = upstreamLocation(proposals, kind, parameters.get("name"), placed);
      Point location;
      if (from != null) {
        location = new Point(from.x + STEP_X, from.y);
      } else {
        rightMost += STEP_X;
        location = new Point(rightMost, top);
      }
      rightMost = Math.max(rightMost, location.x);
      parameters.put("locationX", Integer.toString(location.x));
      parameters.put("locationY", Integer.toString(location.y));
      if (!Utils.isEmpty(parameters.get("name"))) {
        placed.put(parameters.get("name"), location);
      }
    }
    resolveHopEnds(proposals, kind, existingNames);
  }

  /**
   * Hops that name a node by its plugin id, or by a slightly different name, point at the node that
   * is clearly meant: one with that exact name, else the one added here with that plugin id, else
   * the one whose name only differs in case and spaces. Only when exactly one fits.
   */
  private static void resolveHopEnds(
      List<AiProposal> proposals, Kind kind, List<String> existingNames) {
    List<String> names = new ArrayList<>(existingNames);
    Map<String, List<String>> addedByPluginId = new HashMap<>();
    for (AiProposal proposal : proposals) {
      if (proposal != null && kind.addType().equals(proposal.getType())) {
        String name = proposal.parameter("name");
        if (!Utils.isEmpty(name)) {
          names.add(name);
          String pluginId = proposal.parameter(kind.pluginIdKey());
          if (!Utils.isEmpty(pluginId)) {
            addedByPluginId.computeIfAbsent(simple(pluginId), key -> new ArrayList<>()).add(name);
          }
        }
      }
    }
    for (AiProposal proposal : proposals) {
      if (proposal == null || !kind.hopType().equals(proposal.getType())) {
        continue;
      }
      for (String key : List.of(kind.fromKey(), kind.toKey())) {
        String end = proposal.parameter(key);
        if (Utils.isEmpty(end) || names.contains(end)) {
          continue;
        }
        String resolved = null;
        List<String> byPlugin = addedByPluginId.get(simple(end));
        if (byPlugin != null && byPlugin.size() == 1) {
          resolved = byPlugin.get(0);
        } else {
          List<String> similar =
              names.stream().filter(name -> simple(name).equals(simple(end))).toList();
          if (similar.size() == 1) {
            resolved = similar.get(0);
          } else {
            // An invented name such as "dummy-new": the one node whose name starts like it.
            String word = firstWord(end);
            List<String> starting =
                word.length() < 4
                    ? List.of()
                    : names.stream().filter(name -> simple(name).startsWith(word)).toList();
            if (starting.size() == 1) {
              resolved = starting.get(0);
            }
          }
        }
        if (resolved != null) {
          proposal.getParameters().put(key, resolved);
        }
      }
    }
  }

  private static Point upstreamLocation(
      List<AiProposal> proposals, Kind kind, String name, Map<String, Point> placed) {
    if (Utils.isEmpty(name)) {
      return null;
    }
    for (AiProposal hop : proposals) {
      if (hop == null || !kind.hopType().equals(hop.getType())) {
        continue;
      }
      if (name.equals(hop.parameter(kind.toKey()))) {
        String from = hop.parameter(kind.fromKey());
        if (from == null) {
          continue;
        }
        Point point = placed.get(from);
        return point != null ? point : kind.locationOf().apply(from);
      }
    }
    return null;
  }

  static IPlugin findPlugin(Class<? extends IPluginType> type, String id) {
    if (Utils.isEmpty(id)) {
      return null;
    }
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.findPluginWithId(type, id.trim());
    if (plugin != null) {
      return plugin;
    }
    String wanted = simple(id);
    for (IPlugin candidate : registry.getPlugins(type)) {
      if (candidate.getIds() != null
          && candidate.getIds().length > 0
          && (simple(candidate.getIds()[0]).equals(wanted)
              || simple(candidate.getName()).equals(wanted))) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * The plugin a name or description points at: the longest plugin id or name found in it, so
   * "Write to log" finds WriteToLog rather than a shorter plugin whose name happens to be inside.
   */
  static IPlugin guessPlugin(Class<? extends IPluginType> type, String... texts) {
    IPlugin best = null;
    int bestLength = 0;
    for (String text : texts) {
      if (Utils.isEmpty(text)) {
        continue;
      }
      String haystack = simple(text);
      for (IPlugin plugin : PluginRegistry.getInstance().getPlugins(type)) {
        if (plugin.getIds() == null || plugin.getIds().length == 0) {
          continue;
        }
        for (String needle : List.of(simple(plugin.getIds()[0]), simple(nameWithoutNote(plugin)))) {
          if (needle.length() >= 4 && needle.length() > bestLength && haystack.contains(needle)) {
            best = plugin;
            bestLength = needle.length();
          }
        }
      }
      if (best != null) {
        return best;
      }
    }
    return best;
  }

  /** "Dummy (do nothing)" is called Dummy. */
  private static String nameWithoutNote(IPlugin plugin) {
    String name = Const.NVL(plugin.getName(), "");
    int bracket = name.indexOf('(');
    return bracket > 0 ? name.substring(0, bracket) : name;
  }

  private static String firstWord(String text) {
    String[] words = text.toLowerCase(Locale.ROOT).split("[^a-z0-9]+");
    for (String word : words) {
      if (!word.isEmpty()) {
        return word;
      }
    }
    return "";
  }

  private static String simple(String text) {
    return text == null ? "" : text.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
  }
}
