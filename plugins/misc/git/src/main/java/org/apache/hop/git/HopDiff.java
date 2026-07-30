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
 *
 */

package org.apache.hop.git;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

public class HopDiff {
  public static final String ATTR_GIT = "Git";
  public static final String ATTR_STATUS = "Status";
  public static final String ATTR_GIT_HOPS = "GitHops";

  public static final String UNCHANGED = "UNCHANGED";
  public static final String CHANGED = "CHANGED";
  public static final String REMOVED = "DELETE";
  public static final String ADDED = "INSERT";

  /**
   * The location of a transform or an action, as written by TransformMeta and ActionMeta. Only
   * those two write an xloc/yloc pair, so this does not reach into what a plugin serialized.
   */
  private static final Pattern POSITION_TAGS =
      Pattern.compile("[ \\t]*<(xloc|yloc)>.*?</\\1>\\r?\\n?");

  /**
   * The name of the transform or action itself. Both TransformMeta and ActionMeta write it as the
   * first name tag of the element, ahead of anything the plugin serializes, so only the first
   * occurrence may be replaced -- a transform's own fields carry name tags of their own.
   */
  private static final Pattern NAME_TAG =
      Pattern.compile("[ \\t]*<name(\\s*/>|>.*?</name>)\\r?\\n?");

  private HopDiff() {}

  /**
   * Drop the position from serialized XML so that a transform or action which was only dragged
   * somewhere else does not read as modified. The surrounding GUI tag is left in place: it is
   * identical on both sides once the coordinates are gone.
   */
  private static String removePosition(String xml) {
    return POSITION_TAGS.matcher(xml).replaceAll("");
  }

  private static boolean sameXml(String xml1, String xml2, boolean ignorePosition) {
    if (ignorePosition) {
      return removePosition(xml1).equals(removePosition(xml2));
    }
    return xml1.equals(xml2);
  }

  /**
   * What is left of a transform or action once the two things a rename is allowed to alter -- its
   * name and where it sits -- are taken out. Two entries carrying the same remainder under
   * different names are the same one, renamed.
   *
   * <p>The position goes regardless of the ignore-position option: that option decides whether a
   * move counts as a change, which is a different question from whether these are the same
   * transform.
   */
  private static String identity(String xml) {
    return removePosition(NAME_TAG.matcher(xml).replaceFirst(""));
  }

  /**
   * Pair the names that exist on only one side against each other. A name present on both sides is
   * matched by the regular comparison and is never a rename; of what remains, two entries with the
   * same identity are a rename. Each candidate is claimed once, so identical transforms renamed in
   * the same commit pair off one by one rather than all collapsing onto the first.
   *
   * @return the names in the first version mapped to what they are called in the second
   */
  private static Map<String, String> pairRenames(
      Map<String, String> identities1, Map<String, String> identities2) {
    Map<String, String> renames = new LinkedHashMap<>();
    Set<String> claimed = new HashSet<>();

    for (Map.Entry<String, String> entry : identities1.entrySet()) {
      if (identities2.containsKey(entry.getKey())) {
        continue;
      }
      for (Map.Entry<String, String> candidate : identities2.entrySet()) {
        if (identities1.containsKey(candidate.getKey()) || claimed.contains(candidate.getKey())) {
          continue;
        }
        if (entry.getValue().equals(candidate.getValue())) {
          renames.put(entry.getKey(), candidate.getKey());
          claimed.add(candidate.getKey());
          break;
        }
      }
    }
    return renames;
  }

  private static String renamedTo(String name, Map<String, String> renames) {
    return renames.getOrDefault(name, name);
  }

  /** Transforms renamed between the two versions, by name in the first version. */
  public static Map<String, String> detectTransformRenames(
      PipelineMeta pipelineMeta1, PipelineMeta pipelineMeta2) {
    return pairRenames(transformIdentities(pipelineMeta1), transformIdentities(pipelineMeta2));
  }

  private static Map<String, String> transformIdentities(PipelineMeta pipelineMeta) {
    Map<String, String> identities = new LinkedHashMap<>();
    for (TransformMeta transform : pipelineMeta.getTransforms()) {
      // AttributeMap("Git") cannot affect the comparison: by the time the second version is
      // compared the first one already carries the status of the first pass.
      //
      Map<String, String> tmp = transform.getAttributesMap().remove(ATTR_GIT);
      try {
        identities.put(transform.getName(), identity(transform.getXml()));
      } catch (HopException e) {
        LogChannel.GENERAL.logError(
            "Error serializing transform '" + transform.getName() + "' to detect renames", e);
      } finally {
        transform.setAttributes(ATTR_GIT, tmp);
      }
    }
    return identities;
  }

  /** Actions renamed between the two versions, by name in the first version. */
  public static Map<String, String> detectActionRenames(
      WorkflowMeta workflowMeta1, WorkflowMeta workflowMeta2) {
    return pairRenames(actionIdentities(workflowMeta1), actionIdentities(workflowMeta2));
  }

  private static Map<String, String> actionIdentities(WorkflowMeta workflowMeta) {
    Map<String, String> identities = new LinkedHashMap<>();
    for (ActionMeta action : workflowMeta.getActions()) {
      Map<String, String> tmp = action.getAttributesMap().remove(ATTR_GIT);
      try {
        identities.put(action.getName(), identity(action.getXml()));
      } finally {
        action.setAttributes(ATTR_GIT, tmp);
      }
    }
    return identities;
  }

  public static PipelineMeta compareTransforms(
      PipelineMeta pipelineMeta1,
      PipelineMeta pipelineMeta2,
      boolean isForward,
      boolean ignorePosition,
      Map<String, String> renames) {
    pipelineMeta1
        .getTransforms()
        .forEach(
            transform -> {
              String name = renamedTo(transform.getName(), renames);
              Optional<TransformMeta> transform2 =
                  pipelineMeta2.getTransforms().stream()
                      .filter(obj -> name.equals(obj.getName()))
                      .findFirst();
              String status = null;
              if (transform2.isPresent()) {
                Map<String, String> tmp = null;
                Map<String, String> tmp2 = null;
                try {
                  // AttributeMap("Git") cannot affect the XML comparison
                  tmp = transform.getAttributesMap().remove(ATTR_GIT);
                  tmp2 = transform2.get().getAttributesMap().remove(ATTR_GIT);
                  if (sameXml(transform.getXml(), transform2.get().getXml(), ignorePosition)) {
                    status = UNCHANGED;
                  } else {
                    status = CHANGED;
                  }
                } catch (HopException e) {
                  e.printStackTrace();
                } finally {
                  transform.setAttributes(ATTR_GIT, tmp);
                  transform2.get().setAttributes(ATTR_GIT, tmp2);
                }
              } else {
                if (isForward) {
                  status = REMOVED;
                } else {
                  status = ADDED;
                }
              }
              transform.setAttribute(ATTR_GIT, ATTR_STATUS, status);
            });
    return pipelineMeta1;
  }

  public static PipelineMeta comparePipelineHops(
      PipelineMeta pipelineMeta1,
      PipelineMeta pipelineMeta2,
      boolean isForward,
      Map<String, String> renames) {
    pipelineMeta1
        .getPipelineHops()
        .forEach(
            hop -> {
              // A hop is identified by the transforms it connects, so it has to be looked up under
              // what those are called in the other version. The status is still stored under the
              // name in this version: that is what the painter asks for.
              //
              String hopName = getPipelineHopName(hop);
              String lookupName = getPipelineHopName(hop, renames);
              Optional<PipelineHopMeta> hop2 =
                  pipelineMeta2.getPipelineHops().stream()
                      .filter(otherHop -> lookupName.equals(getPipelineHopName(otherHop)))
                      .findFirst();
              String status = null;
              if (hop2.isPresent()) {
                if (hop.isEnabled() != hop2.get().isEnabled()) {
                  status = CHANGED;
                }
              } else {
                if (isForward) {
                  status = REMOVED;
                } else {
                  status = ADDED;
                }
              }
              if (status != null) {
                pipelineMeta1.setAttribute(ATTR_GIT_HOPS, hopName, status);
              }
            });
    return pipelineMeta1;
  }

  public static String getPipelineHopName(PipelineHopMeta hopMeta) {
    return getPipelineHopName(hopMeta, Map.of());
  }

  private static String getPipelineHopName(PipelineHopMeta hopMeta, Map<String, String> renames) {

    String name = "";
    TransformMeta from = hopMeta.getFromTransform();
    if (from != null) {
      name += renamedTo(from.getName(), renames);
    }
    name += " - ";
    TransformMeta to = hopMeta.getToTransform();
    if (to != null) {
      name += renamedTo(to.getName(), renames);
    }
    return name;
  }

  public static WorkflowMeta compareActions(
      WorkflowMeta workflowMeta1,
      WorkflowMeta workflowMeta2,
      boolean isForward,
      boolean ignorePosition,
      Map<String, String> renames) {
    workflowMeta1
        .getActions()
        .forEach(
            je -> {
              String name = renamedTo(je.getName(), renames);
              Optional<ActionMeta> je2 =
                  workflowMeta2.getActions().stream()
                      .filter(obj -> name.equals(obj.getName()))
                      .findFirst();
              String status = null;
              if (je2.isPresent()) {
                Map<String, String> tmp = null;
                Map<String, String> tmp2 = null;
                // AttributeMap("Git") cannot affect the XML comparison
                tmp = je.getAttributesMap().remove(ATTR_GIT);
                tmp2 = je2.get().getAttributesMap().remove(ATTR_GIT);
                if (sameXml(je.getXml(), je2.get().getXml(), ignorePosition)) {
                  status = UNCHANGED;
                } else {
                  status = CHANGED;
                }
                je.setAttributes(ATTR_GIT, tmp);
                je2.get().setAttributes(ATTR_GIT, tmp2);
              } else {
                if (isForward) {
                  status = REMOVED;
                } else {
                  status = ADDED;
                }
              }
              je.setAttribute(ATTR_GIT, ATTR_STATUS, status);
            });
    return workflowMeta1;
  }

  public static WorkflowMeta compareWorkflowHops(
      WorkflowMeta workflowMeta1,
      WorkflowMeta workflowMeta2,
      boolean isForward,
      Map<String, String> renames) {
    workflowMeta1
        .getWorkflowHops()
        .forEach(
            hop -> {
              String hopName = getWorkflowHopName(hop);
              String lookupName = getWorkflowHopName(hop, renames);
              Optional<WorkflowHopMeta> hop2 =
                  workflowMeta2.getWorkflowHops().stream()
                      .filter(otherHop -> lookupName.equals(getWorkflowHopName(otherHop)))
                      .findFirst();
              String status = null;
              if (hop2.isPresent()) {
                if (hop.isEnabled() != hop2.get().isEnabled()) {
                  status = CHANGED;
                }
              } else {
                if (isForward) {
                  status = REMOVED;
                } else {
                  status = ADDED;
                }
              }
              if (status != null) {
                workflowMeta1.setAttribute(ATTR_GIT_HOPS, hopName, status);
              }
            });
    return workflowMeta1;
  }

  public static String getWorkflowHopName(WorkflowHopMeta hopMeta) {
    return getWorkflowHopName(hopMeta, Map.of());
  }

  private static String getWorkflowHopName(WorkflowHopMeta hopMeta, Map<String, String> renames) {
    String name = "";
    ActionMeta from = hopMeta.getFromAction();
    if (from != null) {
      name += renamedTo(from.getName(), renames);
    }
    name += " - ";
    ActionMeta to = hopMeta.getToAction();
    if (to != null) {
      name += renamedTo(to.getName(), renames);
    }
    return name;
  }
}
