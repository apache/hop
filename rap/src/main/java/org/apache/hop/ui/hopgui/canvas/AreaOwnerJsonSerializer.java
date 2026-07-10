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

package org.apache.hop.ui.hopgui.canvas;

import java.util.List;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.eclipse.rap.json.JsonArray;
import org.eclipse.rap.json.JsonObject;

/** Serializes {@link AreaOwner} click regions to JSON for the Hop Web canvas client. */
public final class AreaOwnerJsonSerializer {

  private AreaOwnerJsonSerializer() {}

  public static JsonArray toJsonArray(List<AreaOwner> areaOwners) {
    JsonArray areas = new JsonArray();
    if (areaOwners == null) {
      return areas;
    }
    for (AreaOwner areaOwner : areaOwners) {
      areas.add(toJson(areaOwner));
    }
    return areas;
  }

  public static JsonObject toJson(AreaOwner areaOwner) {
    JsonObject json = new JsonObject();
    json.add("areaType", areaOwner.getAreaType().name());
    Rectangle area = areaOwner.getArea();
    json.add("x", area.x);
    json.add("y", area.y);
    json.add("width", area.width);
    json.add("height", area.height);
    json.add("hover", areaOwner.getAreaType().isSupportHover());
    json.add(
        "owner", ownerToJson(areaOwner.getOwner(), areaOwner.getParent(), areaOwner.getAreaType()));
    return json;
  }

  private static JsonObject ownerToJson(Object owner, Object parent, AreaOwner.AreaType areaType) {
    JsonObject json = new JsonObject();
    if (owner == null) {
      json.add("kind", "none");
      return json;
    }
    if (owner instanceof TransformMeta transformMeta) {
      json.add("kind", "transform");
      json.add("name", transformMeta.getName());
      return json;
    }
    if (owner instanceof ActionMeta actionMeta) {
      json.add("kind", "action");
      json.add("name", actionMeta.getName());
      return json;
    }
    if (owner instanceof NotePadMeta notePadMeta) {
      json.add("kind", "note");
      json.add("note", notePadMeta.getNote());
      return json;
    }
    if (owner instanceof String label) {
      json.add("kind", "label");
      json.add("value", label);
      return json;
    }
    if (owner instanceof PipelineHopMeta pipelineHop) {
      json.add("kind", "pipelineHop");
      if (pipelineHop.getFromTransform() != null) {
        json.add("from", pipelineHop.getFromTransform().getName());
      }
      if (pipelineHop.getToTransform() != null) {
        json.add("to", pipelineHop.getToTransform().getName());
      }
      return json;
    }
    if (owner instanceof WorkflowHopMeta workflowHop) {
      json.add("kind", "workflowHop");
      if (workflowHop.getFromAction() != null) {
        json.add("from", workflowHop.getFromAction().getName());
      }
      if (workflowHop.getToAction() != null) {
        json.add("to", workflowHop.getToAction().getName());
      }
      return json;
    }
    if (owner instanceof RowBuffer) {
      json.add("kind", "rowBuffer");
      if (parent instanceof TransformMeta transformMeta) {
        json.add("transformName", transformMeta.getName());
      }
      return json;
    }
    LogChannel.UI.logDebug(
        "Unknown AreaOwner owner type for web canvas: "
            + owner.getClass().getName()
            + " ("
            + areaType
            + ")");
    json.add("kind", "unknown");
    json.add("type", owner.getClass().getName());
    return json;
  }
}
