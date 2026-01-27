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

package org.apache.hop.ui.hopgui;

import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.rap.json.JsonArray;
import org.eclipse.rap.json.JsonObject;
import org.eclipse.swt.widgets.Canvas;

public class CanvasFacadeImpl extends CanvasFacade {

  public static final String CONST_SELECTED = "selected";

  @Override
  void setDataInternal(Canvas canvas, float magnification, DPoint offset, Object meta) {
    setDataCommon(canvas, magnification, offset, meta);
    if (meta instanceof WorkflowMeta workflowMeta) {
      setDataWorkflow(canvas, workflowMeta);
    } else {
      setDataPipeline(canvas, (PipelineMeta) meta);
    }
  }

  private void setDataCommon(Canvas canvas, float magnification, DPoint offset, Object meta) {
    JsonObject jsonProps = new JsonObject();
    jsonProps.add("themeId", System.getProperty(HopWeb.HOP_WEB_THEME, "light"));
    jsonProps.add("gridSize", PropsUi.getInstance().getCanvasGridSize());
    jsonProps.add("showGrid", PropsUi.getInstance().isShowCanvasGridEnabled());
    jsonProps.add("iconSize", PropsUi.getInstance().getIconSize());
    jsonProps.add("magnification", (float) (magnification * PropsUi.getNativeZoomFactor()));
    jsonProps.add("offsetX", offset.x);
    jsonProps.add("offsetY", offset.y);

    // Add pan data if available
    Point panStartOffset = (Point) canvas.getData("panStartOffset");
    Rectangle panBoundaries = (Rectangle) canvas.getData("panBoundaries");
    if (panStartOffset != null) {
      JsonObject jsonPanStartOffset = new JsonObject();
      jsonPanStartOffset.add("x", panStartOffset.x);
      jsonPanStartOffset.add("y", panStartOffset.y);
      jsonProps.add("panStartOffset", jsonPanStartOffset);
    }
    if (panBoundaries != null) {
      JsonObject jsonPanBoundaries = new JsonObject();
      jsonPanBoundaries.add("x", panBoundaries.x);
      jsonPanBoundaries.add("y", panBoundaries.y);
      jsonPanBoundaries.add("width", panBoundaries.width);
      jsonPanBoundaries.add("height", panBoundaries.height);
      jsonProps.add("panBoundaries", jsonPanBoundaries);
    }

    canvas.setData("props", jsonProps);

    JsonArray jsonNotes = new JsonArray();
    if (meta instanceof AbstractMeta abstractMeta) {
      abstractMeta
          .getNotes()
          .forEach(
              note -> {
                JsonObject jsonNote = new JsonObject();
                jsonNote.add("x", note.getLocation().x);
                jsonNote.add("y", note.getLocation().y);
                jsonNote.add("width", note.getWidth());
                jsonNote.add("height", note.getHeight());
                jsonNote.add(CONST_SELECTED, note.isSelected());
                jsonNote.add("note", note.getNote());
                jsonNotes.add(jsonNote);
              });
    }
    canvas.setData("notes", jsonNotes);
  }

  private void setDataWorkflow(Canvas canvas, WorkflowMeta workflowMeta) {
    JsonObject jsonNodes = new JsonObject();

    // Store the individual SVG images of the actions as well
    //
    workflowMeta
        .getActions()
        .forEach(
            actionMeta -> {
              JsonObject jsonNode = new JsonObject();
              jsonNode.add("x", actionMeta.getLocation().x);
              jsonNode.add("y", actionMeta.getLocation().y);
              jsonNode.add(CONST_SELECTED, actionMeta.isSelected());

              jsonNodes.add(actionMeta.getName(), jsonNode);
            });
    canvas.setData("nodes", jsonNodes);

    JsonArray jsonHops = new JsonArray();
    for (int i = 0; i < workflowMeta.nrWorkflowHops(); i++) {
      JsonObject jsonHop = new JsonObject();
      WorkflowHopMeta hop = workflowMeta.getWorkflowHop(i);
      jsonHop.add("from", hop.getFromAction().getName());
      jsonHop.add("to", hop.getToAction().getName());
      jsonHops.add(jsonHop);
    }
    canvas.setData("hops", jsonHops);
  }

  private void setDataPipeline(Canvas canvas, PipelineMeta pipelineMeta) {
    JsonObject jsonNodes = new JsonObject();

    pipelineMeta
        .getTransforms()
        .forEach(
            transformMeta -> {
              JsonObject jsonNode = new JsonObject();
              jsonNode.add("x", transformMeta.getLocation().x);
              jsonNode.add("y", transformMeta.getLocation().y);
              jsonNode.add(CONST_SELECTED, transformMeta.isSelected());

              jsonNodes.add(transformMeta.getName(), jsonNode);
            });
    canvas.setData("nodes", jsonNodes);

    JsonArray jsonHops = new JsonArray();
    for (int i = 0; i < pipelineMeta.nrPipelineHops(); i++) {
      JsonObject jsonHop = new JsonObject();
      PipelineHopMeta hop = pipelineMeta.getPipelineHop(i);
      if (hop.getFromTransform() != null && hop.getToTransform() != null) {
        jsonHop.add("from", hop.getFromTransform().getName());
        jsonHop.add("to", hop.getToTransform().getName());
        jsonHops.add(jsonHop);
      }
    }
    canvas.setData("hops", jsonHops);
  }
}
