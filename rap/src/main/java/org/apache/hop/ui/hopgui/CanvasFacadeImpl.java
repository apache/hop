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
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.rap.json.JsonArray;
import org.eclipse.rap.json.JsonObject;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Display;

public class CanvasFacadeImpl extends CanvasFacade {

  @Override
  void setDataInternal( Canvas canvas, float magnification, AbstractMeta meta, Class type ) {
    setDataCommon( canvas, magnification, meta );
    if ( type == HopGuiWorkflowGraph.class ) {
      setDataWorkflow( canvas, magnification, meta );
    } else {
      setDataPipeline( canvas, magnification, meta );
    }
  }

  private void setDataCommon ( Canvas canvas, float magnification, AbstractMeta meta ) {
    JsonObject jsonProps = new JsonObject();
    jsonProps.add( "gridsize", PropsUi.getInstance().isShowCanvasGridEnabled() ? PropsUi.getInstance().getCanvasGridSize() : 1 );
    jsonProps.add( "iconsize", PropsUi.getInstance().getIconSize() );
    jsonProps.add( "magnification", magnification );
    canvas.setData( "props", jsonProps );

    JsonArray jsonNotes = new JsonArray();
    meta.getNotes().forEach( note -> {
      JsonObject jsonNote = new JsonObject();
      jsonNote.add( "x", note.getLocation().x );
      jsonNote.add( "y", note.getLocation().y );
      jsonNote.add( "width", note.getWidth() );
      jsonNote.add( "height", note.getHeight() );
      jsonNote.add( "selected", note.isSelected() );
      jsonNote.add( "note", note.getNote() );
      jsonNotes.add( jsonNote );
    } );
    canvas.setData( "notes", jsonNotes );
  }
  
  private void setDataWorkflow ( Canvas canvas, float magnification, AbstractMeta meta ) {
    final int iconSize = HopGui.getInstance().getProps().getIconSize();

    WorkflowMeta workflowMeta = (WorkflowMeta) meta;
    JsonObject jsonNodes = new JsonObject();
    workflowMeta.getActions().forEach( actionMeta -> {
      JsonObject jsonNode = new JsonObject();
      jsonNode.add( "x", actionMeta.getLocation().x );
      jsonNode.add( "y", actionMeta.getLocation().y );
      jsonNode.add( "selected", actionMeta.isSelected() );

      // Translated from org.apache.hop.ui.hopgui.shared.SwtGc.drawActionIcon(int, int, ActionMeta, float)
      SwtUniversalImage swtImage = null;

      if ( actionMeta.isMissing() ) {
        swtImage = GuiResource.getInstance().getSwtImageMissing();
      }
      else {
        String pluginId = actionMeta.getAction().getPluginId();
        if ( pluginId != null ) {
          swtImage = GuiResource.getInstance().getImagesActions().get( pluginId );
        }
      }

      if ( swtImage == null ) {
        return;
      }

      int w = Math.round( iconSize * magnification );
      int h = Math.round( iconSize * magnification );
      Image image = swtImage.getAsBitmapForSize( Display.getCurrent(), w, h );
      // Translated

      jsonNode.add( "img",  image.internalImage.getResourceName() );
      jsonNodes.add( actionMeta.getName(), jsonNode );
    } );
    canvas.setData( "nodes", jsonNodes );

    JsonArray jsonHops = new JsonArray();
    for ( int i = 0; i < workflowMeta.nrWorkflowHops(); i++ ) {
      JsonObject jsonHop = new JsonObject();
      WorkflowHopMeta hop = workflowMeta.getWorkflowHop( i );
      jsonHop.add( "from", hop.getFromAction().getName() );
      jsonHop.add( "to", hop.getToAction().getName() );
      jsonHops.add( jsonHop );
    }
    canvas.setData( "hops", jsonHops );
  }

  private void setDataPipeline( Canvas canvas, float magnification, AbstractMeta meta ) {
    final int iconSize = HopGui.getInstance().getProps().getIconSize();

    PipelineMeta pipelineMeta = (PipelineMeta) meta;
    JsonObject jsonNodes = new JsonObject();
    pipelineMeta.getTransforms().forEach( transformMeta -> {
      JsonObject jsonNode = new JsonObject();
      jsonNode.add( "x", transformMeta.getLocation().x );
      jsonNode.add( "y", transformMeta.getLocation().y );
      jsonNode.add( "selected", transformMeta.isSelected() );

      // Translated from org.apache.hop.ui.hopgui.shared.SwtGc.drawTransformIcon(int, int, TransformMeta, float)
      SwtUniversalImage swtImage = null;

      if ( transformMeta.isMissing() ) {
        swtImage = GuiResource.getInstance().getSwtImageMissing();
      } else if ( transformMeta.isDeprecated() ) {
        swtImage = GuiResource.getInstance().getSwtImageDeprecated();
      } else {
        String pluginId = transformMeta.getPluginId();
        if ( pluginId != null ) {
          swtImage = GuiResource.getInstance().getImagesTransforms().get( pluginId );
        }
      }

      if ( swtImage == null ) {
        return;
      }

      int w = Math.round( iconSize * magnification );
      int h = Math.round( iconSize * magnification );
      Image image = swtImage.getAsBitmapForSize( Display.getCurrent(), w, h );
      // Translated

      jsonNode.add( "img",  image.internalImage.getResourceName() );
      jsonNodes.add( transformMeta.getName(), jsonNode );
    } );
    canvas.setData( "nodes", jsonNodes );

    JsonArray jsonHops = new JsonArray();
    for ( int i = 0; i < pipelineMeta.nrPipelineHops(); i++ ) {
      JsonObject jsonHop = new JsonObject();
      PipelineHopMeta hop = pipelineMeta.getPipelineHop( i );
      if ( hop.getFromTransform() != null && hop.getToTransform() != null ) {
        jsonHop.add( "from", hop.getFromTransform().getName() );
        jsonHop.add( "to", hop.getToTransform().getName() );
        jsonHops.add( jsonHop );
      }
    }
    canvas.setData( "hops", jsonHops );
  }
}
