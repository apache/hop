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
    workflowMeta.getActionCopies().forEach( node -> {
      JsonObject jsonNode = new JsonObject();
      jsonNode.add( "x", node.getLocation().x );
      jsonNode.add( "y", node.getLocation().y );
      jsonNode.add( "selected", node.isSelected() );

      SwtUniversalImage swtImage = null;

      if ( node.isSpecial() ) {
        if ( node.isStart() ) {
          swtImage = GuiResource.getInstance().getSwtImageStart();
        }
        if ( node.isDummy() ) {
          swtImage = GuiResource.getInstance().getSwtImageDummy();
        }
      } else {
        String configId = node.getAction().getPluginId();
        if ( configId != null ) {
          swtImage = GuiResource.getInstance().getImagesActions().get( configId );
        }
      }
      if ( node.isMissing() ) {
        swtImage = GuiResource.getInstance().getSwtImageMissing();
      }
      if ( swtImage == null ) {
        return;
      }

      Image image = swtImage.getAsBitmapForSize( Display.getCurrent(), Math.round( iconSize * magnification ), Math.round( iconSize * magnification ) );
      jsonNode.add( "img",  image.internalImage.getResourceName() );
      jsonNodes.add( node.getName(), jsonNode );
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
    pipelineMeta.getTransforms().forEach( transform -> {
      JsonObject jsonNode = new JsonObject();
      jsonNode.add( "x", transform.getLocation().x );
      jsonNode.add( "y", transform.getLocation().y );
      jsonNode.add( "selected", transform.isSelected() );
      Image im = null;
      if ( transform.isMissing() ) {
        im = GuiResource.getInstance().getImageMissing();
      } else {
        im = GuiResource.getInstance().getImagesTransforms().get( transform.getTransformPluginId() )
        .getAsBitmapForSize( Display.getCurrent(), Math.round( iconSize * magnification ), Math.round( iconSize * magnification ) );
      }
      jsonNode.add( "img",  im.internalImage.getResourceName() );
      jsonNodes.add( transform.getName(), jsonNode );
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
