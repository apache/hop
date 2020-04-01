package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.StepErrorMeta;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiPipelineClipboardDelegate {
  private static final Class<?> PKG = HopGui.class; // i18n messages bundle location

  public static final String XML_TAG_PIPELINE_STEPS = "pipeline-steps";
  private static final String XML_TAG_STEPS = "steps";

  private HopGui hopGui;
  private HopGuiPipelineGraph pipelineGraph;
  private LogChannelInterface log;

  public HopGuiPipelineClipboardDelegate( HopGui hopGui, HopGuiPipelineGraph pipelineGraph ) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
    this.log = hopGui.getLog();
  }

  public void toClipboard( String clipText ) {
    try {
      GUIResource.getInstance().toClipboard( clipText );
    } catch ( Throwable e ) {
      new ErrorDialog( hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Title" ), BaseMessages
        .getString( PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Message" ), e );
    }
  }

  public String fromClipboard() {
    try {
      return GUIResource.getInstance().fromClipboard();
    } catch ( Throwable e ) {
      new ErrorDialog(
        hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Title" ), BaseMessages
        .getString( PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Message" ), e );
      return null;
    }
  }

  public void pasteXML( PipelineMeta pipelineMeta, String clipcontent, Point loc ) {

    try {
      Document doc = XMLHandler.loadXMLString( clipcontent );
      Node pipelineNode = XMLHandler.getSubNode( doc, XML_TAG_PIPELINE_STEPS );
      // De-select all, re-select pasted steps...
      pipelineMeta.unselectAll();

      Node stepsNode = XMLHandler.getSubNode( pipelineNode, "steps" );
      int nr = XMLHandler.countNodes( stepsNode, "step" );
      if ( log.isDebug() ) {
        // "I found "+nr+" steps to paste on location: "
        log.logDebug( BaseMessages.getString( PKG, "HopGui.Log.FoundSteps", "" + nr ) + loc );
      }
      StepMeta[] steps = new StepMeta[ nr ];
      ArrayList<String> stepOldNames = new ArrayList<>( nr );

      // Point min = new Point(loc.x, loc.y);
      Point min = new Point( 99999999, 99999999 );

      // Load the steps...
      for ( int i = 0; i < nr; i++ ) {
        Node stepNode = XMLHandler.getSubNodeByNr( stepsNode, "step", i );
        steps[ i ] = new StepMeta( stepNode, hopGui.getMetaStore() );

        if ( loc != null ) {
          Point p = steps[ i ].getLocation();

          if ( min.x > p.x ) {
            min.x = p.x;
          }
          if ( min.y > p.y ) {
            min.y = p.y;
          }
        }
      }

      // Load the hops...
      Node hopsNode = XMLHandler.getSubNode( pipelineNode, "order" );
      nr = XMLHandler.countNodes( hopsNode, "hop" );
      if ( log.isDebug() ) {
        // "I found "+nr+" hops to paste."
        log.logDebug( BaseMessages.getString( PKG, "HopGui.Log.FoundHops", "" + nr ) );
      }
      PipelineHopMeta[] hops = new PipelineHopMeta[ nr ];

      for ( int i = 0; i < nr; i++ ) {
        Node hopNode = XMLHandler.getSubNodeByNr( hopsNode, "hop", i );
        hops[ i ] = new PipelineHopMeta( hopNode, Arrays.asList( steps ) );
      }

      // This is the offset:
      Point offset = new Point( loc.x - min.x, loc.y - min.y );

      // Undo/redo object positions...
      int[] position = new int[ steps.length ];

      for ( int i = 0; i < steps.length; i++ ) {
        Point p = steps[ i ].getLocation();
        String name = steps[ i ].getName();

        steps[ i ].setLocation( p.x + offset.x, p.y + offset.y );

        // Check the name, find alternative...
        stepOldNames.add( name );
        steps[ i ].setName( pipelineMeta.getAlternativeStepname( name ) );
        pipelineMeta.addStep( steps[ i ] );
        position[ i ] = pipelineMeta.indexOfStep( steps[ i ] );
        steps[ i ].setSelected( true );
      }

      // Add the hops too...
      for ( PipelineHopMeta hop : hops ) {
        pipelineMeta.addPipelineHop( hop );
      }

      // Load the notes...
      Node notesNode = XMLHandler.getSubNode( pipelineNode, "notepads" );
      nr = XMLHandler.countNodes( notesNode, "notepad" );
      if ( log.isDebug() ) {
        // "I found "+nr+" notepads to paste."
        log.logDebug( BaseMessages.getString( PKG, "HopGui.Log.FoundNotepads", "" + nr ) );
      }
      NotePadMeta[] notes = new NotePadMeta[ nr ];

      for ( int i = 0; i < notes.length; i++ ) {
        Node noteNode = XMLHandler.getSubNodeByNr( notesNode, "notepad", i );
        notes[ i ] = new NotePadMeta( noteNode );
        Point p = notes[ i ].getLocation();
        notes[ i ].setLocation( p.x + offset.x, p.y + offset.y );
        pipelineMeta.addNote( notes[ i ] );
        notes[ i ].setSelected( true );
      }

      // Set the source and target steps ...
      for ( StepMeta step : steps ) {
        StepMetaInterface smi = step.getStepMetaInterface();
        smi.searchInfoAndTargetSteps( pipelineMeta.getSteps() );
      }

      // Set the error handling hops
      Node errorHandlingNode = XMLHandler.getSubNode( pipelineNode, PipelineMeta.XML_TAG_STEP_ERROR_HANDLING );
      int nrErrorHandlers = XMLHandler.countNodes( errorHandlingNode, StepErrorMeta.XML_ERROR_TAG );
      for ( int i = 0; i < nrErrorHandlers; i++ ) {
        Node stepErrorMetaNode = XMLHandler.getSubNodeByNr( errorHandlingNode, StepErrorMeta.XML_ERROR_TAG, i );
        StepErrorMeta stepErrorMeta =
          new StepErrorMeta( pipelineMeta.getParentVariableSpace(), stepErrorMetaNode, pipelineMeta.getSteps() );

        // Handle pasting multiple times, need to update source and target step names
        int srcStepPos = stepOldNames.indexOf( stepErrorMeta.getSourceStep().getName() );
        int tgtStepPos = stepOldNames.indexOf( stepErrorMeta.getTargetStep().getName() );
        StepMeta sourceStep = pipelineMeta.findStep( steps[ srcStepPos ].getName() );
        if ( sourceStep != null ) {
          sourceStep.setStepErrorMeta( stepErrorMeta );
        }
        sourceStep.setStepErrorMeta( null );
        if ( tgtStepPos >= 0 ) {
          sourceStep.setStepErrorMeta( stepErrorMeta );
          StepMeta targetStep = pipelineMeta.findStep( steps[ tgtStepPos ].getName() );
          stepErrorMeta.setSourceStep( sourceStep );
          stepErrorMeta.setTargetStep( targetStep );
        }
      }

      // Save undo information too...
      hopGui.undoDelegate.addUndoNew( pipelineMeta, steps, position, false );

      int[] hopPos = new int[ hops.length ];
      for ( int i = 0; i < hops.length; i++ ) {
        hopPos[ i ] = pipelineMeta.indexOfPipelineHop( hops[ i ] );
      }
      hopGui.undoDelegate.addUndoNew( pipelineMeta, hops, hopPos, true );

      int[] notePos = new int[ notes.length ];
      for ( int i = 0; i < notes.length; i++ ) {
        notePos[ i ] = pipelineMeta.indexOfNote( notes[ i ] );
      }
      hopGui.undoDelegate.addUndoNew( pipelineMeta, notes, notePos, true );

    } catch ( HopException e ) {
      // "Error pasting steps...",
      // "I was unable to paste steps to this pipeline"
      new ErrorDialog( hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.UnablePasteSteps.Title" ), BaseMessages
        .getString( PKG, "HopGui.Dialog.UnablePasteSteps.Message" ), e );
    }
    pipelineGraph.redraw();
  }

  public void copySelected( PipelineMeta pipelineMeta, List<StepMeta> steps, List<NotePadMeta> notes ) {
    if ( steps == null || steps.size() == 0 ) {
      return;
    }

    StringBuilder xml = new StringBuilder( 5000 ).append( XMLHandler.getXMLHeader() );
    try {
      xml.append( XMLHandler.openTag( XML_TAG_PIPELINE_STEPS ) ).append( Const.CR );

      xml.append( XMLHandler.openTag( XML_TAG_STEPS ) ).append( Const.CR );
      for ( StepMeta step : steps ) {
        xml.append( step.getXML() );
      }
      xml.append( XMLHandler.closeTag( XML_TAG_STEPS ) ).append( Const.CR );

      // Also check for the hops in between the selected steps...
      xml.append( XMLHandler.openTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );
      for ( StepMeta step1 : steps ) {
        for ( StepMeta step2 : steps ) {
          if ( step1 != step2 ) {
            PipelineHopMeta hop = pipelineMeta.findPipelineHop( step1, step2, true );
            if ( hop != null ) {
              // Ok, we found one...
              xml.append( hop.getXML() ).append( Const.CR );
            }
          }
        }
      }
      xml.append( XMLHandler.closeTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );

      xml.append( XMLHandler.openTag( PipelineMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );
      if ( notes != null ) {
        for ( NotePadMeta note : notes ) {
          xml.append( note.getXML() );
        }
      }
      xml.append( XMLHandler.closeTag( PipelineMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );

      xml.append( XMLHandler.openTag( PipelineMeta.XML_TAG_STEP_ERROR_HANDLING ) ).append( Const.CR );
      for ( StepMeta step : steps ) {
        if ( step.getStepErrorMeta() != null ) {
          xml.append( step.getStepErrorMeta().getXML() ).append( Const.CR );
        }
      }
      xml.append( XMLHandler.closeTag( PipelineMeta.XML_TAG_STEP_ERROR_HANDLING ) ).append( Const.CR );

      xml.append( XMLHandler.closeTag( XML_TAG_PIPELINE_STEPS ) ).append( Const.CR );

      toClipboard( xml.toString() );
    } catch ( Exception ex ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error encoding to XML", ex );
    }
  }


  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  /**
   * Gets pipelineGraph
   *
   * @return value of pipelineGraph
   */
  public HopGuiPipelineGraph getPipelineGraph() {
    return pipelineGraph;
  }

  /**
   * @param pipelineGraph The pipelineGraph to set
   */
  public void setPipelineGraph( HopGuiPipelineGraph pipelineGraph ) {
    this.pipelineGraph = pipelineGraph;
  }
}
