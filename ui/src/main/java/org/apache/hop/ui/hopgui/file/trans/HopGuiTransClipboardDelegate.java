package org.apache.hop.ui.hopgui.file.trans;

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepErrorMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopui.HopUi;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiTransClipboardDelegate {
  private static final Class<?> PKG = HopUi.class; // i18n messages bundle location

  public static final String XML_TAG_TRANSFORMATION_STEPS = "transformation-steps";
  private static final String XML_TAG_STEPS = "steps";

  private HopGui hopGui;
  private HopGuiTransGraph transGraph;
  private LogChannelInterface log;
  
  public HopGuiTransClipboardDelegate( HopGui hopGui, HopGuiTransGraph transGraph ) {
    this.hopGui = hopGui;
    this.transGraph = transGraph;
    this.log = hopGui.getLog();
  }

  public void toClipboard( String clipText ) {
    try {
      GUIResource.getInstance().toClipboard( clipText );
    } catch ( Throwable e ) {
      new ErrorDialog( hopGui.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.ExceptionCopyToClipboard.Title" ), BaseMessages
        .getString( PKG, "Spoon.Dialog.ExceptionCopyToClipboard.Message" ), e );
    }
  }

  public String fromClipboard() {
    try {
      return GUIResource.getInstance().fromClipboard();
    } catch ( Throwable e ) {
      new ErrorDialog(
        hopGui.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.ExceptionPasteFromClipboard.Title" ), BaseMessages
        .getString( PKG, "Spoon.Dialog.ExceptionPasteFromClipboard.Message" ), e );
      return null;
    }
  }

  public void pasteXML( TransMeta transMeta, String clipcontent, Point loc ) {

    try {
      Document doc = XMLHandler.loadXMLString( clipcontent );
      Node transNode = XMLHandler.getSubNode( doc, HopUi.XML_TAG_TRANSFORMATION_STEPS );
      // De-select all, re-select pasted steps...
      transMeta.unselectAll();

      Node stepsNode = XMLHandler.getSubNode( transNode, "steps" );
      int nr = XMLHandler.countNodes( stepsNode, "step" );
      if ( log.isDebug() ) {
        // "I found "+nr+" steps to paste on location: "
        log.logDebug( BaseMessages.getString( PKG, "Spoon.Log.FoundSteps", "" + nr ) + loc );
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
      Node hopsNode = XMLHandler.getSubNode( transNode, "order" );
      nr = XMLHandler.countNodes( hopsNode, "hop" );
      if ( log.isDebug() ) {
        // "I found "+nr+" hops to paste."
        log.logDebug( BaseMessages.getString( PKG, "Spoon.Log.FoundHops", "" + nr ) );
      }
      TransHopMeta[] hops = new TransHopMeta[ nr ];

      for ( int i = 0; i < nr; i++ ) {
        Node hopNode = XMLHandler.getSubNodeByNr( hopsNode, "hop", i );
        hops[ i ] = new TransHopMeta( hopNode, Arrays.asList( steps ) );
      }

      // This is the offset:
      Point offset = new Point( loc.x - min.x, loc.y - min.y );

      // Undo/redo object positions...
      int[] position = new int[ steps.length ];

      for ( int i = 0; i < steps.length; i++ ) {
        Point p = steps[ i ].getLocation();
        String name = steps[ i ].getName();

        steps[ i ].setLocation( p.x + offset.x, p.y + offset.y );
        steps[ i ].setDraw( true );

        // Check the name, find alternative...
        stepOldNames.add( name );
        steps[ i ].setName( transMeta.getAlternativeStepname( name ) );
        transMeta.addStep( steps[ i ] );
        position[ i ] = transMeta.indexOfStep( steps[ i ] );
        steps[ i ].setSelected( true );
      }

      // Add the hops too...
      for ( TransHopMeta hop : hops ) {
        transMeta.addTransHop( hop );
      }

      // Load the notes...
      Node notesNode = XMLHandler.getSubNode( transNode, "notepads" );
      nr = XMLHandler.countNodes( notesNode, "notepad" );
      if ( log.isDebug() ) {
        // "I found "+nr+" notepads to paste."
        log.logDebug( BaseMessages.getString( PKG, "Spoon.Log.FoundNotepads", "" + nr ) );
      }
      NotePadMeta[] notes = new NotePadMeta[ nr ];

      for ( int i = 0; i < notes.length; i++ ) {
        Node noteNode = XMLHandler.getSubNodeByNr( notesNode, "notepad", i );
        notes[ i ] = new NotePadMeta( noteNode );
        Point p = notes[ i ].getLocation();
        notes[ i ].setLocation( p.x + offset.x, p.y + offset.y );
        transMeta.addNote( notes[ i ] );
        notes[ i ].setSelected( true );
      }

      // Set the source and target steps ...
      for ( StepMeta step : steps ) {
        StepMetaInterface smi = step.getStepMetaInterface();
        smi.searchInfoAndTargetSteps( transMeta.getSteps() );
      }

      // Set the error handling hops
      Node errorHandlingNode = XMLHandler.getSubNode( transNode, TransMeta.XML_TAG_STEP_ERROR_HANDLING );
      int nrErrorHandlers = XMLHandler.countNodes( errorHandlingNode, StepErrorMeta.XML_ERROR_TAG );
      for ( int i = 0; i < nrErrorHandlers; i++ ) {
        Node stepErrorMetaNode = XMLHandler.getSubNodeByNr( errorHandlingNode, StepErrorMeta.XML_ERROR_TAG, i );
        StepErrorMeta stepErrorMeta =
          new StepErrorMeta( transMeta.getParentVariableSpace(), stepErrorMetaNode, transMeta.getSteps() );

        // Handle pasting multiple times, need to update source and target step names
        int srcStepPos = stepOldNames.indexOf( stepErrorMeta.getSourceStep().getName() );
        int tgtStepPos = stepOldNames.indexOf( stepErrorMeta.getTargetStep().getName() );
        StepMeta sourceStep = transMeta.findStep( steps[ srcStepPos ].getName() );
        if ( sourceStep != null ) {
          sourceStep.setStepErrorMeta( stepErrorMeta );
        }
        sourceStep.setStepErrorMeta( null );
        if ( tgtStepPos >= 0 ) {
          sourceStep.setStepErrorMeta( stepErrorMeta );
          StepMeta targetStep = transMeta.findStep( steps[ tgtStepPos ].getName() );
          stepErrorMeta.setSourceStep( sourceStep );
          stepErrorMeta.setTargetStep( targetStep );
        }
      }

      // Save undo information too...
      hopGui.undoDelegate.addUndoNew( transMeta, steps, position, false );

      int[] hopPos = new int[ hops.length ];
      for ( int i = 0; i < hops.length; i++ ) {
        hopPos[ i ] = transMeta.indexOfTransHop( hops[ i ] );
      }
      hopGui.undoDelegate.addUndoNew( transMeta, hops, hopPos, true );

      int[] notePos = new int[ notes.length ];
      for ( int i = 0; i < notes.length; i++ ) {
        notePos[ i ] = transMeta.indexOfNote( notes[ i ] );
      }
      hopGui.undoDelegate.addUndoNew( transMeta, notes, notePos, true );

    } catch ( HopException e ) {
      // "Error pasting steps...",
      // "I was unable to paste steps to this transformation"
      new ErrorDialog( hopGui.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.UnablePasteSteps.Title" ), BaseMessages
        .getString( PKG, "Spoon.Dialog.UnablePasteSteps.Message" ), e );
    }
    transGraph.redraw();
  }

  public void copySelected( TransMeta transMeta, List<StepMeta> steps, List<NotePadMeta> notes ) {
    if ( steps == null || steps.size() == 0 ) {
      return;
    }

    StringBuilder xml = new StringBuilder( 5000 ).append( XMLHandler.getXMLHeader() );
    try {
      xml.append( XMLHandler.openTag( XML_TAG_TRANSFORMATION_STEPS ) ).append( Const.CR );

      xml.append( XMLHandler.openTag( XML_TAG_STEPS ) ).append( Const.CR );
      for ( StepMeta step : steps ) {
        xml.append( step.getXML() );
      }
      xml.append( XMLHandler.closeTag( XML_TAG_STEPS ) ).append( Const.CR );

      // Also check for the hops in between the selected steps...
      xml.append( XMLHandler.openTag( TransMeta.XML_TAG_ORDER ) ).append( Const.CR );
      for ( StepMeta step1 : steps ) {
        for ( StepMeta step2 : steps ) {
          if ( step1 != step2 ) {
            TransHopMeta hop = transMeta.findTransHop( step1, step2, true );
            if ( hop != null ) {
              // Ok, we found one...
              xml.append( hop.getXML() ).append( Const.CR );
            }
          }
        }
      }
      xml.append( XMLHandler.closeTag( TransMeta.XML_TAG_ORDER ) ).append( Const.CR );

      xml.append( XMLHandler.openTag( TransMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );
      if ( notes != null ) {
        for ( NotePadMeta note : notes ) {
          xml.append( note.getXML() );
        }
      }
      xml.append( XMLHandler.closeTag( TransMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );

      xml.append( XMLHandler.openTag( TransMeta.XML_TAG_STEP_ERROR_HANDLING ) ).append( Const.CR );
      for ( StepMeta step : steps ) {
        if ( step.getStepErrorMeta() != null ) {
          xml.append( step.getStepErrorMeta().getXML() ).append( Const.CR );
        }
      }
      xml.append( XMLHandler.closeTag( TransMeta.XML_TAG_STEP_ERROR_HANDLING ) ).append( Const.CR );

      xml.append( XMLHandler.closeTag( HopUi.XML_TAG_TRANSFORMATION_STEPS ) ).append( Const.CR );

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
   * Gets transGraph
   *
   * @return value of transGraph
   */
  public HopGuiTransGraph getTransGraph() {
    return transGraph;
  }

  /**
   * @param transGraph The transGraph to set
   */
  public void setTransGraph( HopGuiTransGraph transGraph ) {
    this.transGraph = transGraph;
  }
}
