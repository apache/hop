package org.apache.hop.ui.hopgui.file.workflow.delegates;

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiWorkflowClipboardDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!
  
  public static final String XML_TAG_WORKFLOW_ACTIONS = "workflow-actions";
  private static final String XML_TAG_ACTIONS = "actions";

  private HopGui hopGui;
  private HopGuiWorkflowGraph jobGraph;
  private ILogChannel log;

  public HopGuiWorkflowClipboardDelegate( HopGui hopGui, HopGuiWorkflowGraph jobGraph ) {
    this.hopGui = hopGui;
    this.jobGraph = jobGraph;
    this.log = hopGui.getLog();
  }

  public void toClipboard( String clipText ) {
    try {
      GUIResource.getInstance().toClipboard( clipText );
    } catch ( Throwable e ) {
      new ErrorDialog( hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Message" ), e );
    }
  }

  public String fromClipboard() {
    try {
      return GUIResource.getInstance().fromClipboard();
    } catch ( Throwable e ) {
      new ErrorDialog(
        hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Message" ), e );
      return null;
    }
  }

  public void pasteXML( WorkflowMeta workflowMeta, String clipcontent, Point loc ) {

    try {
      Document doc = XMLHandler.loadXMLString( clipcontent );
      Node workflowNode = XMLHandler.getSubNode( doc, XML_TAG_WORKFLOW_ACTIONS );
      // De-select all, re-select pasted transforms...
      workflowMeta.unselectAll();

      Node entriesNode = XMLHandler.getSubNode( workflowNode, XML_TAG_ACTIONS );
      int nr = XMLHandler.countNodes( entriesNode, ActionCopy.XML_TAG );
      ActionCopy[] entries = new ActionCopy[ nr ];
      ArrayList<String> entriesOldNames = new ArrayList<>( nr );

      // Point min = new Point(loc.x, loc.y);
      Point min = new Point( 99999999, 99999999 );

      // Load the entries...
      for ( int i = 0; i < nr; i++ ) {
        Node entryNode = XMLHandler.getSubNodeByNr( entriesNode, ActionCopy.XML_TAG, i );
        entries[ i ] = new ActionCopy( entryNode, hopGui.getMetaStore() );

        if ( loc != null ) {
          Point p = entries[ i ].getLocation();

          if ( min.x > p.x ) {
            min.x = p.x;
          }
          if ( min.y > p.y ) {
            min.y = p.y;
          }
        }
      }

      // Load the hops...
      Node hopsNode = XMLHandler.getSubNode( workflowNode, "order" );
      nr = XMLHandler.countNodes( hopsNode, "hop" );
      WorkflowHopMeta[] hops = new WorkflowHopMeta[ nr ];

      for ( int i = 0; i < nr; i++ ) {
        Node hopNode = XMLHandler.getSubNodeByNr( hopsNode, "hop", i );
        hops[ i ] = new WorkflowHopMeta( hopNode, Arrays.asList( entries ) );
      }

      // This is the offset:
      Point offset = new Point( loc.x - min.x, loc.y - min.y );

      // Undo/redo object positions...
      int[] position = new int[ entries.length ];

      for ( int i = 0; i < entries.length; i++ ) {
        Point p = entries[ i ].getLocation();
        String name = entries[ i ].getName();
        entries[ i ].setLocation( p.x + offset.x, p.y + offset.y );

        // Check the name, find alternative...
        entriesOldNames.add( name );
        entries[ i ].setName( workflowMeta.getAlternativeJobentryName( name ) );
        workflowMeta.addAction( entries[ i ] );
        position[ i ] = workflowMeta.indexOfAction( entries[ i ] );
        entries[ i ].setSelected( true );
      }

      // Add the hops too...
      //
      for ( WorkflowHopMeta hop : hops ) {
        workflowMeta.addWorkflowHop( hop );
      }

      // Load the notes...
      Node notesNode = XMLHandler.getSubNode( workflowNode, "notepads" );
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
        workflowMeta.addNote( notes[ i ] );
        notes[ i ].setSelected( true );
      }

      // Save undo information too...
      hopGui.undoDelegate.addUndoNew( workflowMeta, entries, position, false );

      int[] hopPos = new int[ hops.length ];
      for ( int i = 0; i < hops.length; i++ ) {
        hopPos[ i ] = workflowMeta.indexOfWorkflowHop( hops[ i ] );
      }
      hopGui.undoDelegate.addUndoNew( workflowMeta, hops, hopPos, true );

      int[] notePos = new int[ notes.length ];
      for ( int i = 0; i < notes.length; i++ ) {
        notePos[ i ] = workflowMeta.indexOfNote( notes[ i ] );
      }
      hopGui.undoDelegate.addUndoNew( workflowMeta, notes, notePos, true );

    } catch ( HopException e ) {
      // "Error pasting transforms...",
      // "I was unable to paste transforms to this pipeline"
      new ErrorDialog( hopGui.getShell(),
        BaseMessages.getString( PKG, "HopGui.Dialog.UnablePasteEntries.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.UnablePasteEntries.Message" ), e );
    }
    jobGraph.redraw();
  }

  public void copySelected( WorkflowMeta workflowMeta, List<ActionCopy> actions, List<NotePadMeta> notes ) {
    if ( actions == null || actions.size() == 0 ) {
      return;
    }

    StringBuilder xml = new StringBuilder( 5000 ).append( XMLHandler.getXMLHeader() );
    try {
      xml.append( XMLHandler.openTag( XML_TAG_WORKFLOW_ACTIONS ) ).append( Const.CR );

      xml.append( XMLHandler.openTag( XML_TAG_ACTIONS ) ).append( Const.CR );
      for ( ActionCopy action : actions ) {
        xml.append( action.getXml() );
      }
      xml.append( XMLHandler.closeTag( XML_TAG_ACTIONS ) ).append( Const.CR );

      // Also check for the hops in between the selected transforms...
      xml.append( XMLHandler.openTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );
      for ( ActionCopy transform1 : actions ) {
        for ( ActionCopy transform2 : actions ) {
          if ( !transform1.equals( transform2 ) ) {
            WorkflowHopMeta hop = workflowMeta.findWorkflowHop( transform1, transform2, true );
            if ( hop != null ) {
              // Ok, we found one...
              xml.append( hop.getXml() ).append( Const.CR );
            }
          }
        }
      }
      xml.append( XMLHandler.closeTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );

      xml.append( XMLHandler.openTag( PipelineMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );
      if ( notes != null ) {
        for ( NotePadMeta note : notes ) {
          xml.append( note.getXml() );
        }
      }
      xml.append( XMLHandler.closeTag( PipelineMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );

      xml.append( XMLHandler.closeTag( XML_TAG_WORKFLOW_ACTIONS ) ).append( Const.CR );

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
   * Gets jobGraph
   *
   * @return value of jobGraph
   */
  public HopGuiWorkflowGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The jobGraph to set
   */
  public void setJobGraph( HopGuiWorkflowGraph jobGraph ) {
    this.jobGraph = jobGraph;
  }

  /**
   * Gets log
   *
   * @return value of log
   */
  public ILogChannel getLog() {
    return log;
  }

  /**
   * @param log The log to set
   */
  public void setLog( ILogChannel log ) {
    this.log = log;
  }
}
