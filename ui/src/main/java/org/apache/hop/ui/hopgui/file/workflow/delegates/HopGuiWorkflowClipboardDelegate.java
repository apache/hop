/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopgui.file.workflow.delegates;

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
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
  private HopGuiWorkflowGraph workflowGraph;
  private ILogChannel log;

  public HopGuiWorkflowClipboardDelegate( HopGui hopGui, HopGuiWorkflowGraph workflowGraph ) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;
    this.log = hopGui.getLog();
  }

  public void toClipboard( String clipText ) {
    try {
      GuiResource.getInstance().toClipboard( clipText );
    } catch ( Throwable e ) {
      new ErrorDialog( hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Message" ), e );
    }
  }

  public String fromClipboard() {
    try {
      return GuiResource.getInstance().fromClipboard();
    } catch ( Throwable e ) {
      new ErrorDialog(
        hopGui.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Message" ), e );
      return null;
    }
  }

  public void pasteXML( WorkflowMeta workflowMeta, String clipcontent, Point loc ) {

    try {
      Document doc = XmlHandler.loadXmlString( clipcontent );
      Node workflowNode = XmlHandler.getSubNode( doc, XML_TAG_WORKFLOW_ACTIONS );
      // De-select all, re-select pasted transforms...
      workflowMeta.unselectAll();

      Node entriesNode = XmlHandler.getSubNode( workflowNode, XML_TAG_ACTIONS );
      int nr = XmlHandler.countNodes( entriesNode, ActionCopy.XML_TAG );
      ActionCopy[] entries = new ActionCopy[ nr ];
      ArrayList<String> entriesOldNames = new ArrayList<>( nr );

      // Point min = new Point(loc.x, loc.y);
      Point min = new Point( 99999999, 99999999 );

      // Load the entries...
      for ( int i = 0; i < nr; i++ ) {
        Node entryNode = XmlHandler.getSubNodeByNr( entriesNode, ActionCopy.XML_TAG, i );
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
      Node hopsNode = XmlHandler.getSubNode( workflowNode, "order" );
      nr = XmlHandler.countNodes( hopsNode, "hop" );
      WorkflowHopMeta[] hops = new WorkflowHopMeta[ nr ];

      for ( int i = 0; i < nr; i++ ) {
        Node hopNode = XmlHandler.getSubNodeByNr( hopsNode, "hop", i );
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
      Node notesNode = XmlHandler.getSubNode( workflowNode, "notepads" );
      nr = XmlHandler.countNodes( notesNode, "notepad" );
      if ( log.isDebug() ) {
        // "I found "+nr+" notepads to paste."
        log.logDebug( BaseMessages.getString( PKG, "HopGui.Log.FoundNotepads", "" + nr ) );
      }
      NotePadMeta[] notes = new NotePadMeta[ nr ];

      for ( int i = 0; i < notes.length; i++ ) {
        Node noteNode = XmlHandler.getSubNodeByNr( notesNode, "notepad", i );
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
    workflowGraph.redraw();
  }

  public void copySelected( WorkflowMeta workflowMeta, List<ActionCopy> actions, List<NotePadMeta> notes ) {
    if ( actions == null || actions.size() == 0 ) {
      return;
    }

    StringBuilder xml = new StringBuilder( 5000 ).append( XmlHandler.getXMLHeader() );
    try {
      xml.append( XmlHandler.openTag( XML_TAG_WORKFLOW_ACTIONS ) ).append( Const.CR );

      xml.append( XmlHandler.openTag( XML_TAG_ACTIONS ) ).append( Const.CR );
      for ( ActionCopy action : actions ) {
        xml.append( action.getXml() );
      }
      xml.append( XmlHandler.closeTag( XML_TAG_ACTIONS ) ).append( Const.CR );

      // Also check for the hops in between the selected transforms...
      xml.append( XmlHandler.openTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );
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
      xml.append( XmlHandler.closeTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );

      xml.append( XmlHandler.openTag( PipelineMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );
      if ( notes != null ) {
        for ( NotePadMeta note : notes ) {
          xml.append( note.getXml() );
        }
      }
      xml.append( XmlHandler.closeTag( PipelineMeta.XML_TAG_NOTEPADS ) ).append( Const.CR );

      xml.append( XmlHandler.closeTag( XML_TAG_WORKFLOW_ACTIONS ) ).append( Const.CR );

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
   * Gets workflowGraph
   *
   * @return value of workflowGraph
   */
  public HopGuiWorkflowGraph getJobGraph() {
    return workflowGraph;
  }

  /**
   * @param workflowGraph The workflowGraph to set
   */
  public void setJobGraph( HopGuiWorkflowGraph workflowGraph ) {
    this.workflowGraph = workflowGraph;
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
