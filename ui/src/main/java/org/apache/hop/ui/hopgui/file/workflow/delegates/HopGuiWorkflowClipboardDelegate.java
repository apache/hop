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

package org.apache.hop.ui.hopgui.file.workflow.delegates;

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiWorkflowClipboardDelegate {
  private static final Class<?> PKG = HopGui.class; // For Translator
  
  public static final String XML_TAG_WORKFLOW_ACTIONS = "workflow-actions";
  public static final String XML_TAG_ACTIONS = "actions";

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

  public void pasteXml(WorkflowMeta workflowMeta, String clipboardContent, Point locaction ) {

    try {
      Document doc = XmlHandler.loadXmlString( clipboardContent );
      Node workflowNode = XmlHandler.getSubNode( doc, XML_TAG_WORKFLOW_ACTIONS );
      // De-select all, re-select pasted transforms...
      workflowMeta.unselectAll();

      Node actionsNode = XmlHandler.getSubNode( workflowNode, XML_TAG_ACTIONS );
      int nr = XmlHandler.countNodes( actionsNode, ActionMeta.XML_TAG );
      ActionMeta[] actions = new ActionMeta[ nr ];
      ArrayList<String> actionsOldNames = new ArrayList<>( nr );

      // Point min = new Point(locaction.x, locaction.y);
      Point min = new Point( 99999999, 99999999 );

      // Load the entries...
      for ( int i = 0; i < nr; i++ ) {
        Node actionNode = XmlHandler.getSubNodeByNr( actionsNode, ActionMeta.XML_TAG, i );
        actions[i] =
            new ActionMeta(actionNode, hopGui.getMetadataProvider(), workflowGraph.getVariables() );

        if ( locaction != null ) {
          Point p = actions[ i ].getLocation();

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
        hops[ i ] = new WorkflowHopMeta( hopNode, Arrays.asList( actions ) );
      }

      // This is the offset:
      Point offset = new Point( locaction.x - min.x, locaction.y - min.y );

      // Undo/redo object positions...
      int[] position = new int[ actions.length ];

      for ( int i = 0; i < actions.length; i++ ) {
        Point p = actions[ i ].getLocation();
        String name = actions[ i ].getName();
        actions[ i ].setLocation( p.x + offset.x, p.y + offset.y );

        // Check the name, find alternative...
        actionsOldNames.add( name );
        actions[ i ].setName( workflowMeta.getAlternativeActionName( name ) );
        workflowMeta.addAction( actions[ i ] );
        position[ i ] = workflowMeta.indexOfAction( actions[ i ] );
        actions[ i ].setSelected( true );
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
      hopGui.undoDelegate.addUndoNew( workflowMeta, actions, position, false );

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
      // See if this was different (non-XML) content
      //
      pasteNoXmlContent(workflowMeta, clipboardContent, locaction);
    }

    workflowGraph.redraw();
  }

  private void pasteNoXmlContent( WorkflowMeta workflowMeta, String clipboardContent, Point location ) {
    try {
      // Are we pasting filenames?
      //
      if (clipboardContent.startsWith( "file:///" ) || clipboardContent.startsWith( "/" )) {
        String[] filenames = clipboardContent.split( Const.CR );

        for (String filename : filenames) {

          String cleanFilename = HopVfs.getFilename(HopVfs.getFileObject(filename));

          // See if the filename needs to be treated somehow...
          // We don't have a file dialog so we pass in null.
          //
          HopGuiFileOpenedExtension ext =
              new HopGuiFileOpenedExtension(null, hopGui.getVariables(), cleanFilename);
          ExtensionPointHandler.callExtensionPoint(
              LogChannel.UI,
              workflowGraph.getVariables(),
              HopGuiExtensionPoint.HopGuiFileOpenedDialog.id,
              ext);
          if (ext.filename != null) {
            cleanFilename = ext.filename;
          }
          File file = new File(cleanFilename);

          // Pipeline?
          //
          HopPipelineFileType pipelineFileType = new HopPipelineFileType();
          if (filename.endsWith(pipelineFileType.getDefaultFileExtension())) {

            // Add a new Pipeline action...
            //
            String name = getUniqueName(workflowMeta, file.getName());

            ActionPipeline actionPipeline = new ActionPipeline(name);
            actionPipeline.setFileName(cleanFilename);

            // Pick the first run configuration available...
            //
            List<String> names = hopGui.getMetadataProvider().getSerializer( PipelineRunConfiguration.class ).listObjectNames();
            if (!names.isEmpty()) {
              actionPipeline.setRunConfiguration( names.get(0) );
            }

            ActionMeta actionMeta = new ActionMeta(actionPipeline);
            actionMeta.setLocation(new Point(location));
            workflowMeta.addAction(actionMeta);

            hopGui.undoDelegate.addUndoNew(
              workflowMeta,
              new ActionMeta[] {actionMeta},
              new int[] {workflowMeta.indexOfAction(actionMeta)}
              );

            // Shift the location for the next action
            //
            shiftLocation( location );
          }

          // Workflow?
          //
          HopWorkflowFileType workflowFileType = new HopWorkflowFileType<>();
          if (filename.endsWith(workflowFileType.getDefaultFileExtension())) {

            // Add a new Workflow action...
            //
            String name = getUniqueName(workflowMeta, file.getName());

            ActionWorkflow actionWorkflow = new ActionWorkflow(name);
            actionWorkflow.setFileName(cleanFilename);

            // Pick the first run configuration available...
            //
            List<String> names = hopGui.getMetadataProvider().getSerializer( WorkflowRunConfiguration.class ).listObjectNames();
            if (!names.isEmpty()) {
              actionWorkflow.setRunConfiguration( names.get(0) );
            }

            ActionMeta actionMeta = new ActionMeta(actionWorkflow);
            actionMeta.setLocation(new Point(location) );

            workflowMeta.addAction(actionMeta);

            hopGui.undoDelegate.addUndoNew(
              workflowMeta,
              new ActionMeta[] {actionMeta},
              new int[] {workflowMeta.indexOfAction(actionMeta)}
            );

            // Shift the location for the next action
            //
            shiftLocation( location );
          }
        }
      } else {
        // Add a notepad
        //
        NotePadMeta notePadMeta = new NotePadMeta(clipboardContent, location.x, location.y, ConstUi.NOTE_MIN_SIZE, ConstUi.NOTE_MIN_SIZE);
        workflowMeta.addNote( notePadMeta );
        hopGui.undoDelegate.addUndoNew(
          workflowMeta,
          new NotePadMeta[] {notePadMeta},
          new int[] {workflowMeta.indexOfNote(notePadMeta)}
        );
        shiftLocation( location );
      }

    } catch(Exception e) {
      // "Error pasting transforms...",
      // "I was unable to paste transforms to this pipeline"
      new ErrorDialog( hopGui.getShell(),
        BaseMessages.getString( PKG, "HopGui.Dialog.UnablePasteEntries.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.UnablePasteEntries.Message" ), e );
    }
  }

  private void shiftLocation( Point location ) {
    location.x = location.x+(int)(10* PropsUi.getInstance().getZoomFactor());
    location.y = location.y+(int)( 5* PropsUi.getInstance().getZoomFactor());
  }

  private String getUniqueName( WorkflowMeta meta, String name ) {
    String uniqueName = name;
    int nr = 2;
    while (meta.findAction( uniqueName )!=null) {
      uniqueName = name + " "+nr;
      nr++;
    }
    return uniqueName;
  }

  private String getUniqueName( PipelineMeta meta, String name ) {
    String uniqueName = name;
    int nr = 2;
    while (meta.findTransform( uniqueName )!=null) {
      uniqueName = name + " "+nr;
      nr++;
    }
    return uniqueName;
  }


  public void copySelected( WorkflowMeta workflowMeta, List<ActionMeta> actions, List<NotePadMeta> notes ) {
    if ( actions == null || actions.size() == 0 ) {
      return;
    }

    StringBuilder xml = new StringBuilder( 5000 ).append( XmlHandler.getXmlHeader() );
    try {
      xml.append( XmlHandler.openTag( XML_TAG_WORKFLOW_ACTIONS ) ).append( Const.CR );

      xml.append( XmlHandler.openTag( XML_TAG_ACTIONS ) ).append( Const.CR );
      for ( ActionMeta action : actions ) {
        xml.append( action.getXml() );
      }
      xml.append( XmlHandler.closeTag( XML_TAG_ACTIONS ) ).append( Const.CR );

      // Also check for the hops in between the selected transforms...
      xml.append( XmlHandler.openTag( PipelineMeta.XML_TAG_ORDER ) ).append( Const.CR );
      for ( ActionMeta transform1 : actions ) {
        for ( ActionMeta transform2 : actions ) {
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
