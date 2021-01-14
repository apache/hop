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
import org.apache.hop.core.Result;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

@GuiPlugin(description = "Workflow Graph Grid Delegate")
public class HopGuiWorkflowGridDelegate {

  private static final Class<?> PKG = HopGuiWorkflowGridDelegate.class; // For Translator

  private HopGui hopGui;

  public static final long REFRESH_TIME = 100L;
  public static final long UPDATE_TIME_VIEW = 1000L;
  private static final String STRING_CHEF_LOG_TREE_NAME = "Workflow Log Tree";

  private HopGuiWorkflowGraph workflowGraph;
  private CTabItem workflowGridTab;
  private Tree wTree;

  public WorkflowTracker workflowTracker;
  public int previousNrItems;

  private int nrRow = 0;

  /**
   * @param hopGui
   * @param workflowGraph
   */
  public HopGuiWorkflowGridDelegate( HopGui hopGui, HopGuiWorkflowGraph workflowGraph ) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;
  }

  /**
   * Add a grid with the execution metrics per action in a table view
   */
  public void addWorkflowGrid() {

    // First, see if we need to add the extra view...
    //
    if ( workflowGraph.extraViewComposite == null || workflowGraph.extraViewComposite.isDisposed() ) {
      workflowGraph.addExtraView();
    } else {
      if ( workflowGridTab != null && !workflowGridTab.isDisposed() ) {
        // just set this one active and get out...
        //
        workflowGraph.extraViewTabFolder.setSelection( workflowGridTab );
        return;
      }
    }

    workflowGridTab = new CTabItem( workflowGraph.extraViewTabFolder, SWT.NONE );
    workflowGridTab.setImage( GuiResource.getInstance().getImageShowGrid() );
    workflowGridTab.setText( BaseMessages.getString( PKG, "HopGui.WorkflowGraph.GridTab.Name" ) );

    addControls();

    workflowGridTab.setControl( wTree );

    workflowGraph.extraViewTabFolder.setSelection( workflowGridTab );
  }

  /**
   * Add the controls to the tab
   */
  private void addControls() {

    // Create the tree table...
    wTree = new Tree( workflowGraph.extraViewTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wTree.setHeaderVisible( true );
    TreeMemory.addTreeListener( wTree, STRING_CHEF_LOG_TREE_NAME );

    TreeColumn column1 = new TreeColumn( wTree, SWT.LEFT );
    column1.setText( BaseMessages.getString( PKG, "WorkflowLog.Column.WorkflowAction" ) );
    column1.setWidth( 200 );

    TreeColumn column2 = new TreeColumn( wTree, SWT.LEFT );
    column2.setText( BaseMessages.getString( PKG, "WorkflowLog.Column.Comment" ) );
    column2.setWidth( 200 );

    TreeColumn column3 = new TreeColumn( wTree, SWT.LEFT );
    column3.setText( BaseMessages.getString( PKG, "WorkflowLog.Column.Result" ) );
    column3.setWidth( 100 );

    TreeColumn column4 = new TreeColumn( wTree, SWT.LEFT );
    column4.setText( BaseMessages.getString( PKG, "WorkflowLog.Column.Reason" ) );
    column4.setWidth( 200 );

    TreeColumn column5 = new TreeColumn( wTree, SWT.LEFT );
    column5.setText( BaseMessages.getString( PKG, "WorkflowLog.Column.Filename" ) );
    column5.setWidth( 200 );

    TreeColumn column6 = new TreeColumn( wTree, SWT.RIGHT );
    column6.setText( BaseMessages.getString( PKG, "WorkflowLog.Column.Nr" ) );
    column6.setWidth( 50 );

    TreeColumn column7 = new TreeColumn( wTree, SWT.RIGHT );
    column7.setText( BaseMessages.getString( PKG, "WorkflowLog.Column.LogDate" ) );
    column7.setWidth( 120 );

    FormData fdTree = new FormData();
    fdTree.left = new FormAttachment( 0, 0 );
    fdTree.top = new FormAttachment( 0, 0 );
    fdTree.right = new FormAttachment( 100, 0 );
    fdTree.bottom = new FormAttachment( 100, 0 );
    wTree.setLayoutData( fdTree );

    final Timer tim = new Timer( "JobGrid: " + workflowGraph.getMeta().getName() );
    TimerTask timtask = new TimerTask() {
      public void run() {
        Display display = workflowGraph.getDisplay();
        if ( display != null && !display.isDisposed() ) {
          display.asyncExec( () -> {
            // Check if the widgets are not disposed.
            // This happens is the rest of the window is not yet disposed.
            // We ARE running in a different thread after all.
            //
            // TODO: add a "auto refresh" check box somewhere
            if ( !wTree.isDisposed() ) {
              refreshTreeTable();
            }
          } );
        }
      }
    };
    tim.schedule( timtask, 10L, 2000L ); // refresh every 2 seconds...

    workflowGraph.workflowLogDelegate.getJobLogTab().addDisposeListener( disposeEvent -> tim.cancel() );

  }

  /**
   * Refresh the data in the tree-table... Use the data from the WorkflowTracker in the workflow
   */
  private void refreshTreeTable() {
    if ( workflowTracker != null ) {
      int nrItems = workflowTracker.getTotalNumberOfItems();

      if ( nrItems != previousNrItems ) {
        // Allow some flickering for now ;-)
        wTree.removeAll();

        // Re-populate this...
        TreeItem treeItem = new TreeItem( wTree, SWT.NONE );
        String workflowName = workflowTracker.getWorkflowName();

        if ( Utils.isEmpty( workflowName ) ) {
          if ( !Utils.isEmpty( workflowTracker.getWorfkflowFilename() ) ) {
            workflowName = workflowTracker.getWorfkflowFilename();
          } else {
            workflowName = BaseMessages.getString( PKG, "WorkflowLog.Tree.StringToDisplayWhenWorkflowHasNoName" );
          }
        }
        treeItem.setText( 0, workflowName );
        TreeMemory.getInstance().storeExpanded( STRING_CHEF_LOG_TREE_NAME, new String[] { workflowName }, true );

        for ( int i = 0; i < workflowTracker.nrWorkflowTrackers(); i++ ) {
          addTrackerToTree( workflowTracker.getWorkflowTracker( i ), treeItem );
        }
        previousNrItems = nrItems;

        TreeMemory.setExpandedFromMemory( wTree, STRING_CHEF_LOG_TREE_NAME );
      }
    }
  }

  private void addTrackerToTree( WorkflowTracker workflowTracker, TreeItem parentItem ) {
    try {
      if ( workflowTracker != null ) {
        TreeItem treeItem = new TreeItem( parentItem, SWT.NONE );
        if ( nrRow % 2 != 0 ) {
          treeItem.setBackground( GuiResource.getInstance().getColorBlueCustomGrid() );
        }
        nrRow++;
        if ( workflowTracker.nrWorkflowTrackers() > 0 ) {
          // This is a sub-workflow: display the name at the top of the list...
          treeItem.setText( 0, BaseMessages.getString( PKG, "WorkflowLog.Tree.WorkflowPrefix" ) + workflowTracker.getWorkflowName() );

          // then populate the sub-actions ...
          for ( int i = 0; i < workflowTracker.nrWorkflowTrackers(); i++ ) {
            addTrackerToTree( workflowTracker.getWorkflowTracker( i ), treeItem );
          }
        } else {
          ActionResult result = workflowTracker.getActionResult();
          if ( result != null ) {
            String jobEntryName = result.getActionName();
            if ( !Utils.isEmpty( jobEntryName ) ) {
              treeItem.setText( 0, jobEntryName );
              treeItem.setText( 4, Const.NVL( result.getActionFilename(), "" ) );
            } else {
              treeItem.setText( 0, BaseMessages.getString( PKG, "WorkflowLog.Tree.WorkflowPrefix2" )
                + workflowTracker.getWorkflowName() );
            }
            String comment = result.getComment();
            if ( comment != null ) {
              treeItem.setText( 1, comment );
            }
            Result res = result.getResult();
            if ( res != null ) {
              treeItem.setText( 2, res.getResult()
                ? BaseMessages.getString( PKG, "WorkflowLog.Tree.Success" ) : BaseMessages.getString(
                PKG, "WorkflowLog.Tree.Failure" ) );
              treeItem.setText( 5, Long.toString( res.getEntryNr() ) );
              if ( res.getResult() ) {
                treeItem.setForeground( GuiResource.getInstance().getColorSuccessGreen() );
              } else {
                treeItem.setForeground( GuiResource.getInstance().getColorRed() );
              }
            }
            String reason = result.getReason();
            if ( reason != null ) {
              treeItem.setText( 3, reason );
            }
            Date logDate = result.getLogDate();
            if ( logDate != null ) {
              treeItem.setText( 6, new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss" ).format( logDate ) );
            }
          }
        }
        treeItem.setExpanded( true );
      }
    } catch ( Exception e ) {
      workflowGraph.getLogChannel().logError( Const.getStackTracker( e ) );
    }
  }

  public CTabItem getWorkflowGridTab() {
    return workflowGridTab;
  }

  public void setWorkflowTracker( WorkflowTracker workflowTracker ) {
    this.workflowTracker = workflowTracker;

  }

}
