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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiLogBrowser;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.util.ArrayList;

@GuiPlugin(description = "Workflow Graph Log Delegate")
public class HopGuiWorkflowLogDelegate {
  private static final Class<?> PKG = HopGuiWorkflowGraph.class; // For Translator

  private static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiWorkflowLogDelegate-ToolBar";
  public static final String TOOLBAR_ICON_CLEAR_LOG_VIEW = "ToolbarIcon-10000-ClearLog";
  public static final String TOOLBAR_ICON_SHOW_ERROR_LINES = "ToolbarIcon-10010-ShowErrorLines";
  public static final String TOOLBAR_ICON_LOG_SETTINGS = "ToolbarIcon-10020-LogSettings";
  public static final String TOOLBAR_ICON_LOG_PAUSE_RESUME = "ToolbarIcon-10030-LogPauseResume";


  private HopGui hopGui;
  private HopGuiWorkflowGraph workflowGraph;

  private CTabItem jobLogTab;

  public Text jobLogText;

  /**
   * The number of lines in the log tab
   */
  // private int textSize;
  private Composite jobLogComposite;

  private ToolBar toolbar;
  private GuiToolbarWidgets toolBarWidgets;

  private HopGuiLogBrowser logBrowser;

  /**
   * @param hopGui
   */
  public HopGuiWorkflowLogDelegate( HopGui hopGui, HopGuiWorkflowGraph workflowGraph ) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;
  }

  public void addJobLog() {
    // First, see if we need to add the extra view...
    //
    if ( workflowGraph.extraViewComposite == null || workflowGraph.extraViewComposite.isDisposed() ) {
      workflowGraph.addExtraView();
    } else {
      if ( jobLogTab != null && !jobLogTab.isDisposed() ) {
        // just set this one active and get out...
        //
        workflowGraph.extraViewTabFolder.setSelection( jobLogTab );
        return;
      }
    }

    // Add a pipelineLogTab : display the logging...
    //
    jobLogTab = new CTabItem( workflowGraph.extraViewTabFolder, SWT.NONE );
    jobLogTab.setImage( GuiResource.getInstance().getImageShowLog() );
    jobLogTab.setText( BaseMessages.getString( PKG, "WorkflowGraph.LogTab.Name" ) );

    jobLogComposite = new Composite( workflowGraph.extraViewTabFolder, SWT.NONE );
    jobLogComposite.setLayout( new FormLayout() );

    addToolBar();

    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    toolbar.setLayoutData( fd );

    jobLogText = new Text( jobLogComposite, SWT.READ_ONLY | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL );
    hopGui.getProps().setLook( jobLogText );
    FormData fdText = new FormData();
    fdText.left = new FormAttachment( 0, 0 );
    fdText.right = new FormAttachment( 100, 0 );
    fdText.top = new FormAttachment( (Control) toolbar, 0 );
    fdText.bottom = new FormAttachment( 100, 0 );
    jobLogText.setLayoutData( fdText );

    logBrowser = new HopGuiLogBrowser( jobLogText, workflowGraph );
    logBrowser.installLogSniffer();

    // If the workflow is closed, we should dispose of all the logging information in the buffer and registry for it
    //
    workflowGraph.addDisposeListener( event -> {
      if ( workflowGraph.getWorkflow() != null ) {
        HopLogStore.discardLines( workflowGraph.getWorkflow().getLogChannelId(), true );
      }
    } );

    jobLogTab.setControl( jobLogComposite );

    workflowGraph.extraViewTabFolder.setSelection( jobLogTab );
  }

  /**
   * When a toolbar is hit it knows the class so it will come here to ask for the instance.
   *
   * @return The active instance of this class
   */
  public static HopGuiWorkflowLogDelegate getInstance() {
    IHopFileTypeHandler fileTypeHandler = HopGui.getInstance().getActiveFileTypeHandler();
    if (fileTypeHandler instanceof HopGuiWorkflowGraph) {
      HopGuiWorkflowGraph graph = (HopGuiWorkflowGraph) fileTypeHandler;
      return graph.workflowLogDelegate;
    }
    return null;
  }

  private void addToolBar() {
    toolbar = new ToolBar( jobLogComposite, SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.LEFT | SWT.HORIZONTAL );
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment( 0, 0 );
    fdToolBar.top = new FormAttachment( 0, 0 );
    fdToolBar.right = new FormAttachment( 100, 0 );
    toolbar.setLayoutData( fdToolBar );
    hopGui.getProps().setLook( toolbar, Props.WIDGET_STYLE_TOOLBAR );

    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject( this );
    toolBarWidgets.createToolbarWidgets( toolbar, GUI_PLUGIN_TOOLBAR_PARENT_ID );
    toolbar.pack();
  }

  public void clearLog() {
    if ( jobLogText != null && !jobLogText.isDisposed() ) {
      jobLogText.setText( "" );
    }
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ICON_LOG_SETTINGS,
    // label = "WorkflowLog.Button.LogSettings",
    toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.LogSettings",
    image = "ui/images/settings.svg"
  )
  public void showLogSettings() {
    // TODO: implement or rethink
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ICON_SHOW_ERROR_LINES,
    // label = "WorkflowLog.Button.ShowErrorLines",
    toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.ShowErrorLines",
    image = "ui/images/filter.svg"
  )
  public void showErrors() {
    String all = jobLogText.getText();
    ArrayList<String> err = new ArrayList<>();

    int i = 0;
    int startpos = 0;
    int crlen = Const.CR.length();

    String line = null;
    String lineUpper = null;
    while ( i < all.length() - crlen ) {
      if ( all.substring( i, i + crlen ).equalsIgnoreCase( Const.CR ) ) {
        line = all.substring( startpos, i );
        lineUpper = line.toUpperCase();
        if ( lineUpper.indexOf( BaseMessages.getString( PKG, "WorkflowLog.System.ERROR" ) ) >= 0
          || lineUpper.indexOf( BaseMessages.getString( PKG, "WorkflowLog.System.EXCEPTION" ) ) >= 0 ) {
          err.add( line );
        }
        // New start of line
        startpos = i + crlen;
      }

      i++;
    }
    line = all.substring( startpos );
    lineUpper = line.toUpperCase();
    if ( lineUpper.indexOf( BaseMessages.getString( PKG, "WorkflowLog.System.ERROR" ) ) >= 0
      || lineUpper.indexOf( BaseMessages.getString( PKG, "WorkflowLog.System.EXCEPTION" ) ) >= 0 ) {
      err.add( line );
    }

    if ( err.size() > 0 ) {
      String[] err_lines = new String[ err.size() ];
      for ( i = 0; i < err_lines.length; i++ ) {
        err_lines[ i ] = err.get( i );
      }

      EnterSelectionDialog esd = new EnterSelectionDialog( workflowGraph.getShell(), err_lines,
        BaseMessages.getString( PKG, "WorkflowLog.Dialog.ErrorLines.Title" ),
        BaseMessages.getString( PKG, "WorkflowLog.Dialog.ErrorLines.Message" ) );
      line = esd.open();
      if ( line != null ) {
        WorkflowMeta workflowMeta = workflowGraph.getManagedObject();
        for ( i = 0; i < workflowMeta.nrActions(); i++ ) {
          ActionMeta entryCopy = workflowMeta.getAction( i );
          if ( line.indexOf( entryCopy.getName() ) >= 0 ) {
            workflowGraph.editAction( workflowMeta, entryCopy );
          }
        }
      }
    }
  }

  /**
   * @return the workflow log tab
   */
  public CTabItem getJobLogTab() {
    return jobLogTab;
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ICON_LOG_PAUSE_RESUME,
    // label = "WorkflowLog.Button.Pause",
    toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.Pause",
    image = "ui/images/pause.svg",
    separator = true
  )
  public void pauseLog() {
    ToolItem item = toolBarWidgets.findToolItem( TOOLBAR_ICON_LOG_PAUSE_RESUME );
    if ( logBrowser.isPaused() ) {
      logBrowser.setPaused( false );
      item.setImage( GuiResource.getInstance().getImageRun() );
    } else {
      logBrowser.setPaused( true );
      item.setImage( GuiResource.getInstance().getImagePause() );
    }
  }

  public boolean hasSelectedText() {
    return jobLogText != null && !jobLogText.isDisposed() && StringUtils.isNotEmpty( jobLogText.getSelectionText() );
  }

  public void copySelected() {
    if ( hasSelectedText() ) {
      workflowGraph.workflowClipboardDelegate.toClipboard( jobLogText.getSelectionText() );
    }
  }
}
