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

import java.util.ArrayList;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextVar;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiLogBrowser;
import org.apache.hop.ui.hopgui.file.shared.TextZoom;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

@GuiPlugin(description = "Workflow Graph Log Delegate")
public class HopGuiWorkflowLogDelegate {
  private static final Class<?> PKG = HopGuiWorkflowGraph.class;

  private static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiWorkflowLogDelegate-ToolBar";
  public static final String TOOLBAR_ICON_CLEAR_LOG_VIEW = "ToolbarIcon-10000-ClearLog";
  public static final String TOOLBAR_ICON_SHOW_ERROR_LINES = "ToolbarIcon-10010-ShowErrorLines";
  public static final String TOOLBAR_ICON_LOG_COPY_TO_CLIPBOARD =
      "ToolbarIcon-10020-LogCopyToClipboard";
  public static final String TOOLBAR_ICON_LOG_PAUSE_RESUME = "ToolbarIcon-10030-LogPauseResume";
  public static final String TOOLBAR_ICON_LOG_INCREASE_FONT = "ToolbarIcon-10040-LogIncreaseFont";
  public static final String TOOLBAR_ICON_LOG_DECREASE_FONT = "ToolbarIcon-10050-LogDecreaseFont";
  public static final String TOOLBAR_ICON_LOG_RESET_FONT = "ToolbarIcon-10060-LogResetFont";

  private HopGui hopGui;
  private HopGuiWorkflowGraph workflowGraph;

  private CTabItem workflowLogTab;

  private TextComposite workflowLogText;
  private TextZoom textZoom;

  /** The number of lines in the log tab */
  private Composite workflowLogComposite;

  private ToolBar toolbar;
  private GuiToolbarWidgets toolBarWidgets;

  private HopGuiLogBrowser logBrowser;

  /**
   * @param hopGui
   */
  public HopGuiWorkflowLogDelegate(HopGui hopGui, HopGuiWorkflowGraph workflowGraph) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;
  }

  public void addWorkflowLog() {
    // First, see if we need to add the extra view...
    //
    if (workflowGraph.extraViewTabFolder == null || workflowGraph.extraViewTabFolder.isDisposed()) {
      workflowGraph.addExtraView();
    } else {
      if (workflowLogTab != null && !workflowLogTab.isDisposed()) {
        return;
      }
    }

    // Add a pipelineLogTab : display the logging...
    //
    workflowLogTab = new CTabItem(workflowGraph.extraViewTabFolder, SWT.NONE);
    workflowLogTab.setFont(GuiResource.getInstance().getFontDefault());
    workflowLogTab.setImage(GuiResource.getInstance().getImageShowLog());
    workflowLogTab.setText(BaseMessages.getString(PKG, "WorkflowGraph.LogTab.Name"));

    workflowLogComposite = new Composite(workflowGraph.extraViewTabFolder, SWT.NONE);
    workflowLogComposite.setLayout(new FormLayout());

    addToolBar();

    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0); // First one in the left top corner
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    toolbar.setLayoutData(fd);

    // Use StyledTextComp for web (uses Text widget), StyledTextVar for desktop (uses StyledText
    // for highlighting)
    if (EnvironmentUtils.getInstance().isWeb()) {
      workflowLogText =
          new StyledTextComp(
              workflowGraph.getVariables(),
              workflowLogComposite,
              SWT.READ_ONLY | SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    } else {
      workflowLogText =
          new StyledTextVar(
              workflowGraph.getVariables(),
              workflowLogComposite,
              SWT.READ_ONLY | SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
              false,
              false);
      // Error highlighting is applied directly in HopGuiLogBrowser when adding lines
    }
    PropsUi.setLook(workflowLogText);
    FormData fdText = new FormData();
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.top = new FormAttachment(toolbar, 0);
    fdText.bottom = new FormAttachment(100, 0);
    workflowLogText.setLayoutData(fdText);

    this.textZoom = new TextZoom(workflowLogText, GuiResource.getInstance().getFontFixed());
    this.textZoom.resetFont();

    // add a CR to avoid fontStyle from getting lost on macos HOP-2583
    if (OsHelper.isMac()) {
      workflowLogText.setText(Const.CR);
    }

    logBrowser = new HopGuiLogBrowser(workflowLogText, workflowGraph);
    logBrowser.installLogSniffer();

    // If the workflow is closed, we should dispose of all the logging information in the buffer and
    // registry for it
    //
    workflowGraph.addDisposeListener(
        event -> {
          if (workflowGraph.getWorkflow() != null) {
            HopLogStore.discardLines(workflowGraph.getWorkflow().getLogChannelId(), true);
          }
        });

    workflowLogTab.setControl(workflowLogComposite);
  }

  /**
   * When a toolbar is hit it knows the class so it will come here to ask for the instance.
   *
   * @return The active instance of this class
   */
  public static HopGuiWorkflowLogDelegate getInstance() {
    IHopFileTypeHandler fileTypeHandler = HopGui.getInstance().getActiveFileTypeHandler();
    if (fileTypeHandler instanceof HopGuiWorkflowGraph hopGuiWorkflowGraph) {
      HopGuiWorkflowGraph graph = hopGuiWorkflowGraph;
      return graph.workflowLogDelegate;
    }
    return null;
  }

  private void addToolBar() {
    toolbar = new ToolBar(workflowLogComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment(0, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    fdToolBar.right = new FormAttachment(100, 0);
    toolbar.setLayoutData(fdToolBar);
    PropsUi.setLook(toolbar, Props.WIDGET_STYLE_TOOLBAR);

    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolbar, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    toolbar.pack();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_CLEAR_LOG_VIEW,
      toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.ClearLog",
      image = "ui/images/delete.svg")
  public void clearLog() {
    if (workflowLogText != null && !workflowLogText.isDisposed()) {
      // add a CR to avoid fontStyle from getting lost on macos HOP-2583
      String textToSet = OsHelper.isMac() ? Const.CR : "";
      workflowLogText.setText(textToSet);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_COPY_TO_CLIPBOARD,
      toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.LogCopyToClipboard",
      image = "ui/images/copy.svg")
  public void copyToClipboard() {
    String text = workflowLogText.getText();
    GuiResource.getInstance().toClipboard(text);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_SHOW_ERROR_LINES,
      // label = "WorkflowLog.Button.ShowErrorLines",
      toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.ShowErrorLines",
      image = "ui/images/filter.svg")
  public void showErrors() {
    String all = workflowLogText.getText();
    ArrayList<String> err = new ArrayList<>();

    int i = 0;
    int startpos = 0;
    int crlen = Const.CR.length();

    String line = null;
    String lineUpper = null;
    while (i < all.length() - crlen) {
      if (all.substring(i, i + crlen).equalsIgnoreCase(Const.CR)) {
        line = all.substring(startpos, i);
        lineUpper = line.toUpperCase();
        if (lineUpper.indexOf(BaseMessages.getString(PKG, "WorkflowLog.System.ERROR")) >= 0
            || lineUpper.indexOf(BaseMessages.getString(PKG, "WorkflowLog.System.EXCEPTION"))
                >= 0) {
          err.add(line);
        }
        // New start of line
        startpos = i + crlen;
      }

      i++;
    }
    line = all.substring(startpos);
    lineUpper = line.toUpperCase();
    if (lineUpper.indexOf(BaseMessages.getString(PKG, "WorkflowLog.System.ERROR")) >= 0
        || lineUpper.indexOf(BaseMessages.getString(PKG, "WorkflowLog.System.EXCEPTION")) >= 0) {
      err.add(line);
    }

    if (!err.isEmpty()) {
      String[] errLines = new String[err.size()];
      for (i = 0; i < errLines.length; i++) {
        errLines[i] = err.get(i);
      }

      EnterSelectionDialog esd =
          new EnterSelectionDialog(
              workflowGraph.getShell(),
              errLines,
              BaseMessages.getString(PKG, "WorkflowLog.Dialog.ErrorLines.Title"),
              BaseMessages.getString(PKG, "WorkflowLog.Dialog.ErrorLines.Message"));
      line = esd.open();
      if (line != null) {
        WorkflowMeta workflowMeta = workflowGraph.getManagedObject();
        for (i = 0; i < workflowMeta.nrActions(); i++) {
          ActionMeta entryCopy = workflowMeta.getAction(i);
          if (line.indexOf(entryCopy.getName()) >= 0) {
            workflowGraph.editAction(workflowMeta, entryCopy);
          }
        }
      }
    }
  }

  /**
   * @return the workflow log tab
   */
  public CTabItem getWorkflowLogTab() {
    return workflowLogTab;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_PAUSE_RESUME,
      // label = "WorkflowLog.Button.Pause",
      toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.Pause",
      image = "ui/images/pause.svg",
      separator = true)
  public void pauseLog() {
    ToolItem item = toolBarWidgets.findToolItem(TOOLBAR_ICON_LOG_PAUSE_RESUME);
    if (logBrowser.isPaused()) {
      logBrowser.setPaused(false);
      item.setImage(GuiResource.getInstance().getImageRun());
    } else {
      logBrowser.setPaused(true);
      item.setImage(GuiResource.getInstance().getImagePause());
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_INCREASE_FONT,
      toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.IncreaseFont",
      image = "ui/images/zoom-in.svg",
      separator = true)
  public void increaseFont() {
    this.textZoom.increaseFont();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_DECREASE_FONT,
      toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.DecreaseFont",
      image = "ui/images/zoom-out.svg",
      separator = false)
  public void decreaseFont() {
    this.textZoom.decreaseFont();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_RESET_FONT,
      toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.ResetFont",
      image = "ui/images/zoom-100.svg",
      separator = false)
  public void resetFont() {
    this.textZoom.resetFont();
  }

  public boolean hasSelectedText() {
    if (workflowLogText == null || workflowLogText.isDisposed()) {
      return false;
    }
    return StringUtils.isNotEmpty(workflowLogText.getSelectionText());
  }

  public void copySelected() {
    if (hasSelectedText()) {
      workflowGraph.workflowClipboardDelegate.toClipboard(workflowLogText.getSelectionText());
    }
  }
}
