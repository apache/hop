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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import java.util.ArrayList;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiLogBrowser;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.shared.TextZoom;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

@GuiPlugin(description = "Pipeline Graph Log Delegate")
public class HopGuiPipelineLogDelegate {
  private static final Class<?> PKG = HopGui.class;

  private static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiPipelineLogDelegate-ToolBar";
  public static final String TOOLBAR_ICON_CLEAR_LOG_VIEW = "ToolbarIcon-10000-ClearLog";
  public static final String TOOLBAR_ICON_SHOW_ERROR_LINES = "ToolbarIcon-10010-ShowErrorLines";
  public static final String TOOLBAR_ICON_LOG_COPY_TO_CLIPBOARD =
      "ToolbarIcon-10020-LogCopyToClipboard";
  public static final String TOOLBAR_ICON_LOG_PAUSE_RESUME = "ToolbarIcon-10030-LogPauseResume";
  public static final String TOOLBAR_ICON_LOG_INCREASE_FONT = "ToolbarIcon-10040-LogIncreaseFont";
  public static final String TOOLBAR_ICON_LOG_DECREASE_FONT = "ToolbarIcon-10050-LogDecreaseFont";
  public static final String TOOLBAR_ICON_LOG_RESET_FONT = "ToolbarIcon-10060-LogResetFont";

  private final HopGuiPipelineGraph pipelineGraph;

  private HopGui hopGui;

  @Getter private CTabItem pipelineLogTab;

  private Text pipelineLogText;
  private TextZoom textZoom;

  private ToolBar toolbar;
  private GuiToolbarWidgets toolBarWidgets;

  private Composite pipelineLogComposite;

  @Getter private HopGuiLogBrowser logBrowser;

  public HopGuiPipelineLogDelegate(HopGui hopGui, HopGuiPipelineGraph pipelineGraph) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
  }

  public void addPipelineLog() {
    // First, see if we need to add the extra view...
    //
    if (pipelineGraph.extraViewTabFolder == null || pipelineGraph.extraViewTabFolder.isDisposed()) {
      pipelineGraph.addExtraView();
    } else {
      if (pipelineLogTab != null && !pipelineLogTab.isDisposed()) {
        return;
      }
    }

    // Add a pipelineLogTab : display the logging...
    //
    pipelineLogTab = new CTabItem(pipelineGraph.extraViewTabFolder, SWT.NONE);
    pipelineLogTab.setFont(GuiResource.getInstance().getFontDefault());
    pipelineLogTab.setImage(GuiResource.getInstance().getImageShowLog());
    pipelineLogTab.setText(BaseMessages.getString(PKG, "HopGui.PipelineGraph.LogTab.Name"));

    pipelineLogComposite = new Composite(pipelineGraph.extraViewTabFolder, SWT.NONE);
    pipelineLogComposite.setLayout(new FormLayout());

    addToolBar();

    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0); // First one in the left top corner
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    toolbar.setLayoutData(fd);

    pipelineLogText =
        new Text(
            pipelineLogComposite,
            SWT.READ_ONLY | SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(pipelineLogText);
    FormData fdText = new FormData();
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.top = new FormAttachment(toolbar, 0);
    fdText.bottom = new FormAttachment(100, 0);
    pipelineLogText.setLayoutData(fdText);

    this.textZoom = new TextZoom(pipelineLogText, GuiResource.getInstance().getFontFixed());
    this.textZoom.resetFont();

    // add a CR to avoid fontStyle from getting lost on macos HOP-2583
    if (OsHelper.isMac()) {
      pipelineLogText.setText(Const.CR);
    }

    logBrowser = new HopGuiLogBrowser(pipelineLogText, pipelineGraph);
    logBrowser.installLogSniffer();

    // If the pipeline is closed, we should dispose of all the logging information in the buffer and
    // registry for
    // this pipeline
    //
    pipelineGraph.addDisposeListener(
        event -> {
          if (pipelineGraph.pipeline != null) {
            HopLogStore.discardLines(pipelineGraph.pipeline.getLogChannelId(), true);
          }
        });

    pipelineLogTab.setControl(pipelineLogComposite);
  }

  /**
   * When a toolbar is hit, it knows the class so it will come here to ask for the instance.
   *
   * @return The active instance of this class
   */
  public static HopGuiPipelineLogDelegate getInstance() {
    IHopFileTypeHandler fileTypeHandler = HopGui.getInstance().getActiveFileTypeHandler();
    if (fileTypeHandler instanceof HopGuiPipelineGraph hopGuiPipelineGraph) {
      HopGuiPipelineGraph graph = hopGuiPipelineGraph;
      return graph.pipelineLogDelegate;
    }
    return null;
  }

  private void addToolBar() {
    toolbar = new ToolBar(pipelineLogComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
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

  public void showLogView() {
    if (pipelineLogTab == null || pipelineLogTab.isDisposed()) {
      addPipelineLog();
    } else {
      pipelineLogTab.dispose();

      pipelineGraph.checkEmptyExtraView();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_CLEAR_LOG_VIEW,
      toolTip = "i18n:org.apache.hop.ui.hopgui:PipelineLog.Button.ClearLog",
      image = "ui/images/delete.svg")
  public void clearLog() {
    if (pipelineLogText != null && !pipelineLogText.isDisposed()) {
      // add a CR to avoid fontStyle from getting lost on macos HOP-2583
      if (OsHelper.isMac()) {
        pipelineLogText.setText(Const.CR);
      } else {
        pipelineLogText.setText("");
      }
    }
    Map<String, String> transformLogMap = pipelineGraph.getTransformLogMap();
    if (transformLogMap != null) {
      transformLogMap.clear();
      pipelineGraph.getDisplay().asyncExec(pipelineGraph::redraw);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_COPY_TO_CLIPBOARD,
      toolTip = "i18n:org.apache.hop.ui.hopgui:PipelineLog.Button.LogCopyToClipboard",
      image = "ui/images/copy.svg")
  public void copyToClipboard() {
    GuiResource.getInstance().toClipboard(pipelineLogText.getText());
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_SHOW_ERROR_LINES,
      // label = "PipelineLog.Button.ShowErrorLines",
      toolTip = "i18n:org.apache.hop.ui.hopgui:PipelineLog.Button.ShowErrorLines",
      image = "ui/images/filter.svg")
  public void showErrors() {
    String all = pipelineLogText.getText();
    ArrayList<String> err = new ArrayList<>();

    int i = 0;
    int startpos = 0;
    int crlen = Const.CR.length();

    while (i < all.length() - crlen) {
      if (all.substring(i, i + crlen).equalsIgnoreCase(Const.CR)) {
        String line = all.substring(startpos, i);
        String uLine = line.toUpperCase();
        if (uLine.contains(BaseMessages.getString(PKG, "PipelineLog.System.ERROR"))
            || uLine.contains(BaseMessages.getString(PKG, "PipelineLog.System.EXCEPTION"))
            || uLine.contains("ERROR")
            || // i18n for compatibilty to non translated transforms a.s.o.
            uLine.contains("EXCEPTION") // i18n for compatibilty to non translated transforms a.s.o.
        ) {
          err.add(line);
        }
        // New start of line
        startpos = i + crlen;
      }

      i++;
    }
    String line = all.substring(startpos);
    String uLine = line.toUpperCase();
    if (uLine.contains(BaseMessages.getString(PKG, "PipelineLog.System.ERROR2"))
        || uLine.contains(BaseMessages.getString(PKG, "PipelineLog.System.EXCEPTION2"))
        || uLine.contains("ERROR")
        || // i18n for compatibilty to non translated transforms a.s.o.
        uLine.contains("EXCEPTION") // i18n for compatibilty to non translated transforms a.s.o.
    ) {
      err.add(line);
    }

    if (!err.isEmpty()) {
      String[] errLines = new String[err.size()];
      for (i = 0; i < errLines.length; i++) {
        errLines[i] = err.get(i);
      }

      EnterSelectionDialog esd =
          new EnterSelectionDialog(
              pipelineGraph.getShell(),
              errLines,
              BaseMessages.getString(PKG, "PipelineLog.Dialog.ErrorLines.Title"),
              BaseMessages.getString(PKG, "PipelineLog.Dialog.ErrorLines.Message"));
      line = esd.open();
      if (line != null) {
        PipelineMeta pipelineMeta = pipelineGraph.getManagedObject();
        for (i = 0; i < pipelineMeta.nrTransforms(); i++) {
          TransformMeta transformMeta = pipelineMeta.getTransform(i);
          if (line.contains(transformMeta.getName())) {
            pipelineGraph.editTransform(pipelineMeta, transformMeta);
          }
        }
      }
    }
  }

  public String getLoggingText() {
    if (pipelineLogText != null && !pipelineLogText.isDisposed()) {
      return pipelineLogText.getText();
    } else {
      return null;
    }
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
      item.setImage(GuiResource.getInstance().getImagePause());
      item.setToolTipText(BaseMessages.getString(PKG, "PipelineLog.Dialog.Pause.Tooltip"));
    } else {
      logBrowser.setPaused(true);
      item.setImage(GuiResource.getInstance().getImageRun());
      item.setToolTipText(BaseMessages.getString(PKG, "PipelineLog.Dialog.Resume.Tooltip"));
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
    return pipelineLogText != null
        && !pipelineLogText.isDisposed()
        && StringUtils.isNotEmpty(pipelineLogText.getSelectionText());
  }

  public void copySelected() {
    if (hasSelectedText()) {
      pipelineGraph.pipelineClipboardDelegate.toClipboard(pipelineLogText.getSelectionText());
    }
  }
}
