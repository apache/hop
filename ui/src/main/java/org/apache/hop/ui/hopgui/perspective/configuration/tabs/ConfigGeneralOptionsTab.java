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
 *
 */

package org.apache.hop.ui.hopgui.perspective.configuration.tabs;

import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

@GuiPlugin
public class ConfigGeneralOptionsTab {
  private static final Class<?> PKG = BaseDialog.class;

  private Text wDefaultPreview;
  private Button wUseCache;
  private Button wOpenLast;
  private Button wAutoSave;
  private Button wAutoSplit;
  private Button wCopyDistribute;
  private Button wExitWarning;
  private Button wToolTip;
  private Button wResolveVarsInTips;
  private Button wHelpTip;
  private Button wbUseDoubleClick;
  private Button wbDrawBorderAroundCanvasNames;
  private Button wbUseGlobalFileBookmarks;
  private Button wSortFieldByName;
  private Text wMaxExecutionLoggingTextSize;

  public ConfigGeneralOptionsTab() {
    // This instance is created in the GuiPlugin system by calling this constructor, after which it
    // calls the addGeneralOptionsTab() method.
  }

  @GuiTab(
      id = "10000-config-perspective-general-options-tab",
      parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
      description = "General options tab")
  public void addGeneralOptionsTab(CTabFolder wTabFolder) {
    Shell shell = wTabFolder.getShell();
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setImage(GuiResource.getInstance().getImageOptions());
    PropsUi.setLook(wGeneralTab);
    wGeneralTab.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.General.Label"));

    ScrolledComposite sGeneralComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    sGeneralComp.setLayout(new FillLayout());

    Composite wGeneralComp = new Composite(sGeneralComp, SWT.NONE);
    PropsUi.setLook(wGeneralComp);
    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = PropsUi.getFormMargin();
    generalLayout.marginHeight = PropsUi.getFormMargin();
    wGeneralComp.setLayout(generalLayout);

    // The name of the Hop configuration filename
    Label wlFilename = new Label(wGeneralComp, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.ConfigFilename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    fdlFilename.top = new FormAttachment(0, margin);
    wlFilename.setLayoutData(fdlFilename);
    Text wFilename = new Text(wGeneralComp, SWT.SINGLE | SWT.LEFT);
    wFilename.setText(Const.NVL(HopConfig.getInstance().getConfigFilename(), ""));
    wFilename.setEditable(false);
    PropsUi.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(100, 0);
    fdFilename.top = new FormAttachment(0, margin);
    wFilename.setLayoutData(fdFilename);
    Control lastControl = wFilename;

    // Explain HOP_CONFIG
    Label wlWhatIsHopConfig = new Label(wGeneralComp, SWT.LEFT);
    wlWhatIsHopConfig.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.WhatIsHopConfigSize.Label"));
    PropsUi.setLook(wlWhatIsHopConfig);
    FormData fdlWhatIsHopConfig = new FormData();
    fdlWhatIsHopConfig.left = new FormAttachment(middle, 0);
    fdlWhatIsHopConfig.right = new FormAttachment(100, 0);
    fdlWhatIsHopConfig.top = new FormAttachment(lastControl, margin);
    wlWhatIsHopConfig.setLayoutData(fdlWhatIsHopConfig);
    lastControl = wlWhatIsHopConfig;

    // The default preview size
    Label wlDefaultPreview = new Label(wGeneralComp, SWT.RIGHT);
    wlDefaultPreview.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.DefaultPreviewSize.Label"));
    PropsUi.setLook(wlDefaultPreview);
    FormData fdlDefaultPreview = new FormData();
    fdlDefaultPreview.left = new FormAttachment(0, 0);
    fdlDefaultPreview.right = new FormAttachment(middle, -margin);
    fdlDefaultPreview.top = new FormAttachment(lastControl, margin);
    wlDefaultPreview.setLayoutData(fdlDefaultPreview);
    wDefaultPreview = new Text(wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDefaultPreview.setText(Integer.toString(props.getDefaultPreviewSize()));
    PropsUi.setLook(wDefaultPreview);
    FormData fdDefaultPreview = new FormData();
    fdDefaultPreview.left = new FormAttachment(middle, 0);
    fdDefaultPreview.right = new FormAttachment(100, 0);
    fdDefaultPreview.top = new FormAttachment(wlDefaultPreview, 0, SWT.CENTER);
    wDefaultPreview.setLayoutData(fdDefaultPreview);
    wDefaultPreview.addListener(SWT.Modify, this::saveValues);
    lastControl = wDefaultPreview;

    // Use DB Cache?
    Label wlUseCache = new Label(wGeneralComp, SWT.RIGHT);
    wlUseCache.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.UseDatabaseCache.Label"));
    PropsUi.setLook(wlUseCache);
    FormData fdlUseCache = new FormData();
    fdlUseCache.left = new FormAttachment(0, 0);
    fdlUseCache.top = new FormAttachment(lastControl, margin);
    fdlUseCache.right = new FormAttachment(middle, -margin);
    wlUseCache.setLayoutData(fdlUseCache);
    wUseCache = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wUseCache);
    wUseCache.setSelection(props.useDBCache());
    FormData fdUseCache = new FormData();
    fdUseCache.left = new FormAttachment(middle, 0);
    fdUseCache.top = new FormAttachment(wlUseCache, 0, SWT.CENTER);
    fdUseCache.right = new FormAttachment(100, 0);
    wUseCache.setLayoutData(fdUseCache);
    wUseCache.addListener(SWT.Selection, this::saveValues);
    lastControl = wlUseCache;

    // Auto load last file at startup?
    Label wlOpenLast = new Label(wGeneralComp, SWT.RIGHT);
    wlOpenLast.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.OpenLastFileStartup.Label"));
    PropsUi.setLook(wlOpenLast);
    FormData fdlOpenLast = new FormData();
    fdlOpenLast.left = new FormAttachment(0, 0);
    fdlOpenLast.top = new FormAttachment(lastControl, margin);
    fdlOpenLast.right = new FormAttachment(middle, -margin);
    wlOpenLast.setLayoutData(fdlOpenLast);
    wOpenLast = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wOpenLast);
    wOpenLast.setSelection(props.openLastFile());
    FormData fdOpenLast = new FormData();
    fdOpenLast.left = new FormAttachment(middle, 0);
    fdOpenLast.top = new FormAttachment(wlOpenLast, 0, SWT.CENTER);
    fdOpenLast.right = new FormAttachment(100, 0);
    wOpenLast.setLayoutData(fdOpenLast);
    wOpenLast.addListener(SWT.Selection, this::saveValues);
    lastControl = wlOpenLast;

    // Auto save changed files?
    Label wlAutoSave = new Label(wGeneralComp, SWT.RIGHT);
    wlAutoSave.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.AutoSave.Label"));
    PropsUi.setLook(wlAutoSave);
    FormData fdlAutoSave = new FormData();
    fdlAutoSave.left = new FormAttachment(0, 0);
    fdlAutoSave.top = new FormAttachment(lastControl, margin);
    fdlAutoSave.right = new FormAttachment(middle, -margin);
    wlAutoSave.setLayoutData(fdlAutoSave);
    wAutoSave = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wAutoSave);
    wAutoSave.setSelection(props.getAutoSave());
    FormData fdAutoSave = new FormData();
    fdAutoSave.left = new FormAttachment(middle, 0);
    fdAutoSave.top = new FormAttachment(wlAutoSave, 0, SWT.CENTER);
    fdAutoSave.right = new FormAttachment(100, 0);
    wAutoSave.setLayoutData(fdAutoSave);
    wAutoSave.addListener(SWT.Selection, this::saveValues);
    lastControl = wlAutoSave;

    // Automatically split hops?
    Label wlAutoSplit = new Label(wGeneralComp, SWT.RIGHT);
    wlAutoSplit.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.AutoSplitHops.Label"));
    PropsUi.setLook(wlAutoSplit);
    FormData fdlAutoSplit = new FormData();
    fdlAutoSplit.left = new FormAttachment(0, 0);
    fdlAutoSplit.top = new FormAttachment(lastControl, margin);
    fdlAutoSplit.right = new FormAttachment(middle, -margin);
    wlAutoSplit.setLayoutData(fdlAutoSplit);
    wAutoSplit = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wAutoSplit);
    wAutoSplit.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.AutoSplitHops.Tooltip"));
    wAutoSplit.setSelection(props.getAutoSplit());
    FormData fdAutoSplit = new FormData();
    fdAutoSplit.left = new FormAttachment(middle, 0);
    fdAutoSplit.top = new FormAttachment(wlAutoSplit, 0, SWT.CENTER);
    fdAutoSplit.right = new FormAttachment(100, 0);
    wAutoSplit.setLayoutData(fdAutoSplit);
    wAutoSplit.addListener(SWT.Selection, this::saveValues);
    lastControl = wlAutoSplit;

    // Show warning for copy / distribute...
    Label wlCopyDistrib = new Label(wGeneralComp, SWT.RIGHT);
    wlCopyDistrib.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.CopyOrDistributeDialog.Label"));
    PropsUi.setLook(wlCopyDistrib);
    FormData fdlCopyDistrib = new FormData();
    fdlCopyDistrib.left = new FormAttachment(0, 0);
    fdlCopyDistrib.top = new FormAttachment(lastControl, margin);
    fdlCopyDistrib.right = new FormAttachment(middle, -margin);
    wlCopyDistrib.setLayoutData(fdlCopyDistrib);
    wCopyDistribute = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wCopyDistribute);
    wCopyDistribute.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.CopyOrDistributeDialog.Tooltip"));
    wCopyDistribute.setSelection(props.showCopyOrDistributeWarning());
    FormData fdCopyDistrib = new FormData();
    fdCopyDistrib.left = new FormAttachment(middle, 0);
    fdCopyDistrib.top = new FormAttachment(wlCopyDistrib, 0, SWT.CENTER);
    fdCopyDistrib.right = new FormAttachment(100, 0);
    wCopyDistribute.setLayoutData(fdCopyDistrib);
    wCopyDistribute.addListener(SWT.Selection, this::saveValues);
    lastControl = wlCopyDistrib;

    // Show exit warning?
    Label wlExitWarning = new Label(wGeneralComp, SWT.RIGHT);
    wlExitWarning.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.AskOnExit.Label"));
    PropsUi.setLook(wlExitWarning);
    FormData fdlExitWarning = new FormData();
    fdlExitWarning.left = new FormAttachment(0, 0);
    fdlExitWarning.top = new FormAttachment(lastControl, margin);
    fdlExitWarning.right = new FormAttachment(middle, -margin);
    wlExitWarning.setLayoutData(fdlExitWarning);
    wExitWarning = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wExitWarning);
    wExitWarning.setSelection(props.showExitWarning());
    FormData fdExitWarning = new FormData();
    fdExitWarning.left = new FormAttachment(middle, 0);
    fdExitWarning.top = new FormAttachment(wlExitWarning, 0, SWT.CENTER);
    fdExitWarning.right = new FormAttachment(100, 0);
    wExitWarning.setLayoutData(fdExitWarning);
    wExitWarning.addListener(SWT.Selection, this::saveValues);
    lastControl = wlExitWarning;

    // Clear custom parameters. (from transform)
    Label wlClearCustom = new Label(wGeneralComp, SWT.RIGHT);
    wlClearCustom.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ClearCustomParameters.Label"));
    PropsUi.setLook(wlClearCustom);
    FormData fdlClearCustom = new FormData();
    fdlClearCustom.left = new FormAttachment(0, 0);
    fdlClearCustom.top = new FormAttachment(lastControl, margin + 10);
    fdlClearCustom.right = new FormAttachment(middle, -margin);
    wlClearCustom.setLayoutData(fdlClearCustom);

    Button wClearCustom = new Button(wGeneralComp, SWT.PUSH);
    PropsUi.setLook(wClearCustom);
    FormData fdClearCustom = layoutResetOptionButton(wClearCustom);
    fdClearCustom.width = fdClearCustom.width + 6;
    fdClearCustom.height = fdClearCustom.height + 18;
    fdClearCustom.left = new FormAttachment(middle, 0);
    fdClearCustom.top = new FormAttachment(wlClearCustom, 0, SWT.CENTER);
    wClearCustom.setLayoutData(fdClearCustom);
    wClearCustom.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ClearCustomParameters.Tooltip"));
    wClearCustom.addListener(
        SWT.Selection,
        e -> {
          MessageBox mb = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
          mb.setMessage(
              BaseMessages.getString(PKG, "EnterOptionsDialog.ClearCustomParameters.Question"));
          mb.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.ClearCustomParameters.Title"));
          int id = mb.open();
          if (id == SWT.YES) {
            try {
              props.clearCustomParameters();
              saveValues(null);
              MessageBox ok = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
              ok.setMessage(
                  BaseMessages.getString(
                      PKG, "EnterOptionsDialog.ClearCustomParameters.Confirmation"));
              ok.open();
            } catch (Exception ex) {
              new ErrorDialog(
                  shell, "Error", "Error clearing custom parameters, saving config file", ex);
            }
          }
        });
    lastControl = wClearCustom;

    // Sort field by name
    Label wlSortFieldByName = new Label(wGeneralComp, SWT.RIGHT);
    wlSortFieldByName.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.SortFieldByName.Label"));
    wlSortFieldByName.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.SortFieldByName.ToolTip"));
    PropsUi.setLook(wlSortFieldByName);
    FormData fdlSortFieldByName = new FormData();
    fdlSortFieldByName.left = new FormAttachment(0, 0);
    fdlSortFieldByName.right = new FormAttachment(middle, -margin);
    fdlSortFieldByName.top = new FormAttachment(lastControl, 2 * margin);
    wlSortFieldByName.setLayoutData(fdlSortFieldByName);
    wSortFieldByName = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wSortFieldByName);
    FormData fdSortFieldByName = new FormData();
    fdSortFieldByName.left = new FormAttachment(middle, 0);
    fdSortFieldByName.right = new FormAttachment(100, -margin);
    fdSortFieldByName.top = new FormAttachment(wlSortFieldByName, 0, SWT.CENTER);
    wSortFieldByName.setLayoutData(fdSortFieldByName);
    wSortFieldByName.setSelection(props.isSortFieldByName());
    wSortFieldByName.addListener(SWT.Selection, this::saveValues);
    lastControl = wSortFieldByName;

    // Tooltips
    Label wlToolTip = new Label(wGeneralComp, SWT.RIGHT);
    wlToolTip.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.ToolTipsEnabled.Label"));
    PropsUi.setLook(wlToolTip);
    FormData fdlToolTip = new FormData();
    fdlToolTip.left = new FormAttachment(0, 0);
    fdlToolTip.top = new FormAttachment(lastControl, margin);
    fdlToolTip.right = new FormAttachment(middle, -margin);
    wlToolTip.setLayoutData(fdlToolTip);
    wToolTip = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wToolTip);
    wToolTip.setSelection(props.showToolTips());
    FormData fdbToolTip = new FormData();
    fdbToolTip.left = new FormAttachment(middle, 0);
    fdbToolTip.top = new FormAttachment(wlToolTip, 0, SWT.CENTER);
    fdbToolTip.right = new FormAttachment(100, 0);
    wToolTip.setLayoutData(fdbToolTip);
    wToolTip.addListener(SWT.Selection, this::saveValues);
    lastControl = wlToolTip;

    // Resolve variables in tooltips
    //
    Label wlResolveVarInTips = new Label(wGeneralComp, SWT.RIGHT);
    wlResolveVarInTips.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ResolveVarsInTips.Label"));
    PropsUi.setLook(wlResolveVarInTips);
    FormData fdlResolveVarInTips = new FormData();
    fdlResolveVarInTips.left = new FormAttachment(0, 0);
    fdlResolveVarInTips.top = new FormAttachment(lastControl, margin);
    fdlResolveVarInTips.right = new FormAttachment(middle, -margin);
    wlResolveVarInTips.setLayoutData(fdlResolveVarInTips);
    wResolveVarsInTips = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wResolveVarsInTips);
    wResolveVarsInTips.setSelection(props.resolveVariablesInToolTips());
    FormData fdbResolveVarInTips = new FormData();
    fdbResolveVarInTips.left = new FormAttachment(middle, 0);
    fdbResolveVarInTips.top = new FormAttachment(wlResolveVarInTips, 0, SWT.CENTER);
    fdbResolveVarInTips.right = new FormAttachment(100, 0);
    wResolveVarsInTips.setLayoutData(fdbResolveVarInTips);
    wResolveVarsInTips.addListener(SWT.Selection, this::saveValues);
    lastControl = wlResolveVarInTips;

    // Help tool tips
    Label wlHelpTip = new Label(wGeneralComp, SWT.RIGHT);
    wlHelpTip.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.HelpToolTipsEnabled.Label"));
    PropsUi.setLook(wlHelpTip);
    FormData fdlHelpTip = new FormData();
    fdlHelpTip.left = new FormAttachment(0, 0);
    fdlHelpTip.top = new FormAttachment(lastControl, margin);
    fdlHelpTip.right = new FormAttachment(middle, -margin);
    wlHelpTip.setLayoutData(fdlHelpTip);
    wHelpTip = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wHelpTip);
    wHelpTip.setSelection(props.isShowingHelpToolTips());
    FormData fdbHelpTip = new FormData();
    fdbHelpTip.left = new FormAttachment(middle, 0);
    fdbHelpTip.top = new FormAttachment(wlHelpTip, 0, SWT.CENTER);
    fdbHelpTip.right = new FormAttachment(100, 0);
    wHelpTip.setLayoutData(fdbHelpTip);
    wHelpTip.addListener(SWT.Selection, this::saveValues);
    lastControl = wlHelpTip;

    // Use double click on the canvas
    //
    Label wlUseDoubleClick = new Label(wGeneralComp, SWT.RIGHT);
    wlUseDoubleClick.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.UseDoubleClickOnCanvas.Label"));
    PropsUi.setLook(wlUseDoubleClick);
    FormData fdlUseDoubleClick = new FormData();
    fdlUseDoubleClick.left = new FormAttachment(0, 0);
    fdlUseDoubleClick.top = new FormAttachment(lastControl, margin);
    fdlUseDoubleClick.right = new FormAttachment(middle, -margin);
    wlUseDoubleClick.setLayoutData(fdlUseDoubleClick);
    wbUseDoubleClick = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wbUseDoubleClick);
    wbUseDoubleClick.setSelection(props.useDoubleClick());
    FormData fdbUseDoubleClick = new FormData();
    fdbUseDoubleClick.left = new FormAttachment(middle, 0);
    fdbUseDoubleClick.top = new FormAttachment(wlUseDoubleClick, 0, SWT.CENTER);
    fdbUseDoubleClick.right = new FormAttachment(100, 0);
    wbUseDoubleClick.setLayoutData(fdbUseDoubleClick);
    wbUseDoubleClick.addListener(SWT.Selection, this::saveValues);
    lastControl = wlUseDoubleClick;

    // Use double click on the canvas
    //
    Label wlDrawBorderAroundCanvasNames = new Label(wGeneralComp, SWT.RIGHT);
    wlDrawBorderAroundCanvasNames.setText(
        BaseMessages.getString(
            PKG, "EnterOptionsDialog.DrawBorderAroundCanvasNamesOnCanvas.Label"));
    PropsUi.setLook(wlDrawBorderAroundCanvasNames);
    FormData fdlDrawBorderAroundCanvasNames = new FormData();
    fdlDrawBorderAroundCanvasNames.left = new FormAttachment(0, 0);
    fdlDrawBorderAroundCanvasNames.top = new FormAttachment(lastControl, margin);
    fdlDrawBorderAroundCanvasNames.right = new FormAttachment(middle, -margin);
    wlDrawBorderAroundCanvasNames.setLayoutData(fdlDrawBorderAroundCanvasNames);
    wbDrawBorderAroundCanvasNames = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wbDrawBorderAroundCanvasNames);
    wbDrawBorderAroundCanvasNames.setSelection(props.useDoubleClick());
    FormData fdbDrawBorderAroundCanvasNames = new FormData();
    fdbDrawBorderAroundCanvasNames.left = new FormAttachment(middle, 0);
    fdbDrawBorderAroundCanvasNames.top =
        new FormAttachment(wlDrawBorderAroundCanvasNames, 0, SWT.CENTER);
    fdbDrawBorderAroundCanvasNames.right = new FormAttachment(100, 0);
    wbDrawBorderAroundCanvasNames.setLayoutData(fdbDrawBorderAroundCanvasNames);
    wbDrawBorderAroundCanvasNames.addListener(SWT.Selection, this::saveValues);
    lastControl = wlDrawBorderAroundCanvasNames;

    // Use global file bookmarks?
    Label wlUseGlobalFileBookmarks = new Label(wGeneralComp, SWT.RIGHT);
    wlUseGlobalFileBookmarks.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.UseGlobalFileBookmarks.Label"));
    PropsUi.setLook(wlUseGlobalFileBookmarks);
    FormData fdlUseGlobalFileBookmarks = new FormData();
    fdlUseGlobalFileBookmarks.left = new FormAttachment(0, 0);
    fdlUseGlobalFileBookmarks.top = new FormAttachment(lastControl, margin);
    fdlUseGlobalFileBookmarks.right = new FormAttachment(middle, -margin);
    wlUseGlobalFileBookmarks.setLayoutData(fdlUseGlobalFileBookmarks);
    wbUseGlobalFileBookmarks = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wbUseGlobalFileBookmarks);
    wbUseGlobalFileBookmarks.setSelection(props.useGlobalFileBookmarks());
    FormData fdbUseGlobalFileBookmarks = new FormData();
    fdbUseGlobalFileBookmarks.left = new FormAttachment(middle, 0);
    fdbUseGlobalFileBookmarks.top = new FormAttachment(wlUseGlobalFileBookmarks, 0, SWT.CENTER);
    fdbUseGlobalFileBookmarks.right = new FormAttachment(100, 0);
    wbUseGlobalFileBookmarks.setLayoutData(fdbUseGlobalFileBookmarks);
    wbUseGlobalFileBookmarks.addListener(SWT.Selection, this::saveValues);
    lastControl = wlUseGlobalFileBookmarks;

    // The default preview size
    Label wlMaxExecutionLoggingTextSize = new Label(wGeneralComp, SWT.RIGHT);
    wlMaxExecutionLoggingTextSize.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.MaxExecutionLoggingTextSizeSize.Label"));
    PropsUi.setLook(wlMaxExecutionLoggingTextSize);
    FormData fdlMaxExecutionLoggingTextSize = new FormData();
    fdlMaxExecutionLoggingTextSize.left = new FormAttachment(0, 0);
    fdlMaxExecutionLoggingTextSize.right = new FormAttachment(middle, -margin);
    fdlMaxExecutionLoggingTextSize.top = new FormAttachment(lastControl, 2 * margin);
    wlMaxExecutionLoggingTextSize.setLayoutData(fdlMaxExecutionLoggingTextSize);
    wMaxExecutionLoggingTextSize = new Text(wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMaxExecutionLoggingTextSize.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.MaxExecutionLoggingTextSizeSize.ToolTip"));
    wMaxExecutionLoggingTextSize.setText(Integer.toString(props.getMaxExecutionLoggingTextSize()));
    PropsUi.setLook(wMaxExecutionLoggingTextSize);
    FormData fdMaxExecutionLoggingTextSize = new FormData();
    fdMaxExecutionLoggingTextSize.left = new FormAttachment(middle, 0);
    fdMaxExecutionLoggingTextSize.right = new FormAttachment(100, 0);
    fdMaxExecutionLoggingTextSize.top =
        new FormAttachment(wlMaxExecutionLoggingTextSize, 0, SWT.CENTER);
    wMaxExecutionLoggingTextSize.setLayoutData(fdMaxExecutionLoggingTextSize);
    wMaxExecutionLoggingTextSize.addListener(SWT.Modify, this::saveValues);

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 100);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.pack();

    Rectangle bounds = wGeneralComp.getBounds();

    sGeneralComp.setContent(wGeneralComp);
    sGeneralComp.setExpandHorizontal(true);
    sGeneralComp.setExpandVertical(true);
    sGeneralComp.setMinWidth(bounds.width);
    sGeneralComp.setMinHeight(bounds.height);

    wGeneralTab.setControl(sGeneralComp);
  }

  /**
   * Setting the layout of a <i>Reset</i> option button. Either a button image is set - if existing
   * - or a text.
   *
   * @param button The button
   */
  private FormData layoutResetOptionButton(Button button) {
    FormData fd = new FormData();
    Image editButton = GuiResource.getInstance().getImageResetOption();
    if (editButton != null) {
      button.setImage(editButton);
      button.setBackground(GuiResource.getInstance().getColorWhite());
      fd.width = editButton.getBounds().width + 20;
      fd.height = editButton.getBounds().height;
    } else {
      button.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.Button.Reset"));
    }

    button.setToolTipText(BaseMessages.getString(PKG, "EnterOptionsDialog.Button.Reset.Tooltip"));
    return fd;
  }

  private void saveValues(Event event) {
    PropsUi props = PropsUi.getInstance();

    props.setDefaultPreviewSize(
        Const.toInt(wDefaultPreview.getText(), props.getDefaultPreviewSize()));
    props.setUseDBCache(wUseCache.getSelection());
    props.setOpenLastFile(wOpenLast.getSelection());
    props.setAutoSave(wAutoSave.getSelection());
    props.setAutoSplit(wAutoSplit.getSelection());
    props.setShowCopyOrDistributeWarning(wCopyDistribute.getSelection());
    props.setExitWarningShown(wExitWarning.getSelection());
    props.setShowToolTips(wToolTip.getSelection());
    props.setResolveVariablesInToolTips(wResolveVarsInTips.getSelection());
    props.setSortFieldByName(wSortFieldByName.getSelection());
    props.setShowingHelpToolTips(wHelpTip.getSelection());
    props.setUseDoubleClickOnCanvas(wbUseDoubleClick.getSelection());
    props.setDrawBorderAroundCanvasNames(wbDrawBorderAroundCanvasNames.getSelection());
    props.setUseGlobalFileBookmarks(wbUseGlobalFileBookmarks.getSelection());
    props.setMaxExecutionLoggingTextSize(
        Const.toInt(
            wMaxExecutionLoggingTextSize.getText(),
            PropsUi.DEFAULT_MAX_EXECUTION_LOGGING_TEXT_SIZE));

    try {
      HopConfig.getInstance().saveToFile();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), "Error", "Error saving configuration to file", e);
    }
  }
}
