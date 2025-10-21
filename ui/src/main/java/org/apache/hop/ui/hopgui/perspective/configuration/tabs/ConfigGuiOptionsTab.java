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
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.i18n.GlobalMessages;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FontDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

@GuiPlugin
public class ConfigGuiOptionsTab {
  private static final Class<?> PKG = BaseDialog.class;

  private FontData defaultFontData;
  private Font defaultFont;
  private FontData fixedFontData;
  private Font fixedFont;
  private FontData graphFontData;
  private Font graphFont;
  private FontData noteFontData;
  private Font noteFont;

  private Canvas wDefaultCanvas;
  private Canvas wFixedCanvas;
  private Canvas wGraphCanvas;
  private Canvas wNoteCanvas;

  private Text wIconSize;
  private Text wLineWidth;
  private Text wMiddlePct;
  private Combo wGlobalZoom;
  private Text wGridSize;
  private Button wDarkMode;
  private Button wShowCanvasGrid;
  private Button wHideViewport;
  private Button wHideMenuBar;
  private Button wShowTableViewToolbar;
  private Combo wDefaultLocale;

  public ConfigGuiOptionsTab() {
    // This instance is created in the GuiPlugin system by calling this constructor, after which it
    // calls the addGeneralOptionsTab() method.
  }

  @GuiTab(
      id = "10100-config-perspective-gui-options-tab",
      parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
      description = "GUI options tab")
  public void addGuiOptionsTab(CTabFolder wTabFolder) {
    Shell shell = wTabFolder.getShell();
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();
    int h = (int) (40 * props.getZoomFactor());

    CTabItem wLookTab = new CTabItem(wTabFolder, SWT.NONE);
    wLookTab.setFont(GuiResource.getInstance().getFontDefault());
    wLookTab.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.LookAndFeel.Label"));
    wLookTab.setImage(GuiResource.getInstance().getImageColor());

    ScrolledComposite sLookComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    sLookComp.setLayout(new FillLayout());

    Composite wLookComp = new Composite(sLookComp, SWT.NONE);
    PropsUi.setLook(wLookComp);

    FormLayout lookLayout = new FormLayout();
    lookLayout.marginWidth = PropsUi.getFormMargin();
    lookLayout.marginHeight = PropsUi.getFormMargin();
    wLookComp.setLayout(lookLayout);

    defaultFontData = props.getDefaultFont();
    defaultFont = new Font(shell.getDisplay(), defaultFontData);
    fixedFontData = props.getFixedFont();
    fixedFont = new Font(shell.getDisplay(), fixedFontData);
    graphFontData = props.getGraphFont();
    graphFont = new Font(shell.getDisplay(), graphFontData);
    noteFontData = props.getNoteFont();
    noteFont = new Font(shell.getDisplay(), noteFontData);

    // Default font
    int nr = 0;
    {
      Label wlDFont = new Label(wLookComp, SWT.RIGHT);
      wlDFont.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.DefaultFont.Label"));
      PropsUi.setLook(wlDFont);
      FormData fdlDFont = new FormData();
      fdlDFont.left = new FormAttachment(0, 0);
      fdlDFont.right = new FormAttachment(middle, -margin);
      fdlDFont.top = new FormAttachment(0, margin + 10);
      wlDFont.setLayoutData(fdlDFont);

      Button wdDFont = new Button(wLookComp, SWT.PUSH | SWT.CENTER);
      PropsUi.setLook(wdDFont);
      FormData fddDFont = layoutResetOptionButton(wdDFont);
      fddDFont.right = new FormAttachment(100, 0);
      fddDFont.top = new FormAttachment(0, margin);
      fddDFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wdDFont.setLayoutData(fddDFont);
      wdDFont.addListener(SWT.Selection, e -> resetDefaultFont(shell));

      Button wbDFont = new Button(wLookComp, SWT.PUSH);
      PropsUi.setLook(wbDFont);
      FormData fdbDFont = layoutEditOptionButton(wbDFont);
      fdbDFont.right = new FormAttachment(wdDFont, -margin);
      fdbDFont.top = new FormAttachment(0, nr * h + margin);
      fdbDFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wbDFont.setLayoutData(fdbDFont);
      wbDFont.addListener(SWT.Selection, e -> editDefaultFont(shell));

      wDefaultCanvas = new Canvas(wLookComp, SWT.BORDER);
      PropsUi.setLook(wDefaultCanvas);
      FormData fdDFont = new FormData();
      fdDFont.left = new FormAttachment(middle, 0);
      fdDFont.right = new FormAttachment(wbDFont, -margin);
      fdDFont.top = new FormAttachment(0, margin);
      fdDFont.bottom = new FormAttachment(0, h);
      wDefaultCanvas.setLayoutData(fdDFont);
      wDefaultCanvas.addPaintListener(this::paintDefaultFont);
      wDefaultCanvas.addListener(SWT.MouseDown, e -> editDefaultFont(shell));
    }

    // Fixed font
    nr++;
    {
      Label wlFFont = new Label(wLookComp, SWT.RIGHT);
      wlFFont.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.FixedWidthFont.Label"));
      PropsUi.setLook(wlFFont);
      FormData fdlFFont = new FormData();
      fdlFFont.left = new FormAttachment(0, 0);
      fdlFFont.right = new FormAttachment(middle, -margin);
      fdlFFont.top = new FormAttachment(0, nr * h + margin + 10);
      wlFFont.setLayoutData(fdlFFont);

      Button wdFFont = new Button(wLookComp, SWT.PUSH | SWT.CENTER);
      PropsUi.setLook(wdFFont);
      FormData fddFFont = layoutResetOptionButton(wdFFont);
      fddFFont.right = new FormAttachment(100, 0);
      fddFFont.top = new FormAttachment(0, nr * h + margin);
      fddFFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wdFFont.setLayoutData(fddFFont);
      wdFFont.addListener(SWT.Selection, e -> resetFixedFont(shell));

      Button wbFFont = new Button(wLookComp, SWT.PUSH);
      PropsUi.setLook(wbFFont);
      FormData fdbFFont = layoutEditOptionButton(wbFFont);
      fdbFFont.right = new FormAttachment(wdFFont, -margin);
      fdbFFont.top = new FormAttachment(0, nr * h + margin);
      fdbFFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wbFFont.setLayoutData(fdbFFont);
      wbFFont.addListener(SWT.Selection, e -> editFixedFont(shell));

      wFixedCanvas = new Canvas(wLookComp, SWT.BORDER);
      PropsUi.setLook(wFixedCanvas);
      FormData fdFFont = new FormData();
      fdFFont.left = new FormAttachment(middle, 0);
      fdFFont.right = new FormAttachment(wbFFont, -margin);
      fdFFont.top = new FormAttachment(0, nr * h + margin);
      fdFFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wFixedCanvas.setLayoutData(fdFFont);
      wFixedCanvas.addPaintListener(this::paintFixedFont);
      wFixedCanvas.addListener(SWT.MouseDown, e -> editFixedFont(shell));
    }

    // Graph font
    nr++;
    {
      Label wlGFont = new Label(wLookComp, SWT.RIGHT);
      wlGFont.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.GraphFont.Label"));
      PropsUi.setLook(wlGFont);
      FormData fdlGFont = new FormData();
      fdlGFont.left = new FormAttachment(0, 0);
      fdlGFont.right = new FormAttachment(middle, -margin);
      fdlGFont.top = new FormAttachment(0, nr * h + margin + 10);
      wlGFont.setLayoutData(fdlGFont);

      Button wdGFont = new Button(wLookComp, SWT.PUSH);
      PropsUi.setLook(wdGFont);

      FormData fddGFont = layoutResetOptionButton(wdGFont);
      fddGFont.right = new FormAttachment(100, 0);
      fddGFont.top = new FormAttachment(0, nr * h + margin);
      fddGFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wdGFont.setLayoutData(fddGFont);
      wdGFont.addListener(SWT.Selection, e -> resetGraphFont(shell, props));

      Button wbGFont = new Button(wLookComp, SWT.PUSH);
      PropsUi.setLook(wbGFont);

      FormData fdbGFont = layoutEditOptionButton(wbGFont);
      fdbGFont.right = new FormAttachment(wdGFont, -margin);
      fdbGFont.top = new FormAttachment(0, nr * h + margin);
      fdbGFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wbGFont.setLayoutData(fdbGFont);
      wbGFont.addListener(SWT.Selection, e -> editGraphFont(shell));

      wGraphCanvas = new Canvas(wLookComp, SWT.BORDER);
      PropsUi.setLook(wGraphCanvas);
      FormData fdGFont = new FormData();
      fdGFont.left = new FormAttachment(middle, 0);
      fdGFont.right = new FormAttachment(wbGFont, -margin);
      fdGFont.top = new FormAttachment(0, nr * h + margin);
      fdGFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wGraphCanvas.setLayoutData(fdGFont);
      wGraphCanvas.addPaintListener(this::drawGraphFont);
      wGraphCanvas.addListener(SWT.MouseDown, e -> editGraphFont(shell));
    }

    // Note font
    nr++;
    {
      Label wlNFont = new Label(wLookComp, SWT.RIGHT);
      wlNFont.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.NoteFont.Label"));
      PropsUi.setLook(wlNFont);
      FormData fdlNFont = new FormData();
      fdlNFont.left = new FormAttachment(0, 0);
      fdlNFont.right = new FormAttachment(middle, -margin);
      fdlNFont.top = new FormAttachment(0, nr * h + margin + 10);
      wlNFont.setLayoutData(fdlNFont);

      Button wdNFont = new Button(wLookComp, SWT.PUSH);
      PropsUi.setLook(wdNFont);
      FormData fddNFont = layoutResetOptionButton(wdNFont);
      fddNFont.right = new FormAttachment(100, 0);
      fddNFont.top = new FormAttachment(0, nr * h + margin);
      fddNFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wdNFont.setLayoutData(fddNFont);
      wdNFont.addListener(SWT.Selection, e -> resetNoteFont(e, props, shell.getDisplay()));

      Button wbNFont = new Button(wLookComp, SWT.PUSH);
      PropsUi.setLook(wbNFont);
      FormData fdbNFont = layoutEditOptionButton(wbNFont);
      fdbNFont.right = new FormAttachment(wdNFont, -margin);
      fdbNFont.top = new FormAttachment(0, nr * h + margin);
      fdbNFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wbNFont.setLayoutData(fdbNFont);
      wbNFont.addListener(SWT.Selection, e -> editNoteFont(shell));

      wNoteCanvas = new Canvas(wLookComp, SWT.BORDER);
      PropsUi.setLook(wNoteCanvas);
      FormData fdNFont = new FormData();
      fdNFont.left = new FormAttachment(middle, 0);
      fdNFont.right = new FormAttachment(wbNFont, -margin);
      fdNFont.top = new FormAttachment(0, nr * h + margin);
      fdNFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
      wNoteCanvas.setLayoutData(fdNFont);
      wNoteCanvas.addPaintListener(this::paintNoteFont);
      wNoteCanvas.addListener(SWT.MouseDown, e -> editNoteFont(shell));
    }

    // IconSize line
    Label wlIconSize = new Label(wLookComp, SWT.RIGHT);
    wlIconSize.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.IconSize.Label"));
    PropsUi.setLook(wlIconSize);
    FormData fdlIconSize = new FormData();
    fdlIconSize.left = new FormAttachment(0, 0);
    fdlIconSize.right = new FormAttachment(middle, -margin);
    fdlIconSize.top = new FormAttachment(wNoteCanvas, margin);
    wlIconSize.setLayoutData(fdlIconSize);
    wIconSize = new Text(wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wIconSize.setText(Integer.toString(props.getIconSize()));
    PropsUi.setLook(wIconSize);
    FormData fdIconSize = new FormData();
    fdIconSize.left = new FormAttachment(middle, 0);
    fdIconSize.right = new FormAttachment(100, -margin);
    fdIconSize.top = new FormAttachment(wlIconSize, 0, SWT.CENTER);
    wIconSize.setLayoutData(fdIconSize);
    wIconSize.addListener(SWT.Modify, e -> saveValues());

    // LineWidth line
    Label wlLineWidth = new Label(wLookComp, SWT.RIGHT);
    wlLineWidth.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.LineWidth.Label"));
    PropsUi.setLook(wlLineWidth);
    FormData fdlLineWidth = new FormData();
    fdlLineWidth.left = new FormAttachment(0, 0);
    fdlLineWidth.right = new FormAttachment(middle, -margin);
    fdlLineWidth.top = new FormAttachment(wIconSize, margin);
    wlLineWidth.setLayoutData(fdlLineWidth);
    wLineWidth = new Text(wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wLineWidth.setText(Integer.toString(props.getLineWidth()));
    PropsUi.setLook(wLineWidth);
    FormData fdLineWidth = new FormData();
    fdLineWidth.left = new FormAttachment(middle, 0);
    fdLineWidth.right = new FormAttachment(100, -margin);
    fdLineWidth.top = new FormAttachment(wlLineWidth, 0, SWT.CENTER);
    wLineWidth.setLayoutData(fdLineWidth);
    wLineWidth.addListener(SWT.Modify, e -> saveValues());

    // MiddlePct line
    Label wlMiddlePct = new Label(wLookComp, SWT.RIGHT);
    wlMiddlePct.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.DialogMiddlePercentage.Label"));
    PropsUi.setLook(wlMiddlePct);
    FormData fdlMiddlePct = new FormData();
    fdlMiddlePct.left = new FormAttachment(0, 0);
    fdlMiddlePct.right = new FormAttachment(middle, -margin);
    fdlMiddlePct.top = new FormAttachment(wLineWidth, margin);
    wlMiddlePct.setLayoutData(fdlMiddlePct);
    wMiddlePct = new Text(wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMiddlePct.setText(Integer.toString(props.getMiddlePct()));
    PropsUi.setLook(wMiddlePct);
    FormData fdMiddlePct = new FormData();
    fdMiddlePct.left = new FormAttachment(middle, 0);
    fdMiddlePct.right = new FormAttachment(100, -margin);
    fdMiddlePct.top = new FormAttachment(wlMiddlePct, 0, SWT.CENTER);
    wMiddlePct.setLayoutData(fdMiddlePct);
    wMiddlePct.addListener(SWT.Modify, e -> saveValues());

    // Global Zoom
    Label wlGlobalZoom = new Label(wLookComp, SWT.RIGHT);
    wlGlobalZoom.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.GlobalZoom.Label"));
    PropsUi.setLook(wlGlobalZoom);
    FormData fdlGlobalZoom = new FormData();
    fdlGlobalZoom.left = new FormAttachment(0, 0);
    fdlGlobalZoom.right = new FormAttachment(middle, -margin);
    fdlGlobalZoom.top = new FormAttachment(wMiddlePct, margin);
    wlGlobalZoom.setLayoutData(fdlGlobalZoom);
    wGlobalZoom = new Combo(wLookComp, SWT.SINGLE | SWT.READ_ONLY | SWT.LEFT | SWT.BORDER);
    wGlobalZoom.setItems(PropsUi.getGlobalZoomFactorLevels());
    PropsUi.setLook(wGlobalZoom);
    FormData fdGlobalZoom = new FormData();
    fdGlobalZoom.left = new FormAttachment(middle, 0);
    fdGlobalZoom.right = new FormAttachment(100, -margin);
    fdGlobalZoom.top = new FormAttachment(wlGlobalZoom, 0, SWT.CENTER);
    wGlobalZoom.setLayoutData(fdGlobalZoom);
    // set the current value
    String globalZoomFactor = Integer.toString((int) (props.getGlobalZoomFactor() * 100)) + '%';
    wGlobalZoom.setText(globalZoomFactor);
    wGlobalZoom.addListener(SWT.Modify, e -> saveValues());

    // GridSize line
    Label wlGridSize = new Label(wLookComp, SWT.RIGHT);
    wlGridSize.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.GridSize.Label"));
    wlGridSize.setToolTipText(BaseMessages.getString(PKG, "EnterOptionsDialog.GridSize.ToolTip"));
    PropsUi.setLook(wlGridSize);
    FormData fdlGridSize = new FormData();
    fdlGridSize.left = new FormAttachment(0, 0);
    fdlGridSize.right = new FormAttachment(middle, -margin);
    fdlGridSize.top = new FormAttachment(wGlobalZoom, margin);
    wlGridSize.setLayoutData(fdlGridSize);
    wGridSize = new Text(wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wGridSize.setText(Integer.toString(props.getCanvasGridSize()));
    wGridSize.setToolTipText(BaseMessages.getString(PKG, "EnterOptionsDialog.GridSize.ToolTip"));
    PropsUi.setLook(wGridSize);
    FormData fdGridSize = new FormData();
    fdGridSize.left = new FormAttachment(middle, 0);
    fdGridSize.right = new FormAttachment(100, -margin);
    fdGridSize.top = new FormAttachment(wlGridSize, 0, SWT.CENTER);
    wGridSize.setLayoutData(fdGridSize);
    wGridSize.addListener(SWT.Modify, e -> saveValues());

    // Show Canvas Grid
    Label wlShowCanvasGrid = new Label(wLookComp, SWT.RIGHT);
    wlShowCanvasGrid.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ShowCanvasGrid.Label"));
    wlShowCanvasGrid.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ShowCanvasGrid.ToolTip"));
    PropsUi.setLook(wlShowCanvasGrid);
    FormData fdlShowCanvasGrid = new FormData();
    fdlShowCanvasGrid.left = new FormAttachment(0, 0);
    fdlShowCanvasGrid.right = new FormAttachment(middle, -margin);
    fdlShowCanvasGrid.top = new FormAttachment(wGridSize, margin);
    wlShowCanvasGrid.setLayoutData(fdlShowCanvasGrid);
    wShowCanvasGrid = new Button(wLookComp, SWT.CHECK);
    PropsUi.setLook(wShowCanvasGrid);
    wShowCanvasGrid.setSelection(props.isShowCanvasGridEnabled());
    FormData fdShowCanvasGrid = new FormData();
    fdShowCanvasGrid.left = new FormAttachment(middle, 0);
    fdShowCanvasGrid.right = new FormAttachment(100, -margin);
    fdShowCanvasGrid.top = new FormAttachment(wlShowCanvasGrid, 0, SWT.CENTER);
    wShowCanvasGrid.setLayoutData(fdShowCanvasGrid);
    wShowCanvasGrid.addListener(SWT.Selection, e -> saveValues());

    // Hide Viewport
    Label wlHideViewport = new Label(wLookComp, SWT.RIGHT);
    wlHideViewport.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.ShowViewport.Label"));
    wlHideViewport.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ShowViewport.ToolTip"));
    PropsUi.setLook(wlHideViewport);
    FormData fdlHideViewport = new FormData();
    fdlHideViewport.left = new FormAttachment(0, 0);
    fdlHideViewport.right = new FormAttachment(middle, -margin);
    fdlHideViewport.top = new FormAttachment(wShowCanvasGrid, margin);
    wlHideViewport.setLayoutData(fdlHideViewport);
    wHideViewport = new Button(wLookComp, SWT.CHECK);
    PropsUi.setLook(wHideViewport);
    wHideViewport.setSelection(props.isHideViewportEnabled());
    FormData fdHideViewport = new FormData();
    fdHideViewport.left = new FormAttachment(middle, 0);
    fdHideViewport.right = new FormAttachment(100, -margin);
    fdHideViewport.top = new FormAttachment(wlHideViewport, 0, SWT.CENTER);
    wHideViewport.setLayoutData(fdHideViewport);
    wHideViewport.addListener(SWT.Selection, e -> saveValues());

    // Hide menu bar?
    Label wlHideMenuBar = new Label(wLookComp, SWT.RIGHT);
    wlHideMenuBar.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.HideMenuBar.Label"));
    wlHideMenuBar.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.HideMenuBar.ToolTip"));
    PropsUi.setLook(wlHideMenuBar);
    FormData fdlHideMenuBar = new FormData();
    fdlHideMenuBar.left = new FormAttachment(0, 0);
    fdlHideMenuBar.right = new FormAttachment(middle, -margin);
    fdlHideMenuBar.top = new FormAttachment(wHideViewport, 2 * margin);
    wlHideMenuBar.setLayoutData(fdlHideMenuBar);
    wHideMenuBar = new Button(wLookComp, SWT.CHECK);
    PropsUi.setLook(wHideMenuBar);
    wHideMenuBar.setSelection(props.isHidingMenuBar());
    FormData fdHideMenuBar = new FormData();
    fdHideMenuBar.left = new FormAttachment(middle, 0);
    fdHideMenuBar.right = new FormAttachment(100, -margin);
    fdHideMenuBar.top = new FormAttachment(wlHideMenuBar, 0, SWT.CENTER);
    wHideMenuBar.setLayoutData(fdHideMenuBar);
    wHideMenuBar.addListener(SWT.Selection, e -> saveValues());

    // Show tableview tool bar ?
    Label wlShowTableViewToolbar = new Label(wLookComp, SWT.RIGHT);
    wlShowTableViewToolbar.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ShowTableViewToolbar.Label"));
    wlShowTableViewToolbar.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ShowTableViewToolbar.ToolTip"));
    PropsUi.setLook(wlShowTableViewToolbar);
    FormData fdlShowTableViewToolbar = new FormData();
    fdlShowTableViewToolbar.left = new FormAttachment(0, 0);
    fdlShowTableViewToolbar.right = new FormAttachment(middle, -margin);
    fdlShowTableViewToolbar.top = new FormAttachment(wHideMenuBar, 2 * margin);
    wlShowTableViewToolbar.setLayoutData(fdlShowTableViewToolbar);
    wShowTableViewToolbar = new Button(wLookComp, SWT.CHECK);
    PropsUi.setLook(wShowTableViewToolbar);
    wShowTableViewToolbar.setSelection(props.isShowTableViewToolbar());
    FormData fdShowTableViewToolbar = new FormData();
    fdShowTableViewToolbar.left = new FormAttachment(middle, 0);
    fdShowTableViewToolbar.right = new FormAttachment(100, -margin);
    fdShowTableViewToolbar.top = new FormAttachment(wlShowTableViewToolbar, 0, SWT.CENTER);
    wShowTableViewToolbar.setLayoutData(fdShowTableViewToolbar);
    wShowTableViewToolbar.addListener(SWT.Selection, e -> saveValues());

    // Is Dark Mode enabled
    Label wlDarkMode = new Label(wLookComp, SWT.RIGHT);
    wlDarkMode.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.DarkMode.Label"));
    PropsUi.setLook(wlDarkMode);
    FormData fdlDarkMode = new FormData();
    fdlDarkMode.left = new FormAttachment(0, 0);
    fdlDarkMode.top = new FormAttachment(wShowTableViewToolbar, 2 * margin);
    fdlDarkMode.right = new FormAttachment(middle, -margin);
    wlDarkMode.setLayoutData(fdlDarkMode);
    wDarkMode = new Button(wLookComp, SWT.CHECK);
    wDarkMode.setSelection(props.isDarkMode());
    PropsUi.setLook(wDarkMode);
    FormData fdDarkMode = new FormData();
    fdDarkMode.left = new FormAttachment(middle, 0);
    fdDarkMode.top = new FormAttachment(wlDarkMode, 0, SWT.CENTER);
    fdDarkMode.right = new FormAttachment(100, 0);
    wDarkMode.setLayoutData(fdDarkMode);
    wlDarkMode.setEnabled(Const.isWindows());
    wDarkMode.setEnabled(Const.isWindows());
    wDarkMode.addListener(SWT.Selection, e -> saveValues());

    // DefaultLocale line
    Label wlDefaultLocale = new Label(wLookComp, SWT.RIGHT);
    wlDefaultLocale.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.DefaultLocale.Label"));
    PropsUi.setLook(wlDefaultLocale);
    FormData fdlDefaultLocale = new FormData();
    fdlDefaultLocale.left = new FormAttachment(0, 0);
    fdlDefaultLocale.right = new FormAttachment(middle, -margin);
    fdlDefaultLocale.top = new FormAttachment(wlDarkMode, 2 * margin);
    wlDefaultLocale.setLayoutData(fdlDefaultLocale);
    wDefaultLocale = new Combo(wLookComp, SWT.SINGLE | SWT.READ_ONLY | SWT.LEFT | SWT.BORDER);
    wDefaultLocale.setItems(GlobalMessages.localeDescr);
    PropsUi.setLook(wDefaultLocale);
    FormData fdDefaultLocale = new FormData();
    fdDefaultLocale.left = new FormAttachment(middle, 0);
    fdDefaultLocale.right = new FormAttachment(100, -margin);
    fdDefaultLocale.top = new FormAttachment(wlDefaultLocale, 0, SWT.CENTER);
    wDefaultLocale.setLayoutData(fdDefaultLocale);
    wDefaultLocale.addListener(SWT.Modify, e -> saveValues());

    // language selections...
    int idxDefault =
        Const.indexOfString(
            LanguageChoice.getInstance().getDefaultLocale().toString(), GlobalMessages.localeCodes);
    if (idxDefault >= 0) {
      wDefaultLocale.select(idxDefault);
    }

    FormData fdLookComp = new FormData();
    fdLookComp.left = new FormAttachment(0, 0);
    fdLookComp.right = new FormAttachment(100, 0);
    fdLookComp.top = new FormAttachment(0, 0);
    fdLookComp.bottom = new FormAttachment(100, 100);
    wLookComp.setLayoutData(fdLookComp);

    wLookComp.pack();

    Rectangle bounds = wLookComp.getBounds();
    sLookComp.setContent(wLookComp);
    sLookComp.setExpandHorizontal(true);
    sLookComp.setExpandVertical(true);
    sLookComp.setMinWidth(bounds.width);
    sLookComp.setMinHeight(bounds.height);

    wLookTab.setControl(sLookComp);
  }

  private void paintNoteFont(PaintEvent pe) {
    pe.gc.setFont(noteFont);
    Rectangle max = wNoteCanvas.getBounds();
    String name = noteFontData.getName() + " - " + noteFontData.getHeight();
    Point size = pe.gc.textExtent(name);

    pe.gc.drawText(name, (max.width - size.x) / 2, (max.height - size.y) / 2, true);
  }

  private void resetNoteFont(Event e, PropsUi props, Display display) {
    noteFontData = props.getDefaultFontData();
    noteFont.dispose();
    noteFont = new Font(display, noteFontData);
    wNoteCanvas.redraw();
    saveValues();
  }

  private void editNoteFont(Shell shell) {
    FontDialog fd = new FontDialog(shell);
    fd.setFontList(new FontData[] {noteFontData});
    FontData newfd = fd.open();
    if (newfd != null) {
      noteFontData = newfd;
      noteFont.dispose();
      noteFont = new Font(shell.getDisplay(), noteFontData);
      wNoteCanvas.redraw();
      saveValues();
    }
  }

  private void drawGraphFont(PaintEvent pe) {
    pe.gc.setFont(graphFont);
    Rectangle max = wGraphCanvas.getBounds();
    String name = graphFontData.getName() + " - " + graphFontData.getHeight();
    Point size = pe.gc.textExtent(name);

    pe.gc.drawText(name, (max.width - size.x) / 2, (max.height - size.y) / 2, true);
  }

  private void editGraphFont(Shell shell) {
    FontDialog fd = new FontDialog(shell);
    fd.setFontList(new FontData[] {graphFontData});
    FontData newfd = fd.open();
    if (newfd != null) {
      graphFontData = newfd;
      graphFont.dispose();
      graphFont = new Font(shell.getDisplay(), graphFontData);
      wGraphCanvas.redraw();
      saveValues();
    }
  }

  private void resetGraphFont(Shell shell, PropsUi props) {
    graphFont.dispose();

    graphFontData = props.getDefaultFontData();
    graphFont = new Font(shell.getDisplay(), graphFontData);
    wGraphCanvas.redraw();
    saveValues();
  }

  private void resetFixedFont(Shell shell) {
    fixedFontData =
        new FontData(
            PropsUi.getInstance().getFixedFont().getName(),
            PropsUi.getInstance().getFixedFont().getHeight(),
            PropsUi.getInstance().getFixedFont().getStyle());
    fixedFont.dispose();
    fixedFont = new Font(shell.getDisplay(), fixedFontData);
    wFixedCanvas.redraw();
    saveValues();
  }

  private void editFixedFont(Shell shell) {
    FontDialog fd = new FontDialog(shell);
    fd.setFontList(new FontData[] {fixedFontData});
    FontData newfd = fd.open();
    if (newfd != null) {
      fixedFontData = newfd;
      fixedFont.dispose();
      fixedFont = new Font(shell.getDisplay(), fixedFontData);
      wFixedCanvas.redraw();
      saveValues();
    }
  }

  private void paintFixedFont(PaintEvent pe) {
    pe.gc.setFont(fixedFont);
    Rectangle max = wFixedCanvas.getBounds();
    String name = fixedFontData.getName() + " - " + fixedFontData.getHeight();
    Point size = pe.gc.textExtent(name);

    pe.gc.drawText(name, (max.width - size.x) / 2, (max.height - size.y) / 2, true);
  }

  private void resetDefaultFont(Shell shell) {
    defaultFontData =
        new FontData(
            PropsUi.getInstance().getFixedFont().getName(),
            PropsUi.getInstance().getFixedFont().getHeight(),
            PropsUi.getInstance().getFixedFont().getStyle());
    defaultFont.dispose();
    defaultFont = new Font(shell.getDisplay(), defaultFontData);
    wDefaultCanvas.redraw();
    saveValues();
  }

  private void paintDefaultFont(PaintEvent pe) {
    pe.gc.setFont(defaultFont);
    Rectangle max = wDefaultCanvas.getBounds();
    String name = defaultFontData.getName() + " - " + defaultFontData.getHeight();
    Point size = pe.gc.textExtent(name);

    pe.gc.drawText(name, (max.width - size.x) / 2, (max.height - size.y) / 2, true);
  }

  private void editDefaultFont(Shell shell) {
    FontDialog fd = new FontDialog(shell);
    fd.setFontList(new FontData[] {defaultFontData});
    FontData newfd = fd.open();
    if (newfd != null) {
      defaultFontData = newfd;
      defaultFont.dispose();
      defaultFont = new Font(shell.getDisplay(), defaultFontData);
      wDefaultCanvas.redraw();
      saveValues();
    }
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

  /**
   * Setting the layout of an <i>Edit</i> option button. Either a button image is set - if existing
   * - or a text.
   *
   * @param button The button
   */
  private FormData layoutEditOptionButton(Button button) {
    FormData fd = new FormData();
    Image editButton = GuiResource.getInstance().getImageEdit();
    if (editButton != null) {
      button.setImage(editButton);
      button.setBackground(GuiResource.getInstance().getColorWhite());
      fd.width = editButton.getBounds().width + 20;
      fd.height = editButton.getBounds().height;
    } else {
      button.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.Button.Edit"));
    }

    button.setToolTipText(BaseMessages.getString(PKG, "EnterOptionsDialog.Button.Edit.Tooltip"));
    return fd;
  }

  private void saveValues() {
    PropsUi props = PropsUi.getInstance();

    props.setDefaultFont(defaultFontData);
    props.setFixedFont(fixedFontData);
    props.setGraphFont(graphFontData);
    props.setNoteFont(noteFontData);
    props.setIconSize(Const.toInt(wIconSize.getText(), props.getIconSize()));
    props.setLineWidth(Const.toInt(wLineWidth.getText(), props.getLineWidth()));
    props.setMiddlePct(Const.toInt(wMiddlePct.getText(), props.getMiddlePct()));
    props.setCanvasGridSize(Const.toInt(wGridSize.getText(), 1));
    props.setGlobalZoomFactor(Const.toDouble(wGlobalZoom.getText().replace("%", ""), 100) / 100);
    props.setShowCanvasGridEnabled(wShowCanvasGrid.getSelection());
    props.setHideViewportEnabled(wHideViewport.getSelection());
    props.setDarkMode(wDarkMode.getSelection());
    props.setHidingMenuBar(wHideMenuBar.getSelection());
    props.setShowTableViewToolbar(wShowTableViewToolbar.getSelection());

    int defaultLocaleIndex = wDefaultLocale.getSelectionIndex();
    if (defaultLocaleIndex < 0 || defaultLocaleIndex >= GlobalMessages.localeCodes.length) {
      // Code hardening, when the combo-box ever gets in a strange state,
      // use the first language as default (should be English)
      defaultLocaleIndex = 0;
    }
    String defaultLocale = GlobalMessages.localeCodes[defaultLocaleIndex];
    LanguageChoice.getInstance().setDefaultLocale(EnvUtil.createLocale(defaultLocale));

    try {
      HopConfig.getInstance().saveToFile();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), "Error", "Error saving configuration to file", e);
    }
  }
}
