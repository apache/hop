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
import org.eclipse.swt.widgets.ExpandBar;
import org.eclipse.swt.widgets.ExpandItem;
import org.eclipse.swt.widgets.FontDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

@GuiPlugin
public class ConfigGuiOptionsTab {
  private static final Class<?> PKG = BaseDialog.class;
  public static final String ENTER_OPTIONS_DIALOG_ENTER_NUMBER_HINT =
      "EnterOptionsDialog.EnterNumber.Hint";

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
  private Label wlGridSize; // Label for grid size (for enable/disable)
  private Button wDarkMode;
  private Button wShowCanvasGrid;
  private Button wHideViewport;
  private Button wUseDoubleClick;
  private Button wDrawBorderAroundCanvasNames;
  private Button wHideMenuBar;
  private Button wShowTableViewToolbar;
  private Combo wDefaultLocale;

  private boolean isReloading = false; // Flag to prevent saving during reload
  private boolean isInitializing = false; // Flag to prevent saving during initialization

  public ConfigGuiOptionsTab() {
    // This instance is created in the GuiPlugin system by calling this constructor, after which it
    // calls the addGuiOptionsTab() method.
  }

  /**
   * Reload values from PropsUi into the widgets. This is useful when values are changed outside of
   * the options dialog.
   */
  public void reloadValues() {
    if (wIconSize == null || wIconSize.isDisposed()) {
      return; // Tab not yet initialized or already disposed
    }

    // Set flag to prevent saveValues from being triggered during reload
    isReloading = true;

    try {
      PropsUi props = PropsUi.getInstance();

      // Reload all values from PropsUi
      defaultFontData = props.getDefaultFont();
      fixedFontData = props.getFixedFont();
      graphFontData = props.getGraphFont();
      noteFontData = props.getNoteFont();

      // Recreate fonts
      Shell shell = wIconSize.getShell();
      Display display = shell.getDisplay();
      if (defaultFont != null && !defaultFont.isDisposed()) {
        defaultFont.dispose();
      }
      defaultFont = new Font(display, defaultFontData);
      if (fixedFont != null && !fixedFont.isDisposed()) {
        fixedFont.dispose();
      }
      fixedFont = new Font(display, fixedFontData);
      if (graphFont != null && !graphFont.isDisposed()) {
        graphFont.dispose();
      }
      graphFont = new Font(display, graphFontData);
      if (noteFont != null && !noteFont.isDisposed()) {
        noteFont.dispose();
      }
      noteFont = new Font(display, noteFontData);

      // Redraw canvases
      wDefaultCanvas.redraw();
      wFixedCanvas.redraw();
      wGraphCanvas.redraw();
      wNoteCanvas.redraw();

      // Reload text fields and checkboxes
      wIconSize.setText(Integer.toString(props.getIconSize()));
      wLineWidth.setText(Integer.toString(props.getLineWidth()));
      wMiddlePct.setText(Integer.toString(props.getMiddlePct()));
      wGridSize.setText(Integer.toString(props.getCanvasGridSize()));
      wShowCanvasGrid.setSelection(props.isShowCanvasGridEnabled());

      wHideViewport.setSelection(!props.isHideViewportEnabled()); // Inverted logic
      wUseDoubleClick.setSelection(props.useDoubleClick());
      wDrawBorderAroundCanvasNames.setSelection(props.isBorderDrawnAroundCanvasNames());
      wHideMenuBar.setSelection(props.isHidingMenuBar());
      wShowTableViewToolbar.setSelection(props.isShowTableViewToolbar());
      wDarkMode.setSelection(props.isDarkMode());

      // Reload global zoom
      String globalZoomFactor = Integer.toString((int) (props.getGlobalZoomFactor() * 100)) + '%';
      wGlobalZoom.setText(globalZoomFactor);

      // Reload default locale
      int idxDefault =
          Const.indexOfString(
              LanguageChoice.getInstance().getDefaultLocale().toString(),
              GlobalMessages.localeCodes);
      if (idxDefault >= 0) {
        wDefaultLocale.select(idxDefault);
      }
    } finally {
      // Always reset the flag
      isReloading = false;
    }
  }

  @GuiTab(
      id = "10100-config-perspective-gui-options-tab",
      parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
      description = "GUI options tab")
  public void addGuiOptionsTab(CTabFolder wTabFolder) {
    // Set initialization flag to prevent saving during widget creation
    isInitializing = true;

    Shell shell = wTabFolder.getShell();
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();

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

    // Initialize fonts
    defaultFontData = props.getDefaultFont();
    defaultFont = new Font(shell.getDisplay(), defaultFontData);
    fixedFontData = props.getFixedFont();
    fixedFont = new Font(shell.getDisplay(), fixedFontData);
    graphFontData = props.getGraphFont();
    graphFont = new Font(shell.getDisplay(), graphFontData);
    noteFontData = props.getNoteFont();
    noteFont = new Font(shell.getDisplay(), noteFontData);

    // Track the last control for vertical positioning
    org.eclipse.swt.widgets.Control lastControl = null;

    // Preferred language - at the top
    org.eclipse.swt.widgets.Control[] defaultLocaleControls =
        createComboField(
            wLookComp,
            "EnterOptionsDialog.DefaultLocale.Label",
            null,
            GlobalMessages.localeDescr,
            lastControl,
            margin);
    wDefaultLocale = (Combo) defaultLocaleControls[1];
    int idxDefault =
        Const.indexOfString(
            LanguageChoice.getInstance().getDefaultLocale().toString(), GlobalMessages.localeCodes);
    if (idxDefault >= 0) {
      wDefaultLocale.select(idxDefault);
    }
    lastControl = wDefaultLocale;

    // Hide menu bar - at the top
    wHideMenuBar =
        createCheckbox(
            wLookComp,
            "EnterOptionsDialog.HideMenuBar.Label",
            "EnterOptionsDialog.HideMenuBar.ToolTip",
            props.isHidingMenuBar(),
            lastControl,
            margin);
    lastControl = wHideMenuBar;

    // Dark mode (Windows only)
    wDarkMode =
        createCheckbox(
            wLookComp,
            "EnterOptionsDialog.DarkMode.Label",
            null,
            props.isDarkMode(),
            lastControl,
            margin);
    wDarkMode.setEnabled(Const.isWindows());
    lastControl = wDarkMode;

    // General appearance section - using ExpandBar
    ExpandBar appearanceExpandBar = new ExpandBar(wLookComp, SWT.V_SCROLL);
    PropsUi.setLook(appearanceExpandBar);

    FormData fdAppearanceExpandBar = new FormData();
    fdAppearanceExpandBar.left = new FormAttachment(0, 0);
    fdAppearanceExpandBar.right = new FormAttachment(100, 0);
    fdAppearanceExpandBar.top = new FormAttachment(lastControl, 2 * margin);
    appearanceExpandBar.setLayoutData(fdAppearanceExpandBar);

    // Create expandable item for appearance
    Composite appearanceContent = new Composite(appearanceExpandBar, SWT.NONE);
    PropsUi.setLook(appearanceContent);
    FormLayout appearanceLayout = new FormLayout();
    appearanceLayout.marginWidth = PropsUi.getFormMargin();
    appearanceLayout.marginHeight = PropsUi.getFormMargin();
    appearanceContent.setLayout(appearanceLayout);

    // Appearance controls inside the expandable content
    org.eclipse.swt.widgets.Control lastAppearanceControl = null;

    // Global zoom (at the top)
    org.eclipse.swt.widgets.Control[] globalZoomControls =
        createComboField(
            appearanceContent,
            "EnterOptionsDialog.GlobalZoom.Label",
            null,
            PropsUi.getGlobalZoomFactorLevels(),
            lastAppearanceControl,
            margin);
    wGlobalZoom = (Combo) globalZoomControls[1];
    String globalZoomFactor = Integer.toString((int) (props.getGlobalZoomFactor() * 100)) + '%';
    wGlobalZoom.setText(globalZoomFactor);
    lastAppearanceControl = wGlobalZoom;

    // Icon size
    org.eclipse.swt.widgets.Control[] iconSizeControls =
        createTextField(
            appearanceContent,
            "EnterOptionsDialog.IconSize.Label",
            null,
            Integer.toString(props.getIconSize()),
            lastAppearanceControl,
            margin);
    wIconSize = (Text) iconSizeControls[1];
    wIconSize.setMessage(BaseMessages.getString(PKG, ENTER_OPTIONS_DIALOG_ENTER_NUMBER_HINT));
    wIconSize.addListener(
        SWT.Verify,
        e -> {
          String currentText = ((Text) e.widget).getText();
          String newText =
              currentText.substring(0, e.start) + e.text + currentText.substring(e.end);
          if (!newText.isEmpty() && !newText.matches("\\d+")) {
            e.doit = false;
          }
        });
    lastAppearanceControl = wIconSize;

    // Line width
    org.eclipse.swt.widgets.Control[] lineWidthControls =
        createTextField(
            appearanceContent,
            "EnterOptionsDialog.LineWidth.Label",
            null,
            Integer.toString(props.getLineWidth()),
            lastAppearanceControl,
            margin);
    wLineWidth = (Text) lineWidthControls[1];
    wLineWidth.setMessage(BaseMessages.getString(PKG, ENTER_OPTIONS_DIALOG_ENTER_NUMBER_HINT));
    wLineWidth.addListener(
        SWT.Verify,
        e -> {
          String currentText = ((Text) e.widget).getText();
          String newText =
              currentText.substring(0, e.start) + e.text + currentText.substring(e.end);
          if (!newText.isEmpty() && !newText.matches("\\d+")) {
            e.doit = false;
          }
        });
    lastAppearanceControl = wLineWidth;

    // Dialog middle percentage
    org.eclipse.swt.widgets.Control[] middlePctControls =
        createTextField(
            appearanceContent,
            "EnterOptionsDialog.DialogMiddlePercentage.Label",
            null,
            Integer.toString(props.getMiddlePct()),
            lastAppearanceControl,
            margin);
    wMiddlePct = (Text) middlePctControls[1];
    wMiddlePct.setMessage(BaseMessages.getString(PKG, ENTER_OPTIONS_DIALOG_ENTER_NUMBER_HINT));
    wMiddlePct.addListener(
        SWT.Verify,
        e -> {
          String currentText = ((Text) e.widget).getText();
          String newText =
              currentText.substring(0, e.start) + e.text + currentText.substring(e.end);
          if (!newText.isEmpty() && !newText.matches("\\d+")) {
            e.doit = false;
          }
        });

    // Create the general appearance expand item
    ExpandItem appearanceItem = new ExpandItem(appearanceExpandBar, SWT.NONE);
    appearanceItem.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.Section.GeneralAppearance"));
    appearanceItem.setControl(appearanceContent);
    appearanceItem.setHeight(appearanceContent.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
    appearanceItem.setExpanded(true); // Start expanded

    // Add expand/collapse listeners for space reclamation
    appearanceExpandBar.addListener(
        SWT.Expand,
        e ->
            Display.getDefault()
                .asyncExec(
                    () -> {
                      if (!wLookComp.isDisposed() && !sLookComp.isDisposed()) {
                        wLookComp.layout();
                        sLookComp.setMinHeight(wLookComp.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                      }
                    }));
    appearanceExpandBar.addListener(
        SWT.Collapse,
        e ->
            Display.getDefault()
                .asyncExec(
                    () -> {
                      if (!wLookComp.isDisposed() && !sLookComp.isDisposed()) {
                        wLookComp.layout();
                        sLookComp.setMinHeight(wLookComp.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                      }
                    }));

    lastControl = appearanceExpandBar;

    // Fonts section - using ExpandBar
    ExpandBar fontsExpandBar = new ExpandBar(wLookComp, SWT.V_SCROLL);
    PropsUi.setLook(fontsExpandBar);

    FormData fdFontsExpandBar = new FormData();
    fdFontsExpandBar.left = new FormAttachment(0, 0);
    fdFontsExpandBar.right = new FormAttachment(100, 0);
    fdFontsExpandBar.top = new FormAttachment(lastControl, 2 * margin);
    fontsExpandBar.setLayoutData(fdFontsExpandBar);

    // Create expandable item for fonts
    Composite fontsContent = new Composite(fontsExpandBar, SWT.NONE);
    PropsUi.setLook(fontsContent);
    FormLayout fontsLayout = new FormLayout();
    fontsLayout.marginWidth = PropsUi.getFormMargin();
    fontsLayout.marginHeight = PropsUi.getFormMargin();
    fontsContent.setLayout(fontsLayout);

    // Fonts inside the expandable content
    org.eclipse.swt.widgets.Control lastFontControl = null;

    // Default font
    org.eclipse.swt.widgets.Control[] defaultFontControls =
        createFontPicker(
            fontsContent, "EnterOptionsDialog.DefaultFont.Label", shell, lastFontControl, margin);
    wDefaultCanvas = (Canvas) defaultFontControls[0];
    wDefaultCanvas.addPaintListener(this::paintDefaultFont);
    wDefaultCanvas.addListener(SWT.MouseDown, e -> editDefaultFont(shell));
    Button wbDefaultFont = (Button) defaultFontControls[1];
    wbDefaultFont.addListener(SWT.Selection, e -> editDefaultFont(shell));
    Button wdDefaultFont = (Button) defaultFontControls[2];
    wdDefaultFont.addListener(SWT.Selection, e -> resetDefaultFont(shell));
    lastFontControl = wDefaultCanvas;

    // Fixed width font
    org.eclipse.swt.widgets.Control[] fixedFontControls =
        createFontPicker(
            fontsContent,
            "EnterOptionsDialog.FixedWidthFont.Label",
            shell,
            lastFontControl,
            margin);
    wFixedCanvas = (Canvas) fixedFontControls[0];
    wFixedCanvas.addPaintListener(this::paintFixedFont);
    wFixedCanvas.addListener(SWT.MouseDown, e -> editFixedFont(shell));
    Button wbFixedFont = (Button) fixedFontControls[1];
    wbFixedFont.addListener(SWT.Selection, e -> editFixedFont(shell));
    Button wdFixedFont = (Button) fixedFontControls[2];
    wdFixedFont.addListener(SWT.Selection, e -> resetFixedFont(shell));
    lastFontControl = wFixedCanvas;

    // Graph font
    org.eclipse.swt.widgets.Control[] graphFontControls =
        createFontPicker(
            fontsContent, "EnterOptionsDialog.GraphFont.Label", shell, lastFontControl, margin);
    wGraphCanvas = (Canvas) graphFontControls[0];
    wGraphCanvas.addPaintListener(this::drawGraphFont);
    wGraphCanvas.addListener(SWT.MouseDown, e -> editGraphFont(shell));
    Button wbGraphFont = (Button) graphFontControls[1];
    wbGraphFont.addListener(SWT.Selection, e -> editGraphFont(shell));
    Button wdGraphFont = (Button) graphFontControls[2];
    wdGraphFont.addListener(SWT.Selection, e -> resetGraphFont(shell, props));
    lastFontControl = wGraphCanvas;

    // Note font
    org.eclipse.swt.widgets.Control[] noteFontControls =
        createFontPicker(
            fontsContent, "EnterOptionsDialog.NoteFont.Label", shell, lastFontControl, margin);
    wNoteCanvas = (Canvas) noteFontControls[0];
    wNoteCanvas.addPaintListener(this::paintNoteFont);
    wNoteCanvas.addListener(SWT.MouseDown, e -> editNoteFont(shell));
    Button wbNoteFont = (Button) noteFontControls[1];
    wbNoteFont.addListener(SWT.Selection, e -> editNoteFont(shell));
    Button wdNoteFont = (Button) noteFontControls[2];
    wdNoteFont.addListener(SWT.Selection, e -> resetNoteFont(e, props, shell.getDisplay()));

    // Create the fonts expand item
    ExpandItem fontsItem = new ExpandItem(fontsExpandBar, SWT.NONE);
    fontsItem.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.Section.Fonts"));
    fontsItem.setControl(fontsContent);
    fontsItem.setHeight(fontsContent.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
    fontsItem.setExpanded(true); // Start expanded

    // Add expand/collapse listeners for space reclamation
    fontsExpandBar.addListener(
        SWT.Expand,
        e ->
            Display.getDefault()
                .asyncExec(
                    () -> {
                      if (!wLookComp.isDisposed() && !sLookComp.isDisposed()) {
                        wLookComp.layout();
                        sLookComp.setMinHeight(wLookComp.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                      }
                    }));
    fontsExpandBar.addListener(
        SWT.Collapse,
        e ->
            Display.getDefault()
                .asyncExec(
                    () -> {
                      if (!wLookComp.isDisposed() && !sLookComp.isDisposed()) {
                        wLookComp.layout();
                        sLookComp.setMinHeight(wLookComp.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                      }
                    }));

    lastControl = fontsExpandBar;

    // Pipeline & Workflow canvas section - using ExpandBar
    ExpandBar canvasExpandBar = new ExpandBar(wLookComp, SWT.V_SCROLL);
    PropsUi.setLook(canvasExpandBar);

    FormData fdCanvasExpandBar = new FormData();
    fdCanvasExpandBar.left = new FormAttachment(0, 0);
    fdCanvasExpandBar.right = new FormAttachment(100, 0);
    fdCanvasExpandBar.top = new FormAttachment(lastControl, 2 * margin);
    canvasExpandBar.setLayoutData(fdCanvasExpandBar);

    // Create expandable item for canvas settings
    Composite canvasContent = new Composite(canvasExpandBar, SWT.NONE);
    PropsUi.setLook(canvasContent);
    FormLayout canvasLayout = new FormLayout();
    canvasLayout.marginWidth = PropsUi.getFormMargin();
    canvasLayout.marginHeight = PropsUi.getFormMargin();
    canvasContent.setLayout(canvasLayout);

    // Show canvas grid checkbox inside the expandable content
    org.eclipse.swt.widgets.Control lastCanvasControl = null;
    wShowCanvasGrid =
        createCheckbox(
            canvasContent,
            "EnterOptionsDialog.ShowCanvasGrid.Label",
            "EnterOptionsDialog.ShowCanvasGrid.ToolTip",
            props.isShowCanvasGridEnabled(),
            lastCanvasControl,
            margin);
    lastCanvasControl = wShowCanvasGrid;

    // Grid size - placed under Show canvas grid checkbox
    org.eclipse.swt.widgets.Control[] gridSizeControls =
        createTextField(
            canvasContent,
            "EnterOptionsDialog.GridSize.Label",
            "EnterOptionsDialog.GridSize.ToolTip",
            Integer.toString(props.getCanvasGridSize()),
            lastCanvasControl,
            margin);
    wGridSize = (Text) gridSizeControls[1];
    wlGridSize = (Label) gridSizeControls[0];
    wGridSize.setMessage(BaseMessages.getString(PKG, ENTER_OPTIONS_DIALOG_ENTER_NUMBER_HINT));
    wGridSize.addListener(
        SWT.Verify,
        e -> {
          String currentText = ((Text) e.widget).getText();
          String newText =
              currentText.substring(0, e.start) + e.text + currentText.substring(e.end);
          if (!newText.isEmpty() && !newText.matches("\\d+")) {
            e.doit = false;
          }
        });

    lastCanvasControl = wGridSize;

    // Show viewport checkbox (inverted logic from hideViewport)
    wHideViewport =
        createCheckbox(
            canvasContent,
            "EnterOptionsDialog.ShowViewport.Label",
            "EnterOptionsDialog.ShowViewport.ToolTip",
            !props.isHideViewportEnabled(),
            lastCanvasControl,
            margin);
    lastCanvasControl = wHideViewport;

    // Use double click on canvas
    wUseDoubleClick =
        createCheckbox(
            canvasContent,
            "EnterOptionsDialog.UseDoubleClickOnCanvas.Label",
            null,
            props.useDoubleClick(),
            lastCanvasControl,
            margin);
    lastCanvasControl = wUseDoubleClick;

    // Draw border around canvas names
    wDrawBorderAroundCanvasNames =
        createCheckbox(
            canvasContent,
            "EnterOptionsDialog.DrawBorderAroundCanvasNamesOnCanvas.Label",
            null,
            props.isBorderDrawnAroundCanvasNames(),
            lastCanvasControl,
            margin);

    // Create the expand item
    ExpandItem canvasItem = new ExpandItem(canvasExpandBar, SWT.NONE);
    canvasItem.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.Section.PipelineWorkflowCanvas"));
    canvasItem.setControl(canvasContent);
    canvasItem.setHeight(canvasContent.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
    canvasItem.setExpanded(true); // Start expanded

    // Add expand/collapse listeners for space reclamation
    canvasExpandBar.addListener(
        SWT.Expand,
        e ->
            Display.getDefault()
                .asyncExec(
                    () -> {
                      if (!wLookComp.isDisposed() && !sLookComp.isDisposed()) {
                        wLookComp.layout();
                        sLookComp.setMinHeight(wLookComp.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                      }
                    }));
    canvasExpandBar.addListener(
        SWT.Collapse,
        e ->
            Display.getDefault()
                .asyncExec(
                    () -> {
                      if (!wLookComp.isDisposed() && !sLookComp.isDisposed()) {
                        wLookComp.layout();
                        sLookComp.setMinHeight(wLookComp.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                      }
                    }));

    lastControl = canvasExpandBar;

    // Tables & grids section - using ExpandBar
    ExpandBar tablesExpandBar = new ExpandBar(wLookComp, SWT.V_SCROLL);
    PropsUi.setLook(tablesExpandBar);

    FormData fdTablesExpandBar = new FormData();
    fdTablesExpandBar.left = new FormAttachment(0, 0);
    fdTablesExpandBar.right = new FormAttachment(100, 0);
    fdTablesExpandBar.top = new FormAttachment(lastControl, 2 * margin);
    tablesExpandBar.setLayoutData(fdTablesExpandBar);

    // Create expandable item for tables & grids
    Composite tablesContent = new Composite(tablesExpandBar, SWT.NONE);
    PropsUi.setLook(tablesContent);
    FormLayout tablesLayout = new FormLayout();
    tablesLayout.marginWidth = PropsUi.getFormMargin();
    tablesLayout.marginHeight = PropsUi.getFormMargin();
    tablesContent.setLayout(tablesLayout);

    // Show toolbar checkbox inside the expandable content
    org.eclipse.swt.widgets.Control lastTablesControl = null;
    wShowTableViewToolbar =
        createCheckbox(
            tablesContent,
            "EnterOptionsDialog.ShowTableViewToolbar.Label",
            "EnterOptionsDialog.ShowTableViewToolbar.ToolTip",
            props.isShowTableViewToolbar(),
            lastTablesControl,
            margin);

    // Create the expand item
    ExpandItem tablesItem = new ExpandItem(tablesExpandBar, SWT.NONE);
    tablesItem.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.Section.TablesGrids"));
    tablesItem.setControl(tablesContent);
    tablesItem.setHeight(tablesContent.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
    tablesItem.setExpanded(true); // Start expanded

    // Add expand/collapse listeners for space reclamation
    tablesExpandBar.addListener(
        SWT.Expand,
        e ->
            Display.getDefault()
                .asyncExec(
                    () -> {
                      if (!wLookComp.isDisposed() && !sLookComp.isDisposed()) {
                        wLookComp.layout();
                        sLookComp.setMinHeight(wLookComp.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                      }
                    }));
    tablesExpandBar.addListener(
        SWT.Collapse,
        e ->
            Display.getDefault()
                .asyncExec(
                    () -> {
                      if (!wLookComp.isDisposed() && !sLookComp.isDisposed()) {
                        wLookComp.layout();
                        sLookComp.setMinHeight(wLookComp.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                      }
                    }));

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

    // Reset initialization flag so saveValues can work normally
    isInitializing = false;
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
    // Don't save if we're currently reloading values or initializing widgets
    if (isReloading || isInitializing) {
      return;
    }

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
    props.setHideViewportEnabled(
        !wHideViewport.getSelection()); // Inverted: checkbox is "show", property is "hide"
    props.setUseDoubleClickOnCanvas(wUseDoubleClick.getSelection());
    props.setDrawBorderAroundCanvasNames(wDrawBorderAroundCanvasNames.getSelection());
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

  /**
   * Creates a text field with label above it.
   *
   * @param parent The parent composite
   * @param labelKey The message key for the label text
   * @param tooltipKey Optional tooltip message key (can be null)
   * @param initialValue The initial text value
   * @param lastControl The last control to attach to
   * @param margin The margin to use
   * @return An array containing [Label, Text] controls
   */
  private org.eclipse.swt.widgets.Control[] createTextField(
      Composite parent,
      String labelKey,
      String tooltipKey,
      String initialValue,
      org.eclipse.swt.widgets.Control lastControl,
      int margin) {
    // Label above
    Label label = new Label(parent, SWT.LEFT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));

    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdLabel.top = new FormAttachment(lastControl, margin);
    } else {
      fdLabel.top = new FormAttachment(0, margin);
    }
    label.setLayoutData(fdLabel);

    // Text field below label
    Text text = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    text.setText(initialValue);
    if (tooltipKey != null) {
      text.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    }
    text.addListener(SWT.Modify, e -> saveValues());

    FormData fdText = new FormData();
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.top = new FormAttachment(label, margin / 2);
    text.setLayoutData(fdText);

    return new org.eclipse.swt.widgets.Control[] {label, text};
  }

  /**
   * Creates a combo field with label above it.
   *
   * @param parent The parent composite
   * @param labelKey The message key for the label text
   * @param tooltipKey Optional tooltip message key (can be null)
   * @param items The items for the combo
   * @param lastControl The last control to attach to
   * @param margin The margin to use
   * @return An array containing [Label, Combo] controls
   */
  private org.eclipse.swt.widgets.Control[] createComboField(
      Composite parent,
      String labelKey,
      String tooltipKey,
      String[] items,
      org.eclipse.swt.widgets.Control lastControl,
      int margin) {
    // Label above
    Label label = new Label(parent, SWT.LEFT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));

    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdLabel.top = new FormAttachment(lastControl, margin);
    } else {
      fdLabel.top = new FormAttachment(0, margin);
    }
    label.setLayoutData(fdLabel);

    // Combo field below label
    Combo combo = new Combo(parent, SWT.SINGLE | SWT.READ_ONLY | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(combo);
    combo.setItems(items);
    if (tooltipKey != null) {
      combo.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    }
    combo.addListener(SWT.Modify, e -> saveValues());

    FormData fdCombo = new FormData();
    fdCombo.left = new FormAttachment(0, 0);
    fdCombo.right = new FormAttachment(100, 0);
    fdCombo.top = new FormAttachment(label, margin / 2);
    combo.setLayoutData(fdCombo);

    return new org.eclipse.swt.widgets.Control[] {label, combo};
  }

  /**
   * Creates a checkbox with the checkbox in front of the label text.
   *
   * @param parent The parent composite
   * @param labelKey The message key for the label text
   * @param tooltipKey Optional tooltip message key (can be null)
   * @param selected Whether the checkbox is initially selected
   * @param lastControl The last control to attach to
   * @param margin The margin to use
   * @return The created Button (checkbox)
   */
  private Button createCheckbox(
      Composite parent,
      String labelKey,
      String tooltipKey,
      boolean selected,
      org.eclipse.swt.widgets.Control lastControl,
      int margin) {
    Button checkbox = new Button(parent, SWT.CHECK);
    PropsUi.setLook(checkbox);
    checkbox.setText(BaseMessages.getString(PKG, labelKey));
    if (tooltipKey != null) {
      checkbox.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    }
    checkbox.setSelection(selected);
    checkbox.addListener(SWT.Selection, e -> saveValues());

    FormData fdCheckbox = new FormData();
    fdCheckbox.left = new FormAttachment(0, 0);
    fdCheckbox.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdCheckbox.top = new FormAttachment(lastControl, margin);
    } else {
      fdCheckbox.top = new FormAttachment(0, margin);
    }
    checkbox.setLayoutData(fdCheckbox);

    return checkbox;
  }

  /**
   * Creates a font picker with label above, canvas preview, and edit/reset buttons.
   *
   * @param parent The parent composite
   * @param labelKey The message key for the label text
   * @param shell The shell for opening dialogs
   * @param lastControl The last control to attach to
   * @param margin The margin to use
   * @return An array containing [Canvas, EditButton, ResetButton] controls
   */
  private org.eclipse.swt.widgets.Control[] createFontPicker(
      Composite parent,
      String labelKey,
      Shell shell,
      org.eclipse.swt.widgets.Control lastControl,
      int margin) {
    int h = (int) (40 * PropsUi.getInstance().getZoomFactor());

    // Label above
    Label label = new Label(parent, SWT.LEFT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));

    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdLabel.top = new FormAttachment(lastControl, margin);
    } else {
      fdLabel.top = new FormAttachment(0, margin);
    }
    label.setLayoutData(fdLabel);

    // Reset button (right)
    Button resetButton = new Button(parent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(resetButton);
    FormData fdResetButton = layoutResetOptionButton(resetButton);
    fdResetButton.right = new FormAttachment(100, 0);
    fdResetButton.top = new FormAttachment(label, margin / 2);
    fdResetButton.height = h; // Match canvas height
    resetButton.setLayoutData(fdResetButton);

    // Edit button (next to reset button)
    Button editButton = new Button(parent, SWT.PUSH);
    PropsUi.setLook(editButton);
    FormData fdEditButton = layoutEditOptionButton(editButton);
    fdEditButton.right = new FormAttachment(resetButton, -margin);
    fdEditButton.top = new FormAttachment(label, margin / 2);
    fdEditButton.height = h; // Match canvas height
    editButton.setLayoutData(fdEditButton);

    // Canvas preview (left side)
    Canvas canvas = new Canvas(parent, SWT.BORDER);
    PropsUi.setLook(canvas);
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment(0, 0);
    fdCanvas.right = new FormAttachment(editButton, -margin);
    fdCanvas.top = new FormAttachment(label, margin / 2);
    fdCanvas.height = h;
    canvas.setLayoutData(fdCanvas);

    return new org.eclipse.swt.widgets.Control[] {canvas, editButton, resetButton};
  }
}
