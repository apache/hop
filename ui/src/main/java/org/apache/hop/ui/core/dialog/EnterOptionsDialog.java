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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.i18n.GlobalMessages;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.ColorDialog;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FontDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Allows you to set the configurable options for the Hop environment
 *
 * @author Matt
 * @since 15-12-2003
 */
public class EnterOptionsDialog extends Dialog {
  private static final Class<?> PKG = EnterOptionsDialog.class; // For Translator

  public static final String GUI_WIDGETS_PARENT_ID = "EnterOptionsDialog-GuiWidgetsParent";

  public static final String STRING_USAGE_WARNING_PARAMETER = "EnterOptionsRestartWarning";

  private Display display;

  private CTabFolder wTabFolder;

  private FontData fixedFontData, graphFontData, noteFontData;
  private Font fixedFont, graphFont, noteFont;
  private RGB backgroundRGB, graphColorRGB, tabColorRGB;
  private Color background, graphColor, tabColor;

  private Canvas wFFont;

  private Canvas wGFont;

  private Canvas wNFont;

  private Canvas wBGColor;

  private Canvas wGrColor;

  private Canvas wTabColor;

  private Text wFilename;

  private Text wIconSize;

  private Text wLineWidth;

  private Text wDefaultPreview;

  private Text wMiddlePct;

  private Text wGridSize;

  private Button wOriginalLook;

  private Button wDarkMode;

  private Button wShowCanvasGrid;

  private Button wUseCache;

  private Button wOpenLast;

  private Button wAutoSave;

  private Button wAutoSplit;

  private Button wCopyDistrib;

  private Button wExitWarning;

  private Combo wDefaultLocale;

  private Shell shell;

  private PropsUi props;

  private int middle;

  private int margin;

  private Button wToolTip;

  private Button wHelpTip;

  private Button wbUseDoubleClick;

  private Button wbUseGlobalFileBookmarks;

  private Button wAutoCollapse;

  private class PluginWidgetContents {
    public GuiCompositeWidgets compositeWidgets;
    public Object sourceData;

    public PluginWidgetContents(GuiCompositeWidgets compositeWidgets, Object sourceData) {
      this.compositeWidgets = compositeWidgets;
      this.sourceData = sourceData;
    }
  }

  private List<PluginWidgetContents> pluginWidgetContentsList;

  public EnterOptionsDialog(Shell parent) {
    super(parent, SWT.NONE);
    props = PropsUi.getInstance();
    pluginWidgetContentsList = new ArrayList<>();
  }

  public Props open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    getData();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.SHEET | SWT.RESIZE);
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.Title"));

    middle = props.getMiddlePct();
    margin = props.getMargin();

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addGeneralTab();
    addLookTab();
    addPluginTabs();

    // Some buttons
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // ///////////////////////////////////////////////////////////
    // / END OF TABS
    // ///////////////////////////////////////////////////////////

    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOk = e -> ok();

    wOk.addListener(SWT.Selection, lsOk);
    wCancel.addListener(SWT.Selection, lsCancel);

    SelectionAdapter lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };
    wIconSize.addSelectionListener(lsDef);
    wLineWidth.addSelectionListener(lsDef);
    wMiddlePct.addSelectionListener(lsDef);
    wDefaultPreview.addSelectionListener(lsDef);
    wGridSize.addSelectionListener(lsDef);

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    wTabFolder.setSelection(0);

    BaseTransformDialog.setSize(shell);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return props;
  }

  private void addLookTab() {
    int h = 40;

    // ////////////////////////
    // START OF LOOK TAB///
    // /
    CTabItem wLookTab = new CTabItem(wTabFolder, SWT.NONE);
    wLookTab.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.LookAndFeel.Label"));

    ScrolledComposite sLookComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    sLookComp.setLayout(new FillLayout());

    Composite wLookComp = new Composite(sLookComp, SWT.NONE);
    props.setLook(wLookComp);

    FormLayout lookLayout = new FormLayout();
    lookLayout.marginWidth = 3;
    lookLayout.marginHeight = 3;
    wLookComp.setLayout(lookLayout);

    // Fixed font
    int nr = 0;
    Label wlFFont = new Label(wLookComp, SWT.RIGHT);
    wlFFont.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.FixedWidthFont.Label"));
    props.setLook(wlFFont);
    FormData fdlFFont = new FormData();
    fdlFFont.left = new FormAttachment(0, 0);
    fdlFFont.right = new FormAttachment(middle, -margin);
    fdlFFont.top = new FormAttachment(0, nr * h + margin + 10);
    wlFFont.setLayoutData(fdlFFont);

    Button wdFFont = new Button(wLookComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wdFFont);
    FormData fddFFont = layoutResetOptionButton(wdFFont);
    fddFFont.right = new FormAttachment(100, 0);
    fddFFont.top = new FormAttachment(0, nr * h + margin);
    fddFFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);

    wdFFont.setLayoutData(fddFFont);
    wdFFont.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            fixedFontData =
                new FontData(
                    PropsUi.getInstance().getFixedFont().getName(),
                    PropsUi.getInstance().getFixedFont().getHeight(),
                    PropsUi.getInstance().getFixedFont().getStyle());
            fixedFont.dispose();
            fixedFont = new Font(display, fixedFontData);
            wFFont.redraw();
          }
        });

    Button wbFFont = new Button(wLookComp, SWT.PUSH);
    props.setLook(wbFFont);

    FormData fdbFFont = layoutEditOptionButton(wbFFont);
    fdbFFont.right = new FormAttachment(wdFFont, -margin);
    fdbFFont.top = new FormAttachment(0, nr * h + margin);
    fdbFFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wbFFont.setLayoutData(fdbFFont);
    wbFFont.addListener(
        SWT.Selection,
        e -> {
          FontDialog fd = new FontDialog(shell);
          fd.setFontList(new FontData[] {fixedFontData});
          FontData newfd = fd.open();
          if (newfd != null) {
            fixedFontData = newfd;
            fixedFont.dispose();
            fixedFont = new Font(display, fixedFontData);
            wFFont.redraw();
          }
        });

    wFFont = new Canvas(wLookComp, SWT.BORDER);
    props.setLook(wFFont);
    FormData fdFFont = new FormData();
    fdFFont.left = new FormAttachment(middle, 0);
    fdFFont.right = new FormAttachment(wbFFont, -margin);
    fdFFont.top = new FormAttachment(0, margin);
    fdFFont.bottom = new FormAttachment(0, h);
    wFFont.setLayoutData(fdFFont);
    wFFont.addPaintListener(
        pe -> {
          pe.gc.setFont(fixedFont);
          Rectangle max = wFFont.getBounds();
          String name = fixedFontData.getName() + " - " + fixedFontData.getHeight();
          Point size = pe.gc.textExtent(name);

          pe.gc.drawText(name, (max.width - size.x) / 2, (max.height - size.y) / 2, true);
        });

    // Graph font
    nr++;
    Label wlGFont = new Label(wLookComp, SWT.RIGHT);
    wlGFont.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.GraphFont.Label"));
    props.setLook(wlGFont);
    FormData fdlGFont = new FormData();
    fdlGFont.left = new FormAttachment(0, 0);
    fdlGFont.right = new FormAttachment(middle, -margin);
    fdlGFont.top = new FormAttachment(0, nr * h + margin + 10);
    wlGFont.setLayoutData(fdlGFont);

    Button wdGFont = new Button(wLookComp, SWT.PUSH);
    props.setLook(wdGFont);

    FormData fddGFont = layoutResetOptionButton(wdGFont);
    fddGFont.right = new FormAttachment(100, 0);
    fddGFont.top = new FormAttachment(0, nr * h + margin);
    fddGFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wdGFont.setLayoutData(fddGFont);
    wdGFont.addListener(
        SWT.Selection,
        e -> {
          graphFont.dispose();

          graphFontData = props.getDefaultFontData();
          graphFont = new Font(display, graphFontData);
          wGFont.redraw();
        });

    Button wbGFont = new Button(wLookComp, SWT.PUSH);
    props.setLook(wbGFont);

    FormData fdbGFont = layoutEditOptionButton(wbGFont);
    fdbGFont.right = new FormAttachment(wdGFont, -margin);
    fdbGFont.top = new FormAttachment(0, nr * h + margin);
    fdbGFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wbGFont.setLayoutData(fdbGFont);
    wbGFont.addListener(
        SWT.Selection,
        e -> {
          FontDialog fd = new FontDialog(shell);
          fd.setFontList(new FontData[] {graphFontData});
          FontData newfd = fd.open();
          if (newfd != null) {
            graphFontData = newfd;
            graphFont.dispose();
            graphFont = new Font(display, graphFontData);
            wGFont.redraw();
          }
        });

    wGFont = new Canvas(wLookComp, SWT.BORDER);
    props.setLook(wGFont);
    FormData fdGFont = new FormData();
    fdGFont.left = new FormAttachment(middle, 0);
    fdGFont.right = new FormAttachment(wbGFont, -margin);
    fdGFont.top = new FormAttachment(0, nr * h + margin);
    fdGFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wGFont.setLayoutData(fdGFont);
    wGFont.addPaintListener(
        pe -> {
          pe.gc.setFont(graphFont);
          Rectangle max = wGFont.getBounds();
          String name = graphFontData.getName() + " - " + graphFontData.getHeight();
          Point size = pe.gc.textExtent(name);

          pe.gc.drawText(name, (max.width - size.x) / 2, (max.height - size.y) / 2, true);
        });

    // Note font
    nr++;
    Label wlNFont = new Label(wLookComp, SWT.RIGHT);
    wlNFont.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.NoteFont.Label"));
    props.setLook(wlNFont);
    FormData fdlNFont = new FormData();
    fdlNFont.left = new FormAttachment(0, 0);
    fdlNFont.right = new FormAttachment(middle, -margin);
    fdlNFont.top = new FormAttachment(0, nr * h + margin + 10);
    wlNFont.setLayoutData(fdlNFont);

    Button wdNFont = new Button(wLookComp, SWT.PUSH);
    props.setLook(wdNFont);

    FormData fddNFont = layoutResetOptionButton(wdNFont);
    fddNFont.right = new FormAttachment(100, 0);
    fddNFont.top = new FormAttachment(0, nr * h + margin);
    fddNFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wdNFont.setLayoutData(fddNFont);
    wdNFont.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            noteFontData = props.getDefaultFontData();
            noteFont.dispose();
            noteFont = new Font(display, noteFontData);
            wNFont.redraw();
          }
        });

    Button wbNFont = new Button(wLookComp, SWT.PUSH);
    props.setLook(wbNFont);

    FormData fdbNFont = layoutEditOptionButton(wbNFont);
    fdbNFont.right = new FormAttachment(wdNFont, -margin);
    fdbNFont.top = new FormAttachment(0, nr * h + margin);
    fdbNFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wbNFont.setLayoutData(fdbNFont);
    wbNFont.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            FontDialog fd = new FontDialog(shell);
            fd.setFontList(new FontData[] {noteFontData});
            FontData newfd = fd.open();
            if (newfd != null) {
              noteFontData = newfd;
              noteFont.dispose();
              noteFont = new Font(display, noteFontData);
              wNFont.redraw();
            }
          }
        });

    wNFont = new Canvas(wLookComp, SWT.BORDER);
    props.setLook(wNFont);
    FormData fdNFont = new FormData();
    fdNFont.left = new FormAttachment(middle, 0);
    fdNFont.right = new FormAttachment(wbNFont, -margin);
    fdNFont.top = new FormAttachment(0, nr * h + margin);
    fdNFont.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wNFont.setLayoutData(fdNFont);
    wNFont.addPaintListener(
        pe -> {
          pe.gc.setFont(noteFont);
          Rectangle max = wNFont.getBounds();
          String name = noteFontData.getName() + " - " + noteFontData.getHeight();
          Point size = pe.gc.textExtent(name);

          pe.gc.drawText(name, (max.width - size.x) / 2, (max.height - size.y) / 2, true);
        });

    // Background color
    nr++;
    Label wlBGColor = new Label(wLookComp, SWT.RIGHT);
    wlBGColor.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.BackgroundColor.Label"));
    props.setLook(wlBGColor);
    FormData fdlBGColor = new FormData();
    fdlBGColor.left = new FormAttachment(0, 0);
    fdlBGColor.right = new FormAttachment(middle, -margin);
    fdlBGColor.top = new FormAttachment(0, nr * h + margin + 10);
    wlBGColor.setLayoutData(fdlBGColor);

    Button wdBGcolor = new Button(wLookComp, SWT.PUSH);
    props.setLook(wdBGcolor);

    FormData fddBGColor = layoutResetOptionButton(wdBGcolor);
    fddBGColor.right = new FormAttachment(100, 0); // to the right of the
    // dialog
    fddBGColor.top = new FormAttachment(0, nr * h + margin);
    fddBGColor.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wdBGcolor.setLayoutData(fddBGColor);
    wdBGcolor.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            background.dispose();

            backgroundRGB =
                new RGB(
                    ConstUi.COLOR_BACKGROUND_RED,
                    ConstUi.COLOR_BACKGROUND_GREEN,
                    ConstUi.COLOR_BACKGROUND_BLUE);
            background = new Color(display, backgroundRGB);
            wBGColor.setBackground(background);
            wBGColor.redraw();
          }
        });

    Button wbBGColor = new Button(wLookComp, SWT.PUSH);
    props.setLook(wbBGColor);

    FormData fdbBGColor = layoutEditOptionButton(wbBGColor);
    fdbBGColor.right = new FormAttachment(wdBGcolor, -margin); // to the
    // left of
    // the
    // "default"
    // button
    fdbBGColor.top = new FormAttachment(0, nr * h + margin);
    fdbBGColor.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wbBGColor.setLayoutData(fdbBGColor);
    wbBGColor.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            ColorDialog cd = new ColorDialog(shell);
            cd.setRGB(props.getBackgroundRGB());
            RGB newbg = cd.open();
            if (newbg != null) {
              backgroundRGB = newbg;
              background.dispose();
              background = new Color(display, backgroundRGB);
              wBGColor.setBackground(background);
              wBGColor.redraw();
            }
          }
        });

    wBGColor = new Canvas(wLookComp, SWT.BORDER);
    props.setLook(wBGColor);
    wBGColor.setBackground(background);
    FormData fdBGColor = new FormData();
    fdBGColor.left = new FormAttachment(middle, 0);
    fdBGColor.right = new FormAttachment(wbBGColor, -margin);
    fdBGColor.top = new FormAttachment(0, nr * h + margin);
    fdBGColor.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wBGColor.setLayoutData(fdBGColor);

    // Graph background color
    nr++;
    Label wlGrColor = new Label(wLookComp, SWT.RIGHT);
    wlGrColor.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.BackgroundColorGraph.Label"));
    props.setLook(wlGrColor);
    FormData fdlGrColor = new FormData();
    fdlGrColor.left = new FormAttachment(0, 0);
    fdlGrColor.right = new FormAttachment(middle, -margin);
    fdlGrColor.top = new FormAttachment(0, nr * h + margin + 10);
    wlGrColor.setLayoutData(fdlGrColor);

    Button wdGrColor = new Button(wLookComp, SWT.PUSH);
    props.setLook(wdGrColor);

    FormData fddGrColor = layoutResetOptionButton(wdGrColor);
    fddGrColor.right = new FormAttachment(100, 0);
    fddGrColor.top = new FormAttachment(0, nr * h + margin);
    fddGrColor.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wdGrColor.setLayoutData(fddGrColor);
    wdGrColor.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            graphColor.dispose();

            graphColorRGB =
                new RGB(
                    ConstUi.COLOR_GRAPH_RED, ConstUi.COLOR_GRAPH_GREEN, ConstUi.COLOR_GRAPH_BLUE);
            graphColor = new Color(display, graphColorRGB);
            wGrColor.setBackground(graphColor);
            wGrColor.redraw();
          }
        });

    Button wbGrColor = new Button(wLookComp, SWT.PUSH);
    props.setLook(wbGrColor);

    FormData fdbGrColor = layoutEditOptionButton(wbGrColor);
    fdbGrColor.right = new FormAttachment(wdGrColor, -margin);
    fdbGrColor.top = new FormAttachment(0, nr * h + margin);
    fdbGrColor.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wbGrColor.setLayoutData(fdbGrColor);
    wbGrColor.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            ColorDialog cd = new ColorDialog(shell);
            cd.setRGB(props.getGraphColorRGB());
            RGB newbg = cd.open();
            if (newbg != null) {
              graphColorRGB = newbg;
              graphColor.dispose();
              graphColor = new Color(display, graphColorRGB);
              wGrColor.setBackground(graphColor);
              wGrColor.redraw();
            }
          }
        });

    wGrColor = new Canvas(wLookComp, SWT.BORDER);
    props.setLook(wGrColor);
    wGrColor.setBackground(graphColor);
    FormData fdGrColor = new FormData();
    fdGrColor.left = new FormAttachment(middle, 0);
    fdGrColor.right = new FormAttachment(wbGrColor, -margin);
    fdGrColor.top = new FormAttachment(0, nr * h + margin);
    fdGrColor.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wGrColor.setLayoutData(fdGrColor);

    // Tab selected color
    nr++;
    Label wlTabColor = new Label(wLookComp, SWT.RIGHT);
    wlTabColor.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.TabColor.Label"));
    props.setLook(wlTabColor);
    FormData fdlTabColor = new FormData();
    fdlTabColor.left = new FormAttachment(0, 0);
    fdlTabColor.right = new FormAttachment(middle, -margin);
    fdlTabColor.top = new FormAttachment(0, nr * h + margin + 10);
    wlTabColor.setLayoutData(fdlTabColor);

    Button wdTabColor = new Button(wLookComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wdTabColor);

    FormData fddTabColor = layoutResetOptionButton(wdTabColor);
    fddTabColor.right = new FormAttachment(100, 0);
    fddTabColor.top = new FormAttachment(0, nr * h + margin);
    fddTabColor.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wdTabColor.setLayoutData(fddTabColor);
    wdTabColor.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            tabColor.dispose();

            tabColorRGB =
                new RGB(ConstUi.COLOR_TAB_RED, ConstUi.COLOR_TAB_GREEN, ConstUi.COLOR_TAB_BLUE);
            tabColor = new Color(display, tabColorRGB);
            wTabColor.setBackground(tabColor);
            wTabColor.redraw();
          }
        });

    Button wbTabColor = new Button(wLookComp, SWT.PUSH);
    props.setLook(wbTabColor);

    FormData fdbTabColor = layoutEditOptionButton(wbTabColor);
    fdbTabColor.right = new FormAttachment(wdTabColor, -margin);
    fdbTabColor.top = new FormAttachment(0, nr * h + margin);
    fdbTabColor.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wbTabColor.setLayoutData(fdbTabColor);
    wbTabColor.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            ColorDialog cd = new ColorDialog(shell);
            cd.setRGB(props.getTabColorRGB());
            RGB newbg = cd.open();
            if (newbg != null) {
              tabColorRGB = newbg;
              tabColor.dispose();
              tabColor = new Color(display, tabColorRGB);
              wTabColor.setBackground(tabColor);
              wTabColor.redraw();
            }
          }
        });

    wTabColor = new Canvas(wLookComp, SWT.BORDER);
    props.setLook(wTabColor);
    wTabColor.setBackground(tabColor);
    FormData fdTabColor = new FormData();
    fdTabColor.left = new FormAttachment(middle, 0);
    fdTabColor.right = new FormAttachment(wbTabColor, -margin);
    fdTabColor.top = new FormAttachment(0, nr * h + margin);
    fdTabColor.bottom = new FormAttachment(0, (nr + 1) * h + margin);
    wTabColor.setLayoutData(fdTabColor);

    // IconSize line
    Label wlIconSize = new Label(wLookComp, SWT.RIGHT);
    wlIconSize.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.IconSize.Label"));
    props.setLook(wlIconSize);
    FormData fdlIconSize = new FormData();
    fdlIconSize.left = new FormAttachment(0, 0);
    fdlIconSize.right = new FormAttachment(middle, -margin);
    fdlIconSize.top = new FormAttachment(wTabColor, margin);
    wlIconSize.setLayoutData(fdlIconSize);
    wIconSize = new Text(wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wIconSize.setText(Integer.toString(props.getIconSize()));
    props.setLook(wIconSize);
    FormData fdIconSize = new FormData();
    fdIconSize.left = new FormAttachment(middle, 0);
    fdIconSize.right = new FormAttachment(100, -margin);
    fdIconSize.top = new FormAttachment(wlIconSize, 0, SWT.CENTER);
    wIconSize.setLayoutData(fdIconSize);

    // LineWidth line
    Label wlLineWidth = new Label(wLookComp, SWT.RIGHT);
    wlLineWidth.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.LineWidth.Label"));
    props.setLook(wlLineWidth);
    FormData fdlLineWidth = new FormData();
    fdlLineWidth.left = new FormAttachment(0, 0);
    fdlLineWidth.right = new FormAttachment(middle, -margin);
    fdlLineWidth.top = new FormAttachment(wIconSize, margin);
    wlLineWidth.setLayoutData(fdlLineWidth);
    wLineWidth = new Text(wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wLineWidth.setText(Integer.toString(props.getLineWidth()));
    props.setLook(wLineWidth);
    FormData fdLineWidth = new FormData();
    fdLineWidth.left = new FormAttachment(middle, 0);
    fdLineWidth.right = new FormAttachment(100, -margin);
    fdLineWidth.top = new FormAttachment(wlLineWidth, 0, SWT.CENTER);
    wLineWidth.setLayoutData(fdLineWidth);

    // MiddlePct line
    Label wlMiddlePct = new Label(wLookComp, SWT.RIGHT);
    wlMiddlePct.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.DialogMiddlePercentage.Label"));
    props.setLook(wlMiddlePct);
    FormData fdlMiddlePct = new FormData();
    fdlMiddlePct.left = new FormAttachment(0, 0);
    fdlMiddlePct.right = new FormAttachment(middle, -margin);
    fdlMiddlePct.top = new FormAttachment(wLineWidth, margin);
    wlMiddlePct.setLayoutData(fdlMiddlePct);
    wMiddlePct = new Text(wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMiddlePct.setText(Integer.toString(props.getMiddlePct()));
    props.setLook(wMiddlePct);
    FormData fdMiddlePct = new FormData();
    fdMiddlePct.left = new FormAttachment(middle, 0);
    fdMiddlePct.right = new FormAttachment(100, -margin);
    fdMiddlePct.top = new FormAttachment(wlMiddlePct, 0, SWT.CENTER);
    wMiddlePct.setLayoutData(fdMiddlePct);

    // GridSize line
    Label wlGridSize = new Label(wLookComp, SWT.RIGHT);
    wlGridSize.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.GridSize.Label"));
    wlGridSize.setToolTipText(BaseMessages.getString(PKG, "EnterOptionsDialog.GridSize.ToolTip"));
    props.setLook(wlGridSize);
    FormData fdlGridSize = new FormData();
    fdlGridSize.left = new FormAttachment(0, 0);
    fdlGridSize.right = new FormAttachment(middle, -margin);
    fdlGridSize.top = new FormAttachment(wMiddlePct, margin);
    wlGridSize.setLayoutData(fdlGridSize);
    wGridSize = new Text(wLookComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wGridSize.setText(Integer.toString(props.getCanvasGridSize()));
    wGridSize.setToolTipText(BaseMessages.getString(PKG, "EnterOptionsDialog.GridSize.ToolTip"));
    props.setLook(wGridSize);
    FormData fdGridSize = new FormData();
    fdGridSize.left = new FormAttachment(middle, 0);
    fdGridSize.right = new FormAttachment(100, -margin);
    fdGridSize.top = new FormAttachment(wMiddlePct, margin);
    wGridSize.setLayoutData(fdGridSize);

    // Show Canvas Grid
    Label wlShowCanvasGrid = new Label(wLookComp, SWT.RIGHT);
    wlShowCanvasGrid.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ShowCanvasGrid.Label"));
    wlShowCanvasGrid.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ShowCanvasGrid.ToolTip"));
    props.setLook(wlShowCanvasGrid);
    FormData fdlShowCanvasGrid = new FormData();
    fdlShowCanvasGrid.left = new FormAttachment(0, 0);
    fdlShowCanvasGrid.right = new FormAttachment(middle, -margin);
    fdlShowCanvasGrid.top = new FormAttachment(wGridSize, margin);
    wlShowCanvasGrid.setLayoutData(fdlShowCanvasGrid);
    wShowCanvasGrid = new Button(wLookComp, SWT.CHECK);
    props.setLook(wShowCanvasGrid);
    wShowCanvasGrid.setSelection(props.isShowCanvasGridEnabled());
    FormData fdShowCanvasGrid = new FormData();
    fdShowCanvasGrid.left = new FormAttachment(middle, 0);
    fdShowCanvasGrid.right = new FormAttachment(100, -margin);
    fdShowCanvasGrid.top = new FormAttachment(wlShowCanvasGrid, 0, SWT.CENTER);
    wShowCanvasGrid.setLayoutData(fdShowCanvasGrid);

    // Show original look
    Label wlOriginalLook = new Label(wLookComp, SWT.RIGHT);
    wlOriginalLook.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.UseOSLook.Label"));
    props.setLook(wlOriginalLook);
    FormData fdlOriginalLook = new FormData();
    fdlOriginalLook.left = new FormAttachment(0, 0);
    fdlOriginalLook.top = new FormAttachment(wShowCanvasGrid, margin);
    fdlOriginalLook.right = new FormAttachment(middle, -margin);
    wlOriginalLook.setLayoutData(fdlOriginalLook);
    wOriginalLook = new Button(wLookComp, SWT.CHECK);
    props.setLook(wOriginalLook);
    wOriginalLook.setSelection(props.isOSLookShown());
    FormData fdOriginalLook = new FormData();
    fdOriginalLook.left = new FormAttachment(middle, 0);
    fdOriginalLook.top = new FormAttachment(wlOriginalLook, 0, SWT.CENTER);
    fdOriginalLook.right = new FormAttachment(100, 0);
    wOriginalLook.setLayoutData(fdOriginalLook);

    // Show original look
    Label wlDarkMode = new Label(wLookComp, SWT.RIGHT);
    wlDarkMode.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.DarkMode.Label"));
    props.setLook(wlDarkMode);
    FormData fdlDarkMode = new FormData();
    fdlDarkMode.left = new FormAttachment(0, 0);
    fdlDarkMode.top = new FormAttachment(wlOriginalLook, 2*margin);
    fdlDarkMode.right = new FormAttachment(middle, -margin);
    wlDarkMode.setLayoutData(fdlDarkMode);
    wDarkMode = new Button(wLookComp, SWT.CHECK);
    wDarkMode.setSelection( props.isDarkMode() );
    props.setLook(wDarkMode);
    FormData fdDarkMode = new FormData();
    fdDarkMode.left = new FormAttachment(middle, 0);
    fdDarkMode.top = new FormAttachment(wlDarkMode, 0, SWT.CENTER);
    fdDarkMode.right = new FormAttachment(100, 0);
    wDarkMode.setLayoutData(fdDarkMode);

    // DefaultLocale line
    Label wlDefaultLocale = new Label(wLookComp, SWT.RIGHT);
    wlDefaultLocale.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.DefaultLocale.Label"));
    props.setLook(wlDefaultLocale);
    FormData fdlDefaultLocale = new FormData();
    fdlDefaultLocale.left = new FormAttachment(0, 0);
    fdlDefaultLocale.right = new FormAttachment(middle, -margin);
    fdlDefaultLocale.top = new FormAttachment(wlDarkMode, 2*margin);
    wlDefaultLocale.setLayoutData(fdlDefaultLocale);
    wDefaultLocale = new Combo(wLookComp, SWT.SINGLE | SWT.READ_ONLY | SWT.LEFT | SWT.BORDER);
    wDefaultLocale.setItems(GlobalMessages.localeDescr);
    props.setLook(wDefaultLocale);
    FormData fdDefaultLocale = new FormData();
    fdDefaultLocale.left = new FormAttachment(middle, 0);
    fdDefaultLocale.right = new FormAttachment(100, -margin);
    fdDefaultLocale.top = new FormAttachment(wlDefaultLocale, 0, SWT.CENTER);
    wDefaultLocale.setLayoutData(fdDefaultLocale);
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

    // ///////////////////////////////////////////////////////////
    // / END OF LOOK TAB
    // ///////////////////////////////////////////////////////////
  }

  private void addGeneralTab() {
    // ////////////////////////
    // START OF GENERAL TAB///
    // /
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.General.Label"));

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;

    ScrolledComposite sGeneralComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    sGeneralComp.setLayout(new FillLayout());

    Composite wGeneralComp = new Composite(sGeneralComp, SWT.NONE);
    props.setLook(wGeneralComp);
    wGeneralComp.setLayout(generalLayout);

    // Default preview size
    Label wlFilename = new Label(wGeneralComp, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.ConfigFilename.Label"));
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    fdlFilename.top = new FormAttachment(0, margin);
    wlFilename.setLayoutData(fdlFilename);
    wFilename = new Text(wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFilename.setText(Const.NVL(HopConfig.getInstance().getConfigFilename(), ""));
    wFilename.setEditable(false);
    props.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(100, -margin);
    fdFilename.top = new FormAttachment(0, margin);
    wFilename.setLayoutData(fdFilename);
    Control lastControl = wFilename;

    // Default preview size
    Label wlDefaultPreview = new Label(wGeneralComp, SWT.RIGHT);
    wlDefaultPreview.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.DefaultPreviewSize.Label"));
    props.setLook(wlDefaultPreview);
    FormData fdlDefaultPreview = new FormData();
    fdlDefaultPreview.left = new FormAttachment(0, 0);
    fdlDefaultPreview.right = new FormAttachment(middle, -margin);
    fdlDefaultPreview.top = new FormAttachment(lastControl, margin);
    wlDefaultPreview.setLayoutData(fdlDefaultPreview);
    wDefaultPreview = new Text(wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDefaultPreview.setText(Integer.toString(props.getDefaultPreviewSize()));
    props.setLook(wDefaultPreview);
    FormData fdDefaultPreview = new FormData();
    fdDefaultPreview.left = new FormAttachment(middle, 0);
    fdDefaultPreview.right = new FormAttachment(100, -margin);
    fdDefaultPreview.top = new FormAttachment(wlDefaultPreview, 0, SWT.CENTER);
    wDefaultPreview.setLayoutData(fdDefaultPreview);
    lastControl = wDefaultPreview;

    // Use DB Cache?
    Label wlUseCache = new Label(wGeneralComp, SWT.RIGHT);
    wlUseCache.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.UseDatabaseCache.Label"));
    props.setLook(wlUseCache);
    FormData fdlUseCache = new FormData();
    fdlUseCache.left = new FormAttachment(0, 0);
    fdlUseCache.top = new FormAttachment(lastControl, margin);
    fdlUseCache.right = new FormAttachment(middle, -margin);
    wlUseCache.setLayoutData(fdlUseCache);
    wUseCache = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wUseCache);
    wUseCache.setSelection(props.useDBCache());
    FormData fdUseCache = new FormData();
    fdUseCache.left = new FormAttachment(middle, 0);
    fdUseCache.top = new FormAttachment(wlUseCache, 0, SWT.CENTER);
    fdUseCache.right = new FormAttachment(100, 0);
    wUseCache.setLayoutData(fdUseCache);
    lastControl = wlUseCache;

    // Auto load last file at startup?
    Label wlOpenLast = new Label(wGeneralComp, SWT.RIGHT);
    wlOpenLast.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.OpenLastFileStartup.Label"));
    props.setLook(wlOpenLast);
    FormData fdlOpenLast = new FormData();
    fdlOpenLast.left = new FormAttachment(0, 0);
    fdlOpenLast.top = new FormAttachment(lastControl, margin);
    fdlOpenLast.right = new FormAttachment(middle, -margin);
    wlOpenLast.setLayoutData(fdlOpenLast);
    wOpenLast = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wOpenLast);
    wOpenLast.setSelection(props.openLastFile());
    FormData fdOpenLast = new FormData();
    fdOpenLast.left = new FormAttachment(middle, 0);
    fdOpenLast.top = new FormAttachment(wlOpenLast, 0, SWT.CENTER);
    fdOpenLast.right = new FormAttachment(100, 0);
    wOpenLast.setLayoutData(fdOpenLast);
    lastControl = wlOpenLast;

    // Auto save changed files?
    Label wlAutoSave = new Label(wGeneralComp, SWT.RIGHT);
    wlAutoSave.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.AutoSave.Label"));
    props.setLook(wlAutoSave);
    FormData fdlAutoSave = new FormData();
    fdlAutoSave.left = new FormAttachment(0, 0);
    fdlAutoSave.top = new FormAttachment(lastControl, margin);
    fdlAutoSave.right = new FormAttachment(middle, -margin);
    wlAutoSave.setLayoutData(fdlAutoSave);
    wAutoSave = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wAutoSave);
    wAutoSave.setSelection(props.getAutoSave());
    FormData fdAutoSave = new FormData();
    fdAutoSave.left = new FormAttachment(middle, 0);
    fdAutoSave.top = new FormAttachment(wlAutoSave, 0, SWT.CENTER);
    fdAutoSave.right = new FormAttachment(100, 0);
    wAutoSave.setLayoutData(fdAutoSave);
    lastControl = wlAutoSave;

    // Automatically split hops?
    Label wlAutoSplit = new Label(wGeneralComp, SWT.RIGHT);
    wlAutoSplit.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.AutoSplitHops.Label"));
    props.setLook(wlAutoSplit);
    FormData fdlAutoSplit = new FormData();
    fdlAutoSplit.left = new FormAttachment(0, 0);
    fdlAutoSplit.top = new FormAttachment(lastControl, margin);
    fdlAutoSplit.right = new FormAttachment(middle, -margin);
    wlAutoSplit.setLayoutData(fdlAutoSplit);
    wAutoSplit = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wAutoSplit);
    wAutoSplit.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.AutoSplitHops.Tooltip"));
    wAutoSplit.setSelection(props.getAutoSplit());
    FormData fdAutoSplit = new FormData();
    fdAutoSplit.left = new FormAttachment(middle, 0);
    fdAutoSplit.top = new FormAttachment(wlAutoSplit, 0, SWT.CENTER);
    fdAutoSplit.right = new FormAttachment(100, 0);
    wAutoSplit.setLayoutData(fdAutoSplit);
    lastControl = wlAutoSplit;

    // Show warning for copy / distribute...
    Label wlCopyDistrib = new Label(wGeneralComp, SWT.RIGHT);
    wlCopyDistrib.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.CopyOrDistributeDialog.Label"));
    props.setLook(wlCopyDistrib);
    FormData fdlCopyDistrib = new FormData();
    fdlCopyDistrib.left = new FormAttachment(0, 0);
    fdlCopyDistrib.top = new FormAttachment(lastControl, margin);
    fdlCopyDistrib.right = new FormAttachment(middle, -margin);
    wlCopyDistrib.setLayoutData(fdlCopyDistrib);
    wCopyDistrib = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wCopyDistrib);
    wCopyDistrib.setToolTipText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.CopyOrDistributeDialog.Tooltip"));
    wCopyDistrib.setSelection(props.showCopyOrDistributeWarning());
    FormData fdCopyDistrib = new FormData();
    fdCopyDistrib.left = new FormAttachment(middle, 0);
    fdCopyDistrib.top = new FormAttachment(wlCopyDistrib, 0, SWT.CENTER);
    fdCopyDistrib.right = new FormAttachment(100, 0);
    wCopyDistrib.setLayoutData(fdCopyDistrib);
    lastControl = wlCopyDistrib;

    // Show exit warning?
    Label wlExitWarning = new Label(wGeneralComp, SWT.RIGHT);
    wlExitWarning.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.AskOnExit.Label"));
    props.setLook(wlExitWarning);
    FormData fdlExitWarning = new FormData();
    fdlExitWarning.left = new FormAttachment(0, 0);
    fdlExitWarning.top = new FormAttachment(lastControl, margin);
    fdlExitWarning.right = new FormAttachment(middle, -margin);
    wlExitWarning.setLayoutData(fdlExitWarning);
    wExitWarning = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wExitWarning);
    wExitWarning.setSelection(props.showExitWarning());
    FormData fdExitWarning = new FormData();
    fdExitWarning.left = new FormAttachment(middle, 0);
    fdExitWarning.top = new FormAttachment(wlExitWarning, 0, SWT.CENTER);
    fdExitWarning.right = new FormAttachment(100, 0);
    wExitWarning.setLayoutData(fdExitWarning);
    lastControl = wlExitWarning;

    // Clear custom parameters. (from transform)
    Label wlClearCustom = new Label(wGeneralComp, SWT.RIGHT);
    wlClearCustom.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.ClearCustomParameters.Label"));
    props.setLook(wlClearCustom);
    FormData fdlClearCustom = new FormData();
    fdlClearCustom.left = new FormAttachment(0, 0);
    fdlClearCustom.top = new FormAttachment(lastControl, margin + 10);
    fdlClearCustom.right = new FormAttachment(middle, -margin);
    wlClearCustom.setLayoutData(fdlClearCustom);

    Button wClearCustom = new Button(wGeneralComp, SWT.PUSH);
    props.setLook(wClearCustom);
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

    // Auto-collapse core objects tree branches?
    Label wlAutoCollapse = new Label(wGeneralComp, SWT.RIGHT);
    wlAutoCollapse.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.EnableAutoCollapseCoreObjectTree.Label"));
    props.setLook(wlAutoCollapse);
    FormData fdlAutoCollapse = new FormData();
    fdlAutoCollapse.left = new FormAttachment(0, 0);
    fdlAutoCollapse.top = new FormAttachment(lastControl, 2 * margin);
    fdlAutoCollapse.right = new FormAttachment(middle, -margin);
    wlAutoCollapse.setLayoutData(fdlAutoCollapse);
    wAutoCollapse = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wAutoCollapse);
    wAutoCollapse.setSelection(props.getAutoCollapseCoreObjectsTree());
    FormData fdAutoCollapse = new FormData();
    fdAutoCollapse.left = new FormAttachment(middle, 0);
    fdAutoCollapse.top = new FormAttachment(wlAutoCollapse, 0, SWT.CENTER);
    fdAutoCollapse.right = new FormAttachment(100, 0);
    wAutoCollapse.setLayoutData(fdAutoCollapse);
    lastControl = wlAutoCollapse;

    // Tooltips
    Label wlToolTip = new Label(wGeneralComp, SWT.RIGHT);
    wlToolTip.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.ToolTipsEnabled.Label"));
    props.setLook(wlToolTip);
    FormData fdlToolTip = new FormData();
    fdlToolTip.left = new FormAttachment(0, 0);
    fdlToolTip.top = new FormAttachment(lastControl, margin);
    fdlToolTip.right = new FormAttachment(middle, -margin);
    wlToolTip.setLayoutData(fdlToolTip);
    wToolTip = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wToolTip);
    wToolTip.setSelection(props.showToolTips());
    FormData fdbToolTip = new FormData();
    fdbToolTip.left = new FormAttachment(middle, 0);
    fdbToolTip.top = new FormAttachment(wlToolTip, 0, SWT.CENTER);
    fdbToolTip.right = new FormAttachment(100, 0);
    wToolTip.setLayoutData(fdbToolTip);
    lastControl = wlToolTip;

    // Help tool tips
    Label wlHelpTip = new Label(wGeneralComp, SWT.RIGHT);
    wlHelpTip.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.HelpToolTipsEnabled.Label"));
    props.setLook(wlHelpTip);
    FormData fdlHelpTip = new FormData();
    fdlHelpTip.left = new FormAttachment(0, 0);
    fdlHelpTip.top = new FormAttachment(lastControl, margin);
    fdlHelpTip.right = new FormAttachment(middle, -margin);
    wlHelpTip.setLayoutData(fdlHelpTip);
    wHelpTip = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wHelpTip);
    wHelpTip.setSelection(props.isShowingHelpToolTips());
    FormData fdbHelpTip = new FormData();
    fdbHelpTip.left = new FormAttachment(middle, 0);
    fdbHelpTip.top = new FormAttachment(wlHelpTip, 0, SWT.CENTER);
    fdbHelpTip.right = new FormAttachment(100, 0);
    wHelpTip.setLayoutData(fdbHelpTip);
    lastControl = wlHelpTip;

    // Help tool tips
    Label wlUseDoubleClick = new Label(wGeneralComp, SWT.RIGHT);
    wlUseDoubleClick.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.UseDoubleClickOnCanvas.Label"));
    props.setLook(wlUseDoubleClick);
    FormData fdlUseDoubleClick = new FormData();
    fdlUseDoubleClick.left = new FormAttachment(0, 0);
    fdlUseDoubleClick.top = new FormAttachment(lastControl, margin);
    fdlUseDoubleClick.right = new FormAttachment(middle, -margin);
    wlUseDoubleClick.setLayoutData(fdlUseDoubleClick);
    wbUseDoubleClick = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wbUseDoubleClick);
    wbUseDoubleClick.setSelection(props.useDoubleClick());
    FormData fdbUseDoubleClick = new FormData();
    fdbUseDoubleClick.left = new FormAttachment(middle, 0);
    fdbUseDoubleClick.top = new FormAttachment(wlUseDoubleClick, 0, SWT.CENTER);
    fdbUseDoubleClick.right = new FormAttachment(100, 0);
    wbUseDoubleClick.setLayoutData(fdbUseDoubleClick);
    lastControl = wlUseDoubleClick;

    // Use global file bookmarks?
    Label wlUseGlobalFileBookmarks = new Label(wGeneralComp, SWT.RIGHT);
    wlUseGlobalFileBookmarks.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.UseGlobalFileBookmarks.Label"));
    props.setLook(wlUseGlobalFileBookmarks);
    FormData fdlUseGlobalFileBookmarks = new FormData();
    fdlUseGlobalFileBookmarks.left = new FormAttachment(0, 0);
    fdlUseGlobalFileBookmarks.top = new FormAttachment(lastControl, margin);
    fdlUseGlobalFileBookmarks.right = new FormAttachment(middle, -margin);
    wlUseGlobalFileBookmarks.setLayoutData(fdlUseGlobalFileBookmarks);
    wbUseGlobalFileBookmarks = new Button(wGeneralComp, SWT.CHECK);
    props.setLook(wbUseGlobalFileBookmarks);
    wbUseGlobalFileBookmarks.setSelection(props.useGlobalFileBookmarks());
    FormData fdbUseGlobalFileBookmarks = new FormData();
    fdbUseGlobalFileBookmarks.left = new FormAttachment(middle, 0);
    fdbUseGlobalFileBookmarks.top = new FormAttachment(wlUseGlobalFileBookmarks, 0, SWT.CENTER);
    fdbUseGlobalFileBookmarks.right = new FormAttachment(100, 0);
    wbUseGlobalFileBookmarks.setLayoutData(fdbUseGlobalFileBookmarks);
    lastControl = wbUseGlobalFileBookmarks;

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

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

  }

  private void addPluginTabs() {

    // Add a new tab for every config plugin which is also a GuiPlugin
    // Then simply add the widgets on a separate tab
    //
    HopGui hopGui = HopGui.getInstance();
    PluginRegistry pluginRegistry = PluginRegistry.getInstance();

    List<IPlugin> configPlugins = pluginRegistry.getPlugins(ConfigPluginType.class);
    for (IPlugin configPlugin : configPlugins) {
      try {
        Object emptySourceData = pluginRegistry.loadClass(configPlugin);
        GuiPlugin annotation = emptySourceData.getClass().getAnnotation(GuiPlugin.class);
        if (annotation != null) {

          // Load the instance
          //
          Method method = emptySourceData.getClass().getMethod("getInstance");
          Object sourceData = method.invoke(null, (Object[]) null);

          // This config plugin is also a GUI plugin
          // Add a tab
          //
          CTabItem wPluginTab = new CTabItem(wTabFolder, SWT.NONE);
          wPluginTab.setText(Const.NVL(annotation.description(), ""));

          ScrolledComposite sOtherComp =
              new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
          sOtherComp.setLayout(new FormLayout());

          Composite wPluginsComp = new Composite(sOtherComp, SWT.NONE);
          props.setLook(wPluginsComp);
          wPluginsComp.setLayout(new FormLayout());

          GuiCompositeWidgets compositeWidgets = new GuiCompositeWidgets(hopGui.getVariables(), 20);
          compositeWidgets.createCompositeWidgets(
              sourceData, null, wPluginsComp, GUI_WIDGETS_PARENT_ID, null);
          compositeWidgets.setWidgetsContents(sourceData, wPluginsComp, GUI_WIDGETS_PARENT_ID);

          pluginWidgetContentsList.add(new PluginWidgetContents(compositeWidgets, sourceData));

          // Add a default selection listener to all the widgets...
          //
          compositeWidgets.getWidgetsMap().values().stream()
              .forEach(control -> control.addListener(SWT.DefaultSelection, e -> ok()));

          wPluginsComp.pack();

          Rectangle bounds = wPluginsComp.getBounds();

          sOtherComp.setContent(wPluginsComp);
          sOtherComp.setExpandHorizontal(true);
          sOtherComp.setExpandVertical(true);
          sOtherComp.setMinWidth(bounds.width);
          sOtherComp.setMinHeight(bounds.height);

          wPluginTab.setControl(sOtherComp);
        }

      } catch (Exception e) {
        new ErrorDialog(
            shell,
            "Error",
            "Error handling configuration options for config / GUI plugin "
                + configPlugin.getIds()[0],
            e);
      }

      // ///////////////////////////////////////////////////////////
      // / END OF PLUGINS TAB
      // ///////////////////////////////////////////////////////////

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

  public void dispose() {
    fixedFont.dispose();
    graphFont.dispose();
    noteFont.dispose();

    background.dispose();
    graphColor.dispose();
    tabColor.dispose();

    shell.dispose();
  }

  public void getData() {
    fixedFontData = props.getFixedFont();
    fixedFont = new Font(display, fixedFontData);

    // Magnify to compensate for the same reduction elsewhere.
    //
    graphFontData = props.getGraphFont();
    graphFontData.setHeight((int) (graphFontData.getHeight() * PropsUi.getNativeZoomFactor()));
    graphFont = new Font(display, graphFontData);

    noteFontData = props.getNoteFont();
    noteFont = new Font(display, noteFontData);

    backgroundRGB = props.getBackgroundRGB();
    if (backgroundRGB == null) {
      backgroundRGB = display.getSystemColor(SWT.COLOR_WIDGET_BACKGROUND).getRGB();
    }
    background = new Color(display, backgroundRGB);

    graphColorRGB = props.getGraphColorRGB();
    graphColor = new Color(display, graphColorRGB);

    tabColorRGB = props.getTabColorRGB();
    tabColor = new Color(display, tabColorRGB);
  }

  private void cancel() {
    props.setScreen(new WindowProperty(shell));
    props = null;
    dispose();
  }

  private void ok() {
    props.setFixedFont(fixedFontData);
    props.setGraphFont(graphFontData);
    props.setNoteFont(noteFontData);
    props.setBackgroundRGB(backgroundRGB);
    props.setGraphColorRGB(graphColorRGB);
    props.setTabColorRGB(tabColorRGB);
    props.setIconSize(Const.toInt(wIconSize.getText(), props.getIconSize()));
    props.setLineWidth(Const.toInt(wLineWidth.getText(), props.getLineWidth()));
    props.setMiddlePct(Const.toInt(wMiddlePct.getText(), props.getMiddlePct()));
    props.setCanvasGridSize(Const.toInt(wGridSize.getText(), 1));

    props.setDefaultPreviewSize(
        Const.toInt(wDefaultPreview.getText(), props.getDefaultPreviewSize()));

    props.setUseDBCache(wUseCache.getSelection());
    props.setOpenLastFile(wOpenLast.getSelection());
    props.setAutoSave(wAutoSave.getSelection());
    props.setAutoSplit(wAutoSplit.getSelection());
    props.setShowCopyOrDistributeWarning(wCopyDistrib.getSelection());
    props.setShowCanvasGridEnabled(wShowCanvasGrid.getSelection());
    props.setExitWarningShown(wExitWarning.getSelection());
    props.setOSLookShown(wOriginalLook.getSelection());
    props.setDarkMode( wDarkMode.getSelection() );
    props.setShowToolTips(wToolTip.getSelection());
    props.setAutoCollapseCoreObjectsTree(wAutoCollapse.getSelection());
    props.setShowingHelpToolTips(wHelpTip.getSelection());
    props.setUseDoubleClickOnCanvas(wbUseDoubleClick.getSelection());
    props.setUseGlobalFileBookmarks(wbUseGlobalFileBookmarks.getSelection());

    int defaultLocaleIndex = wDefaultLocale.getSelectionIndex();
    if (defaultLocaleIndex < 0 || defaultLocaleIndex >= GlobalMessages.localeCodes.length) {
      // Code hardening, when the combo-box ever gets in a strange state,
      // use the first language as default (should be English)
      defaultLocaleIndex = 0;
    }

    String defaultLocale = GlobalMessages.localeCodes[defaultLocaleIndex];
    LanguageChoice.getInstance().setDefaultLocale(EnvUtil.createLocale(defaultLocale));

    // Persist the plugin configuration options as well...
    //
    for (PluginWidgetContents contents : pluginWidgetContentsList) {
      if (contents.sourceData instanceof IGuiPluginCompositeWidgetsListener) {
        ((IGuiPluginCompositeWidgetsListener) contents.sourceData)
            .persistContents(contents.compositeWidgets);
      }
    }

    if ("Y".equalsIgnoreCase(props.getCustomParameter(STRING_USAGE_WARNING_PARAMETER, "Y"))) {
      MessageDialogWithToggle md =
          new MessageDialogWithToggle(
              shell,
              BaseMessages.getString(PKG, "EnterOptionsDialog.RestartWarning.DialogTitle"),
              BaseMessages.getString(
                      PKG, "EnterOptionsDialog.RestartWarning.DialogMessage", Const.CR)
                  + Const.CR,
              SWT.ICON_WARNING,
              new String[] {
                BaseMessages.getString(PKG, "EnterOptionsDialog.RestartWarning.Option1")
              },
              BaseMessages.getString(PKG, "EnterOptionsDialog.RestartWarning.Option2"),
              "N".equalsIgnoreCase(props.getCustomParameter(STRING_USAGE_WARNING_PARAMETER, "Y")));
      md.open();
      props.setCustomParameter(STRING_USAGE_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y");
    }

    dispose();
  }
}
