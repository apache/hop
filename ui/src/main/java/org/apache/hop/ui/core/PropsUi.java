/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.IGuiPosition;
import org.apache.hop.core.gui.IGuiSize;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.TextSizeUtilFacade;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Layout;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.Widget;

/**
 * We use Props to store all kinds of user interactive information such as the selected colors,
 * fonts, positions of windows, etc.
 */
public class PropsUi extends Props {
  private static final String OS = System.getProperty("os.name").toLowerCase();

  private static double nativeZoomFactor;
  private static final String STRING_SHOW_COPY_OR_DISTRIBUTE_WARNING =
      "ShowCopyOrDistributeWarning";
  private static final String SHOW_TOOL_TIPS = "ShowToolTips";
  private static final String RESOLVE_VARIABLES_IN_TOOLTIPS = "ResolveVariablesInToolTips";
  private static final String SHOW_HELP_TOOL_TIPS = "ShowHelpToolTips";
  private static final String HIDE_MENU_BAR = "HideMenuBar";
  private static final String SORT_FIELD_BY_NAME = "SortFieldByName";
  private static final String CANVAS_GRID_SIZE = "CanvasGridSize";
  private static final String LEGACY_PERSPECTIVE_MODE = "LegacyPerspectiveMode";
  private static final String DISABLE_BROWSER_ENVIRONMENT_CHECK = "DisableBrowserEnvironmentCheck";
  private static final String USE_DOUBLE_CLICK_ON_CANVAS = "UseDoubleClickOnCanvas";
  private static final String DRAW_BORDER_AROUND_CANVAS_NAMES = "DrawBorderAroundCanvasNames";
  private static final String USE_GLOBAL_FILE_BOOKMARKS = "UseGlobalFileBookmarks";
  private static final String DARK_MODE = "DarkMode";
  private static final String GLOBAL_ZOOMFACTOR = "GlobalZoomFactor";
  private static final String MAX_EXECUTION_LOGGING_TEXT_SIZE = "MaxExecutionLoggingTextSize";
  private static final String GRAPH_EXTRA_VIEW_VERTICAL_ORIENTATION =
      "GraphExtraViewVerticalOrientation";

  public static final int DEFAULT_MAX_EXECUTION_LOGGING_TEXT_SIZE = 2000000;
  private Map<RGB, RGB> contrastingColors;
  private static PropsUi instance;

  public static PropsUi getInstance() {
    if (instance == null) {
      instance = new PropsUi();
    }
    return instance;
  }

  private PropsUi() {
    super();

    // If the zoom factor is set with variable HOP_GUI_ZOOM_FACTOR we set this first.
    //
    String hopGuiZoomFactor = System.getProperty(ConstUi.HOP_GUI_ZOOM_FACTOR);
    if (StringUtils.isNotEmpty(hopGuiZoomFactor)) {
      setProperty(GLOBAL_ZOOMFACTOR, hopGuiZoomFactor);
    }

    reCalculateNativeZoomFactor();

    setDefault();
  }

  /**
   * Re-calculate the static native zoom factor. Do not make this method static because Sonar
   * recommends it.
   */
  public void reCalculateNativeZoomFactor() {
    double globalZoom = getGlobalZoomFactor();
    if (EnvironmentUtils.getInstance().isWeb()) {
      nativeZoomFactor = globalZoom / 0.75;
    } else {
      // Calculate the native default zoom factor...
      // We take the default font and render it, calculate the height.
      // Compare that to the standard small icon size of 16
      //
      org.eclipse.swt.graphics.Point extent =
          TextSizeUtilFacade.textExtent("The quick brown fox jumped over the lazy dog!");
      nativeZoomFactor = (extent.y / (double) ConstUi.SMALL_ICON_SIZE) * globalZoom;
    }
  }

  @Override
  public void setDefault() {
    super.setDefault();
    Display display = Display.getCurrent();

    populateContrastingColors();

    // Only set OS look shown once in case we switch to dark mode
    // and vice versa.  We don't want to override user choices all the time.
    // If we do it like before it becomes impossible to choose your own font and colors.
    //
    if (!EnvironmentUtils.getInstance().isWeb() && !OsHelper.isWindows()) {
      if (Display.isSystemDarkTheme()) {
        if (!isDarkMode()) {
          setDarkMode(true);
        }
      } else {
        if (isDarkMode()) {
          setDarkMode(false);
        }
      }
    }

    // Various tweaks to improve dark theme experience on Windows
    if (OsHelper.isWindows() && isDarkMode()) {
      display.setData("org.eclipse.swt.internal.win32.useDarkModeExplorerTheme", true);
      display.setData("org.eclipse.swt.internal.win32.useShellTitleColoring", true);
      display.setData(
          "org.eclipse.swt.internal.win32.menuBarForegroundColor",
          new Color(display, 0xD0, 0xD0, 0xD0));
      display.setData(
          "org.eclipse.swt.internal.win32.menuBarBackgroundColor",
          new Color(display, 0x30, 0x30, 0x30));
      display.setData(
          "org.eclipse.swt.internal.win32.menuBarBorderColor",
          new Color(display, 0x50, 0x50, 0x50));
      display.setData("org.eclipse.swt.internal.win32.all.use_WS_BORDER", true);
      display.setData(
          "org.eclipse.swt.internal.win32.Table.headerLineColor",
          new Color(display, 0x50, 0x50, 0x50));
      display.setData(
          "org.eclipse.swt.internal.win32.Label.disabledForegroundColor",
          new Color(display, 0x80, 0x80, 0x80));
      display.setData("org.eclipse.swt.internal.win32.Combo.useDarkTheme", true);
      display.setData(
          "org.eclipse.swt.internal.win32.ToolBar.backgroundColor",
          new Color(display, 0xD0, 0xD0, 0xD0));
      display.setData(
          "org.eclipse.swt.internal.win32.Combo.backgroundColor",
          new Color(display, 0xD0, 0xD0, 0xD0));
      display.setData("org.eclipse.swt.internal.win32.ProgressBar.useColors", true);
    }

    if (display != null) {
      FontData fontData = getDefaultFont();
      setProperty(STRING_FONT_DEFAULT_NAME, fontData.getName());
      setProperty(STRING_FONT_DEFAULT_SIZE, "" + fontData.getHeight());
      setProperty(STRING_FONT_DEFAULT_STYLE, "" + fontData.getStyle());

      fontData = getFixedFont();
      setProperty(STRING_FONT_FIXED_NAME, fontData.getName());
      setProperty(STRING_FONT_FIXED_SIZE, "" + fontData.getHeight());
      setProperty(STRING_FONT_FIXED_STYLE, "" + fontData.getStyle());

      fontData = getGraphFont();
      setProperty(STRING_FONT_GRAPH_NAME, fontData.getName());
      setProperty(STRING_FONT_GRAPH_SIZE, "" + fontData.getHeight());
      setProperty(STRING_FONT_GRAPH_STYLE, "" + fontData.getStyle());

      fontData = getNoteFont();
      setProperty(STRING_FONT_NOTE_NAME, fontData.getName());
      setProperty(STRING_FONT_NOTE_SIZE, "" + fontData.getHeight());
      setProperty(STRING_FONT_NOTE_STYLE, "" + fontData.getStyle());

      setProperty(STRING_ICON_SIZE, "" + getIconSize());
      setProperty(STRING_LINE_WIDTH, "" + getLineWidth());
      setProperty(STRING_MAX_UNDO, "" + getMaxUndo());
    }
  }

  public void setFixedFont(FontData fd) {
    setProperty(STRING_FONT_FIXED_NAME, fd.getName());
    setProperty(STRING_FONT_FIXED_SIZE, "" + fd.getHeight());
    setProperty(STRING_FONT_FIXED_STYLE, "" + fd.getStyle());
  }

  public FontData getFixedFont() {
    FontData def = getDefaultFontData();

    String name = getProperty(STRING_FONT_FIXED_NAME);
    if (StringUtils.isEmpty(name)) {
      if (Const.isWindows()) {
        name = "Consolas";
      } else if (Const.isLinux()) {
        name = "Monospace";
      } else if (Const.isOSX()) {
        name = "Monaco";
      } else if (EnvironmentUtils.getInstance().isWeb()) {
        name = "monospace";
      } else {
        name = java.awt.Font.MONOSPACED;
      }
    }
    int size = Const.toInt(getProperty(STRING_FONT_FIXED_SIZE), def.getHeight());
    int style = Const.toInt(getProperty(STRING_FONT_FIXED_STYLE), def.getStyle());

    return new FontData(name, size, style);
  }

  public FontData getDefaultFont() {
    FontData def = getDefaultFontData();

    String name = getProperty(STRING_FONT_DEFAULT_NAME, def.getName());
    int size = Const.toInt(getProperty(STRING_FONT_DEFAULT_SIZE), def.getHeight());
    int style = Const.toInt(getProperty(STRING_FONT_DEFAULT_STYLE), def.getStyle());

    return new FontData(name, size, style);
  }

  public void setDefaultFont(FontData fd) {
    setProperty(STRING_FONT_DEFAULT_NAME, fd.getName());
    setProperty(STRING_FONT_DEFAULT_SIZE, "" + fd.getHeight());
    setProperty(STRING_FONT_DEFAULT_STYLE, "" + fd.getStyle());
  }

  public void setGraphFont(FontData fd) {
    setProperty(STRING_FONT_GRAPH_NAME, fd.getName());
    setProperty(STRING_FONT_GRAPH_SIZE, "" + fd.getHeight());
    setProperty(STRING_FONT_GRAPH_STYLE, "" + fd.getStyle());
  }

  public FontData getGraphFont() {
    FontData def = getDefaultFontData();

    String name = getProperty(STRING_FONT_GRAPH_NAME, def.getName());
    int size = Const.toInt(getProperty(STRING_FONT_GRAPH_SIZE), def.getHeight());
    int style = Const.toInt(getProperty(STRING_FONT_GRAPH_STYLE), def.getStyle());

    return new FontData(name, size, style);
  }

  public void setNoteFont(FontData fd) {
    setProperty(STRING_FONT_NOTE_NAME, fd.getName());
    setProperty(STRING_FONT_NOTE_SIZE, "" + fd.getHeight());
    setProperty(STRING_FONT_NOTE_STYLE, "" + fd.getStyle());
  }

  public FontData getNoteFont() {
    FontData def = getDefaultFontData();

    String name = getProperty(STRING_FONT_NOTE_NAME, def.getName());
    int size = Const.toInt(getProperty(STRING_FONT_NOTE_SIZE), def.getHeight());
    int style = Const.toInt(getProperty(STRING_FONT_NOTE_STYLE), def.getStyle());

    return new FontData(name, size, style);
  }

  public void setIconSize(int size) {
    setProperty(STRING_ICON_SIZE, "" + size);
  }

  public int getIconSize() {
    return Const.toInt(getProperty(STRING_ICON_SIZE), ConstUi.ICON_SIZE);
  }

  public void setZoomFactor(double factor) {
    setProperty(STRING_ZOOM_FACTOR, Double.toString(factor));
  }

  public double getZoomFactor() {
    return getNativeZoomFactor();
  }

  /**
   * Get the margin compensated for the zoom factor
   *
   * @return
   */
  /** The margin between the different dialog components &amp; widgets */
  public static int getMargin() {
    return (int) Math.round(4 * getNativeZoomFactor());
  }

  public void setLineWidth(int width) {
    setProperty(STRING_LINE_WIDTH, "" + width);
  }

  public int getLineWidth() {
    return Const.toInt(getProperty(STRING_LINE_WIDTH), ConstUi.LINE_WIDTH);
  }

  public void setLastPreview(String[] lastpreview, int[] transformsize) {
    setProperty(STRING_LAST_PREVIEW_TRANSFORM, "" + lastpreview.length);

    for (int i = 0; i < lastpreview.length; i++) {
      setProperty(STRING_LAST_PREVIEW_TRANSFORM + (i + 1), lastpreview[i]);
      setProperty(STRING_LAST_PREVIEW_SIZE + (i + 1), "" + transformsize[i]);
    }
  }

  public String[] getLastPreview() {
    String snr = getProperty(STRING_LAST_PREVIEW_TRANSFORM);
    int nr = Const.toInt(snr, 0);
    String[] lp = new String[nr];
    for (int i = 0; i < nr; i++) {
      lp[i] = getProperty(STRING_LAST_PREVIEW_TRANSFORM + (i + 1), "");
    }
    return lp;
  }

  public int[] getLastPreviewSize() {
    String snr = getProperty(STRING_LAST_PREVIEW_TRANSFORM);
    int nr = Const.toInt(snr, 0);
    int[] si = new int[nr];
    for (int i = 0; i < nr; i++) {
      si[i] = Const.toInt(getProperty(STRING_LAST_PREVIEW_SIZE + (i + 1), ""), 0);
    }
    return si;
  }

  public FontData getDefaultFontData() {
    return Display.getCurrent().getSystemFont().getFontData()[0];
  }

  public void setMaxUndo(int max) {
    setProperty(STRING_MAX_UNDO, "" + max);
  }

  public int getMaxUndo() {
    return Const.toInt(getProperty(STRING_MAX_UNDO), Const.MAX_UNDO);
  }

  public void setMiddlePct(int pct) {
    setProperty(STRING_MIDDLE_PCT, "" + pct);
  }

  /** The percentage of the width of screen where we consider the middle of a dialog. */
  public int getMiddlePct() {
    return Const.toInt(getProperty(STRING_MIDDLE_PCT), 35);
  }

  /** The horizontal and vertical margin of a dialog box. */
  public static int getFormMargin() {
    return (int) Math.round(5 * getNativeZoomFactor());
  }

  public void setScreen(WindowProperty windowProperty) {
    AuditManager.storeState(
        LogChannel.UI,
        HopGui.DEFAULT_HOP_GUI_NAMESPACE,
        "shells",
        windowProperty.getName(),
        windowProperty.getStateProperties());
  }

  public WindowProperty getScreen(String windowName) {
    if (windowName == null) {
      return null;
    }
    AuditState auditState =
        AuditManager.retrieveState(
            LogChannel.UI, HopGui.DEFAULT_HOP_GUI_NAMESPACE, "shells", windowName);
    if (auditState == null) {
      return null;
    }
    return new WindowProperty(windowName, auditState.getStateMap());
  }

  public void setOpenLastFile(boolean open) {
    setProperty(STRING_OPEN_LAST_FILE, open ? YES : NO);
  }

  public boolean openLastFile() {
    String open = getProperty(STRING_OPEN_LAST_FILE);
    return !NO.equalsIgnoreCase(open);
  }

  public void setAutoSave(boolean autosave) {
    setProperty(STRING_AUTO_SAVE, autosave ? YES : NO);
  }

  public boolean getAutoSave() {
    String autosave = getProperty(STRING_AUTO_SAVE);
    return YES.equalsIgnoreCase(autosave); // Default = OFF
  }

  public void setSaveConfirmation(boolean saveconf) {
    setProperty(STRING_SAVE_CONF, saveconf ? YES : NO);
  }

  public boolean getSaveConfirmation() {
    String saveconf = getProperty(STRING_SAVE_CONF);
    return YES.equalsIgnoreCase(saveconf); // Default = OFF
  }

  public boolean isGraphExtraViewVerticalOrientation() {
    String vertical = this.getProperty(GRAPH_EXTRA_VIEW_VERTICAL_ORIENTATION);
    return YES.equalsIgnoreCase(vertical);
  }

  /** Set true for vertical orientation */
  public void setGraphExtraViewVerticalOrientation(boolean vertical) {
    setProperty(GRAPH_EXTRA_VIEW_VERTICAL_ORIENTATION, vertical ? YES : NO);
  }

  public void setAutoSplit(boolean autosplit) {
    setProperty(STRING_AUTO_SPLIT, autosplit ? YES : NO);
  }

  public boolean getAutoSplit() {
    String autosplit = getProperty(STRING_AUTO_SPLIT);
    return YES.equalsIgnoreCase(autosplit); // Default = OFF
  }

  public void setExitWarningShown(boolean show) {
    setProperty(STRING_SHOW_EXIT_WARNING, show ? YES : NO);
  }

  public boolean isShowCanvasGridEnabled() {
    String showCanvas = getProperty(STRING_SHOW_CANVAS_GRID, NO);
    return YES.equalsIgnoreCase(showCanvas); // Default: don't show canvas grid
  }

  public void setShowCanvasGridEnabled(boolean anti) {
    setProperty(STRING_SHOW_CANVAS_GRID, anti ? YES : NO);
  }

  public boolean isHideViewportEnabled() {
    String showViewport = getProperty(STRING_HIDE_VIEWPORT, NO);
    return YES.equalsIgnoreCase(showViewport); // Default: don't hide the viewport
  }

  public void setHideViewportEnabled(boolean anti) {
    setProperty(STRING_HIDE_VIEWPORT, anti ? YES : NO);
  }

  public boolean showExitWarning() {
    String show = getProperty(STRING_SHOW_EXIT_WARNING, YES);
    return YES.equalsIgnoreCase(show); // Default: show repositories dialog at startup
  }

  public boolean isShowTableViewToolbar() {
    String show = getProperty(STRING_SHOW_TABLE_VIEW_TOOLBAR, YES);
    return YES.equalsIgnoreCase(show); // Default: show the toolbar
  }

  public void setShowTableViewToolbar(boolean show) {
    setProperty(STRING_SHOW_TABLE_VIEW_TOOLBAR, show ? YES : NO);
  }

  public static void setLook(Widget widget) {
    int style = WIDGET_STYLE_DEFAULT;
    if (widget instanceof Table) {
      style = WIDGET_STYLE_TABLE;
    } else if (widget instanceof Tree) {
      style = WIDGET_STYLE_TREE;
    } else if (widget instanceof ToolBar) {
      style = WIDGET_STYLE_TOOLBAR;
    } else if (widget instanceof CTabFolder) {
      style = WIDGET_STYLE_TAB;
    } else if (OS.contains("mac") && (widget instanceof Group)) {
      style = WIDGET_STYLE_OSX_GROUP;
    } else if (widget instanceof Button) {
      if (Const.isWindows() && ((widget.getStyle() & (SWT.CHECK | SWT.RADIO)) != 0)) {
        style = WIDGET_STYLE_DEFAULT;
      } else {
        style = WIDGET_STYLE_PUSH_BUTTON;
      }
    }

    setLook(widget, style);

    if (widget instanceof Composite composite) {
      for (Control child : composite.getChildren()) {
        setLook(child);
      }
    }
  }

  public static void setLook(final Widget widget, int style) {
    if (OsHelper.isWindows()) {
      setLookOnWindows(widget, style);
    } else if (OsHelper.isMac()) {
      setLookOnMac(widget, style);
    } else {
      setLookOnLinux(widget, style);
    }
  }

  /**
   * Ensures that a CTabFolder has a safe renderer that catches IllegalArgumentException when
   * drawing tab images. This prevents crashes on some Linux desktop environments (e.g., KDE Plasma
   * on Wayland) where images may be invalid or disposed. Only applied on Linux to avoid any
   * potential impact on unaffected platforms.
   *
   * <p>SafeCTabFolderRenderer is in the RCP module (desktop-specific) and loaded via reflection.
   * Since this method short-circuits in web mode and SafeCTabFolderRenderer is not included in web
   * builds (hop-ui-rcp is excluded), it will never be loaded in RAP/web mode.
   *
   * @param tabFolder the CTabFolder to protect
   */
  private static void ensureSafeRenderer(CTabFolder tabFolder) {
    // CTabFolderRenderer is not available in RAP (web mode), so skip entirely.
    // SafeCTabFolderRenderer is in the RCP module (not included in web builds), so it's never
    // available in web mode.
    if (EnvironmentUtils.getInstance().isWeb()) {
      return;
    }
    // Only apply safe renderer on Linux where the issue occurs
    if (Const.isLinux()) {
      try {
        // Use reflection to load SafeCTabFolderRenderer from the RCP module (desktop-specific).
        // This avoids compile-time dependencies and ensures the class isn't loaded in web mode
        // (where hop-ui-rcp is excluded from the build).
        Class<?> rendererClass = Class.forName("org.apache.hop.ui.core.SafeCTabFolderRenderer");
        Object currentRenderer = tabFolder.getRenderer();
        if (currentRenderer == null || !rendererClass.isInstance(currentRenderer)) {
          Object safeRenderer =
              rendererClass.getConstructor(CTabFolder.class).newInstance(tabFolder);
          // Use reflection to call setRenderer to avoid compile-time dependency on
          // CTabFolderRenderer which doesn't exist in RAP/web mode
          Class<?> rendererParamClass = Class.forName("org.eclipse.swt.custom.CTabFolderRenderer");
          java.lang.reflect.Method setRendererMethod =
              CTabFolder.class.getMethod("setRenderer", rendererParamClass);
          setRendererMethod.invoke(tabFolder, safeRenderer);
        }
      } catch (ClassNotFoundException e) {
        // SafeCTabFolderRenderer not available (e.g., in web builds where hop-ui-rcp is excluded)
        // This is expected and safe to ignore
      } catch (Exception e) {
        // If CTabFolderRenderer can't be loaded or setRenderer fails, just continue without
        // the safe renderer
        LogChannel.GENERAL.logDetailed("Could not apply SafeCTabFolderRenderer: " + e.getMessage());
      }
    }
  }

  protected static void setLookOnWindows(final Widget widget, int style) {
    final GuiResource gui = GuiResource.getInstance();
    Font font = gui.getFontDefault();
    Color background = GuiResource.getInstance().getWidgetBackGroundColor();
    Color foreground = gui.getColorBlack();

    if (widget instanceof Shell shell) {
      shell.setBackgroundMode(SWT.INHERIT_FORCE);
      shell.setForeground(foreground);
      shell.setBackground(background);
      return;
    }

    switch (style) {
      case WIDGET_STYLE_DEFAULT:
        break;
      case WIDGET_STYLE_FIXED:
        font = gui.getFontFixed();
        break;
      case WIDGET_STYLE_TABLE:
        if (PropsUi.getInstance().isDarkMode()) {
          Table table = (Table) widget;
          table.setHeaderBackground(background);
          table.setHeaderForeground(foreground);
        }
        break;
      case WIDGET_STYLE_TREE:
        if (PropsUi.getInstance().isDarkMode()) {
          Tree tree = (Tree) widget;
          tree.setHeaderBackground(background);
          tree.setHeaderForeground(foreground);
        }
        break;
      case WIDGET_STYLE_TOOLBAR:
        break;
      case WIDGET_STYLE_TAB:
        CTabFolder tabFolder = (CTabFolder) widget;
        tabFolder.setBorderVisible(true);
        tabFolder.setTabHeight(28);
        ensureSafeRenderer(tabFolder);
        if (PropsUi.getInstance().isDarkMode()) {
          tabFolder.setBackground(background);
          tabFolder.setForeground(foreground);
          tabFolder.setSelectionBackground(background);
          tabFolder.setSelectionForeground(foreground);
        }
        break;
      case WIDGET_STYLE_PUSH_BUTTON:
        break;
      default:
        background = GuiResource.getInstance().getWidgetBackGroundColor();
        font = null;
        break;
    }

    if (font != null && !font.isDisposed() && (widget instanceof Control controlWidget)) {
      controlWidget.setFont(font);
    }

    if (background != null
        && !background.isDisposed()
        && (widget instanceof Control controlWidget)) {
      controlWidget.setBackground(background);
    }

    if (foreground != null
        && !foreground.isDisposed()
        && (widget instanceof Control controlWidget)) {
      controlWidget.setForeground(foreground);
    }
  }

  protected static void setLookOnMac(final Widget widget, int style) {
    final GuiResource gui = GuiResource.getInstance();
    Font font = gui.getFontDefault();
    Color background = null;

    Display display = Display.getCurrent();
    if (display == null) {
      return;
    }

    // Handle Shell windows with system background, but let macOS handle text color
    if (widget instanceof Shell shell) {
      shell.setBackgroundMode(SWT.INHERIT_FORCE);
      return;
    }

    switch (style) {
      case WIDGET_STYLE_DEFAULT:
        // Use system widget background for default composites
        // Don't set foreground - let macOS handle text colors
        break;
      case WIDGET_STYLE_OSX_GROUP:
        font = gui.getFontDefault();
        Group group = ((Group) widget);
        final Color groupBg = background;
        group.addPaintListener(
            paintEvent -> {
              // Use system colors in paint listener
              paintEvent.gc.setForeground(display.getSystemColor(SWT.COLOR_WIDGET_FOREGROUND));
              paintEvent.gc.setBackground(groupBg);
              paintEvent.gc.fillRectangle(
                  2, 0, group.getBounds().width - 8, group.getBounds().height - 20);
            });
        break;
      case WIDGET_STYLE_FIXED:
        font = gui.getFontFixed();
        break;
      case WIDGET_STYLE_TABLE:
        Table table = (Table) widget;
        table.setHeaderBackground(background);
        // Don't set foreground colors - let macOS handle them
        break;
      case WIDGET_STYLE_TREE:
        break;
      case WIDGET_STYLE_TOOLBAR:
        break;
      case WIDGET_STYLE_TAB:
        CTabFolder tabFolder = (CTabFolder) widget;
        tabFolder.setBorderVisible(true);
        tabFolder.setBackground(background);
        tabFolder.setSelectionBackground(background);
        ensureSafeRenderer(tabFolder);
        break;
      case WIDGET_STYLE_PUSH_BUTTON:
        break;
      default:
        font = null;
        break;
    }

    if (font != null && !font.isDisposed() && (widget instanceof Control controlWidget)) {
      controlWidget.setFont(font);
    }

    if (background != null
        && !background.isDisposed()
        && (widget instanceof Control controlWidget)) {
      controlWidget.setBackground(background);
    }
  }

  protected static void setLookOnLinux(final Widget widget, int style) {
    final GuiResource gui = GuiResource.getInstance();
    Font font = gui.getFontDefault();
    Color background = GuiResource.getInstance().getWidgetBackGroundColor();
    Color foreground = gui.getColorBlack();

    switch (style) {
      case WIDGET_STYLE_DEFAULT:
        break;
      case WIDGET_STYLE_OSX_GROUP:
        // TODO: Adjust for Linux
        break;
      case WIDGET_STYLE_FIXED:
        font = gui.getFontFixed();
        break;
      case WIDGET_STYLE_TABLE:
        background = gui.getColorLightGray();
        foreground = gui.getColorDarkGray();
        Table table = (Table) widget;
        table.setHeaderBackground(gui.getColorLightGray());
        table.setHeaderForeground(gui.getColorDarkGray());
        break;
      case WIDGET_STYLE_TREE:
        // TODO: Adjust for Linux
        break;
      case WIDGET_STYLE_TOOLBAR:
        if (PropsUi.getInstance().isDarkMode()) {
          background = gui.getColorLightGray();
        } else {
          background = gui.getColorDemoGray();
        }
        break;
      case WIDGET_STYLE_TAB:
        CTabFolder tabFolder = (CTabFolder) widget;
        tabFolder.setBorderVisible(true);
        tabFolder.setBackground(gui.getColorGray());
        tabFolder.setForeground(gui.getColorBlack());
        ensureSafeRenderer(tabFolder);
        tabFolder.setSelectionBackground(gui.getColorWhite());
        tabFolder.setSelectionForeground(gui.getColorBlack());
        break;
      case WIDGET_STYLE_PUSH_BUTTON:
        background = null;
        foreground = null;
        break;
      default:
        background = gui.getColorBackground();
        font = null;
        break;
    }

    if (font != null && !font.isDisposed() && (widget instanceof Control controlWidget)) {
      controlWidget.setFont(font);
    }

    if (background != null
        && !background.isDisposed()
        && (widget instanceof Control controlWidget)) {
      controlWidget.setBackground(background);
    }

    if (foreground != null
        && !foreground.isDisposed()
        && (widget instanceof Control controlWidget)) {
      controlWidget.setForeground(foreground);
    }
  }

  /**
   * @return Returns the display.
   */
  public static Display getDisplay() {
    return Display.getCurrent();
  }

  public void setDefaultPreviewSize(int size) {
    setProperty(STRING_DEFAULT_PREVIEW_SIZE, "" + size);
  }

  public int getDefaultPreviewSize() {
    return Const.toInt(getProperty(STRING_DEFAULT_PREVIEW_SIZE), 1000);
  }

  public boolean showCopyOrDistributeWarning() {
    String show = getProperty(STRING_SHOW_COPY_OR_DISTRIBUTE_WARNING, YES);
    return YES.equalsIgnoreCase(show);
  }

  public void setShowCopyOrDistributeWarning(boolean show) {
    setProperty(STRING_SHOW_COPY_OR_DISTRIBUTE_WARNING, show ? YES : NO);
  }

  public void setDialogSize(Shell shell, String styleProperty) {
    String prop = getProperty(styleProperty);
    if (Utils.isEmpty(prop)) {
      return;
    }

    String[] xy = prop.split(",");
    if (xy.length != 2) {
      return;
    }

    shell.setSize(Integer.parseInt(xy[0]), Integer.parseInt(xy[1]));
  }

  public boolean useDoubleClick() {
    return YES.equalsIgnoreCase(getProperty(USE_DOUBLE_CLICK_ON_CANVAS, NO));
  }

  public void setUseDoubleClickOnCanvas(boolean use) {
    setProperty(USE_DOUBLE_CLICK_ON_CANVAS, use ? YES : NO);
  }

  public boolean isBorderDrawnAroundCanvasNames() {
    return YES.equalsIgnoreCase(getProperty(DRAW_BORDER_AROUND_CANVAS_NAMES, NO));
  }

  public void setDrawBorderAroundCanvasNames(boolean draw) {
    setProperty(DRAW_BORDER_AROUND_CANVAS_NAMES, draw ? YES : NO);
  }

  public boolean useGlobalFileBookmarks() {
    return YES.equalsIgnoreCase(getProperty(USE_GLOBAL_FILE_BOOKMARKS, YES));
  }

  public void setUseGlobalFileBookmarks(boolean use) {
    setProperty(USE_GLOBAL_FILE_BOOKMARKS, use ? YES : NO);
  }

  public boolean isDarkMode() {
    return YES.equalsIgnoreCase(getProperty(DARK_MODE, NO));
  }

  public void setDarkMode(boolean darkMode) {
    setProperty(DARK_MODE, darkMode ? YES : NO);
  }

  public boolean showToolTips() {
    return YES.equalsIgnoreCase(getProperty(SHOW_TOOL_TIPS, YES));
  }

  public void setShowToolTips(boolean show) {
    setProperty(SHOW_TOOL_TIPS, show ? YES : NO);
  }

  public boolean resolveVariablesInToolTips() {
    return YES.equalsIgnoreCase(getProperty(RESOLVE_VARIABLES_IN_TOOLTIPS, YES));
  }

  public void setResolveVariablesInToolTips(boolean resolve) {
    setProperty(RESOLVE_VARIABLES_IN_TOOLTIPS, resolve ? YES : NO);
  }

  public boolean isShowingHelpToolTips() {
    return YES.equalsIgnoreCase(getProperty(SHOW_HELP_TOOL_TIPS, YES));
  }

  public void setHidingMenuBar(boolean show) {
    setProperty(HIDE_MENU_BAR, show ? YES : NO);
  }

  public boolean isHidingMenuBar() {
    return YES.equalsIgnoreCase(
        System.getProperty(ConstUi.HOP_GUI_HIDE_MENU, getProperty(HIDE_MENU_BAR, YES)));
  }

  public void setSortFieldByName(boolean sort) {
    setProperty(SORT_FIELD_BY_NAME, sort ? YES : NO);
  }

  public boolean isSortFieldByName() {
    return YES.equalsIgnoreCase(
        System.getProperty(SORT_FIELD_BY_NAME, getProperty(SORT_FIELD_BY_NAME, YES)));
  }

  public void setShowingHelpToolTips(boolean show) {
    setProperty(SHOW_HELP_TOOL_TIPS, show ? YES : NO);
  }

  public int getCanvasGridSize() {
    return Const.toInt(getProperty(CANVAS_GRID_SIZE, "16"), 16);
  }

  public void setCanvasGridSize(int gridSize) {
    setProperty(CANVAS_GRID_SIZE, Integer.toString(gridSize));
  }

  /**
   * Gets the supported version of the requested software.
   *
   * @param property the key for the software version
   * @return an integer that represents the supported version for the software.
   */
  public int getSupportedVersion(String property) {
    return Integer.parseInt(getProperty(property));
  }

  /**
   * Ask if the browsing environment checks are disabled.
   *
   * @return 'true' if disabled 'false' otherwise.
   */
  public boolean isBrowserEnvironmentCheckDisabled() {
    return "Y".equalsIgnoreCase(getProperty(DISABLE_BROWSER_ENVIRONMENT_CHECK, "N"));
  }

  public boolean isLegacyPerspectiveMode() {
    return "Y".equalsIgnoreCase(getProperty(LEGACY_PERSPECTIVE_MODE, "N"));
  }

  public static void setLocation(IGuiPosition guiElement, int x, int y) {
    if (x < 0) {
      x = 0;
    }
    if (y < 0) {
      y = 0;
    }
    guiElement.setLocation(calculateGridPosition(new Point(x, y)));
  }

  public static Point calculateGridPosition(Point p) {
    int gridSize = PropsUi.getInstance().getCanvasGridSize();
    if (gridSize > 1) {
      // Snap to grid...
      //
      return new Point(
          gridSize * (int) Math.round((float) (p.x / gridSize)),
          gridSize * (int) Math.round((float) (p.y / gridSize)));
    } else {
      // Normal draw
      //
      return p;
    }
  }

  /**
   * Sets the size of a given GUI element, ensuring that the width and height do not fall below a
   * predefined minimum size. The size is adjusted to align with a grid through calculation.
   *
   * @param element the GUI element whose size is to be set
   * @param width the desired width of the element
   * @param height the desired height of the element
   */
  public static void setSize(IGuiSize element, int width, int height) {

    if (width < ConstUi.NOTE_MIN_SIZE) {
      width = ConstUi.NOTE_MIN_SIZE;
    }
    if (height < ConstUi.NOTE_MIN_SIZE) {
      height = ConstUi.NOTE_MIN_SIZE;
    }
    int gridSize = PropsUi.getInstance().getCanvasGridSize();
    if (gridSize > 1) {
      int w = width / gridSize;
      if (width % gridSize > 0) w += 1;
      int h = height / gridSize;
      if (height % gridSize > 0) h += 1;
      width = gridSize * w;
      height = gridSize * h;
    }
    element.setWidth(width);
    element.setHeight(height);
  }

  public boolean isIndicateSlowPipelineTransformsEnabled() {
    String indicate = getProperty(STRING_INDICATE_SLOW_PIPELINE_TRANSFORMS, "Y");
    return YES.equalsIgnoreCase(indicate);
  }

  public void setIndicateSlowPipelineTransformsEnabled(boolean indicate) {
    setProperty(STRING_INDICATE_SLOW_PIPELINE_TRANSFORMS, indicate ? YES : NO);
  }

  /**
   * Gets nativeZoomFactor
   *
   * @return value of nativeZoomFactor
   */
  public static double getNativeZoomFactor() {
    return nativeZoomFactor;
  }

  /**
   * @param nativeZoomFactor The nativeZoomFactor to set
   */
  public static void setNativeZoomFactor(double nativeZoomFactor) {
    PropsUi.nativeZoomFactor = nativeZoomFactor;
  }

  private void populateContrastingColors() {
    contrastingColors = new HashMap<>();
    contrastingColors.put(toRGB("#000000"), toRGB("#ffffff"));
    contrastingColors.put(toRGB("#0e3a5a"), toRGB("#c8e7fa"));
    contrastingColors.put(toRGB("#0f3b5a"), toRGB("#c7e6fa"));

    contrastingColors.put(toRGB("#f0f0f0"), toRGB("#0f0f0f"));
    contrastingColors.put(toRGB("#e1e1e1"), toRGB("#303030"));
    contrastingColors.put(toRGB("#646464"), toRGB("#707070"));

    contrastingColors.put(toRGB("#ffd700"), toRGB("#0028ff"));

    // brighten workflow logo color by 50%
    contrastingColors.put(toRGB("#033d5d"), toRGB("#36b3f8"));

    // Darken yellow in JS transform by 75%
    contrastingColors.put(toRGB("#eeffaa"), toRGB("#556a00"));

    // Darken light blue by 50%
    //
    contrastingColors.put(toRGB("#c9e8fb"), toRGB("#0f88d2"));

    contrastingColors.put(new RGB(254, 254, 254), new RGB(35, 35, 35));
    contrastingColors.put(new RGB(245, 245, 245), new RGB(40, 40, 40));
    contrastingColors.put(new RGB(240, 240, 240), new RGB(45, 45, 45));
    contrastingColors.put(new RGB(235, 235, 235), new RGB(50, 50, 50));
    contrastingColors.put(new RGB(225, 225, 225), new RGB(70, 70, 70));
    contrastingColors.put(new RGB(215, 215, 215), new RGB(100, 100, 100));
    contrastingColors.put(new RGB(100, 100, 100), new RGB(215, 215, 215));
    contrastingColors.put(new RGB(50, 50, 50), new RGB(235, 235, 235));

    // Add all the inverse color mappings as well
    //
    Map<RGB, RGB> inverse = new HashMap<>();
    contrastingColors.keySet().forEach(key -> inverse.put(contrastingColors.get(key), key));
    contrastingColors.putAll(inverse);
  }

  private RGB toRGB(String colorString) {
    int red = Integer.valueOf(colorString.substring(1, 3), 16);
    int green = Integer.valueOf(colorString.substring(3, 5), 16);
    int blue = Integer.valueOf(colorString.substring(5, 7), 16);
    return new RGB(red, green, blue);
  }

  /**
   * @param rgb the color to contrast if the system is in "Dark Mode"
   * @return The contrasted color
   */
  public RGB contrastColor(RGB rgb) {
    if (PropsUi.getInstance().isDarkMode()) {
      RGB contrastingRGB = contrastingColors.get(rgb);
      if (contrastingRGB != null) {
        return contrastingRGB;
      }
    }
    return rgb;
  }

  public RGB contrastColor(int r, int g, int b) {
    return contrastColor(new RGB(r, g, b));
  }

  public Map<String, String> getContrastingColorStrings() {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<RGB, RGB> entry : contrastingColors.entrySet()) {
      RGB rgb = entry.getKey();
      RGB contrastingRGB = entry.getValue();

      String fromColor = toColorString(rgb);
      String toColor = toColorString(contrastingRGB);

      // Lowercase & uppercase
      //
      map.put(fromColor.toLowerCase(), toColor);
      map.put(fromColor.toUpperCase(), toColor);
    }
    return map;
  }

  private String toColorString(RGB rgb) {
    String r = Integer.toString(rgb.red, 16);
    r = r.length() == 1 ? "0" + r : r;
    String g = Integer.toString(rgb.green, 16);
    g = g.length() == 1 ? "0" + g : g;
    String b = Integer.toString(rgb.blue, 16);
    b = b.length() == 1 ? "0" + b : b;
    return ("#" + r + g + b).toLowerCase();
  }

  /**
   * Gets contrastingColors
   *
   * @return value of contrastingColors
   */
  public Map<RGB, RGB> getContrastingColors() {
    return contrastingColors;
  }

  /**
   * @param contrastingColors The contrastingColors to set
   */
  public void setContrastingColors(Map<RGB, RGB> contrastingColors) {
    this.contrastingColors = contrastingColors;
  }

  public double getGlobalZoomFactor() {
    return Const.toDouble(getProperty(GLOBAL_ZOOMFACTOR, "1.0"), 1.0);
  }

  public void setGlobalZoomFactor(double globalZoomFactor) {
    setProperty(GLOBAL_ZOOMFACTOR, Double.toString(globalZoomFactor));
  }

  public int getMaxExecutionLoggingTextSize() {
    return Const.toInt(
        getProperty(
            MAX_EXECUTION_LOGGING_TEXT_SIZE,
            Integer.toString(DEFAULT_MAX_EXECUTION_LOGGING_TEXT_SIZE)),
        DEFAULT_MAX_EXECUTION_LOGGING_TEXT_SIZE);
  }

  public void setMaxExecutionLoggingTextSize(int maxExecutionLoggingTextSize) {
    setProperty(MAX_EXECUTION_LOGGING_TEXT_SIZE, Integer.toString(maxExecutionLoggingTextSize));
  }

  protected static final String[] globalZoomFactorLevels =
      new String[] {
        "200%", "175%", "150%", "140%", "130%", "120%", "110%", "100%", "90%", "80%", "70%"
      };

  public static String[] getGlobalZoomFactorLevels() {
    return globalZoomFactorLevels;
  }

  public Layout createFormLayout() {
    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = getFormMargin();
    formLayout.marginTop = getFormMargin();
    formLayout.marginBottom = getFormMargin();
    formLayout.marginRight = getFormMargin();
    return formLayout;
  }
}
