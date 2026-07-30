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

package org.apache.hop.ui.hopgui.perspective.execution;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextVar;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.file.shared.TextZoom;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Text;

/**
 * Log tab content for pipeline/workflow execution viewers: toolbar (copy, zoom, filter) plus the
 * log text widget. Snapshot-based (not a live log sniffer).
 */
@GuiPlugin(description = "Execution Log Panel")
public class ExecutionLogPanel {

  private static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "ExecutionLogPanel-ToolBar";

  public static final String TOOLBAR_ICON_LOG_COPY_TO_CLIPBOARD =
      "ToolbarIcon-10000-LogCopyToClipboard";
  public static final String TOOLBAR_ICON_LOG_INCREASE_FONT = "ToolbarIcon-10010-LogIncreaseFont";
  public static final String TOOLBAR_ICON_LOG_DECREASE_FONT = "ToolbarIcon-10020-LogDecreaseFont";
  public static final String TOOLBAR_ICON_LOG_RESET_FONT = "ToolbarIcon-10030-LogResetFont";
  public static final String TOOLBAR_ICON_LOG_FILTER_TEXT = "ToolbarIcon-10040-LogFilterText";
  public static final String TOOLBAR_ICON_LOG_FILTER_HIGHLIGHT =
      "ToolbarIcon-10050-LogFilterHighlight";
  public static final String TOOLBAR_ICON_LOG_FILTER_CASE_SENSITIVE =
      "ToolbarIcon-10060-LogFilterCaseSensitive";
  public static final String TOOLBAR_ICON_LOG_FILTER_EXCLUDE = "ToolbarIcon-10070-LogFilterExclude";

  private Composite composite;
  private Control toolbar;
  private GuiToolbarWidgets toolBarWidgets;
  private TextComposite logText;
  private TextZoom textZoom;

  private String rawLoggingText = "";
  private String filterText = "";
  private boolean highlightMatches;
  private boolean caseSensitive;
  private boolean excludeMatches;

  @Getter private boolean created;

  /**
   * Build the log panel UI under the given parent (typically the execution viewer tab folder).
   *
   * @param parent parent composite
   * @return the root composite to set as the log tab control
   */
  public Control create(Composite parent) {
    composite = new Composite(parent, SWT.NONE);
    composite.setLayout(new FormLayout());
    PropsUi.setLook(composite);

    addToolBar();

    FormData fdToolbar = new FormData();
    fdToolbar.left = new FormAttachment(0, 0);
    fdToolbar.top = new FormAttachment(0, 0);
    fdToolbar.right = new FormAttachment(100, 0);
    toolbar.setLayoutData(fdToolbar);

    // Desktop: StyledText for highlight ranges; web: plain Text (StyledTextComp)
    if (EnvironmentUtils.getInstance().isWeb()) {
      logText =
          new StyledTextComp(
              Variables.getADefaultVariableSpace(),
              composite,
              SWT.READ_ONLY | SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    } else {
      logText =
          new StyledTextVar(
              Variables.getADefaultVariableSpace(),
              composite,
              SWT.READ_ONLY | SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
              false,
              false);
    }
    PropsUi.setLook(logText);
    FormData fdText = new FormData();
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.top = new FormAttachment(toolbar, 0);
    fdText.bottom = new FormAttachment(100, 0);
    logText.setLayoutData(fdText);

    textZoom = new TextZoom(logText, GuiResource.getInstance().getFontFixed());
    textZoom.resetFont();

    created = true;
    return composite;
  }

  private void addToolBar() {
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    toolbar = toolBarContainer.getControl();
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment(0, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    fdToolBar.right = new FormAttachment(100, 0);
    toolbar.setLayoutData(fdToolBar);
    PropsUi.setLook(toolbar, Props.WIDGET_STYLE_TOOLBAR);

    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);

    Control filterControl = toolBarWidgets.getControlForMenu(TOOLBAR_ICON_LOG_FILTER_TEXT);
    if (filterControl instanceof Text filterField) {
      filterField.addListener(SWT.Modify, event -> applyFilterFromToolbar());
    }

    toolbar.pack();
  }

  /**
   * Store the unfiltered log text from the execution location and refresh the display with the
   * current filter options.
   */
  public void setRawLoggingText(String text) {
    this.rawLoggingText = Const.NVL(text, "");
    applyDisplayFromRaw();
  }

  public String getDisplayedText() {
    if (logText == null || logText.isDisposed()) {
      return "";
    }
    return Const.NVL(logText.getText(), "");
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_COPY_TO_CLIPBOARD,
      toolTip = "i18n:org.apache.hop.ui.hopgui:PipelineLog.Button.LogCopyToClipboard",
      image = "ui/images/copy.svg")
  public void copyToClipboard() {
    GuiResource.getInstance().toClipboard(getDisplayedText());
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_INCREASE_FONT,
      toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.IncreaseFont",
      image = "ui/images/zoom-in.svg",
      separator = true)
  public void increaseFont() {
    if (textZoom != null) {
      textZoom.increaseFont();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_DECREASE_FONT,
      toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.DecreaseFont",
      image = "ui/images/zoom-out.svg")
  public void decreaseFont() {
    if (textZoom != null) {
      textZoom.decreaseFont();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_RESET_FONT,
      toolTip = "i18n:org.apache.hop.ui.hopgui:WorkflowLog.Button.ResetFont",
      image = "ui/images/zoom-100.svg")
  public void resetFont() {
    if (textZoom != null) {
      textZoom.resetFont();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_FILTER_TEXT,
      type = GuiToolbarElementType.TEXT,
      label = "i18n:org.apache.hop.ui.hopgui:LogBrowser.Filter.Text.Label",
      toolTip = "i18n:org.apache.hop.ui.hopgui:LogBrowser.Filter.Text.Tooltip",
      separator = true)
  public void filterTextChanged() {
    applyFilterFromToolbar();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_FILTER_HIGHLIGHT,
      type = GuiToolbarElementType.CHECKBOX,
      label = "i18n:org.apache.hop.ui.hopgui:LogBrowser.Filter.Highlight.Label",
      toolTip = "i18n:org.apache.hop.ui.hopgui:LogBrowser.Filter.Highlight.Tooltip")
  public void filterHighlightChanged() {
    applyFilterFromToolbar();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_FILTER_CASE_SENSITIVE,
      type = GuiToolbarElementType.CHECKBOX,
      label = "i18n:org.apache.hop.ui.hopgui:LogBrowser.Filter.CaseSensitive.Label",
      toolTip = "i18n:org.apache.hop.ui.hopgui:LogBrowser.Filter.CaseSensitive.Tooltip")
  public void filterCaseSensitiveChanged() {
    applyFilterFromToolbar();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_LOG_FILTER_EXCLUDE,
      type = GuiToolbarElementType.CHECKBOX,
      label = "i18n:org.apache.hop.ui.hopgui:LogBrowser.Filter.Exclude.Label",
      toolTip = "i18n:org.apache.hop.ui.hopgui:LogBrowser.Filter.Exclude.Tooltip")
  public void filterExcludeChanged() {
    applyFilterFromToolbar();
  }

  private void applyFilterFromToolbar() {
    if (toolBarWidgets == null || logText == null || logText.isDisposed()) {
      return;
    }

    String filter = "";
    Control filterControl = toolBarWidgets.getControlForMenu(TOOLBAR_ICON_LOG_FILTER_TEXT);
    if (filterControl instanceof Text textWidget && !textWidget.isDisposed()) {
      filter = Const.NVL(textWidget.getText(), "").trim();
    }

    this.filterText = filter;
    this.highlightMatches = isToolbarCheckboxSelected(TOOLBAR_ICON_LOG_FILTER_HIGHLIGHT);
    this.caseSensitive = isToolbarCheckboxSelected(TOOLBAR_ICON_LOG_FILTER_CASE_SENSITIVE);
    this.excludeMatches = isToolbarCheckboxSelected(TOOLBAR_ICON_LOG_FILTER_EXCLUDE);

    applyDisplayFromRaw();
  }

  private boolean isToolbarCheckboxSelected(String id) {
    Control control = toolBarWidgets.getControlForMenu(id);
    if (control instanceof Button button && !button.isDisposed()) {
      return button.getSelection();
    }
    return false;
  }

  private void applyDisplayFromRaw() {
    if (logText == null || logText.isDisposed()) {
      return;
    }

    // No filter: show the raw log as-is (no rebuild, no highlight styles).
    if (StringUtils.isEmpty(filterText)) {
      logText.setText(rawLoggingText);
      if (!logText.isDisposed()) {
        logText.setSelection(logText.getCharCount());
      }
      return;
    }

    String[] lines = splitLines(rawLoggingText);
    StringBuilder display = new StringBuilder(rawLoggingText.length());
    List<int[]> highlightSpans = new ArrayList<>(); // [start, length] in display text

    for (String line : lines) {
      if (!shouldDisplayLine(line)) {
        continue;
      }
      int lineStart = display.length();
      display.append(line).append(Const.CR);

      if (highlightMatches && lineMatches(line)) {
        collectHighlightSpans(line, lineStart, highlightSpans);
      }
    }

    String displayText = display.toString();
    StyledText styledText = getStyledText();
    if (styledText != null && !styledText.isDisposed()) {
      styledText.setText(displayText);
      for (int[] span : highlightSpans) {
        applyHighlightRange(styledText, span[0], span[1]);
      }
    } else {
      logText.setText(displayText);
    }

    if (!logText.isDisposed()) {
      logText.setSelection(logText.getCharCount());
    }
  }

  private static String[] splitLines(String text) {
    if (text == null || text.isEmpty()) {
      return new String[0];
    }
    // Preserve empty trailing behavior similar to String.split(-1)
    return text.split("\\r?\\n", -1);
  }

  private void collectHighlightSpans(String line, int lineStart, List<int[]> highlightSpans) {
    String haystack = caseSensitive ? line : line.toLowerCase();
    String needle = caseSensitive ? filterText : filterText.toLowerCase();
    if (needle.isEmpty()) {
      return;
    }
    int from = 0;
    while (from <= haystack.length() - needle.length()) {
      int idx = haystack.indexOf(needle, from);
      if (idx < 0) {
        break;
      }
      highlightSpans.add(new int[] {lineStart + idx, needle.length()});
      from = idx + Math.max(needle.length(), 1);
    }
  }

  private void applyHighlightRange(StyledText styledText, int start, int length) {
    StyleRange range = new StyleRange();
    range.start = start;
    range.length = length;
    range.fontStyle = SWT.NORMAL;
    // Avoid contrast-remapped colors for reliable dark-mode readability
    if (PropsUi.getInstance().isDarkMode()) {
      range.background = GuiResource.getInstance().getColor(180, 90, 0);
    } else {
      range.background = GuiResource.getInstance().getColorYellow();
    }
    styledText.setStyleRange(range);
  }

  private StyledText getStyledText() {
    if (logText instanceof StyledTextVar) {
      return ((StyledTextVar) logText).getTextWidget();
    }
    return null;
  }

  private boolean shouldDisplayLine(String line) {
    if (StringUtils.isEmpty(filterText)) {
      return true;
    }
    boolean matches = lineMatches(line);
    if (excludeMatches) {
      return !matches;
    }
    if (highlightMatches) {
      return true;
    }
    return matches;
  }

  private boolean lineMatches(String line) {
    if (StringUtils.isEmpty(filterText) || line == null) {
      return false;
    }
    if (caseSensitive) {
      return line.contains(filterText);
    }
    return StringUtils.containsIgnoreCase(line, filterText);
  }
}
