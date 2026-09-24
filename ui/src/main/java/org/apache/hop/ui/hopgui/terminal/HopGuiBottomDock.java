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

package org.apache.hop.ui.hopgui.terminal;

import java.util.function.Function;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.history.AuditStateMap;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.widget.TabFolderReorder;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.perspective.TabClosable;
import org.apache.hop.ui.hopgui.perspective.TabCloseHandler;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/**
 * Bottom panel for Hop GUI. It wraps the main perspectives composite in a vertical SashForm, with
 * the perspectives in the top section and a tabbed dock in the bottom section that persists across
 * perspective switches.
 *
 * <p>The dock hosts terminal tabs (a capability that can be turned off — see {@link
 * #terminalsEnabled}) and generic tool tabs opened through {@link #focusOrOpenToolTab} (search,
 * database, VFS file explorer, AI workbench). The "+" tab, font sizing, and terminal save/restore
 * apply only to terminal tabs.
 */
@GuiPlugin(name = "Terminal panel", description = "Terminal panel")
public class HopGuiBottomDock extends Composite implements TabClosable {

  private static final Class<?> PKG = HopGuiBottomDock.class;

  public static final String ID_MAIN_MENU_TOOLS_TERMINAL = "40010-menu-tools-terminal";
  public static final String ID_MAIN_MENU_TOOLS_NEW_TERMINAL = "40020-menu-tools-new-terminal";

  /** Selected-tool id for a terminal tab. Tool tabs use their own {@code DATA_TOOL_ID}. */
  public static final String TOOL_ID_TERMINAL = "terminal";

  /** Tool-id prefix for search-results tabs opened in this dock. */
  public static final String SEARCH_TOOL_ID_PREFIX = "search-";

  private final HopGui hopGui;

  private SashForm verticalSash;
  @Getter private Composite perspectiveComposite;
  private Composite bottomPanelComposite;
  private Composite terminalComposite;
  @Getter private CTabFolder terminalTabs;
  private CTabItem newTerminalTab;
  @Getter private boolean terminalVisible = false;
  @Getter private int terminalHeightPercent = 35;
  private boolean isClearing = false;
  private int terminalCounter = 1;

  /**
   * Whether the terminal capability is available. Off on Hop Web, when {@code
   * disabledGuiElements.xml} excludes the terminal menu, or when the user clears Enable embedded
   * terminal. Updated at runtime by {@link #setTerminalsEnabled(boolean)}.
   */
  @Getter private boolean terminalsEnabled = true;

  /** Font-size toolbar items. Present only while {@link #terminalsEnabled} is true. */
  private ToolBar dockToolBar;

  private ToolItem increaseFontItem;
  private ToolItem decreaseFontItem;
  private ToolItem resetFontItem;
  private ToolItem fontSeparatorItem;

  /** Counter used to give each non-singleton tool tab (e.g. a search result tab) a unique id. */
  private int toolTabCounter = 1;

  /** Font size scale for all terminal tabs (100 = 100%). Persisted and applied to new tabs. */
  private int terminalFontSizePercent = 100;

  private static final String TERMINAL_AUDIT_TYPE = "terminal";

  /** Reserved state key for panel visibility (minimized vs visible). Not a terminal tab. */
  private static final String STATE_PANEL_VISIBLE_KEY = "terminalPanelVisible";

  private static final String STATE_PANEL_VISIBLE_PROP = "visible";

  /** Reserved state key for terminal font size percent (e.g. 100 = 100%). */
  private static final String STATE_TERMINAL_FONT_SIZE_PERCENT_KEY = "terminalFontSizePercent";

  /** Reserved state key for the dock's height as a percentage of the vertical sash. */
  private static final String STATE_TERMINAL_HEIGHT_PERCENT_KEY = "terminalHeightPercent";

  // State map keys
  private static final String STATE_TAB_NAME = "tabName";
  private static final String STATE_SHELL_PATH = "shellPath";
  private static final String STATE_WORKING_DIR = "workingDirectory";

  /** Tab data keys marking a non-terminal "tool" tab (e.g. search results) and its content. */
  private static final String DATA_TOOL_ID = "dockToolId";

  private static final String DATA_TOOL_CONTENT = "dockToolContent";

  /**
   * Constructor - Creates the bottom dock structure
   *
   * @param parent The parent composite (mainHopGuiComposite from HopGui)
   * @param hopGui The HopGui instance
   * @param terminalsEnabled whether the integrated terminal capability is available (off on web or
   *     when disabled via disabledGuiElements.xml)
   */
  public HopGuiBottomDock(Composite parent, HopGui hopGui, boolean terminalsEnabled) {
    super(parent, SWT.NONE);
    this.hopGui = hopGui;
    this.terminalsEnabled = terminalsEnabled;

    createContents();
  }

  /**
   * The embedded terminal is on only when the user left it enabled, this is not Hop Web, and {@code
   * disabledGuiElements.xml} does not exclude the terminal menu.
   */
  public static boolean isTerminalCapabilityEnabled(
      boolean web, boolean disabledByRegistry, boolean userEnabled) {
    return userEnabled && !web && !disabledByRegistry;
  }

  /** Live capability check used by the sidebar and by {@code HopGui} at startup. */
  public static boolean isTerminalCapabilityEnabled() {
    return isTerminalCapabilityEnabled(
        EnvironmentUtils.getInstance().isWeb(),
        GuiRegistry.getDisabledGuiElements().contains(ID_MAIN_MENU_TOOLS_TERMINAL),
        PropsUi.getInstance().isEmbeddedTerminalEnabled());
  }

  /** Create the UI structure */
  private void createContents() {
    setLayout(new FormLayout());

    verticalSash = new SashForm(this, SWT.VERTICAL | SWT.SMOOTH);
    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.top = new FormAttachment(0, 0);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.bottom = new FormAttachment(100, 0);
    verticalSash.setLayoutData(fdSash);

    perspectiveComposite = new Composite(verticalSash, SWT.NONE);
    perspectiveComposite.setLayout(new FormLayout());

    bottomPanelComposite = new Composite(verticalSash, SWT.NONE);
    bottomPanelComposite.setLayout(new FormLayout());
    createBottomPanel();

    verticalSash.setMaximizedControl(perspectiveComposite);

    // Register with key handler so Ctrl+J / Cmd+J and Ctrl+Shift+J / Cmd+Shift+J work in this panel
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(this);
    hopGui.replaceKeyboardShortcutListeners(this, keyHandler);
  }

  /** Create the bottom panel with terminal */
  private void createBottomPanel() {
    // Terminal area directly in bottom panel composite
    terminalComposite = new Composite(bottomPanelComposite, SWT.NONE);
    terminalComposite.setLayout(new FormLayout());

    FormData fdTerminal = new FormData();
    fdTerminal.left = new FormAttachment(0, 0);
    fdTerminal.top = new FormAttachment(0, 0);
    fdTerminal.right = new FormAttachment(100, 0);
    fdTerminal.bottom = new FormAttachment(100, 0);
    terminalComposite.setLayoutData(fdTerminal);

    createTerminalArea();
  }

  /** Create the terminal area with tab folder */
  private void createTerminalArea() {
    terminalTabs = new CTabFolder(terminalComposite, SWT.MULTI | SWT.BORDER);
    PropsUi.setLook(terminalTabs, PropsUi.WIDGET_STYLE_TAB);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(0, 0);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(100, 0);
    terminalTabs.setLayoutData(fdTabs);

    createTerminalToolbar();
    createNewTerminalTab();

    new TabCloseHandler(this);
    new TabFolderReorder(terminalTabs);

    final boolean[] isClosingTab = {false};
    terminalTabs.addListener(
        SWT.Selection,
        event -> {
          CTabItem item = terminalTabs.getSelection();
          // When only the "+" tab exists, getSelection() can be null; create a new terminal.
          if (terminalsEnabled
              && item == null
              && terminalTabs.getItemCount() == 1
              && !isClearing
              && !isClosingTab[0]) {
            createNewTerminal(null, null);
            return;
          }
          if (item == newTerminalTab) {
            // Creation is handled by MouseDown so we don't double-create when both fire
            return;
          }

          if (item != null) {
            ITerminalWidget widget = (ITerminalWidget) item.getData("terminalWidget");
            if (widget != null) {
              Composite composite = widget.getTerminalComposite();
              if (composite != null && !composite.isDisposed()) {
                composite.forceFocus();
              }
            }
          }
          hopGui.refreshSidebarToolbarButtonStates();
        });

    // Ensure + tab click always creates a terminal (e.g. when it's the only tab and
    // Selection doesn't fire because selection doesn't change)
    terminalTabs.addListener(
        SWT.MouseDown,
        event -> {
          if (!terminalsEnabled) {
            return;
          }
          CTabItem item = terminalTabs.getItem(new Point(event.x, event.y));
          if (item == newTerminalTab && !isClearing && !isClosingTab[0]) {
            createNewTerminal(null, null);
          }
        });

    terminalTabs.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            isClosingTab[0] = true;
            try {
              CTabItem item = (CTabItem) event.item;
              if (item == newTerminalTab) {
                event.doit = false;
                return;
              }
              closeTab(event, item);
            } finally {
              getDisplay()
                  .asyncExec(
                      () -> {
                        isClosingTab[0] = false;
                      });
            }
          }
        });
    terminalTabs.addListener(
        SWT.MouseDoubleClick,
        event -> {
          CTabItem item = terminalTabs.getSelection();
          // Only terminal tabs can be renamed (not the "+" tab or generic tool tabs).
          if (item != null && item != newTerminalTab && item.getData("terminalWidget") != null) {
            renameTerminalTab(item);
          }
        });
  }

  public void createNewTerminal(String workingDirectory, String shellPath) {
    createNewTerminal(workingDirectory, shellPath, null);
  }

  public void createNewTerminal(String workingDirectory, String shellPath, String customTabName) {
    createNewTerminal(workingDirectory, shellPath, customTabName, true);
  }

  /**
   * @param reveal when true, show the dock, select the new tab, and focus the terminal. Restore
   *     passes false so a hidden session is recreated without stealing the current tool tab.
   */
  private void createNewTerminal(
      String workingDirectory, String shellPath, String customTabName, boolean reveal) {
    if (!terminalsEnabled) {
      return;
    }
    if (shellPath == null) {
      shellPath = TerminalShellDetector.detectDefaultShell();
    }

    if (workingDirectory == null) {
      workingDirectory = getDefaultWorkingDirectory();
    }

    int insertAt = terminalTabs.getItemCount();
    if (newTerminalTab != null && !newTerminalTab.isDisposed()) {
      insertAt = Math.min(1, terminalTabs.getItemCount());
    }
    CTabItem terminalTab = new CTabItem(terminalTabs, SWT.CLOSE, insertAt);

    String terminalId = "terminal-" + terminalCounter++ + "-" + System.currentTimeMillis();

    if (customTabName != null && !customTabName.trim().isEmpty()) {
      terminalTab.setText(customTabName);
    } else {
      String shellName = extractShellName(shellPath);
      terminalTab.setText(shellName + " (" + (terminalCounter - 1) + ")");
    }
    terminalTab.setImage(GuiResource.getInstance().getImageTerminal());
    terminalTab.setToolTipText(
        BaseMessages.getString(
            PKG, "HopGuiTerminalPanel.Tab.Tooltip", shellPath, workingDirectory));

    terminalTab.setData("terminalId", terminalId);
    terminalTab.setData("workingDirectory", workingDirectory);
    terminalTab.setData("shellPath", shellPath);

    Composite terminalWidgetComposite = new Composite(terminalTabs, SWT.NONE);
    terminalWidgetComposite.setLayout(new FormLayout());
    terminalTab.setControl(terminalWidgetComposite);

    ITerminalWidget terminalWidget =
        new JediTerminalWidget(
            terminalWidgetComposite, shellPath, workingDirectory, getTerminalFontSizePercent());

    terminalTab.setData("terminalWidget", terminalWidget);

    updateTabTextWithTerminalType(terminalTab, terminalWidget);

    registerTerminal(terminalId, workingDirectory, shellPath);

    if (reveal) {
      terminalTabs.setSelection(terminalTab);
      if (!isDockVisible()) {
        showDock();
      }
      focusTerminalComposite(terminalTab);
    }
  }

  private void focusTerminalComposite(CTabItem terminalTab) {
    if (terminalTab == null || terminalTab.isDisposed()) {
      return;
    }
    ITerminalWidget terminalWidget = (ITerminalWidget) terminalTab.getData("terminalWidget");
    if (terminalWidget == null) {
      return;
    }
    getDisplay()
        .asyncExec(
            () -> {
              Composite composite = terminalWidget.getTerminalComposite();
              if (composite != null && !composite.isDisposed()) {
                composite.setFocus();
                composite.forceFocus();
              }
            });
  }

  /** Extract shell name from full path (e.g., "/bin/bash" -> "bash") */
  private String extractShellName(String shellPath) {
    if (shellPath == null || shellPath.isEmpty()) {
      return BaseMessages.getString(PKG, "HopGuiTerminalPanel.ShellName.Default");
    }

    // Handle Windows paths
    if (shellPath.contains("\\")) {
      int lastBackslash = shellPath.lastIndexOf('\\');
      shellPath = shellPath.substring(lastBackslash + 1);
    }

    // Handle Unix paths
    if (shellPath.contains("/")) {
      int lastSlash = shellPath.lastIndexOf('/');
      shellPath = shellPath.substring(lastSlash + 1);
    }

    // Remove .exe extension
    if (shellPath.endsWith(".exe")) {
      shellPath = shellPath.substring(0, shellPath.length() - 4);
    }

    return shellPath;
  }

  private void updateTabTextWithTerminalType(CTabItem terminalTab, ITerminalWidget terminalWidget) {
    if (terminalTab == null || terminalWidget == null) {
      return;
    }

    String currentText = terminalTab.getText();
    String indicator = " [JT]";

    if (!currentText.contains(indicator)) {
      terminalTab.setText(currentText + indicator);
    }
  }

  /** True when the bottom panel is on screen. */
  public boolean isDockVisible() {
    return terminalVisible;
  }

  /** Make the dock visible without creating a terminal. */
  public void showDock() {
    if (!terminalVisible) {
      verticalSash.setMaximizedControl(null);
      int perspectivePercent = 100 - terminalHeightPercent;
      verticalSash.setWeights(perspectivePercent, terminalHeightPercent);
      terminalVisible = true;
      layout(true, true);
      hopGui.refreshSidebarToolbarButtonStates();
    }
  }

  /** Show or hide the bottom panel. Does not create a terminal. */
  public void toggleDock() {
    if (isDockVisible()) {
      hideDock();
    } else {
      showDock();
    }
  }

  /** Hide the bottom panel. */
  public void hideDock() {
    if (terminalVisible) {
      terminalVisible = false;
      verticalSash.setMaximizedControl(perspectiveComposite);
      layout(true, true);
      hopGui.refreshSidebarToolbarButtonStates();
    }
  }

  /** Show the dock and make sure at least one terminal is present. */
  public void showTerminal() {
    if (!terminalsEnabled) {
      return;
    }
    showDock();
    if (countTerminalTabs() == 0) {
      createNewTerminal(null, null);
    }
  }

  /** Hide the dock. Kept for callers that still use the terminal name. */
  public void hideTerminal() {
    hideDock();
  }

  /**
   * Show the dock and focus a terminal tab, creating one when none is open. Does nothing when the
   * terminal capability is off.
   */
  public void focusTerminal() {
    if (!terminalsEnabled) {
      return;
    }
    CTabItem terminal = findLastTerminalTab();
    if (terminal == null) {
      showTerminal();
      return;
    }
    selectTab(terminal);
    focusTerminalComposite(terminal);
  }

  /**
   * Id of the selected tab: {@link #TOOL_ID_TERMINAL} for a terminal, the tool id for a tool tab,
   * or null for the "+" tab and an empty selection.
   */
  public String getSelectedToolId() {
    if (terminalTabs == null || terminalTabs.isDisposed()) {
      return null;
    }
    CTabItem selected = terminalTabs.getSelection();
    if (selected == null || selected == newTerminalTab || selected.isDisposed()) {
      return null;
    }
    if (selected.getData("terminalWidget") != null) {
      return TOOL_ID_TERMINAL;
    }
    Object toolId = selected.getData(DATA_TOOL_ID);
    return toolId instanceof String id ? id : null;
  }

  /**
   * True when {@code toolId} is the selected tab. A value of {@link #SEARCH_TOOL_ID_PREFIX} matches
   * any search-results tab.
   */
  public boolean isToolSelected(String toolId) {
    String selected = getSelectedToolId();
    if (selected == null || toolId == null) {
      return false;
    }
    if (SEARCH_TOOL_ID_PREFIX.equals(toolId)) {
      return selected.startsWith(SEARCH_TOOL_ID_PREFIX);
    }
    return toolId.equals(selected);
  }

  /**
   * True when the panel is visible and a tab for {@code toolId} is open. {@link
   * #SEARCH_TOOL_ID_PREFIX} matches any search-results tab. {@link #TOOL_ID_TERMINAL} matches any
   * terminal tab. Used for the sidebar highlight, which stays on for every open tool while the
   * panel is showing.
   */
  public boolean isToolOpen(String toolId) {
    if (!isDockVisible() || toolId == null || terminalTabs == null || terminalTabs.isDisposed()) {
      return false;
    }
    if (TOOL_ID_TERMINAL.equals(toolId)) {
      return findLastTerminalTab() != null;
    }
    if (SEARCH_TOOL_ID_PREFIX.equals(toolId)) {
      return findLastToolTabByPrefix(SEARCH_TOOL_ID_PREFIX) != null;
    }
    CTabItem item = findToolTab(toolId);
    return item != null && !item.isDisposed();
  }

  /** Select a tab and make sure the dock is visible. */
  public void selectTab(CTabItem item) {
    if (item == null || item.isDisposed()) {
      return;
    }
    showDock();
    terminalTabs.setSelection(item);
    hopGui.refreshSidebarToolbarButtonStates();
  }

  /** Content control stored on a tool tab, or null. */
  public Control getToolContent(CTabItem item) {
    if (item == null || item.isDisposed()) {
      return null;
    }
    Object content = item.getData(DATA_TOOL_CONTENT);
    return content instanceof Control control ? control : null;
  }

  /** Last open terminal tab, or null. */
  public CTabItem findLastTerminalTab() {
    CTabItem last = null;
    if (terminalTabs == null || terminalTabs.isDisposed()) {
      return null;
    }
    for (CTabItem item : terminalTabs.getItems()) {
      if (item != newTerminalTab && item.getData("terminalWidget") != null) {
        last = item;
      }
    }
    return last;
  }

  /** Last tool tab whose id starts with {@code prefix}, or null. */
  public CTabItem findLastToolTabByPrefix(String prefix) {
    if (prefix == null || terminalTabs == null || terminalTabs.isDisposed()) {
      return null;
    }
    CTabItem last = null;
    for (CTabItem item : terminalTabs.getItems()) {
      Object id = item.getData(DATA_TOOL_ID);
      if (id instanceof String toolId && toolId.startsWith(prefix)) {
        last = item;
      }
    }
    return last;
  }

  /** Next id for a search-results tab ({@code search-1}, {@code search-2}, ...). */
  public String nextSearchToolId() {
    return SEARCH_TOOL_ID_PREFIX + (toolTabCounter++);
  }

  /** Number of real terminal tabs currently open (excludes the "+" tab and any tool tabs). */
  private int countTerminalTabs() {
    int count = 0;
    for (CTabItem item : terminalTabs.getItems()) {
      if (item != newTerminalTab && item.getData("terminalWidget") != null) {
        count++;
      }
    }
    return count;
  }

  // --- Generic tool tabs (non-terminal dock content) ---------------------------------------------

  /**
   * Open a non-terminal "tool" tab in the dock, or focus it if a tab with the same {@code toolId}
   * is already open. The dock is made visible. The content is created lazily by {@code
   * contentFactory} with the tab's container as its parent.
   *
   * @param toolId a stable identifier used to find/refocus the tab
   * @param title the tab title
   * @param image the tab image (may be null)
   * @param closable whether the user can close the tab
   * @param contentFactory builds the tab content given its container
   * @return the content control of the (new or existing) tab
   */
  public Control focusOrOpenToolTab(
      String toolId,
      String title,
      Image image,
      boolean closable,
      Function<Composite, Control> contentFactory) {
    CTabItem existing = findToolTab(toolId);
    if (existing != null) {
      showDock();
      terminalTabs.setSelection(existing);
      return (Control) existing.getData(DATA_TOOL_CONTENT);
    }
    return createToolTab(toolId, title, image, closable, contentFactory);
  }

  /**
   * Always open a <em>new</em> tool tab (for tools that allow several instances, e.g. multiple
   * search-result tabs). The dock is made visible and the new tab selected.
   *
   * @param title the tab title
   * @param image the tab image (may be null)
   * @param closable whether the user can close the tab
   * @param contentFactory builds the tab content given its container
   * @return the content control of the new tab
   */
  public Control openToolTab(
      String title, Image image, boolean closable, Function<Composite, Control> contentFactory) {
    return openToolTab(null, title, image, closable, contentFactory);
  }

  /**
   * Always open a new tool tab. {@code toolId} is stored on the tab so it can be found again; a
   * blank id gets a generated {@code tool-} id.
   */
  public Control openToolTab(
      String toolId,
      String title,
      Image image,
      boolean closable,
      Function<Composite, Control> contentFactory) {
    String id = StringUtils.isEmpty(toolId) ? "tool-" + (toolTabCounter++) : toolId;
    return createToolTab(id, title, image, closable, contentFactory);
  }

  private Control createToolTab(
      String toolId,
      String title,
      Image image,
      boolean closable,
      Function<Composite, Control> contentFactory) {
    CTabItem item = new CTabItem(terminalTabs, closable ? SWT.CLOSE : SWT.NONE);
    item.setText(title);
    if (image != null) {
      item.setImage(image);
    }
    item.setData(DATA_TOOL_ID, toolId);

    Composite container = new Composite(terminalTabs, SWT.NONE);
    container.setLayout(new FormLayout());
    item.setControl(container);

    Control content = contentFactory.apply(container);
    FormData fdContent = new FormData();
    fdContent.left = new FormAttachment(0, 0);
    fdContent.top = new FormAttachment(0, 0);
    fdContent.right = new FormAttachment(100, 0);
    fdContent.bottom = new FormAttachment(100, 0);
    content.setLayoutData(fdContent);
    item.setData(DATA_TOOL_CONTENT, content);

    // Attach shortcuts
    hopGui.replaceKeyboardShortcutListeners(container, HopGuiKeyHandler.getInstance());

    showDock();
    terminalTabs.setSelection(item);
    return content;
  }

  /** Find an open tool tab by its tool id, or {@code null} when not open. */
  public CTabItem findToolTab(String toolId) {
    if (toolId == null) {
      return null;
    }
    for (CTabItem item : terminalTabs.getItems()) {
      if (toolId.equals(item.getData(DATA_TOOL_ID))) {
        return item;
      }
    }
    return null;
  }

  /**
   * Show the terminal, or hide the panel when a terminal tab is already selected. Does nothing when
   * the terminal capability is off.
   */
  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_TERMINAL,
      label = "i18n::HopGuiTerminalPanel.Menu.Terminal",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID)
  @GuiKeyboardShortcut(control = true, key = 'j', global = true)
  @GuiOsxKeyboardShortcut(command = true, key = 'j', global = true)
  public void toggleTerminal() {
    if (EnvironmentUtils.getInstance().isWeb() || !terminalsEnabled) {
      return;
    }
    if (isDockVisible() && isToolSelected(TOOL_ID_TERMINAL)) {
      hideDock();
    } else {
      focusTerminal();
    }
  }

  /** Open a new terminal tab */
  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_NEW_TERMINAL,
      label = "i18n::HopGuiTerminalPanel.Menu.NewTerminal",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID)
  @GuiKeyboardShortcut(control = true, shift = true, key = 'j', global = true)
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'j', global = true)
  public void newTerminal() {
    if (EnvironmentUtils.getInstance().isWeb() || !terminalsEnabled) {
      return;
    }
    createNewTerminal(null, null);
  }

  /** Close a terminal tab (implements TabClosable interface) */
  @Override
  public void closeTab(CTabFolderEvent event, CTabItem tabItem) {
    if (tabItem == newTerminalTab) {
      if (event != null) {
        event.doit = false;
      }
      return;
    }

    ITerminalWidget widget = (ITerminalWidget) tabItem.getData("terminalWidget");
    if (widget != null) {
      widget.dispose();
    }

    String terminalId = (String) tabItem.getData("terminalId");
    if (terminalId != null) {
      unregisterTerminal(terminalId);
    }

    Object toolContent = tabItem.getData(DATA_TOOL_CONTENT);
    if (toolContent instanceof Control content && !content.isDisposed()) {
      content.dispose();
    }
    Control tabControl = tabItem.getControl();
    if (tabControl != null && !tabControl.isDisposed()) {
      tabControl.dispose();
    }

    tabItem.dispose();
    hopGui.refreshSidebarToolbarButtonStates();
  }

  /** Get the terminal tabs folder (implements TabClosable interface) */
  @Override
  public CTabFolder getTabFolder() {
    return terminalTabs;
  }

  /** Get all tabs to the right (excluding the + tab) */
  @Override
  public java.util.List<CTabItem> getTabsToRight(CTabItem selectedTabItem) {
    java.util.List<CTabItem> items = new java.util.ArrayList<>();
    for (int i = getTabFolder().getItems().length - 1; i >= 0; i--) {
      CTabItem item = getTabFolder().getItems()[i];
      if (selectedTabItem.equals(item)) {
        break;
      } else if (item != newTerminalTab) {
        items.add(item);
      }
    }
    return items;
  }

  /** Get all tabs to the left (excluding the + tab) */
  @Override
  public java.util.List<CTabItem> getTabsToLeft(CTabItem selectedTabItem) {
    java.util.List<CTabItem> items = new java.util.ArrayList<>();
    for (CTabItem item : getTabFolder().getItems()) {
      if (selectedTabItem.equals(item)) {
        break;
      } else if (item != newTerminalTab) {
        items.add(item);
      }
    }
    return items;
  }

  /** Get all other tabs (excluding the + tab) */
  @Override
  public java.util.List<CTabItem> getOtherTabs(CTabItem selectedTabItem) {
    java.util.List<CTabItem> items = new java.util.ArrayList<>();
    for (CTabItem item : getTabFolder().getItems()) {
      if (!selectedTabItem.equals(item) && item != newTerminalTab) {
        items.add(item);
      }
    }
    return items;
  }

  /** Create toolbar with font size controls and panel controls (maximize/minimize, close) */
  private void createTerminalToolbar() {
    dockToolBar = new ToolBar(terminalTabs, SWT.FLAT);
    terminalTabs.setTopRight(dockToolBar, SWT.RIGHT);
    PropsUi.setLook(dockToolBar);

    GuiResource gui = GuiResource.getInstance();
    if (PropsUi.getInstance().isDarkMode()) {
      dockToolBar.setBackground(gui.getColorWhite());
    } else {
      dockToolBar.setBackground(terminalTabs.getBackground());
    }

    createFontToolItems();

    // Maximize/Minimize button
    final ToolItem maximizeItem = new ToolItem(dockToolBar, SWT.PUSH);
    maximizeItem.setImage(GuiResource.getInstance().getImageMaximizePanel());
    maximizeItem.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.Maximize"));
    maximizeItem.addListener(
        SWT.Selection,
        e -> {
          if (verticalSash.getMaximizedControl() == null) {
            // Maximize the bottom panel
            verticalSash.setMaximizedControl(bottomPanelComposite);
            maximizeItem.setImage(GuiResource.getInstance().getImageMinimizePanel());
            maximizeItem.setToolTipText(
                BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.Restore"));
          } else {
            // Restore normal split
            verticalSash.setMaximizedControl(null);
            verticalSash.setWeights(100 - terminalHeightPercent, terminalHeightPercent);
            maximizeItem.setImage(GuiResource.getInstance().getImageMaximizePanel());
            maximizeItem.setToolTipText(
                BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.Maximize"));
          }
        });

    // Close button
    final ToolItem closeItem = new ToolItem(dockToolBar, SWT.PUSH);
    closeItem.setImage(GuiResource.getInstance().getImageClose());
    closeItem.setToolTipText(BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.Close"));
    closeItem.addListener(SWT.Selection, e -> hideDock());

    int height = dockToolBar.computeSize(SWT.DEFAULT, SWT.DEFAULT).y;
    terminalTabs.setTabHeight(Math.max(height, terminalTabs.getTabHeight()));
  }

  /** Font-size controls sit in front of maximize and close, and only while the terminal is on. */
  private void createFontToolItems() {
    if (!terminalsEnabled || dockToolBar == null || dockToolBar.isDisposed()) {
      return;
    }
    if (increaseFontItem != null && !increaseFontItem.isDisposed()) {
      return;
    }
    increaseFontItem = new ToolItem(dockToolBar, SWT.PUSH, 0);
    increaseFontItem.setImage(GuiResource.getInstance().getImage("ui/images/zoom-in.svg", 16, 16));
    increaseFontItem.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.IncreaseFont"));
    increaseFontItem.addListener(SWT.Selection, e -> increaseTerminalFontSize());

    decreaseFontItem = new ToolItem(dockToolBar, SWT.PUSH, 1);
    decreaseFontItem.setImage(GuiResource.getInstance().getImage("ui/images/zoom-out.svg", 16, 16));
    decreaseFontItem.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.DecreaseFont"));
    decreaseFontItem.addListener(SWT.Selection, e -> decreaseTerminalFontSize());

    resetFontItem = new ToolItem(dockToolBar, SWT.PUSH, 2);
    resetFontItem.setImage(GuiResource.getInstance().getImage("ui/images/zoom-100.svg", 16, 16));
    resetFontItem.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.ResetFont"));
    resetFontItem.addListener(SWT.Selection, e -> resetTerminalFontSize());

    fontSeparatorItem = new ToolItem(dockToolBar, SWT.SEPARATOR, 3);
  }

  private void disposeFontToolItems() {
    disposeToolItem(fontSeparatorItem);
    disposeToolItem(resetFontItem);
    disposeToolItem(decreaseFontItem);
    disposeToolItem(increaseFontItem);
    fontSeparatorItem = null;
    resetFontItem = null;
    decreaseFontItem = null;
    increaseFontItem = null;
  }

  private static void disposeToolItem(ToolItem item) {
    if (item != null && !item.isDisposed()) {
      item.dispose();
    }
  }

  private void createNewTerminalTab() {
    if (!terminalsEnabled) {
      return;
    }
    if (newTerminalTab != null && !newTerminalTab.isDisposed()) {
      return;
    }
    newTerminalTab = new CTabItem(terminalTabs, SWT.NONE, 0);
    newTerminalTab.setText("+");
    newTerminalTab.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiTerminalPanel.NewTab.Tooltip"));
    Composite placeholder = new Composite(terminalTabs, SWT.NONE);
    newTerminalTab.setControl(placeholder);
  }

  private void disposeNewTerminalTab() {
    if (newTerminalTab == null || newTerminalTab.isDisposed()) {
      newTerminalTab = null;
      return;
    }
    Control control = newTerminalTab.getControl();
    newTerminalTab.dispose();
    if (control != null && !control.isDisposed()) {
      control.dispose();
    }
    newTerminalTab = null;
  }

  /**
   * Turn the embedded terminal on or off without restarting. Turning it off closes PTY tabs and
   * drops the "+" tab and font controls. Turning it on puts those back and does not open a shell.
   */
  public void setTerminalsEnabled(boolean enabled) {
    if (this.terminalsEnabled == enabled) {
      return;
    }
    this.terminalsEnabled = enabled;
    if (!enabled) {
      // Save first. Disposing the tabs must not write an empty list over the open sessions, so a
      // later restore (after the user turns the terminal back on) can recreate them.
      saveOpenTerminals();
      disposeTerminalTabs();
      disposeNewTerminalTab();
      disposeFontToolItems();
      if (!hasContentTabs()) {
        hideDock();
      }
    } else {
      createNewTerminalTab();
      createFontToolItems();
    }
  }

  /**
   * Drop terminal tabs and their PTY widgets without updating the saved terminal list. {@link
   * #closeTab} is the path that forgets a tab the user closed.
   */
  private void disposeTerminalTabs() {
    java.util.List<CTabItem> itemsToClose = new java.util.ArrayList<>();
    for (CTabItem item : terminalTabs.getItems()) {
      if (!item.isDisposed() && item.getData("terminalWidget") != null) {
        itemsToClose.add(item);
      }
    }
    for (CTabItem item : itemsToClose) {
      disposeTerminalTab(item);
    }
    hopGui.refreshSidebarToolbarButtonStates();
  }

  private void disposeTerminalTab(CTabItem item) {
    if (item == null || item.isDisposed()) {
      return;
    }
    ITerminalWidget widget = (ITerminalWidget) item.getData("terminalWidget");
    if (widget != null) {
      widget.dispose();
    }
    Control tabControl = item.getControl();
    item.dispose();
    if (tabControl != null && !tabControl.isDisposed()) {
      tabControl.dispose();
    }
  }

  /** True when a tab other than "+" is open (a terminal or a tool). */
  private boolean hasContentTabs() {
    for (CTabItem item : terminalTabs.getItems()) {
      if (item != newTerminalTab && !item.isDisposed()) {
        return true;
      }
    }
    return false;
  }

  private boolean hasToolTabs() {
    for (CTabItem item : terminalTabs.getItems()) {
      if (!item.isDisposed() && item.getData(DATA_TOOL_ID) != null) {
        return true;
      }
    }
    return false;
  }

  private void increaseTerminalFontSize() {
    terminalFontSizePercent = Math.min(200, terminalFontSizePercent + 10);
    applyFontSizeToAllTerminals();
    saveOpenTerminals();
  }

  private void decreaseTerminalFontSize() {
    terminalFontSizePercent = Math.max(50, terminalFontSizePercent - 10);
    applyFontSizeToAllTerminals();
    saveOpenTerminals();
  }

  private void resetTerminalFontSize() {
    terminalFontSizePercent = 100;
    applyFontSizeToAllTerminals();
    saveOpenTerminals();
  }

  /** Apply current terminal font size percent to all open terminal tabs. */
  private void applyFontSizeToAllTerminals() {
    for (CTabItem item : terminalTabs.getItems()) {
      if (item == newTerminalTab) {
        continue;
      }
      ITerminalWidget widget = (ITerminalWidget) item.getData("terminalWidget");
      if (widget != null) {
        widget.setFontScalePercent(terminalFontSizePercent);
      }
    }
  }

  private int getTerminalFontSizePercent() {
    return terminalFontSizePercent;
  }

  /** Rename a terminal tab via dialog */
  private void renameTerminalTab(CTabItem item) {
    if (item == null || item == newTerminalTab) {
      return;
    }

    final Text text = new Text(terminalTabs, SWT.BORDER);
    text.setText(item.getText());

    Rectangle bounds = item.getBounds();
    text.setBounds(bounds.x, bounds.y, bounds.width, bounds.height);
    text.moveAbove(null);

    text.setFocus();
    text.selectAll();

    text.addListener(
        SWT.Traverse,
        event -> {
          if (event.detail == SWT.TRAVERSE_RETURN) {
            String newName = text.getText().trim();
            if (!newName.isEmpty()) {
              item.setText(newName);
              saveOpenTerminals();
            }
            text.dispose();
            event.doit = false;
          } else if (event.detail == SWT.TRAVERSE_ESCAPE) {
            text.dispose();
            event.doit = false;
          }
        });

    text.addListener(
        SWT.FocusOut,
        event -> {
          if (!text.isDisposed()) {
            String newName = text.getText().trim();
            if (!newName.isEmpty()) {
              item.setText(newName);
              saveOpenTerminals();
            }
            text.dispose();
          }
        });
  }

  /** Save terminals on shutdown */
  public void saveTerminalsOnShutdown() {
    saveOpenTerminals();
  }

  /** Save all open terminals and panel visibility */
  private void saveOpenTerminals() {
    try {
      java.util.List<String> terminalIds = new java.util.ArrayList<>();
      AuditStateMap stateMap = new AuditStateMap();

      for (CTabItem item : terminalTabs.getItems()) {
        if (item == newTerminalTab) {
          continue;
        }
        String terminalId = (String) item.getData("terminalId");
        if (terminalId != null) {
          terminalIds.add(terminalId);

          java.util.Map<String, Object> state = new java.util.HashMap<>();
          state.put(STATE_TAB_NAME, item.getText());
          state.put(STATE_WORKING_DIR, item.getData("workingDirectory"));
          state.put(STATE_SHELL_PATH, item.getData("shellPath"));

          stateMap.add(new AuditState(terminalId, state));
        }
      }

      // Persist panel visibility so we don't reopen when user had minimized the terminal
      stateMap.add(
          new AuditState(
              STATE_PANEL_VISIBLE_KEY,
              java.util.Map.of(STATE_PANEL_VISIBLE_PROP, Boolean.valueOf(terminalVisible))));

      stateMap.add(
          new AuditState(
              STATE_TERMINAL_FONT_SIZE_PERCENT_KEY,
              java.util.Map.of("value", Integer.valueOf(terminalFontSizePercent))));

      // Persist the dock height. When visible, read the live sash ratio so a user-dragged divider
      // is captured (there is no drag listener updating terminalHeightPercent); otherwise keep the
      // last known value.
      stateMap.add(
          new AuditState(
              STATE_TERMINAL_HEIGHT_PERCENT_KEY,
              java.util.Map.of("value", Integer.valueOf(getCurrentTerminalHeightPercent()))));

      AuditList auditList = new AuditList(terminalIds);
      AuditManager.getActive()
          .storeList(HopNamespace.getNamespace(), TERMINAL_AUDIT_TYPE, auditList);

      AuditManager.getActive()
          .saveAuditStateMap(HopNamespace.getNamespace(), TERMINAL_AUDIT_TYPE, stateMap);

      hopGui
          .getLog()
          .logDebug("Saved " + terminalIds.size() + " open terminal(s) for current project");
    } catch (Exception e) {
      hopGui
          .getLog()
          .logError(BaseMessages.getString(PKG, "HopGuiTerminalPanel.Error.SavingTerminals"), e);
    }
  }

  /** Clear all terminals */
  public void clearAllTerminals() {
    if (isDisposed() || terminalTabs == null || terminalTabs.isDisposed()) {
      hopGui.getLog().logDebug("clearAllTerminals: skipped (disposed or not initialized)");
      return;
    }

    isClearing = true;

    try {
      // Persist this project's terminals before the tabs go away. disposeTerminalTabs does not
      // write the audit, so the list survives for the next visit to this project.
      saveOpenTerminals();
      disposeTerminalTabs();
      if (!hasContentTabs()) {
        hideDock();
      }
    } finally {
      isClearing = false;
    }
  }

  private void registerTerminal(String terminalId, String workingDirectory, String shellPath) {
    // Terminal state is saved on shutdown
  }

  private void unregisterTerminal(String terminalId) {
    saveOpenTerminals();
  }

  private String getDefaultWorkingDirectory() {
    try {
      String projectHome = hopGui.getVariables().getVariable("PROJECT_HOME");
      if (StringUtils.isNotEmpty(projectHome)) {
        projectHome = hopGui.getVariables().resolve(projectHome);
        if (StringUtils.isNotEmpty(projectHome)) {
          return projectHome;
        }
      }
    } catch (Exception e) {
      // Ignore
    }

    return System.getProperty("user.home");
  }

  /** Restore terminals from previous session; respects saved panel visibility (minimized state). */
  public void restoreTerminals() {
    if (!terminalsEnabled) {
      return;
    }
    try {
      String namespace = HopNamespace.getNamespace();

      // Tool tabs (search, database, VFS, AI) stay open across a project switch and must not block
      // restoring this project's terminals.
      if (countTerminalTabs() > 0) {
        return;
      }

      AuditList auditList = AuditManager.getActive().retrieveList(namespace, TERMINAL_AUDIT_TYPE);

      AuditStateMap stateMap;
      try {
        stateMap =
            AuditManager.getActive()
                .loadAuditStateMap(HopNamespace.getNamespace(), TERMINAL_AUDIT_TYPE);
      } catch (Exception e) {
        hopGui
            .getLog()
            .logError(BaseMessages.getString(PKG, "HopGuiTerminalPanel.Error.LoadingStateMap"), e);
        stateMap = new AuditStateMap();
      }

      // Restore panel visibility: if user had minimized (hidden) the terminal, keep it hidden
      boolean savedPanelVisible = true;
      AuditState panelVisibleState = stateMap.get(STATE_PANEL_VISIBLE_KEY);
      if (panelVisibleState != null
          && panelVisibleState.getStateMap() != null
          && panelVisibleState.getStateMap().get(STATE_PANEL_VISIBLE_PROP) instanceof Boolean) {
        savedPanelVisible =
            Boolean.TRUE.equals(panelVisibleState.getStateMap().get(STATE_PANEL_VISIBLE_PROP));
      }

      // Restore terminal font size percent
      AuditState fontSizeState = stateMap.get(STATE_TERMINAL_FONT_SIZE_PERCENT_KEY);
      if (fontSizeState != null
          && fontSizeState.getStateMap() != null
          && fontSizeState.getStateMap().get("value") != null) {
        int saved = Const.toInt(fontSizeState.getStateMap().get("value").toString(), 100);
        terminalFontSizePercent = Math.clamp(saved, 50, 200);
      }

      // Restore dock height percent (applied by showDock/showTerminal when the panel is shown)
      AuditState heightState = stateMap.get(STATE_TERMINAL_HEIGHT_PERCENT_KEY);
      if (heightState != null
          && heightState.getStateMap() != null
          && heightState.getStateMap().get("value") != null) {
        int saved = Const.toInt(heightState.getStateMap().get("value").toString(), 35);
        terminalHeightPercent = Math.clamp(saved, 5, 95);
      }

      if (auditList.getNames().isEmpty()) {
        return;
      }

      // A tool tab the user is looking at stays put when this project last had the panel hidden.
      // Otherwise a visible save selects the restored terminal, and a hidden save with no tool tab
      // keeps the shells off screen.
      boolean keepToolTab = !savedPanelVisible && hasToolTabs() && isDockVisible();
      boolean reveal = savedPanelVisible;

      for (String terminalId : auditList.getNames()) {
        if (STATE_PANEL_VISIBLE_KEY.equals(terminalId)
            || STATE_TERMINAL_FONT_SIZE_PERCENT_KEY.equals(terminalId)
            || STATE_TERMINAL_HEIGHT_PERCENT_KEY.equals(terminalId)) {
          continue;
        }
        String customTabName = null;
        String workingDir = null;
        String shellPath = null;

        AuditState state = stateMap.get(terminalId);
        if (state != null && state.getStateMap() != null) {
          Object tabNameObj = state.getStateMap().get(STATE_TAB_NAME);
          if (tabNameObj != null) {
            customTabName = tabNameObj.toString();
          }
          Object workingDirObj = state.getStateMap().get(STATE_WORKING_DIR);
          if (workingDirObj != null) {
            workingDir = workingDirObj.toString();
          }
          Object shellPathObj = state.getStateMap().get(STATE_SHELL_PATH);
          if (shellPathObj != null) {
            shellPath = shellPathObj.toString();
          }
        }

        createNewTerminal(workingDir, shellPath, customTabName, reveal);
      }

      if (!savedPanelVisible && !keepToolTab) {
        hideDock();
      }
    } catch (Exception e) {
      hopGui
          .getLog()
          .logError(BaseMessages.getString(PKG, "HopGuiTerminalPanel.Error.RestoringTerminals"), e);
    }
  }

  /**
   * The dock's current height as a percentage of the vertical sash. When the dock is visible this
   * reads the live sash weights (capturing a user-dragged divider); otherwise it returns the last
   * known {@link #terminalHeightPercent}.
   */
  private int getCurrentTerminalHeightPercent() {
    if (terminalVisible
        && verticalSash != null
        && !verticalSash.isDisposed()
        && verticalSash.getMaximizedControl() == null) {
      int[] weights = verticalSash.getWeights();
      if (weights.length == 2) {
        long total = (long) weights[0] + weights[1];
        if (total > 0) {
          int percent = (int) Math.round(weights[1] * 100.0 / total);
          if (percent > 0 && percent < 100) {
            terminalHeightPercent = percent;
          }
        }
      }
    }
    return terminalHeightPercent;
  }

  /** Set terminal height percentage */
  public void setTerminalHeightPercent(int percent) {
    if (percent > 0 && percent < 100) {
      this.terminalHeightPercent = percent;
      if (terminalVisible) {
        int perspectivePercent = 100 - terminalHeightPercent;
        verticalSash.setWeights(perspectivePercent, terminalHeightPercent);
      }
    }
  }
}
