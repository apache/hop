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

import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
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
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/**
 * Terminal panel for Hop GUI providing integrated command-line access.
 *
 * <p>The panel wraps the main perspectives composite in a SashForm, with perspectives in the top
 * section and the terminal panel in the bottom section. The terminal panel persists across
 * perspective switches.
 */
@GuiPlugin(name = "Terminal panel", description = "Terminal panel")
public class HopGuiTerminalPanel extends Composite implements TabClosable {

  private static final Class<?> PKG = HopGuiTerminalPanel.class;

  public static final String ID_MAIN_MENU_TOOLS_TERMINAL = "40010-menu-tools-terminal";
  public static final String ID_MAIN_MENU_TOOLS_NEW_TERMINAL = "40020-menu-tools-new-terminal";

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

  /** Font size scale for all terminal tabs (100 = 100%). Persisted and applied to new tabs. */
  private int terminalFontSizePercent = 100;

  private static final String TERMINAL_AUDIT_TYPE = "terminal";

  /** Reserved state key for panel visibility (minimized vs visible). Not a terminal tab. */
  private static final String STATE_PANEL_VISIBLE_KEY = "terminalPanelVisible";

  private static final String STATE_PANEL_VISIBLE_PROP = "visible";

  /** Reserved state key for terminal font size percent (e.g. 100 = 100%). */
  private static final String STATE_TERMINAL_FONT_SIZE_PERCENT_KEY = "terminalFontSizePercent";

  // State map keys
  private static final String STATE_TAB_NAME = "tabName";
  private static final String STATE_SHELL_PATH = "shellPath";
  private static final String STATE_WORKING_DIR = "workingDirectory";

  /**
   * Constructor - Creates the terminal panel structure
   *
   * @param parent The parent composite (mainHopGuiComposite from HopGui)
   * @param hopGui The HopGui instance
   */
  public HopGuiTerminalPanel(Composite parent, HopGui hopGui) {
    super(parent, SWT.NONE);
    this.hopGui = hopGui;

    createContents();
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

    newTerminalTab = new CTabItem(terminalTabs, SWT.NONE);
    newTerminalTab.setText("+");
    newTerminalTab.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiTerminalPanel.NewTab.Tooltip"));
    Composite newTerminalPlaceholder = new Composite(terminalTabs, SWT.NONE);
    newTerminalTab.setControl(newTerminalPlaceholder);

    new TabCloseHandler(this);
    new TabFolderReorder(terminalTabs);

    final boolean[] isClosingTab = {false};
    terminalTabs.addListener(
        SWT.Selection,
        event -> {
          CTabItem item = terminalTabs.getSelection();
          // When only the "+" tab exists, getSelection() can be null; create a new terminal.
          if (item == null && terminalTabs.getItemCount() == 1 && !isClearing && !isClosingTab[0]) {
            createNewTerminal(null, null);
            return;
          }
          if (item == newTerminalTab) {
            // Creation is handled by MouseDown so we don't double-create when both fire
            return;
          }

          if (item != null) {
            ITerminalWidget widget = (ITerminalWidget) item.getData("terminalWidget");
            if (widget != null && widget instanceof JediTerminalWidget) {
              Composite composite = widget.getTerminalComposite();
              if (composite != null && !composite.isDisposed()) {
                composite.forceFocus();
              }
            }
          }
        });

    // Ensure + tab click always creates a terminal (e.g. when it's the only tab and
    // Selection doesn't fire because selection doesn't change)
    terminalTabs.addListener(
        SWT.MouseDown,
        event -> {
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
          if (item != null && item != newTerminalTab) {
            renameTerminalTab(item);
          }
        });
  }

  public void createNewTerminal(String workingDirectory, String shellPath) {
    createNewTerminal(workingDirectory, shellPath, null);
  }

  public void createNewTerminal(String workingDirectory, String shellPath, String customTabName) {
    if (shellPath == null) {
      shellPath = TerminalShellDetector.detectDefaultShell();
    }

    if (workingDirectory == null) {
      workingDirectory = getDefaultWorkingDirectory();
    }

    CTabItem terminalTab = new CTabItem(terminalTabs, SWT.CLOSE, 1);

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

    terminalTabs.setSelection(terminalTab);

    if (!terminalVisible) {
      showTerminal();
    }

    getDisplay()
        .asyncExec(
            () -> {
              if (terminalWidget == null) {
                return;
              }

              if (terminalWidget instanceof JediTerminalWidget) {
                Composite composite = terminalWidget.getTerminalComposite();
                if (composite != null && !composite.isDisposed()) {
                  composite.setFocus();
                  composite.forceFocus();
                }
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

  /** Show the terminal panel */
  public void showTerminal() {
    if (!terminalVisible) {
      verticalSash.setMaximizedControl(null);
      int perspectivePercent = 100 - terminalHeightPercent;
      verticalSash.setWeights(perspectivePercent, terminalHeightPercent);
      terminalVisible = true;

      if (terminalTabs.getItemCount() <= 1) {
        createNewTerminal(null, null);
      }

      layout(true, true);
      hopGui.refreshSidebarToolbarButtonStates();
    }
  }

  /** Hide the terminal panel */
  public void hideTerminal() {
    if (terminalVisible) {
      terminalVisible = false;
      verticalSash.setMaximizedControl(perspectiveComposite);
      layout(true, true);
      hopGui.refreshSidebarToolbarButtonStates();
    }
  }

  /** Toggle terminal panel visibility */
  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_TERMINAL,
      label = "i18n::HopGuiTerminalPanel.Menu.Terminal",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID)
  @GuiKeyboardShortcut(control = true, key = 'j', global = true)
  @GuiOsxKeyboardShortcut(command = true, key = 'j', global = true)
  public void toggleTerminal() {
    if (EnvironmentUtils.getInstance().isWeb()) {
      return;
    }
    if (terminalVisible) {
      hideTerminal();
    } else {
      showTerminal();
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
    if (EnvironmentUtils.getInstance().isWeb()) {
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

    tabItem.dispose();
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
    ToolBar toolBar = new ToolBar(terminalTabs, SWT.FLAT);
    terminalTabs.setTopRight(toolBar, SWT.RIGHT);
    PropsUi.setLook(toolBar);

    GuiResource gui = GuiResource.getInstance();
    if (PropsUi.getInstance().isDarkMode()) {
      toolBar.setBackground(gui.getColorWhite());
    } else {
      toolBar.setBackground(terminalTabs.getBackground());
    }

    // Font size: increase
    ToolItem increaseFontItem = new ToolItem(toolBar, SWT.PUSH);
    increaseFontItem.setImage(GuiResource.getInstance().getImage("ui/images/zoom-in.svg", 16, 16));
    increaseFontItem.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.IncreaseFont"));
    increaseFontItem.addListener(SWT.Selection, e -> increaseTerminalFontSize());

    // Font size: decrease
    ToolItem decreaseFontItem = new ToolItem(toolBar, SWT.PUSH);
    decreaseFontItem.setImage(GuiResource.getInstance().getImage("ui/images/zoom-out.svg", 16, 16));
    decreaseFontItem.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.DecreaseFont"));
    decreaseFontItem.addListener(SWT.Selection, e -> decreaseTerminalFontSize());

    // Font size: reset to 100%
    ToolItem resetFontItem = new ToolItem(toolBar, SWT.PUSH);
    resetFontItem.setImage(GuiResource.getInstance().getImage("ui/images/zoom-100.svg", 16, 16));
    resetFontItem.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.ResetFont"));
    resetFontItem.addListener(SWT.Selection, e -> resetTerminalFontSize());

    new ToolItem(toolBar, SWT.SEPARATOR);

    // Maximize/Minimize button
    final ToolItem maximizeItem = new ToolItem(toolBar, SWT.PUSH);
    maximizeItem.setImage(GuiResource.getInstance().getImageMaximizePanel());
    maximizeItem.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.Maximize"));
    maximizeItem.addListener(
        SWT.Selection,
        e -> {
          if (verticalSash.getMaximizedControl() == null) {
            // Maximize terminal panel
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
    final ToolItem closeItem = new ToolItem(toolBar, SWT.PUSH);
    closeItem.setImage(GuiResource.getInstance().getImageClose());
    closeItem.setToolTipText(BaseMessages.getString(PKG, "HopGuiTerminalPanel.Toolbar.Close"));
    closeItem.addListener(SWT.Selection, e -> hideTerminal());

    int height = toolBar.computeSize(SWT.DEFAULT, SWT.DEFAULT).y;
    terminalTabs.setTabHeight(Math.max(height, terminalTabs.getTabHeight()));
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
      saveOpenTerminals();

      java.util.List<CTabItem> itemsToClose = new java.util.ArrayList<>();
      for (CTabItem item : terminalTabs.getItems()) {
        if (item != newTerminalTab && !item.isDisposed()) {
          itemsToClose.add(item);
        }
      }

      for (CTabItem item : itemsToClose) {
        if (!item.isDisposed()) {
          ITerminalWidget widget = (ITerminalWidget) item.getData("terminalWidget");
          if (widget != null) {
            widget.dispose();
          }
          item.dispose();
        }
      }

      if (terminalVisible) {
        hideTerminal();
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
    try {
      String namespace = HopNamespace.getNamespace();

      int existingCount = 0;
      for (CTabItem item : terminalTabs.getItems()) {
        if (item != newTerminalTab) {
          existingCount++;
        }
      }

      if (existingCount > 0) {
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
        terminalFontSizePercent = Math.max(50, Math.min(200, saved));
      }

      if (auditList.getNames().isEmpty()) {
        return;
      }

      // If panel was hidden when saved, create terminals without showing the panel
      boolean wasVisible = terminalVisible;
      if (!savedPanelVisible) {
        terminalVisible = true; // prevent createNewTerminal from calling showTerminal()
      }

      for (String terminalId : auditList.getNames()) {
        if (STATE_PANEL_VISIBLE_KEY.equals(terminalId)) {
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

        createNewTerminal(workingDir, shellPath, customTabName);
      }

      if (!savedPanelVisible) {
        terminalVisible = wasVisible;
        hideTerminal();
      }
    } catch (Exception e) {
      hopGui
          .getLog()
          .logError(BaseMessages.getString(PKG, "HopGuiTerminalPanel.Error.RestoringTerminals"), e);
    }
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
