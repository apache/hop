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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.history.AuditStateMap;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.widget.TabFolderReorder;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.TabClosable;
import org.apache.hop.ui.hopgui.perspective.TabCloseHandler;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/**
 * Terminal panel for Hop GUI that provides integrated command-line access.
 *
 * <p>This panel sits at the HopGui main window level and provides: - Multiple terminal tabs (bash,
 * zsh, PowerShell, etc.) - Side-by-side layout with pipeline/workflow log panels - Show/hide
 * functionality - Resizable split panels
 *
 * <p>Architecture: - Created by HopGui during initialization - Wraps the mainPerspectivesComposite
 * in a SashForm - Terminal panel is global (persists across perspective switches) - Independent
 * from pipeline/workflow log panels (different hierarchy level)
 */
public class HopGuiTerminalPanel extends Composite implements TabClosable {

  private final HopGui hopGui;

  // Main layout components
  private SashForm verticalSash; // Splits perspectives (top) and terminal (bottom)
  private Composite
      perspectiveComposite; // Where perspectives render (replaces mainPerspectivesComposite)
  private Composite bottomPanelComposite; // Container for bottom panel

  // Bottom panel layout (logs + terminal side-by-side)
  private SashForm bottomHorizontalSash; // Horizontal split: logs left, terminal right
  private Composite logsPlaceholder; // Where pipeline/workflow logs render
  private Composite terminalComposite; // Terminal panel container

  // Terminal UI components
  private CTabFolder terminalTabs; // Tab folder for multiple terminals
  private CTabItem newTerminalTab; // Special "+" tab for creating new terminals

  // State
  private boolean terminalVisible = false;
  private boolean logsVisible = false;
  private int terminalHeightPercent = 35; // Default: 35% of window height
  private int logsWidthPercent = 50; // Default: 50% width when both visible
  private boolean isClearing = false; // Flag to prevent tab creation during cleanup

  // Terminal tab counter for naming and unique IDs
  private int terminalCounter = 1;

  // Audit type for terminal persistence (namespace is HopNamespace.getNamespace())
  private static final String TERMINAL_AUDIT_TYPE = "terminal";

  // State map keys
  private static final String STATE_TAB_NAME = "tabName";
  private static final String STATE_SHELL_PATH = "shellPath";
  private static final String STATE_WORKING_DIR = "workingDirectory";
  private static final String STATE_TERMINAL_TYPE = "terminalType";

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

  /**
   * Create the UI structure: - Vertical SashForm splits perspectives and terminal - Top:
   * Perspectives composite (where all perspectives render) - Bottom: Terminal composite (with tabs
   * and toolbar)
   */
  private void createContents() {
    setLayout(new FormLayout());

    // Main vertical sash form - splits perspective area and terminal area
    verticalSash = new SashForm(this, SWT.VERTICAL | SWT.SMOOTH);
    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.top = new FormAttachment(0, 0);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.bottom = new FormAttachment(100, 0);
    verticalSash.setLayoutData(fdSash);

    // Top part: Perspective composite (this is where mainPerspectivesComposite will be rendered)
    perspectiveComposite = new Composite(verticalSash, SWT.NONE);
    perspectiveComposite.setLayout(new FormLayout());

    // Bottom part: Container for logs and terminal (side-by-side)
    bottomPanelComposite = new Composite(verticalSash, SWT.NONE);
    bottomPanelComposite.setLayout(new FormLayout());
    createBottomPanel();

    // Initially hide bottom panel (show only perspectives)
    verticalSash.setMaximizedControl(perspectiveComposite);
  }

  /** Create the bottom panel with horizontal sash for logs (left) and terminal (right) */
  private void createBottomPanel() {
    // Create horizontal sash for side-by-side layout
    bottomHorizontalSash = new SashForm(bottomPanelComposite, SWT.HORIZONTAL | SWT.SMOOTH);
    FormData fdHorizontalSash = new FormData();
    fdHorizontalSash.left = new FormAttachment(0, 0);
    fdHorizontalSash.top = new FormAttachment(0, 0);
    fdHorizontalSash.right = new FormAttachment(100, 0);
    fdHorizontalSash.bottom = new FormAttachment(100, 0);
    bottomHorizontalSash.setLayoutData(fdHorizontalSash);

    // Left side: Placeholder for pipeline/workflow logs
    logsPlaceholder = new Composite(bottomHorizontalSash, SWT.NONE);
    logsPlaceholder.setLayout(new FormLayout());

    // Right side: Terminal area
    terminalComposite = new Composite(bottomHorizontalSash, SWT.NONE);
    terminalComposite.setLayout(new FormLayout());
    createTerminalArea();

    // Initially show only terminal (hide logs placeholder)
    bottomHorizontalSash.setMaximizedControl(terminalComposite);
  }

  /** Create the terminal area with tab folder */
  private void createTerminalArea() {
    // Tab folder for multiple terminals (using standard Hop tab style)
    // SWT.MULTI matches other tab folders in Hop Gui (logs, metrics, perspectives, etc.)
    terminalTabs = new CTabFolder(terminalComposite, SWT.MULTI | SWT.BORDER);
    PropsUi.setLook(terminalTabs, PropsUi.WIDGET_STYLE_TAB);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(0, 0);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(100, 0);
    terminalTabs.setLayoutData(fdTabs);

    // Add toolbar with panel controls (maximize, close)
    createTerminalToolbar();

    // Create a special "+" tab that acts as a button to create new terminals
    // No SWT.CLOSE flag = no close button
    newTerminalTab = new CTabItem(terminalTabs, SWT.NONE);
    newTerminalTab.setText("+");
    newTerminalTab.setToolTipText("Create a new terminal");
    // Create an empty composite for the + tab (it won't be displayed)
    Composite newTerminalPlaceholder = new Composite(terminalTabs, SWT.NONE);
    newTerminalTab.setControl(newTerminalPlaceholder);

    // Add context menu with close left/right/others/all options
    new TabCloseHandler(this);

    // Enable drag-and-drop tab reordering
    new TabFolderReorder(terminalTabs);

    // Track if we're closing a tab to prevent auto-creation when + tab is auto-selected
    final boolean[] isClosingTab = {false};

    // Handle tab selection - intercept + tab clicks or focus input field
    terminalTabs.addListener(
        SWT.Selection,
        event -> {
          CTabItem item = terminalTabs.getSelection();
          if (item == newTerminalTab) {
            // Don't create terminal if we're in the middle of clearing tabs or closing a tab
            if (isClearing || isClosingTab[0]) {
              return;
            }
            // User clicked the + tab - create a new terminal
            createNewTerminal(null, null);
            // Don't actually select the + tab, it will be overridden by the new terminal
            return;
          }

          // Regular terminal tab selected - focus its input/output field
          if (item != null) {
            ITerminalWidget widget = (ITerminalWidget) item.getData("terminalWidget");
            if (widget != null) {
              // For SimpleTerminalWidget, focus the input field
              if (widget instanceof SimpleTerminalWidget simpleWidget) {
                if (simpleWidget.getInputText() != null
                    && !simpleWidget.getInputText().isDisposed()) {
                  simpleWidget.getInputText().forceFocus();
                }
              } else if (widget instanceof JediTerminalWidget) {
                // For JediTerm, focus the SWT composite (which forwards to AWT)
                Composite composite = widget.getTerminalComposite();
                if (composite != null && !composite.isDisposed()) {
                  composite.forceFocus();
                }
              } else if (widget.getOutputText() != null && !widget.getOutputText().isDisposed()) {
                // For terminals with StyledText output, focus the StyledText
                widget.getOutputText().forceFocus();
              } else if (widget instanceof SimpleTerminalWidget) {
                // For SimpleTerminalWidget in web mode, focus the output control
                SimpleTerminalWidget simpleWidget = (SimpleTerminalWidget) widget;
                Control outputControl = simpleWidget.getOutputControl();
                if (outputControl != null && !outputControl.isDisposed()) {
                  outputControl.setFocus();
                }
              }
            }
          }
        });

    // Modify close handler to set flag
    terminalTabs.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            // Set flag to prevent auto-creation when + tab is auto-selected
            isClosingTab[0] = true;
            try {
              CTabItem item = (CTabItem) event.item;
              // Don't allow closing the + tab
              if (item == newTerminalTab) {
                event.doit = false;
                return;
              }
              closeTab(event, item);
            } finally {
              // Reset flag after a short delay
              getDisplay()
                  .asyncExec(
                      () -> {
                        isClosingTab[0] = false;
                      });
            }
          }
        });

    // Add double-click to rename terminal tabs (except + tab)
    terminalTabs.addListener(
        SWT.MouseDoubleClick,
        event -> {
          CTabItem item = terminalTabs.getSelection();
          if (item != null && item != newTerminalTab) {
            renameTerminalTab(item);
          }
        });
  }

  /**
   * Create a new terminal tab
   *
   * @param workingDirectory Optional working directory (null for user home)
   * @param shellPath Optional shell path (null for auto-detect)
   */
  public void createNewTerminal(String workingDirectory, String shellPath) {
    createNewTerminal(workingDirectory, shellPath, null);
  }

  /**
   * Create a new terminal tab with custom name
   *
   * @param workingDirectory Optional working directory (null for user home)
   * @param shellPath Optional shell path (null for auto-detect)
   * @param customTabName Optional custom tab name (null for auto-generated)
   */
  public void createNewTerminal(String workingDirectory, String shellPath, String customTabName) {
    createNewTerminal(workingDirectory, shellPath, customTabName, null);
  }

  /**
   * Create a new terminal tab with custom name and terminal type
   *
   * @param workingDirectory Optional working directory (null for user home)
   * @param shellPath Optional shell path (null for auto-detect)
   * @param customTabName Optional custom tab name (null for auto-generated)
   * @param terminalType Optional terminal type ("JediTerm" or "Simple", null to use config default)
   */
  public void createNewTerminal(
      String workingDirectory, String shellPath, String customTabName, String terminalType) {
    // Detect shell if not specified
    if (shellPath == null) {
      shellPath = TerminalShellDetector.detectDefaultShell();
    }

    // Default working directory: try project home first, then user home
    if (workingDirectory == null) {
      workingDirectory = getDefaultWorkingDirectory();
    }

    // Create terminal tab - insert after the + tab (at index 1)
    CTabItem terminalTab = new CTabItem(terminalTabs, SWT.CLOSE, 1);

    // Generate unique terminal ID (counter + timestamp to avoid conflicts)
    String terminalId = "terminal-" + terminalCounter++ + "-" + System.currentTimeMillis();

    // Set tab name: use custom name if provided, otherwise generate default
    if (customTabName != null && !customTabName.trim().isEmpty()) {
      terminalTab.setText(customTabName);
    } else {
      String shellName = extractShellName(shellPath);
      terminalTab.setText(shellName + " (" + (terminalCounter - 1) + ")");
    }
    terminalTab.setImage(GuiResource.getInstance().getImageTerminal());
    terminalTab.setToolTipText("Terminal: " + shellPath + " in " + workingDirectory);

    // Store unique ID and metadata in tab data
    terminalTab.setData("terminalId", terminalId);
    terminalTab.setData("workingDirectory", workingDirectory);
    terminalTab.setData("shellPath", shellPath);

    // Create terminal widget composite
    Composite terminalWidgetComposite = new Composite(terminalTabs, SWT.NONE);
    terminalWidgetComposite.setLayout(new FormLayout());
    terminalTab.setControl(terminalWidgetComposite);

    // Create terminal widget based on saved type or configuration
    PropsUi props = PropsUi.getInstance();
    ITerminalWidget terminalWidget;

    // JediTerm uses SWT_AWT bridge which doesn't work in RAP/web mode
    // Force simple terminal in web mode regardless of configuration or saved type
    boolean isWeb = EnvironmentUtils.getInstance().isWeb();
    if (isWeb) {
      // Web mode: always use simple terminal (JediTerm not available in RAP)
      terminalWidget = new SimpleTerminalWidget(terminalWidgetComposite, workingDirectory);
    } else if (terminalType != null && terminalType.equals("JediTerm")) {
      // Use saved terminal type (JediTerm)
      terminalWidget = new JediTerminalWidget(terminalWidgetComposite, shellPath, workingDirectory);
    } else if (terminalType != null && terminalType.equals("Simple")) {
      // Use saved terminal type (Simple)
      terminalWidget = new SimpleTerminalWidget(terminalWidgetComposite, workingDirectory);
    } else if (props.useJediTerm()) {
      // Use configuration default (JediTerm)
      terminalWidget = new JediTerminalWidget(terminalWidgetComposite, shellPath, workingDirectory);
    } else {
      // Use configuration default (Simple)
      terminalWidget = new SimpleTerminalWidget(terminalWidgetComposite, workingDirectory);
    }

    // Store widget in tab data for later access
    terminalTab.setData("terminalWidget", terminalWidget);

    // Update tab text with terminal type indicator
    updateTabTextWithTerminalType(terminalTab, terminalWidget);

    // Register in audit system for persistence
    registerTerminal(terminalId, workingDirectory, shellPath);

    // Select the new tab
    terminalTabs.setSelection(terminalTab);

    // Ensure terminal panel is visible
    if (!terminalVisible) {
      showTerminal();
    }

    // Focus the terminal after a short delay to ensure everything is laid out
    getDisplay()
        .asyncExec(
            () -> {
              if (terminalWidget == null) {
                return;
              }

              if (terminalWidget instanceof JediTerminalWidget) {
                // For JediTerm, focus the composite (which has AWT focus listener)
                Composite composite = terminalWidget.getTerminalComposite();
                if (composite != null && !composite.isDisposed()) {
                  composite.setFocus();
                  composite.forceFocus();
                }
              } else if (terminalWidget.getOutputText() != null
                  && !terminalWidget.getOutputText().isDisposed()) {
                // For other terminals, focus the StyledText
                terminalWidget.getOutputText().setFocus();
                terminalWidget.getOutputText().forceFocus();
              } else if (terminalWidget instanceof SimpleTerminalWidget) {
                // For SimpleTerminalWidget in web mode, focus the output control
                SimpleTerminalWidget simpleWidget = (SimpleTerminalWidget) terminalWidget;
                Control outputControl = simpleWidget.getOutputControl();
                if (outputControl != null && !outputControl.isDisposed()) {
                  outputControl.setFocus();
                }
              }
            });
  }

  /** Extract shell name from full path (e.g., "/bin/bash" -> "bash") */
  private String extractShellName(String shellPath) {
    if (shellPath == null || shellPath.isEmpty()) {
      return "Terminal";
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

  /**
   * Update tab text with terminal type indicator (JediTerm or Simple)
   *
   * @param terminalTab The tab item to update
   * @param terminalWidget The terminal widget instance
   */
  private void updateTabTextWithTerminalType(CTabItem terminalTab, ITerminalWidget terminalWidget) {
    if (terminalTab == null || terminalWidget == null) {
      return;
    }

    String currentText = terminalTab.getText();
    String indicator;

    if (terminalWidget instanceof JediTerminalWidget) {
      indicator = " [JT]";
    } else if (terminalWidget instanceof SimpleTerminalWidget) {
      indicator = " [Simple]";
    } else {
      // Unknown type, don't add indicator
      return;
    }

    // Only add indicator if it's not already present (to avoid duplicates)
    if (!currentText.contains(indicator)) {
      terminalTab.setText(currentText + indicator);
    }
  }

  /** Show the terminal panel */
  public void showTerminal() {
    if (!terminalVisible) {
      verticalSash.setMaximizedControl(null);
      int perspectivePercent = 100 - terminalHeightPercent;
      verticalSash.setWeights(new int[] {perspectivePercent, terminalHeightPercent});
      terminalVisible = true;

      // Update horizontal sash to show terminal
      updateBottomPanelLayout();

      // Create first terminal if none exist (only + tab present, or count is 0)
      // Count <= 1 means either empty or just the + tab
      if (terminalTabs.getItemCount() <= 1) {
        createNewTerminal(null, null);
      }

      layout(true, true);
    }
  }

  /** Check if terminal panel is currently visible */
  public boolean isTerminalVisible() {
    return terminalVisible;
  }

  /** Hide the terminal panel */
  public void hideTerminal() {
    if (terminalVisible) {
      terminalVisible = false;

      // If logs are still visible, show only logs in bottom panel
      if (logsVisible) {
        bottomHorizontalSash.setMaximizedControl(logsPlaceholder);
        // Keep bottom panel visible
      } else {
        // Hide entire bottom panel
        verticalSash.setMaximizedControl(perspectiveComposite);
      }

      layout(true, true);
    }
  }

  /** Show the logs area (called by pipeline/workflow when showing execution results) */
  public void showLogs() {
    if (!logsVisible) {
      logsVisible = true;

      // Show bottom panel if hidden
      if (!terminalVisible && verticalSash.getMaximizedControl() != null) {
        verticalSash.setMaximizedControl(null);
        int perspectivePercent = 100 - terminalHeightPercent;
        verticalSash.setWeights(new int[] {perspectivePercent, terminalHeightPercent});
      }

      // Update horizontal layout
      updateBottomPanelLayout();
      layout(true, true);
    }
  }

  /** Hide the logs area (called by pipeline/workflow when hiding execution results) */
  public void hideLogs() {
    if (logsVisible) {
      logsVisible = false;

      // Update horizontal layout
      updateBottomPanelLayout();

      // If terminal also hidden, hide entire bottom panel
      if (!terminalVisible) {
        verticalSash.setMaximizedControl(perspectiveComposite);
      }

      layout(true, true);
    }
  }

  /** Update the bottom panel layout based on what's visible */
  private void updateBottomPanelLayout() {
    if (logsVisible && terminalVisible) {
      // Both visible: side-by-side split
      bottomHorizontalSash.setMaximizedControl(null);
      bottomHorizontalSash.setWeights(new int[] {logsWidthPercent, 100 - logsWidthPercent});
    } else if (logsVisible) {
      // Only logs: full width
      bottomHorizontalSash.setMaximizedControl(logsPlaceholder);
    } else if (terminalVisible) {
      // Only terminal: full width
      bottomHorizontalSash.setMaximizedControl(terminalComposite);
    }
  }

  /** Toggle terminal panel visibility */
  public void toggleTerminal() {
    if (terminalVisible) {
      hideTerminal();
    } else {
      showTerminal();
    }
  }

  /** Close a terminal tab (implements TabClosable interface) */
  @Override
  public void closeTab(CTabFolderEvent event, CTabItem tabItem) {
    // Don't allow closing the + tab
    if (tabItem == newTerminalTab) {
      if (event != null) {
        event.doit = false;
      }
      return;
    }

    // Get and dispose terminal widget
    ITerminalWidget widget = (ITerminalWidget) tabItem.getData("terminalWidget");
    if (widget != null) {
      widget.dispose();
    }

    // Unregister from audit
    String terminalId = (String) tabItem.getData("terminalId");
    if (terminalId != null) {
      unregisterTerminal(terminalId);
    }

    // Dispose the tab
    tabItem.dispose();

    // User can close all terminal tabs - panel stays open with just the + tab
    // They can manually close the panel using the close button if desired
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
        // Don't include the + tab
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
        // Don't include the + tab
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
        // Don't include the + tab
        items.add(item);
      }
    }
    return items;
  }

  /** Create toolbar with panel controls (maximize/minimize, close) */
  private void createTerminalToolbar() {
    ToolBar toolBar = new ToolBar(terminalTabs, SWT.FLAT);
    terminalTabs.setTopRight(toolBar, SWT.RIGHT);
    PropsUi.setLook(toolBar);

    // Maximize/Minimize button
    final ToolItem maximizeItem = new ToolItem(toolBar, SWT.PUSH);
    maximizeItem.setImage(GuiResource.getInstance().getImageMaximizePanel());
    maximizeItem.setToolTipText("Maximize terminal panel");
    maximizeItem.addListener(
        SWT.Selection,
        e -> {
          if (verticalSash.getMaximizedControl() == null) {
            // Maximize terminal panel
            verticalSash.setMaximizedControl(bottomPanelComposite);
            maximizeItem.setImage(GuiResource.getInstance().getImageMinimizePanel());
            maximizeItem.setToolTipText("Restore terminal panel");
          } else {
            // Restore normal split
            verticalSash.setMaximizedControl(null);
            verticalSash.setWeights(new int[] {100 - terminalHeightPercent, terminalHeightPercent});
            maximizeItem.setImage(GuiResource.getInstance().getImageMaximizePanel());
            maximizeItem.setToolTipText("Maximize terminal panel");
          }
        });

    // Split orientation toggle removed - behavior is confusing with new terminal panel structure
    // The execution results panel (logging/metrics/problems) handles its own orientation
    // independently within pipeline/workflow graphs. Re-introducing this requires a more
    // comprehensive layout refactor (floating/dockable panels) which is deferred to future work.

    // Close button
    final ToolItem closeItem = new ToolItem(toolBar, SWT.PUSH);
    closeItem.setImage(GuiResource.getInstance().getImageClose());
    closeItem.setToolTipText("Close terminal panel");
    closeItem.addListener(SWT.Selection, e -> hideTerminal());

    // Adjust tab height to accommodate toolbar
    int height = toolBar.computeSize(SWT.DEFAULT, SWT.DEFAULT).y;
    terminalTabs.setTabHeight(Math.max(height, terminalTabs.getTabHeight()));
  }

  /** Rename a terminal tab via dialog */
  private void renameTerminalTab(CTabItem item) {
    if (item == null || item == newTerminalTab) {
      return;
    }

    // Create a simple inline text editor for renaming
    final Text text = new Text(terminalTabs, SWT.BORDER);
    text.setText(item.getText());

    // Position the text editor over the tab
    org.eclipse.swt.graphics.Rectangle bounds = item.getBounds();
    text.setBounds(bounds.x, bounds.y, bounds.width, bounds.height);
    text.moveAbove(null);

    // Focus and select all text
    text.setFocus();
    text.selectAll();

    // Handle Enter key - accept rename
    text.addListener(
        SWT.Traverse,
        event -> {
          if (event.detail == SWT.TRAVERSE_RETURN) {
            String newName = text.getText().trim();
            if (!newName.isEmpty()) {
              item.setText(newName);
              // Save terminals to persist the rename
              saveOpenTerminals();
            }
            text.dispose();
            event.doit = false;
          } else if (event.detail == SWT.TRAVERSE_ESCAPE) {
            text.dispose();
            event.doit = false;
          }
        });

    // Handle focus lost - accept rename
    text.addListener(
        SWT.FocusOut,
        event -> {
          if (!text.isDisposed()) {
            String newName = text.getText().trim();
            if (!newName.isEmpty()) {
              item.setText(newName);
              // Save terminals to persist the rename
              saveOpenTerminals();
            }
            text.dispose();
          }
        });
  }

  /** Public method to save terminals on shutdown (called by HopGuiFileDelegate.fileExit) */
  public void saveTerminalsOnShutdown() {
    saveOpenTerminals();
  }

  /** Save all open terminals to audit system (per-project using HopNamespace) */
  private void saveOpenTerminals() {
    try {
      java.util.List<String> terminalIds = new java.util.ArrayList<>();
      AuditStateMap stateMap = new AuditStateMap();

      // Collect terminal IDs and metadata from all open terminal tabs (skip the + tab)
      for (CTabItem item : terminalTabs.getItems()) {
        if (item == newTerminalTab) {
          continue; // Skip the + tab
        }
        String terminalId = (String) item.getData("terminalId");
        if (terminalId != null) {
          terminalIds.add(terminalId);

          // Save terminal state (tab name, working dir, shell path, terminal type)
          java.util.Map<String, Object> state = new java.util.HashMap<>();
          state.put(STATE_TAB_NAME, item.getText());
          state.put(STATE_WORKING_DIR, item.getData("workingDirectory"));
          state.put(STATE_SHELL_PATH, item.getData("shellPath"));

          // Save terminal type (Simple or JediTerm)
          ITerminalWidget widget = (ITerminalWidget) item.getData("terminalWidget");
          if (widget != null) {
            String terminalType = widget instanceof JediTerminalWidget ? "JediTerm" : "Simple";
            state.put(STATE_TERMINAL_TYPE, terminalType);
          }

          stateMap.add(new AuditState(terminalId, state));
        }
      }

      // Save the list per-project (HopNamespace.getNamespace() handles project context)
      AuditList auditList = new AuditList(terminalIds);
      AuditManager.getActive()
          .storeList(HopNamespace.getNamespace(), TERMINAL_AUDIT_TYPE, auditList);

      // Save the state map (tab names, working dirs, shell paths)
      AuditManager.getActive()
          .saveAuditStateMap(HopNamespace.getNamespace(), TERMINAL_AUDIT_TYPE, stateMap);

      hopGui
          .getLog()
          .logDebug("Saved " + terminalIds.size() + " open terminal(s) for current project");
    } catch (Exception e) {
      hopGui.getLog().logError("Error saving open terminals", e);
    }
  }

  /** Clear all terminals (called when switching projects) - runs async to avoid blocking */
  public void clearAllTerminals() {
    // Safety check - don't run if disposed or not initialized
    if (isDisposed() || terminalTabs == null || terminalTabs.isDisposed()) {
      hopGui.getLog().logDebug("clearAllTerminals: skipped (disposed or not initialized)");
      return;
    }

    // Set flag to prevent + tab from creating terminals during cleanup
    isClearing = true;

    try {
      // Save current terminals synchronously (fast operation)
      saveOpenTerminals();

      // Close all terminal tabs (except the + tab) - MUST be synchronous on UI thread
      java.util.List<CTabItem> itemsToClose = new java.util.ArrayList<>();
      for (CTabItem item : terminalTabs.getItems()) {
        if (item != newTerminalTab && !item.isDisposed()) {
          itemsToClose.add(item);
        }
      }

      // Close each terminal tab (widgets dispose on UI thread automatically now)
      for (CTabItem item : itemsToClose) {
        if (!item.isDisposed()) {
          ITerminalWidget widget = (ITerminalWidget) item.getData("terminalWidget");
          if (widget != null) {
            widget.dispose();
          }
          item.dispose();
        }
      }

      // Hide terminal panel if it was visible
      if (terminalVisible) {
        hideTerminal();
      }
    } finally {
      // Always clear the flag
      isClearing = false;
    }
  }

  /** Register a terminal in the audit system for persistence */
  private void registerTerminal(String terminalId, String workingDirectory, String shellPath) {
    // Don't save immediately - let HopGui's shutdown save mechanism handle it
    // Saving on every create causes duplicates and performance issues
  }

  /** Unregister a terminal from the audit system */
  private void unregisterTerminal(String terminalId) {
    // Save terminals when closing (this is the right time)
    saveOpenTerminals();
  }

  /**
   * Get the default working directory for new terminals. Tries to use the current project's home
   * directory if available, otherwise falls back to user home directory.
   *
   * @return The default working directory path
   */
  private String getDefaultWorkingDirectory() {
    // Try to get PROJECT_HOME variable (set by Projects plugin when project is active)
    try {
      String projectHome = hopGui.getVariables().getVariable("PROJECT_HOME");
      if (StringUtils.isNotEmpty(projectHome)) {
        // Resolve in case it contains other variables
        projectHome = hopGui.getVariables().resolve(projectHome);
        if (StringUtils.isNotEmpty(projectHome)) {
          return projectHome;
        }
      }
    } catch (Exception e) {
      // Ignore - fall back to user home
    }

    // Fallback to user home directory
    return System.getProperty("user.home");
  }

  /** Restore terminals from previous session */
  public void restoreTerminals() {
    try {
      String namespace = HopNamespace.getNamespace();

      // Count existing terminals (excluding + tab)
      int existingCount = 0;
      for (CTabItem item : terminalTabs.getItems()) {
        if (item != newTerminalTab) {
          existingCount++;
        }
      }

      // If terminals already exist, don't restore (avoid duplicates)
      if (existingCount > 0) {
        return;
      }

      // Get list of terminals from audit system using current namespace (per-project)
      AuditList auditList = AuditManager.getActive().retrieveList(namespace, TERMINAL_AUDIT_TYPE);

      if (auditList.getNames().isEmpty()) {
        return;
      }

      // Get the state map (custom tab names, etc.)
      AuditStateMap stateMap;
      try {
        stateMap =
            AuditManager.getActive()
                .loadAuditStateMap(HopNamespace.getNamespace(), TERMINAL_AUDIT_TYPE);
      } catch (Exception e) {
        hopGui.getLog().logError("Error loading terminal state map", e);
        stateMap = new AuditStateMap();
      }

      for (String terminalId : auditList.getNames()) {
        // Get terminal state (tab name, working dir, shell path, terminal type)
        String customTabName = null;
        String workingDir = null;
        String shellPath = null;
        String terminalType = null;

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
          Object terminalTypeObj = state.getStateMap().get(STATE_TERMINAL_TYPE);
          if (terminalTypeObj != null) {
            terminalType = terminalTypeObj.toString();
          }
        }

        // Create terminal with stored state (including terminal type)
        // Note: createNewTerminal will automatically add terminal type indicator
        createNewTerminal(workingDir, shellPath, customTabName, terminalType);
      }
    } catch (Exception e) {
      hopGui.getLog().logError("Error restoring terminals", e);
    }
  }

  /**
   * Get the perspective composite where perspectives should render. This replaces the original
   * mainPerspectivesComposite in HopGui.
   */
  public Composite getPerspectiveComposite() {
    return perspectiveComposite;
  }

  /** Check if logs panel is visible */
  public boolean isLogsVisible() {
    return logsVisible;
  }

  /** Get the terminal tabs folder (for advanced control if needed) */
  public CTabFolder getTerminalTabs() {
    return terminalTabs;
  }

  /**
   * Get the logs placeholder composite where pipeline/workflow logs should render This is the left
   * side of the horizontal sash in the bottom panel
   */
  public Composite getLogsPlaceholder() {
    return logsPlaceholder;
  }

  /** Get the horizontal sash (for resizing between logs and terminal) */
  public SashForm getBottomHorizontalSash() {
    return bottomHorizontalSash;
  }

  /** Set terminal height percentage (default is 35%) */
  public void setTerminalHeightPercent(int percent) {
    if (percent > 0 && percent < 100) {
      this.terminalHeightPercent = percent;
      if (terminalVisible) {
        int perspectivePercent = 100 - terminalHeightPercent;
        verticalSash.setWeights(new int[] {perspectivePercent, terminalHeightPercent});
      }
    }
  }

  /** Get current terminal height percentage */
  public int getTerminalHeightPercent() {
    return terminalHeightPercent;
  }
}
