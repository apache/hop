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

import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;

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
public class HopGuiTerminalPanel extends Composite {

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

  // Terminal tab counter for naming
  private int terminalCounter = 1;

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
    terminalTabs = new CTabFolder(terminalComposite, SWT.MULTI);
    PropsUi.setLook(terminalTabs, PropsUi.WIDGET_STYLE_TAB);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(0, 0);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(100, 0);
    terminalTabs.setLayoutData(fdTabs);

    // Create a special "+" tab that acts as a button to create new terminals
    // No SWT.CLOSE flag = no close button
    newTerminalTab = new CTabItem(terminalTabs, SWT.NONE);
    newTerminalTab.setText("+");
    newTerminalTab.setToolTipText("Create a new terminal");
    // Create an empty composite for the + tab (it won't be displayed)
    Composite newTerminalPlaceholder = new Composite(terminalTabs, SWT.NONE);
    newTerminalTab.setControl(newTerminalPlaceholder);

    // Handle tab close events
    terminalTabs.addListener(
        SWT.Close,
        event -> {
          CTabItem item = (CTabItem) event.item;
          // Don't allow closing the + tab
          if (item == newTerminalTab) {
            event.doit = false;
            return;
          }
          closeTerminalTab(item);
        });

    // Handle tab selection - intercept + tab clicks or focus input field
    terminalTabs.addListener(
        SWT.Selection,
        event -> {
          CTabItem item = terminalTabs.getSelection();
          if (item == newTerminalTab) {
            // User clicked the + tab - create a new terminal
            createNewTerminal(null, null);
            // Don't actually select the + tab, it will be overridden by the new terminal
            return;
          }

          // Regular terminal tab selected - focus its input field
          if (item != null) {
            TerminalWidget widget = (TerminalWidget) item.getData("terminalWidget");
            if (widget != null
                && widget.getInputText() != null
                && !widget.getInputText().isDisposed()) {
              widget.getInputText().forceFocus();
            }
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
    // Detect shell if not specified
    if (shellPath == null) {
      shellPath = TerminalShellDetector.detectDefaultShell();
    }

    // Default working directory
    if (workingDirectory == null) {
      workingDirectory = System.getProperty("user.home");
    }

    // Create terminal tab - insert after the + tab (at index 1)
    CTabItem terminalTab = new CTabItem(terminalTabs, SWT.CLOSE, 1);

    // Extract shell name for tab title
    String shellName = extractShellName(shellPath);
    terminalTab.setText(shellName + " (" + terminalCounter++ + ")");
    terminalTab.setImage(GuiResource.getInstance().getImageTerminal());
    terminalTab.setToolTipText("Terminal: " + shellPath + " in " + workingDirectory);

    // Create terminal widget composite
    Composite terminalWidgetComposite = new Composite(terminalTabs, SWT.NONE);
    terminalWidgetComposite.setLayout(new FormLayout());
    terminalTab.setControl(terminalWidgetComposite);

    // Create and connect the actual terminal widget
    TerminalWidget terminalWidget =
        new TerminalWidget(terminalWidgetComposite, shellPath, workingDirectory);

    // Store widget in tab data for later access
    terminalTab.setData("terminalWidget", terminalWidget);

    // Select the new tab
    terminalTabs.setSelection(terminalTab);

    // Ensure terminal panel is visible
    if (!terminalVisible) {
      showTerminal();
    }

    // Focus the input field after a short delay to ensure everything is laid out
    getDisplay()
        .asyncExec(
            () -> {
              if (terminalWidget != null
                  && terminalWidget.getInputText() != null
                  && !terminalWidget.getInputText().isDisposed()) {
                terminalWidget.getInputText().setFocus();
                terminalWidget.getInputText().forceFocus();
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

  /** Show the terminal panel */
  public void showTerminal() {
    if (!terminalVisible) {
      verticalSash.setMaximizedControl(null);
      int perspectivePercent = 100 - terminalHeightPercent;
      verticalSash.setWeights(new int[] {perspectivePercent, terminalHeightPercent});
      terminalVisible = true;

      // Update horizontal sash to show terminal
      updateBottomPanelLayout();

      // Create first terminal if none exist
      if (terminalTabs.getItemCount() == 0) {
        createNewTerminal(null, null);
      }

      layout(true, true);
    }
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

  /** Close a terminal tab */
  private void closeTerminalTab(CTabItem item) {
    // Get and dispose terminal widget
    TerminalWidget widget = (TerminalWidget) item.getData("terminalWidget");
    if (widget != null) {
      widget.dispose();
    }

    // If only the + tab remains, hide panel
    // (itemCount will be 2 before the item is disposed - the + tab and the tab being closed)
    if (terminalTabs.getItemCount() == 2) {
      hideTerminal();
    }
  }

  /**
   * Get the perspective composite where perspectives should render. This replaces the original
   * mainPerspectivesComposite in HopGui.
   */
  public Composite getPerspectiveComposite() {
    return perspectiveComposite;
  }

  /** Check if terminal panel is visible */
  public boolean isTerminalVisible() {
    return terminalVisible;
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
