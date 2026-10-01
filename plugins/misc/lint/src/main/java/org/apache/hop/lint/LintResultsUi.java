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
package org.apache.hop.lint;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * Opens the lint results utility window (non-modal). Use Tools → Lint → Show Lint Results for the
 * full project view, or showResultsForFile after single-file / problems-bar actions.
 */
public final class LintResultsUi {

  private static final Class<?> PKG = LintResultsUi.class; // for i18n purposes

  private static Shell resultsShell;
  private static LintResultsPanel resultsPanel;

  private LintResultsUi() {}

  /** Show all cached lint results (project lint). */
  public static void showResults() {
    showResultsForFile(null);
  }

  /**
   * Show results for one file, or all results when filePath is null.
   *
   * <p>Findings for a file that is open belong in that editor's Problems tab, next to Hop's own
   * checks, rather than in a window of their own. The window is what is left for the cases the
   * Problems tab cannot serve: a whole project or folder, and files which are not open.
   */
  /**
   * Show the findings for everything under a folder, in the results window.
   *
   * <p>A folder has no editor to put its findings in, so unlike a single file this always opens the
   * window rather than trying the Problems tab first.
   *
   * @param folderPath the folder that was linted
   */
  public static void showResultsForFolder(String folderPath) {
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui == null || hopGui.getShell() == null) {
      return;
    }
    openResultsWindow(hopGui, panel -> panel.setFolderFilter(folderPath));
  }

  public static void showResultsForFile(String filePath) {
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui == null || hopGui.getShell() == null) {
      return;
    }

    if (filePath != null
        && (PipelineProblemsTabSync.revealForFile(filePath)
            || WorkflowProblemsTabSync.revealForFile(filePath))) {
      return;
    }

    final String normalizedFilter = filePath != null ? LintPathUtils.normalizePath(filePath) : null;
    openResultsWindow(hopGui, panel -> panel.setFileFilter(normalizedFilter));
  }

  /** Open (or raise) the shared results window and apply a filter to it. */
  private static void openResultsWindow(
      HopGui hopGui, java.util.function.Consumer<LintResultsPanel> applyFilter) {
    Runnable open =
        () -> {
          Shell parent = hopGui.getShell();
          if (parent.isDisposed()) {
            return;
          }

          if (resultsShell == null || resultsShell.isDisposed()) {
            resultsShell = new Shell(parent, SWT.SHELL_TRIM | SWT.RESIZE | SWT.MAX);
            resultsShell.setText(BaseMessages.getString(PKG, "LintResultsPanel.Title"));
            resultsShell.setSize(900, 600);
            resultsShell.setLayout(new FillLayout());
            resultsPanel = new LintResultsPanel(resultsShell, SWT.NONE);
            resultsShell.addListener(SWT.Dispose, event -> resultsPanel = null);
          }

          if (resultsPanel != null && !resultsPanel.isDisposed()) {
            applyFilter.accept(resultsPanel);
          }

          centerOverParent(resultsShell, parent);
          if (!resultsShell.isVisible()) {
            resultsShell.open();
          }
          resultsShell.forceActive();
        };

    Display display = hopGui.getDisplay();
    if (display != null) {
      display.asyncExec(open);
    } else {
      open.run();
    }
  }

  public static void logSummary(java.util.List<LintResult> results, String contextLabel) {
    if (results == null || results.isEmpty()) {
      LogChannel.GENERAL.logBasic("Lint check for " + contextLabel + ": no issues found.");
      return;
    }

    long errors = results.stream().filter(r -> "ERROR".equalsIgnoreCase(r.getSeverity())).count();
    long warnings =
        results.stream().filter(r -> "WARNING".equalsIgnoreCase(r.getSeverity())).count();
    LogChannel.GENERAL.logBasic(
        String.format(
            "Lint check for %s: %d issue(s) (%d errors, %d warnings). "
                + "Open Tools → Lint → Show Lint Results or click the canvas problems bar.",
            contextLabel, results.size(), errors, warnings));
  }

  private static void centerOverParent(Shell child, Shell parent) {
    child.setLocation(
        parent.getLocation().x + Math.max(0, (parent.getSize().x - child.getSize().x) / 2),
        parent.getLocation().y + Math.max(0, (parent.getSize().y - child.getSize().y) / 2));
  }
}
