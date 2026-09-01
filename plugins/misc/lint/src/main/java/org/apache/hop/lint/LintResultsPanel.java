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

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;

/** Panel displaying lint results in the Hop GUI */
public class LintResultsPanel extends Composite implements LintResultsManager.LintResultsListener {

  private static final Class<?> PKG = LintResultsPanel.class; // for i18n purposes

  private static final ILogChannel log = LogChannel.GENERAL;

  private LintResultsManager resultsManager;
  private Tree resultsTree;
  private Text detailsText;
  private Label statusLabel;
  private String fileFilter;
  private String folderFilter;

  public LintResultsPanel(Composite parent, int style) {
    super(parent, style);

    this.resultsManager = LintResultsManager.getInstance();
    this.resultsManager.addListener(this);

    createContents();
    refreshResults();
  }

  public void setFileFilter(String filePath) {
    this.fileFilter = filePath != null ? LintPathUtils.normalizePath(filePath) : null;
    this.folderFilter = null;
    refreshResults();
    updateShellTitle();
  }

  /**
   * Show the findings for everything under a folder.
   *
   * <p>Distinct from the file filter, which looks a single file up by path: a folder matches every
   * result beneath it, which is what a "lint this folder" run wants to show.
   *
   * @param folderPath the folder that was linted
   */
  public void setFolderFilter(String folderPath) {
    this.folderFilter = folderPath != null ? LintPathUtils.normalizePath(folderPath) : null;
    this.fileFilter = null;
    refreshResults();
    updateShellTitle();
  }

  private void updateShellTitle() {
    Shell shell = getShell();
    if (shell == null || shell.isDisposed()) {
      return;
    }
    if (folderFilter != null) {
      shell.setText(
          BaseMessages.getString(
              PKG, "LintResultsPanel.Shell.TitleForFolder", new File(folderFilter).getName()));
      return;
    }
    if (fileFilter != null) {
      shell.setText(
          BaseMessages.getString(
              PKG,
              "LintResultsPanel.Title.ForFile",
              LintEditorGraphHelper.displayName(fileFilter)));
    } else {
      shell.setText(BaseMessages.getString(PKG, "LintResultsPanel.Title"));
    }
  }

  private void createContents() {
    setLayout(new GridLayout(1, false));

    // Status bar
    statusLabel = new Label(this, SWT.NONE);
    statusLabel.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
    statusLabel.setText(BaseMessages.getString(PKG, "LintResultsPanel.Status.NoResults"));

    // Splitter for results tree and details
    SashForm sashForm = new SashForm(this, SWT.VERTICAL);
    sashForm.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

    // Results tree
    resultsTree = new Tree(sashForm, SWT.BORDER | SWT.FULL_SELECTION);
    resultsTree.setHeaderVisible(true);
    resultsTree.setLinesVisible(true);

    // Tree columns
    TreeColumn severityColumn = new TreeColumn(resultsTree, SWT.LEFT);
    severityColumn.setText(BaseMessages.getString(PKG, "LintResultsPanel.Column.Severity"));
    severityColumn.setWidth(80);

    TreeColumn ruleColumn = new TreeColumn(resultsTree, SWT.LEFT);
    ruleColumn.setText(BaseMessages.getString(PKG, "LintResultsPanel.Column.Rule"));
    ruleColumn.setWidth(100);

    TreeColumn messageColumn = new TreeColumn(resultsTree, SWT.LEFT);
    messageColumn.setText(BaseMessages.getString(PKG, "LintResultsPanel.Column.Message"));
    messageColumn.setWidth(400);

    TreeColumn fileColumn = new TreeColumn(resultsTree, SWT.LEFT);
    fileColumn.setText(BaseMessages.getString(PKG, "LintResultsPanel.Column.File"));
    fileColumn.setWidth(200);

    // Details panel
    Group detailsGroup = new Group(sashForm, SWT.NONE);
    detailsGroup.setText(BaseMessages.getString(PKG, "LintResultsPanel.Group.Details"));
    detailsGroup.setLayout(new FillLayout());

    detailsText = new Text(detailsGroup, SWT.MULTI | SWT.BORDER | SWT.V_SCROLL | SWT.WRAP);
    detailsText.setEditable(false);
    detailsText.setText(BaseMessages.getString(PKG, "LintResultsPanel.Details.SelectResult"));

    // Set sash weights (70% tree, 30% details)
    sashForm.setWeights(new int[] {70, 30});

    // Add selection listener
    resultsTree.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            showSelectedDetails();
          }
        });

    // Double-click to navigate to file
    resultsTree.addListener(
        SWT.MouseDoubleClick,
        event -> {
          TreeItem[] selection = resultsTree.getSelection();
          if (selection.length > 0) {
            navigateToFile(selection[0]);
          }
        });
  }

  private void refreshResults() {
    Display.getDefault()
        .asyncExec(
            () -> {
              if (isDisposed()) {
                return;
              }

              resultsTree.removeAll();

              List<LintResult> displayedResults = getDisplayedResults();

              if (displayedResults.isEmpty()) {
                if (fileFilter != null) {
                  statusLabel.setText(
                      "No lint issues for "
                          + LintEditorGraphHelper.displayName(fileFilter)
                          + ". Run lint on this file or check Tools → Lint → Show Lint Results.");
                } else {
                  statusLabel.setText(
                      BaseMessages.getString(PKG, "LintResultsPanel.Status.NoResults"));
                }
                detailsText.setText(
                    BaseMessages.getString(PKG, "LintResultsPanel.Details.NoResults"));
                return;
              }

              int errorCount = 0;
              int warningCount = 0;
              for (LintResult result : displayedResults) {
                if ("ERROR".equalsIgnoreCase(result.getSeverity())) {
                  errorCount++;
                } else if ("WARNING".equalsIgnoreCase(result.getSeverity())) {
                  warningCount++;
                }
              }

              String scope =
                  fileFilter != null ? " for " + LintEditorGraphHelper.displayName(fileFilter) : "";
              statusLabel.setText(
                  String.format(
                      "Found %d issues%s (%d errors, %d warnings)",
                      displayedResults.size(), scope, errorCount, warningCount));

              Map<String, List<LintResult>> resultsBySeverity = groupBySeverity(displayedResults);

              // Add error group
              if (resultsBySeverity.containsKey("ERROR")) {
                TreeItem errorGroup = new TreeItem(resultsTree, SWT.NONE);
                errorGroup.setText(
                    new String[] {
                      "ERROR", "", "Errors (" + resultsBySeverity.get("ERROR").size() + ")", ""
                    });
                errorGroup.setExpanded(true);

                for (LintResult result : resultsBySeverity.get("ERROR")) {
                  TreeItem item = new TreeItem(errorGroup, SWT.NONE);
                  item.setText(
                      new String[] {
                        result.getSeverity(),
                        result.getRuleId(),
                        result.getMessage(),
                        LintEditorGraphHelper.displayName(result.getFileName())
                      });
                  item.setData(result);
                }
              }

              // Add warning group
              if (resultsBySeverity.containsKey("WARNING")) {
                TreeItem warningGroup = new TreeItem(resultsTree, SWT.NONE);
                warningGroup.setText(
                    new String[] {
                      "WARNING",
                      "",
                      "Warnings (" + resultsBySeverity.get("WARNING").size() + ")",
                      ""
                    });
                warningGroup.setExpanded(true);

                for (LintResult result : resultsBySeverity.get("WARNING")) {
                  TreeItem item = new TreeItem(warningGroup, SWT.NONE);
                  item.setText(
                      new String[] {
                        result.getSeverity(),
                        result.getRuleId(),
                        result.getMessage(),
                        LintEditorGraphHelper.displayName(result.getFileName())
                      });
                  item.setData(result);
                }
              }

              // Add other severity groups
              for (Map.Entry<String, List<LintResult>> entry : resultsBySeverity.entrySet()) {
                if (!"ERROR".equals(entry.getKey()) && !"WARNING".equals(entry.getKey())) {
                  TreeItem group = new TreeItem(resultsTree, SWT.NONE);
                  group.setText(
                      new String[] {
                        entry.getKey(),
                        "",
                        entry.getKey() + " (" + entry.getValue().size() + ")",
                        ""
                      });
                  group.setExpanded(true);

                  for (LintResult result : entry.getValue()) {
                    TreeItem item = new TreeItem(group, SWT.NONE);
                    item.setText(
                        new String[] {
                          result.getSeverity(),
                          result.getRuleId(),
                          result.getMessage(),
                          result.getFileName()
                        });
                    item.setData(result);
                  }
                }
              }
            });
  }

  private List<LintResult> getDisplayedResults() {
    if (fileFilter != null) {
      return resultsManager.getResultsForFile(fileFilter);
    }
    if (folderFilter != null) {
      String prefix = folderFilter.endsWith("/") ? folderFilter : folderFilter + "/";
      return resultsManager.getAllResults().stream()
          .filter(
              result -> {
                String file = LintPathUtils.normalizePath(result.getFileName());
                return file != null && file.startsWith(prefix);
              })
          .toList();
    }
    return resultsManager.getAllResults();
  }

  private static Map<String, List<LintResult>> groupBySeverity(List<LintResult> results) {
    Map<String, List<LintResult>> grouped = new java.util.LinkedHashMap<>();
    for (LintResult result : results) {
      grouped.computeIfAbsent(result.getSeverity(), key -> new java.util.ArrayList<>()).add(result);
    }
    return grouped;
  }

  private void showSelectedDetails() {
    TreeItem[] selection = resultsTree.getSelection();
    if (selection.length == 0) {
      detailsText.setText(BaseMessages.getString(PKG, "LintResultsPanel.Details.SelectResult"));
      return;
    }

    TreeItem item = selection[0];
    Object data = item.getData();

    if (data instanceof LintResult) {
      LintResult result = (LintResult) data;
      StringBuilder details = new StringBuilder();

      details.append("Rule ID: ").append(result.getRuleId()).append("\n");
      details.append("Rule Name: ").append(result.getRuleName()).append("\n");
      details.append("Severity: ").append(result.getSeverity()).append("\n");
      details.append("File: ").append(result.getFileName()).append("\n");
      if (result.getSource() != null && result.getSource().hasName()) {
        details
            .append("Source: ")
            .append(result.getSource().getKind())
            .append(" ")
            .append(result.getSource().getName())
            .append("\n");
      }
      details.append("Origin: ").append(result.getOrigin()).append("\n\n");
      details.append("Message:\n").append(result.getMessage());

      detailsText.setText(details.toString());
    } else {
      detailsText.setText(BaseMessages.getString(PKG, "LintResultsPanel.Details.GroupSelected"));
    }
  }

  private void navigateToFile(TreeItem item) {
    Object data = item.getData();
    if (data instanceof LintResult) {
      LintNavigationHelper.navigateTo((LintResult) data);
    }
  }

  @Override
  public void onResultsUpdated() {
    refreshResults();
  }

  @Override
  public void dispose() {
    if (resultsManager != null) {
      resultsManager.removeListener(this);
    }
    super.dispose();
  }
}
