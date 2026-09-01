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
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.callback.GuiCallback;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElementType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;

/** GUI plugin that adds lint actions to the Explorer perspective */
@GuiPlugin(
    id = "HopLintCheckerExplorerGuiPlugin",
    description = "Hop Lint Checker Explorer Integration")
public class ExplorerLintGuiPlugin {

  private static final ILogChannel log = LogChannel.GENERAL;
  private static ExplorerLintGuiPlugin instance;
  private static LintStatusFilePainter filePainter;

  public static ExplorerLintGuiPlugin getInstance() {
    if (instance == null) {
      instance = new ExplorerLintGuiPlugin();
    }
    return instance;
  }

  public ExplorerLintGuiPlugin() {
    instance = this;
  }

  @GuiCallback(callbackId = ExplorerPerspective.GUI_TOOLBAR_CREATED_CALLBACK_ID)
  public void registerExplorerPaintListener() {
    if (filePainter == null) {
      filePainter = new LintStatusFilePainter();
    }
    ExplorerPerspective perspective = ExplorerPerspective.getInstance();
    if (!perspective.getFilePaintListeners().contains(filePainter)) {
      perspective.getFilePaintListeners().add(filePainter);
    }
  }

  /** Get the file painter instance */
  public static LintStatusFilePainter getFilePainter() {
    return filePainter;
  }

  /**
   * Repaint the explorer tree so lint status overlays update, without rebuilding the tree (which
   * would lose the user's selection and expanded folders). Falls back to a full refresh only when
   * the painter has not yet been attached.
   */
  public static void refreshExplorerIcons() {
    Display.getDefault()
        .asyncExec(
            () -> {
              try {
                if (filePainter != null) {
                  filePainter.repaintExplorerIcons();
                  return;
                }
                ExplorerPerspective perspective = HopGui.getExplorerPerspective();
                if (perspective != null) {
                  perspective.refresh();
                }
              } catch (Exception e) {
                log.logError("Error refreshing explorer lint icons: " + e.getMessage(), e);
              }
            });
  }

  private static final String CONTEXT_MENU_LINT_FILE = "context-menu-lint-file";
  private static final String CONTEXT_MENU_LINT_FOLDER = "context-menu-lint-folder";

  /** Lint selected file in explorer - context menu */
  @GuiMenuElement(
      root = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_LINT_FILE,
      type = GuiMenuElementType.MENU_ITEM,
      label = "i18n::ExplorerLintGuiPlugin.Menu.LintThisFile.Label",
      image = "lint-check.svg",
      separator = true)
  public void lintSelectedFileContext() {
    lintSelectedFile();
  }

  // TODO(apache/hop#7330): restore the "Lint This Metadata" context-menu element once the PR that
  // adds MetadataPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID is merged and the hop jar published.
  // Removed here because the constant does not exist in the current 2.19.0-SNAPSHOT dependency.

  /** Tools → Lint → Lint This File (Explorer selection, else active editor tab). */
  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = "lint-selected-file",
      type = GuiMenuElementType.MENU_ITEM,
      label = "i18n::ExplorerLintGuiPlugin.Menu.LintThisFile.Label",
      parentId = LinterGuiPlugin.LINT_SUBMENU_ID,
      image = "lint-check.svg")
  public static void lintSelectedFile() {
    try {
      String actualPath = LintFileSelection.resolveLintFilePath();
      if (Utils.isEmpty(actualPath)) {
        showMessage(
            "No File To Lint",
            "Open a pipeline or workflow tab, or select a .hpl/.hwf file in the Explorer.",
            SWT.ICON_INFORMATION);
        return;
      }

      runLintOnFile(actualPath);
    } catch (Exception e) {
      log.logError("Error linting selected file: " + e.getMessage(), e);
      showErrorDialog(
          "Linting Error",
          "An error occurred while linting the selected file: " + e.getMessage(),
          e);
    }
  }

  /** Lint selected folder in explorer - context menu */
  @GuiMenuElement(
      root = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_LINT_FOLDER,
      type = GuiMenuElementType.MENU_ITEM,
      label = "i18n::ExplorerLintGuiPlugin.Menu.LintThisFolder.Label",
      image = "lint-check.svg",
      separator = false)
  public void lintSelectedFolderContext() {
    lintSelectedFolder();
  }

  /** Lint selected folder in explorer - main menu (keeping for backward compatibility) */
  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = "lint-selected-folder",
      type = GuiMenuElementType.MENU_ITEM,
      label = "i18n::ExplorerLintGuiPlugin.Menu.LintSelectedFolder.Label",
      parentId = LinterGuiPlugin.LINT_SUBMENU_ID,
      image = "lint-check.svg")
  public static void lintSelectedFolder() {
    try {

      ExplorerPerspective perspective = HopGui.getExplorerPerspective();
      if (perspective == null) {
        showMessage(
            "Explorer Not Available",
            "Could not access the Explorer perspective.",
            SWT.ICON_WARNING);
        return;
      }

      ExplorerFile selectedFile = perspective.getSelectedFile();
      if (selectedFile == null) {
        showMessage("No Folder Selected", "Please select a folder to lint.", SWT.ICON_INFORMATION);
        return;
      }

      String filename = selectedFile.getFilename();

      try {
        FileObject fileObject = HopVfs.getFileObject(filename);
        String actualPath = fileObject.getName().getPath();

        if (!fileObject.isFolder()) {
          showMessage("Not a Folder", "Please select a folder to lint.", SWT.ICON_INFORMATION);
          return;
        }

        runLintOnFolder(actualPath);

      } catch (Exception e) {
        LogChannel.GENERAL.logError("Error resolving folder path: " + e.getMessage(), e);
        showMessage(
            "Folder Path Error",
            "Could not resolve the selected folder path: " + e.getMessage(),
            SWT.ICON_ERROR);
        return;
      }

    } catch (Exception e) {
      log.logError("Error linting selected folder: " + e.getMessage(), e);
      showErrorDialog(
          "Linting Error",
          "An error occurred while linting the selected folder: " + e.getMessage(),
          e);
    }
  }

  private static void runLintOnFile(String filePath) {
    HopGui hopGui = HopGui.getInstance();
    if (hopGui == null) {
      showMessage("Hop GUI Not Available", "Could not access Hop GUI instance.", SWT.ICON_ERROR);
      return;
    }

    final String normalizedPath = LintPathUtils.normalizePath(filePath);
    log.logBasic("Linting " + LintEditorGraphHelper.displayName(normalizedPath) + "...");

    // Pipelines and workflows behave like the "Verify" button: open the file and surface the
    // findings in the editor's Problems tab. Other lintable files (e.g. metadata) keep the
    // standalone results popup.
    if (isGraphFile(normalizedPath)) {
      runLintIntoProblemsTab(hopGui, normalizedPath);
    } else {
      runLintWithPopup(hopGui, normalizedPath);
    }
  }

  private static boolean isGraphFile(String normalizedPath) {
    String lower = normalizedPath.toLowerCase();
    return lower.endsWith(".hpl") || lower.endsWith(".hwf");
  }

  /**
   * Lint a single pipeline/workflow: open it in the editor, run the lint, and show the results in
   * the editor's Problems tab (consistent with the Verify pipeline/workflow buttons).
   */
  private static void runLintIntoProblemsTab(HopGui hopGui, String normalizedPath) {
    final IVariables variables = hopGui.getVariables();
    final IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();

    // Opening the file must happen on the UI thread; the menu callback already runs there.
    IHopFileTypeHandler handler;
    try {
      handler = hopGui.fileDelegate.fileOpen(normalizedPath);
    } catch (Exception e) {
      log.logError("Error opening file for linting: " + e.getMessage(), e);
      showErrorDialog("Linting Error", "Could not open the file to lint: " + e.getMessage(), e);
      return;
    }

    if (!(handler instanceof HopGuiAbstractGraph)) {
      // Could not get an editor graph; fall back to the popup so the user still sees results.
      runLintWithPopup(hopGui, normalizedPath);
      return;
    }

    final HopGuiAbstractGraph graph = (HopGuiAbstractGraph) handler;
    final IHopFileTypeHandler openHandler = handler;

    Thread linterThread =
        new Thread(
            () -> {
              try {
                List<LintResult> results =
                    lintFileResults(normalizedPath, openHandler, metadataProvider, variables);

                LintResultsManager.getInstance().updateResultsForFile(normalizedPath, results);

                Display.getDefault()
                    .asyncExec(
                        () -> {
                          // Populates the editor Problems tab + toolbar badge (UI thread).
                          LintProblemsBarManager.getInstance().updateProblemsBar(normalizedPath);
                          refreshExplorerIcons();
                          LintResultsUi.logSummary(results, new File(normalizedPath).getName());
                          // Bring the Problems tab to the front once it has been populated.
                          Display.getDefault().asyncExec(() -> bringProblemsTabToFront(graph));
                        });
              } catch (Exception e) {
                log.logError("Error during file linting: " + e.getMessage(), e);
                Display.getDefault()
                    .asyncExec(
                        () ->
                            showErrorDialog(
                                "Linting Error",
                                "An error occurred during linting: " + e.getMessage(),
                                e));
              }
            },
            "HopLinter-File");
    linterThread.start();
  }

  /** Bring the editor's check/Problems tab to the front, creating it if needed. */
  private static void bringProblemsTabToFront(HopGuiAbstractGraph graph) {
    try {
      if (graph == null || graph.isDisposed()) {
        return;
      }
      if (graph instanceof HopGuiPipelineGraph pipelineGraph) {
        var delegate = pipelineGraph.pipelineCheckDelegate;
        if (delegate == null) {
          return;
        }
        delegate.addPipelineCheck();
        org.eclipse.swt.custom.CTabItem tab = delegate.getPipelineCheckTab();
        if (pipelineGraph.extraViewTabFolder != null
            && !pipelineGraph.extraViewTabFolder.isDisposed()
            && tab != null
            && !tab.isDisposed()) {
          pipelineGraph.extraViewTabFolder.setSelection(tab);
        }
      } else if (graph instanceof HopGuiWorkflowGraph workflowGraph) {
        var delegate = workflowGraph.workflowCheckDelegate;
        if (delegate == null) {
          return;
        }
        delegate.addWorkflowCheck();
        org.eclipse.swt.custom.CTabItem tab = delegate.getWorkflowCheckTab();
        if (workflowGraph.extraViewTabFolder != null
            && !workflowGraph.extraViewTabFolder.isDisposed()
            && tab != null
            && !tab.isDisposed()) {
          workflowGraph.extraViewTabFolder.setSelection(tab);
        }
      }
    } catch (Exception e) {
      log.logError("Error showing lint Problems tab: " + e.getMessage(), e);
    }
  }

  /** Lint a single file and show the results in the standalone popup (metadata files, etc.). */
  private static void runLintWithPopup(HopGui hopGui, String normalizedPath) {
    final IHopFileTypeHandler openHandler =
        LintEditorGraphHelper.findOpenHandlerForPath(normalizedPath);
    final IVariables variables = hopGui.getVariables();
    final IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();

    Thread linterThread =
        new Thread(
            () -> {
              try {
                List<LintResult> results =
                    lintFileResults(normalizedPath, openHandler, metadataProvider, variables);

                LintResultsManager.getInstance().updateResultsForFile(normalizedPath, results);

                Display.getDefault()
                    .asyncExec(
                        () -> {
                          LintProblemsBarManager.getInstance().updateProblemsBar(normalizedPath);
                          refreshExplorerIcons();
                          LintResultsUi.logSummary(results, new File(normalizedPath).getName());
                          LintResultsUi.showResultsForFile(normalizedPath);
                        });
              } catch (Exception e) {
                log.logError("Error during file linting: " + e.getMessage(), e);
                Display.getDefault()
                    .asyncExec(
                        () ->
                            showErrorDialog(
                                "Linting Error",
                                "An error occurred during linting: " + e.getMessage(),
                                e));
              }
            },
            "HopLinter-File");
    linterThread.start();
  }

  private static List<LintResult> lintFileResults(
      String normalizedPath,
      IHopFileTypeHandler openHandler,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws Exception {
    if (normalizedPath.toLowerCase().endsWith(".hpl")) {
      PipelineMeta pipelineMeta =
          resolvePipelineMeta(normalizedPath, openHandler, metadataProvider, variables);
      String pipelinePath =
          LintPathUtils.normalizePath(
              Utils.isEmpty(pipelineMeta.getFilename())
                  ? normalizedPath
                  : pipelineMeta.getFilename());
      return PipelineLintResultsBuilder.build(
          pipelineMeta, pipelinePath, metadataProvider, variables);
    }

    if (HopMetadataFileLoader.isMetadataJsonFile(normalizedPath)) {
      HopLinter linter = new HopLinter();
      return linter.lintFile(normalizedPath, metadataProvider, variables);
    }

    if (normalizedPath.toLowerCase().endsWith(".hwf")) {
      org.apache.hop.workflow.WorkflowMeta workflowMeta =
          resolveWorkflowMeta(normalizedPath, openHandler, metadataProvider, variables);
      String workflowPath =
          LintPathUtils.normalizePath(
              Utils.isEmpty(workflowMeta.getFilename())
                  ? normalizedPath
                  : workflowMeta.getFilename());
      if (openHandler instanceof HopGuiWorkflowGraph) {
        return WorkflowLintResultsBuilder.build(
            workflowMeta, workflowPath, metadataProvider, variables);
      }
      HopLinter linter = new HopLinter();
      return linter.lintFile(workflowPath, metadataProvider, variables);
    }

    throw new IllegalArgumentException("Not a lintable Hop file: " + normalizedPath);
  }

  private static PipelineMeta resolvePipelineMeta(
      String normalizedPath,
      IHopFileTypeHandler openHandler,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws org.apache.hop.core.exception.HopException {
    if (openHandler instanceof HopGuiPipelineGraph pipelineGraph
        && pipelineGraph.getPipelineMeta() != null
        && LintPathUtils.pathsMatch(pipelineGraph.getFilename(), normalizedPath)) {
      return pipelineGraph.getPipelineMeta();
    }
    return new PipelineMeta(normalizedPath, metadataProvider, variables);
  }

  private static org.apache.hop.workflow.WorkflowMeta resolveWorkflowMeta(
      String normalizedPath,
      IHopFileTypeHandler openHandler,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws org.apache.hop.core.exception.HopException {
    if (openHandler instanceof HopGuiWorkflowGraph workflowGraph
        && workflowGraph.getWorkflowMeta() != null
        && LintPathUtils.pathsMatch(workflowGraph.getFilename(), normalizedPath)) {
      return workflowGraph.getWorkflowMeta();
    }
    return new org.apache.hop.workflow.WorkflowMeta(variables, normalizedPath, metadataProvider);
  }

  private static void runLintOnFolder(String folderPath) {
    HopGui hopGui = HopGui.getInstance();
    if (hopGui == null) {
      showMessage("Hop GUI Not Available", "Could not access Hop GUI instance.", SWT.ICON_ERROR);
      return;
    }

    final IVariables variables = hopGui.getVariables();
    final IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();

    final LinterProgressDialog progressDialog = new LinterProgressDialog(hopGui.getShell());

    Thread linterThread =
        new Thread(
            () -> {
              try {
                HopLinter linter = new HopLinter();
                linter.loadConfigurationForContext(new File(folderPath));

                List<String> hopFilePaths = linter.findLintableFiles(folderPath, true);

                if (hopFilePaths.isEmpty()) {
                  Display.getDefault()
                      .asyncExec(
                          () -> {
                            progressDialog.close();
                            showMessage(
                                "No Hop Files",
                                "No .hpl or .hwf files found in the selected folder.",
                                SWT.ICON_INFORMATION);
                          });
                  return;
                }

                progressDialog.updateProgress(
                    "Found " + hopFilePaths.size() + " files to analyze", 0, hopFilePaths.size());

                Display.getDefault().asyncExec(progressDialog::show);

                List<LintResult> results = new java.util.ArrayList<>();
                int processedFilesCount = 0;

                for (String filePath : hopFilePaths) {
                  File file = new File(filePath);
                  if (progressDialog.isCancelled()) {
                    log.logDetailed("Folder linting cancelled by user");
                    return;
                  }

                  try {
                    progressDialog.updateProgress(
                        "Processing: " + file.getName(), processedFilesCount, hopFilePaths.size());
                    String normalizedPath = LintPathUtils.normalizePath(file.getAbsolutePath());
                    List<LintResult> fileResults;
                    if (normalizedPath.toLowerCase().endsWith(".hpl")) {
                      PipelineMeta pipelineMeta =
                          new PipelineMeta(file.getAbsolutePath(), metadataProvider, variables);
                      fileResults =
                          PipelineLintResultsBuilder.build(
                              pipelineMeta, normalizedPath, metadataProvider, variables);
                    } else if (HopMetadataFileLoader.isMetadataJsonFile(normalizedPath)) {
                      fileResults = linter.processFile(file, metadataProvider, variables);
                    } else {
                      fileResults = linter.processFile(file, metadataProvider, variables);
                    }
                    results.addAll(fileResults);
                    LintResultsManager.getInstance()
                        .updateResultsForFile(normalizedPath, fileResults);
                    processedFilesCount++;
                  } catch (Exception e) {
                    log.logError("Error processing file: " + file.getAbsolutePath(), e);
                    LintResult errorResult =
                        new LintResult(
                            "SYSTEM-001",
                            "File Processing Error",
                            "ERROR",
                            "Failed to process file: " + e.getMessage(),
                            LintPathUtils.normalizePath(file.getAbsolutePath()));
                    results.add(errorResult);
                    LintResultsManager.getInstance()
                        .updateResultsForFile(
                            LintPathUtils.normalizePath(file.getAbsolutePath()),
                            List.of(errorResult));
                    processedFilesCount++;
                  }
                }

                progressDialog.setComplete("Completed. Found " + results.size() + " issues");

                LintProblemsBarManager.getInstance().refreshAllOpenEditors();

                Display.getDefault()
                    .asyncExec(
                        () -> {
                          progressDialog.close();
                          refreshExplorerIcons();
                          LintResultsUi.logSummary(results, new File(folderPath).getName());
                          // A folder has no editor to put findings in, so this is one of the cases
                          // the results window exists for. Without this the run finished with
                          // nothing to show for it but a line in the log.
                          LintResultsUi.showResultsForFolder(folderPath);
                        });

              } catch (Exception e) {
                log.logError("Error during folder linting: " + e.getMessage(), e);
                Display.getDefault()
                    .asyncExec(
                        () -> {
                          progressDialog.close();
                          showErrorDialog(
                              "Linting Error",
                              "An error occurred during linting: " + e.getMessage(),
                              e);
                        });
              }
            });
    linterThread.start();
  }

  private static void showMessage(String title, String message, int style) {
    log.logBasic(title + ": " + message);
    HopGui hopGui = HopGui.getInstance();
    if (hopGui != null && hopGui.getShell() != null) {
      MessageBox box = new MessageBox(hopGui.getShell(), style | SWT.OK);
      box.setText(title);
      box.setMessage(message);
      box.open();
    }
  }

  private static void showErrorDialog(String title, String message, Exception e) {
    HopGui hopGui = HopGui.getInstance();
    if (hopGui != null && hopGui.getShell() != null) {
      new ErrorDialog(hopGui.getShell(), title, message, e);
    }
  }
}
