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
import java.nio.file.Path;
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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lint.registry.RuleRegistry;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MenuAdapter;
import org.eclipse.swt.events.MenuEvent;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.MenuItem;

/** GUI plugin that adds lint actions to the Explorer perspective */
@GuiPlugin(
    id = "HopLintCheckerExplorerGuiPlugin",
    description = "Hop Lint Checker Explorer Integration")
public class ExplorerLintGuiPlugin {

  private static final Class<?> PKG = ExplorerLintGuiPlugin.class; // for i18n purposes

  private static final ILogChannel log = LogChannel.GENERAL;

  /** Used when there is no GUI to own this: unit tests. */
  private static ExplorerLintGuiPlugin fallback;

  private LintStatusFilePainter filePainter;

  /**
   * The plugin state of the GUI that asks for it. Hop Web serves many people from one JVM, each
   * with an Explorer of their own; the desktop has one HopGui and therefore one of these.
   */
  public static ExplorerLintGuiPlugin getInstance() {
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui != null) {
      return hopGui.getSessionSingleton(ExplorerLintGuiPlugin.class, ExplorerLintGuiPlugin::new);
    }
    synchronized (ExplorerLintGuiPlugin.class) {
      if (fallback == null) {
        fallback = new ExplorerLintGuiPlugin();
      }
      return fallback;
    }
  }

  public ExplorerLintGuiPlugin() {
    // The GUI plugin registry builds one of these for its callbacks, and getInstance() builds one
    // per session. Both go through the static helpers below, so this holds nothing itself.
  }

  @GuiCallback(callbackId = ExplorerPerspective.GUI_TOOLBAR_CREATED_CALLBACK_ID)
  public void registerExplorerPaintListener() {
    LintStatusFilePainter painter = getFilePainter();
    ExplorerPerspective perspective = ExplorerPerspective.getInstance();
    if (!perspective.getFilePaintListeners().contains(painter)) {
      perspective.getFilePaintListeners().add(painter);
    }
  }

  /**
   * The painter of the GUI that asks for it.
   *
   * <p>It caches the icons it composites, and an image belongs to the display that created it: in
   * Hop Web a shared painter hands one session images another session's display disposed, which SWT
   * reports as "Argument not valid" (issue #3508). It also remembers the Explorer tree it last
   * painted, and that tree is one session's widget.
   */
  public static LintStatusFilePainter getFilePainter() {
    return getInstance().filePainter();
  }

  /**
   * Built here rather than by the session singleton map itself: the painter subscribes to that
   * session's lint results, and asking for one singleton while another is being built is a
   * recursive update of the map they both live in.
   */
  private synchronized LintStatusFilePainter filePainter() {
    if (filePainter == null) {
      filePainter = new LintStatusFilePainter();
    }
    return filePainter;
  }

  /**
   * Repaint the explorer tree so lint status overlays update, without rebuilding the tree (which
   * would lose the user's selection and expanded folders). Falls back to a full refresh only when
   * the painter has not yet been attached.
   */
  public static void refreshExplorerIcons() {
    LintStatusFilePainter painter = getFilePainter();
    Display.getDefault()
        .asyncExec(
            () -> {
              try {
                if (painter != null) {
                  painter.repaintExplorerIcons();
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

  /**
   * Menu items are ordered by id, so this one has to sort after {@link #CONTEXT_MENU_LINT_FOLDER}
   * to sit with the other two rather than being pushed above the separator that starts the group.
   */
  private static final String CONTEXT_MENU_LINT_EXCLUDE = "context-menu-lint-ignore-selection";

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
    HopGui hopGui = HopGui.peekInstance();
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

    BackgroundThreadFacade.start(
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

    BackgroundThreadFacade.start(
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
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui == null) {
      showMessage("Hop GUI Not Available", "Could not access Hop GUI instance.", SWT.ICON_ERROR);
      return;
    }

    final IVariables variables = hopGui.getVariables();
    final IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();

    final LinterProgressDialog progressDialog = new LinterProgressDialog(hopGui.getShell());

    BackgroundThreadFacade.start(
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
                LintResultsManager.getInstance().updateResultsForFile(normalizedPath, fileResults);
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
                        LintPathUtils.normalizePath(file.getAbsolutePath()), List.of(errorResult));
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
        },
        "HopLinter-Folder");
  }

  /**
   * Keep the selected file or folder out of linting, on the record in the project configuration.
   *
   * <p>The file-level counterpart to accepting a finding on a single transform: a template that is
   * dynamic from end to end has nothing worth checking at design time, and saying so once beats
   * marking every transform in it.
   */
  @GuiMenuElement(
      root = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_LINT_EXCLUDE,
      type = GuiMenuElementType.MENU_ITEM,
      label = "i18n::ExplorerLintGuiPlugin.Menu.ExcludeFromLinting.Label",
      image = "lint-check.svg",
      separator = false)
  public void excludeFromLintingContext() {
    excludeFromLinting();
  }

  /** Tools → Lint → Exclude From Linting, for the Explorer selection. */
  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = "lint-selected-ignore",
      type = GuiMenuElementType.MENU_ITEM,
      label = "i18n::ExplorerLintGuiPlugin.Menu.ExcludeFromLinting.Label",
      parentId = LinterGuiPlugin.LINT_SUBMENU_ID,
      image = "lint-check.svg")
  public static void excludeFromLinting() {
    try {
      Selection selection = currentSelection();
      if (selection == null) {
        return;
      }

      if (selection.excluded()) {
        includeInLintingAgain(selection);
      } else {
        excludeFromLinting(selection);
      }
    } catch (Exception e) {
      log.logError("Error changing what is linted: " + e.getMessage(), e);
      showErrorDialog(
          BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Exclude.Failed.Title"),
          BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Exclude.Failed.Message"),
          e);
    }
  }

  private static void excludeFromLinting(Selection selection) throws Exception {
    HopGui hopGui = HopGui.peekInstance();
    String reason =
        new EnterStringDialog(
                hopGui.getShell(),
                "",
                BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Exclude.Reason.Title"),
                BaseMessages.getString(
                    PKG, "ExplorerLintGuiPlugin.Exclude.Reason.Message", selection.pattern()))
            .open();
    if (reason == null) {
      return;
    }

    LintPolicyYamlWriter.addExclude(selection.projectYaml().toPath(), selection.pattern(), reason);
    refreshAfterPolicyChange(selection.path());

    showMessage(
        BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Exclude.Done.Title"),
        BaseMessages.getString(
            PKG,
            "ExplorerLintGuiPlugin.Exclude.Done.Message",
            selection.pattern(),
            selection.projectYaml().getPath()),
        SWT.ICON_INFORMATION);
  }

  private static void includeInLintingAgain(Selection selection) throws Exception {
    boolean removed =
        LintPolicyYamlWriter.removeExclude(selection.projectYaml().toPath(), selection.pattern());
    if (!removed) {
      // Excluded by a pattern somebody wrote by hand, "templates/**" rather than this file: the
      // entry to remove is a judgement call, so say where to look instead of guessing.
      showMessage(
          BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Include.ByPattern.Title"),
          BaseMessages.getString(
              PKG,
              "ExplorerLintGuiPlugin.Include.ByPattern.Message",
              selection.pattern(),
              selection.projectYaml().getPath()),
          SWT.ICON_INFORMATION);
      return;
    }

    refreshAfterPolicyChange(selection.path());
    BackgroundLintService.getInstance().scheduleFileLint(selection.path(), true);

    showMessage(
        BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Include.Done.Title"),
        BaseMessages.getString(
            PKG, "ExplorerLintGuiPlugin.Include.Done.Message", selection.pattern()),
        SWT.ICON_INFORMATION);
  }

  /** What is selected in the Explorer, as the project configuration would record it. */
  private record Selection(String path, File projectYaml, String pattern, boolean excluded) {}

  /**
   * The Explorer selection resolved against the project configuration, or null when there is
   * nothing to act on. Complains to the user itself, so callers only have to check for null.
   */
  private static Selection currentSelection() {
    ExplorerPerspective perspective = HopGui.getExplorerPerspective();
    ExplorerFile selectedFile = perspective == null ? null : perspective.getSelectedFile();
    if (selectedFile == null || Utils.isEmpty(selectedFile.getFilename())) {
      showMessage(
          BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Exclude.NoSelection.Title"),
          BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Exclude.NoSelection.Message"),
          SWT.ICON_INFORMATION);
      return null;
    }
    Selection selection = resolveSelection(selectedFile.getFilename());
    if (selection == null) {
      explainWhyNot(LintPathUtils.normalizePath(selectedFile.getFilename()));
    }
    return selection;
  }

  /**
   * Two different problems look the same to the caller: there is no project configuration to write
   * to, or there is one but the file sits outside the folder its patterns are relative to. Saying
   * which is which is the difference between a message someone can act on and one they cannot.
   */
  private static void explainWhyNot(String path) {
    File projectYaml = resolveProjectYaml(path);
    if (projectYaml == null || projectYaml.getParentFile() == null) {
      showMessage(
          BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Exclude.NoProject.Title"),
          BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Exclude.NoProject.Message"),
          SWT.ICON_INFORMATION);
      return;
    }
    showMessage(
        BaseMessages.getString(PKG, "ExplorerLintGuiPlugin.Exclude.OutsideProject.Title"),
        BaseMessages.getString(
            PKG,
            "ExplorerLintGuiPlugin.Exclude.OutsideProject.Message",
            projectYaml.getParentFile().getAbsolutePath()),
        SWT.ICON_INFORMATION);
  }

  /** As above but silent, for deciding what the menu item should say. */
  private static Selection resolveSelection(String filename) {
    String path = LintPathUtils.normalizePath(filename);
    File projectYaml = resolveProjectYaml(path);
    if (projectYaml == null || projectYaml.getParentFile() == null) {
      return null;
    }
    Path projectRoot = projectYaml.getParentFile().toPath().toAbsolutePath();
    String pattern = LintPolicy.relativise(path, projectRoot);
    if (new File(path).isAbsolute() && pattern.equals(path)) {
      // relativise hands the path back untouched when it sits outside the project, and an
      // absolute path in a portable configuration file would only work on this machine.
      return null;
    }
    if (isFolder(filename)) {
      pattern = pattern + "/**";
    }
    return new Selection(path, projectYaml, pattern, isExcluded(path));
  }

  private static boolean isExcluded(String path) {
    try {
      HopLinter linter = new HopLinter();
      linter.loadConfigurationForContext(new File(path));
      return linter.isExcluded(path);
    } catch (Exception e) {
      log.logDetailed("Could not read the lint configuration for " + path + ": " + e.getMessage());
      return false;
    }
  }

  /** The findings on screen were computed under the old configuration. */
  private static void refreshAfterPolicyChange(String path) {
    BackgroundLintService.getInstance().getTracker().invalidate(path);
    LintResultsManager.getInstance().updateResultsForFile(path, List.of());
    LintProblemsBarManager.getInstance().updateProblemsBar(path);
    LintCanvasOverlayRefresh.redrawOpenGraphs();
  }

  /**
   * Keep the menu item saying what it will do.
   *
   * <p>The wording is decided when the menu opens rather than when it is built, because it depends
   * on what is selected: the same item excludes a file that is linted and puts back one that is
   * not. A one-way "Exclude From Linting" leaves people editing YAML to undo a menu click.
   */
  @GuiCallback(callbackId = ExplorerPerspective.GUI_CONTEXT_MENU_CREATED_CALLBACK_ID)
  public void trackExplorerSelectionForLintMenu() {
    ExplorerPerspective perspective = ExplorerPerspective.getInstance();
    if (perspective == null || perspective.getMenuWidgets() == null) {
      return;
    }
    MenuItem item = perspective.getMenuWidgets().findMenuItem(CONTEXT_MENU_LINT_EXCLUDE);
    if (item == null || item.isDisposed() || item.getParent() == null) {
      return;
    }
    item.getParent()
        .addMenuListener(
            new MenuAdapter() {
              @Override
              public void menuShown(MenuEvent event) {
                updateExcludeMenuItem(item);
              }
            });
  }

  private static void updateExcludeMenuItem(MenuItem item) {
    if (item.isDisposed()) {
      return;
    }
    try {
      ExplorerPerspective perspective = HopGui.getExplorerPerspective();
      ExplorerFile selectedFile = perspective == null ? null : perspective.getSelectedFile();
      Selection selection =
          selectedFile == null || Utils.isEmpty(selectedFile.getFilename())
              ? null
              : resolveSelection(selectedFile.getFilename());
      boolean excluded = selection != null && selection.excluded();
      item.setText(
          BaseMessages.getString(
              PKG,
              excluded
                  ? "ExplorerLintGuiPlugin.Menu.IncludeInLinting.Label"
                  : "ExplorerLintGuiPlugin.Menu.ExcludeFromLinting.Label"));
    } catch (Exception e) {
      log.logDetailed("Could not work out the lint menu wording: " + e.getMessage());
    }
  }

  /**
   * The hop-lint.yml governing the selection, creating a path for one when the project has none.
   */
  static File resolveProjectYaml(String selectedPath) {
    File found = RuleRegistry.getInstance().findProjectYaml(new File(selectedPath));
    if (found != null) {
      return found;
    }
    try {
      String projectPath = LinterConfigPlugin.getInstance().getProjectPath();
      if (!Utils.isEmpty(projectPath)) {
        return new File(projectPath, "hop-lint.yml");
      }
    } catch (Exception e) {
      log.logDetailed("No project configuration available: " + e.getMessage());
    }
    return null;
  }

  private static boolean isFolder(String filename) {
    try (FileObject fileObject = HopVfs.getFileObject(filename)) {
      return fileObject != null && fileObject.isFolder();
    } catch (Exception e) {
      return new File(filename).isDirectory();
    }
  }

  private static void showMessage(String title, String message, int style) {
    log.logBasic(title + ": " + message);
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui != null && hopGui.getShell() != null) {
      MessageBox box = new MessageBox(hopGui.getShell(), style | SWT.OK);
      box.setText(title);
      box.setMessage(message);
      box.open();
    }
  }

  private static void showErrorDialog(String title, String message, Exception e) {
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui != null && hopGui.getShell() != null) {
      new ErrorDialog(hopGui.getShell(), title, message, e);
    }
  }
}
