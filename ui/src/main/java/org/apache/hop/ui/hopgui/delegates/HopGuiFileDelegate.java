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

package org.apache.hop.ui.hopgui.delegates;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.diagram.DiagramExportService;
import org.apache.hop.core.diagram.IDiagramExporter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.security.Permission;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.dialog.SelectRowDialog;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.security.HopSecurityUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.dialog.DiagramExportDialog;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.database.DatabaseSqlEditorTab;
import org.apache.hop.ui.hopgui.perspective.execution.ExecutionPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;

public class HopGuiFileDelegate {

  private static final Class<?> PKG = BaseDialog.class;
  public static final String CONST_ERROR = "Error";
  private final HopGui hopGui;

  /** Returns a boolean indicating whether the gui in the process of closing files. */
  @Getter private boolean isClosing;

  public HopGuiFileDelegate(HopGui hopGui) {
    this.hopGui = hopGui;
    this.isClosing = false;
  }

  public IHopFileTypeHandler getActiveFileTypeHandler() {
    return hopGui.getActiveFileTypeHandler();
  }

  public void fileOpen() {
    try {
      // Ask for the file names
      // Check in the registry for extensions and names...
      //
      HopFileTypeRegistry fileRegistry = HopFileTypeRegistry.getInstance();

      String[] filenames =
          BaseDialog.presentMultiFileDialog(
              hopGui.getShell(),
              hopGui.getVariables(),
              fileRegistry.getFilterExtensions(),
              fileRegistry.getFilterNames(),
              true);
      if (filenames.length == 0) {
        return;
      }
      fileOpen(filenames);
    } catch (Exception e) {
      new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, "Error opening file", e);
    }
  }

  /**
   * Open all the given files. The files that can't be opened are collected and reported once, so a
   * single bad file doesn't stop the others from opening.
   *
   * @param filenames the files to open
   */
  public void fileOpen(String[] filenames) {
    List<String> errors = new ArrayList<>();

    for (int i = 0; i < filenames.length; i++) {
      String filename = hopGui.getVariables().resolve(filenames[i]);
      try {
        // Only bring the perspective of the last file to the front: switching for every file in
        // between simply flickers.
        //
        fileOpen(filename, i == filenames.length - 1);
      } catch (Exception e) {
        errors.add(filename + " : " + e.getMessage());
        hopGui.getLog().logError("Error opening file '" + filename + "'", e);
      }
    }

    if (!errors.isEmpty()) {
      MessageBox messageBox = new MessageBox(hopGui.getActiveShell(), SWT.ICON_ERROR | SWT.OK);
      messageBox.setText(CONST_ERROR);
      messageBox.setMessage("Error opening file:" + Const.CR + String.join(Const.CR, errors));
      messageBox.open();
    }
  }

  public IHopFileTypeHandler fileOpen(String filename) throws Exception {
    return fileOpen(filename, true);
  }

  public IHopFileTypeHandler fileOpen(String filename, boolean activatePerspective)
      throws Exception {
    HopFileTypeRegistry fileRegistry = HopFileTypeRegistry.getInstance();
    IHopFileType hopFile = fileRegistry.findHopFileType(filename);
    if (hopFile == null) {
      throw new HopException(
          "We looked at "
              + fileRegistry.getFileTypes().size()
              + " different Hop GUI file types but none know how to open file '"
              + filename
              + "'");
    }
    return fileOpenWithType(filename, hopFile, activatePerspective);
  }

  /**
   * Open a file with a specific file type. Used when restoring tabs so the same file can be
   * reopened in different modes (e.g. pipeline and text).
   */
  public IHopFileTypeHandler fileOpenWithType(
      String filename, IHopFileType hopFile, boolean activatePerspective) throws Exception {
    IHopFileTypeHandler fileTypeHandler = hopFile.openFile(hopGui, filename, hopGui.getVariables());
    if (fileTypeHandler != null) {
      hopGui.handleFileCapabilities(
          hopFile, fileTypeHandler, fileTypeHandler.hasChanged(), false, false);
      if (EnvironmentUtils.getInstance().isWeb()) {
        // Do it again to test
        hopGui.handleFileCapabilities(
            hopFile, fileTypeHandler, fileTypeHandler.hasChanged(), false, false);
      }

      // Also save the state of Hop GUI
      //
      hopGui.auditDelegate.writeLastOpenFiles();

      // Switch to the perspective
      //
      if (activatePerspective) {
        IHopPerspective perspective = hopGui.getPerspectiveManager().findPerspective(hopFile);
        if (perspective != null) {
          perspective.activate();
        }
      }
    }

    return fileTypeHandler;
  }

  /**
   * We need to figure out which file is open at the given time so we can save it. To do this we see
   * which is the active perspective. Then we ask the perspective for the shown/active file. We then
   * know the filter extension and name so we can show a dialog. We can then also have the {@link
   * IHopFileType} to save the file.
   *
   * @return The original filename, not having any variables replaced. It returns null if no file
   *     was saved
   */
  public String fileSaveAs() {
    try {
      if (!HopSecurityUi.check(Permission.FILE_SAVE)) {
        return null;
      }
      IHopFileTypeHandler typeHandler = getActiveFileTypeHandler();
      IHopFileType fileType = typeHandler.getFileType();
      FileObject file = null;
      if (!fileType.hasCapability(IHopFileType.CAPABILITY_SAVE_AS)) {
        return null;
      }
      if (typeHandler.getFilename() != null) {
        file = HopVfs.getFileObject(typeHandler.getFilename());
      }

      String filename =
          BaseDialog.presentFileDialog(
              true,
              hopGui.getShell(),
              null,
              file,
              fileType.getFilterExtensions(),
              fileType.getFilterNames(),
              true);
      if (filename == null) {
        return null;
      }

      filename = hopGui.getVariables().resolve(filename);

      typeHandler.saveAs(filename);

      // Also save the state of Hop GUI
      //
      hopGui.auditDelegate.writeLastOpenFiles();

      return filename;
    } catch (Exception e) {
      new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, "Error saving file", e);
      return null;
    }
  }

  public void fileSave() {
    try {
      if (!HopSecurityUi.check(Permission.FILE_SAVE)) {
        return;
      }
      IHopFileTypeHandler typeHandler = getActiveFileTypeHandler();
      IHopFileType fileType = typeHandler.getFileType();
      if (fileType.hasCapability(IHopFileType.CAPABILITY_SAVE)) {
        // Metadata just needs to be saved.
        //
        if (StringUtils.isEmpty(typeHandler.getFilename())
            && !fileType.hasCapability(IHopFileType.CAPABILITY_HANDLE_METADATA)) {
          // Ask for the filename: saveAs
          //
          fileSaveAs();
        } else {
          typeHandler.save();
        }
      }
    } catch (Exception e) {
      new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, "Error saving file", e);
    }
  }

  public boolean fileClose() {
    try {
      IHopPerspective perspective = hopGui.getActivePerspective();
      IHopFileTypeHandler typeHandler = getActiveFileTypeHandler();
      IHopFileType fileType = typeHandler.getFileType();
      if (fileType.hasCapability(IHopFileType.CAPABILITY_CLOSE)) {
        boolean removed =
            typeHandler instanceof DatabaseSqlEditorTab sqlTab
                ? sqlTab.requestClose()
                : perspective.remove(typeHandler);
        if (removed) {
          hopGui.auditDelegate.writeLastOpenFiles();
        }
        return removed;
      }
    } catch (Exception e) {
      new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, "Error saving/closing file", e);
    }
    return false;
  }

  /**
   * Go over all files and ask to save the ones who have changed.
   *
   * @return True if all files are saveguarded (or changes are ignored)
   */
  public boolean saveGuardAllFiles() {
    for (IHopPerspective perspective : hopGui.getPerspectiveManager().getPerspectives()) {
      List<TabItemHandler> tabItemHandlers = perspective.getItems();
      if (tabItemHandlers != null) {
        for (TabItemHandler tabItemHandler : tabItemHandlers) {
          IHopFileTypeHandler typeHandler = tabItemHandler.getTypeHandler();
          if (!typeHandler.isCloseable()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  public void closeAllFiles() {
    this.isClosing = true;
    try {
      for (IHopPerspective perspective : hopGui.getPerspectiveManager().getPerspectives()) {
        List<TabItemHandler> tabItemHandlers = perspective.getItems();
        if (tabItemHandlers != null) {
          // Copy the list to avoid changing the list we're editing (closing items)
          //
          List<TabItemHandler> handlers = new ArrayList<>(tabItemHandlers);
          for (TabItemHandler tabItemHandler : handlers) {
            IHopFileTypeHandler typeHandler = tabItemHandler.getTypeHandler();
            typeHandler.close();
          }
        }
      }

      // Execution Information tabs are not IHopFileTypeHandlers, so they never appear in
      // getItems(). Close them explicitly so project switches (and File → Close All) do not leave
      // viewers from the previous project open. Callers that need to remember tabs (project switch)
      // must call ExecutionPerspective.saveState() first.
      //
      ExecutionPerspective executionPerspective = findExecutionPerspective();
      if (executionPerspective != null) {
        executionPerspective.closeAllTabs();
      }
    } finally {
      this.isClosing = false;

      IHopFileTypeHandler activeHandler = hopGui.getActiveFileTypeHandler();
      if (activeHandler == null) {
        hopGui.handleFileCapabilities(new EmptyFileType(), false, false, false);
      } else {
        hopGui.handleFileCapabilities(
            activeHandler.getFileType(), activeHandler, activeHandler.hasChanged(), false, false);
      }
    }
  }

  /** When the app exits we need to see if all open files are saved in all perspectives... */
  public boolean fileExit() {

    if (!saveGuardAllFiles()) {
      return false;
    }

    // Check if we should ask the user for confirmation before exiting
    //
    PropsUi props = PropsUi.getInstance();
    if (props.showExitWarning()) {
      String title = BaseMessages.getString(PKG, "EnterOptionsDialog.AskOnExit.Label");
      String message = BaseMessages.getString(PKG, "EnterOptionsDialog.AskOnExit.ConfirmMessage");
      String toggleLabel =
          BaseMessages.getString(PKG, "EnterOptionsDialog.AskOnExit.DoNotAskAgain");
      String[] buttonLabels = {
        BaseMessages.getString(PKG, "System.Button.Yes"),
        BaseMessages.getString(PKG, "System.Button.No")
      };

      MessageDialogWithToggle dialog =
          new MessageDialogWithToggle(
              hopGui.getShell(),
              title,
              message,
              SWT.ICON_QUESTION,
              buttonLabels,
              toggleLabel,
              false);
      int answer = dialog.open();

      // If user checked "Do not ask this again", disable the exit warning
      if (dialog.getToggleState()) {
        props.setExitWarningShown(false);
        try {
          HopConfig.getInstance().saveToFile();
        } catch (Exception e) {
          new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, "Error saving configuration", e);
        }
      }

      // Return code 0 is Yes, 1 is No
      if (answer != 0) {
        return false; // User chose not to exit
      }
    }

    // Also save all the open files in a list
    //
    hopGui.auditDelegate.writeLastOpenFiles();

    // Save all open terminal tabs
    //
    if (hopGui.getTerminalPanel() != null) {
      hopGui.getTerminalPanel().saveTerminalsOnShutdown();
    }

    // Save explorer perspective state (file explorer panel visibility)
    //
    ExplorerPerspective explorerPerspective = findExplorerPerspective();
    if (explorerPerspective != null) {
      explorerPerspective.saveExplorerStateOnShutdown();
    }

    ExecutionPerspective executionPerspective = findExecutionPerspective();
    if (executionPerspective != null) {
      executionPerspective.saveState();
    }

    return true;
  }

  private ExecutionPerspective findExecutionPerspective() {
    if (hopGui.getPerspectiveManager() == null) {
      return null;
    }
    return hopGui.getPerspectiveManager().findPerspective(ExecutionPerspective.class);
  }

  private ExplorerPerspective findExplorerPerspective() {
    if (hopGui.getPerspectiveManager() == null) {
      return null;
    }
    return hopGui.getPerspectiveManager().findPerspective(ExplorerPerspective.class);
  }

  /** Show all the recent files in a new dialog... */
  public void fileOpenRecent() {
    // Get the recent files for the active perspective...
    //
    IHopPerspective perspective = hopGui.getActivePerspective();
    try {
      // Let's limit ourselves to 100 operations...
      //
      List<AuditEvent> events =
          AuditManager.findEvents(HopNamespace.getNamespace(), "file", "open", 100, true);
      Set<String> filenames = new HashSet<>();
      List<RowMetaAndData> rows = new ArrayList<>();
      IRowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaString("filename"));
      rowMeta.addValueMeta(new ValueMetaString("operation"));
      rowMeta.addValueMeta(new ValueMetaString("date"));

      for (AuditEvent event : events) {
        String filename = event.getName();
        if (!filenames.contains(filename)) {
          filenames.add(filename);
          String operation = event.getOperation();
          String dateString = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(event.getDate());
          rows.add(new RowMetaAndData(rowMeta, filename, operation, dateString));
        }
      }

      SelectRowDialog rowDialog =
          new SelectRowDialog(hopGui.getShell(), hopGui.getVariables(), SWT.NONE, rows);
      rowDialog.setTitle("Select the file to open");
      RowMetaAndData row = rowDialog.open();
      if (row != null) {
        String filename = row.getString("filename", null);
        fileOpen(filename);
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(), CONST_ERROR, "Error getting list of recently opened files", e);
    }
  }

  public void exportDiagram() {
    if (!HopSecurityUi.check(Permission.FILE_EXPORT)) {
      return;
    }
    try {
      Object subject = null;
      IVariables variables = hopGui.getVariables();
      String proposedName = "diagram.svg";
      String sourceFilename = null;

      IHopFileTypeHandler activeHandler = hopGui.getActiveFileTypeHandler();
      if (activeHandler != null) {
        subject = activeHandler.getSubject();
        if (activeHandler.getVariables() != null) {
          variables = activeHandler.getVariables();
        }
        if (StringUtils.isNotEmpty(activeHandler.getName())) {
          proposedName = activeHandler.getName() + ".svg";
        }
        if (StringUtils.isNotEmpty(activeHandler.getFilename())) {
          sourceFilename = activeHandler.getFilename();
        }
      }

      if (subject == null) {
        HopGuiPipelineGraph pipelineGraph = HopGui.getActivePipelineGraph();
        if (pipelineGraph != null) {
          subject = pipelineGraph.getPipelineMeta();
          variables = pipelineGraph.getVariables();
          proposedName = pipelineGraph.getPipelineMeta().getName() + ".svg";
          sourceFilename = pipelineGraph.getPipelineMeta().getFilename();
        }
      }

      if (subject == null) {
        HopGuiWorkflowGraph workflowGraph = HopGui.getActiveWorkflowGraph();
        if (workflowGraph != null) {
          subject = workflowGraph.getWorkflowMeta();
          variables = workflowGraph.getVariables();
          proposedName = workflowGraph.getWorkflowMeta().getName() + ".svg";
          sourceFilename = workflowGraph.getWorkflowMeta().getFilename();
        }
      }

      if (subject == null) {
        return;
      }

      List<IDiagramExporter<?>> exporters =
          DiagramExportService.getInstance().findExportersForSubject(subject);
      if (exporters.isEmpty()) {
        MessageBox box = new MessageBox(hopGui.getActiveShell(), SWT.OK | SWT.ICON_INFORMATION);
        box.setText(
            BaseMessages.getString(
                DiagramExportDialog.class, "DiagramExportDialog.NoExporters.Title"));
        box.setMessage(
            BaseMessages.getString(
                DiagramExportDialog.class, "DiagramExportDialog.NoExporters.Message"));
        box.open();
        return;
      }

      String defaultFilename = proposedExportFilename(variables, sourceFilename, proposedName);

      DiagramExportDialog dialog =
          new DiagramExportDialog(
              hopGui.getActiveShell(),
              variables,
              hopGui.getMetadataProvider(),
              subject,
              defaultFilename);
      dialog.open();

    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          CONST_ERROR,
          BaseMessages.getString(DiagramExportDialog.class, "DiagramExportDialog.Error.Message"),
          e);
    }
  }

  static String proposedExportFilename(
      IVariables variables, String sourceFilename, String proposedName) {
    try {
      if (StringUtils.isNotEmpty(sourceFilename)) {
        FileObject current = HopVfs.getFileObject(variables.resolve(sourceFilename));
        FileObject parent = current.getParent();
        if (parent != null) {
          FileObject target = parent.resolveFile(proposedName);
          return HopVfs.separatorsToUnix(target.getName().getURI());
        }
      }
    } catch (Exception e) {
      // Fall back to the user home directory.
    }
    String userHome = variables.getVariable("user.home", ".");
    return HopVfs.separatorsToUnix(userHome + "/" + proposedName);
  }

  public void exportToSvg() {
    exportDiagram();
  }
}
