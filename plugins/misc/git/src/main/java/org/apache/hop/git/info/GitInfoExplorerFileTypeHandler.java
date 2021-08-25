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
 *
 */

package org.apache.hop.git.info;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.git.GitGuiPlugin;
import org.apache.hop.git.HopDiff;
import org.apache.hop.git.model.UIFile;
import org.apache.hop.git.model.UIGit;
import org.apache.hop.git.model.VCS;
import org.apache.hop.git.model.revision.ObjectRevision;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.*;

/** Show git information about a file or folder : revisions */
public class GitInfoExplorerFileTypeHandler extends BaseExplorerFileTypeHandler
    implements IExplorerFileTypeHandler, Listener {

  private String id;

  private Composite parentComposite;

  private Text wFile;
  private Text wStatus;
  private Text wBranch;
  private TableView wFiles;
  private TableView wRevisions;
  private Composite wDiffComposite;
  private Text wDiff;
  private Button wbDiff;

  public GitInfoExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
    id = UUID.randomUUID().toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GitInfoExplorerFileTypeHandler that = (GitInfoExplorerFileTypeHandler) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  // Render the SVG file...
  //
  @Override
  public void renderFile(Composite composite) {
    this.parentComposite = composite;

    PropsUi props = PropsUi.getInstance();
    int margin = props.getMargin();

    // A label showing the file/folder
    //
    Label wlFile = new Label(composite, SWT.LEFT | SWT.SINGLE);
    props.setLook(wlFile);
    wlFile.setText("File or folder");
    FormData fdlFile = new FormData();
    fdlFile.left = new FormAttachment(0, 0);
    fdlFile.top = new FormAttachment(0, 0);
    wlFile.setLayoutData(fdlFile);
    wFile = new Text(composite, SWT.LEFT | SWT.SINGLE | SWT.BORDER);
    wFile.setEditable(false);
    props.setLook(wFile);
    FormData fdFile = new FormData();
    fdFile.left = new FormAttachment(wlFile, 2 * margin);
    fdFile.top = new FormAttachment(wlFile, 0, SWT.CENTER);
    fdFile.right = new FormAttachment(100, 0);
    wFile.setLayoutData(fdFile);
    Control lastControl = wFile;

    // The file status:
    //
    Label wlStatus = new Label(composite, SWT.LEFT | SWT.SINGLE);
    props.setLook(wlStatus);
    wlStatus.setText("Status");
    FormData fdlStatus = new FormData();
    fdlStatus.left = new FormAttachment(0, 0);
    fdlStatus.top = new FormAttachment(lastControl, margin);
    wlStatus.setLayoutData(fdlStatus);
    wStatus = new Text(composite, SWT.LEFT | SWT.SINGLE | SWT.BORDER);
    props.setLook(wStatus);
    wStatus.setEditable(false);
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(wlFile, 2 * margin);
    fdStatus.top = new FormAttachment(wlStatus, 0, SWT.CENTER);
    fdStatus.right = new FormAttachment(100, 0);
    wStatus.setLayoutData(fdStatus);
    lastControl = wStatus;

    // The branch we're in
    //
    Label wlBranch = new Label(composite, SWT.LEFT | SWT.SINGLE);
    props.setLook(wlBranch);
    wlBranch.setText("Branch");
    FormData fdlBranch = new FormData();
    fdlBranch.left = new FormAttachment(0, 0);
    fdlBranch.top = new FormAttachment(lastControl, margin);
    wlBranch.setLayoutData(fdlBranch);
    wBranch = new Text(composite, SWT.LEFT | SWT.SINGLE | SWT.BORDER);
    props.setLook(wBranch);
    wBranch.setEditable(false);
    FormData fdBranch = new FormData();
    fdBranch.left = new FormAttachment(wlFile, 2 * margin);
    fdBranch.top = new FormAttachment(wlBranch, 0, SWT.CENTER);
    fdBranch.right = new FormAttachment(100, 0);
    wBranch.setLayoutData(fdBranch);
    lastControl = wBranch;

    // The revisions
    //
    Label wlRevisions = new Label(composite, SWT.LEFT | SWT.SINGLE);
    props.setLook(wlRevisions);
    wlRevisions.setText("Revisions");
    FormData fdlRevisions = new FormData();
    fdlRevisions.left = new FormAttachment(0, 0);
    fdlRevisions.top = new FormAttachment(lastControl, margin);
    fdlRevisions.right = new FormAttachment(100, 0);
    wlRevisions.setLayoutData(fdlRevisions);
    lastControl = wlRevisions;

    ColumnInfo[] revisionColumns = {
      new ColumnInfo("RevisionId", ColumnInfo.COLUMN_TYPE_TEXT),
      new ColumnInfo("Creation", ColumnInfo.COLUMN_TYPE_TEXT),
      new ColumnInfo("Login", ColumnInfo.COLUMN_TYPE_TEXT),
      new ColumnInfo("Comment", ColumnInfo.COLUMN_TYPE_TEXT),
    };
    wRevisions =
        new TableView(hopGui.getVariables(), composite, SWT.NONE, revisionColumns, 1, null, props);
    wRevisions.setReadonly(true);
    props.setLook(wRevisions);
    FormData fdRevisions = new FormData();
    fdRevisions.left = new FormAttachment(0, margin);
    fdRevisions.top = new FormAttachment(lastControl, margin);
    fdRevisions.right = new FormAttachment(100, 0);
    fdRevisions.bottom = new FormAttachment(40, 0);
    wRevisions.setLayoutData(fdRevisions);
    wRevisions.table.addListener(SWT.Selection, e -> refreshChangedFiles());
    lastControl = wRevisions;

    Label wlFiles = new Label(composite, SWT.LEFT | SWT.SINGLE);
    props.setLook(wlFiles);
    wlFiles.setText("Changed files");
    FormData fdlFiles = new FormData();
    fdlFiles.left = new FormAttachment(0, 0);
    fdlFiles.right = new FormAttachment(100, 0);
    fdlFiles.top = new FormAttachment(lastControl, margin);
    wlFiles.setLayoutData(fdlFiles);
    lastControl = wlFiles;

    // The files
    //
    SashForm sashForm = new SashForm(composite, SWT.HORIZONTAL);
    props.setLook(sashForm);
    FormData fdSashForm = new FormData();
    fdSashForm.left = new FormAttachment(0, 0);
    fdSashForm.right = new FormAttachment(100, 0);
    fdSashForm.top = new FormAttachment(lastControl, margin);
    fdSashForm.bottom = new FormAttachment(100, 0);
    sashForm.setLayoutData(fdSashForm);

    ColumnInfo[] filesColumns = {
      new ColumnInfo("Filename", ColumnInfo.COLUMN_TYPE_TEXT),
      new ColumnInfo("Status", ColumnInfo.COLUMN_TYPE_TEXT),
      new ColumnInfo("Staged?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {"Y", "N"}),
    };
    wFiles = new TableView(hopGui.getVariables(), sashForm, SWT.NONE, filesColumns, 1, null, props);
    wFiles.setReadonly(true);
    props.setLook(wFiles);
    wFiles.table.addListener(SWT.Selection, e -> fileSelected());

    wDiffComposite = new Composite(sashForm, SWT.NONE);
    wDiffComposite.setLayout(new FormLayout());

    wbDiff = new Button(wDiffComposite, SWT.PUSH);
    props.setLook(wbDiff);
    wbDiff.setEnabled(false);
    wbDiff.setText("Visual diff");
    wbDiff.addListener(SWT.Selection, e -> showHopFileDiff());
    FormData fdbDiff = new FormData();
    fdbDiff.right = new FormAttachment(100, 0);
    fdbDiff.top = new FormAttachment(0, 0);
    wbDiff.setLayoutData(fdbDiff);

    Label wlDiff = new Label(wDiffComposite, SWT.LEFT | SWT.SINGLE);
    props.setLook(wlDiff);
    wlDiff.setText("Select a file to see the text diff below:");
    FormData fdlDiff = new FormData();
    fdlDiff.left = new FormAttachment(0, 0);
    fdlDiff.right = new FormAttachment(wbDiff, -margin);
    fdlDiff.top = new FormAttachment(wbDiff, 0, SWT.CENTER);
    wlDiff.setLayoutData(fdlDiff);

    wDiff = new Text(wDiffComposite, SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    props.setLook(wDiff);
    FormData fdDiff = new FormData();
    fdDiff.left = new FormAttachment(0, 0);
    fdDiff.right = new FormAttachment(100, 0);
    fdDiff.top = new FormAttachment(wbDiff, margin);
    fdDiff.bottom = new FormAttachment(100, 0);
    wDiff.setLayoutData(fdDiff);

    sashForm.setWeights(new int[] {40, 60});

    refresh();

    perspective.getTree().addListener(SWT.Selection, this);
  }

  public void showHopFileDiff() {
    if (wFiles.getSelectionIndices().length == 0) {
      return;
    }
    TableItem fileItem = wFiles.table.getSelection()[0];
    String filename = fileItem.getText(1);
    if (StringUtils.isEmpty(filename)) {
      return;
    }

    GitGuiPlugin guiPlugin = GitGuiPlugin.getInstance();
    UIGit git = guiPlugin.getGit();

    try {

      // Determine revisions...
      //
      // A revision/commit was selected...
      //
      TableItem revisionItem = wRevisions.table.getSelection()[0];
      String revisionId = revisionItem.getText(1);
      boolean workingTree = VCS.WORKINGTREE.equals(revisionId);

      // A file in wFiles was selected...
      //
      boolean staged = "Y".equalsIgnoreCase(fileItem.getText(3));

      String commitIdNew;
      String commitIdOld;

      if (workingTree) {
        commitIdNew = VCS.WORKINGTREE;
        commitIdOld = Constants.HEAD;
      } else {
        commitIdNew = revisionId;
        commitIdOld = git.getParentCommitId(revisionId);

        if (commitIdOld == null) {
          return; // No parent to compare to
        }
      }

      if (commitIdNew.equals(commitIdOld)) {
        return; // No changes expected
      }

      HopDataOrchestrationPerspective dop = HopGui.getDataOrchestrationPerspective();

      if (dop.getPipelineFileType().isHandledBy(filename, false)) {
        // A pipeline
        //
        showPipelineFileDiff(filename, commitIdNew, commitIdOld);
      } else if (dop.getWorkflowFileType().isHandledBy(filename, false)) {
        // A workflow
        //
        showWorkflowFileDiff(filename, commitIdNew, commitIdOld);
      }

    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), "Error", "Error while doing visual diff on file : " + filename, e);
    }
  }

  private void showPipelineFileDiff(String filename, String commitIdNew, String commitIdOld)
      throws HopException {
    GitGuiPlugin guiPlugin = GitGuiPlugin.getInstance();
    UIGit git = guiPlugin.getGit();

    InputStream xmlStreamOld = null;
    InputStream xmlStreamNew = null;

    try {
      xmlStreamOld = git.open(filename, commitIdOld);
      xmlStreamNew = git.open(filename, commitIdNew);

      PipelineMeta pipelineMetaOld =
          new PipelineMeta(xmlStreamOld, hopGui.getMetadataProvider(), true, hopGui.getVariables());
      PipelineMeta pipelineMetaNew =
          new PipelineMeta(xmlStreamNew, hopGui.getMetadataProvider(), true, hopGui.getVariables());

      pipelineMetaOld = HopDiff.compareTransforms(pipelineMetaOld, pipelineMetaNew, true);
      pipelineMetaOld = HopDiff.comparePipelineHops(pipelineMetaOld, pipelineMetaNew, true);
      pipelineMetaNew = HopDiff.compareTransforms(pipelineMetaNew, pipelineMetaOld, false);
      pipelineMetaNew = HopDiff.comparePipelineHops(pipelineMetaNew, pipelineMetaOld, false);

      pipelineMetaOld.setPipelineVersion("git: " + commitIdOld);
      pipelineMetaNew.setPipelineVersion("git: " + commitIdNew);

      // Change the name to indicate the git revisions of the file
      //
      pipelineMetaOld.setName(
          String.format(
              "%s (%s -> %s)",
              pipelineMetaOld.getName(),
              git.getShortenedName(commitIdOld, VCS.TYPE_COMMIT),
              git.getShortenedName(commitIdNew, VCS.TYPE_COMMIT)));
      pipelineMetaOld.setNameSynchronizedWithFilename(false);

      pipelineMetaNew.setName(
          String.format(
              "%s (%s -> %s)",
              pipelineMetaNew.getName(),
              git.getShortenedName(commitIdNew, VCS.TYPE_COMMIT),
              git.getShortenedName(commitIdOld, VCS.TYPE_COMMIT)));
      pipelineMetaNew.setNameSynchronizedWithFilename(false);

      // Load both in the data orchestration perspective...
      //
      HopDataOrchestrationPerspective dop = HopGui.getDataOrchestrationPerspective();
      dop.addPipeline(hopGui, pipelineMetaOld, dop.getPipelineFileType());
      dop.addPipeline(hopGui, pipelineMetaNew, dop.getPipelineFileType());
      dop.activate();
    } finally {
      try {
        if (xmlStreamOld != null) {
          xmlStreamOld.close();
        }
        if (xmlStreamNew != null) {
          xmlStreamNew.close();
        }
      } catch (Exception e) {
        LogChannel.UI.logError("Error closing XML file after reading", e);
      }
    }
  }

  private void showWorkflowFileDiff(String filename, String commitIdNew, String commitIdOld)
      throws HopException {
    GitGuiPlugin guiPlugin = GitGuiPlugin.getInstance();
    UIGit git = guiPlugin.getGit();

    InputStream xmlStreamOld = null;
    InputStream xmlStreamNew = null;

    try {
      xmlStreamOld = git.open(filename, commitIdOld);
      xmlStreamNew = git.open(filename, commitIdNew);

      WorkflowMeta workflowMetaOld =
          new WorkflowMeta(xmlStreamOld, hopGui.getMetadataProvider(), hopGui.getVariables());
      WorkflowMeta workflowMetaNew =
          new WorkflowMeta(xmlStreamNew, hopGui.getMetadataProvider(), hopGui.getVariables());

      workflowMetaOld = HopDiff.compareActions(workflowMetaOld, workflowMetaNew, true);
      workflowMetaOld = HopDiff.compareWorkflowHops(workflowMetaOld, workflowMetaNew, true);
      workflowMetaNew = HopDiff.compareActions(workflowMetaNew, workflowMetaOld, false);
      workflowMetaNew = HopDiff.compareWorkflowHops(workflowMetaNew, workflowMetaOld, false);

      workflowMetaOld.setWorkflowVersion("git: " + commitIdOld);
      workflowMetaNew.setWorkflowVersion("git: " + commitIdNew);

      // Change the name to indicate the git revisions of the file
      //
      workflowMetaOld.setName(
          String.format(
              "%s (%s -> %s)",
              workflowMetaOld.getName(),
              git.getShortenedName(commitIdOld, VCS.TYPE_COMMIT),
              git.getShortenedName(commitIdNew, VCS.TYPE_COMMIT)));
      workflowMetaOld.setNameSynchronizedWithFilename(false);

      workflowMetaNew.setName(
          String.format(
              "%s (%s -> %s)",
              workflowMetaNew.getName(),
              git.getShortenedName(commitIdNew, VCS.TYPE_COMMIT),
              git.getShortenedName(commitIdOld, VCS.TYPE_COMMIT)));
      workflowMetaNew.setNameSynchronizedWithFilename(false);

      // Load both in the data orchestration perspective...
      //
      HopDataOrchestrationPerspective dop = HopGui.getDataOrchestrationPerspective();
      dop.addWorkflow(hopGui, workflowMetaOld, dop.getWorkflowFileType());
      dop.addWorkflow(hopGui, workflowMetaNew, dop.getWorkflowFileType());
      dop.activate();
    } finally {
      try {
        if (xmlStreamOld != null) {
          xmlStreamOld.close();
        }
        if (xmlStreamNew != null) {
          xmlStreamNew.close();
        }
      } catch (Exception e) {
        LogChannel.UI.logError("Error closing XML file after reading", e);
      }
    }
  }

  @Override
  public void close() {
    perspective.getTree().removeListener(SWT.Selection, GitInfoExplorerFileTypeHandler.this);
  }

  /**
   * Handle the Selection event on the parent Explorer perspective Tree. If this info tab is in
   * perspective we can ask if we want to change to the selected folder...
   *
   * @param event
   */
  @Override
  public void handleEvent(Event event) {
    if (parentComposite == null || parentComposite.isDisposed() || !parentComposite.isVisible()) {
      return;
    }

    ExplorerFile file = perspective.getSelectedFile();
    if (file == null) {
      return;
    }
    // If the file is already selected simply refresh the view.
    //
    if (file.getFilename().equals(wFile.getText())) {
      refresh();
      return;
    }
    try {
      String relativePath = calculateRelativePath(perspective.getRootFolder(), file.getFilename());

      if (".".equals(relativePath)) {
        relativePath = "Git project root";
      }
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      box.setText("Change location?");
      box.setMessage(
          "Do you want to change the location of the current git info view?"
              + Const.CR
              + Const.CR
              + relativePath);
      int answer = box.open();
      if ((answer & SWT.YES) != 0) {
        this.explorerFile = file;
        refresh();
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error calculating relative path to change git info view", e);
    }
  }

  public void refresh() {
    // Relative path of the file...
    //
    GitGuiPlugin guiPlugin = GitGuiPlugin.getInstance();
    UIGit git = guiPlugin.getGit();
    List<ObjectRevision> revisions = new ArrayList<>();
    try {
      String relativePath =
          calculateRelativePath(perspective.getRootFolder(), explorerFile.getFilename());
      revisions = git.getRevisions(relativePath);
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error getting git object revisions for path: " + explorerFile.getFilename(), e);
    }

    wFile.setText(Const.NVL(explorerFile.getFilename(), ""));
    wStatus.setText(getStatusDescription(guiPlugin));
    wBranch.setText(Const.NVL(git.getBranch(), ""));

    wRevisions.removeAll();
    for (ObjectRevision revision : revisions) {
      TableItem item = new TableItem(wRevisions.table, SWT.NONE);
      item.setText(1, Const.NVL(revision.getRevisionId(), ""));
      item.setText(2, getDateString(revision.getCreationDate()));
      item.setText(3, Const.NVL(revision.getLogin(), ""));
      item.setText(4, Const.NVL(revision.getComment(), ""));
    }
    wRevisions.optimizeTableView();
    if (!revisions.isEmpty()) {
      // Select the first line
      wRevisions.setSelection(new int[] {0});
    }

    refreshChangedFiles();
  }

  private String calculateRelativePath(String rootFolder, String filename)
      throws HopFileException, FileSystemException {
    FileObject root = HopVfs.getFileObject(rootFolder);
    FileObject file = HopVfs.getFileObject(filename);

    String relativePath = root.getName().getRelativeName(file.getName());
    return relativePath;
  }

  private void fileSelected() {
    String filename = showFileDiff();
    wbDiff.setEnabled(false);

    // Enable visual diff button?
    //
    if (filename != null) {

      // If it's the last revision then we can't compare it to the previous one...
      //
      if (wRevisions.getSelectionIndex() == wRevisions.table.getItemCount() - 1) {
        return; // Don't even try to compare with something that's not there.
      }

      try {
        if (HopGui.getDataOrchestrationPerspective()
            .getPipelineFileType()
            .isHandledBy(filename, false)) {
          wbDiff.setEnabled(true);
        }
        if (HopGui.getDataOrchestrationPerspective()
            .getWorkflowFileType()
            .isHandledBy(filename, false)) {
          wbDiff.setEnabled(true);
        }
      } catch (Exception e) {
        LogChannel.UI.logError(
            "Error checking if this file is a pipeline or workflow: " + filename, e);
      }
    }
  }

  private String showFileDiff() {
    GitGuiPlugin guiPlugin = GitGuiPlugin.getInstance();
    UIGit git = guiPlugin.getGit();

    if (wRevisions.getSelectionIndices().length == 0) {
      return null;
    }
    if (wFiles.getSelectionIndices().length == 0) {
      return null;
    }

    String diff;

    // A revision/commit was selected...
    //
    TableItem revisionItem = wRevisions.table.getSelection()[0];
    String revisionId = revisionItem.getText(1);
    boolean workingTree = VCS.WORKINGTREE.equals(revisionId);

    // A file in wFiles was selected...
    //
    TableItem fileItem = wFiles.table.getSelection()[0];
    String filename = fileItem.getText(1);
    boolean staged = "Y".equalsIgnoreCase(fileItem.getText(3));

    if (workingTree) {
      if (staged) {
        diff = git.diff(Constants.HEAD, VCS.INDEX, filename);
      } else {
        diff = git.diff(VCS.INDEX, VCS.WORKINGTREE, filename);
      }
    } else {
      String parentCommitId = git.getParentCommitId(revisionId);
      diff = git.diff(parentCommitId, revisionId, filename);
    }
    wDiff.setText(Const.NVL(diff, ""));
    return filename;
  }

  private void refreshChangedFiles() {

    GitGuiPlugin guiPlugin = GitGuiPlugin.getInstance();
    UIGit git = guiPlugin.getGit();
    List<UIFile> changedFiles;

    String selectedFile = wFile.getText();
    String rootFolder = git.getDirectory();
    boolean showStaged = true;

    // Pick up the revision ID...
    //
    if (wRevisions.table.getSelectionCount() == 0) {
      changedFiles = new ArrayList<>(guiPlugin.getChangedFiles().values());
    } else {
      String revisionId = wRevisions.table.getSelection()[0].getText(1);

      if (UIGit.WORKINGTREE.equals(revisionId)) {
        changedFiles = new ArrayList<>();
        for (UIFile changedFile : guiPlugin.getChangedFiles().values()) {
          if (isFilteredPath(rootFolder, changedFile.getName(), selectedFile)) {
            changedFiles.add(changedFile);
          }
        }

      } else {
        showStaged = false;
        changedFiles = new ArrayList<>();
        try {
          RevCommit commit = git.resolve(revisionId);
          RevTree tree = commit.getTree();
          try (TreeWalk treeWalk = new TreeWalk(git.getGit().getRepository())) {
            treeWalk.setRecursive(true);
            treeWalk.reset(tree);
            while (treeWalk.next()) {
              String path = treeWalk.getPathString();
              if (isFilteredPath(rootFolder, path, selectedFile)) {
                changedFiles.add(new UIFile(path, DiffEntry.ChangeType.MODIFY, false));
              }
            }
          }
        } catch (Exception e) {
          LogChannel.UI.logError("Error getting changed file in revision " + revisionId, e);
        }
      }
    }

    wFiles.removeAll();
    for (UIFile file : changedFiles) {
      TableItem item = new TableItem(wFiles.table, SWT.NONE);
      item.setText(1, Const.NVL(file.getName(), ""));
      if (showStaged) {
        item.setText(2, Const.NVL(file.getChangeType().name(), ""));
        item.setText(3, file.getIsStaged() ? "Y" : "N");
      }
    }
    wFiles.optimizeTableView();
  }

  /**
   * See if the given path is the same or in a sub-folder
   *
   * @param root The reference folder for the relative path calculation
   * @param path The short relative path to compare with the selected file
   * @param selectedFile The selected file with a full path
   * @return True if the path is the same or in a sub-folder of the selected file
   */
  private boolean isFilteredPath(String root, String path, String selectedFile) {
    try {
      String relativeSelected = calculateRelativePath(root, selectedFile);
      if (".".equals(relativeSelected)) {
        return true; // path is whole project
      }
      return path.startsWith(relativeSelected);
    } catch (Exception e) {
      return false;
    }
  }

  private String getStatusDescription(GitGuiPlugin guiPlugin) {

    Map<String, UIFile> changedFiles = guiPlugin.getChangedFiles();
    Map<String, String> ignoredFiles = guiPlugin.getIgnoredFiles();

    UIFile file = changedFiles.get(explorerFile.getFilename());
    if (file == null) {
      String ignored = ignoredFiles.get(explorerFile.getFilename());
      if (ignored == null) {
        return "Not changed";
      } else {
        return "Ignored";
      }
    } else {
      switch (file.getChangeType()) {
        case ADD:
          return "Not added";
        case COPY:
          return "Copied";
        case MODIFY:
          return "Modified";
        case DELETE:
          return "Deleted";
        case RENAME:
          return "Renamed";
        default:
          return "Changed";
      }
    }
  }

  private String getDateString(Date date) {
    return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(date);
  }
}
