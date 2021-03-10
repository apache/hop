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

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.git.GitGuiPlugin;
import org.apache.hop.git.model.UIFile;
import org.apache.hop.git.model.UIGit;
import org.apache.hop.git.model.VCS;
import org.apache.hop.git.model.revision.ObjectRevision;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/** Show git information about a file or folder : revisions */
public class GitInfoExplorerFileTypeHandler extends BaseExplorerFileTypeHandler
    implements IExplorerFileTypeHandler {

  private Text wFile;
  private TableView wFiles;
  private TableView wRevisions;
  private Text wDiff;

  public GitInfoExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  // Render the SVG file...
  //
  @Override
  public void renderFile(Composite composite) {
    PropsUi props = PropsUi.getInstance();
    int margin = props.getMargin();

    GitGuiPlugin guiPlugin = GitGuiPlugin.getInstance();
    UIGit git = guiPlugin.getGit();

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
    wFile.setText(Const.NVL(explorerFile.getFilename(), ""));
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
    Text wStatus = new Text(composite, SWT.LEFT | SWT.SINGLE | SWT.BORDER);
    props.setLook(wStatus);
    wStatus.setEditable(false);
    wStatus.setText(getStatusDescription(guiPlugin));
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
    Text wBranch = new Text(composite, SWT.LEFT | SWT.SINGLE | SWT.BORDER);
    props.setLook(wBranch);
    wBranch.setEditable(false);
    wBranch.setText(Const.NVL(git.getBranch(), ""));
    FormData fdBranch = new FormData();
    fdBranch.left = new FormAttachment(wlFile, 2 * margin);
    fdBranch.top = new FormAttachment(wlBranch, 0, SWT.CENTER);
    fdBranch.right = new FormAttachment(100, 0);
    wBranch.setLayoutData(fdBranch);
    lastControl = wBranch;

    // Relative path of the file...
    //
    List<ObjectRevision> revisions = new ArrayList<>();
    try {
      String relativePath =
          calculateRelativePath(perspective.getRootFolder(), explorerFile.getFilename());
      revisions = git.getRevisions(relativePath);
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error getting git object revisions for path: " + explorerFile.getFilename(), e);
    }

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
        new TableView(
            hopGui.getVariables(),
            composite,
            SWT.NONE,
            revisionColumns,
            revisions.size(),
            null,
            props);
    wRevisions.setReadonly(true);
    props.setLook(wRevisions);
    FormData fdRevisions = new FormData();
    fdRevisions.left = new FormAttachment(0, margin);
    fdRevisions.top = new FormAttachment(lastControl, margin);
    fdRevisions.right = new FormAttachment(100, 0);
    fdRevisions.bottom = new FormAttachment(40, 0);
    wRevisions.setLayoutData(fdRevisions);

    for (int i = 0; i < revisions.size(); i++) {
      ObjectRevision revision = revisions.get(i);
      TableItem item = wRevisions.table.getItem(i);
      item.setText(1, Const.NVL(revision.getRevisionId(), ""));
      item.setText(2, getDateString(revision.getCreationDate()));
      item.setText(3, Const.NVL(revision.getLogin(), ""));
      item.setText(4, Const.NVL(revision.getComment(), ""));
    }
    wRevisions.optimizeTableView();
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

    wFiles.table.addListener(SWT.Selection, e -> showFileDiff());

    wDiff = new Text(sashForm, SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    props.setLook(wDiff);

    sashForm.setWeights(new int[] {50, 50});

    refreshChangedFiles();
  }

  private String calculateRelativePath(String rootFolder, String filename)
      throws HopFileException, FileSystemException {
    FileObject root = HopVfs.getFileObject(rootFolder);
    FileObject file = HopVfs.getFileObject(filename);

    String relativePath = root.getName().getRelativeName(file.getName());
    return relativePath;
  }

  private void showFileDiff() {
    GitGuiPlugin guiPlugin = GitGuiPlugin.getInstance();
    UIGit git = guiPlugin.getGit();

    if (wRevisions.getSelectionIndices().length == 0) {
      return;
    }
    if (wFiles.getSelectionIndices().length == 0) {
      return;
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
        changedFiles = new ArrayList<>(guiPlugin.getChangedFiles().values());
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
    for (int i = 0; i < changedFiles.size(); i++) {
      UIFile file = changedFiles.get(i);
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
   * See if the given path is the same or
   *
   * @param path
   * @param selectedFile
   * @return
   */
  private boolean isFilteredPath(String root, String path, String selectedFile) {
    try {
      String relativeSelected = calculateRelativePath(root, selectedFile);
      if (".".equals( relativeSelected )) {
        return true; // path is whole project
      }
      return path.startsWith( relativeSelected );
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
