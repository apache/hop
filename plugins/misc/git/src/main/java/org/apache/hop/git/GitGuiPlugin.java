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

package org.apache.hop.git;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.callback.GuiCallback;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.git.info.GitInfoExplorerFileType;
import org.apache.hop.git.info.GitInfoExplorerFileTypeHandler;
import org.apache.hop.git.model.UIFile;
import org.apache.hop.git.model.UIGit;
import org.apache.hop.git.model.VCS;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.IExplorerFilePaintListener;
import org.apache.hop.ui.hopgui.perspective.explorer.IExplorerRefreshListener;
import org.apache.hop.ui.hopgui.perspective.explorer.IExplorerRootChangedListener;
import org.apache.hop.ui.hopgui.perspective.explorer.IExplorerSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@GuiPlugin
public class GitGuiPlugin
    implements IExplorerRootChangedListener,
        IExplorerFilePaintListener,
        IExplorerRefreshListener,
        IExplorerSelectionListener {

  public static final String TOOLBAR_ITEM_GIT_INFO = "ExplorerPerspective-Toolbar-20000-GitInfo";
  public static final String TOOLBAR_ITEM_ADD = "ExplorerPerspective-Toolbar-20100-Add";
  public static final String TOOLBAR_ITEM_REVERT = "ExplorerPerspective-Toolbar-20200-Revert";
  public static final String TOOLBAR_ITEM_COMMIT = "ExplorerPerspective-Toolbar-21000-Commit";
  public static final String TOOLBAR_ITEM_PUSH = "ExplorerPerspective-Toolbar-21100-Push";
  public static final String TOOLBAR_ITEM_PULL = "ExplorerPerspective-Toolbar-21200-Pull";

  private static GitGuiPlugin instance;

  private UIGit git;
  private Map<String, UIFile> changedFiles;
  private Map<String, String> ignoredFiles;

  private Color colorIgnored;
  private Color colorStaged;
  private Color colorUnstaged;

  public static GitGuiPlugin getInstance() {
    if (instance == null) {
      instance = new GitGuiPlugin();
    }
    return instance;
  }

  public GitGuiPlugin() {
    if (instance != null) {
      git = instance.git;
    } else {
      instance = this;
      git = null;
    }

    colorIgnored = new Color(HopGui.getInstance().getDisplay(), 125, 125, 125);
    colorStaged = GuiResource.getInstance().getColorBlue();
    colorUnstaged = GuiResource.getInstance().getColorRed();

    refreshChangedFiles();
  }

  /**
   * We want to change the "git project" whenever the root folder changes in the file perspective
   */
  @GuiCallback(callbackId = ExplorerPerspective.GUI_TOOLBAR_CREATED_CALLBACK_ID)
  public void addRootChangedListener() {
    git = null;
    ExplorerPerspective explorerPerspective = ExplorerPerspective.getInstance();

    // Listener to what's going on in the explorer perspective...
    //
    explorerPerspective.getRootChangedListeners().add(this);
    explorerPerspective.getFilePaintListeners().add(this);
    explorerPerspective.getRefreshListeners().add(this);
    explorerPerspective.getSelectionListeners().add(this);
  }

  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_COMMIT,
      toolTip = "Git commit",
      image = "git-commit.svg")
  public void gitCommit() {

    try {
      // Ask the user to select the list of changed files in the commit...
      //
      ExplorerFile explorerFile = getSelectedFile();
      if (git == null || explorerFile == null) {
        return;
      }
      String relativePath = calculateRelativePath(git.getDirectory(), explorerFile);
      if (relativePath == null) {
        return;
      }
      List<String> changedFiles = git.getRevertPathFiles(relativePath);
      if (changedFiles.isEmpty()) {
        MessageBox box =
            new MessageBox(HopGui.getInstance().getShell(), SWT.OK | SWT.ICON_INFORMATION);
        box.setText("No files to commit");
        box.setMessage("Sorry, there are no files to commit");
        box.open();
      } else {
        String[] files = changedFiles.toArray(new String[0]);
        int[] selectedIndexes = new int[files.length];
        for (int i = 0; i < files.length; i++) {
          selectedIndexes[i] = i;
        }
        EnterSelectionDialog selectionDialog =
            new EnterSelectionDialog(
                HopGui.getInstance().getShell(),
                files,
                "Select files to commit",
                "Please select the files to commit. They'll be staged (add) for the commit to git:");
        selectionDialog.setMulti(true);
        // Select all files by default
        //
        selectionDialog.setSelectedNrs(selectedIndexes);
        String selection = selectionDialog.open();
        if (selection != null) {

          EnterStringDialog enterStringDialog =
              new EnterStringDialog(
                  HopGui.getInstance().getShell(),
                  "Commit message",
                  "Please enter a commit message:",
                  "");
          String message = enterStringDialog.open();
          if (message != null) {

            // Now stage/add the selected files and commit...
            //
            int[] selectedNrs = selectionDialog.getSelectionIndeces();
            for (int selectedNr : selectedNrs) {
              // If the file is gone, git.rm(), otherwise add()
              //
              String file = files[selectedNr];
              if (fileExists(file)) {
                git.add(file);
              } else {
                git.rm(file);
              }
            }

            // Standard author by default
            //
            String authorName = git.getAuthorName(VCS.WORKINGTREE);

            // Commit...
            //
            git.commit(authorName, message);
          }
        }
      }

      // Refresh the tree, change colors...
      //
      ExplorerPerspective.getInstance().refresh();
      enableButtons();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          "Commit error",
          "There was an error doing a git commit",
          e);
    }
  }

  private boolean fileExists(String file) throws HopFileException, FileSystemException {
    String filename = git.getDirectory() + File.separator + file;
    return HopVfs.getFileObject(filename).exists();
  }

  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PUSH,
      toolTip = "Git push",
      image = "push.svg")
  public void gitPush() {
    try {
      git.push();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), "Push error", "There was an error doing a git push", e);
    }
  }

  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PULL,
      toolTip = "Git pull",
      image = "pull.svg")
  public void gitPull() {
    try {
      git.pull();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), "Push error", "There was an error doing a git pull", e);
    }
  }

  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ADD,
      toolTip = "Git add",
      image = "git-add.svg")
  public void gitAdd() {
    try {
      ExplorerFile explorerFile = getSelectedFile();
      if (git == null || explorerFile == null) {
        return;
      }
      String relativePath = calculateRelativePath(git.getDirectory(), explorerFile);
      if (relativePath == null) {
        return;
      }
      git.add(relativePath);

      // Refresh the tree, change colors...
      //
      ExplorerPerspective.getInstance().refresh();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), "Add error", "There was an error doing a git add", e);
    }
  }

  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REVERT,
      toolTip = "Git revert file to the last committed version (HEAD)",
      image = "git-revert.svg")
  public void gitRevert() {
    try {
      ExplorerFile explorerFile = getSelectedFile();
      if (git == null || explorerFile == null) {
        return;
      }
      String relativePath = calculateRelativePath(git.getDirectory(), explorerFile);
      if (relativePath == null) {
        return;
      }
      List<String> revertPathFiles = git.getRevertPathFiles(relativePath);
      if (revertPathFiles.isEmpty()) {
        MessageBox box =
            new MessageBox(HopGui.getInstance().getShell(), SWT.OK | SWT.ICON_INFORMATION);
        box.setText("No files to revert");
        box.setMessage("Sorry, there are no files to revert");
        box.open();
      } else {
        String[] files = revertPathFiles.toArray(new String[0]);
        int[] selectedIndexes = new int[files.length];
        for (int i = 0; i < files.length; i++) {
          selectedIndexes[i] = i;
        }
        EnterSelectionDialog selectionDialog =
            new EnterSelectionDialog(
                HopGui.getInstance().getShell(),
                files,
                "Select files to revert",
                "Please select the files to revert");
        selectionDialog.setMulti(true);
        // Select all files by default
        //
        selectionDialog.setSelectedNrs(selectedIndexes);
        String selection = selectionDialog.open();
        if (selection != null) {
          int[] selectedNrs = selectionDialog.getSelectionIndeces();
          for (int selectedNr : selectedNrs) {
            String file = files[selectedNr];
            git.revertPath(file);
          }
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          "Revert error",
          "There was an error doing a git revert",
          e);
    }

    // Refresh the tree, change colors...
    //
    ExplorerPerspective.getInstance().refresh();
    enableButtons();
  }

  private String calculateRelativePath(String directory, ExplorerFile explorerFile) {

    try {
      FileObject file = HopVfs.getFileObject(explorerFile.getFilename());
      FileObject root = HopVfs.getFileObject(directory);
      return root.getName().getRelativeName(file.getName());
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error calculating relative path for filename '"
              + explorerFile.getFilename()
              + "' against '"
              + directory
              + "'",
          e);
      return null;
    }
  }

  private ExplorerFile getSelectedFile() {
    ExplorerPerspective explorerPerspective = ExplorerPerspective.getInstance();
    return explorerPerspective.getSelectedFile();
  }

  @Override
  public void rootChanged(String rootFolder, String rootName) {
    // OK, let's see if we can determine the current git instance...
    //
    try {
      FileObject gitConfig = HopVfs.getFileObject(rootFolder + "/.git/config");
      if (gitConfig.exists()) {
        git = new UIGit();
        git.openRepo(rootFolder);
        LogChannel.UI.logBasic("Found git project for: " + rootFolder);
      }
    } catch (Exception e) {
      // This is not a git project...
      git = null;
      LogChannel.UI.logBasic("No git project found in " + rootFolder);
    }
    refreshChangedFiles();
    enableButtons();
  }

  private String getAbsoluteFilename(String root, String relativePath) {
    String path = root + "/" + relativePath;
    try {
      // Get absolute filename
      //
      path = HopVfs.getFileObject(path).getName().getPath();
    } catch (Exception e) {
      // Ignore, keep simple path
    }
    return path;
  }

  private void refreshChangedFiles() {
    changedFiles = new HashMap<>();
    ignoredFiles = new HashMap<>();

    if (git != null) {
      // List the staged and unstaged files...
      //
      List<UIFile> files = new ArrayList<>(git.getStagedFiles());
      files.addAll(git.getUnstagedFiles());

      for (UIFile file : files) {
        String path = getAbsoluteFilename(git.getDirectory(), file.getName());
        changedFiles.put(path, file);
      }

      Set<String> ignored = git.getIgnored(null);
      for (String ignore : ignored) {
        String filename = getAbsoluteFilename(git.getDirectory(), ignore);
        ignoredFiles.put(filename, ignore);
      }
    }
  }

  @Override
  public void beforeRefresh() {
    refreshChangedFiles();
  }

  @Override
  public void fileSelected() {
    enableButtons();
  }

  private void enableButtons() {
    GuiToolbarWidgets widgets = ExplorerPerspective.getInstance().getToolBarWidgets();
    boolean isGit = git != null;
    boolean isSelected = isGit && getSelectedFile() != null;

    widgets.enableToolbarItem(TOOLBAR_ITEM_GIT_INFO, isGit);
    widgets.enableToolbarItem(TOOLBAR_ITEM_ADD, isSelected);
    widgets.enableToolbarItem(TOOLBAR_ITEM_REVERT, isSelected);
    widgets.enableToolbarItem(TOOLBAR_ITEM_COMMIT, isSelected);

    widgets.enableToolbarItem(TOOLBAR_ITEM_PUSH, isGit);
    widgets.enableToolbarItem(TOOLBAR_ITEM_PULL, isGit);

  }

  /**
   * If we have a git project we can take a look and see if a file is changed.
   *
   * @param tree
   * @param treeItem
   * @param path
   * @param name
   */
  @Override
  public void filePainted(Tree tree, TreeItem treeItem, String path, String name) {

    GuiResource guiResource = GuiResource.getInstance();

    // Changed git file colored blue
    //
    UIFile file = changedFiles.get(path);
    if (file != null) {
      switch (file.getChangeType()) {
        case DELETE:
        case MODIFY:
        case RENAME:
        case COPY:
          treeItem.setForeground(colorStaged);
          break;
        case ADD:
          if (file.getIsStaged()) {
            treeItem.setForeground(colorStaged);
          } else {
            treeItem.setForeground(colorUnstaged);
          }
          break;
      }
    }
    String ignored = ignoredFiles.get(path);
    if (ignored != null) {
      treeItem.setForeground(colorIgnored);
    }
  }

  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_GIT_INFO,
      toolTip = "Git Info",
      image = "git-info.svg",
      separator = true)
  public void showGitInfo() {

    if (git == null) {
      return;
    }

    // Open a new tab showing information about the selected file...
    //
    ExplorerPerspective explorerPerspective = ExplorerPerspective.getInstance();
    if (explorerPerspective == null) {
      return;
    }
    ExplorerFile activeFile = explorerPerspective.getSelectedFile();
    if (activeFile == null) {
      activeFile = new ExplorerFile();
      activeFile.setName( "Git project" );
      activeFile.setFilename( git.getDirectory() );
    }
    activeFile.setName("Git info: " + activeFile.getName());
    GitInfoExplorerFileType fileType = new GitInfoExplorerFileType();
    activeFile.setFileType(fileType);
    GitInfoExplorerFileTypeHandler fileTypeHandler =
        fileType.createFileTypeHandler(HopGui.getInstance(), explorerPerspective, activeFile);
    activeFile.setFileTypeHandler(fileTypeHandler);

    explorerPerspective.addFile(activeFile, fileTypeHandler);
  }

  public UIGit getGit() {
    return git;
  }

  /**
   * Gets changedFiles
   *
   * @return value of changedFiles
   */
  public Map<String, UIFile> getChangedFiles() {
    return changedFiles;
  }

  /**
   * Gets ignoredFiles
   *
   * @return value of ignoredFiles
   */
  public Map<String, String> getIgnoredFiles() {
    return ignoredFiles;
  }
}
