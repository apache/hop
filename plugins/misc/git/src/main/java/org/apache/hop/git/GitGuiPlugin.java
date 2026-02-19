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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.callback.GuiCallback;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.git.config.GitConfig;
import org.apache.hop.git.config.GitConfigSingleton;
import org.apache.hop.git.info.GitInfoExplorerFileType;
import org.apache.hop.git.info.GitInfoExplorerFileTypeHandler;
import org.apache.hop.git.model.UIFile;
import org.apache.hop.git.model.UIGit;
import org.apache.hop.git.model.VCS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.IExplorerFilePaintListener;
import org.apache.hop.ui.hopgui.perspective.explorer.IExplorerRefreshListener;
import org.apache.hop.ui.hopgui.perspective.explorer.IExplorerRootChangedListener;
import org.apache.hop.ui.hopgui.perspective.explorer.IExplorerSelectionListener;
import org.eclipse.jgit.merge.MergeStrategy;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

@GuiPlugin
public class GitGuiPlugin
    implements IExplorerRootChangedListener,
        IExplorerFilePaintListener,
        IExplorerRefreshListener,
        IExplorerSelectionListener {

  public static final Class<?> PKG = GitGuiPlugin.class;

  public static final String ID_TOOLBAR_ITEM_GIT = "toolbar-item-30000-git";
  public static final String ID_CONTEXT_MENU_GIT = "context-menu-git";

  public static final String CONTEXT_MENU_GIT_CREATE_BRANCH = "context-menu-git-20010-CreateBranch";
  public static final String CONTEXT_MENU_GIT_MERGE_BRANCH = "context-menu-git-20020-MergeBranch";
  public static final String CONTEXT_MENU_GIT_RENAME_BRANCH = "context-menu-git-20020-RenameBranch";
  public static final String CONTEXT_MENU_GIT_DELETE_BRANCH = "context-menu-git-20030-DeleteBranch";
  public static final String CONTEXT_MENU_GIT_PUSH = "context-menu-git-30000-Push";
  public static final String CONTEXT_MENU_GIT_PULL = "context-menu-git-30010-Pull";

  public static final String TOOLBAR_ITEM_GIT_INFO = "ExplorerPerspective-Toolbar-20100-GitInfo";
  public static final String TOOLBAR_ITEM_ADD = "ExplorerPerspective-Toolbar-20200-Add";
  public static final String TOOLBAR_ITEM_REVERT = "ExplorerPerspective-Toolbar-20300-Revert";
  public static final String TOOLBAR_ITEM_COMMIT = "ExplorerPerspective-Toolbar-21000-Commit";
  public static final String TOOLBAR_ITEM_PUSH = "ExplorerPerspective-Toolbar-21100-Push";
  public static final String TOOLBAR_ITEM_PULL = "ExplorerPerspective-Toolbar-21200-Pull";

  public static final String CONTEXT_MENU_GIT_INFO =
      "ExplorerPerspective-ContextMenu-20000-GitInfo";
  public static final String CONTEXT_MENU_GIT_ADD = "ExplorerPerspective-ContextMenu-20100-GitAdd";
  public static final String CONTEXT_MENU_GIT_REVERT =
      "ExplorerPerspective-ContextMenu-20200-GitRevert";
  public static final String CONTEXT_MENU_GIT_COMMIT =
      "ExplorerPerspective-ContextMenu-21000-GitCommit";
  public static final String TOOLBAR_ITEM_BRANCH = "ExplorerPerspective-Toolbar-22000-GitBranch";

  private static GitGuiPlugin instance;

  private static UIGit git;

  @Getter private Map<String, UIFile> changedFiles;

  @Getter private Map<String, String> ignoredFiles;

  private final Color colorIgnored;
  private final Color colorStagedUnchanged;
  private final Color colorStagedAdd;
  private final Color colorStagedModify;
  private final Color colorUnstaged;

  /** Muted variants when the explorer has already grayed the item (non-openable file). */
  private final Color colorStagedAddGray;

  private final Color colorStagedModifyGray;
  private final Color colorUnstagedGray;
  private final Color colorIgnoredGray;
  private final Color colorStagedUnchangedGray;

  public static GitGuiPlugin getInstance() {
    if (instance == null) {
      instance = new GitGuiPlugin();
    }
    return instance;
  }

  public GitGuiPlugin() {

    // Adjust color for light/dark mode
    if (PropsUi.getInstance().isDarkMode()) {
      colorStagedModify = GuiResource.getInstance().getColorLightBlue();
      colorIgnored = GuiResource.getInstance().getColorGray();
      colorUnstaged = GuiResource.getInstance().getColor(217, 105, 73);
    } else {
      colorStagedModify = GuiResource.getInstance().getColorBlue();
      colorIgnored = GuiResource.getInstance().getColorDarkGray();
      colorUnstaged = GuiResource.getInstance().getColor(225, 30, 70);
    }
    colorStagedUnchanged = GuiResource.getInstance().getColorBlack();
    colorStagedAdd = GuiResource.getInstance().getColorDarkGreen();

    colorStagedAddGray = GuiResource.getInstance().getColorDarkGreenMuted();
    colorStagedModifyGray = GuiResource.getInstance().getColorLightBlueMuted();
    colorUnstagedGray = GuiResource.getInstance().getColorRedMuted();
    colorIgnoredGray = GuiResource.getInstance().getColorDarkGrayMuted();
    colorStagedUnchangedGray = GuiResource.getInstance().getColorBlackMuted();

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

    enableButtons();
  }

  @GuiMenuElement(
      root = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_GIT_COMMIT,
      label = "i18n::GitGuiPlugin.Menu.Commit.Text",
      image = "git-commit.svg")
  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_COMMIT,
      toolTip = "i18n::GitGuiPlugin.Toolbar.Commit.Tooltip",
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
      List<String> changedFilesToCommit = git.getRevertPathFiles(relativePath);
      if (changedFilesToCommit.isEmpty()) {
        MessageBox box =
            new MessageBox(HopGui.getInstance().getShell(), SWT.OK | SWT.ICON_INFORMATION);
        box.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.NoFilesToCommit.Header"));
        box.setMessage(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.NoFilesToCommit.Message"));
        box.open();
      } else {
        String[] files = changedFilesToCommit.toArray(new String[0]);
        int[] selectedIndexes = new int[files.length];
        for (int i = 0; i < files.length; i++) {
          selectedIndexes[i] = i;
        }
        EnterSelectionDialog selectionDialog =
            new EnterSelectionDialog(
                HopGui.getInstance().getShell(),
                files,
                BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.StageFiles.Header"),
                BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.StageFiles.Message"));
        selectionDialog.setMulti(true);
        // Select all files by default
        //
        selectionDialog.setSelectedNrs(selectedIndexes);
        String selection = selectionDialog.open();
        if (selection != null) {

          EnterStringDialog enterStringDialog =
              new EnterStringDialog(
                  HopGui.getInstance().getShell(),
                  "",
                  BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.SelectFilesToCommit.Header"),
                  BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.SelectFilesToCommit.Message"));
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
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CommitError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CommitError.Message"),
          e);
    }
  }

  private boolean fileExists(String file) throws HopFileException, FileSystemException {
    String filename = git.getDirectory() + File.separator + file;
    return HopVfs.getFileObject(filename).exists();
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_GIT,
      parentId = ID_CONTEXT_MENU_GIT,
      id = CONTEXT_MENU_GIT_PUSH,
      label = "i18n::GitGuiPlugin.Menu.Branch.Push.Text",
      image = "push.svg",
      separator = true)
  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PUSH,
      toolTip = "i18n::GitGuiPlugin.Toolbar.Push.Tooltip",
      image = "push.svg")
  public void gitPush() {
    try {
      git.push();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PushError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PushError.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_GIT,
      parentId = ID_CONTEXT_MENU_GIT,
      id = CONTEXT_MENU_GIT_PULL,
      label = "i18n::GitGuiPlugin.Menu.Branch.Pull.Text",
      image = "pull.svg")
  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PULL,
      toolTip = "i18n::GitGuiPlugin.Toolbar.Pull.Tooltip",
      image = "pull.svg")
  public void gitPull() {
    try {
      if (git.pull()) {
        ExplorerPerspective.getInstance().refresh();
        MessageBox pullSuccessful =
            new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_INFORMATION);
        pullSuccessful.setText(
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PullSuccessful.Header"));
        pullSuccessful.setMessage(
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PullSuccessful.Message"));
        pullSuccessful.open();
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PullError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PullError.Message"),
          e);
    }
  }

  @GuiToolbarElement(
      root = HopGui.ID_STATUS_TOOLBAR,
      id = ID_TOOLBAR_ITEM_GIT,
      type = GuiToolbarElementType.BUTTON,
      image = "branch.svg",
      toolTip = "i18n::GitGuiPlugin.Toolbar.Branch.Tooltip",
      separator = true)
  public void showGitContextMenu() {
    GuiToolbarWidgets statusWidgets = HopGui.getInstance().getStatusToolbarWidgets();
    Control control = statusWidgets.getWidgetsMap().get(ID_TOOLBAR_ITEM_GIT);
    if (control != null && !control.isDisposed()) {
      Rectangle rect = control.getBounds();
      Point pt = control.getParent().toDisplay(new Point(rect.x, rect.y + rect.height));
      Menu menu = createGitContextMenu();
      menu.setLocation(pt);
      menu.setVisible(true);
    }
  }

  private Menu createGitContextMenu() {
    Shell shell = HopGui.getInstance().getActiveShell();
    Menu menu = new Menu(shell, SWT.POP_UP);

    if (git == null) {
      return menu;
    }

    try {
      // Create context menu...
      //
      GuiMenuWidgets menuWidgets = new GuiMenuWidgets();
      menuWidgets.registerGuiPluginObject(this);
      menuWidgets.createMenuWidgets(ID_CONTEXT_MENU_GIT, shell, menu);

      new MenuItem(menu, SWT.SEPARATOR);

      // Add all known branches to the list
      // int count = 0;
      String currentBranch = git.getBranch();
      for (String name : git.getBranches()) {
        MenuItem item = new MenuItem(menu, SWT.NONE);
        item.setText(name);
        // If the item is the active branch, mark it as checked
        if (currentBranch.equals(name)) {
          item.setImage(GuiResource.getInstance().getImageCheck());
        }
        // Change Branch when selecting one of the branch options
        item.addListener(SWT.Selection, e -> gitCheckoutBranch(name));
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.Message"),
          e);
    }

    return menu;
  }

  @GuiMenuElement(
      root = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_GIT_ADD,
      label = "i18n::GitGuiPlugin.Menu.Add.Text",
      image = "git-add.svg")
  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ADD,
      toolTip = "i18n::GitGuiPlugin.Toolbar.Add.Tooltip",
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
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.AddError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.AddError.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_GIT_REVERT,
      label = "i18n::GitGuiPlugin.Menu.Revert.Text",
      image = "git-revert.svg")
  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REVERT,
      toolTip = "i18n::GitGuiPlugin.Toolbar.Revert.Tooltip",
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
        box.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.NoFilesToRevert.Header"));
        box.setMessage(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.NoFilesToRevert.Message"));
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
                BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.RevertFiles.Header"),
                BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.RevertFiles.Message"));
        selectionDialog.setMulti(true);
        // Select all files by default
        //
        selectionDialog.setSelectedNrs(selectedIndexes);
        String selection = selectionDialog.open();
        if (selection != null) {
          int[] selectedNrs = selectionDialog.getSelectionIndeces();

          // Only close tabs for files that will be deleted (untracked/added); reload the rest
          Set<String> pathsThatWillBeDeleted =
              git.getRevertPathFilesThatWillBeDeleted(relativePath);
          List<String> filenamesToClose = new ArrayList<>();
          List<String> filenamesToReload = new ArrayList<>();
          for (int selectedNr : selectedNrs) {
            String filePath = files[selectedNr];
            String fullFilename;
            try {
              FileObject fileObj =
                  HopVfs.getFileObject(new File(git.getDirectory(), filePath).getAbsolutePath());
              fullFilename =
                  fileObj.exists()
                      ? HopVfs.getFilename(fileObj)
                      : new File(git.getDirectory(), filePath).getAbsolutePath();
            } catch (Exception ignored) {
              fullFilename = new File(git.getDirectory(), filePath).getAbsolutePath();
            }
            if (pathsThatWillBeDeleted.contains(filePath)) {
              filenamesToClose.add(fullFilename);
            } else {
              filenamesToReload.add(fullFilename);
            }
          }

          for (int selectedNr : selectedNrs) {
            String file = files[selectedNr];
            git.revertPath(file);
          }

          // Close tabs for reverted files that were deleted (untracked/added)
          ExplorerPerspective.getInstance().closeTabsForFilenames(filenamesToClose);
          // Reload tabs for reverted files that still exist (changed/missing/uncommitted)
          ExplorerPerspective.getInstance().reloadTabsForFilenames(filenamesToReload);

          // When a folder was selected, ask if user wants to run git clean (yes/no)
          boolean isFolder = false;
          try {
            isFolder = HopVfs.getFileObject(explorerFile.getFilename()).isFolder();
          } catch (Exception ignored) {
            // not a folder
          }
          if (isFolder) {
            MessageBox cleanBox =
                new MessageBox(
                    HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
            cleanBox.setText(
                BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CleanConfirm.Header"));
            cleanBox.setMessage(
                BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CleanConfirm.Message"));
            if ((cleanBox.open() & SWT.YES) != 0) {
              Set<String> foldersToClean = new HashSet<>();
              foldersToClean.add(relativePath);
              for (int selectedNr : selectedNrs) {
                String filePath = files[selectedNr];
                int lastSlash = Math.max(filePath.lastIndexOf('/'), filePath.lastIndexOf('\\'));
                String parentFolder = (lastSlash <= 0) ? "" : filePath.substring(0, lastSlash);
                foldersToClean.add(parentFolder);
              }
              for (String folder : foldersToClean) {
                try {
                  git.cleanPath(folder);
                } catch (Exception cleanEx) {
                  LogChannel.UI.logError("Git clean failed for " + folder, cleanEx);
                }
              }
            }
          }

          // Show confirmation message once after all files have been reverted
          MessageBox box =
              new MessageBox(HopGui.getInstance().getShell(), SWT.OK | SWT.ICON_INFORMATION);
          box.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.FilesReverted.Header"));
          box.setMessage(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.FilesReverted.Message"));
          box.open();
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.RevertError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.RevertError.Message"),
          e);
    }

    // Refresh the tree, change colors...
    //
    ExplorerPerspective.getInstance().refresh();
    enableButtons();
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_GIT,
      parentId = ID_CONTEXT_MENU_GIT,
      id = CONTEXT_MENU_GIT_CREATE_BRANCH,
      label = "i18n::GitGuiPlugin.Menu.Branch.Create.Text",
      image = "ui/images/add.svg")
  public void gitCreateBranch() {
    EnterStringDialog enterStringDialog =
        new EnterStringDialog(
            HopGui.getInstance().getShell(),
            "",
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.CreateBranch.Header"),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.CreateBranch.Message"));
    String message = enterStringDialog.open();
    if (message != null) {
      boolean branchCreated = git.createBranch(message);
      if (branchCreated) {
        MessageBox pullSuccessful =
            new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_INFORMATION);
        pullSuccessful.setText(
            BaseMessages.getString(
                PKG, "GitGuiPlugin.Dialog.Branch.CreateBranchSuccessFul.Header"));
        pullSuccessful.setMessage(
            BaseMessages.getString(
                PKG, "GitGuiPlugin.Dialog.Branch.CreateBranchSuccessFul.Message"));
        pullSuccessful.open();
      }
      // Refresh the tree, change colors...
      //
      ExplorerPerspective.getInstance().refresh();
    }
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_GIT,
      parentId = ID_CONTEXT_MENU_GIT,
      id = CONTEXT_MENU_GIT_RENAME_BRANCH,
      label = "i18n::GitGuiPlugin.Menu.Branch.Rename.Text",
      image = "ui/images/rename.svg")
  public void gitRenameBranch() {
    String oldName =
        HopGui.getInstance().getStatusToolbarWidgets().getToolbarItemText(ID_TOOLBAR_ITEM_GIT);
    EnterStringDialog enterStringDialog =
        new EnterStringDialog(
            HopGui.getInstance().getShell(),
            oldName,
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.RenameBranch.Header"),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.RenameBranch.Message"));
    String newName = enterStringDialog.open();
    if (!Utils.isEmpty(newName) && !newName.equals(oldName)) {
      boolean renamed = git.renameBranch(oldName, newName);
      if (renamed) {
        this.setBranchLabel(newName);
      }
    }
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_GIT,
      parentId = ID_CONTEXT_MENU_GIT,
      id = CONTEXT_MENU_GIT_MERGE_BRANCH,
      label = "i18n::GitGuiPlugin.Menu.Branch.Merge.Text",
      image = "merge.svg")
  public void gitMergeBranch() {
    List<String> branches = git.getBranches();
    EnterSelectionDialog selectionDialog =
        new EnterSelectionDialog(
            HopGui.getInstance().getShell(),
            branches.toArray(new String[0]),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.MergeBranch.Header"),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.MergeBranch.Message"));
    String branchToMerge = selectionDialog.open();
    if (branchToMerge != null) {
      try {
        boolean branchMerged = git.mergeBranch(branchToMerge, MergeStrategy.RECURSIVE);
        if (branchMerged) {
          MessageBox mergeSuccessful =
              new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_INFORMATION);
          mergeSuccessful.setText(
              BaseMessages.getString(
                  PKG, "GitGuiPlugin.Dialog.Branch.MergeBranchSuccessFul.Header"));
          mergeSuccessful.setMessage(
              BaseMessages.getString(
                  PKG, "GitGuiPlugin.Dialog.Branch.MergeBranchSuccessFul.Message"));
          mergeSuccessful.open();
        }
      } catch (Exception ex) {
        new ErrorDialog(
            HopGui.getInstance().getShell(),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.MergeBranchError.Header"),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.MergeBranchError.Message"),
            ex);
      }
      // Refresh the tree, change colors...
      //
      ExplorerPerspective.getInstance().refresh();
    }
  }

  private void gitCheckoutBranch(String name) {
    git.checkout(name);

    // Refresh the tree, change colors...
    //
    ExplorerPerspective.getInstance().refresh();
  }

  @GuiMenuElement(
      root = ID_CONTEXT_MENU_GIT,
      parentId = ID_CONTEXT_MENU_GIT,
      id = CONTEXT_MENU_GIT_DELETE_BRANCH,
      label = "i18n::GitGuiPlugin.Menu.Branch.Delete.Text",
      image = "ui/images/delete.svg")
  public void gitDeleteBranch() {
    List<String> branches = git.getBranches();
    EnterSelectionDialog selectionDialog =
        new EnterSelectionDialog(
            HopGui.getInstance().getShell(),
            branches.toArray(new String[0]),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.DeleteBranch.Header"),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.DeleteBranch.Message"));
    String branchToDelete = selectionDialog.open();
    if (branchToDelete != null) {
      boolean branchDeleted = git.deleteBranch(branchToDelete, true);
      if (branchDeleted) {
        MessageBox pullSuccessful =
            new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_INFORMATION);
        pullSuccessful.setText(
            BaseMessages.getString(
                PKG, "GitGuiPlugin.Dialog.Branch.DeleteBranchSuccessFul.Header"));
        pullSuccessful.setMessage(
            BaseMessages.getString(
                PKG, "GitGuiPlugin.Dialog.Branch.DeleteBranchSuccessFul.Message"));
        pullSuccessful.open();
      }
      // Refresh the tree, change colors...
      //
      ExplorerPerspective.getInstance().refresh();
    }
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
    GitConfig config = GitConfigSingleton.getConfig();

    git = null;
    setBranchLabel(null);

    if (config.isEnabled()) {
      try (FileObject folder = findGitConfig(rootFolder, config.isSearchingParentFolders())) {
        if (folder != null) {
          git = new UIGit();
          git.openRepo(HopVfs.getFilename(folder));
          setBranchLabel(git.getBranch());
        }
      } catch (Exception e) {
        // This is not a git project...
        git = null;
        LogChannel.UI.logBasic("No git project found in " + rootFolder);
      }
    }
    refreshChangedFiles();
    enableButtons();
  }

  private FileObject findGitConfig(String rootFolderName, boolean searchParentFolders)
      throws HopFileException, FileSystemException {
    FileObject folder = HopVfs.getFileObject(rootFolderName);
    FileObject fileObject = folder.resolveFile(".git/config");
    while (searchParentFolders && !fileObject.exists() && folder.getParent() != null) {
      folder = folder.getParent();
      fileObject = folder.resolveFile(".git/config");
    }
    if (fileObject.exists()) {
      return folder;
    } else {
      return null;
    }
  }

  /**
   * Normalize absolute filename.
   *
   * @param path the path to normalize
   * @return normalized path
   */
  private String getAbsoluteFilename(String path) {
    try {
      path = HopVfs.getFileObject(path).getName().getPath();
    } catch (Exception e) {
      // Ignore, keep simple path
    }
    return path;
  }

  /**
   * Normalize absolute filename
   *
   * @param root The root path
   * @param relativePath The relative path
   * @return The absolute filename
   */
  private String getAbsoluteFilename(String root, String relativePath) {
    String path = root + File.separator + relativePath;
    try {
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
      for (String file : ignored) {
        String path = getAbsoluteFilename(git.getDirectory(), file);
        ignoredFiles.put(path, file);
      }
      setBranchLabel(git.getBranch());
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

    boolean isGit = git != null;
    boolean isSelected = isGit && getSelectedFile() != null;

    GuiToolbarWidgets toolBarWidgets = ExplorerPerspective.getInstance().getToolBarWidgets();
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_GIT_INFO, isGit);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_ADD, isSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_REVERT, isSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_COMMIT, isSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_PUSH, isGit);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_PULL, isGit);

    GuiMenuWidgets menuWidgets = ExplorerPerspective.getInstance().getMenuWidgets();
    menuWidgets.enableMenuItem(CONTEXT_MENU_GIT_INFO, isGit);
    menuWidgets.enableMenuItem(CONTEXT_MENU_GIT_ADD, isSelected);
    menuWidgets.enableMenuItem(CONTEXT_MENU_GIT_COMMIT, isSelected);
    menuWidgets.enableMenuItem(CONTEXT_MENU_GIT_REVERT, isSelected);

    HopGui.getInstance().getStatusToolbarWidgets().enableToolbarItem(ID_TOOLBAR_ITEM_GIT, isGit);
  }

  /**
   * If we have a git project we can take a look and see if a file is changed.
   *
   * @param tree The tree to use
   * @param treeItem The tree item to paint
   * @param path The file path
   * @param name The name of the file
   */
  @Override
  public void filePainted(Tree tree, TreeItem treeItem, String path, String name) {
    // Normalize path
    String absolutePath = getAbsoluteFilename(path);

    // Use gray variants when the explorer has already grayed this item (non-openable file)
    Color systemDarkGray = tree.getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY);
    Color currentFg = treeItem.getForeground();
    boolean useGrayVariants = currentFg != null && currentFg.equals(systemDarkGray);

    // Changed git file colored blue
    UIFile file = changedFiles.get(absolutePath);
    if (file != null) {
      switch (file.getChangeType()) {
        case ADD:
        case COPY:
        case RENAME:
          treeItem.setForeground(
              file.isStaged()
                  ? (useGrayVariants ? colorStagedAddGray : colorStagedAdd)
                  : (useGrayVariants ? colorUnstagedGray : colorUnstaged));
          break;
        case MODIFY:
          treeItem.setForeground(
              file.isStaged()
                  ? (useGrayVariants ? colorStagedModifyGray : colorStagedModify)
                  : (useGrayVariants ? colorUnstagedGray : colorUnstaged));
          break;
        case DELETE:
          treeItem.setForeground(useGrayVariants ? colorStagedUnchangedGray : colorStagedUnchanged);
      }
    }

    if (ignoredFiles.containsKey(absolutePath)) {
      treeItem.setForeground(useGrayVariants ? colorIgnoredGray : colorIgnored);
    }
  }

  @GuiMenuElement(
      root = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_GIT_INFO,
      label = "i18n::GitGuiPlugin.Menu.Info.Text",
      image = "git-info.svg",
      separator = true)
  @GuiToolbarElement(
      root = ExplorerPerspective.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_GIT_INFO,
      toolTip = "i18n::GitGuiPlugin.Toolbar.Info.Tooltip",
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
      activeFile.setName(BaseMessages.getString(PKG, "GitGuiPlugin.Project.Label"));
      activeFile.setFilename(git.getDirectory());
    }
    activeFile.setName(
        BaseMessages.getString(PKG, "GitGuiPlugin.Info.Label", activeFile.getName()));
    GitInfoExplorerFileType fileType = new GitInfoExplorerFileType();
    activeFile.setFileType(fileType);
    GitInfoExplorerFileTypeHandler fileTypeHandler =
        fileType.createFileTypeHandler(HopGui.getInstance(), explorerPerspective, activeFile);

    explorerPerspective.addFile(fileTypeHandler);
  }

  public UIGit getGit() {
    return git;
  }

  private void setBranchLabel(String branch) {
    // Set the branch name using the new method that handles both SWT and RWT (and flow toolbar)
    HopGui.getInstance()
        .getStatusToolbarWidgets()
        .setToolbarItemText(ID_TOOLBAR_ITEM_GIT, Const.NVL(branch, ""));
  }
}
