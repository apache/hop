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

package org.apache.hop.git;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.git.model.UIFile;
import org.apache.hop.git.model.UIGit;
import org.apache.hop.git.model.VCS;
import org.apache.hop.git.util.FileTypeUtils;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CLabel;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

@HopPerspectivePlugin(
    id = "310-GitCommitPerspective",
    name = "i18n::GitCommitPerspective.Name",
    description = "i18n::GitCommitPerspective.Description",
    image = "git-commit-perspective.svg",
    documentationUrl = "/hop-gui/perspective-git-commit.html")
@GuiPlugin(
    name = "i18n::GitCommitPerspective.Name",
    description = "i18n::GitCommitPerspective.Description")
public class GitCommitPerspective implements IHopPerspective {
  public static final Class<?> PKG = GitCommitPerspective.class; // i18n

  public static final String GUI_PLUGIN_CONTEXT_MENU_PARENT_ID = "GitCommitPerspective-ContextMenu";
  public static final String CONTEXT_MENU_ADD = "GitCommitPerspective-ContextMenu-10100-AddToGit";
  public static final String CONTEXT_MENU_ADD_TO_GIT_IGNORE =
      "GitCommitPerspective-ContextMenu-10110-AddToGitIgnore";
  public static final String CONTEXT_MENU_UNSTAGE =
      "GitCommitPerspective-ContextMenu-10120-Unstage";
  public static final String CONTEXT_MENU_SHOW_TEXT_DIFF =
      "GitCommitPerspective-ContextMenu-10200-ShowTextDiff";
  public static final String CONTEXT_MENU_SHOW_GRAPH_DIFF =
      "GitCommitPerspective-ContextMenu-10210-ShowGraphDiff";
  public static final String CONTEXT_MENU_RESTORE =
      "GitCommitPerspective-ContextMenu-10300-Restore";
  public static final String CONTEXT_MENU_DELETE = "GitCommitPerspective-ContextMenu-10400-Delete";

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "GitCommitPerspective-Toolbar";
  public static final String TOOLBAR_ITEM_REFRESH = "GitCommitPerspective-Toolbar-10100-Refresh";
  public static final String TOOLBAR_ITEM_ADD = "GitCommitPerspective-Toolbar-10200-Add";
  public static final String TOOLBAR_ITEM_UNSTAGE = "GitCommitPerspective-Toolbar-10250-Unstage";
  public static final String TOOLBAR_ITEM_RESTORE = "GitCommitPerspective-Toolbar-10300-Restore";
  public static final String TOOLBAR_ITEM_DELETE = "GitCommitPerspective-Toolbar-10400-Delete";

  private static final String COMMIT_MESSAGES_AUDIT_TYPE = "commit-messages";
  private static final String STAGED_LABEL = "GitCommitPerspective.Status.Staged.Label";
  private static final String UNSTAGED_LABEL = "GitCommitPerspective.Status.Unstaged.Label";
  private static final String UNTRACKED_LABEL = "GitCommitPerspective.Status.Untracked.Label";
  private static GitCommitPerspective instance;

  private HopGui hopGui;
  private SashForm wSashForm;
  private Control wToolBar;
  private Tree wTree;
  private TreeItem stagedRootItem;
  private TreeItem unstagedRootItem;
  private TreeItem untrackedRootItem;
  private Text wMessage;
  private CLabel wStatus;
  private Button wAmend;
  private Button wCommit;
  private Button wCommitAndPush;
  private GuiToolbarWidgets toolBarWidgets;
  private GuiMenuWidgets menuWidgets;

  public GitCommitPerspective() {
    instance = this;
  }

  public static GitCommitPerspective getInstance() {
    try {
      GitCommitPerspective fromGui = HopGui.findSessionPerspective(GitCommitPerspective.class);
      if (fromGui != null) {
        return fromGui;
      }
    } catch (Throwable e) {
      // No HopGuiImpl in unit tests
    }
    return instance;
  }

  @Override
  public String getId() {
    return "GitCommitPerspective";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'o', global = true)
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'o', global = true)
  @Override
  public void activate() {
    if (hopGui == null) {
      // The perspective is switched off in disabledGuiElements.xml: it was never initialized, so
      // there is nothing to switch to. The git toolbar and menu items that lead here live on the
      // explorer perspective and are still there.
      //
      return;
    }
    hopGui.setActivePerspective(this);
  }

  /**
   * Enable or disable a context menu item. Every menu item can be switched off with an exclusion
   * for its id in disabledGuiElements.xml, in which case it was never created and there is nothing
   * here to enable.
   */
  private static void setMenuItemEnabled(GuiMenuWidgets menuWidgets, String id, boolean enabled) {
    MenuItem menuItem = menuWidgets.findMenuItem(id);
    if (menuItem != null) {
      menuItem.setEnabled(enabled);
    }
  }

  @Override
  public void perspectiveActivated() {
    // TODO: avoid refresh when perspective is activated, detect change in the config, metadata,
    // file explorer
    refresh();
  }

  @Override
  public boolean isActive() {
    return hopGui != null && hopGui.isActivePerspective(this);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;

    // Split changed files tree and commit message text
    //
    wSashForm = new SashForm(parent, SWT.VERTICAL);
    wSashForm.setLayoutData(FormDataBuilder.builder().fullSize().result());

    createTree(wSashForm);
    createMessageText(wSashForm);

    wSashForm.setWeights(70, 30);

    retrieveState();
    refresh();

    // Refresh the state when a file changes
    // TODO: For now we refresh when we active the perspective
    //    hopGui
    //        .getEventsHandler()
    //        .addEventListener(
    //            getClass().getName(),
    //            e -> refresh(),
    //            HopGuiEvents.ProjectUpdated.name(),
    //            HopGuiEvents.PipelineCreated.name(),
    //            HopGuiEvents.PipelineUpdated.name(),
    //            HopGuiEvents.PipelineDeleted.name(),
    //            HopGuiEvents.WorkflowCreated.name(),
    //            HopGuiEvents.WorkflowUpdated.name(),
    //            HopGuiEvents.WorkflowDeleted.name(),
    //            HopGuiEvents.MetadataCreated.name(),
    //            HopGuiEvents.MetadataChanged.name(),
    //            HopGuiEvents.MetadataDeleted.name(),
    //            HopGuiEvents.FileCreated.name(),
    //            HopGuiEvents.FileChanged.name(),
    //            HopGuiEvents.FileDeleted.name()
    //        );

    HopGuiKeyHandler.getInstance().addParentObjectToHandle(this);
  }

  protected void createTree(Composite parent) {
    Composite composite = new Composite(parent, SWT.BORDER);
    composite.setLayout(new FormLayout());
    composite.setLayoutData(FormDataBuilder.builder().fullSize().result());
    PropsUi.setLook(composite);

    // Create toolbar
    //
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);

    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);

    wToolBar = toolBarContainer.getControl();
    wToolBar.setLayoutData(FormDataBuilder.builder().fullWidth().top().result());
    wToolBar.pack();
    PropsUi.setLook(wToolBar, Props.WIDGET_STYLE_TOOLBAR);

    wTree = new Tree(composite, SWT.CHECK | SWT.V_SCROLL | SWT.H_SCROLL);
    wTree.setHeaderVisible(false);
    wTree.setLayoutData(FormDataBuilder.builder().fullWidth().top(wToolBar).bottom().result());
    wTree.addListener(SWT.Selection, this::onSelection);
    wTree.addListener(SWT.DefaultSelection, this::openFile);
    PropsUi.setLook(wTree);

    // Create context menu...
    //
    Menu menu = new Menu(wTree);
    menuWidgets = new GuiMenuWidgets();
    menuWidgets.registerGuiPluginObject(this);
    menuWidgets.createMenuWidgets(GUI_PLUGIN_CONTEXT_MENU_PARENT_ID, getShell(), menu);
    wTree.setMenu(menu);
    wTree.addListener(
        SWT.MenuDetect,
        event -> {
          List<UIFile> selectedFiles = this.getSelectedFiles();

          setMenuItemEnabled(menuWidgets, CONTEXT_MENU_ADD, !getSelectedUnstagedFiles().isEmpty());
          setMenuItemEnabled(
              menuWidgets, CONTEXT_MENU_UNSTAGE, !getSelectedStagedFiles().isEmpty());
          setMenuItemEnabled(menuWidgets, CONTEXT_MENU_ADD_TO_GIT_IGNORE, !selectedFiles.isEmpty());
          setMenuItemEnabled(menuWidgets, CONTEXT_MENU_SHOW_TEXT_DIFF, !selectedFiles.isEmpty());
          setMenuItemEnabled(
              menuWidgets,
              CONTEXT_MENU_SHOW_GRAPH_DIFF,
              selectedFiles.stream().anyMatch(file -> FileTypeUtils.isHopFileType(file.getName())));
          setMenuItemEnabled(
              menuWidgets, CONTEXT_MENU_RESTORE, !getSelectedStagedFiles().isEmpty());
          setMenuItemEnabled(menuWidgets, CONTEXT_MENU_DELETE, !selectedFiles.isEmpty());

          // Show the menu
          menu.setVisible(true);
        });
  }

  protected void createMessageText(Composite parent) {
    Composite composite = new Composite(parent, SWT.BORDER);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    composite.setLayout(layout);
    composite.setLayoutData(FormDataBuilder.builder().fullSize().result());
    PropsUi.setLook(composite);

    wAmend = new Button(composite, SWT.CHECK);
    wAmend.setText(BaseMessages.getString(PKG, "GitCommitPerspective.Button.Amend.Label"));
    wAmend.setToolTipText(BaseMessages.getString(PKG, "GitCommitPerspective.Button.Amend.Tooltip"));
    wAmend.setLayoutData(FormDataBuilder.builder().top(0, ConstUi.MEDIUM_MARGIN).left().result());
    PropsUi.setLook(wAmend);

    wCommit = new Button(composite, SWT.PUSH);
    wCommit.setText(BaseMessages.getString(PKG, "GitCommitPerspective.Button.Commit.Label"));
    wCommit.setLayoutData(FormDataBuilder.builder().bottom().left().result());
    wCommit.addListener(SWT.Selection, event -> commitFiles(false));
    PropsUi.setLook(wCommit);

    wCommitAndPush = new Button(composite, SWT.PUSH);
    wCommitAndPush.setText(
        BaseMessages.getString(PKG, "GitCommitPerspective.Button.CommitAndPush.Label"));
    wCommitAndPush.setLayoutData(
        FormDataBuilder.builder().bottom().left(wCommit, ConstUi.MEDIUM_MARGIN).result());
    wCommitAndPush.addListener(SWT.Selection, event -> commitFiles(true));
    PropsUi.setLook(wCommitAndPush);

    wStatus = new CLabel(composite, SWT.NONE);
    wStatus.setLayoutData(
        FormDataBuilder.builder()
            .bottom()
            .left(wCommitAndPush, ConstUi.MEDIUM_MARGIN)
            .right()
            .result());
    wStatus.setVisible(false);
    PropsUi.setLook(wStatus);

    wMessage = new Text(composite, SWT.BORDER | SWT.MULTI);
    wMessage.setLayoutData(
        FormDataBuilder.builder()
            .top(wAmend, ConstUi.MEDIUM_MARGIN)
            .fullWidth()
            .bottom(wCommit, -ConstUi.MEDIUM_MARGIN)
            .result());
    wMessage.addListener(
        SWT.Modify,
        event -> {
          saveState();
          updateGui();
        });
    PropsUi.setLook(wMessage);

    getShell().setDefaultButton(wCommit);
  }

  @GuiKeyboardShortcut(control = true, key = 'k', global = true)
  @GuiOsxKeyboardShortcut(command = true, key = 'k', global = true)
  public void selectAllChanged() {
    checkChangedFiles();

    wMessage.selectAll();
    wMessage.setFocus();
  }

  /**
   * Check the files git already knows about, staged or not: the commit acts on what is checked, so
   * this offers all the work in progress for the next commit. Untracked files are left unchecked:
   * committing a file for the first time is a deliberate act, git doesn't do it for you either.
   */
  private void checkChangedFiles() {
    setChecked(stagedRootItem, true);
    setChecked(unstagedRootItem, true);
    setChecked(untrackedRootItem, false);
  }

  /** Check or uncheck a whole group of files. */
  private void setChecked(TreeItem rootItem, boolean checked) {
    if (!isUsable(rootItem)) {
      return;
    }
    rootItem.setChecked(checked && rootItem.getItemCount() > 0);
    for (TreeItem item : rootItem.getItems()) {
      item.setChecked(checked);
    }
  }

  /** A file git doesn't know about yet: added in the working tree, not in the index. */
  private boolean isUntracked(UIFile file) {
    return !file.isStaged() && file.getChangeType() == DiffEntry.ChangeType.ADD;
  }

  /**
   * Uncheck the given files wherever they ended up outside the index: they are no longer part of
   * the commit. A file that was staged as new lands under "Untracked" once it is unstaged.
   */
  private void uncheckUnstagedFiles(Set<String> fileNames) {
    if (fileNames.isEmpty()) {
      return;
    }

    for (TreeItem rootItem : new TreeItem[] {unstagedRootItem, untrackedRootItem}) {
      if (!isUsable(rootItem)) {
        continue;
      }
      boolean allChecked = rootItem.getItemCount() > 0;
      for (TreeItem item : rootItem.getItems()) {
        if (item.getData() instanceof UIFile file && fileNames.contains(file.getName())) {
          item.setChecked(false);
        }
        allChecked = allChecked && item.getChecked();
      }
      rootItem.setChecked(allChecked);
    }
  }

  protected void retrieveState() {
    if (wMessage == null || wMessage.isDisposed()) {
      return;
    }
    try {
      AuditList auditList =
          AuditManager.getActive()
              .retrieveList(HopNamespace.getNamespace(), COMMIT_MESSAGES_AUDIT_TYPE);
      String message = "";
      if (!auditList.getNames().isEmpty()) {
        message = auditList.getNames().getFirst();
      }

      wMessage.setText(message);
    } catch (HopException e) {
      LogChannel.UI.logError("Error retrieve commit messages history", e);
    }
  }

  protected void saveState() {
    try {
      List<String> messages = new ArrayList<>();
      messages.add(wMessage.getText());
      AuditManager.getActive()
          .storeList(
              HopNamespace.getNamespace(), COMMIT_MESSAGES_AUDIT_TYPE, new AuditList(messages));
    } catch (HopException e) {
      LogChannel.UI.logError("Error saving commit messages history", e);
    }
  }

  protected void onSelection(Event event) {
    TreeItem item = (TreeItem) event.item;
    // If we select or unselect a root item, check all children
    if (item != null && item.getData() == null) {
      for (TreeItem child : item.getItems()) {
        child.setChecked(item.getChecked());
      }
    }
    updateGui();
  }

  protected void openFile(Event event) {
    try {
      TreeItem item = (TreeItem) event.item;
      if (item != null) {
        if (item.getData() instanceof UIFile file) {
          ExplorerPerspective perspective = ExplorerPerspective.getInstance();
          UIGit git = GitGuiPlugin.getInstance().getGit();
          String path = this.getAbsolutePath(git.getDirectory(), file.getName());
          IHopFileType fileType = perspective.getFileType(path);
          fileType.openFile(hopGui, path, hopGui.getVariables());
          perspective.activate();
        } else {
          item.setExpanded(!item.getExpanded());
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "GitCommitPerspective.Error.OpenFile.Header"),
          BaseMessages.getString(PKG, "GitCommitPerspective.Error.OpenFile.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_ADD,
      label = "i18n::GitCommitPerspective.Menu.AddToGit.Text",
      image = "git-add.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ADD,
      toolTip = "i18n::GitCommitPerspective.Toolbar.AddToGit.Tooltip",
      image = "git-add.svg")
  @GuiKeyboardShortcut(control = true, alt = true, key = 'A')
  @GuiOsxKeyboardShortcut(control = true, alt = true, key = 'A')
  public void addFilesToGit() {
    try {
      List<UIFile> files = getSelectedFiles();
      if (files.isEmpty()) {
        return;
      }

      UIGit git = GitGuiPlugin.getInstance().getGit();
      for (UIFile file : files) {
        git.add(file.getName());
      }

      GitGuiPlugin.getInstance().beforeRefresh();
      refresh();

      // Refresh the tree, change colors...
      ExplorerPerspective.getInstance().refresh();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.AddError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.AddError.Message"),
          e);
    }
  }

  /**
   * Unstage the selected staged files: the equivalent of git restore --staged. The changes are kept
   * in the working tree, only the index is updated. Use {@link #restoreFiles()} to throw the
   * changes away.
   */
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_UNSTAGE,
      label = "i18n::GitCommitPerspective.Menu.Unstage.Text",
      image = "git-unstage.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_UNSTAGE,
      toolTip = "i18n::GitCommitPerspective.Toolbar.Unstage.Tooltip",
      image = "git-unstage.svg")
  @GuiKeyboardShortcut(control = true, alt = true, key = 'U')
  @GuiOsxKeyboardShortcut(control = true, alt = true, key = 'U')
  public void unstageFiles() {
    try {
      List<UIFile> files = getSelectedStagedFiles();
      if (files.isEmpty()) {
        return;
      }

      UIGit git = GitGuiPlugin.getInstance().getGit();
      Set<String> unstagedFileNames = new HashSet<>();
      for (UIFile file : files) {
        git.resetPath(file.getName());
        unstagedFileNames.add(file.getName());
      }

      GitGuiPlugin.getInstance().beforeRefresh();
      refresh();

      // Taking a file out of the commit is the point of unstaging it, so leave these unchecked
      // even though refresh() offers every changed file by default.
      //
      uncheckUnstagedFiles(unstagedFileNames);
      updateGui();

      // Refresh the tree, change colors...
      ExplorerPerspective.getInstance().refresh();
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.UnstageError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.UnstageError.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_ADD_TO_GIT_IGNORE,
      label = "i18n::GitCommitPerspective.Menu.AddToGitIgnore.Text",
      image = "ignore.svg")
  public void addFilesToGitIgnore() {
    List<UIFile> files = getSelectedFiles();
    if (files.isEmpty()) {
      return;
    }

    UIGit git = GitGuiPlugin.getInstance().getGit();
    for (UIFile file : files) {
      git.addPathToIgnore(file.getName());
    }

    GitGuiPlugin.getInstance().beforeRefresh();
    refresh();

    // Refresh the tree, change colors...
    ExplorerPerspective.getInstance().refresh();
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_DELETE,
      separator = true,
      label = "i18n::GitCommitPerspective.Menu.Delete.Text",
      image = "ui/images/delete.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DELETE,
      separator = true,
      toolTip = "i18n::GitCommitPerspective.Toolbar.Delete.Tooltip",
      image = "ui/images/delete.svg")
  @GuiKeyboardShortcut(key = SWT.DEL)
  @GuiOsxKeyboardShortcut(key = SWT.DEL)
  public void deleteFiles() {
    try {
      List<UIFile> files = getSelectedFiles();
      if (!files.isEmpty()) {
        UIGit git = GitGuiPlugin.getInstance().getGit();
        ExplorerPerspective perspective = ExplorerPerspective.getInstance();

        List<String> filesToClose = new ArrayList<>();

        for (UIFile file : files) {

          // Already deleted
          if (file.getChangeType() == DiffEntry.ChangeType.DELETE) {
            continue;
          }

          String path = getAbsolutePath(git.getDirectory(), file.getName());
          if (!perspective.confirmDeleteWithReferenceCheck(List.of(path), file.getName())) {
            break;
          }

          FileObject fileObject = HopVfs.getFileObject(path);
          if (fileObject.delete()) {
            filesToClose.add(path);
          }
        }

        // Close tabs for delete files
        perspective.closeTabsForFilenames(filesToClose);

        refresh();

        // Refresh perspectives (file explorer, metadata)
        // TODO: Find a better way
        GitPerspective.getInstance().refresh(true);
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "GitCommitPerspective.Error.DeleteFile.Header"),
          BaseMessages.getString(PKG, "GitCommitPerspective.Error.DeleteFile.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_RESTORE,
      separator = true,
      label = "i18n::GitCommitPerspective.Menu.Restore.Text",
      image = "git-restore.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_RESTORE,
      // Keep restore, which throws changes away, out of the staging group
      separator = true,
      toolTip = "i18n::GitCommitPerspective.Toolbar.Restore.Tooltip",
      image = "git-restore.svg")
  @GuiKeyboardShortcut(alt = true, control = true, key = 'Z')
  @GuiOsxKeyboardShortcut(alt = true, control = true, key = 'Z')
  public void restoreFiles() {
    try {
      // Restore only selected staged file, use delete for untracked files
      List<UIFile> files = this.getSelectedStagedFiles();
      if (!files.isEmpty()) {

        // Ask confirmation if staged files will be reverted to the HEAD
        // and optionally added or unstaged files will be deleted
        MessageDialogWithToggle dialog =
            new MessageDialogWithToggle(
                getShell(),
                "Restore changes",
                "Are you sure you want to restore the selected files?",
                SWT.ICON_QUESTION,
                new String[] {
                  BaseMessages.getString(PKG, "GitCommitPerspective.Button.Restore.Label"),
                  BaseMessages.getString(PKG, "System.Button.Cancel")
                },
                "Delete local copies of added files",
                false);

        if (dialog.open() == 1) { // Means: "Cancel" button clicked!
          return;
        }

        boolean deleteAddedFiles = dialog.getToggleState();
        UIGit git = GitGuiPlugin.getInstance().getGit();

        List<String> filesToClose = new ArrayList<>();
        List<String> filesToReload = new ArrayList<>();
        List<String> addedFilesToDelete = new ArrayList<>();
        for (UIFile file : files) {
          // Absolute path for file explorer
          String path = getAbsolutePath(git.getDirectory(), file.getName());

          switch (file.getChangeType()) {
            case ADD -> {
              // Restoring an added file only unstages it, the file stays on disk unless the
              // user explicitly asked to delete the local copies as well.
              git.revertPath(file.getName());
              if (deleteAddedFiles) {
                addedFilesToDelete.add(file.getName());
                filesToClose.add(path);
              }
            }
            case MODIFY, RENAME, DELETE, COPY -> {
              git.revertPath(file.getName());
              filesToReload.add(path);
            }
          }
        }

        // The files which were added are untracked now: delete the local copies when asked
        git.cleanPaths(addedFilesToDelete);

        // Close tabs for restored files that were deleted (untracked/added)
        ExplorerPerspective.getInstance().closeTabsForFilenames(filesToClose);

        // Reload tabs for restored files that still exist (changed/missing/uncommitted)
        ExplorerPerspective.getInstance().reloadTabsForFilenames(filesToReload);

        // Refresh git status of files
        GitGuiPlugin.getInstance().beforeRefresh();
        refresh();

        // Refresh perspectives (file explorer, metadata)
        // TODO: Find a better way
        GitPerspective.getInstance().refresh(true);
      }

    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.RestoreError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.RestoreError.Message"),
          e);
    }
  }

  private String getAbsolutePath(String root, String relativePath) {
    try {
      FileObject fileObj = HopVfs.getFileObject(new File(root, relativePath).getAbsolutePath());
      String path =
          fileObj.exists()
              ? HopVfs.getFilename(fileObj)
              : new File(root, relativePath).getAbsolutePath();

      return path;
    } catch (Exception ignored) {
      return new File(root, relativePath).getAbsolutePath();
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_SHOW_TEXT_DIFF,
      separator = true,
      label = "i18n::GitCommitPerspective.Menu.ShowTextDiff.Text",
      image = "diff-text.svg")
  @GuiKeyboardShortcut(control = true, key = 'D')
  @GuiOsxKeyboardShortcut(control = true, key = 'D')
  public void showTextDiff() {
    try {
      TreeItem treeItem = wTree.getSelection()[0];
      if (treeItem != null) {
        UIFile file = (UIFile) treeItem.getData();

        String commitIdNew = VCS.WORKINGTREE;
        String commitIdOld = Constants.HEAD;

        GitGuiPlugin.getInstance().showTextFileDiff(file.getName(), commitIdNew, commitIdOld);
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.ShowDiffError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.ShowDiffError.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_SHOW_GRAPH_DIFF,
      label = "i18n::GitCommitPerspective.Menu.ShowGraphDiff.Text",
      image = "diff-graph.svg")
  @GuiKeyboardShortcut(control = true, shift = true, key = 'D')
  @GuiOsxKeyboardShortcut(control = true, shift = true, key = 'D')
  public void showGraphDiff() {
    try {
      TreeItem treeItem = wTree.getSelection()[0];
      if (treeItem != null) {
        UIFile file = (UIFile) treeItem.getData();

        String commitIdNew = VCS.WORKINGTREE;
        String commitIdOld = Constants.HEAD;
        ExplorerPerspective perspective = HopGui.getExplorerPerspective();

        if (perspective.getPipelineFileType().isHandledBy(file.getName(), false)) {
          // A pipeline
          //
          GitGuiPlugin.getInstance().showPipelineFileDiff(file.getName(), commitIdNew, commitIdOld);
        } else if (perspective.getWorkflowFileType().isHandledBy(file.getName(), false)) {
          // A workflow
          //
          GitGuiPlugin.getInstance().showWorkflowFileDiff(file.getName(), commitIdNew, commitIdOld);
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.ShowDiffError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.ShowDiffError.Message"),
          e);
    }
  }

  public void commitFiles(boolean push) {
    try {

      // Selected staged or unstaged files
      List<UIFile> filesToCommit = this.getSelectedFiles(wTree.getItems(), new ArrayList<>(), true);

      // Unselected staged files
      List<UIFile> filesToIgnore =
          isUsable(stagedRootItem)
              ? this.getSelectedFiles(stagedRootItem.getItems(), new ArrayList<>(), false)
              : new ArrayList<>();

      // No files to commit. Selecting a row in the tree only highlights it, so tell the user to
      // check the box when there are files listed but none of them are checked.
      //
      if (filesToCommit.isEmpty()) {
        boolean treeHasFiles = !filesToIgnore.isEmpty() || !getUncheckedFiles().isEmpty();
        showStatus(
            GuiResource.getInstance().getImageError(),
            BaseMessages.getString(
                PKG,
                treeHasFiles
                    ? "GitCommitPerspective.Error.NoFilesChecked.Message"
                    : "GitGuiPlugin.Dialog.NoFilesToCommit.Message"));
        return;
      }

      // Standard author by default
      UIGit uiGit = GitGuiPlugin.getInstance().getGit();
      String authorName = uiGit.getAuthorName(VCS.WORKINGTREE);
      String message = wMessage.getText();
      boolean amend = wAmend.getSelection();

      // A merge, cherry-pick or revert has to be committed as a whole: the commit records the
      // complete result, and for a merge it also records the second parent. Say so instead of
      // dropping the unchecked files from the commit behind the user's back.
      //
      String pendingOperation = getPendingOperation(uiGit);
      if (pendingOperation != null && !filesToIgnore.isEmpty()) {
        showStatus(
            GuiResource.getInstance().getImageError(),
            BaseMessages.getString(
                PKG, "GitCommitPerspective.Error.PartialCommit.Message", pendingOperation));
        return;
      }

      // Stage and commit the checked files, keeping the staged files which were unchecked out of
      // the commit
      //
      uiGit.commitPaths(
          filesToCommit.stream().map(UIFile::getName).toList(), authorName, message, amend);
      String commitId = uiGit.getCommitId(Constants.HEAD);

      GitGuiPlugin.getInstance().beforeRefresh();
      refresh();

      // Refresh file and git perspectives
      GitPerspective.getInstance().refresh(true);

      wAmend.setSelection(false);

      boolean pushed = false;
      if (push) {
        try {
          pushed = uiGit.push();
        } catch (Exception e) {
          new ErrorDialog(
              HopGui.getInstance().getShell(),
              BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PushError.Header"),
              BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PushError.Message"),
              e);
        }
      }

      // Confirm what happened. The status stays until the next thing the user does, no dialog to
      // click away. It goes last: refreshing the tree clears the status line.
      //
      showStatus(
          GuiResource.getInstance().getImageSuccess(),
          BaseMessages.getString(
              PKG,
              pushed
                  ? "GitCommitPerspective.Status.CommittedAndPushed.Message"
                  : "GitCommitPerspective.Status.Committed.Message",
              Integer.toString(filesToCommit.size()),
              getCommitIdLabel(commitId)));
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CommitError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CommitError.Message"),
          e);
    }
  }

  /**
   * The name of the operation git is in the middle of, to name it in a message. Git refuses to
   * commit part of the index while one of these is in progress.
   *
   * @param uiGit the repository to check
   * @return the name of the operation, or null when git is not in the middle of one
   */
  private String getPendingOperation(UIGit uiGit) {
    return switch (uiGit.getRepositoryState()) {
      case MERGING, MERGING_RESOLVED ->
          BaseMessages.getString(PKG, "GitCommitPerspective.Operation.Merge.Label");
      case CHERRY_PICKING, CHERRY_PICKING_RESOLVED ->
          BaseMessages.getString(PKG, "GitCommitPerspective.Operation.CherryPick.Label");
      case REVERTING, REVERTING_RESOLVED ->
          BaseMessages.getString(PKG, "GitCommitPerspective.Operation.Revert.Label");
      default -> null;
    };
  }

  /**
   * Show a line of feedback next to the commit buttons. It is wiped by the next {@link
   * #updateGui()} so it lives exactly as long as the user's next action.
   */
  private void showStatus(Image image, String message) {
    wStatus.setImage(image);
    wStatus.setText(message);
    wStatus.setVisible(true);
  }

  /**
   * The short commit id to append to the status, or nothing at all if git did not hand one back.
   */
  private String getCommitIdLabel(String commitId) {
    if (commitId == null || commitId.length() < 7) {
      return "";
    }
    return BaseMessages.getString(
        PKG, "GitCommitPerspective.Status.CommitId.Label", commitId.substring(0, 7));
  }

  /** Create one of the group nodes the files are listed under. */
  private TreeItem createRootItem(String labelKey) {
    TreeItem rootItem = new TreeItem(wTree, SWT.NONE);
    rootItem.setImage(GuiResource.getInstance().getImageFolder());
    setRootItemLabel(rootItem, labelKey);
    return rootItem;
  }

  /**
   * Label a group node with the number of files in it, so a collapsed group still tells you how
   * much is in there.
   */
  private void setRootItemLabel(TreeItem rootItem, String labelKey) {
    if (!isUsable(rootItem)) {
      return;
    }
    rootItem.setText(
        BaseMessages.getString(PKG, labelKey, Integer.toString(rootItem.getItemCount())));
  }

  /** The group a file belongs to, the way "git status" reports it. */
  private TreeItem getRootItemFor(UIFile file) {
    if (file.isStaged()) {
      return stagedRootItem;
    }
    return isUntracked(file) ? untrackedRootItem : unstagedRootItem;
  }

  private boolean isUsable(TreeItem rootItem) {
    return wTree != null && !wTree.isDisposed() && rootItem != null && !rootItem.isDisposed();
  }

  protected List<UIFile> getSelectedFiles() {
    if (wTree == null || wTree.isDisposed()) {
      return List.of();
    }
    return getSelectedFiles(wTree.getItems(), new ArrayList<>(), true);
  }

  protected List<UIFile> getSelectedStagedFiles() {
    if (!isUsable(stagedRootItem)) {
      return List.of();
    }
    return getSelectedFiles(stagedRootItem.getItems(), new ArrayList<>(), true);
  }

  /**
   * The checked files which are not in the index: changed files git tracks and files it doesn't
   * know about yet. These are the ones "Add" can stage.
   */
  protected List<UIFile> getSelectedUnstagedFiles() {
    List<UIFile> files = new ArrayList<>();
    for (TreeItem rootItem : new TreeItem[] {unstagedRootItem, untrackedRootItem}) {
      if (isUsable(rootItem)) {
        getSelectedFiles(rootItem.getItems(), files, true);
      }
    }
    return files;
  }

  protected List<UIFile> getUncheckedFiles() {
    if (wTree == null || wTree.isDisposed()) {
      return List.of();
    }
    return getSelectedFiles(wTree.getItems(), new ArrayList<>(), false);
  }

  protected List<UIFile> getSelectedFiles(TreeItem[] items, List<UIFile> files, boolean selected) {
    for (TreeItem item : items) {
      if ((item.getData() instanceof UIFile file) && item.getChecked() == selected) {
        files.add(file);
      }
      getSelectedFiles(item.getItems(), files, selected);
    }
    return files;
  }

  /* Update toolbar, button and menu state */
  public void updateGui() {
    boolean gitEnabled = GitGuiPlugin.getInstance().getGit() != null;
    boolean commitEnabled = gitEnabled && !wMessage.getText().trim().isEmpty();
    wToolBar.setEnabled(gitEnabled);
    wTree.setEnabled(gitEnabled);
    wAmend.setEnabled(gitEnabled);
    wMessage.setEnabled(gitEnabled);

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_ADD, !getSelectedUnstagedFiles().isEmpty());
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_UNSTAGE, !getSelectedStagedFiles().isEmpty());
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_RESTORE, !getSelectedStagedFiles().isEmpty());
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DELETE, !getSelectedFiles().isEmpty());

    wStatus.setVisible(false);
    wCommit.setEnabled(commitEnabled);
    wCommitAndPush.setEnabled(commitEnabled);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "i18n::GitCommitPerspective.Toolbar.Refresh.Tooltip",
      image = "ui/images/refresh.svg")
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refresh() {
    try {
      if (wTree == null || wTree.isDisposed()) {
        return;
      }
      wTree.setRedraw(false);
      wTree.removeAll();

      UIGit git = GitGuiPlugin.getInstance().getGit();
      if (git != null) {
        // The three groups "git status" reports: what is in the index, what changed in files git
        // tracks, and the files it doesn't know about yet.
        //
        stagedRootItem = createRootItem(STAGED_LABEL);
        unstagedRootItem = createRootItem(UNSTAGED_LABEL);
        untrackedRootItem = createRootItem(UNTRACKED_LABEL);

        // Reload changes files
        GitGuiPlugin.getInstance().refreshChangedFiles();
        Map<String, UIFile> filesToCommit = GitGuiPlugin.getInstance().getChangedFiles();

        GitResource resource = GitResource.getInstance();
        for (String fileName : filesToCommit.keySet()) {
          UIFile file = filesToCommit.get(fileName);

          TreeItem item = new TreeItem(getRootItemFor(file), SWT.NONE);
          switch (file.getChangeType()) {
            case ADD, COPY, RENAME ->
                item.setForeground(
                    file.isStaged() ? resource.getStagedAddColor() : resource.getUnstagedColor());
            case MODIFY ->
                item.setForeground(
                    file.isStaged()
                        ? resource.getStagedModifyColor()
                        : resource.getUnstagedColor());
            case DELETE -> item.setForeground(resource.getIgnoredColor());
          }

          item.setText(file.getName());
          item.setImage(FileTypeUtils.getImage(fileName));
          item.setData(file);
        }

        // The files are in, so the groups can say how many they hold
        //
        setRootItemLabel(stagedRootItem, STAGED_LABEL);
        setRootItemLabel(unstagedRootItem, UNSTAGED_LABEL);
        setRootItemLabel(untrackedRootItem, UNTRACKED_LABEL);

        for (TreeItem rootItem : wTree.getItems()) {
          rootItem.setExpanded(true);
        }

        // Offer all the work in progress for the next commit
        //
        checkChangedFiles();
      }
      wTree.setRedraw(true);

      updateGui();
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "GitPerspective.Refresh.Error.Header"),
          BaseMessages.getString(PKG, "GitPerspective.Refresh.Error.Message"),
          e);
    }
  }

  protected Shell getShell() {
    return hopGui.getShell();
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
    return false; // Nothing to do here
  }

  @Override
  public List<TabItemHandler> getItems() {
    return null;
  }

  @Override
  public Control getControl() {
    return wSashForm;
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return List.of();
  }
}
