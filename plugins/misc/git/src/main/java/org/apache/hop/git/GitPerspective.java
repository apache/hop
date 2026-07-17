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

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.git.config.GitConfigSingleton;
import org.apache.hop.git.info.DiffStyledTextComp;
import org.apache.hop.git.model.UIFile;
import org.apache.hop.git.model.UIGit;
import org.apache.hop.git.util.FileTypeUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.hopgui.shared.SashFormMemory;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.jgit.api.CherryPickResult;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.RenameDetector;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.merge.MergeStrategy;
import org.eclipse.jgit.revplot.PlotCommit;
import org.eclipse.jgit.revplot.PlotWalk;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.EmptyTreeIterator;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.TreeFilter;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.TreeEditor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

@HopPerspectivePlugin(
    id = "311-GitPerspective",
    name = "i18n::GitPerspective.Name",
    description = "i18n::GitPerspective.Description",
    image = "git-perspective.svg",
    documentationUrl = "/hop-gui/perspective-git.html")
@GuiPlugin(name = "i18n::GitPerspective.Name", description = "i18n::GitPerspective.Description")
public class GitPerspective implements IHopPerspective {
  public static final Class<?> PKG = GitPerspective.class; // i18n

  private static final String GIT_PERSPECTIVE_REF_TREE = "Git ref tree";

  // Fetch automatically every 20 minutes
  private static final long FETCH_AUTOMATIC_INTERVAL_MS = 20L * 60L * 1000L;

  public static final String GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID =
      "GitPerspective-RefContextMenu";
  public static final String REF_CONTEXT_MENU_CHECKOUT =
      "GitPerspective-RefContextMenu-100000-Checkout";
  public static final String REF_CONTEXT_MENU_CREATE_BRANCH =
      "GitPerspective-RefContextMenu-100100-CreateBranch";
  public static final String REF_CONTEXT_MENU_MERGE_BRANCH =
      "GitPerspective-RefContextMenu-100200-Merge";
  public static final String REF_CONTEXT_MENU_FETCH = "GitPerspective-RefContextMenu-100300-Fetch";
  public static final String REF_CONTEXT_MENU_PULL = "GitPerspective-RefContextMenu-100310-Pull";
  public static final String REF_CONTEXT_MENU_PUSH = "GitPerspective-RefContextMenu-100330-Push";
  public static final String REF_CONTEXT_MENU_RENAME =
      "GitPerspective-RefContextMenu-100500-Rename";
  public static final String REF_CONTEXT_MENU_DELETE =
      "GitPerspective-RefContextMenu-100600-Deelete";

  public static final String GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID =
      "GitPerspective-HistoryContextMenu";
  public static final String HISTORY_CONTEXT_MENU_CREATE_BRANCH =
      "GitPerspective-HistoryContextMenu-10000-CreateBranch";
  public static final String HISTORY_CONTEXT_MENU_CREATE_TAG =
      "GitPerspective-HistoryContextMenu-10100-CreateTag";
  public static final String HISTORY_CONTEXT_MENU_CHECKOUT_REVISION =
      "GitPerspective-HistoryContextMenu-10300-CheckoutRevsion";
  public static final String HISTORY_CONTEXT_MENU_COPY_REVISION =
      "GitPerspective-HistoryContextMenu-10300-CopyRevision";
  public static final String HISTORY_CONTEXT_MENU_RESET =
      "GitPerspective-HistoryContextMenu-10400-Reset";
  public static final String HISTORY_CONTEXT_MENU_REVERT_COMMIT =
      "GitPerspective-HistoryContextMenu-10500-Revert";
  public static final String HISTORY_CONTEXT_MENU_CHERRY_PICK =
      "GitPerspective-HistoryContextMenu-10600-CherryPick";

  public static final String GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID =
      "GitPerspective-FileContextMenu";
  public static final String FILE_CONTEXT_MENU_SHOW_TEXT_DIFF =
      "GitPerspective-FileContextMenu-10000-ShowTextDiff";
  public static final String FILE_CONTEXT_MENU_SHOW_GRAPH_DIFF =
      "GitPerspective-FileContextMenu-10010-ShowGraphDiff";
  public static final String FILE_CONTEXT_MENU_COPY_PATH =
      "GitPerspective-FileContextMenu-10100-CopyPath";
  public static final String FILE_CONTEXT_MENU_REVERT =
      "GitPerspective-FileContextMenu-10200-Revert";
  public static final String FILE_CONTEXT_MENU_CHERRY_PICK =
      "GitPerspective-FileContextMenu-10300-CherryPick";

  public static final String GUI_PLUGIN_HISTORY_TOOLBAR_PARENT_ID = "GitPerspective-HistoryToolbar";
  public static final String TOOLBAR_ITEM_REFRESH = "GitPerspective-HistoryToolbar-10000-Refresh";
  public static final String TOOLBAR_ITEM_CREATE_BRANCH =
      "GitPerspective-HistoryToolbar-20100-CreateBranch";
  public static final String TOOLBAR_ITEM_CREATE_TAG =
      "GitPerspective-HistoryToolbar-20200-CreateTag";
  public static final String TOOLBAR_ITEM_COMMIT_CHERRY_PICK =
      "GitPerspective-HistoryToolbar-30000-CherryPick";

  public static final String TOOLBAR_ITEM_FETCH = "GitPerspective-HistoryToolbar-30000-Fetch";
  public static final String TOOLBAR_ITEM_PULL = "GitPerspective-HistoryToolbar-30100-Pull";
  public static final String TOOLBAR_ITEM_PUSH = "GitPerspective-HistoryToolbar-30200-Push";

  public static final String TOOLBAR_ITEM_SHOW_HIDDEN =
      "GitPerspective-HistoryToolbar-40000-ShowHidenAll";

  public static final String GUI_PLUGIN_FILE_TOOLBAR_PARENT_ID = "GitPerspective-FileToolbar";
  public static final String TOOLBAR_ITEM_FILE_REVERT = "GitPerspective-FileToolbar-10200-Revert";
  public static final String TOOLBAR_ITEM_FILE_SHOW_TEXT_DIFF =
      "GitPerspective-FileToolbar-10000-ShowTextDiff";
  public static final String TOOLBAR_ITEM_FILE_SHOW_GRAPH_DIFF =
      "GitPerspective-FileToolbar-10010-ShowGraphDiff";
  public static final String TOOLBAR_ITEM_FILE_CHERRY_PICK =
      "GitPerspective-FileToolbar-10300-CherryPick";

  public static final String OPTION_SHOW_ALL_REF = "Git.ShowAllRef";

  @Getter private static GitPerspective instance;

  private HopGui hopGui;
  private SashForm wSashForm;

  private Control wRefToolBar;
  private Control wFileToolBar;
  private Control wHistoryToolBar;

  private Tree wRefTree;
  private TreeEditor wRefTreeEditor;
  private Tree wFileTree;
  private Table wHistoryTable;

  private Control wDiff; // Can be Text (web) or DiffStyledTextComp (desktop)
  private Control wDiffStyled; // Declared as Control (not DiffStyledTextComp) so works in hop web
  private Text wSearchText;
  private Text wDiffText;
  // private GuiToolbarWidgets refToolBarWidgets;
  private GuiToolbarWidgets fileToolBarWidgets;
  private GuiToolbarWidgets historyToolBarWidgets;
  private GuiMenuWidgets fileMenuWidgets;
  private GuiMenuWidgets refMenuWidgets;
  private GuiMenuWidgets historyMenuWidgets;

  private SwtCommitRenderer plotRenderer;
  private boolean enableAntialias = true;
  private boolean showAllRef = true;
  private Timer fetchAutomaticTimer;

  public GitPerspective() {
    instance = this;
  }

  @Override
  public String getId() {
    return "GitPerspective";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'g', global = true)
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'g', global = true)
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
    // TODO: avoid refresh when perspective is activated, detect change in the file explorer model
    refresh();

    wSearchText.setFocus();
  }

  @Override
  public boolean isActive() {
    return hopGui != null && hopGui.isActivePerspective(this);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;

    wSashForm = new SashForm(parent, SWT.HORIZONTAL);
    wSashForm.setLayoutData(FormDataBuilder.builder().fullSize().result());

    createRefTree(wSashForm);

    SashForm wSashFormCommit = new SashForm(wSashForm, SWT.HORIZONTAL);
    createHistoryTable(wSashFormCommit);

    SashForm wSashFormViewer = new SashForm(wSashFormCommit, SWT.VERTICAL);
    createFileTree(wSashFormViewer);
    createDiffView(wSashFormViewer);

    SashFormMemory.persist(wSashForm, "git-perspective-tree-width");
    SashFormMemory.persist(wSashFormCommit, "git-perspective-commit-split", 70, 30);
    SashFormMemory.persist(wSashFormViewer, "git-perspective-viewer-split", 30, 70);

    // Restore options
    restorePerspectiveSettings();

    // Automatically fetch from remote
    startFetchAutomaticTimer();

    // Add key listeners
    HopGuiKeyHandler.getInstance().addParentObjectToHandle(this);

    hopGui
        .getEventsHandler()
        .addEventListener(
            getClass().getName() + "ProjectActivated",
            e -> hopGui.getDisplay().asyncExec(this::clearSearchFilters),
            HopGuiEvents.ProjectActivated.name());
  }

  @Override
  public void clearSearchFilters() {
    if (wSearchText != null && !wSearchText.isDisposed()) {
      wSearchText.setText("");
    }
  }

  protected void createRefTree(Composite parent) {
    // Create composite
    Composite composite = new Composite(parent, SWT.NONE);
    FormLayout layout = new FormLayout();
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    composite.setLayout(layout);
    PropsUi.setLook(composite);

    // Create search/filter text box
    wSearchText = new Text(composite, SWT.SEARCH | SWT.ICON_CANCEL | SWT.ICON_SEARCH);
    wSearchText.setMessage(BaseMessages.getString(PKG, "GitPerspective.Search.Placeholder"));
    FormData searchFormData = new FormData();
    searchFormData.left = new FormAttachment(0, 0);
    searchFormData.top = new FormAttachment(0, 0);
    searchFormData.right = new FormAttachment(100, 0);
    wSearchText.setLayoutData(searchFormData);
    wSearchText.addListener(SWT.Modify, event -> search());
    PropsUi.setLook(wSearchText);

    // Create a composite with toolbar and tree for the border
    //    Composite composite = new Composite(treeComposite, SWT.BORDER);
    //    composite.setLayout(new FormLayout());
    //    composite.setLayoutData(FormDataBuilder.builder().top(wSearchText,
    // PropsUi.getMargin()).bottom().fullWidth().result());
    //    PropsUi.setLook(composite);

    // Create toolbar
    //
    //    IToolbarContainer toolBarContainer =
    //        ToolbarFacade.createToolbarContainer(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    //
    //    refToolBarWidgets = new GuiToolbarWidgets();
    //    refToolBarWidgets.registerGuiPluginObject(this);
    //    refToolBarWidgets.createToolbarWidgets(toolBarContainer,
    // GUI_PLUGIN_REF_TOOLBAR_PARENT_ID);
    //
    //    wRefToolBar = toolBarContainer.getControl();
    //    wRefToolBar.setLayoutData(FormDataBuilder.builder().fullWidth().top().result());
    //    wRefToolBar.pack();
    //    PropsUi.setLook(wRefToolBar, Props.WIDGET_STYLE_TOOLBAR);

    wRefTree = new Tree(composite, SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    wRefTree.setHeaderVisible(false);
    wRefTree.setLayoutData(
        FormDataBuilder.builder()
            .fullWidth()
            .top(wSearchText, PropsUi.getMargin())
            .bottom()
            .result());
    wRefTree.addListener(SWT.DefaultSelection, this::selectRef);
    PropsUi.setLook(wRefTree);

    // Create context menu
    Menu menu = new Menu(wRefTree);
    refMenuWidgets = new GuiMenuWidgets();
    refMenuWidgets.registerGuiPluginObject(this);
    refMenuWidgets.createMenuWidgets(GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID, getShell(), menu);
    wRefTree.setMenu(menu);
    wRefTree.addListener(
        SWT.MenuDetect,
        event -> {
          Ref ref = getSelectedReference();
          if (ref == null) {
            event.doit = false;
            return;
          }

          String currentBranch = GitGuiPlugin.getInstance().getGit().getBranch();
          String branch = Repository.shortenRefName(ref.getName());
          boolean isCurrentBranch = branch.equals(currentBranch);
          boolean isHeads = ref.getName().startsWith(Constants.R_HEADS);
          boolean isRemotes = ref.getName().startsWith(Constants.R_REMOTES);
          boolean isTags = ref.getName().startsWith(Constants.R_TAGS);

          setMenuItemEnabled(refMenuWidgets, REF_CONTEXT_MENU_CHECKOUT, !isCurrentBranch);
          setMenuItemEnabled(refMenuWidgets, REF_CONTEXT_MENU_PUSH, isHeads);
          setMenuItemEnabled(refMenuWidgets, REF_CONTEXT_MENU_PULL, isHeads);
          setMenuItemEnabled(refMenuWidgets, REF_CONTEXT_MENU_RENAME, isHeads);
          setMenuItemEnabled(refMenuWidgets, REF_CONTEXT_MENU_DELETE, !isCurrentBranch);

          MenuItem menuItem = refMenuWidgets.findMenuItem(REF_CONTEXT_MENU_CREATE_BRANCH);
          if (menuItem != null) {
            menuItem.setEnabled(isHeads || isRemotes);
            menuItem.setText(
                BaseMessages.getString(PKG, "GitPerspective.Menu.CreateBranchFrom.Text", branch));
          }

          menuItem = refMenuWidgets.findMenuItem(REF_CONTEXT_MENU_MERGE_BRANCH);
          if (menuItem != null) {
            menuItem.setEnabled(!isCurrentBranch);
            menuItem.setText(
                BaseMessages.getString(
                    PKG, "GitPerspective.Menu.MergeInto.Text", branch, currentBranch));
          }
        });

    // Create Tree editor for rename
    wRefTreeEditor = new TreeEditor(wRefTree);
    wRefTreeEditor.horizontalAlignment = SWT.LEFT;
    wRefTreeEditor.grabHorizontal = true;

    // Remember tree node expanded/collapsed
    TreeMemory.addTreeListener(wRefTree, GIT_PERSPECTIVE_REF_TREE);
  }

  protected void createFileTree(Composite parent) {
    Composite composite = new Composite(parent, SWT.BORDER);
    composite.setLayout(new FormLayout());
    composite.setLayoutData(FormDataBuilder.builder().fullSize().result());
    PropsUi.setLook(composite);

    // Create toolbar
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);

    fileToolBarWidgets = new GuiToolbarWidgets();
    fileToolBarWidgets.registerGuiPluginObject(this);
    fileToolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_FILE_TOOLBAR_PARENT_ID);

    wFileToolBar = toolBarContainer.getControl();
    wFileToolBar.setLayoutData(FormDataBuilder.builder().fullWidth().top().result());
    wFileToolBar.pack();
    PropsUi.setLook(wFileToolBar, Props.WIDGET_STYLE_TOOLBAR);

    wFileTree = new Tree(composite, SWT.V_SCROLL | SWT.H_SCROLL);
    wFileTree.setHeaderVisible(false);
    wFileTree.setLayoutData(
        FormDataBuilder.builder().fullWidth().top(wFileToolBar).bottom().result());
    wFileTree.addListener(SWT.Selection, this::selectFile);

    // Create context menu
    Menu menu = new Menu(wFileTree);
    fileMenuWidgets = new GuiMenuWidgets();
    fileMenuWidgets.registerGuiPluginObject(this);
    fileMenuWidgets.createMenuWidgets(GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID, getShell(), menu);
    wFileTree.setMenu(menu);
    wFileTree.addListener(SWT.MouseDoubleClick, event -> showTextDiff());
    wFileTree.addListener(
        SWT.MenuDetect,
        event -> {
          RevCommit commit = getSelectedCommit();
          String path = getSelectedFile();
          if (commit != null && path != null) {
            setMenuItemEnabled(fileMenuWidgets, FILE_CONTEXT_MENU_SHOW_TEXT_DIFF, true);
            setMenuItemEnabled(
                fileMenuWidgets,
                FILE_CONTEXT_MENU_SHOW_GRAPH_DIFF,
                FileTypeUtils.isHopFileType(path));
          } else {
            event.doit = false;
          }
        });

    PropsUi.setLook(wFileTree);
  }

  protected void selectRef(Event event) {
    TreeItem item = (TreeItem) event.item;
    if (item != null && item.getData() instanceof Ref ref) {
      refreshHistory(ref.getName());
    }
  }

  protected void selectCommit(Event event) {
    refreshFiles();
    updateGui();
  }

  protected void selectFile(Event event) {
    refreshFileDiff();
    updateGui();
  }

  protected void refreshFiles() {
    try {
      setDiffText("");
      wFileTree.setRedraw(false);
      wFileTree.removeAll();

      RevCommit commit = getSelectedCommit();
      if (commit != null) {
        Git git = GitGuiPlugin.getInstance().getGit().getGit();
        try (TreeWalk treeWalk = new TreeWalk(git.getRepository())) {
          if (commit.getParentCount() > 0) {
            treeWalk.addTree(commit.getParent(0).getTree());
          } else {
            treeWalk.addTree(new EmptyTreeIterator());
          }
          treeWalk.addTree(commit.getTree());
          treeWalk.setRecursive(true);
          treeWalk.setFilter(TreeFilter.ANY_DIFF);

          RenameDetector renameDetector = new RenameDetector(git.getRepository());
          renameDetector.addAll(DiffEntry.scan(treeWalk));

          List<DiffEntry> entries = renameDetector.compute();

          for (DiffEntry entry : entries) {
            String fileName = entry.getNewPath();
            if (entry.getChangeType() == DiffEntry.ChangeType.DELETE) {
              fileName = entry.getOldPath();
            }

            UIFile file = new UIFile(fileName, entry.getChangeType(), true);

            TreeItem item = new TreeItem(wFileTree, SWT.NONE);
            item.setText(fileName);
            item.setImage(FileTypeUtils.getImage(fileName));
            item.setData(file);
            item.setForeground(
                switch (entry.getChangeType()) {
                  case ADD, COPY, RENAME -> GitResource.getInstance().getStagedAddColor();
                  case MODIFY -> GitResource.getInstance().getStagedModifyColor();
                  case DELETE -> GitResource.getInstance().getIgnoredColor();
                });
          }
        }
      }

      wFileTree.setRedraw(true);

      refreshFileDiff();
    } catch (Exception e) {
      LogChannel.UI.logError("Error refresh commit file changes", e);
    }
  }

  protected void refreshFileDiff() {
    RevCommit commit = getSelectedCommit();
    String fileName = getSelectedFile();
    boolean visualDiffEnabled = false;

    if (commit != null && fileName != null) {
      UIGit git = GitGuiPlugin.getInstance().getGit();

      String commitId = commit.name();
      String parentCommitId = git.getParentCommitId(commitId);
      String diff = git.diff(parentCommitId, commitId, fileName);

      setDiffText(diff);

      // Enable visual diff button?
      visualDiffEnabled = FileTypeUtils.isHopFileType(fileName);
    }

    fileToolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_FILE_SHOW_TEXT_DIFF, visualDiffEnabled);
  }

  public Ref getSelectedReference() {
    if (wRefTree.getSelectionCount() < 1) {
      return null;
    }
    TreeItem item = wRefTree.getSelection()[0];
    if (item == null) {
      return null;
    }

    return item.getData() instanceof Ref ref ? ref : null;
  }

  public RevCommit getSelectedCommit() {
    if (wHistoryTable.getSelectionCount() < 1) {
      return null;
    }

    TableItem item = wHistoryTable.getSelection()[0];
    if (item == null) {
      return null;
    }

    return item.getData() instanceof RevCommit commit ? commit : null;
  }

  public String getSelectedFile() {
    if (wFileTree.getSelectionCount() < 1) {
      return null;
    }

    TreeItem item = wFileTree.getSelection()[0];
    if (item == null) {
      return null;
    }

    return item.getText();
  }

  /**
   * Determines whether a given commit is part of the current branch.
   *
   * @param commit The commit to verify against the current branch.
   * @return {@code true} if the specified commit is in the current branch, {@code false} otherwise.
   */
  public boolean isCommitInCurrentBranch(RevCommit commit) {
    if (commit == null) {
      return false;
    }
    try {
      UIGit uiGit = GitGuiPlugin.getInstance().getGit();
      if (uiGit == null) {
        return false;
      }
      return isCommitInCurrentBranch(uiGit.getGit().getRepository(), commit.getId());
    } catch (MissingObjectException e) {
      // Object id not present in the currently open repository (e.g. after project switch)
      return false;
    } catch (Exception e) {
      LogChannel.UI.logError("Error checking if commit is in current branch", e);
      return false;
    }
  }

  /**
   * Whether {@code commitId} is reachable from HEAD in {@code repository}.
   *
   * <p>Returns {@code false} when the id is missing from this object database (for example a stale
   * history selection left over from another project after a repository switch). Does not require
   * SWT or HopGui.
   *
   * @param repository repository to check against (may be {@code null})
   * @param commitId commit object id (may be {@code null})
   * @return {@code true} if the commit is merged into HEAD
   * @throws Exception if a non-missing-object git error occurs while walking history
   */
  static boolean isCommitInCurrentBranch(Repository repository, AnyObjectId commitId)
      throws Exception {
    if (repository == null || commitId == null) {
      return false;
    }
    // Stale selection from a previous project/repo must not be treated as a hard error
    if (!repository.getObjectDatabase().has(commitId)) {
      return false;
    }
    try (RevWalk walk = new RevWalk(repository)) {
      ObjectId headId = repository.resolve(Constants.HEAD);
      if (headId == null) {
        return false;
      }
      RevCommit headCommit = walk.parseCommit(headId);
      RevCommit commitToCheck = walk.parseCommit(commitId);
      return walk.isMergedInto(commitToCheck, headCommit);
    }
  }

  void startFetchAutomaticTimer() {
    if (!GitConfigSingleton.getConfig().isFetchAutomatic()) {
      stopFetchAutomaticTimer();
      return;
    }

    if (fetchAutomaticTimer != null) {
      return;
    }

    fetchAutomaticTimer = new Timer("Git fetch timer", true);
    fetchAutomaticTimer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            fetchAutomatic();
          }
        },
        FETCH_AUTOMATIC_INTERVAL_MS,
        FETCH_AUTOMATIC_INTERVAL_MS);
  }

  void stopFetchAutomaticTimer() {
    if (fetchAutomaticTimer != null) {
      fetchAutomaticTimer.cancel();
      fetchAutomaticTimer = null;
    }
  }

  private void fetchAutomatic() {
    if (!GitConfigSingleton.getConfig().isFetchAutomatic()) {
      stopFetchAutomaticTimer();
      return;
    }

    try {
      UIGit uiGit = GitGuiPlugin.getInstance().getGit();
      if (uiGit == null) {
        return;
      }

      uiGit.fetch();

      if (hopGui != null && !hopGui.getDisplay().isDisposed()) {
        hopGui.getDisplay().asyncExec(() -> refresh(false));
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error during automatic git fetch", e);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      id = HISTORY_CONTEXT_MENU_COPY_REVISION,
      separator = true,
      label = "i18n::GitPerspective.Menu.CopyRevisionId.Text",
      image = "ui/images/copy.svg")
  public void copyRevisionId() {
    RevCommit commit = getSelectedCommit();
    if (commit != null) {
      // Copies the revision ID of the currently selected commit to the clipboard.
      GuiResource.getInstance().toClipboard(commit.getId().name());
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID,
      id = FILE_CONTEXT_MENU_COPY_PATH,
      separator = true,
      label = "i18n::GitPerspective.Menu.CopyPath.Text",
      image = "ui/images/copy.svg")
  public void copyPath() {
    String path = this.getSelectedFile();
    if (path != null) {
      // Copies the path of the currently selected file in the commit to the clipboard.
      GuiResource.getInstance().toClipboard(path);
    }
  }

  /** Reset the current branch to the commit */
  @GuiMenuElement(
      root = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      id = HISTORY_CONTEXT_MENU_RESET,
      separator = true,
      label = "i18n::GitPerspective.Menu.ResetToCommit.Text",
      image = "git-reset.svg")
  public void resetToCommit() {
    RevCommit commit = getSelectedCommit();
    if (commit != null) {
      ResetTypeDialog dialog = new ResetTypeDialog(getShell());
      ResetCommand.ResetType resetType = dialog.open(commit);

      if (resetType != null) {
        UIGit git = GitGuiPlugin.getInstance().getGit();

        git.reset(commit.getId().name(), resetType);

        refresh(true);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      id = HISTORY_CONTEXT_MENU_REVERT_COMMIT,
      label = "i18n::GitPerspective.Menu.RevertCommit.Text",
      image = "git-restore.svg")
  public void revertCommit() {
    RevCommit commit = getSelectedCommit();
    if (commit != null) {
      UIGit uigit = GitGuiPlugin.getInstance().getGit();

      // Check if the git repository is clean
      if (!uigit.isClean()) {
        MessageBox dialog = new MessageBox(getShell(), SWT.ICON_WARNING | SWT.YES | SWT.NO);
        dialog.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.RevertCommit.Header"));
        dialog.setMessage(
            BaseMessages.getString(
                PKG, "GitGuiPlugin.Dialog.RevertCommitRepositoryIsNotClean.Message"));
        return;
      }

      // Ask for confirmation before revert commit
      MessageBox dialog = new MessageBox(getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      dialog.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.RevertCommit.Header"));
      dialog.setMessage(
          BaseMessages.getString(
              PKG,
              "GitGuiPlugin.Dialog.RevertCommitConfirmation.Message",
              commit.getShortMessage()));
      if (dialog.open() != SWT.YES) {
        return;
      }

      try {
        Git git = uigit.getGit();
        RevCommit revertCommit = git.revert().include(commit).call();

        refresh(true);
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.RevertCommit.Header"),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.RevertCommitError.Message"),
            e);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID,
      id = FILE_CONTEXT_MENU_REVERT,
      separator = true,
      label = "i18n::GitPerspective.Menu.RevertFile.Text",
      image = "git-restore.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_FILE_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_FILE_REVERT,
      separator = true,
      toolTip = "i18n::GitPerspective.Toolbar.RevertFile.Tooltip",
      image = "git-restore.svg")
  public void revertFile() {
    RevCommit commit = getSelectedCommit();
    String path = getSelectedFile();

    if (path == null || commit == null) {
      return;
    }

    try {
      Git git = GitGuiPlugin.getInstance().getGit().getGit();
      String commitId = commit.getId().name();

      git.checkout().setStartPoint(commitId).addPath(path).call();

      git.commit().setMessage("Revert " + path + " to version from " + commitId).call();

      refresh(true);
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.RevertFile.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.RevertFileError.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      id = HISTORY_CONTEXT_MENU_CHERRY_PICK,
      label = "i18n::GitPerspective.Menu.CherryPickCommit.Text",
      image = "cherry-pick.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_HISTORY_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_COMMIT_CHERRY_PICK,
      toolTip = "i18n::GitPerspective.Toolbar.CherryPickCommit.Tooltip",
      image = "cherry-pick.svg")
  public void cherryPickCommit() {
    RevCommit commit = getSelectedCommit();
    if (commit != null) {
      UIGit uigit = GitGuiPlugin.getInstance().getGit();

      // Check if the git repository is clean
      if (!uigit.isClean()) {
        MessageBox dialog = new MessageBox(getShell(), SWT.ICON_WARNING | SWT.YES | SWT.NO);
        dialog.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CherryPickCommit.Header"));
        dialog.setMessage(
            BaseMessages.getString(
                PKG, "GitGuiPlugin.Dialog.CherryPickCommitRepositoryIsNotClean.Message"));
        return;
      }

      try {
        Git git = uigit.getGit();

        CherryPickResult result = git.cherryPick().include(commit).setNoCommit(false).call();

        if (result.getStatus() == CherryPickResult.CherryPickStatus.CONFLICTING) {
          MessageBox dialog = new MessageBox(getShell(), SWT.ICON_WARNING | SWT.YES | SWT.NO);
          dialog.setText(
              BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CherryPickCommit.Header"));
          dialog.setMessage(
              BaseMessages.getString(
                  PKG,
                  "GitGuiPlugin.Dialog.CherryPickCommitConflicts.Message",
                  commit,
                  result.getFailingPaths()));
        }

        refresh(true);
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CherryPickCommit.Header"),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CherryPickCommitError.Message"),
            e);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID,
      id = FILE_CONTEXT_MENU_CHERRY_PICK,
      label = "i18n::GitPerspective.Menu.CherryPickFile.Text",
      image = "cherry-pick.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_FILE_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_FILE_CHERRY_PICK,
      toolTip = "i18n::GitPerspective.Toolbar.CherryPickFile.Tooltip",
      image = "cherry-pick.svg")
  public void cherryPickFile() {
    RevCommit commit = getSelectedCommit();
    String path = getSelectedFile();

    if (commit != null && path != null) {
      try {
        Git git = GitGuiPlugin.getInstance().getGit().getGit();

        String commitId = commit.getId().name();

        git.checkout().setStartPoint(commitId).addPath(path).call();

        git.add().addFilepattern(path).call();

        git.commit().setMessage("Cherry-pick " + path + " from commit " + commitId).call();

        refresh(true);
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CherryPickCommit.Header"),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.CherryPickCommitError.Message"),
            e);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      id = REF_CONTEXT_MENU_CHECKOUT,
      label = "i18n::GitPerspective.Menu.Checkout.Text",
      image = "ui/images/check.svg")
  public void checkoutReference() {
    Ref ref = getSelectedReference();
    if (ref != null) {
      GitGuiPlugin.getInstance().getGit().checkout(ref.getName());

      refresh(true);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      id = HISTORY_CONTEXT_MENU_CHECKOUT_REVISION,
      separator = true,
      label = "i18n::GitPerspective.Menu.CheckoutRevision.Text",
      image = "ui/images/check.svg")
  public void checkoutCommit() {
    RevCommit commit = getSelectedCommit();
    if (commit != null) {
      GitGuiPlugin.getInstance().getGit().checkout(commit.getName());

      refresh(true);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      id = HISTORY_CONTEXT_MENU_CREATE_TAG,
      label = "i18n::GitPerspective.Menu.CreateTag.Text",
      image = "tag-add.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_HISTORY_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_CREATE_TAG,
      toolTip = "i18n::GitPerspective.History.Toolbar.CreateTag.Tooltip",
      image = "tag-add.svg")
  public void addTag() {
    RevCommit commit = getSelectedCommit();
    if (commit != null) {
      EnterStringDialog dialog =
          new EnterStringDialog(
              getShell(),
              "",
              BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Tag.CreateTag.Header"),
              BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Tag.CreateTag.Message"));

      String name = dialog.open();
      if (name != null) {
        UIGit git = GitGuiPlugin.getInstance().getGit();
        if (git.createTag(name, commit.getId().name())) {
          refresh(false);
        }
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID,
      id = HISTORY_CONTEXT_MENU_CREATE_BRANCH,
      label = "i18n::GitPerspective.Menu.CreateBranch.Text",
      image = "branch-add.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_HISTORY_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_CREATE_BRANCH,
      separator = true,
      toolTip = "i18n::GitPerspective.History.Toolbar.CreateBranch.Tooltip",
      image = "branch-add.svg")
  public void addBranchFromCommit() {
    RevCommit commit = getSelectedCommit();
    if (commit != null) {
      EnterStringDialog dialog =
          new EnterStringDialog(
              getShell(),
              "",
              BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.CreateBranch.Header"),
              BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.CreateBranch.Message"));

      String name = dialog.open();
      if (name != null) {
        UIGit git = GitGuiPlugin.getInstance().getGit();
        if (git.createBranch(name, commit.getId().name())) {
          // Refresh the explorer file, refs and commit history
          refresh(true);
        }
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      id = REF_CONTEXT_MENU_CREATE_BRANCH,
      separator = true,
      label = "i18n::GitPerspective.Menu.CreateBranchFrom.Text",
      image = "branch-add.svg")
  public void addBranchFromRef() {
    Ref ref = getSelectedReference();
    if (ref != null) {
      EnterStringDialog dialog =
          new EnterStringDialog(
              getShell(),
              "",
              BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.CreateBranch.Header"),
              BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Branch.CreateBranch.Message"));

      String name = dialog.open();
      if (name != null) {
        UIGit git = GitGuiPlugin.getInstance().getGit();
        if (git.createBranch(name)) {
          // Refresh the explorer file, refs and commit history
          refresh(true);
        }
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      id = REF_CONTEXT_MENU_MERGE_BRANCH,
      label = "i18n::GitPerspective.Menu.MergeInto.Text",
      image = "git-merge.svg")
  public void mergeBranch() {
    Ref ref = getSelectedReference();
    if (ref != null) {
      try {
        UIGit git = GitGuiPlugin.getInstance().getGit();
        String name = git.getShortenedName(ref.getName());
        git.mergeBranch(name, MergeStrategy.RECURSIVE);

        // Refresh the explorer file, refs and commit history
        refresh(true);
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.MergeError.Header"),
            BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.MergeError.Message"),
            e);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      id = REF_CONTEXT_MENU_FETCH,
      separator = true,
      label = "i18n::GitPerspective.Menu.Fetch.Text",
      image = "fetch.svg")
  @GuiToolbarElement(
      root = GitPerspective.GUI_PLUGIN_HISTORY_TOOLBAR_PARENT_ID,
      id = GitPerspective.TOOLBAR_ITEM_FETCH,
      separator = true,
      toolTip = "i18n::GitPerspective.History.Toolbar.Fetch.Tooltip",
      image = "fetch.svg")
  public void fetch() {
    try {
      GitGuiPlugin.getInstance().getGit().fetch();

      // Refresh refs and commit history
      refresh(false);
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.FetchError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.FetchError.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      id = REF_CONTEXT_MENU_PULL,
      label = "i18n::GitPerspective.Menu.Pull.Text",
      image = "pull.svg")
  @GuiToolbarElement(
      root = GitPerspective.GUI_PLUGIN_HISTORY_TOOLBAR_PARENT_ID,
      id = GitPerspective.TOOLBAR_ITEM_PULL,
      toolTip = "i18n::GitGuiPlugin.Toolbar.Pull.Tooltip",
      image = "pull.svg")
  @GuiKeyboardShortcut(control = true, key = 'T')
  @GuiOsxKeyboardShortcut(control = true, key = 'T')
  public void pull() {
    try {
      GitGuiPlugin.getInstance().getGit().pull();

      // Refresh the explorer file, refs and commit history
      refresh(true);
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PullError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PullError.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      id = REF_CONTEXT_MENU_PUSH,
      label = "i18n::GitPerspective.Menu.Push.Text",
      image = "push.svg")
  @GuiToolbarElement(
      root = GitPerspective.GUI_PLUGIN_HISTORY_TOOLBAR_PARENT_ID,
      id = GitPerspective.TOOLBAR_ITEM_PUSH,
      toolTip = "i18n::GitGuiPlugin.Toolbar.Push.Tooltip",
      image = "push.svg")
  public void push() {
    try {
      GitGuiPlugin.getInstance().getGit().push();

      // Refresh refs and commit history
      refresh(false);
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PushError.Header"),
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.PushError.Message"),
          e);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      id = REF_CONTEXT_MENU_RENAME,
      separator = true,
      label = "i18n::GitPerspective.Menu.Rename.Text",
      image = "ui/images/rename.svg")
  @GuiKeyboardShortcut(key = SWT.F2)
  @GuiOsxKeyboardShortcut(key = SWT.F2)
  public void renameReference() {
    Ref ref = this.getSelectedReference();
    if (ref != null) {
      UIGit git = GitGuiPlugin.getInstance().getGit();
      String oldName = git.getShortenedName(ref.getName());

      // The control that will be the editor must be a child of the Tree
      Text text = new Text(wRefTree, SWT.BORDER);
      text.setText(oldName);
      text.addListener(SWT.FocusOut, event -> text.dispose());
      text.addListener(
          SWT.KeyUp,
          event -> {
            switch (event.keyCode) {
              case SWT.CR, SWT.KEYPAD_CR:
                String newName = text.getText().trim();
                if (!Utils.isEmpty(newName) && !newName.equals(oldName)) {
                  if (git.renameBranch(oldName, newName)) {

                    // If we rename the active branch
                    if (newName.equals(git.getBranch())) {
                      GitGuiPlugin.getInstance().setBranchLabel(newName);
                    }

                    // Refresh refs and commit history
                    refresh(false);
                  }
                }
                text.dispose();

                break;
              case SWT.ESC:
                text.dispose();
                break;
              default:
                break;
            }
          });
      text.selectAll();
      text.setFocus();
      PropsUi.setLook(text);
      wRefTreeEditor.setEditor(text, wRefTree.getSelection()[0]);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_REF_CONTEXT_MENU_PARENT_ID,
      id = REF_CONTEXT_MENU_DELETE,
      label = "i18n::GitPerspective.Menu.Delete.Text",
      image = "ui/images/delete.svg")
  @GuiKeyboardShortcut(key = SWT.DEL)
  @GuiOsxKeyboardShortcut(key = SWT.DEL)
  public void deleteReference() {
    Ref ref = this.getSelectedReference();
    if (ref != null) {
      if (ref.getName().startsWith(Constants.R_HEADS)) {
        deleteBranch(ref);
      } else {
        deleteTag(ref);
      }
    }
  }

  protected void deleteBranch(Ref ref) {
    if (ref != null) {
      UIGit git = GitGuiPlugin.getInstance().getGit();
      String name = git.getShortenedName(ref.getName());

      // Prevent deletion of the current branch
      if (name.equals(git.getBranch())) {
        return;
      }

      MessageBox dialog = new MessageBox(getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      dialog.setText(
          BaseMessages.getString(
              PKG, "GitGuiPlugin.Dialog.Branch.DeleteBranchConfirmation.Header"));
      dialog.setMessage(
          BaseMessages.getString(
              PKG, "GitGuiPlugin.Dialog.Branch.DeleteBranchConfirmation.Message", name));
      if (dialog.open() == SWT.YES) {
        git.deleteBranch(name, true);

        // Refresh refs and commit history
        refresh(false);
      }
    }
  }

  protected void deleteTag(Ref ref) {
    if (ref != null) {
      UIGit git = GitGuiPlugin.getInstance().getGit();
      String name = git.getShortenedName(ref.getName());

      MessageBox dialog = new MessageBox(getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      dialog.setText(
          BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Tag.DeleteTagConfirmation.Header"));
      dialog.setMessage(
          BaseMessages.getString(
              PKG, "GitGuiPlugin.Dialog.Tag.DeleteTagConfirmation.Message", name));

      if (dialog.open() == SWT.YES) {
        git.deleteTag(name);

        // Refresh refs and commit history
        refresh(false);
      }
    }
  }

  /**
   * Sets the diff text in the appropriate widget (colored styled text for desktop, plain text for
   * web).
   */
  private void setDiffText(String text) {
    if (text == null) {
      text = "";
    }
    if (wDiffStyled != null) {
      // Desktop: Use colored diff.
      ((DiffStyledTextComp) wDiffStyled).setDiffText(text);
    } else if (wDiffText != null) {
      // Web: Use plain text
      wDiffText.setText(text);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_HISTORY_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SHOW_HIDDEN,
      separator = true,
      toolTip = "i18n::GitPerspective.Toolbar.ShowHiddenAllRef.Tooltip",
      image = "ui/images/hide.svg")
  public void showAllRef() {
    showAllRef = !showAllRef;

    // Update toolbar item icon
    ToolItem toolItem = this.historyToolBarWidgets.findToolItem(TOOLBAR_ITEM_SHOW_HIDDEN);
    if (toolItem != null) {
      if (showAllRef) {
        toolItem.setImage(GuiResource.getInstance().getImageShow());
      } else {
        toolItem.setImage(GuiResource.getInstance().getImageHide());
      }
    }

    // Save options
    storePerspectiveSettings();

    refresh();
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID,
      id = FILE_CONTEXT_MENU_SHOW_TEXT_DIFF,
      label = "i18n::GitPerspective.Menu.ShowTextDiff.Text",
      image = "diff-text.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_FILE_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_FILE_SHOW_TEXT_DIFF,
      toolTip = "i18n::GitPerspective.Toolbar.ShowTextDiff.Tooltip",
      image = "diff-text.svg")
  @GuiKeyboardShortcut(control = true, key = 'D')
  @GuiOsxKeyboardShortcut(control = true, key = 'D')
  public void showTextDiff() {
    RevCommit commit = getSelectedCommit();
    String fileName = getSelectedFile();

    if (fileName == null || commit == null) {
      return;
    }

    try {
      UIGit git = GitGuiPlugin.getInstance().getGit();
      String commitIdNew = commit.getId().name();
      String commitIdOld = git.getParentCommitId(commitIdNew);

      if (commitIdOld == null) {
        return; // No parent to compare to
      }
      if (commitIdNew.equals(commitIdOld)) {
        return; // No changes expected
      }

      GitGuiPlugin.getInstance().showTextFileDiff(fileName, commitIdNew, commitIdOld);
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), "Error", "Error while doing text diff on file : " + fileName, e);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_FILE_CONTEXT_MENU_PARENT_ID,
      id = FILE_CONTEXT_MENU_SHOW_GRAPH_DIFF,
      label = "i18n::GitPerspective.Menu.ShowGraphDiff.Text",
      image = "diff-graph.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_FILE_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_FILE_SHOW_GRAPH_DIFF,
      toolTip = "i18n::GitPerspective.Toolbar.ShowGraphDiff.Tooltip",
      image = "diff-graph.svg")
  @GuiKeyboardShortcut(control = true, shift = true, key = 'D')
  @GuiOsxKeyboardShortcut(control = true, shift = true, key = 'D')
  public void showGraphDiff() {
    RevCommit commit = getSelectedCommit();
    String fileName = getSelectedFile();

    if (fileName == null || commit == null) {
      return;
    }

    try {
      UIGit git = GitGuiPlugin.getInstance().getGit();
      String commitIdNew = commit.getId().name();
      String commitIdOld = git.getParentCommitId(commitIdNew);

      if (commitIdOld == null) {
        return; // No parent to compare to
      }
      if (commitIdNew.equals(commitIdOld)) {
        return; // No changes expected
      }

      ExplorerPerspective perspective = HopGui.getExplorerPerspective();
      if (perspective.getPipelineFileType().isHandledBy(fileName, false)) {
        // A pipeline
        GitGuiPlugin.getInstance().showPipelineFileDiff(fileName, commitIdNew, commitIdOld);
      } else if (perspective.getWorkflowFileType().isHandledBy(fileName, false)) {
        // A workflow
        GitGuiPlugin.getInstance().showWorkflowFileDiff(fileName, commitIdNew, commitIdOld);
      }

    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), "Error", "Error while doing visual diff on file : " + fileName, e);
    }
  }

  private void storePerspectiveSettings() {
    try {
      HopConfig.setGuiProperty(OPTION_SHOW_ALL_REF, showAllRef ? "Y" : "N");
      HopConfig.getInstance().saveToFile();
    } catch (HopException e) {
      // Ignore, already logged by saveToFile function
    }
  }

  private void restorePerspectiveSettings() {
    showAllRef = Const.toBoolean(HopConfig.getGuiProperty(OPTION_SHOW_ALL_REF));

    // Update toolbar item icon
    ToolItem toolItem = this.historyToolBarWidgets.findToolItem(TOOLBAR_ITEM_SHOW_HIDDEN);
    if (toolItem != null) {
      if (showAllRef) {
        toolItem.setImage(GuiResource.getInstance().getImageShow());
      } else {
        toolItem.setImage(GuiResource.getInstance().getImageHide());
      }
    }
  }

  public void updateGui() {
    UIGit git = GitGuiPlugin.getInstance().getGit();

    boolean isGitEnabled = git != null;
    boolean isCommitSelected = false;
    boolean isCommitInCurrentBranch = false;

    if (isGitEnabled) {
      RevCommit commit = getSelectedCommit();
      if (commit != null) {
        isCommitSelected = true;
        isCommitInCurrentBranch = isCommitInCurrentBranch(commit);
      }
    }

    wRefTree.setEnabled(isGitEnabled);
    wHistoryToolBar.setEnabled(isGitEnabled);
    wHistoryTable.setEnabled(isGitEnabled);
    wFileToolBar.setEnabled(isGitEnabled);
    wFileTree.setEnabled(isGitEnabled);

    historyToolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_CREATE_BRANCH, isCommitSelected);
    historyToolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_CREATE_TAG, isCommitSelected);
    historyToolBarWidgets.enableToolbarItem(
        TOOLBAR_ITEM_COMMIT_CHERRY_PICK, isCommitSelected && !isCommitInCurrentBranch);

    String selectFile = getSelectedFile();
    boolean isFileSelected = selectFile != null;
    fileToolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_FILE_SHOW_TEXT_DIFF, isFileSelected);
    fileToolBarWidgets.enableToolbarItem(
        TOOLBAR_ITEM_FILE_SHOW_GRAPH_DIFF, FileTypeUtils.isHopFileType(selectFile));
    fileToolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_FILE_REVERT, isFileSelected);
    fileToolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_FILE_CHERRY_PICK, isFileSelected);
  }

  protected void createHistoryTable(Composite parent) {

    Composite composite = new Composite(parent, SWT.BORDER);
    composite.setLayout(new FormLayout());
    composite.setLayoutData(FormDataBuilder.builder().fullSize().result());
    PropsUi.setLook(composite);

    // Create toolbar
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);

    historyToolBarWidgets = new GuiToolbarWidgets();
    historyToolBarWidgets.registerGuiPluginObject(this);
    historyToolBarWidgets.createToolbarWidgets(
        toolBarContainer, GUI_PLUGIN_HISTORY_TOOLBAR_PARENT_ID);

    wHistoryToolBar = toolBarContainer.getControl();
    wHistoryToolBar.setLayoutData(FormDataBuilder.builder().fullWidth().top().result());
    wHistoryToolBar.pack();
    PropsUi.setLook(wHistoryToolBar, Props.WIDGET_STYLE_TOOLBAR);

    wHistoryTable = new Table(composite, SWT.FULL_SELECTION);
    wHistoryTable.setHeaderVisible(true);
    wHistoryTable.setLinesVisible(false);
    wHistoryTable.setLayoutData(
        FormDataBuilder.builder().fullWidth().top(wHistoryToolBar).bottom().result());
    PropsUi.setLook(wHistoryTable);

    TableColumn graphColumn = new TableColumn(wHistoryTable, SWT.NONE);
    graphColumn.setText(BaseMessages.getString(PKG, "GitPerspective.History.ColumnGraph.Label"));
    graphColumn.setResizable(true);
    graphColumn.setWidth(300);
    TableColumn messageColumn = new TableColumn(wHistoryTable, SWT.NONE);
    messageColumn.setText(
        BaseMessages.getString(PKG, "GitPerspective.History.ColumnMessage.Label"));
    messageColumn.setWidth(800);
    TableColumn userColumn = new TableColumn(wHistoryTable, SWT.NONE);
    userColumn.setText(BaseMessages.getString(PKG, "GitPerspective.History.ColumnAuthor.Label"));
    userColumn.setWidth(200);
    TableColumn dateColumn = new TableColumn(wHistoryTable, SWT.NONE);
    dateColumn.setText(BaseMessages.getString(PKG, "GitPerspective.History.ColumnDate.Label"));
    dateColumn.setWidth(180);
    TableColumn commitColumn = new TableColumn(wHistoryTable, SWT.NONE);
    commitColumn.setText(BaseMessages.getString(PKG, "GitPerspective.History.ColumnCommit.Label"));
    commitColumn.setWidth(120);

    plotRenderer = new SwtCommitRenderer();

    wHistoryTable.addListener(SWT.Selection, this::selectCommit);

    // Create context menu
    Menu menu = new Menu(wHistoryTable);
    historyMenuWidgets = new GuiMenuWidgets();
    historyMenuWidgets.registerGuiPluginObject(this);
    historyMenuWidgets.createMenuWidgets(
        GUI_PLUGIN_HISTORY_CONTEXT_MENU_PARENT_ID, getShell(), menu);
    wHistoryTable.setMenu(menu);
    wHistoryTable.addListener(
        SWT.MenuDetect,
        event -> {
          RevCommit commit = getSelectedCommit();
          if (commit != null) {

            boolean isCommitInCurrentBranch = isCommitInCurrentBranch(commit);

            setMenuItemEnabled(
                historyMenuWidgets, HISTORY_CONTEXT_MENU_RESET, isCommitInCurrentBranch);

            // Revert commit with multiple parents isn't allowed
            setMenuItemEnabled(
                historyMenuWidgets,
                HISTORY_CONTEXT_MENU_REVERT_COMMIT,
                isCommitInCurrentBranch && commit.getParentCount() == 1);

            setMenuItemEnabled(
                historyMenuWidgets, HISTORY_CONTEXT_MENU_CHERRY_PICK, !isCommitInCurrentBranch);

          } else {
            event.doit = false;
          }
        });

    wHistoryTable.addListener(
        SWT.PaintItem,
        event -> {
          if (event.index > 0) return;

          // Enable antialiasing early
          if (enableAntialias) {
            try {
              event.gc.setAntialias(SWT.ON);
            } catch (SWTException e) {
              this.enableAntialias = false;
            }
          }

          plotRenderer.setCurrentBranch(GitGuiPlugin.getInstance().getGit().getBranch());
          Object data = event.item.getData();
          if (data == null) {
            plotRenderer.paintWorkingDirectory(event);
          } else {
            PlotCommit<SwtCommitList.Lane> commit = (PlotCommit<SwtCommitList.Lane>) data;
            plotRenderer.paintCommit(event, commit);
          }
        });
  }

  private void search() {
    if (wSearchText == null || wSearchText.isDisposed()) {
      return;
    }

    String search = wSearchText.getText().trim();
    if (plotRenderer != null) {
      plotRenderer.setFiltered(!Utils.isEmpty(search));
    }

    if (GitGuiPlugin.getInstance().getGit() == null) {
      return;
    }

    refreshHistory();
  }

  protected void createDiffView(Composite parent) {
    if (EnvironmentUtils.getInstance().isWeb()) {
      // Hop Web: Use plain Text widget
      wDiffText = new Text(parent, SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
      wDiffText.setEditable(false);
      PropsUi.setLook(wDiffText);

      wDiff = wDiffText;
    } else {
      // Desktop: Use DiffStyledTextComp for colored diff
      wDiffStyled =
          new DiffStyledTextComp(
              hopGui.getVariables(), parent, SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
      PropsUi.setLook(wDiffStyled, Props.WIDGET_STYLE_FIXED);

      wDiff = wDiffStyled;
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_HISTORY_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "i18n::GitPerspective.Toolbar.Refresh.Tooltip",
      image = "ui/images/refresh.svg")
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refresh() {
    refresh(false);
  }

  /**
   * Refreshes the Git perspective and optionally refreshing others perspectives.
   *
   * @param refreshAll A boolean flag indicating whether the file explorer and metadata perspective
   *     should be refreshed.
   */
  public void refresh(boolean refreshAll) {
    try {
      if (wHistoryTable == null || wHistoryTable.isDisposed()) {
        return;
      }

      UIGit uiGit = GitGuiPlugin.getInstance().getGit();

      // Drop UI state bound to the previous repository before reading selection / rebuilding.
      // Otherwise updateGui() would parse stale RevCommit ids against the newly opened repo
      // (MissingObjectException after project switch). See issue #7520.
      clearGitUiState();

      if (uiGit == null) {
        updateGui();
        return;
      }

      // Refresh the file explorer perspective (file tree, change colors...)
      // Refresh the metadata perspective (if file metadata provider)
      if (refreshAll) {
        ExplorerPerspective.getInstance().refresh();
        MetadataPerspective.getInstance().refresh();
      }

      refreshRef(uiGit.getGit());
      refreshHistory(Constants.HEAD);
      updateGui();
    } catch (Exception e) {
      LogChannel.UI.logError("Error refresh git history", e);
    }
  }

  /**
   * Clears ref/history/file widgets and the diff view. Must run before rebuilding after a
   * repository switch so no RevCommit/Ref from the previous repo remains selected.
   */
  protected void clearGitUiState() {
    if (wRefTree != null && !wRefTree.isDisposed()) {
      wRefTree.removeAll();
    }
    if (wHistoryTable != null && !wHistoryTable.isDisposed()) {
      wHistoryTable.removeAll();
    }
    if (wFileTree != null && !wFileTree.isDisposed()) {
      wFileTree.removeAll();
    }
    setDiffText(null);
  }

  protected void refreshRef(Git git) {
    try {
      wRefTree.setRedraw(false);
      wRefTree.removeAll();

      String currentBranch = GitGuiPlugin.getInstance().getGit().getBranch();

      // Local branches
      TreeItem localItem = new TreeItem(wRefTree, SWT.NONE);
      localItem.setText(BaseMessages.getString(PKG, "GitPerspective.Ref.Local.Label"));
      localItem.setImage(GitResource.getInstance().getLocalImage());
      for (Ref ref : git.branchList().call()) {
        if (ref.isSymbolic()) continue;
        String name = Repository.shortenRefName(ref.getName());
        TreeItem item = new TreeItem(localItem, SWT.NONE);
        item.setText(name);
        item.setImage(GitResource.getInstance().getBranchImage());
        item.setData(ref);

        if (name.equals(currentBranch)) {
          item.setImage(GuiResource.getInstance().getImageCheck());
          item.setFont(GuiResource.getInstance().getFontBold());
        }
      }

      // Remote branches
      TreeItem remoteItem = new TreeItem(wRefTree, SWT.NONE);
      remoteItem.setText(BaseMessages.getString(PKG, "GitPerspective.Ref.Remote.Label"));
      remoteItem.setImage(GitResource.getInstance().getRemoteImage());
      for (Ref ref : git.branchList().setListMode(ListBranchCommand.ListMode.REMOTE).call()) {
        if (ref.isSymbolic()) continue;
        TreeItem item = new TreeItem(remoteItem, SWT.NONE);
        item.setText(Repository.shortenRefName(ref.getName()));
        item.setImage(GitResource.getInstance().getBranchImage());
        item.setData(ref);
      }

      // Tags
      TreeItem tagsItem = new TreeItem(wRefTree, SWT.NONE);
      tagsItem.setText(BaseMessages.getString(PKG, "GitPerspective.Ref.Tags.Label"));
      tagsItem.setImage(GitResource.getInstance().getTagImage());
      for (Ref ref : git.tagList().call()) {
        TreeItem item = new TreeItem(tagsItem, SWT.NONE);
        item.setText(Repository.shortenRefName(ref.getName()));
        item.setImage(GitResource.getInstance().getTagImage());
        item.setData(ref);
      }

      TreeMemory.setExpandedFromMemory(wRefTree, GIT_PERSPECTIVE_REF_TREE);

      wRefTree.setRedraw(true);
    } catch (Exception e) {
      LogChannel.UI.logError("Error refresh git commit history", e);
    }
  }

  public static class VirtualPlotCommit extends PlotCommit<SwtCommitList.Lane> {
    private static final PersonIdent AUTHOR = new PersonIdent("Index", "staged@changes");

    public VirtualPlotCommit(AnyObjectId id, RevCommit headCommit) {
      super(id);

      this.parents = new RevCommit[1];
      this.parents[0] = headCommit;

      if (headCommit != null) {
        this.parents = new RevCommit[1];
        this.parents[0] = headCommit;
        // this.lane = headCommit.lane;

        try {
          Field field = PlotCommit.class.getDeclaredField("lane");
          field.setAccessible(true);
          field.set(this, new SwtCommitList.Lane(GuiResource.getInstance().getColorGray()));

        } catch (Exception e) {
          // Ignore
        }
      }
    }
  }

  /** Refresh git commit history */
  protected void refreshHistory() {
    refreshHistory(null);
  }

  protected void refreshHistory(String startRef) {
    Git git = GitGuiPlugin.getInstance().getGit().getGit();

    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm").withZone(ZoneId.systemDefault());

    try {
      Repository repository = git.getRepository();
      PlotWalk walk = new PlotWalk(repository);

      ObjectId startCommitId = null;
      if (!Utils.isEmpty(startRef)) {
        startCommitId = repository.resolve(startRef);
      }

      List<RevCommit> marks = new ArrayList<>();

      // Always show HEAD
      marks.add(walk.parseCommit(repository.resolve(Constants.HEAD)));

      // Show add all branches and tags
      if (showAllRef) {
        for (Ref ref : git.branchList().call()) {
          RevCommit commit = walk.parseCommit(ref.getObjectId());
          if (!marks.contains(commit)) {
            marks.add(commit);
          }
        }
      } else if (startCommitId != null) {
        marks.add(walk.parseCommit(startCommitId));
      }

      walk.markStart(marks);

      SwtCommitList plotList = new SwtCommitList();
      plotList.source(walk);
      plotList.fillTo(Integer.MAX_VALUE);

      wHistoryTable.setRedraw(false);
      wHistoryTable.removeAll();

      // Check if there are changes in the working directory
      Map<String, UIFile> filesToCommit = GitGuiPlugin.getInstance().getChangedFiles();
      if (!filesToCommit.isEmpty()) {
        TableItem item = new TableItem(wHistoryTable, SWT.NONE);
        item.setText(
            1,
            BaseMessages.getString(
                PKG, "GitPerspective.History.UncommittedChanges.Label", filesToCommit.size()));
        item.setForeground(GuiResource.getInstance().getColorGray());
      }

      String search = wSearchText.getText().toLowerCase().trim();

      TableItem selectedItem = null;
      for (PlotCommit<SwtCommitList.Lane> commit : plotList) {
        String message = commit.getShortMessage();
        String author = commit.getAuthorIdent().getName();
        String id = commit.getId().abbreviate(Constants.OBJECT_ID_ABBREV_STRING_LENGTH).name();

        // Apply filter if the search text is not empty
        if (!search.isEmpty()
            && !message.toLowerCase().contains(search)
            && !author.toLowerCase().contains(search)
            && !id.toLowerCase().contains(search)) {
          continue;
        }

        TableItem item = new TableItem(wHistoryTable, SWT.NONE);
        item.setText(1, message);
        item.setText(2, author);
        item.setText(
            3,
            formatter
                .withZone(commit.getCommitterIdent().getZoneId())
                .format(commit.getCommitterIdent().getWhenAsInstant()));
        item.setText(4, id);
        item.setData(commit);

        if (commit.getId().equals(startCommitId)) {
          selectedItem = item;
        }
      }

      if (selectedItem != null) {
        wHistoryTable.setSelection(selectedItem);
      }

      wHistoryTable.setRedraw(true);
      wHistoryTable.redraw();

      refreshFiles();
    } catch (Exception e) {
      LogChannel.UI.logError("Error refresh git history", e);
    }
  }

  protected Shell getShell() {
    return hopGui.getShell();
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
