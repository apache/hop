/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IEngineMeta;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.git.model.IVCS;
import org.apache.hop.git.model.UIFile;
import org.apache.hop.git.model.UIGit;
import org.apache.hop.git.model.repository.GitRepository;
import org.apache.hop.git.model.revision.ObjectRevision;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

@HopPerspectivePlugin(
    id = "400-HopGitPerspective",
    name = "Git",
    image = "git_icon.svg",
    description = "The git perspective"
)
@GuiPlugin
public class HopGitPerspective implements IHopPerspective {

  private static final Class<?> PKG = HopGitPerspective.class; // For Translator

  public static final String ID_PERSPECTIVE_TOOLBAR_ITEM = "20040-perspective-git";

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiGitPerspective-Toolbar";

  public static final String TOOLBAR_ITEM_REPOSITORY_LABEL =
      "HopGitPlugin-ToolBar-10000-git-repository-label";
  public static final String TOOLBAR_ITEM_REPOSITORY_SELECT =
      "HopGitPlugin-ToolBar-10010-git-repository-select";
  public static final String TOOLBAR_ITEM_REPOSITORY_EDIT =
      "HopGitPlugin-ToolBar-10020-git-repository-edit";
  public static final String TOOLBAR_ITEM_REPOSITORY_ADD =
      "HopGitPlugin-ToolBar-10030-git-repository-add";
  public static final String TOOLBAR_ITEM_REPOSITORY_DELETE =
      "HopGitPlugin-ToolBar-10040-git-repository-delete";
  public static final String TOOLBAR_ITEM_PULL = "HopGitPlugin-ToolBar-10100-git-pull";
  public static final String TOOLBAR_ITEM_PUSH = "HopGitPlugin-ToolBar-10110-git-push";
  public static final String TOOLBAR_ITEM_BRANCH = "HopGitPlugin-ToolBar-10120-git-branch";
  public static final String TOOLBAR_ITEM_TAG = "HopGitPlugin-ToolBar-10130-git-tag";
  public static final String TOOLBAR_ITEM_REFRESH = "HopGitPlugin-ToolBar-10140-git-refresh";

  public static final String GUI_PLUGIN_FILES_TOOLBAR_PARENT_ID =
      "HopGuiGitPerspective-FilesToolbar";
  public static final String FILES_TOOLBAR_ITEM_FILES_OPEN =
      "HopGitPlugin-FilesToolBar-10000-files-open";
  public static final String FILES_TOOLBAR_ITEM_FILES_COMMIT =
      "HopGitPlugin-FilesToolBar-10010-files-commit";
  public static final String FILES_TOOLBAR_ITEM_FILES_STAGE =
      "HopGitPlugin-FilesToolBar-10020-files-stage";
  public static final String FILES_TOOLBAR_ITEM_FILES_UNSTAGE =
      "HopGitPlugin-FilesToolBar-10030-files-unstage";
  public static final String FILES_TOOLBAR_ITEM_FILES_DISCARD =
      "HopGitPlugin-FilesToolBar-10040-files-discard";

  public static final String AUDIT_TYPE = "GitRepository";

  private HopGui hopGui;
  private Composite parent;
  private Composite perspectiveComposite;
  private FormData formData;

  private Button commitButton;
  private Button pullButton;
  private Button pushButton;
  private Button branchButton;
  private Button tagButton;

  private Text authorNameTextbox;
  private Text commitMessageTextbox;

  private TableView revisionTable;
  private Table changedTable;

  private static HopGitPerspective instance;
  private ToolBar toolBar;
  private GuiToolbarWidgets toolBarWidgets;
  private SashForm verticalSash;
  private SashForm horizontalSash;
  private Text diffText;
  private Composite rightComposite;

  private ToolItem gitPerspectiveToolbarItem;

  private Image imageGit;
  private Image imageAdded;
  private Image imageChanged;
  private Image imageRemoved;

  private IVCS vcs;
  private List<ObjectRevision> revisions;
  private List<UIFile> changedFiles;
  private PropsUi props;
  private Label wlCommitMessageTextbox;

  private ToolBar filesToolBar;
  private GuiToolbarWidgets filesToolBarWidgets;

  public HopGitPerspective() {
    gitPerspectiveToolbarItem = null;
    revisions = Collections.emptyList();
    changedFiles = Collections.emptyList();
  }

  public static HopGitPerspective getInstance() {
    return (HopGitPerspective)
        HopGui.getInstance().getPerspectiveManager().findPerspective(HopGitPerspective.class);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;
    this.parent = parent;

    props = PropsUi.getInstance();

    perspectiveComposite = new Composite(parent, SWT.NONE);
    perspectiveComposite.setLayout(new FormLayout());

    formData = new FormData();
    formData.left = new FormAttachment(0, 0);
    formData.top = new FormAttachment(0, 0);
    formData.right = new FormAttachment(100, 0);
    formData.bottom = new FormAttachment(100, 0);
    perspectiveComposite.setLayoutData(formData);

    // Add the various components...
    // At the very top we have a series of buttons in the toolbar
    //
    // The composite is split into 2 parts: Left and Right
    // On the left you have a top and bottom composite:
    // - Left Top: revision table
    // - Left Bottom: File text diff

    // On the right you have from top to bottom
    // - changed files table with checkboxes in the first column
    // - A commit message text box (multi)
    // - The author (line)
    // - A commit button
    //
    // Create a new toolbar at the top of the main composite...
    //
    toolBar = new ToolBar(perspectiveComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment(0, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    fdToolBar.right = new FormAttachment(100, 0);
    toolBar.setLayoutData(fdToolBar);
    props.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBar, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    toolBar.pack();

    // The screen is split horizontally with a vertical sash as a result
    //
    verticalSash = new SashForm(perspectiveComposite, SWT.HORIZONTAL);
    FormData fdVerticalSash = new FormData();
    fdVerticalSash.left = new FormAttachment(0, 0);
    fdVerticalSash.top = new FormAttachment(0, 0);
    fdVerticalSash.right = new FormAttachment(100, 0);
    fdVerticalSash.bottom = new FormAttachment(100, 0);
    verticalSash.setLayoutData(fdVerticalSash);

    // On the left hand side we have another sash
    //
    horizontalSash = new SashForm(verticalSash, SWT.VERTICAL);
    FormData fdHorizontalSash = new FormData();
    fdHorizontalSash.left = new FormAttachment(0, 0);
    fdHorizontalSash.top = new FormAttachment(0, 0);
    fdHorizontalSash.right = new FormAttachment(100, 0);
    fdHorizontalSash.bottom = new FormAttachment(100, 0);
    horizontalSash.setLayoutData(fdHorizontalSash);

    // This sash has a top and bottom section: the revision table and the diff text
    //
    ColumnInfo[] columns = {
      new ColumnInfo("ID", ColumnInfo.COLUMN_TYPE_TEXT, false, true),
      new ColumnInfo("Message", ColumnInfo.COLUMN_TYPE_TEXT, false, true),
      new ColumnInfo("Author", ColumnInfo.COLUMN_TYPE_TEXT, false, true),
      new ColumnInfo("Date", ColumnInfo.COLUMN_TYPE_TEXT, false, true),
    };
    revisionTable =
        new TableView(
            hopGui.getVariables(), horizontalSash, SWT.SINGLE, columns, 1, true, null, props);
    FormData fdRevisionTable = new FormData();
    fdRevisionTable.left = new FormAttachment(0, 0);
    fdRevisionTable.top = new FormAttachment(0, 0);
    fdRevisionTable.right = new FormAttachment(100, 0);
    fdRevisionTable.bottom = new FormAttachment(100, 0);
    revisionTable.setLayoutData(fdRevisionTable);
    revisionTable.table.addListener(SWT.Selection, e -> refreshChangedTable());

    diffText = new Text(horizontalSash, SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    FormData fdDiffText = new FormData();
    fdDiffText.left = new FormAttachment(0, 0);
    fdDiffText.top = new FormAttachment(0, 0);
    fdDiffText.right = new FormAttachment(100, 0);
    fdDiffText.bottom = new FormAttachment(100, 0);
    diffText.setLayoutData(fdDiffText);

    horizontalSash.setWeights(new int[] {50, 50});

    // On the right side of the vertical sash we have the author at the bottom, a commit message
    // above it and
    // the changed files above it...
    //
    rightComposite = new Composite(verticalSash, SWT.NO_BACKGROUND);
    rightComposite.setLayout(new FormLayout());

    // The commit button at the very bottom
    //
    commitButton = new Button(rightComposite, SWT.PUSH);
    commitButton.setText("Commit");
    commitButton.setFont(GuiResource.getInstance().getFontBold());
    FormData fdCommitButton = new FormData();
    fdCommitButton.left = new FormAttachment(0, 0);
    fdCommitButton.bottom = new FormAttachment(100, 0);
    fdCommitButton.right = new FormAttachment(100, 0);
    commitButton.setLayoutData(fdCommitButton);
    commitButton.addListener(SWT.Selection, e -> commit());

    // The author just above the commit button
    //
    Label wlAuthorNameTextbox = new Label(rightComposite, SWT.SINGLE);
    wlAuthorNameTextbox.setText("Author: ");
    FormData fdlAuthorNameTextbox = new FormData();
    fdlAuthorNameTextbox.left = new FormAttachment(0, 0);
    fdlAuthorNameTextbox.bottom = new FormAttachment(commitButton, -props.getMargin());
    wlAuthorNameTextbox.setLayoutData(fdlAuthorNameTextbox);

    authorNameTextbox = new Text(rightComposite, SWT.SINGLE);
    FormData fdAuthorNameTextbox = new FormData();
    fdAuthorNameTextbox.left = new FormAttachment(wlAuthorNameTextbox, props.getMargin());
    fdAuthorNameTextbox.right = new FormAttachment(100, 0);
    fdAuthorNameTextbox.bottom = new FormAttachment(commitButton, -props.getMargin());
    authorNameTextbox.setLayoutData(fdAuthorNameTextbox);

    // Above the author line we have a commit message area.
    //
    commitMessageTextbox =
        new Text(rightComposite, SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER);
    FormData fdCommitMessageTextbox = new FormData();
    fdCommitMessageTextbox.left = new FormAttachment(0, 0);
    fdCommitMessageTextbox.right = new FormAttachment(100, 0);
    fdCommitMessageTextbox.top =
        new FormAttachment(authorNameTextbox, (int) (-props.getZoomFactor() * 150));
    fdCommitMessageTextbox.bottom = new FormAttachment(authorNameTextbox, -props.getMargin());
    commitMessageTextbox.setLayoutData(fdCommitMessageTextbox);

    wlCommitMessageTextbox = new Label(rightComposite, SWT.SINGLE);
    wlCommitMessageTextbox.setText("Commit Message:");
    FormData fdlCommitMessageTextbox = new FormData();
    fdlCommitMessageTextbox.left = new FormAttachment(0, 0);
    fdlCommitMessageTextbox.right = new FormAttachment(100, 0);
    fdlCommitMessageTextbox.bottom = new FormAttachment(commitMessageTextbox, -props.getMargin());
    wlCommitMessageTextbox.setLayoutData(fdlCommitMessageTextbox);

    // At the very top we have a toolbar...
    //
    filesToolBar = new ToolBar(rightComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    FormData fdFilesToolBar = new FormData();
    fdFilesToolBar.left = new FormAttachment(0, 0);
    fdFilesToolBar.top = new FormAttachment(0, 0);
    fdFilesToolBar.right = new FormAttachment(100, 0);
    filesToolBar.setLayoutData(fdFilesToolBar);
    props.setLook(filesToolBar, Props.WIDGET_STYLE_TOOLBAR);

    filesToolBarWidgets = new GuiToolbarWidgets();
    filesToolBarWidgets.registerGuiPluginObject(this);
    filesToolBarWidgets.createToolbarWidgets(filesToolBar, GUI_PLUGIN_FILES_TOOLBAR_PARENT_ID);
    filesToolBar.pack();

    // The rest of the height is for the changed files table...
    //
    addChangedTable(true);

    rightComposite.layout(true, true);

    verticalSash.setWeights(new int[] {50, 50});
  }

  private void addChangedTable(boolean withChecks) {
    if (changedTable != null) {
      changedTable.dispose();
    }
    changedTable =
        new Table(
            rightComposite,
            SWT.BORDER
                | (withChecks ? SWT.CHECK : SWT.NONE)
                | SWT.V_SCROLL
                | SWT.H_SCROLL
                | SWT.MULTI);
    changedTable.setLinesVisible(true);
    changedTable.setHeaderVisible(true);
    TableColumn checkColumn = new TableColumn(changedTable, SWT.CHECK);
    checkColumn.setText("Selection");
    checkColumn.setWidth((int) (75 * props.getZoomFactor()));
    TableColumn statusColumn = new TableColumn(changedTable, SWT.CENTER);
    statusColumn.setText("Operation");
    statusColumn.setWidth((int) (100 * props.getZoomFactor()));
    TableColumn stagedColumn = new TableColumn(changedTable, SWT.CENTER);
    stagedColumn.setText("Staged?");
    stagedColumn.setWidth((int) (100 * props.getZoomFactor()));
    TableColumn fileColumn = new TableColumn(changedTable, SWT.LEFT);
    fileColumn.setText("Changed files");
    fileColumn.setWidth((int) (500 * props.getZoomFactor()));

    FormData fdChangedTable = new FormData();
    fdChangedTable.left = new FormAttachment(0, 0);
    fdChangedTable.right = new FormAttachment(100, 0);
    fdChangedTable.top = new FormAttachment(0, filesToolBar.getSize().y);
    fdChangedTable.bottom = new FormAttachment(wlCommitMessageTextbox, -props.getMargin());
    changedTable.setLayoutData(fdChangedTable);
    changedTable.addListener(SWT.Selection, this::showDiffText);
    rightComposite.layout(true, true);
  }

  private void showDiffText(Event event) {
    String diff;
    try {
      List<UIFile> selectedFiles = getSelectedChangedFiles();
      if (selectedFiles.size() != 0) {
        if (isOnlyWIP()) {
          if (selectedFiles.get(0).getIsStaged()) {
            diff = vcs.diff(Constants.HEAD, IVCS.INDEX, selectedFiles.get(0).getName());
          } else {
            diff = vcs.diff(IVCS.INDEX, IVCS.WORKINGTREE, selectedFiles.get(0).getName());
          }
        } else {
          String newCommitId = revisions.get(revisionTable.getSelectionIndex()).getRevisionId();
          String oldCommitId = vcs.getParentCommitId(newCommitId);
          diff = vcs.diff(oldCommitId, newCommitId, selectedFiles.get(0).getName());
        }
      } else {
        diff = "";
      }
    } catch (Exception e) {
      diff = Const.getStackTracker(e);
    }
    diffText.setText(Const.NVL(diff, ""));
  }

  private List<UIFile> getSelectedChangedFiles() {
    boolean checked = (changedTable.getStyle() & SWT.CHECK) != 0;
    List<UIFile> list = new ArrayList<>();
    if (checked) {
      for (int i = 0; i < changedTable.getItemCount(); i++) {
        if (changedTable.getItem(i).getChecked()) {
          list.add(changedFiles.get(i));
        }
      }
    } else {
      for (int index : changedTable.getSelectionIndices()) {
        list.add(changedFiles.get(index));
      }
    }
    return list;
  }

  ObjectRevision getFirstSelectedRevision() {
    if (revisions.isEmpty()) {
      return null;
    }
    int selectionIndex = revisionTable.table.getSelectionIndex();
    if (selectionIndex < 0) {
      return revisions.get(0);
    } else {
      return revisions.get(selectionIndex);
    }
  }

  public String getCommitMessage() {
    return commitMessageTextbox.getText();
  }

  public String getAuthorName() {
    return authorNameTextbox.getText();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_FILES_TOOLBAR_PARENT_ID,
      id = FILES_TOOLBAR_ITEM_FILES_COMMIT,
      label = "Commit",
      toolTip = "Commit the staged files",
      image = "git-commit.svg")
  public void commit() {
    if (!vcs.hasStagedFiles()) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), "There are no staged files");
      return;
    }
    if (StringUtils.isEmpty(getCommitMessage())) {
      showMessageBox(
          BaseMessages.getString(PKG, "Dialog.Error"),
          "Aborting commit due to empty commit message");
      return;
    }

    try {
      if (vcs.commit(getAuthorName(), getCommitMessage())) {
        commitMessageTextbox.setText("");
      }
    } catch (NullPointerException e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), "Malformed author name");
    } catch (Exception e) {
      showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
    }
  }

  public void activate() {
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {}

  private Combo getRepositoryCombo() {
    Control control = toolBarWidgets.getWidgetsMap().get(TOOLBAR_ITEM_REPOSITORY_SELECT);
    if ((control != null) && (control instanceof Combo)) {
      Combo combo = (Combo) control;
      return combo;
    }
    return null;
  }

  private HopGitPerspective findActiveInstance() {
    return (HopGitPerspective)
        HopGui.getInstance().getPerspectiveManager().findPerspective(HopGitPerspective.class);
  }

  public void refreshGitRepositoriesList() {
    HopGitPerspective perspective = findActiveInstance();
    if (perspective != null) {
      perspective.toolBarWidgets.refreshComboItemList(TOOLBAR_ITEM_REPOSITORY_SELECT);
    }
  }

  public void selectRepositoryInList(String name) {
    HopGitPerspective perspective = findActiveInstance();
    if (perspective != null) {
      perspective.toolBarWidgets.selectComboItem(TOOLBAR_ITEM_REPOSITORY_SELECT, name);
    }
  }

  public List<String> getGitRepositoryNames(ILogChannel log, IHopMetadataProvider metadataProvider)
      throws Exception {
    List<String> names = metadataProvider.getSerializer(GitRepository.class).listObjectNames();
    Collections.sort(names);
    return names;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REPOSITORY_LABEL,
      type = GuiToolbarElementType.LABEL,
      label = "Git repository ",
      toolTip = "Click here to edit the active git repository",
      separator = true)
  public void editGitRepository() {
    HopGui hopGui = HopGui.getInstance();
    Combo combo = getRepositoryCombo();
    if (combo == null) {
      return;
    }
    String repositoryName = combo.getText();
    try {
      MetadataManager<GitRepository> manager =
          new MetadataManager<>(
              hopGui.getVariables(), hopGui.getMetadataProvider(), GitRepository.class);
      if (manager.editMetadata(repositoryName)) {
        refreshGitRepositoriesList();
        selectRepositoryInList(repositoryName);
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), "Error", "Error editing environment '" + repositoryName, e);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REPOSITORY_SELECT,
      toolTip = "Select a git repository",
      type = GuiToolbarElementType.COMBO,
      comboValuesMethod = "getGitRepositoryNames",
      extraWidth = 200)
  public void selectRepository() {
    Combo combo = getRepositoryCombo();
    if (combo == null) {
      return;
    }
    String repositoryName = combo.getText();
    if (StringUtils.isEmpty(repositoryName)) {
      return;
    }
    loadRepository(repositoryName);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REPOSITORY_EDIT,
      toolTip = "Edit the selected git repository",
      image = "git-edit.svg")
  public void editSelectedRepository() {
    editGitRepository();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REPOSITORY_ADD,
      toolTip = "Add a new git repository",
      image = "git-add.svg")
  public void addNewRepository() {
    MetadataManager<GitRepository> manager =
        new MetadataManager<>(
            hopGui.getVariables(), hopGui.getMetadataProvider(), GitRepository.class);
    GitRepository gitRepository = manager.newMetadata();
    if (gitRepository != null) {
      refreshGitRepositoriesList();
      selectRepositoryInList(gitRepository.getName());
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REPOSITORY_DELETE,
      toolTip = "Delete the selected git repository",
      image = "git-delete.svg")
  public void deleteSelectedRepository() {
    MetadataManager<GitRepository> manager =
        new MetadataManager<>(
            hopGui.getVariables(), hopGui.getMetadataProvider(), GitRepository.class);
    if (manager.deleteMetadata()) {
      refreshGitRepositoriesList();
    }
  }

  /** Clear all the data from the git repository perspective */
  public void clearRepository() {
    vcs = null;
    revisions = Collections.emptyList();
    revisionTable.table.removeAll();
    changedTable.removeAll();
    diffText.setText("");
    commitMessageTextbox.setText("");
    authorNameTextbox.setText("");
  }

  public void loadRepository(String repositoryName) {
    try {
      GitRepository repo =
          hopGui.getMetadataProvider().getSerializer(GitRepository.class).load(repositoryName);

      if (repo == null) {
        // deleted or moved
        return;
      }

      String baseDirectory = repo.getPhysicalDirectory(hopGui.getVariables() );

      try {
        vcs = new UIGit();
        vcs.setShell(hopGui.getShell());
        vcs.openRepo(baseDirectory);
      } catch (RepositoryNotFoundException e) {
        initGit(baseDirectory);
      } catch (Exception e) {
        e.printStackTrace();
      }

      setActive();
      refreshContents();

      // Set the combo box
      //
      selectRepositoryInList(repositoryName);

      // Remember this
      //
      AuditManagerGuiUtil.addLastUsedValue(AUDIT_TYPE, repositoryName);

    } catch (Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error loading git repository", e);
    }
  }

  private void refreshContents() {
    // Populate the revisions
    //
    revisions = vcs.getRevisions();

    revisionTable.clearAll();
    for (ObjectRevision revision : revisions) {
      TableItem item = new TableItem(revisionTable.table, SWT.NONE);
      item.setText(1, Const.NVL(revision.getRevisionId(), ""));
      item.setText(2, Const.NVL(revision.getComment(), ""));
      item.setText(3, Const.NVL(revision.getLogin(), ""));
      item.setText(4, formatDate(revision.getCreationDate()));
    }
    revisionTable.removeEmptyRows();
    revisionTable.setRowNums();
    revisionTable.optWidth(true);

    // Select the working tree, the first item...
    //
    revisionTable.setSelection(new int[] {0});

    // Refresh the changed files...
    //
    refreshChangedTable();
  }

  private void refreshChangedTable() {
    changedTable.removeAll();

    changedFiles = new ArrayList<>();
    boolean allowChecking;
    if (isOnlyWIP()) {
      // Work in progress, not a committed revision
      //
      authorNameTextbox.setText(Const.NVL(vcs.getAuthorName(IVCS.WORKINGTREE), ""));
      commitMessageTextbox.setText(Const.NVL(vcs.getCommitMessage(IVCS.WORKINGTREE), ""));
      changedFiles.addAll(vcs.getUnstagedFiles());
      changedFiles.addAll(vcs.getStagedFiles());
      allowChecking = true;
    } else {
      String commitId = revisions.get(revisionTable.getSelectionIndex()).getRevisionId();
      authorNameTextbox.setText(Const.NVL(vcs.getAuthorName(commitId), ""));
      commitMessageTextbox.setText(Const.NVL(vcs.getCommitMessage(commitId), ""));
      changedFiles.addAll(vcs.getStagedFiles(vcs.getParentCommitId(commitId), commitId));
      allowChecking = false;
    }

    addChangedTable(allowChecking);
    for (UIFile changedFile : changedFiles) {
      TableItem item = new TableItem(changedTable, SWT.NONE);
      item.setChecked(false);
      switch (changedFile.getChangeType()) {
        case MODIFY:
          item.setImage(1, imageChanged);
          break;
        case ADD:
          item.setImage(1, imageAdded);
          break;
        case DELETE:
          item.setImage(1, imageRemoved);
          break;
        default:
          item.setText(1, changedFile.getChangeType().name());
          break;
      }
      item.setText(2, changedFile.getIsStaged() ? "Staged" : "");
      item.setText(3, changedFile.getName());
    }
  }

  private String formatDate(Date creationDate) {
    if (creationDate == null) {
      return "";
    }
    return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(creationDate);
  }

  Boolean isOnlyWIP() {
    int selectionIndex = revisionTable.getSelectionIndex();
    return (selectionIndex < 0)
        || (revisions.get(selectionIndex).getRevisionId().equals(IVCS.WORKINGTREE));
  }

  public boolean isOpen() {
    return vcs != null;
  }

  void initGit(final String baseDirectory) {
    MessageBox confirmBox = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    confirmBox.setText("Repository not found");
    confirmBox.setMessage("Create a new repository in the following path?\n" + baseDirectory);
    int answer = confirmBox.open();
    if ((answer & SWT.YES) != 0) {
      try {
        vcs.initRepo(baseDirectory);
        showMessageBox(
            BaseMessages.getString(PKG, "Dialog.Success"),
            BaseMessages.getString(PKG, "Dialog.Success"));
      } catch (Exception e) {
        new ErrorDialog(hopGui.getShell(), "Error", BaseMessages.getString(PKG, "Dialog.Error"), e);
      }
    }
  }

  // TODO: enable/disable icon toolbars
  //
  public void setActive() {

    commitButton.setEnabled(vcs != null);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_PULL, vcs != null);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_PUSH, vcs != null);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_BRANCH, vcs != null);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_TAG, vcs != null);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_REFRESH, vcs != null);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PULL,
      toolTip = "Pull",
      image = "pull.svg",
      separator = true)
  public void pull() {
    if (vcs != null) {
      if (vcs.pull()) {
        refresh();
        ;
      }
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PUSH,
      toolTip = "Push",
      image = "push.svg")
  public void push() {
    push("default");
  }

  public void push(String type) {
    if (vcs != null) {
      vcs.push(type);
    }
  }

  /*
  TODO : add branch and tag options

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_BRANCH,
    toolTip = "Branch",
    image = "branch.svg"
  )
  public void branch() {

  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_TAG,
    toolTip = "Tag",
    image = "tag.svg"
  )
  public void tag() {

  }

   */

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "Refresh",
      image = "ui/images/refresh.svg",
      separator = true)
  public void refresh() {
    refreshContents();
  }

  @Override
  public String getId() {
    return "git";
  }

  @Override
  public IHopFileTypeHandler getActiveFileTypeHandler() {
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public void setActiveFileTypeHandler(IHopFileTypeHandler activeFileTypeHandler) {}

  @Override
  public List<IHopFileType> getSupportedHopFileTypes() {
    return Collections.emptyList();
  }

  private void getPerspectiveToolbarImages() {
    int iconSize = (int) (ConstUi.SMALL_ICON_SIZE * props.getZoomFactor());

    imageGit =
        SwtSvgImageUtil.getImage(
            hopGui.getDisplay(), getClass().getClassLoader(), "git_icon.svg", iconSize, iconSize);
    imageAdded =
        SwtSvgImageUtil.getImage(
            hopGui.getDisplay(), getClass().getClassLoader(), "added.svg", iconSize, iconSize);
    imageRemoved =
        SwtSvgImageUtil.getImage(
            hopGui.getDisplay(), getClass().getClassLoader(), "removed.svg", iconSize, iconSize);
    imageChanged =
        SwtSvgImageUtil.getImage(
            hopGui.getDisplay(), getClass().getClassLoader(), "changed.svg", iconSize, iconSize);
  }

  @Override
  public void navigateToPreviousFile() {}

  @Override
  public void navigateToNextFile() {}

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
  }

  @Override
  public boolean hasNavigationPreviousFile() {
    return false;
  }

  @Override
  public boolean hasNavigationNextFile() {
    return false;
  }

  @Override
  public Control getControl() {
    return perspectiveComposite;
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
    return false;
  }

  @Override
  public List<TabItemHandler> getItems() {
    return null;
  }

  @Override
  public List<ISearchable> getSearchables() {
    return Collections.emptyList();
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return null;
  }

  private void showMessageBox(String title, String message) {
    MessageBox messageBox = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
    messageBox.setText(title);
    messageBox.setMessage(message);
    messageBox.open();
  }

  /**
   * Gets git image
   *
   * @return image
   */
  public Image getGitImage() {
    return imageGit;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_FILES_TOOLBAR_PARENT_ID,
      id = FILES_TOOLBAR_ITEM_FILES_STAGE,
      label = "Stage",
      toolTip = "Stage the selected changed files (add to index)")
  public void stage() {
    List<UIFile> contents = getSelectedChangedFiles();
    for (UIFile content : contents) {
      if (content.getChangeType() == DiffEntry.ChangeType.DELETE) {
        vcs.rm(content.getName());
      } else {
        vcs.add(content.getName());
      }
    }
    refresh();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_FILES_TOOLBAR_PARENT_ID,
      id = FILES_TOOLBAR_ITEM_FILES_UNSTAGE,
      label = "Unstage",
      toolTip = "Unstage the selected changed files (remove from index)")
  public void unstage() throws Exception {
    List<UIFile> contents = getSelectedChangedFiles();
    for (UIFile content : contents) {
      vcs.resetPath(content.getName());
    }
    refreshContents();
  }

  /**
   * Discard changes to selected unstaged files. Equivalent to <tt>git checkout --
   * &lt;paths&gt;</tt>
   *
   * @throws Exception
   */
  @GuiToolbarElement(
      root = GUI_PLUGIN_FILES_TOOLBAR_PARENT_ID,
      id = FILES_TOOLBAR_ITEM_FILES_DISCARD,
      label = "Discard",
      toolTip = "Discard changes to the selected files")
  public void discard() throws Exception {
    MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText("Confirm");
    box.setMessage(
        "Are you sure you want to discard changes to the selected "
            + getSelectedChangedFiles().size()
            + " files?");
    int answer = box.open();
    if ((answer & SWT.YES) == 0) {
      return;
    }

    List<UIFile> contents = getSelectedChangedFiles();
    for (UIFile content : contents) {
      vcs.revertPath(content.getName());
    }
    refresh();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_FILES_TOOLBAR_PARENT_ID,
      id = FILES_TOOLBAR_ITEM_FILES_OPEN,
      toolTip = "Open the selected files",
      image = "ui/images/open.svg")
  public void open() {
    MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText("Confirm");
    box.setMessage(
        "Do you want to open the selected "
            + getSelectedChangedFiles().size()
            + " files in the data orchestration perspective?");
    int answer = box.open();
    if ((answer & SWT.YES) == 0) {
      return;
    }
    HopDataOrchestrationPerspective doPerspective =
        HopGui.getInstance().getDataOrchestrationPerspective();
    HopPipelineFileType<PipelineMeta> pipelineFileType = doPerspective.getPipelineFileType();
    HopWorkflowFileType<WorkflowMeta> workflowFileType = doPerspective.getWorkflowFileType();

    List<IHopFileTypeHandler> typeHandlers = new ArrayList<>();

    String baseDirectory = vcs.getDirectory();
    getSelectedChangedFiles().stream()
        .forEach(
            content -> {
              String filePath = baseDirectory + Const.FILE_SEPARATOR + content.getName();
              String commitId;
              commitId =
                  isOnlyWIP() ? IVCS.WORKINGTREE : getFirstSelectedRevision().getRevisionId();
              try (InputStream xmlStream = vcs.open(content.getName(), commitId)) {
                IEngineMeta meta = null;
                if (pipelineFileType.isHandledBy(filePath, false)) {
                  // A pipeline...
                  //
                  PipelineMeta pipelineMeta =
                      new PipelineMeta(
                          xmlStream, hopGui.getMetadataProvider(), true, hopGui.getVariables());
                  meta = pipelineMeta;
                  IHopFileTypeHandler typeHandler =
                      doPerspective.addPipeline(hopGui, pipelineMeta, pipelineFileType);
                  typeHandlers.add(typeHandler);
                }
                if (workflowFileType.isHandledBy(filePath, false)) {
                  // A workflow...
                  //
                  WorkflowMeta workflowMeta =
                      new WorkflowMeta(
                          xmlStream, hopGui.getMetadataProvider(), hopGui.getVariables() );
                  meta = workflowMeta;
                  IHopFileTypeHandler typeHandler =
                      doPerspective.addWorkflow(hopGui, workflowMeta, workflowFileType);
                  typeHandlers.add(typeHandler);
                }
                if (meta != null && !isOnlyWIP()) {
                  meta.setNameSynchronizedWithFilename(false);
                  meta.setName(
                      String.format(
                          "%s (%s)",
                          meta.getName(), vcs.getShortenedName(commitId, IVCS.TYPE_COMMIT)));
                }
              } catch (Exception e) {
                showMessageBox(BaseMessages.getString(PKG, "Dialog.Error"), e.getMessage());
              }
            });

    // Did we open any files?
    //
    if (!typeHandlers.isEmpty()) {
      // switch to the data orchestration perspective and select the first opened file
      //
      doPerspective.activate();

      TabItemHandler tabItemHandler = doPerspective.findTabItemHandler(typeHandlers.get(0));
      if (tabItemHandler != null) {
        doPerspective.switchToTab(tabItemHandler);
      }
    }
  }
}
