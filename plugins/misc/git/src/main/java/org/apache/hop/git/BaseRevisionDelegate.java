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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.git.config.GitConfigSingleton;
import org.apache.hop.git.model.UIGit;
import org.apache.hop.git.model.revision.ObjectRevision;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/** Base class sharing the revisions tab between the pipeline and the workflow graphs. */
public abstract class BaseRevisionDelegate {

  public static final Class<?> PKG = BaseRevisionDelegate.class; // i18n

  protected final HopGui hopGui;

  private GuiToolbarWidgets toolBarWidgets;
  private TableView wRevisions;

  /** The revisions shown in the table, in the very same order as its rows. */
  private final List<ObjectRevision> revisions = new ArrayList<>();

  protected BaseRevisionDelegate(HopGui hopGui) {
    super();
    this.hopGui = hopGui;
  }

  /** The toolbar root id declared by the subclass, used to create the toolbar widgets. */
  protected abstract String getToolbarParentId();

  /** The pipeline or workflow filename to get the revisions of, null when it isn't saved yet. */
  protected abstract String getFilename();

  /** Whether the pipeline or workflow being edited holds changes which aren't written to disk. */
  protected abstract boolean hasChanges();

  /** The toolbar items which only make sense when a revision is selected in the table. */
  protected abstract List<String> getSelectionToolbarItemIds();

  /**
   * Opens the graphical comparison of two revisions of the file, as a pipeline or as a workflow.
   *
   * @param relativePath the path of the file, relative to the root of the git repository
   * @param commitIdNew the identifier of the most recent version to compare
   * @param commitIdOld the identifier of the oldest version to compare
   * @throws HopException when the file can't be read or compared
   */
  protected abstract void showGraphDiff(String relativePath, String commitIdNew, String commitIdOld)
      throws HopException;

  /** Creates the revisions tab in the given tab folder. */
  protected CTabItem createRevisionsTab(CTabFolder tabFolder) {

    // There are no revisions to show if Git plugin is disabled or the project is not a Git
    // repository
    if (!GitConfigSingleton.getConfig().isEnabled()
        || GitGuiPlugin.getInstance().getGit() == null) {
      return null;
    }

    CTabItem tab = new CTabItem(tabFolder, SWT.NONE);
    tab.setFont(GuiResource.getInstance().getFontDefault());
    tab.setImage(GitResource.getInstance().getRevisionImage());
    tab.setText(BaseMessages.getString(PKG, "Revisions.Tab.Name"));

    Composite composite = new Composite(tabFolder, SWT.NONE);
    tab.setControl(composite);
    composite.setLayout(new FormLayout());

    // Create toolbar
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, getToolbarParentId());
    Control toolBar = toolBarContainer.getControl();
    toolBar.setLayoutData(new FormDataBuilder().fullWidth().top().result());
    toolBar.pack();
    PropsUi.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

    // Create the table
    ColumnInfo[] revisionColumns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "Revisions.ColumnMessage.Label"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Revisions.ColumnAuthor.Label"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Revisions.ColumnDate.Label"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Revisions.ColumnRevision.Label"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
    };
    wRevisions =
        new TableView(
            hopGui.getVariables(),
            composite,
            SWT.BORDER | SWT.SINGLE | SWT.FULL_SELECTION,
            revisionColumns,
            1,
            null,
            PropsUi.getInstance());
    wRevisions.setReadonly(true);
    wRevisions.setLayoutData(new FormDataBuilder().fullWidth().top(toolBar, 0).bottom().result());
    wRevisions.getTable().addListener(SWT.Resize, new ColumnsResizer(4, 54, 18, 16, 8));
    wRevisions.getTable().addListener(SWT.MouseDoubleClick, event -> showTextDiff());
    wRevisions.getTable().addListener(SWT.Selection, event -> enableToolbarItems());
    PropsUi.setLook(wRevisions);

    this.refresh();

    // The history can be empty, or not available at all, keep the actions consistent with it
    enableToolbarItems();

    return tab;
  }

  /** Refreshes the revisions table with the git history of the current file. */
  public void refresh() {
    String fileName = getFilename();
    if (fileName == null) {
      return;
    }

    // Without a git project there is no revision to show
    UIGit git = GitGuiPlugin.getInstance().getGit();
    if (git == null) {
      return;
    }

    Shell shell = hopGui.getShell();

    try {
      shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));

      // The git repository needs a relative path
      String relativePath = calculateRelativePath(git.getDirectory(), fileName);
      List<ObjectRevision> fileRevisions = git.getRevisions(relativePath);
      wRevisions.setRedraw(false);
      wRevisions.removeAll();
      revisions.clear();
      for (ObjectRevision revision : fileRevisions) {
        if (UIGit.WORKINGTREE.equals(revision.getRevisionId())) {
          continue;
        }
        revisions.add(revision);

        TableItem item = new TableItem(wRevisions.table, SWT.NONE);
        item.setText(1, Const.NVL(revision.getComment(), ""));
        item.setText(2, Const.NVL(revision.getLogin(), ""));
        item.setText(3, getDateString(revision.getCreationDate()));
        item.setText(4, git.getShortenedName(revision.getRevisionId()));
      }
      wRevisions.optimizeTableView();

      // Preselect the most recent revision so the diff actions always have a subject
      if (wRevisions.table.getItemCount() > 0) {
        wRevisions.table.setSelection(0);
      }

      // A programmatic selection doesn't fire an event, update the actions ourselves
      enableToolbarItems();
    } catch (Exception e) {
      LogChannel.UI.logError("Error refreshing revisions", e);
    } finally {
      // Always restore the redraw, otherwise the table stays frozen after an error
      wRevisions.setRedraw(true);
      shell.setCursor(null);
    }
  }

  /**
   * Opens a side by side text comparison of the selected revision with the file currently being
   * edited. The working tree side stays editable, like in the git perspective.
   */
  public void showTextDiff() {
    try {
      RevisionDiff diff = getSelectedRevisionDiff();
      if (diff != null) {
        warnAboutUnsavedChanges();
        GitGuiPlugin.getInstance()
            .showTextFileDiff(diff.relativePath(), diff.commitIdNew(), diff.commitIdOld());
      }
    } catch (Exception e) {
      showDiffError("Revisions.ShowTextDiff.Error.Message", getFilename(), e);
    }
  }

  /**
   * Opens a graphical comparison of the selected revision with the file currently being edited, the
   * same way the git perspective does for a selected commit.
   */
  public void showVisualDiff() {
    try {
      RevisionDiff diff = getSelectedRevisionDiff();
      if (diff != null) {
        warnAboutUnsavedChanges();
        showGraphDiff(diff.relativePath(), diff.commitIdNew(), diff.commitIdOld());
      }
    } catch (Exception e) {
      showDiffError("Revisions.ShowVisualDiff.Error.Message", getFilename(), e);
    }
  }

  /**
   * Determines what has to be compared, the selected revision with the file currently being edited,
   * as it is stored in the working tree.
   *
   * @return the comparison to show, null when there is nothing to compare
   * @throws HopException when the path of the file can't be resolved in the git repository
   */
  private RevisionDiff getSelectedRevisionDiff() throws HopException {
    String fileName = getFilename();
    if (fileName == null) {
      return null;
    }

    // Without a git project there is no revision to compare
    UIGit git = GitGuiPlugin.getInstance().getGit();
    if (git == null) {
      return null;
    }

    if (!isRevisionSelected()) {
      return null;
    }

    try {
      String relativePath = calculateRelativePath(git.getDirectory(), fileName);
      String revisionId = revisions.get(wRevisions.table.getSelectionIndex()).getRevisionId();
      return new RevisionDiff(relativePath, UIGit.WORKINGTREE, revisionId);
    } catch (HopFileException | FileSystemException e) {
      throw new HopException(
          "Unable to locate file '" + fileName + "' in git repository " + git.getDirectory(), e);
    }
  }

  /** Enables the diff actions only as long as a revision is selected in the table. */
  private void enableToolbarItems() {
    boolean selected = isRevisionSelected();
    for (String itemId : getSelectionToolbarItemIds()) {
      toolBarWidgets.enableToolbarItem(itemId, selected);
    }
  }

  private boolean isRevisionSelected() {
    int index = wRevisions.table.getSelectionIndex();
    return index >= 0 && index < revisions.size();
  }

  /**
   * Warns that the comparison is made against the last saved version of the file, as long as the
   * editor still holds changes which aren't written to disk yet.
   */
  private void warnAboutUnsavedChanges() {
    if (!hasChanges()) {
      return;
    }
    MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_WARNING);
    box.setText(BaseMessages.getString(PKG, "Revisions.UnsavedChanges.Warning.Title"));
    box.setMessage(BaseMessages.getString(PKG, "Revisions.UnsavedChanges.Warning.Message"));
    box.open();
  }

  private void showDiffError(String messageKey, String fileName, Exception e) {
    new ErrorDialog(
        hopGui.getShell(),
        BaseMessages.getString(PKG, "Revisions.Error.Title"),
        BaseMessages.getString(PKG, messageKey, fileName),
        e);
  }

  private String calculateRelativePath(String rootFolder, String filename)
      throws HopFileException, FileSystemException {
    FileObject root = HopVfs.getFileObject(rootFolder);
    FileObject file = HopVfs.getFileObject(filename);
    return root.getName().getRelativeName(file.getName());
  }

  private String getDateString(Date date) {
    return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(date);
  }

  /**
   * The git coordinates of a comparison between the file being edited and a selected revision.
   *
   * @param relativePath the path of the file, relative to the root of the git repository
   * @param commitIdNew the working tree, holding the file currently being edited
   * @param commitIdOld the identifier of the selected revision
   */
  protected record RevisionDiff(String relativePath, String commitIdNew, String commitIdOld) {}
}
