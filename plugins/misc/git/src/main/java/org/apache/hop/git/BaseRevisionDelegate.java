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
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.listeners.IFilenameChangedListener;
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
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;

/** Base class sharing the revisions tab between the pipeline and the workflow graphs. */
public abstract class BaseRevisionDelegate {

  public static final Class<?> PKG = BaseRevisionDelegate.class; // i18n

  protected final HopGui hopGui;

  private GuiToolbarWidgets toolBarWidgets;
  private TableView wRevisions;

  /** Shown instead of the table as long as there is no revision to list. */
  private Label wMessage;

  /**
   * The file the rows were loaded for. Commit ids are repository-wide, so the commits of another
   * file must never be compared with the file being edited.
   */
  private String loadedFilename;

  /** Identifies the last refresh, so a slow history walk never overwrites a more recent one. */
  private int refreshId;

  protected BaseRevisionDelegate(HopGui hopGui) {
    super();
    this.hopGui = hopGui;
  }

  /** The toolbar root id declared by the subclass, used to create the toolbar widgets. */
  protected abstract String getToolbarParentId();

  /** The pipeline or workflow being edited, its filename is null when it isn't saved yet. */
  protected abstract AbstractMeta getMeta();

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

  protected CTabItem createRevisionsTab(CTabFolder tabFolder) {
    // There are no revisions to show
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

    // Takes the place of the table, only one of them is visible at a time
    wMessage = new Label(composite, SWT.WRAP);
    wMessage.setLayoutData(
        new FormDataBuilder()
            .left(0, PropsUi.getMargin())
            .right(100, -PropsUi.getMargin())
            .top(toolBar, PropsUi.getMargin())
            .bottom()
            .result());
    PropsUi.setLook(wMessage);

    // The listener is called before the new filename is set, and not always in the UI thread
    Display display = composite.getDisplay();
    AbstractMeta meta = getMeta();
    IFilenameChangedListener filenameListener =
        (object, oldFilename, newFilename) -> {
          if (!display.isDisposed()) {
            display.asyncExec(this::refresh);
          }
        };
    meta.addFilenameChangedListener(filenameListener);

    // Using the toolbar registers this delegate in the key handler, which is never told that the
    // tab is gone and would keep the whole graph in memory after the file is closed.
    composite.addListener(
        SWT.Dispose,
        event -> {
          meta.removeFilenameChangedListener(filenameListener);
          HopGuiKeyHandler.getInstance().removeParentObjectToHandle(this);
        });

    this.refresh();

    return tab;
  }

  /**
   * Reloads the git history of the current file. The history is read in a background thread, as
   * this tab is created along with the execution results, which have to stay responsive.
   */
  public void refresh() {
    if (wRevisions == null || wRevisions.isDisposed()) {
      return;
    }

    String filename = getMeta().getFilename();
    int currentRefreshId = ++refreshId;
    loadedFilename = filename;
    wRevisions.removeAll();
    enableToolbarItems();

    if (filename == null) {
      showMessage(BaseMessages.getString(PKG, "Revisions.Message.NotSaved"));
      return;
    }

    UIGit git = GitGuiPlugin.getInstance().getGit();
    if (git == null) {
      showMessage(BaseMessages.getString(PKG, "Revisions.Message.NoRepository"));
      return;
    }

    String relativePath;
    try {
      relativePath = calculateRelativePath(git.getDirectory(), filename);
    } catch (Exception e) {
      LogChannel.UI.logError("Error locating file '" + filename + "' in git repository", e);
      showMessage(
          BaseMessages.getString(
              PKG, "Revisions.Message.Error", filename, Const.NVL(e.getMessage(), e.toString())));
      return;
    }
    if (relativePath == null) {
      showMessage(
          BaseMessages.getString(
              PKG, "Revisions.Message.OutsideRepository", filename, git.getDirectory()));
      return;
    }

    showMessage(BaseMessages.getString(PKG, "Revisions.Message.Loading"));

    Display display = wRevisions.getDisplay();
    Thread thread =
        new Thread(
            () -> {
              List<ObjectRevision> fileRevisions = new ArrayList<>();
              Exception error = null;
              try {
                fileRevisions = git.getRevisions(relativePath);
              } catch (Exception e) {
                error = e;
              }
              List<ObjectRevision> loadedRevisions = fileRevisions;
              Exception loadError = error;
              if (!display.isDisposed()) {
                display.asyncExec(
                    () -> showRevisions(currentRefreshId, git, loadedRevisions, loadError));
              }
            },
            "Git revisions of " + relativePath);
    thread.setDaemon(true);
    thread.start();
  }

  private void showRevisions(
      int loadedRefreshId, UIGit git, List<ObjectRevision> fileRevisions, Exception error) {
    // The tab can be closed, or the file renamed, while the history was being read
    if (wRevisions.isDisposed() || loadedRefreshId != refreshId) {
      return;
    }

    if (error != null) {
      LogChannel.UI.logError("Error getting git revisions of file '" + loadedFilename + "'", error);
      showMessage(
          BaseMessages.getString(
              PKG,
              "Revisions.Message.Error",
              loadedFilename,
              Const.NVL(error.getMessage(), error.toString())));
      return;
    }

    int count = 0;
    wRevisions.setRedraw(false);
    try {
      wRevisions.removeAll();
      for (ObjectRevision revision : fileRevisions) {
        if (UIGit.WORKINGTREE.equals(revision.getRevisionId())) {
          continue;
        }

        TableItem item = new TableItem(wRevisions.table, SWT.NONE);
        // The row order changes when a column is sorted, and the commit id is shortened in the
        // table: the row has to carry its own revision, which TableView keeps with it on a sort.
        item.setData(revision);
        item.setText(1, Const.NVL(revision.getComment(), ""));
        item.setText(2, Const.NVL(revision.getLogin(), ""));
        item.setText(3, getDateString(revision.getCreationDate()));
        item.setText(4, git.getShortenedName(revision.getRevisionId()));
        count++;
      }
      wRevisions.optimizeTableView();

      // Preselect the most recent revision so the diff actions always have a subject
      if (count > 0) {
        wRevisions.table.setSelection(0);
      }
    } finally {
      // Always restore the redraw, otherwise the table stays frozen after an error
      wRevisions.setRedraw(true);
    }

    if (count > 0) {
      wMessage.setVisible(false);
      wRevisions.setVisible(true);
    } else {
      showMessage(BaseMessages.getString(PKG, "Revisions.Message.NoRevisions"));
    }

    // A programmatic selection doesn't fire an event, update the actions ourselves
    enableToolbarItems();
  }

  private void showMessage(String message) {
    wMessage.setText(message);
    wRevisions.setVisible(false);
    wMessage.setVisible(true);
  }

  /** The working tree side of the comparison stays editable, like in the git perspective. */
  public void showTextDiff() {
    try {
      RevisionDiff diff = getSelectedRevisionDiff();
      if (diff != null) {
        warnAboutUnsavedChanges();
        GitGuiPlugin.getInstance()
            .showTextFileDiff(diff.relativePath(), diff.commitIdNew(), diff.commitIdOld());
      }
    } catch (Exception e) {
      showDiffError("Revisions.ShowTextDiff.Error.Message", getMeta().getFilename(), e);
    }
  }

  public void showVisualDiff() {
    try {
      RevisionDiff diff = getSelectedRevisionDiff();
      if (diff != null) {
        warnAboutUnsavedChanges();
        showGraphDiff(diff.relativePath(), diff.commitIdNew(), diff.commitIdOld());
      }
    } catch (Exception e) {
      showDiffError("Revisions.ShowVisualDiff.Error.Message", getMeta().getFilename(), e);
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
    String filename = getMeta().getFilename();
    UIGit git = GitGuiPlugin.getInstance().getGit();
    if (filename == null || git == null) {
      return null;
    }

    // The rows still list the commits of the previous file, reload instead of comparing them
    if (!filename.equals(loadedFilename)) {
      refresh();
      return null;
    }

    ObjectRevision revision = getSelectedRevision();
    if (revision == null) {
      return null;
    }

    try {
      String relativePath = calculateRelativePath(git.getDirectory(), filename);
      if (relativePath == null) {
        return null;
      }
      return new RevisionDiff(relativePath, UIGit.WORKINGTREE, revision.getRevisionId());
    } catch (HopFileException | FileSystemException e) {
      throw new HopException(
          "Unable to locate file '" + filename + "' in git repository " + git.getDirectory(), e);
    }
  }

  private void enableToolbarItems() {
    boolean selected = getSelectedRevision() != null;
    for (String itemId : getSelectionToolbarItemIds()) {
      toolBarWidgets.enableToolbarItem(itemId, selected);
    }
  }

  private ObjectRevision getSelectedRevision() {
    if (wRevisions == null || wRevisions.isDisposed()) {
      return null;
    }
    TableItem[] selection = wRevisions.table.getSelection();
    if (selection.length == 1 && selection[0].getData() instanceof ObjectRevision revision) {
      return revision;
    }
    return null;
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

  /**
   * @return the path git knows the file by, null when the file is outside of the repository
   */
  private String calculateRelativePath(String rootFolder, String filename)
      throws HopFileException, FileSystemException {
    FileObject root = HopVfs.getFileObject(rootFolder);
    FileObject file = HopVfs.getFileObject(filename);
    if (!root.getName().isDescendent(file.getName())) {
      return null;
    }
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
