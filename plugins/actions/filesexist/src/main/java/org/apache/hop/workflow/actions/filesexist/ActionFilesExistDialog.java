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

package org.apache.hop.workflow.actions.filesexist;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/** This dialog allows you to edit the Files exist action settings. */
public class ActionFilesExistDialog extends ActionDialog {
  private static final Class<?> PKG = ActionFilesExist.class;

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")};

  private static final String[] YES_NO_COMBO =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  private ActionFilesExist action;

  private boolean changed;

  private TableView wFields;

  public ActionFilesExistDialog(
      Shell parent, ActionFilesExist action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionFilesExist.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionFilesExist.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ActionFilesExist.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(wSpacer, margin);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionFilesExist.Fields.Argument.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT_BUTTON,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionFilesExist.Fields.Wildcard.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionFilesExist.Fields.ExcludeWildcard.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionFilesExist.Fields.IncludeSubfolders.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              YES_NO_COMBO),
        };

    columns[0].setUsingVariables(true);
    columns[0].setToolTip(BaseMessages.getString(PKG, "ActionFilesExist.Fields.Column"));
    columns[0].setTextVarButtonSelectionListener(getFileSelectionAdapter());
    columns[1].setUsingVariables(true);
    columns[1].setToolTip(BaseMessages.getString(PKG, "ActionFilesExist.Wildcard.Column"));
    columns[2].setUsingVariables(true);
    columns[2].setToolTip(BaseMessages.getString(PKG, "ActionFilesExist.ExcludeWildcard.Column"));
    columns[3].setToolTip(BaseMessages.getString(PKG, "ActionFilesExist.IncludeSubfolders.Column"));

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            action.getFileItems().size(),
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wCancel, -margin);
    wFields.setLayoutData(fdFields);
    wFields.getTable().addListener(SWT.Resize, new ColumnsResizer(4, 40, 20, 20, 16));

    getData();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  protected SelectionAdapter getFileSelectionAdapter() {
    return new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent event) {
        try {
          String path = wFields.getActiveTableItem().getText(wFields.getActiveTableColumn());
          FileObject fileObject = HopVfs.getFileObject(path);

          path =
              BaseDialog.presentFileDialog(
                  shell, null, variables, fileObject, new String[] {"*"}, FILETYPES, true);
          if (path != null) {
            wFields.getActiveTableItem().setText(wFields.getActiveTableColumn(), path);
          }
        } catch (HopFileException e) {
          LogChannel.UI.logError("Error selecting file or directory", e);
        }
      }
    };
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }

    int i = 0;
    for (FileItem item : action.getFileItems()) {
      TableItem ti = wFields.table.getItem(i++);
      ti.setText(1, Const.NVL(item.getFileName(), ""));
      ti.setText(2, Const.NVL(item.getFileMask(), ""));
      ti.setText(3, Const.NVL(item.getExcludeFileMask(), ""));
      ti.setText(4, item.isIncludeSubfolders() ? YES_NO_COMBO[1] : YES_NO_COMBO[0]);
    }
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    action.setName(wName.getText());

    int numberOfItems = wFields.nrNonEmpty();
    List<FileItem> items = new ArrayList<>();
    for (int i = 0; i < numberOfItems; i++) {
      TableItem tableItem = wFields.getNonEmpty(i);
      String path = tableItem.getText(1);
      String wildcard = tableItem.getText(2);
      String excludeWildcard = tableItem.getText(3);
      boolean includeSubfolders = YES_NO_COMBO[1].equalsIgnoreCase(tableItem.getText(4));
      if (!Utils.isEmpty(path)) {
        items.add(new FileItem(path, wildcard, excludeWildcard, includeSubfolders));
      }
    }
    action.setFileItems(items);

    dispose();
  }
}
