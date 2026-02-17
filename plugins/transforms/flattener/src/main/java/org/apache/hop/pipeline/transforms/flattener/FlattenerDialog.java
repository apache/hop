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

package org.apache.hop.pipeline.transforms.flattener;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class FlattenerDialog extends BaseTransformDialog {
  private static final Class<?> PKG = FlattenerMeta.class;

  private TableView wFields;

  private CCombo wField;

  private boolean gotPreviousFields = false;

  private final FlattenerMeta input;

  public FlattenerDialog(
      Shell parent, IVariables variables, FlattenerMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "FlattenerDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    Control lastControl = wSpacer;

    // Key field...
    Label wlField = new Label(shell, SWT.RIGHT);
    wlField.setText(BaseMessages.getString(PKG, "FlattenerDialog.FlattenField.Label"));
    PropsUi.setLook(wlField);
    FormData fdlField = new FormData();
    fdlField.left = new FormAttachment(0, 0);
    fdlField.right = new FormAttachment(middle, -margin);
    fdlField.top = new FormAttachment(lastControl, margin);
    wlField.setLayoutData(fdlField);
    wField = new CCombo(shell, SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wField);
    wField.addModifyListener(lsMod);
    FormData fdField = new FormData();
    fdField.left = new FormAttachment(middle, 0);
    fdField.top = new FormAttachment(lastControl, margin);
    fdField.right = new FormAttachment(100, 0);
    wField.setLayoutData(fdField);
    wField.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          getFields();
          shell.setCursor(null);
          busy.dispose();
        });

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "FlattenerDialog.TargetField.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wField, margin);
    wlFields.setLayoutData(fdlFields);

    int nrKeyCols = 1;
    int nrKeyRows = input.getTargetFields().size();

    ColumnInfo[] ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "FlattenerDialog.ColumnInfo.TargetField"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            nrKeyRows,
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, -margin);
    fdFields.bottom = new FormAttachment(wOk, -margin);
    wFields.setLayoutData(fdFields);

    getData();
    input.setChanged(backupChanged);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void getFields() {
    if (!gotPreviousFields) {
      try {
        String field = wField.getText();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wField.setItems(r.getFieldNames());
        }
        if (field != null) {
          wField.setText(field);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "FlattenerDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "FlattenerDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "FlattenerDialog.Log.GettingKeyInfo"));
    }

    wField.setText(Const.NVL(input.getFieldName(), ""));

    for (int i = 0; i < input.getTargetFields().size(); i++) {
      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(input.getTargetFields().get(i).getName(), ""));
    }

    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    input.setFieldName(wField.getText());
    input.getTargetFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      input.getTargetFields().add(new FlattenerMeta.FField(item.getText(1)));
    }
    transformName = wTransformName.getText();

    dispose();
  }
}
