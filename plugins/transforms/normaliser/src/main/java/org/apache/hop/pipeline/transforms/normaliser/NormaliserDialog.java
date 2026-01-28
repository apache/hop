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

package org.apache.hop.pipeline.transforms.normaliser;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class NormaliserDialog extends BaseTransformDialog {
  private static final Class<?> PKG = NormaliserMeta.class;

  private static final int NAME_INDEX = 1;

  private static final int VALUE_INDEX = 2;

  private static final int NORM_INDEX = 3;

  private Text wTypefield;

  private TableView wFields;

  private final NormaliserMeta input;

  private ColumnInfo[] colinf;

  private final List<String> inputFields = new ArrayList<>();

  public NormaliserDialog(
      Shell parent, IVariables variables, NormaliserMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "NormaliserDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Typefield line
    Label wlTypefield = new Label(shell, SWT.RIGHT);
    wlTypefield.setText(BaseMessages.getString(PKG, "NormaliserDialog.TypeField.Label"));
    PropsUi.setLook(wlTypefield);
    FormData fdlTypefield = new FormData();
    fdlTypefield.left = new FormAttachment(0, 0);
    fdlTypefield.right = new FormAttachment(middle, -margin);
    fdlTypefield.top = new FormAttachment(wSpacer, margin);
    wlTypefield.setLayoutData(fdlTypefield);
    wTypefield = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTypefield.setText("");
    PropsUi.setLook(wTypefield);
    wTypefield.addModifyListener(lsMod);
    FormData fdTypefield = new FormData();
    fdTypefield.left = new FormAttachment(middle, 0);
    fdTypefield.top = new FormAttachment(wSpacer, margin);
    fdTypefield.right = new FormAttachment(100, 0);
    wTypefield.setLayoutData(fdTypefield);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "NormaliserDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wTypefield, margin);
    wlFields.setLayoutData(fdlFields);

    final int fieldsCols = 3;
    final int fieldsRows = input.getNormaliserFields().size();
    colinf = new ColumnInfo[fieldsCols];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "NormaliserDialog.ColumnInfo.Fieldname"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "NormaliserDialog.ColumnInfo.Type"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "NormaliserDialog.ColumnInfo.NewField"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            fieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -margin);
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.add(row.getValueMeta(i).getName());
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
  }

  public void getData() {
    if (input.getTypeField() != null) {
      wTypefield.setText(input.getTypeField());
    }

    for (int i = 0; i < input.getNormaliserFields().size(); i++) {
      TableItem item = wFields.table.getItem(i);
      NormaliserField field = input.getNormaliserFields().get(i);
      if (field.getName() != null) {
        item.setText(NAME_INDEX, field.getName());
      }
      if (field.getValue() != null) {
        item.setText(VALUE_INDEX, field.getValue());
      }
      if (field.getNorm() != null) {
        item.setText(NORM_INDEX, field.getNorm());
      }
    }

    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    input.setTypeField(wTypefield.getText());

    int i;

    int nrFields = wFields.nrNonEmpty();
    input.getNormaliserFields().clear();

    for (i = 0; i < nrFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      NormaliserField field = new NormaliserField();
      field.setName(item.getText(NAME_INDEX));
      field.setValue(item.getText(VALUE_INDEX));
      field.setNorm(item.getText(NORM_INDEX));
      input.getNormaliserFields().add(field);
    }

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1, 2}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "NormaliserDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "NormaliserDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
