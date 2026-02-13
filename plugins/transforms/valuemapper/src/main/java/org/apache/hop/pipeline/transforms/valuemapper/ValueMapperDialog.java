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

package org.apache.hop.pipeline.transforms.valuemapper;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class ValueMapperDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ValueMapperMeta.class;

  private CCombo wFieldName;

  private Text wTargetFieldName;

  private TextVar wNonMatchDefault;

  private TableView wFields;

  private final ValueMapperMeta input;

  private boolean gotPreviousFields = false;

  private CCombo wTargetType;

  public ValueMapperDialog(
      Shell parent,
      IVariables variables,
      ValueMapperMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ValueMapperDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();

    // Fieldname line
    Label wlFieldname = new Label(shell, SWT.RIGHT);
    wlFieldname.setText(BaseMessages.getString(PKG, "ValueMapperDialog.FieldnameToUser.Label"));
    PropsUi.setLook(wlFieldname);
    FormData fdlFieldname = new FormData();
    fdlFieldname.left = new FormAttachment(0, 0);
    fdlFieldname.right = new FormAttachment(middle, -margin);
    fdlFieldname.top = new FormAttachment(wSpacer, margin);
    wlFieldname.setLayoutData(fdlFieldname);

    wFieldName = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wFieldName);
    FormData fdFieldname = new FormData();
    fdFieldname.left = new FormAttachment(middle, 0);
    fdFieldname.top = new FormAttachment(wSpacer, margin);
    fdFieldname.right = new FormAttachment(100, 0);
    wFieldName.setLayoutData(fdFieldname);
    wFieldName.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Do nothing
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            getFields();
            shell.setCursor(null);
            busy.dispose();
          }
        });
    // TargetFieldname line
    Label wlTargetFieldname = new Label(shell, SWT.RIGHT);
    wlTargetFieldname.setText(
        BaseMessages.getString(PKG, "ValueMapperDialog.TargetFieldname.Label"));
    PropsUi.setLook(wlTargetFieldname);
    FormData fdlTargetFieldname = new FormData();
    fdlTargetFieldname.left = new FormAttachment(0, 0);
    fdlTargetFieldname.right = new FormAttachment(middle, -margin);
    fdlTargetFieldname.top = new FormAttachment(wFieldName, margin);
    wlTargetFieldname.setLayoutData(fdlTargetFieldname);
    wTargetFieldName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTargetFieldName);
    FormData fdTargetFieldname = new FormData();
    fdTargetFieldname.left = new FormAttachment(middle, 0);
    fdTargetFieldname.top = new FormAttachment(wFieldName, margin);
    fdTargetFieldname.right = new FormAttachment(100, 0);
    wTargetFieldName.setLayoutData(fdTargetFieldname);

    // Non match default line
    Label wlNonMatchDefault = new Label(shell, SWT.RIGHT);
    wlNonMatchDefault.setText(
        BaseMessages.getString(PKG, "ValueMapperDialog.NonMatchDefault.Label"));
    PropsUi.setLook(wlNonMatchDefault);
    FormData fdlNonMatchDefault = new FormData();
    fdlNonMatchDefault.left = new FormAttachment(0, 0);
    fdlNonMatchDefault.right = new FormAttachment(middle, -margin);
    fdlNonMatchDefault.top = new FormAttachment(wTargetFieldName, margin);
    wlNonMatchDefault.setLayoutData(fdlNonMatchDefault);
    wNonMatchDefault = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wNonMatchDefault);
    FormData fdNonMatchDefault = new FormData();
    fdNonMatchDefault.left = new FormAttachment(middle, 0);
    fdNonMatchDefault.top = new FormAttachment(wTargetFieldName, margin);
    fdNonMatchDefault.right = new FormAttachment(100, 0);
    wNonMatchDefault.setLayoutData(fdNonMatchDefault);

    // Type of value
    /*
     * Type of Value: String, Number, Date, Boolean, Integer
     */
    Label wlValueType = new Label(shell, SWT.RIGHT);
    wlValueType.setText(BaseMessages.getString(PKG, "ValueMapperDialog.TargetType.Label"));
    FormData fdlValueType = new FormData();
    fdlValueType.left = new FormAttachment(0, 0);
    fdlValueType.right = new FormAttachment(middle, -margin);
    fdlValueType.top = new FormAttachment(wNonMatchDefault, margin);
    wlValueType.setLayoutData(fdlValueType);
    wTargetType = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);
    wTargetType.setItems(ValueMetaFactory.getValueMetaNames());
    PropsUi.setLook(wTargetType);
    FormData fdValueType = new FormData();
    fdValueType.left = new FormAttachment(middle, 0);
    fdValueType.top = new FormAttachment(wNonMatchDefault, margin);
    fdValueType.right = new FormAttachment(100, 0);
    wTargetType.setLayoutData(fdValueType);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ValueMapperDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wlValueType, margin);
    wlFields.setLayoutData(fdlFields);

    final int FieldsCols = 2;
    final int FieldsRows = input.getValues().size();

    ColumnInfo[] colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ValueMapperDialog.Fields.Column.SourceValue"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ValueMapperDialog.Fields.Column.TargetValue"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[1].setUsingVariables(true);

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            null,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -margin);
    wFields.setLayoutData(fdFields);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void getFields() {
    if (!gotPreviousFields) {
      gotPreviousFields = true;
      try {
        String fieldname = wFieldName.getText();

        wFieldName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wFieldName.setItems(r.getFieldNames());
          if (fieldname != null) {
            wFieldName.setText(fieldname);
          }
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "ValueMapperDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "ValueMapperDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getFieldToUse() != null) {
      wFieldName.setText(input.getFieldToUse());
    }
    if (input.getTargetField() != null) {
      wTargetFieldName.setText(input.getTargetField());
    }
    if (input.getNonMatchDefault() != null) {
      wNonMatchDefault.setText(input.getNonMatchDefault());
    }
    if (input.getTargetType() != null) {
      wTargetType.setText(input.getTargetType());
    }

    int i = 0;
    for (Values v : input.getValues()) {
      TableItem item = wFields.table.getItem(i++);
      String src = v.getSource();
      String tgt = v.getTarget();

      if (src != null) {
        item.setText(1, src);
      }
      if (tgt != null) {
        item.setText(2, tgt);
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

    input.setFieldToUse(wFieldName.getText());
    input.setTargetField(wTargetFieldName.getText());
    input.setNonMatchDefault(wNonMatchDefault.getText());
    input.setTargetType(wTargetType.getText());

    input.getValues().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      Values v = new Values();
      v.setSource(Utils.isEmpty(item.getText(1)) ? null : item.getText(1));
      v.setTarget(item.getText(2));
      input.getValues().add(v);
    }

    dispose();
  }
}
