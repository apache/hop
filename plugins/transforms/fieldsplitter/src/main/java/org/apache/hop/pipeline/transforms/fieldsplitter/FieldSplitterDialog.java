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

package org.apache.hop.pipeline.transforms.fieldsplitter;

import static org.apache.hop.pipeline.transforms.fieldsplitter.FieldSplitterMeta.FSField;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
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
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class FieldSplitterDialog extends BaseTransformDialog {
  private static final Class<?> PKG = FieldSplitterMeta.class;

  private CCombo wSplitField;

  private TextVar wDelimiter;

  private TextVar wEnclosure;

  private TextVar wEscapeString;

  private TableView wFields;

  private final FieldSplitterMeta input;

  private boolean gotPreviousFields = false;

  public FieldSplitterDialog(
      Shell parent,
      IVariables variables,
      FieldSplitterMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "FieldSplitterDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    // Typefield line
    Label wlSplitField = new Label(shell, SWT.RIGHT);
    wlSplitField.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.SplitField.Label"));
    PropsUi.setLook(wlSplitField);
    FormData fdlSplitField = new FormData();
    fdlSplitField.left = new FormAttachment(0, 0);
    fdlSplitField.right = new FormAttachment(middle, -margin);
    fdlSplitField.top = new FormAttachment(wSpacer, margin);
    wlSplitField.setLayoutData(fdlSplitField);
    wSplitField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    wSplitField.setText("");
    PropsUi.setLook(wSplitField);
    FormData fdSplitField = new FormData();
    fdSplitField.left = new FormAttachment(middle, 0);
    fdSplitField.top = new FormAttachment(wSpacer, margin);
    fdSplitField.right = new FormAttachment(100, 0);
    wSplitField.setLayoutData(fdSplitField);
    wSplitField.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          getFields();
          shell.setCursor(null);
          busy.dispose();
        });

    // Delimiter line
    Label wlDelimiter = new Label(shell, SWT.RIGHT);
    wlDelimiter.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.Delimiter.Label"));
    PropsUi.setLook(wlDelimiter);
    FormData fdlDelimiter = new FormData();
    fdlDelimiter.left = new FormAttachment(0, 0);
    fdlDelimiter.right = new FormAttachment(middle, -margin);
    fdlDelimiter.top = new FormAttachment(wSplitField, margin);
    wlDelimiter.setLayoutData(fdlDelimiter);
    wDelimiter = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDelimiter.setToolTipText(BaseMessages.getString(PKG, "FieldSplitterDialog.Delimiter.Tooltip"));
    wDelimiter.setText("");
    PropsUi.setLook(wDelimiter);
    FormData fdDelimiter = new FormData();
    fdDelimiter.left = new FormAttachment(middle, 0);
    fdDelimiter.top = new FormAttachment(wSplitField, margin);
    fdDelimiter.right = new FormAttachment(100, 0);
    wDelimiter.setLayoutData(fdDelimiter);

    // Enclosure
    Label wlEnclosure = new Label(shell, SWT.RIGHT);
    wlEnclosure.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.Enclosure.Label"));
    PropsUi.setLook(wlEnclosure);
    FormData fdlEnclosure = new FormData();
    fdlEnclosure.top = new FormAttachment(wDelimiter, margin);
    fdlEnclosure.left = new FormAttachment(0, 0);
    fdlEnclosure.right = new FormAttachment(middle, -margin);
    wlEnclosure.setLayoutData(fdlEnclosure);
    wEnclosure = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEnclosure.setToolTipText(BaseMessages.getString(PKG, "FieldSplitterDialog.Enclosure.Tooltip"));
    PropsUi.setLook(wEnclosure);
    FormData fdEnclosure = new FormData();
    fdEnclosure.top = new FormAttachment(wDelimiter, margin);
    fdEnclosure.left = new FormAttachment(middle, 0);
    fdEnclosure.right = new FormAttachment(100, 0);
    wEnclosure.setLayoutData(fdEnclosure);

    // Escape string
    Label wlEscapeString = new Label(shell, SWT.RIGHT);
    wlEscapeString.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.EscapeString.Label"));
    PropsUi.setLook(wlEscapeString);
    FormData fdlEscapeString = new FormData();
    fdlEscapeString.top = new FormAttachment(wEnclosure, margin);
    fdlEscapeString.left = new FormAttachment(0, 0);
    fdlEscapeString.right = new FormAttachment(middle, -margin);
    wlEscapeString.setLayoutData(fdlEscapeString);
    wEscapeString = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEscapeString.setToolTipText(
        BaseMessages.getString(PKG, "FieldSplitterDialog.EscapeString.Tooltip"));
    PropsUi.setLook(wEscapeString);
    FormData fdEscapeString = new FormData();
    fdEscapeString.top = new FormAttachment(wEnclosure, margin);
    fdEscapeString.left = new FormAttachment(middle, 0);
    fdEscapeString.right = new FormAttachment(100, 0);
    wEscapeString.setLayoutData(fdEscapeString);

    Label wlFields = new Label(shell, SWT.RIGHT);
    wlFields.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wEscapeString, margin);
    wlFields.setLayoutData(fdlFields);

    final int fieldsRows = input.getFields().size();

    final ColumnInfo[] fieldColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.NewField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.ID"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.RemoveID"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              "Y",
              "N"),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.Format"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              4),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.Group"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.Decimal"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.Currency"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.Nullif"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.IfNull"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FieldSplitterDialog.ColumnInfo.TrimType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaString.trimTypeDesc,
              true),
        };
    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            fieldColumns,
            fieldsRows,
            null,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -margin);
    wFields.setLayoutData(fdFields);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void getFields() {
    if (!gotPreviousFields) {
      try {
        String field = wSplitField.getText();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wSplitField.setItems(r.getFieldNames());
        }
        if (field != null) {
          wSplitField.setText(field);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "FieldSplitterDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "FieldSplitterDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
    }
  }

  public void getData() {
    wSplitField.setText(Const.NVL(input.getSplitField(), ""));
    wDelimiter.setText(Const.NVL(input.getDelimiter(), ""));
    wEnclosure.setText(Const.NVL(input.getEnclosure(), ""));
    wEscapeString.setText(Const.NVL(input.getEscapeString(), ""));

    for (int i = 0; i < input.getFields().size(); i++) {
      FSField field = input.getFields().get(i);
      final TableItem ti = wFields.table.getItem(i);
      ti.setText(1, Const.NVL(field.getName(), ""));
      ti.setText(2, Const.NVL(field.getId(), ""));
      ti.setText(3, field.isIdRemoved() ? "Y" : "N");
      ti.setText(4, Const.NVL(field.getType(), ""));
      if (field.getLength() >= 0) {
        ti.setText(5, "" + field.getLength());
      }
      if (field.getPrecision() >= 0) {
        ti.setText(6, "" + field.getPrecision());
      }
      ti.setText(7, Const.NVL(field.getFormat(), ""));
      ti.setText(8, Const.NVL(field.getGroup(), ""));
      ti.setText(9, Const.NVL(field.getDecimal(), ""));
      ti.setText(10, Const.NVL(field.getCurrency(), ""));
      ti.setText(11, Const.NVL(field.getNullIf(), ""));
      ti.setText(12, Const.NVL(field.getIfNull(), ""));
      ti.setText(13, field.getTrimType() == null ? "" : field.getTrimType().getDescription());
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

    transformName = wTransformName.getText(); // return value

    input.setSplitField(wSplitField.getText());
    input.setDelimiter(wDelimiter.getText());
    input.setEnclosure(wEnclosure.getText());
    input.setEscapeString(wEscapeString.getText());

    input.getFields().clear();
    for (TableItem ti : wFields.getNonEmptyItems()) {
      FSField field = new FSField();
      field.setName(ti.getText(1));
      field.setId(ti.getText(2));
      field.setIdRemoved("Y".equalsIgnoreCase(ti.getText(3)));
      field.setType(ti.getText(4));
      field.setLength(Const.toInt(ti.getText(5), -1));
      field.setPrecision(Const.toInt(ti.getText(6), -1));
      field.setFormat(ti.getText(7));
      field.setGroup(ti.getText(8));
      field.setDecimal(ti.getText(9));
      field.setCurrency(ti.getText(10));
      field.setNullIf(ti.getText(11));
      field.setIfNull(ti.getText(12));
      field.setTrimType(IValueMeta.TrimType.lookupDescription(ti.getText(13)));
      input.getFields().add(field);
    }

    input.setChanged(true);
    dispose();
  }
}
