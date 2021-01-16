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

package org.apache.hop.pipeline.transforms.fieldsplitter;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class FieldSplitterDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = FieldSplitterMeta.class; // For Translator

  private CCombo wSplitField;

  private TextVar wDelimiter;

  private TextVar wEnclosure;

  private TextVar wEscapeString;

  private TableView wFields;

  private final FieldSplitterMeta input;

  private boolean gotPreviousFields = false;

  public FieldSplitterDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (FieldSplitterMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Typefield line
    Label wlSplitfield = new Label(shell, SWT.RIGHT);
    wlSplitfield.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.SplitField.Label"));
    props.setLook(wlSplitfield);
    FormData fdlSplitfield = new FormData();
    fdlSplitfield.left = new FormAttachment(0, 0);
    fdlSplitfield.right = new FormAttachment(middle, -margin);
    fdlSplitfield.top = new FormAttachment(wTransformName, margin);
    wlSplitfield.setLayoutData(fdlSplitfield);
    wSplitField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    wSplitField.setText("");
    props.setLook(wSplitField);
    wSplitField.addModifyListener(lsMod);
    FormData fdSplitfield = new FormData();
    fdSplitfield.left = new FormAttachment(middle, 0);
    fdSplitfield.top = new FormAttachment(wTransformName, margin);
    fdSplitfield.right = new FormAttachment(100, 0);
    wSplitField.setLayoutData(fdSplitfield);
    wSplitField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            getFields();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    // Typefield line
    Label wlDelimiter = new Label(shell, SWT.RIGHT);
    wlDelimiter.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.Delimiter.Label"));
    props.setLook(wlDelimiter);
    FormData fdlDelimiter = new FormData();
    fdlDelimiter.left = new FormAttachment(0, 0);
    fdlDelimiter.right = new FormAttachment(middle, -margin);
    fdlDelimiter.top = new FormAttachment(wSplitField, margin);
    wlDelimiter.setLayoutData(fdlDelimiter);
    wDelimiter = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDelimiter.setToolTipText(BaseMessages.getString(PKG, "FieldSplitterDialog.Delimiter.Tooltip"));
    wDelimiter.setText("");
    props.setLook(wDelimiter);
    wDelimiter.addModifyListener(lsMod);
    FormData fdDelimiter = new FormData();
    fdDelimiter.left = new FormAttachment(middle, 0);
    fdDelimiter.top = new FormAttachment(wSplitField, margin);
    fdDelimiter.right = new FormAttachment(100, 0);
    wDelimiter.setLayoutData(fdDelimiter);

    // enclosure
    Label wlEnclosure = new Label(shell, SWT.RIGHT);
    wlEnclosure.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.Enclosure.Label"));
    props.setLook(wlEnclosure);
    FormData fdlEnclosure = new FormData();
    fdlEnclosure.top = new FormAttachment(wDelimiter, margin);
    fdlEnclosure.left = new FormAttachment(0, 0);
    fdlEnclosure.right = new FormAttachment(middle, -margin);
    wlEnclosure.setLayoutData(fdlEnclosure);
    wEnclosure = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEnclosure.setToolTipText(BaseMessages.getString(PKG, "FieldSplitterDialog.Enclosure.Tooltip"));
    props.setLook(wEnclosure);
    wEnclosure.addModifyListener(lsMod);
    FormData fdEnclosure = new FormData();
    fdEnclosure.top = new FormAttachment(wDelimiter, margin);
    fdEnclosure.left = new FormAttachment(middle, 0);
    fdEnclosure.right = new FormAttachment(100, 0);
    wEnclosure.setLayoutData(fdEnclosure);

    // Escape string
    Label wlEscapeString = new Label(shell, SWT.RIGHT);
    wlEscapeString.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.EscapeString.Label"));
    props.setLook(wlEscapeString);
    FormData fdlEscapeString = new FormData();
    fdlEscapeString.top = new FormAttachment(wEnclosure, margin);
    fdlEscapeString.left = new FormAttachment(0, 0);
    fdlEscapeString.right = new FormAttachment(middle, -margin);
    wlEscapeString.setLayoutData(fdlEscapeString);
    wEscapeString = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEscapeString.setToolTipText(
        BaseMessages.getString(PKG, "FieldSplitterDialog.EscapeString.Tooltip"));
    props.setLook(wEscapeString);
    wEscapeString.addModifyListener(lsMod);
    FormData fdEscapeString = new FormData();
    fdEscapeString.top = new FormAttachment(wEnclosure, margin);
    fdEscapeString.left = new FormAttachment(middle, 0);
    fdEscapeString.right = new FormAttachment(100, 0);
    wEscapeString.setLayoutData(fdEscapeString);

    Label wlFields = new Label(shell, SWT.RIGHT);
    wlFields.setText(BaseMessages.getString(PKG, "FieldSplitterDialog.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wEscapeString, margin);
    wlFields.setLayoutData(fdlFields);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    final int fieldsRows = input.getFieldName().length;

    final ColumnInfo[] colinf =
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
              new String[] {"Y", "N"}),
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
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
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
            colinf,
            fieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    // Add listeners
    lsOk = e -> ok();
    lsCancel = e -> cancel();

    wOk.addListener(SWT.Selection, lsOk);
    wCancel.addListener(SWT.Selection, lsCancel);

    lsDef =
        new SelectionAdapter() {
          @Override
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wDelimiter.addSelectionListener(lsDef);
    wEnclosure.addSelectionListener(lsDef);
    wEscapeString.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged(changed);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
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

    for (int i = 0; i < input.getFieldName().length; i++) {
      final TableItem ti = wFields.table.getItem(i);
      if (input.getFieldName()[i] != null) {
        ti.setText(1, input.getFieldName()[i]);
      }
      if (input.getFieldID()[i] != null) {
        ti.setText(2, input.getFieldID()[i]);
      }
      ti.setText(3, input.getFieldRemoveID()[i] ? "Y" : "N");
      ti.setText(4, ValueMetaFactory.getValueMetaName(input.getFieldType()[i]));
      if (input.getFieldLength()[i] >= 0) {
        ti.setText(5, "" + input.getFieldLength()[i]);
      }
      if (input.getFieldPrecision()[i] >= 0) {
        ti.setText(6, "" + input.getFieldPrecision()[i]);
      }
      if (input.getFieldFormat()[i] != null) {
        ti.setText(7, input.getFieldFormat()[i]);
      }
      if (input.getFieldGroup()[i] != null) {
        ti.setText(8, input.getFieldGroup()[i]);
      }
      if (input.getFieldDecimal()[i] != null) {
        ti.setText(9, input.getFieldDecimal()[i]);
      }
      if (input.getFieldCurrency()[i] != null) {
        ti.setText(10, input.getFieldCurrency()[i]);
      }
      if (input.getFieldNullIf()[i] != null) {
        ti.setText(11, input.getFieldNullIf()[i]);
      }
      if (input.getFieldIfNull()[i] != null) {
        ti.setText(12, input.getFieldIfNull()[i]);
      }
      ti.setText(13, ValueMetaString.getTrimTypeDesc(input.getFieldTrimType()[i]));
    }
    wFields.setRowNums();
    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    input.setSplitField(wSplitField.getText());
    input.setDelimiter(wDelimiter.getText());
    input.setEnclosure(wEnclosure.getText());
    input.setEscapeString(wEscapeString.getText());

    // Table table = wFields.table;
    int nrFields = wFields.nrNonEmpty();

    input.allocate(nrFields);

    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < input.getFieldName().length; i++) {
      final TableItem ti = wFields.getNonEmpty(i);
      input.getFieldName()[i] = ti.getText(1);
      input.getFieldID()[i] = ti.getText(2);
      input.getFieldRemoveID()[i] = "Y".equalsIgnoreCase(ti.getText(3));
      input.getFieldType()[i] = ValueMetaFactory.getIdForValueMeta(ti.getText(4));
      input.getFieldLength()[i] = Const.toInt(ti.getText(5), -1);
      input.getFieldPrecision()[i] = Const.toInt(ti.getText(6), -1);
      input.getFieldFormat()[i] = ti.getText(7);
      input.getFieldGroup()[i] = ti.getText(8);
      input.getFieldDecimal()[i] = ti.getText(9);
      input.getFieldCurrency()[i] = ti.getText(10);
      input.getFieldNullIf()[i] = ti.getText(11);
      input.getFieldIfNull()[i] = ti.getText(12);
      input.getFieldTrimType()[i] = ValueMetaString.getTrimTypeByDesc(ti.getText(13));
    }

    dispose();
  }
}
