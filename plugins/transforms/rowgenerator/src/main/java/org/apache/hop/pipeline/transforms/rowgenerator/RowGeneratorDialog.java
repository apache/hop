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

package org.apache.hop.pipeline.transforms.rowgenerator;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class RowGeneratorDialog extends BaseTransformDialog {
  private static final Class<?> PKG = RowGeneratorMeta.class;
  public static final String CONST_SYSTEM_COMBO_YES = "System.Combo.Yes";

  private Label wlLimit;
  private TextVar wLimit;

  private Button wNeverEnding;

  private Label wlInterval;
  private TextVar wInterval;

  private Label wlRowTimeField;
  private TextVar wRowTimeField;

  private Label wlLastTimeField;
  private TextVar wLastTimeField;

  private TableView wFields;

  private final RowGeneratorMeta input;

  public RowGeneratorDialog(
      Shell parent,
      IVariables variables,
      RowGeneratorMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    wlLimit = new Label(shell, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.Limit.Label"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.right = new FormAttachment(middle, -margin);
    fdlLimit.top = new FormAttachment(lastControl, margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLimit);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.top = new FormAttachment(lastControl, margin);
    fdLimit.right = new FormAttachment(100, 0);
    wLimit.setLayoutData(fdLimit);
    lastControl = wLimit;

    Label wlNeverEnding = new Label(shell, SWT.RIGHT);
    wlNeverEnding.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.NeverEnding.Label"));
    PropsUi.setLook(wlNeverEnding);
    FormData fdlNeverEnding = new FormData();
    fdlNeverEnding.left = new FormAttachment(0, 0);
    fdlNeverEnding.right = new FormAttachment(middle, -margin);
    fdlNeverEnding.top = new FormAttachment(lastControl, margin);
    wlNeverEnding.setLayoutData(fdlNeverEnding);
    wNeverEnding = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wNeverEnding);
    wNeverEnding.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            updateWidgets();
          }
        });
    FormData fdNeverEnding = new FormData();
    fdNeverEnding.left = new FormAttachment(middle, 0);
    fdNeverEnding.top = new FormAttachment(wlNeverEnding, 0, SWT.CENTER);
    fdNeverEnding.right = new FormAttachment(100, 0);
    wNeverEnding.setLayoutData(fdNeverEnding);
    lastControl = wlNeverEnding;

    wlInterval = new Label(shell, SWT.RIGHT);
    wlInterval.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.Interval.Label"));
    PropsUi.setLook(wlInterval);
    FormData fdlInterval = new FormData();
    fdlInterval.left = new FormAttachment(0, 0);
    fdlInterval.right = new FormAttachment(middle, -margin);
    fdlInterval.top = new FormAttachment(lastControl, margin);
    wlInterval.setLayoutData(fdlInterval);
    wInterval = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInterval);
    FormData fdInterval = new FormData();
    fdInterval.left = new FormAttachment(middle, 0);
    fdInterval.top = new FormAttachment(lastControl, margin);
    fdInterval.right = new FormAttachment(100, 0);
    wInterval.setLayoutData(fdInterval);
    lastControl = wInterval;

    wlRowTimeField = new Label(shell, SWT.RIGHT);
    wlRowTimeField.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.RowTimeField.Label"));
    PropsUi.setLook(wlRowTimeField);
    FormData fdlRowTimeField = new FormData();
    fdlRowTimeField.left = new FormAttachment(0, 0);
    fdlRowTimeField.right = new FormAttachment(middle, -margin);
    fdlRowTimeField.top = new FormAttachment(lastControl, margin);
    wlRowTimeField.setLayoutData(fdlRowTimeField);
    wRowTimeField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRowTimeField);
    FormData fdRowTimeField = new FormData();
    fdRowTimeField.left = new FormAttachment(middle, 0);
    fdRowTimeField.top = new FormAttachment(lastControl, margin);
    fdRowTimeField.right = new FormAttachment(100, 0);
    wRowTimeField.setLayoutData(fdRowTimeField);
    lastControl = wRowTimeField;

    wlLastTimeField = new Label(shell, SWT.RIGHT);
    wlLastTimeField.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.LastTimeField.Label"));
    PropsUi.setLook(wlLastTimeField);
    FormData fdlLastTimeField = new FormData();
    fdlLastTimeField.left = new FormAttachment(0, 0);
    fdlLastTimeField.right = new FormAttachment(middle, -margin);
    fdlLastTimeField.top = new FormAttachment(lastControl, margin);
    wlLastTimeField.setLayoutData(fdlLastTimeField);
    wLastTimeField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLastTimeField);
    FormData fdLastTimeField = new FormData();
    fdLastTimeField.left = new FormAttachment(middle, 0);
    fdLastTimeField.top = new FormAttachment(lastControl, margin);
    fdLastTimeField.right = new FormAttachment(100, 0);
    wLastTimeField.setLayoutData(fdLastTimeField);
    lastControl = wLastTimeField;

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview"));
    wPreview.addListener(SWT.Selection, e -> preview());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(lastControl, margin);
    wlFields.setLayoutData(fdlFields);
    lastControl = wlFields;

    final int nrFields = input.getFields().size();

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Format"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Currency"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Decimal"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Group"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Value"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.SetEmptyString"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
              BaseMessages.getString(PKG, "System.Combo.No"))
        };

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            nrFields,
            null,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(lastControl, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    lsResize =
        event -> {
          Point size = shell.getSize();
          wFields.setSize(size.x - 10, size.y - 50);
          wFields.table.setSize(size.x - 10, size.y - 50);
          wFields.redraw();
        };
    shell.addListener(SWT.Resize, lsResize);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void updateWidgets() {
    boolean neverEnding = wNeverEnding.getSelection();

    wlLimit.setEnabled(!neverEnding);
    wLimit.setEnabled(!neverEnding);

    wlInterval.setEnabled(neverEnding);
    wInterval.setEnabled(neverEnding);

    wlRowTimeField.setEnabled(neverEnding);
    wRowTimeField.setEnabled(neverEnding);

    wlLastTimeField.setEnabled(neverEnding);
    wLastTimeField.setEnabled(neverEnding);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (isDebug()) {
      logDebug("getting fields info...");
    }

    wLimit.setText(Const.NVL(input.getRowLimit(), ""));
    wNeverEnding.setSelection(input.isNeverEnding());
    wInterval.setText(Const.NVL(input.getIntervalInMs(), ""));
    wRowTimeField.setText(Const.NVL(input.getRowTimeField(), ""));
    wLastTimeField.setText(Const.NVL(input.getLastTimeField(), ""));

    for (int i = 0; i < input.getFields().size(); i++) {
      GeneratorField field = input.getFields().get(i);
      TableItem item = wFields.table.getItem(i);
      int col = 1;
      item.setText(col++, Const.NVL(field.getName(), ""));

      String type = field.getType();
      String format = field.getFormat();
      String length = field.getLength() < 0 ? "" : ("" + field.getLength());
      String prec = field.getPrecision() < 0 ? "" : ("" + field.getPrecision());

      String curr = field.getCurrency();
      String group = field.getGroup();
      String decim = field.getDecimal();
      String def = field.getValue();

      item.setText(col++, Const.NVL(type, ""));
      item.setText(col++, Const.NVL(format, ""));
      item.setText(col++, Const.NVL(length, ""));
      item.setText(col++, Const.NVL(prec, ""));
      item.setText(col++, Const.NVL(curr, ""));
      item.setText(col++, Const.NVL(decim, ""));
      item.setText(col++, Const.NVL(group, ""));
      item.setText(col++, Const.NVL(def, ""));
      item.setText(
          col++,
          field.isSetEmptyString()
              ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
              : BaseMessages.getString(PKG, "System.Combo.No"));
    }

    wFields.setRowNums();
    wFields.optWidth(true);

    updateWidgets();

    wTransformName.selectAll();
    wTransformName.setFocus();
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
    try {
      getInfo(input); // to put the content on the input structure for real if all is well.
      input.setChanged();
      dispose();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "RowGeneratorDialog.Illegal.Dialog.Settings.Title"),
          BaseMessages.getString(PKG, "RowGeneratorDialog.Illegal.Dialog.Settings.Message"),
          e);
    }
  }

  private void getInfo(RowGeneratorMeta meta) {
    meta.setRowLimit(wLimit.getText());
    meta.setNeverEnding(wNeverEnding.getSelection());
    meta.setIntervalInMs(wInterval.getText());
    meta.setRowTimeField(wRowTimeField.getText());
    meta.setLastTimeField(wLastTimeField.getText());

    meta.getFields().clear();

    for (TableItem item : wFields.getNonEmptyItems()) {
      GeneratorField field = new GeneratorField();
      field.setName(item.getText(1));
      field.setFormat(item.getText(3));
      field.setLength(Const.toInt(item.getText(4), -1));
      field.setPrecision(Const.toInt(item.getText(5), -1));
      field.setCurrency(item.getText(6));
      field.setDecimal(item.getText(7));
      field.setGroup(item.getText(8));
      field.setValue(field.isSetEmptyString() ? "" : item.getText(9));
      field.setSetEmptyString(
          BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES).equalsIgnoreCase(item.getText(10)));
      field.setType(field.isSetEmptyString() ? "String" : item.getText(2));

      meta.getFields().add(field);
    }
  }

  /**
   * Preview the data generated by this transform. This generates a pipeline using this transform &
   * a dummy and previews it.
   */
  private void preview() {
    RowGeneratorMeta oneMeta = new RowGeneratorMeta();
    try {
      getInfo(oneMeta);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "RowGeneratorDialog.Illegal.Dialog.Settings.Title"),
          BaseMessages.getString(PKG, "RowGeneratorDialog.Illegal.Dialog.Settings.Message"),
          e);
      return;
    }

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "System.Dialog.EnterPreviewSize.Title"),
            BaseMessages.getString(PKG, "System.Dialog.EnterPreviewSize.Message"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      oneMeta.setRowLimit(Integer.toString(previewSize));
      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
              pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell,
              variables,
              previewMeta,
              new String[] {wTransformName.getText()},
              new int[] {previewSize});
      progressDialog.open();

      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if (!progressDialog.isCancelled()
          && pipeline.getResult() != null
          && pipeline.getResult().getNrErrors() > 0) {
        EnterTextDialog etd =
            new EnterTextDialog(
                shell,
                BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                loggingText,
                true);
        etd.setReadOnly();
        etd.open();
      }

      PreviewRowsDialog prd =
          new PreviewRowsDialog(
              shell,
              variables,
              SWT.NONE,
              wTransformName.getText(),
              progressDialog.getPreviewRowsMeta(wTransformName.getText()),
              progressDialog.getPreviewRows(wTransformName.getText()),
              loggingText);
      prd.open();
    }
  }
}
