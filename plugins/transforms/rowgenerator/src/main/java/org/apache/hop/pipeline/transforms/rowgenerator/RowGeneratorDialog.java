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

package org.apache.hop.pipeline.transforms.rowgenerator;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
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
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class RowGeneratorDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = RowGeneratorMeta.class; // For Translator

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
    Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (RowGeneratorMeta) in;
  }

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
    shell.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Filename line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
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
    Control lastControl = wTransformName;

    wlLimit = new Label(shell, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.Limit.Label"));
    props.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.right = new FormAttachment(middle, -margin);
    fdlLimit.top = new FormAttachment(lastControl, margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.top = new FormAttachment(lastControl, margin);
    fdLimit.right = new FormAttachment(100, 0);
    wLimit.setLayoutData(fdLimit);
    lastControl = wLimit;

    Label wlNeverEnding = new Label(shell, SWT.RIGHT);
    wlNeverEnding.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.NeverEnding.Label"));
    props.setLook(wlNeverEnding);
    FormData fdlNeverEnding = new FormData();
    fdlNeverEnding.left = new FormAttachment(0, 0);
    fdlNeverEnding.right = new FormAttachment(middle, -margin);
    fdlNeverEnding.top = new FormAttachment(lastControl, margin);
    wlNeverEnding.setLayoutData(fdlNeverEnding);
    wNeverEnding = new Button(shell, SWT.CHECK);
    props.setLook(wNeverEnding);
    wNeverEnding.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setActive();
            input.setChanged();
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
    props.setLook(wlInterval);
    FormData fdlInterval = new FormData();
    fdlInterval.left = new FormAttachment(0, 0);
    fdlInterval.right = new FormAttachment(middle, -margin);
    fdlInterval.top = new FormAttachment(lastControl, margin);
    wlInterval.setLayoutData(fdlInterval);
    wInterval = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wInterval);
    wInterval.addModifyListener(lsMod);
    FormData fdInterval = new FormData();
    fdInterval.left = new FormAttachment(middle, 0);
    fdInterval.top = new FormAttachment(lastControl, margin);
    fdInterval.right = new FormAttachment(100, 0);
    wInterval.setLayoutData(fdInterval);
    lastControl = wInterval;

    wlRowTimeField = new Label(shell, SWT.RIGHT);
    wlRowTimeField.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.RowTimeField.Label"));
    props.setLook(wlRowTimeField);
    FormData fdlRowTimeField = new FormData();
    fdlRowTimeField.left = new FormAttachment(0, 0);
    fdlRowTimeField.right = new FormAttachment(middle, -margin);
    fdlRowTimeField.top = new FormAttachment(lastControl, margin);
    wlRowTimeField.setLayoutData(fdlRowTimeField);
    wRowTimeField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRowTimeField);
    wRowTimeField.addModifyListener(lsMod);
    FormData fdRowTimeField = new FormData();
    fdRowTimeField.left = new FormAttachment(middle, 0);
    fdRowTimeField.top = new FormAttachment(lastControl, margin);
    fdRowTimeField.right = new FormAttachment(100, 0);
    wRowTimeField.setLayoutData(fdRowTimeField);
    lastControl = wRowTimeField;

    wlLastTimeField = new Label(shell, SWT.RIGHT);
    wlLastTimeField.setText(BaseMessages.getString(PKG, "RowGeneratorDialog.LastTimeField.Label"));
    props.setLook(wlLastTimeField);
    FormData fdlLastTimeField = new FormData();
    fdlLastTimeField.left = new FormAttachment(0, 0);
    fdlLastTimeField.right = new FormAttachment(middle, -margin);
    fdlLastTimeField.top = new FormAttachment(lastControl, margin);
    wlLastTimeField.setLayoutData(fdlLastTimeField);
    wLastTimeField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLastTimeField);
    wLastTimeField.addModifyListener(lsMod);
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
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(lastControl, margin);
    wlFields.setLayoutData(fdlFields);
    lastControl = wlFields;

    final int FieldsRows = input.getFieldName().length;

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
              new String[] {
                BaseMessages.getString(PKG, "System.Combo.Yes"),
                BaseMessages.getString(PKG, "System.Combo.No")
              })
        };

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(lastControl, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wLimit.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    lsResize =
        event -> {
          Point size = shell.getSize();
          wFields.setSize(size.x - 10, size.y - 50);
          wFields.table.setSize(size.x - 10, size.y - 50);
          wFields.redraw();
        };
    shell.addListener(SWT.Resize, lsResize);

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

  protected void setActive() {
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

    wLimit.setText(input.getRowLimit());
    wNeverEnding.setSelection(input.isNeverEnding());
    wInterval.setText(Const.NVL(input.getIntervalInMs(), ""));
    wRowTimeField.setText(Const.NVL(input.getRowTimeField(), ""));
    wLastTimeField.setText(Const.NVL(input.getLastTimeField(), ""));

    for (int i = 0; i < input.getFieldName().length; i++) {
      if (input.getFieldName()[i] != null) {
        TableItem item = wFields.table.getItem(i);
        int col = 1;
        item.setText(col++, input.getFieldName()[i]);

        String type = input.getFieldType()[i];
        String format = input.getFieldFormat()[i];
        String length = input.getFieldLength()[i] < 0 ? "" : ("" + input.getFieldLength()[i]);
        String prec = input.getFieldPrecision()[i] < 0 ? "" : ("" + input.getFieldPrecision()[i]);

        String curr = input.getCurrency()[i];
        String group = input.getGroup()[i];
        String decim = input.getDecimal()[i];
        String def = input.getValue()[i];

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
            input.isSetEmptyString()[i]
                ? BaseMessages.getString(PKG, "System.Combo.Yes")
                : BaseMessages.getString(PKG, "System.Combo.No"));
      }
    }

    wFields.setRowNums();
    wFields.optWidth(true);

    setActive();

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
    try {
      getInfo(new RowGeneratorMeta()); // to see if there is an exception
      getInfo(input); // to put the content on the input structure for real if all is well.
      dispose();
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "RowGeneratorDialog.Illegal.Dialog.Settings.Title"),
          BaseMessages.getString(PKG, "RowGeneratorDialog.Illegal.Dialog.Settings.Message"),
          e);
    }
  }

  private void getInfo(RowGeneratorMeta meta) throws HopException {
    meta.setRowLimit(wLimit.getText());
    meta.setNeverEnding(wNeverEnding.getSelection());
    meta.setIntervalInMs(wInterval.getText());
    meta.setRowTimeField(wRowTimeField.getText());
    meta.setLastTimeField(wLastTimeField.getText());

    int nrFields = wFields.nrNonEmpty();

    meta.allocate(nrFields);

    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wFields.getNonEmpty(i);

      meta.getFieldName()[i] = item.getText(1);

      meta.getFieldFormat()[i] = item.getText(3);
      String slength = item.getText(4);
      String sprec = item.getText(5);
      meta.getCurrency()[i] = item.getText(6);
      meta.getDecimal()[i] = item.getText(7);
      meta.getGroup()[i] = item.getText(8);
      meta.isSetEmptyString()[i] =
          BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(item.getText(10));

      meta.getValue()[i] = meta.isSetEmptyString()[i] ? "" : item.getText(9);
      meta.getFieldType()[i] = meta.isSetEmptyString()[i] ? "String" : item.getText(2);
      meta.getFieldLength()[i] = Const.toInt(slength, -1);
      meta.getFieldPrecision()[i] = Const.toInt(sprec, -1);
    }

    // Performs checks...
    /*
     * Commented out verification : if variables are used, this check is a pain!
     *
     * long longLimit = Const.toLong(variables.environmentSubstitute( wLimit.getText()), -1L ); if (longLimit<0) { throw
     * new HopException( BaseMessages.getString(PKG, "RowGeneratorDialog.Wrong.RowLimit.Number") ); }
     */
  }

  /**
   * Preview the data generated by this transform. This generates a pipeline using this transform &
   * a dummy and previews it.
   */
  private void preview() {
    RowGeneratorMeta oneMeta = new RowGeneratorMeta();
    try {
      getInfo(oneMeta);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "RowGeneratorDialog.Illegal.Dialog.Settings.Title"),
          BaseMessages.getString(PKG, "RowGeneratorDialog.Illegal.Dialog.Settings.Message"),
          e);
      return;
    }

    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
            variables, pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "System.Dialog.EnterPreviewSize.Title"),
            BaseMessages.getString(PKG, "System.Dialog.EnterPreviewSize.Message"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell, variables, previewMeta, new String[] {wTransformName.getText()}, new int[] {previewSize});
      progressDialog.open();

      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if (!progressDialog.isCancelled()) {
        if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
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
