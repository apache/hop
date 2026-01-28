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

package org.apache.hop.pipeline.transforms.datagrid;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
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
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class DataGridDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DataGridMeta.class;

  private CTabFolder wTabFolder;
  private Composite wDataComp;

  private TableView wFields;
  private TableView wData;

  private final DataGridMeta input;
  private final DataGridMeta dataGridMeta;
  private ModifyListener lsMod;

  private static final String SYSTEM_COMBO_YES = "System.Combo.Yes";

  public DataGridDialog(
      Shell parent, IVariables variables, DataGridMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;

    dataGridMeta = input.clone();
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "DataGridDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).preview(e -> preview()).cancel(e -> cancel()).build();

    lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // //////////////////////
    // START OF META TAB ///
    // //////////////////////

    CTabItem wMetaTab = new CTabItem(wTabFolder, SWT.NONE);
    wMetaTab.setFont(GuiResource.getInstance().getFontDefault());
    wMetaTab.setText(BaseMessages.getString(PKG, "DataGridDialog.Meta.Label"));

    Composite wMetaComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wMetaComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wMetaComp.setLayout(fileLayout);

    final int FieldsRows = input.getDataGridFields().size();

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataGridDialog.Name.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataGridDialog.Type.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataGridDialog.Format.Column"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataGridDialog.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataGridDialog.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataGridDialog.Currency.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataGridDialog.Decimal.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataGridDialog.Group.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataGridDialog.Value.SetEmptyString"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              BaseMessages.getString(PKG, SYSTEM_COMBO_YES),
              BaseMessages.getString(PKG, "System.Combo.No")),
        };

    wFields =
        new TableView(
            variables,
            wMetaComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            false,
            lsMod,
            props,
            true,
            null,
            false);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);

    wMetaComp.layout();
    wMetaTab.setControl(wMetaComp);

    // //////////////////////
    // START OF DATA TAB ///
    // //////////////////////

    CTabItem wDataTab = new CTabItem(wTabFolder, SWT.NONE);
    wDataTab.setFont(GuiResource.getInstance().getFontDefault());
    wDataTab.setText(BaseMessages.getString(PKG, "DataGridDialog.Data.Label"));

    wDataComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wDataComp);

    FormLayout filesettingLayout = new FormLayout();
    filesettingLayout.marginWidth = 3;
    filesettingLayout.marginHeight = 3;
    wDataComp.setLayout(fileLayout);

    addDataGrid(false);

    FormData fdDataComp = new FormData();
    fdDataComp.left = new FormAttachment(0, 0);
    fdDataComp.top = new FormAttachment(0, 0);
    fdDataComp.right = new FormAttachment(100, 0);
    fdDataComp.bottom = new FormAttachment(100, 0);
    wDataComp.setLayoutData(fdDataComp);

    wDataComp.layout();
    wDataTab.setControl(wDataComp);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    lsResize =
        event -> {
          Point size = shell.getSize();
          wFields.setSize(size.x - 10, size.y - 50);
          wFields.table.setSize(size.x - 10, size.y - 50);
          wFields.redraw();
        };
    shell.addListener(SWT.Resize, lsResize);

    getData();
    wTabFolder.setSelection(0);

    wTabFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            addDataGrid(true);
          }
        });
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addDataGrid(boolean refresh) {

    if (refresh) {
      // retain changes made in the dialog...
      //
      getMetaInfo(dataGridMeta);
    }

    if (refresh) {
      // Retain the data edited in the dialog...
      //
      getDataInfo(dataGridMeta);

      // Clear out the data composite and redraw it completely...
      //
      for (Control control : wDataComp.getChildren()) {
        control.dispose();
      }
    }

    ColumnInfo[] columns = new ColumnInfo[dataGridMeta.getDataGridFields().size()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] =
          new ColumnInfo(
              dataGridMeta.getDataGridFields().get(i).getName(),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false);
    }
    List<DataGridDataMeta> lines = dataGridMeta.getDataLines();
    wData = new TableView(variables, wDataComp, SWT.NONE, columns, lines.size(), lsMod, props);
    wData.setSortable(false);

    for (int i = 0; i < lines.size(); i++) {
      DataGridDataMeta line = lines.get(i);
      TableItem item = wData.table.getItem(i);

      for (int f = 0; f < line.getDatalines().size(); f++) {
        item.setText(f + 1, Const.NVL(line.getDatalines().get(f), ""));
      }
    }

    wData.setRowNums();
    wData.optWidth(true);

    FormData fdData = new FormData();
    fdData.left = new FormAttachment(0, 0);
    fdData.top = new FormAttachment(0, 0);
    fdData.right = new FormAttachment(100, 0);
    fdData.bottom = new FormAttachment(100, 0);
    wData.setLayoutData(fdData);

    wTabFolder.layout(true, true);

    wFields.nrNonEmpty();
    wFields.setContentListener(modifyEvent -> wFields.nrNonEmpty());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    getMetaData();
    addDataGrid(false);
  }

  private void getMetaData() {
    int nrFields = input.getDataGridFields().size();
    if (nrFields > wFields.table.getItemCount()) {
      nrFields = wFields.table.getItemCount();
    }
    for (int i = 0; i < nrFields; i++) {
      if (input.getDataGridFields().get(i).getName() != null) {
        TableItem item = wFields.table.getItem(i);
        int col = 1;

        item.setText(col++, input.getDataGridFields().get(i).getName());
        String type = input.getDataGridFields().get(i).getType();
        String format = input.getDataGridFields().get(i).getFormat();
        String length =
            input.getDataGridFields().get(i).getLenght() < 0
                ? ""
                : ("" + input.getDataGridFields().get(i).getLenght());
        String prec =
            input.getDataGridFields().get(i).getPrecision() < 0
                ? ""
                : ("" + input.getDataGridFields().get(i).getPrecision());

        String curr = input.getDataGridFields().get(i).getCurrency();
        String group = input.getDataGridFields().get(i).getGroup();
        String decim = input.getDataGridFields().get(i).getDecimal();

        item.setText(col++, Const.NVL(type, ""));
        item.setText(col++, Const.NVL(format, ""));
        item.setText(col++, Const.NVL(length, ""));
        item.setText(col++, Const.NVL(prec, ""));
        item.setText(col++, Const.NVL(curr, ""));
        item.setText(col++, Const.NVL(decim, ""));
        item.setText(col++, Const.NVL(group, ""));
        item.setText(
            col,
            Boolean.TRUE.equals(input.getDataGridFields().get(i).isEmptyString())
                ? BaseMessages.getString(PKG, SYSTEM_COMBO_YES)
                : BaseMessages.getString(PKG, "System.Combo.No"));
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

    getInfo(input);

    dispose();
  }

  private void getInfo(DataGridMeta meta) {
    getMetaInfo(meta);
    getDataInfo(meta);
  }

  private void getMetaInfo(DataGridMeta meta) {
    int nrFields = wFields.nrNonEmpty();

    meta.getDataGridFields().clear();

    for (int i = 0; i < nrFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      DataGridFieldMeta field = new DataGridFieldMeta();
      int col = 1;
      field.setName(item.getText(col++));
      field.setType(item.getText(col++));
      field.setFormat(item.getText(col++));
      String slength = item.getText(col++);
      String sprec = item.getText(col++);
      field.setCurrency(item.getText(col++));
      field.setDecimal(item.getText(col++));
      field.setGroup(item.getText(col++));

      try {
        field.setLenght(Integer.parseInt(slength));
      } catch (Exception e) {
        field.setLenght(-1);
      }
      try {
        field.setPrecision(Integer.parseInt(sprec));
      } catch (Exception e) {
        field.setPrecision(-1);
      }
      field.setEmptyString(
          BaseMessages.getString(PKG, SYSTEM_COMBO_YES).equalsIgnoreCase(item.getText(col)));

      if (Boolean.TRUE.equals(field.isEmptyString())) {
        field.setType("String");
      }
      meta.getDataGridFields().add(field);
    }
  }

  private void getDataInfo(DataGridMeta meta) {
    List<DataGridDataMeta> data = new ArrayList<>();

    int nrLines = wData.table.getItemCount();
    int nrFields = meta.getDataGridFields().size();

    for (int i = 0; i < nrLines; i++) {
      DataGridDataMeta line = new DataGridDataMeta();
      TableItem item = wData.table.getItem(i);
      for (int f = 0; f < nrFields; f++) {
        line.getDatalines().add(item.getText(f + 1));
      }
      data.add(line);
    }

    meta.setDataLines(data);
  }

  /**
   * Preview the data generated by this transform. This generates a pipeline using this transform &
   * a dummy and previews it.
   */
  private void preview() {
    // Create the table input reader transform...
    DataGridMeta oneMeta = new DataGridMeta();
    getInfo(oneMeta);

    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
            pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "DataGridDialog.EnterPreviewSize.Title"),
            BaseMessages.getString(PKG, "DataGridDialog.EnterPreviewSize.Message"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
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
