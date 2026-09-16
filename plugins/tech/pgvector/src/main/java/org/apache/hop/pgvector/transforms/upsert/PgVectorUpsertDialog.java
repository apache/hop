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
package org.apache.hop.pgvector.transforms.upsert;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pgvector.util.PgVectorColumnMapping;
import org.apache.hop.pgvector.util.VectorDistanceMetric;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class PgVectorUpsertDialog extends BaseTransformDialog {

  private static final Class<?> PKG = PgVectorUpsertMeta.class;

  private final PgVectorUpsertMeta input;

  private MetaSelectionLine<org.apache.hop.core.database.DatabaseMeta> wConnection;
  private LabelTextVar wSchemaName;
  private LabelTextVar wTableName;
  private ComboVar wIdField;
  private ComboVar wDocumentIdField;
  private ComboVar wChunkIndexField;
  private ComboVar wContentField;
  private ComboVar wEmbeddingField;
  private LabelTextVar wEmbeddingDimensions;
  private Button wCreateTableIfMissing;
  private Button wCreateHnswIndex;
  private Button wDeleteDocumentBeforeUpsert;
  private CCombo wIndexMetric;
  private LabelTextVar wCommitSize;
  private TableView wMappings;

  public PgVectorUpsertDialog(
      Shell parent,
      IVariables variables,
      PgVectorUpsertMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Control lastControl =
        createShell(BaseMessages.getString(PKG, "PgVectorUpsertDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    CTabFolder tabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(tabFolder);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(lastControl, margin);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(wOk, -margin * 2);
    tabFolder.setLayoutData(fdTabs);

    CTabItem mainTab = new CTabItem(tabFolder, SWT.NONE);
    mainTab.setFont(GuiResource.getInstance().getFontDefault());
    mainTab.setText(BaseMessages.getString(PKG, "PgVectorUpsertDialog.Tab.Main"));
    Composite mainComp = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(mainComp);
    mainComp.setLayout(new FormLayout());
    mainTab.setControl(mainComp);

    CTabItem mappingTab = new CTabItem(tabFolder, SWT.NONE);
    mappingTab.setFont(GuiResource.getInstance().getFontDefault());
    mappingTab.setText(BaseMessages.getString(PKG, "PgVectorUpsertDialog.Tab.Mappings"));
    Composite mappingComp = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(mappingComp);
    mappingComp.setLayout(new FormLayout());
    mappingTab.setControl(mappingComp);

    tabFolder.setSelection(mainTab);
    buildMainTab(mainComp);
    buildMappingTab(mappingComp);

    getData();
    loading = false;
    input.setChanged(changed);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void buildMainTab(Composite parent) {
    Control last = null;
    wConnection = addConnectionLine(parent, null, input.getConnection(), lsMod);
    last = wConnection;
    wSchemaName = addText(parent, last, "PgVector.schemaName");
    last = wSchemaName;
    wTableName = addText(parent, last, "PgVector.tableName");
    last = wTableName;
    wIdField = addFieldCombo(parent, last, "PgVectorUpsert.idField");
    last = wIdField;
    wDocumentIdField = addFieldCombo(parent, last, "PgVectorUpsert.documentIdField");
    last = wDocumentIdField;
    wChunkIndexField = addFieldCombo(parent, last, "PgVectorUpsert.chunkIndexField");
    last = wChunkIndexField;
    wContentField = addFieldCombo(parent, last, "PgVectorUpsert.contentField");
    last = wContentField;
    wEmbeddingField = addFieldCombo(parent, last, "PgVectorUpsert.embeddingField");
    last = wEmbeddingField;
    wEmbeddingDimensions = addText(parent, last, "PgVectorUpsert.embeddingDimensions");
    last = wEmbeddingDimensions;

    wCreateTableIfMissing = addCheck(parent, last, "PgVectorUpsert.createTableIfMissing");
    last = wCreateTableIfMissing;
    wCreateHnswIndex = addCheck(parent, last, "PgVectorUpsert.createHnswIndex");
    last = wCreateHnswIndex;
    wDeleteDocumentBeforeUpsert =
        addCheck(parent, last, "PgVectorUpsert.deleteDocumentBeforeUpsert");
    last = wDeleteDocumentBeforeUpsert;

    Label wlMetric = new Label(parent, SWT.RIGHT);
    wlMetric.setText(BaseMessages.getString(PKG, "PgVectorUpsert.indexMetric.Label"));
    wlMetric.setToolTipText(BaseMessages.getString(PKG, "PgVectorUpsert.indexMetric.Tooltip"));
    PropsUi.setLook(wlMetric);
    FormData fdlMetric = new FormData();
    fdlMetric.left = new FormAttachment(0, 0);
    fdlMetric.right = new FormAttachment(middle, -margin);
    fdlMetric.top = new FormAttachment(last, margin);
    wlMetric.setLayoutData(fdlMetric);
    wIndexMetric = new CCombo(parent, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wIndexMetric);
    wIndexMetric.setItems(
        new String[] {
          VectorDistanceMetric.COSINE.name(),
          VectorDistanceMetric.L2.name(),
          VectorDistanceMetric.INNER_PRODUCT.name()
        });
    wIndexMetric.addModifyListener(lsMod);
    FormData fdMetric = new FormData();
    fdMetric.left = new FormAttachment(middle, 0);
    fdMetric.top = new FormAttachment(last, margin);
    fdMetric.right = new FormAttachment(100, 0);
    wIndexMetric.setLayoutData(fdMetric);
    last = wIndexMetric;

    wCommitSize = addText(parent, last, "PgVectorUpsert.commitSize");
  }

  private void buildMappingTab(Composite parent) {
    Label wlMappings = new Label(parent, SWT.LEFT);
    wlMappings.setText(BaseMessages.getString(PKG, "PgVectorUpsertDialog.Mappings.Label"));
    wlMappings.setToolTipText(BaseMessages.getString(PKG, "PgVectorUpsertDialog.Mappings.Tooltip"));
    PropsUi.setLook(wlMappings);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.top = new FormAttachment(0, margin);
    wlMappings.setLayoutData(fdl);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "PgVectorUpsertDialog.Mappings.Column.Table"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PgVectorUpsertDialog.Mappings.Column.Stream"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {},
              true)
        };

    int rows = input.getColumnMappings() != null ? input.getColumnMappings().size() : 0;
    wMappings =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            rows,
            lsMod,
            props);
    FormData fdMappings = new FormData();
    fdMappings.left = new FormAttachment(0, 0);
    fdMappings.right = new FormAttachment(100, 0);
    fdMappings.top = new FormAttachment(wlMappings, margin);
    fdMappings.bottom = new FormAttachment(100, 0);
    wMappings.setLayoutData(fdMappings);
  }

  private Button addCheck(Composite parent, Control previous, String labelKey) {
    Button button = new Button(parent, SWT.CHECK);
    button.setText(BaseMessages.getString(PKG, labelKey + ".Label"));
    button.setToolTipText(BaseMessages.getString(PKG, labelKey + ".Tooltip"));
    PropsUi.setLook(button);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(previous, margin);
    fd.right = new FormAttachment(100, 0);
    button.setLayoutData(fd);
    button.addSelectionListener(
        org.eclipse.swt.events.SelectionListener.widgetSelectedAdapter(e -> input.setChanged()));
    return button;
  }

  private LabelTextVar addText(Composite parent, Control previous, String labelKey) {
    LabelTextVar widget =
        new LabelTextVar(
            variables,
            parent,
            BaseMessages.getString(PKG, labelKey + ".Label"),
            BaseMessages.getString(PKG, labelKey + ".Tooltip"));
    PropsUi.setLook(widget);
    widget.addModifyListener(lsMod);
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(previous, margin);
    fd.right = new FormAttachment(100, 0);
    widget.setLayoutData(fd);
    return widget;
  }

  private ComboVar addFieldCombo(Composite parent, Control previous, String labelKey) {
    Label label = new Label(parent, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, labelKey + ".Label"));
    label.setToolTipText(BaseMessages.getString(PKG, labelKey + ".Tooltip"));
    PropsUi.setLook(label);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    fdl.top = new FormAttachment(previous, margin);
    label.setLayoutData(fdl);
    ComboVar combo = new ComboVar(variables, parent, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(combo);
    combo.addModifyListener(lsMod);
    combo.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            populateFieldCombos(combo);
          }
        });
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(previous, margin);
    fd.right = new FormAttachment(100, 0);
    combo.setLayoutData(fd);
    return combo;
  }

  private void populateFieldCombos(ComboVar source) {
    try {
      String current = source.getText();
      String[] fieldNames = getPreviousFieldNames();
      for (ComboVar combo :
          new ComboVar[] {
            wIdField, wDocumentIdField, wChunkIndexField, wContentField, wEmbeddingField
          }) {
        String keep = combo.getText();
        combo.removeAll();
        if (fieldNames != null) {
          combo.setItems(fieldNames);
        }
        if (!Utils.isEmpty(keep)) {
          combo.setText(keep);
        }
      }
      if (wMappings != null && fieldNames != null) {
        wMappings.setColumnInfo(
            1, new ColumnInfo("", ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, true));
      }
      if (!Utils.isEmpty(current)) {
        source.setText(current);
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "PgVectorDialog.GetFields.Error.Title"),
          BaseMessages.getString(PKG, "PgVectorDialog.GetFields.Error.Message"),
          e);
    }
  }

  private String[] getPreviousFieldNames() throws HopException {
    IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformName);
    return row == null ? new String[0] : row.getFieldNames();
  }

  private void getData() {
    if (!Utils.isEmpty(input.getConnection())) {
      wConnection.setText(input.getConnection());
    }
    wSchemaName.setText(Const.NVL(input.getSchemaName(), ""));
    wTableName.setText(Const.NVL(input.getTableName(), ""));
    wIdField.setText(Const.NVL(input.getIdField(), ""));
    wDocumentIdField.setText(Const.NVL(input.getDocumentIdField(), ""));
    wChunkIndexField.setText(Const.NVL(input.getChunkIndexField(), ""));
    wContentField.setText(Const.NVL(input.getContentField(), ""));
    wEmbeddingField.setText(Const.NVL(input.getEmbeddingField(), ""));
    wEmbeddingDimensions.setText(Integer.toString(input.getEmbeddingDimensions()));
    wCreateTableIfMissing.setSelection(input.isCreateTableIfMissing());
    wCreateHnswIndex.setSelection(input.isCreateHnswIndex());
    wDeleteDocumentBeforeUpsert.setSelection(input.isDeleteDocumentBeforeUpsert());
    wIndexMetric.setText(
        input.getIndexMetric() != null
            ? input.getIndexMetric().name()
            : VectorDistanceMetric.COSINE.name());
    wCommitSize.setText(Integer.toString(input.getCommitSize()));
    if (input.getColumnMappings() != null) {
      for (PgVectorColumnMapping mapping : input.getColumnMappings()) {
        TableItem item = new TableItem(wMappings.table, SWT.NONE);
        item.setText(1, Const.NVL(mapping.getColumnName(), ""));
        item.setText(2, Const.NVL(mapping.getStreamField(), ""));
      }
      wMappings.removeEmptyRows();
      wMappings.setRowNums();
    }
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
    transformName = wTransformName.getText();
    input.setConnection(wConnection.getText());
    input.setSchemaName(wSchemaName.getText());
    input.setTableName(wTableName.getText());
    input.setIdField(wIdField.getText());
    input.setDocumentIdField(wDocumentIdField.getText());
    input.setChunkIndexField(wChunkIndexField.getText());
    input.setContentField(wContentField.getText());
    input.setEmbeddingField(wEmbeddingField.getText());
    input.setEmbeddingDimensions(Const.toInt(wEmbeddingDimensions.getText(), 768));
    input.setCreateTableIfMissing(wCreateTableIfMissing.getSelection());
    input.setCreateHnswIndex(wCreateHnswIndex.getSelection());
    input.setDeleteDocumentBeforeUpsert(wDeleteDocumentBeforeUpsert.getSelection());
    input.setIndexMetric(VectorDistanceMetric.fromString(wIndexMetric.getText()));
    input.setCommitSize(Const.toInt(wCommitSize.getText(), 100));
    input.setColumnMappings(readMappings());
    dispose();
  }

  private List<PgVectorColumnMapping> readMappings() {
    List<PgVectorColumnMapping> mappings = new ArrayList<>();
    for (TableItem item : wMappings.getNonEmptyItems()) {
      String columnName = item.getText(1);
      String streamField = item.getText(2);
      if (!Utils.isEmpty(columnName) && !Utils.isEmpty(streamField)) {
        mappings.add(new PgVectorColumnMapping(columnName, streamField));
      }
    }
    return mappings;
  }
}
