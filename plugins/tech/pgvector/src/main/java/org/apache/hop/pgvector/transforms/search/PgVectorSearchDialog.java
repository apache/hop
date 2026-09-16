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
package org.apache.hop.pgvector.transforms.search;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pgvector.util.PgVectorSearchFilter;
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

public class PgVectorSearchDialog extends BaseTransformDialog {

  private static final Class<?> PKG = PgVectorSearchMeta.class;

  private final PgVectorSearchMeta input;

  private MetaSelectionLine<org.apache.hop.core.database.DatabaseMeta> wConnection;
  private LabelTextVar wSchemaName;
  private LabelTextVar wTableName;
  private ComboVar wEmbeddingField;
  private LabelTextVar wTopK;
  private LabelTextVar wMinScore;
  private Button wEatRowOnNoMatch;
  private CCombo wDistanceMetric;
  private LabelTextVar wResultIdField;
  private LabelTextVar wResultDocumentIdField;
  private LabelTextVar wResultChunkIndexField;
  private LabelTextVar wResultContentField;
  private LabelTextVar wResultScoreField;
  private TableView wFilters;

  public PgVectorSearchDialog(
      Shell parent,
      IVariables variables,
      PgVectorSearchMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Control lastControl =
        createShell(BaseMessages.getString(PKG, "PgVectorSearchDialog.Shell.Title"));
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
    mainTab.setText(BaseMessages.getString(PKG, "PgVectorSearchDialog.Tab.Main"));
    Composite mainComp = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(mainComp);
    mainComp.setLayout(new FormLayout());
    mainTab.setControl(mainComp);

    CTabItem filterTab = new CTabItem(tabFolder, SWT.NONE);
    filterTab.setFont(GuiResource.getInstance().getFontDefault());
    filterTab.setText(BaseMessages.getString(PKG, "PgVectorSearchDialog.Tab.Filters"));
    Composite filterComp = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(filterComp);
    filterComp.setLayout(new FormLayout());
    filterTab.setControl(filterComp);

    tabFolder.setSelection(mainTab);
    buildMainTab(mainComp);
    buildFilterTab(filterComp);

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
    wEmbeddingField = addFieldCombo(parent, last, "PgVectorSearch.embeddingField");
    last = wEmbeddingField;
    wTopK = addText(parent, last, "PgVectorSearch.topK");
    last = wTopK;
    wMinScore = addText(parent, last, "PgVectorSearch.minScore");
    last = wMinScore;

    Label wlEatRow = new Label(parent, SWT.RIGHT);
    wlEatRow.setText(BaseMessages.getString(PKG, "PgVectorSearch.eatingRowOnNoMatch.Label"));
    wlEatRow.setToolTipText(
        BaseMessages.getString(PKG, "PgVectorSearch.eatingRowOnNoMatch.Tooltip"));
    PropsUi.setLook(wlEatRow);
    FormData fdlEatRow = new FormData();
    fdlEatRow.left = new FormAttachment(0, 0);
    fdlEatRow.right = new FormAttachment(middle, -margin);
    fdlEatRow.top = new FormAttachment(last, margin);
    wlEatRow.setLayoutData(fdlEatRow);

    wEatRowOnNoMatch = new Button(parent, SWT.CHECK);
    wEatRowOnNoMatch.setToolTipText(
        BaseMessages.getString(PKG, "PgVectorSearch.eatingRowOnNoMatch.Tooltip"));
    PropsUi.setLook(wEatRowOnNoMatch);
    FormData fdEatRow = new FormData();
    fdEatRow.left = new FormAttachment(middle, 0);
    fdEatRow.right = new FormAttachment(100, 0);
    fdEatRow.top = new FormAttachment(wlEatRow, 0, SWT.CENTER);
    wEatRowOnNoMatch.setLayoutData(fdEatRow);
    wEatRowOnNoMatch.addListener(SWT.Selection, e -> input.setChanged());
    last = wEatRowOnNoMatch;

    Label wlMetric = new Label(parent, SWT.RIGHT);
    wlMetric.setText(BaseMessages.getString(PKG, "PgVectorSearch.distanceMetric.Label"));
    wlMetric.setToolTipText(BaseMessages.getString(PKG, "PgVectorSearch.distanceMetric.Tooltip"));
    PropsUi.setLook(wlMetric);
    FormData fdlMetric = new FormData();
    fdlMetric.left = new FormAttachment(0, 0);
    fdlMetric.right = new FormAttachment(middle, -margin);
    fdlMetric.top = new FormAttachment(last, margin);
    wlMetric.setLayoutData(fdlMetric);
    wDistanceMetric = new CCombo(parent, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wDistanceMetric);
    wDistanceMetric.setItems(
        new String[] {
          VectorDistanceMetric.COSINE.name(),
          VectorDistanceMetric.L2.name(),
          VectorDistanceMetric.INNER_PRODUCT.name()
        });
    wDistanceMetric.addModifyListener(lsMod);
    FormData fdMetric = new FormData();
    fdMetric.left = new FormAttachment(middle, 0);
    fdMetric.top = new FormAttachment(last, margin);
    fdMetric.right = new FormAttachment(100, 0);
    wDistanceMetric.setLayoutData(fdMetric);
    last = wDistanceMetric;

    wResultIdField = addText(parent, last, "PgVectorSearch.resultIdField");
    last = wResultIdField;
    wResultDocumentIdField = addText(parent, last, "PgVectorSearch.resultDocumentIdField");
    last = wResultDocumentIdField;
    wResultChunkIndexField = addText(parent, last, "PgVectorSearch.resultChunkIndexField");
    last = wResultChunkIndexField;
    wResultContentField = addText(parent, last, "PgVectorSearch.resultContentField");
    last = wResultContentField;
    wResultScoreField = addText(parent, last, "PgVectorSearch.resultScoreField");
  }

  private void buildFilterTab(Composite parent) {
    Label wlFilters = new Label(parent, SWT.LEFT);
    wlFilters.setText(BaseMessages.getString(PKG, "PgVectorSearchDialog.Filters.Label"));
    wlFilters.setToolTipText(BaseMessages.getString(PKG, "PgVectorSearchDialog.Filters.Tooltip"));
    PropsUi.setLook(wlFilters);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.top = new FormAttachment(0, margin);
    wlFilters.setLayoutData(fdl);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "PgVectorSearchDialog.Filters.Column.Table"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PgVectorSearchDialog.Filters.Column.Stream"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {},
              true)
        };

    int rows = input.getFilters() != null ? input.getFilters().size() : 0;
    wFilters =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            rows,
            lsMod,
            props);
    FormData fdFilters = new FormData();
    fdFilters.left = new FormAttachment(0, 0);
    fdFilters.right = new FormAttachment(100, 0);
    fdFilters.top = new FormAttachment(wlFilters, margin);
    fdFilters.bottom = new FormAttachment(100, 0);
    wFilters.setLayoutData(fdFilters);
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
            populateStreamFields(combo);
          }
        });
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(previous, margin);
    fd.right = new FormAttachment(100, 0);
    combo.setLayoutData(fd);
    return combo;
  }

  private void populateStreamFields(ComboVar source) {
    try {
      String current = source.getText();
      String[] fieldNames = getPreviousFieldNames();
      source.removeAll();
      if (fieldNames != null) {
        source.setItems(fieldNames);
      }
      if (wFilters != null && fieldNames != null) {
        wFilters.setColumnInfo(
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
    wEmbeddingField.setText(Const.NVL(input.getEmbeddingField(), ""));
    wTopK.setText(Integer.toString(input.getTopK()));
    wMinScore.setText(Double.toString(input.getMinScore()));
    wEatRowOnNoMatch.setSelection(input.isEatingRowOnNoMatch());
    wDistanceMetric.setText(
        input.getDistanceMetric() != null
            ? input.getDistanceMetric().name()
            : VectorDistanceMetric.COSINE.name());
    wResultIdField.setText(Const.NVL(input.getResultIdField(), ""));
    wResultDocumentIdField.setText(Const.NVL(input.getResultDocumentIdField(), ""));
    wResultChunkIndexField.setText(Const.NVL(input.getResultChunkIndexField(), ""));
    wResultContentField.setText(Const.NVL(input.getResultContentField(), ""));
    wResultScoreField.setText(Const.NVL(input.getResultScoreField(), ""));
    if (input.getFilters() != null) {
      for (PgVectorSearchFilter filter : input.getFilters()) {
        TableItem item = new TableItem(wFilters.table, SWT.NONE);
        item.setText(1, Const.NVL(filter.getColumnName(), ""));
        item.setText(2, Const.NVL(filter.getStreamField(), ""));
      }
      wFilters.removeEmptyRows();
      wFilters.setRowNums();
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
    input.setEmbeddingField(wEmbeddingField.getText());
    input.setTopK(Const.toInt(wTopK.getText(), 5));
    input.setMinScore(Const.toDouble(wMinScore.getText(), 0.0));
    input.setEatingRowOnNoMatch(wEatRowOnNoMatch.getSelection());
    input.setDistanceMetric(VectorDistanceMetric.fromString(wDistanceMetric.getText()));
    input.setResultIdField(wResultIdField.getText());
    input.setResultDocumentIdField(wResultDocumentIdField.getText());
    input.setResultChunkIndexField(wResultChunkIndexField.getText());
    input.setResultContentField(wResultContentField.getText());
    input.setResultScoreField(wResultScoreField.getText());
    input.setFilters(readFilters());
    dispose();
  }

  private List<PgVectorSearchFilter> readFilters() {
    List<PgVectorSearchFilter> filters = new ArrayList<>();
    for (TableItem item : wFilters.getNonEmptyItems()) {
      String columnName = item.getText(1);
      String streamField = item.getText(2);
      if (!Utils.isEmpty(columnName) && !Utils.isEmpty(streamField)) {
        filters.add(new PgVectorSearchFilter(columnName, streamField));
      }
    }
    return filters;
  }
}
