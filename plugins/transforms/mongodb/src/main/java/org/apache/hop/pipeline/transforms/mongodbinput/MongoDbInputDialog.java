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

package org.apache.hop.pipeline.transforms.mongodbinput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.field.MongoField;
import org.apache.hop.mongo.wrapper.field.MongodbInputDiscoverFieldsImpl;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoDbInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = MongoDbInputMeta.class; // For i18n - Translator

  private MetaSelectionLine<MongoDbConnection> wConnection;

  private TextVar wFieldsName;
  private CCombo wCollection;
  private TextVar wJsonField;

  private StyledTextComp wJsonQuery;
  private Label wlJsonQuery;
  private Button wbQueryIsPipeline;

  private Button wbOutputAsJson;
  private TableView wFields;

  private Button wbExecuteForEachRow;

  private final MongoDbInputMeta input;

  public MongoDbInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname);
    input = (MongoDbInputMeta) in;
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
    shell.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Some buttons
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

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.TransformName.Label"));
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

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // Input options tab -----
    CTabItem wInputOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wInputOptionsTab.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.InputOptionsTab.TabTitle"));
    Composite wInputOptionsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wInputOptionsComp);
    FormLayout inputLayout = new FormLayout();
    inputLayout.marginWidth = 3;
    inputLayout.marginHeight = 3;
    wInputOptionsComp.setLayout(inputLayout);

    // The connection to use...
    //
    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            MongoDbConnection.class,
            wInputOptionsComp,
            SWT.NONE,
            BaseMessages.getString(PKG, "MongoDbInputDialog.ConnectionName.Label"),
            BaseMessages.getString(PKG, "MongoDbInputDialog.ConnectionName.Tooltip"));
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(0, 0);
    wConnection.setLayoutData(fdConnection);
    lastControl = wConnection;

    try {
      wConnection.fillItems();
    } catch (HopException e) {
      new ErrorDialog(shell, "Error", "Error loading list of MongoDB connection names", e);
    }

    // Collection input ...
    //
    Label wlCollection = new Label(wInputOptionsComp, SWT.RIGHT);
    wlCollection.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Collection.Label"));
    props.setLook(wlCollection);
    FormData fdlCollection = new FormData();
    fdlCollection.left = new FormAttachment(0, 0);
    fdlCollection.right = new FormAttachment(middle, -margin);
    fdlCollection.top = new FormAttachment(lastControl, margin);
    wlCollection.setLayoutData(fdlCollection);

    Button wbGetCollections = new Button(wInputOptionsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetCollections);
    wbGetCollections.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.GetCollections.Button"));
    FormData fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(lastControl, 0);
    wbGetCollections.setLayoutData(fd);
    wbGetCollections.addListener(SWT.Selection, e -> getCollectionNames());

    wCollection = new CCombo(wInputOptionsComp, SWT.BORDER);
    props.setLook(wCollection);
    wCollection.addModifyListener(lsMod);
    FormData fdCollection = new FormData();
    fdCollection.left = new FormAttachment(middle, 0);
    fdCollection.top = new FormAttachment(lastControl, margin);
    fdCollection.right = new FormAttachment(wbGetCollections, 0);
    wCollection.setLayoutData(fdCollection);
    lastControl = wCollection;
    wCollection.addListener(SWT.Selection, e -> updateQueryTitleInfo());
    wCollection.addListener(SWT.FocusOut, e -> updateQueryTitleInfo());

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wInputOptionsComp.setLayoutData(fd);

    wInputOptionsComp.layout();
    wInputOptionsTab.setControl(wInputOptionsComp);

    // Query tab -----
    CTabItem wMongoQueryTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoQueryTab.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.QueryTab.TabTitle"));
    Composite wQueryComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wQueryComp);
    FormLayout queryLayout = new FormLayout();
    queryLayout.marginWidth = 3;
    queryLayout.marginHeight = 3;
    wQueryComp.setLayout(queryLayout);

    // fields input ...
    //
    Label wlFieldsName = new Label(wQueryComp, SWT.RIGHT);
    wlFieldsName.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.FieldsName.Label"));
    props.setLook(wlFieldsName);
    FormData fdlFieldsName = new FormData();
    fdlFieldsName.left = new FormAttachment(0, 0);
    fdlFieldsName.right = new FormAttachment(middle, -margin);
    fdlFieldsName.bottom = new FormAttachment(100, -margin);
    wlFieldsName.setLayoutData(fdlFieldsName);
    wFieldsName = new TextVar(variables, wQueryComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFieldsName);
    wFieldsName.addModifyListener(lsMod);
    FormData fdFieldsName = new FormData();
    fdFieldsName.left = new FormAttachment(middle, 0);
    fdFieldsName.bottom = new FormAttachment(100, -margin);
    fdFieldsName.right = new FormAttachment(100, 0);
    wFieldsName.setLayoutData(fdFieldsName);
    lastControl = wFieldsName;

    Label executeForEachRLab = new Label(wQueryComp, SWT.RIGHT);
    executeForEachRLab.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.ExecuteForEachRow.Label"));
    props.setLook(executeForEachRLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.bottom = new FormAttachment(lastControl, -2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    executeForEachRLab.setLayoutData(fd);
    wbExecuteForEachRow = new Button(wQueryComp, SWT.CHECK);
    props.setLook(wbExecuteForEachRow);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(executeForEachRLab, 0, SWT.CENTER);
    wbExecuteForEachRow.setLayoutData(fd);
    lastControl = executeForEachRLab;

    Label queryIsPipelineL = new Label(wQueryComp, SWT.RIGHT);
    queryIsPipelineL.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Pipeline.Label"));
    props.setLook(queryIsPipelineL);
    fd = new FormData();
    fd.bottom = new FormAttachment(lastControl, -2 * margin);
    fd.left = new FormAttachment(0, -margin);
    fd.right = new FormAttachment(middle, -margin);
    queryIsPipelineL.setLayoutData(fd);
    wbQueryIsPipeline = new Button(wQueryComp, SWT.CHECK);
    props.setLook(wbQueryIsPipeline);
    fd = new FormData();
    fd.top = new FormAttachment(queryIsPipelineL, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wbQueryIsPipeline.setLayoutData(fd);
    wbQueryIsPipeline.addListener(SWT.Selection, e -> updateQueryTitleInfo());
    lastControl = queryIsPipelineL;

    // JSON Query input ...
    //
    wlJsonQuery = new Label(wQueryComp, SWT.NONE);
    wlJsonQuery.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.JsonQuery.Label"));
    props.setLook(wlJsonQuery);
    FormData fdlJsonQuery = new FormData();
    fdlJsonQuery.left = new FormAttachment(0, 0);
    fdlJsonQuery.right = new FormAttachment(100, -margin);
    fdlJsonQuery.top = new FormAttachment(0, margin);
    wlJsonQuery.setLayoutData(fdlJsonQuery);

    wJsonQuery =
        new StyledTextComp(
            variables, wQueryComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    props.setLook(wJsonQuery, PropsUi.WIDGET_STYLE_FIXED);
    wJsonQuery.addModifyListener(lsMod);

    /*
     * wJsonQuery = new TextVar( variables, wQueryComp, SWT.SINGLE | SWT.LEFT |
     * SWT.BORDER); props.setLook(wJsonQuery);
     * wJsonQuery.addModifyListener(lsMod);
     */
    FormData fdJsonQuery = new FormData();
    fdJsonQuery.left = new FormAttachment(0, 0);
    fdJsonQuery.top = new FormAttachment(wlJsonQuery, margin);
    fdJsonQuery.right = new FormAttachment(100, -2 * margin);
    fdJsonQuery.bottom = new FormAttachment(lastControl, -2 * margin);
    // wJsonQuery.setLayoutData(fdJsonQuery);
    wJsonQuery.setLayoutData(fdJsonQuery);
    // lastControl = wJsonQuery;
    lastControl = wJsonQuery;

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wQueryComp.setLayoutData(fd);

    wQueryComp.layout();
    wMongoQueryTab.setControl(wQueryComp);

    // fields tab -----
    CTabItem wMongoFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoFieldsTab.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.FieldsTab.TabTitle"));
    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFieldsComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout(fieldsLayout);

    // Output as Json check box
    Label outputJLab = new Label(wFieldsComp, SWT.RIGHT);
    outputJLab.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.OutputJson.Label"));
    props.setLook(outputJLab);
    fd = new FormData();
    fd.top = new FormAttachment(0, 0);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -2 * margin);
    outputJLab.setLayoutData(fd);
    wbOutputAsJson = new Button(wFieldsComp, SWT.CHECK);
    props.setLook(wbOutputAsJson);
    fd = new FormData();
    fd.top = new FormAttachment(outputJLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wbOutputAsJson.setLayoutData(fd);
    wbOutputAsJson.addListener(
        SWT.Selection,
        e -> {
          input.setChanged();
          wGet.setEnabled(!wbOutputAsJson.getSelection());
          wJsonField.setEnabled(wbOutputAsJson.getSelection());
        });
    lastControl = wbOutputAsJson;

    // JsonField input ...
    //
    Label wlJsonField = new Label(wFieldsComp, SWT.RIGHT);
    wlJsonField.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.JsonField.Label"));
    props.setLook(wlJsonField);
    FormData fdlJsonField = new FormData();
    fdlJsonField.left = new FormAttachment(0, 0);
    fdlJsonField.right = new FormAttachment(middle, -margin);
    fdlJsonField.top = new FormAttachment(lastControl, 2 * margin);
    wlJsonField.setLayoutData(fdlJsonField);
    wJsonField = new TextVar(variables, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wJsonField);
    wJsonField.addModifyListener(lsMod);
    FormData fdJsonField = new FormData();
    fdJsonField.left = new FormAttachment(middle, 0);
    fdJsonField.top = new FormAttachment(lastControl, margin);
    fdJsonField.right = new FormAttachment(100, 0);
    wJsonField.setLayoutData(fdJsonField);
    lastControl = wJsonField;

    // get fields button
    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Button.GetFields"));
    props.setLook(wGet);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fd);
    wGet.addListener(
        SWT.Selection,
        e -> {
          // populate table from schema
          MongoDbInputMeta newMeta = (MongoDbInputMeta) input.clone();
          getFields(newMeta);
        });

    // fields stuff
    final ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.FIELD_NAME"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.FIELD_PATH"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.FIELD_TYPE"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.FIELD_INDEXED"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.SAMPLE_ARRAYINFO"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.SAMPLE_PERCENTAGE"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.SAMPLE_DISPARATE_TYPES"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    colinf[2].setComboValues(ValueMetaFactory.getAllValueMetaNames());
    colinf[4].setReadOnly(true);
    colinf[5].setReadOnly(true);
    colinf[6].setReadOnly(true);

    wFields =
        new TableView(
            variables, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props);

    fd = new FormData();
    fd.top = new FormAttachment(lastControl, margin * 2);
    fd.bottom = new FormAttachment(wGet, -margin * 2);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    wFields.setLayoutData(fd);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fd);

    wFieldsComp.layout();
    wMongoFieldsTab.setControl(wFieldsComp);

    // --------------

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wTransformName, margin);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fd);

    // Add listeners
    lsDef =
        new SelectionAdapter() {
          @Override
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wCollection.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData(input);
    input.setChanged(changed);

    wTabFolder.setSelection(0);
    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData(MongoDbInputMeta meta) {
    wConnection.setText(Const.NVL(meta.getConnectionName(), ""));
    wFieldsName.setText(Const.NVL(meta.getFieldsName(), ""));
    wCollection.setText(Const.NVL(meta.getCollection(), ""));
    wJsonField.setText(Const.NVL(meta.getJsonFieldName(), ""));
    wJsonQuery.setText(Const.NVL(meta.getJsonQuery(), ""));

    wbQueryIsPipeline.setSelection(meta.isQueryIsPipeline());
    wbOutputAsJson.setSelection(meta.isOutputJson());
    wbExecuteForEachRow.setSelection(meta.getExecuteForEachIncomingRow());

    refreshFields(meta.getMongoFields());

    wJsonField.setEnabled(meta.isOutputJson());
    wGet.setEnabled(!meta.isOutputJson());

    updateQueryTitleInfo();

    wTransformName.selectAll();
  }

  private void updateQueryTitleInfo() {
    if (wbQueryIsPipeline.getSelection()) {
      wlJsonQuery.setText(
          BaseMessages.getString(PKG, "MongoDbInputDialog.JsonQuery.Label2")
              + ": db."
              + Const.NVL(wCollection.getText(), "n/a")
              + ".aggregate(...");
      wFieldsName.setEnabled(false);
    } else {
      wlJsonQuery.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.JsonQuery.Label"));
      wFieldsName.setEnabled(true);
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(MongoDbInputMeta meta) {

    meta.setConnectionName(wConnection.getText());
    meta.setFieldsName(wFieldsName.getText());
    meta.setCollection(wCollection.getText());
    meta.setJsonFieldName(wJsonField.getText());
    meta.setJsonQuery(wJsonQuery.getText());

    meta.setOutputJson(wbOutputAsJson.getSelection());
    meta.setQueryIsPipeline(wbQueryIsPipeline.getSelection());
    meta.setExecuteForEachIncomingRow(wbExecuteForEachRow.getSelection());

    int numNonEmpty = wFields.nrNonEmpty();
    if (numNonEmpty > 0) {
      List<MongoField> outputFields = new ArrayList<>();
      for (int i = 0; i < numNonEmpty; i++) {
        TableItem item = wFields.getNonEmpty(i);
        MongoField newField = new MongoField();

        newField.fieldName = item.getText(1).trim();
        newField.fieldPath = item.getText(2).trim();
        newField.hopType = item.getText(3).trim();

        if (!StringUtils.isEmpty(item.getText(4))) {
          newField.indexedValues = MongoDbInputData.indexedValsList(item.getText(4).trim());
        }

        outputFields.add(newField);
      }

      meta.setMongoFields(outputFields);
    }
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    dispose();
  }

  public boolean isTableDisposed() {
    return wFields.isDisposed();
  }

  private void refreshFields( List<MongoField> fields) {
    if (fields == null) {
      return;
    }

    wFields.clearAll();
    for (MongoField f : fields) {
      TableItem item = new TableItem(wFields.table, SWT.NONE);

      updateTableItem(item, f);
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  public void updateFieldTableFields(List<MongoField> fields) {
    Map<String, MongoField> fieldMap = new HashMap<>(fields.size());
    for (MongoField field : fields) {
      fieldMap.put(field.fieldName, field);
    }

    int index = 0;
    List<Integer> indicesToRemove = new ArrayList<>();
    for (TableItem tableItem : wFields.getTable().getItems()) {
      String name = tableItem.getText(1);
      MongoField mongoField = fieldMap.remove(name);
      if (mongoField == null) {
        // Value does not exist in incoming fields list and exists in table, remove old value from
        // table
        indicesToRemove.add(index);
      } else {
        // Value exists in incoming fields list and in table, update entry
        updateTableItem(tableItem, mongoField);
      }
      index++;
    }

    int[] indicesArray = new int[indicesToRemove.size()];
    for (int i = 0; i < indicesArray.length; i++) {
      indicesArray[i] = indicesToRemove.get(i);
    }

    for (MongoField mongoField : fieldMap.values()) {
      TableItem item = new TableItem(wFields.table, SWT.NONE);
      updateTableItem(item, mongoField);
    }
    wFields.setRowNums();
    wFields.remove(indicesArray);
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void updateTableItem(TableItem tableItem, MongoField mongoField) {
    if (!StringUtils.isEmpty(mongoField.fieldName)) {
      tableItem.setText(1, mongoField.fieldName);
    }

    if (!StringUtils.isEmpty(mongoField.fieldPath)) {
      tableItem.setText(2, mongoField.fieldPath);
    }

    if (!StringUtils.isEmpty(mongoField.hopType)) {
      tableItem.setText(3, mongoField.hopType);
    }

    if (mongoField.indexedValues != null && mongoField.indexedValues.size() > 0) {
      tableItem.setText(4, MongoDbInputData.indexedValsList(mongoField.indexedValues));
    }

    if (!StringUtils.isEmpty(mongoField.arrayIndexInfo)) {
      tableItem.setText(5, mongoField.arrayIndexInfo);
    }

    if (!StringUtils.isEmpty(mongoField.occurrenceFraction)) {
      tableItem.setText(6, mongoField.occurrenceFraction);
    }

    if (mongoField.disparateTypes) {
      tableItem.setText(7, "Y");
    }
  }

  private boolean checkForUnresolved(MongoDbInputMeta meta, String title) {

    String query = variables.resolve(meta.getJsonQuery());

    boolean notOk = (query.contains("${") || query.contains("?{"));

    if (notOk) {
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              title,
              BaseMessages.getString(
                  PKG,
                  "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs"));
      smd.open();
    }

    return !notOk;
  }

  // Used to catch exceptions from discoverFields calls that come through the callback
  public void handleNotificationException(Exception exception) {
    new ErrorDialog(
        shell,
        transformName,
        BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.ErrorDuringSampling"),
        exception);
  }

  private void getFields(MongoDbInputMeta meta) {
    if (!StringUtils.isEmpty(wConnection.getText())
        && !StringUtils.isEmpty(wCollection.getText())) {
      EnterNumberDialog end =
          new EnterNumberDialog(
              shell,
              100,
              BaseMessages.getString(PKG, "MongoDbInputDialog.SampleDocuments.Title"),
              BaseMessages.getString(PKG, "MongoDbInputDialog.SampleDocuments.Message"));
      int samples = end.open();
      if (samples > 0) {

        getInfo(meta);
        // Turn off execute for each incoming row (if set).
        // Query is still going to
        // be stuffed if the user has specified field replacement (i.e.
        // ?{...}) in the query string
        boolean current = meta.getExecuteForEachIncomingRow();
        meta.setExecuteForEachIncomingRow(false);

        if (!checkForUnresolved(
            meta,
            BaseMessages.getString(
                PKG,
                "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs.SamplingTitle"))) {

          return;
        }

        try {
          discoverFields(meta, variables, samples, metadataProvider);
          meta.setExecuteForEachIncomingRow(current);
          refreshFields(meta.getMongoFields());
        } catch (HopException e) {
          new ErrorDialog(
              shell,
              transformName,
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.ErrorDuringSampling"),
              e);
        }
      }
    } else {
      // pop up an error dialog

      String missingConDetails = "";
      if (StringUtils.isEmpty(wConnection.getText())) {
        missingConDetails += " connection name";
      }
      if (StringUtils.isEmpty(wCollection.getText())) {
        missingConDetails += " collection";
      }

      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              BaseMessages.getString(
                  PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails.Title"),
              BaseMessages.getString(
                  PKG,
                  "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails",
                  missingConDetails));
      smd.open();
    }
  }

  // Preview the data
  private void preview() {
    // Create the XML input transform
    MongoDbInputMeta oneMeta = new MongoDbInputMeta();
    getInfo(oneMeta);

    // Turn off execute for each incoming row (if set). Query is still going to
    // be stuffed if the user has specified field replacement (i.e. ?{...}) in
    // the query string
    oneMeta.setExecuteForEachIncomingRow(false);

    if (!checkForUnresolved(
        oneMeta,
        BaseMessages.getString(
            PKG,
            "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs.PreviewTitle"))) {
      return;
    }

    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
          metadataProvider, oneMeta, wTransformName.getText());

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "MongoDbInputDialog.PreviewSize.DialogTitle"),
            BaseMessages.getString(PKG, "MongoDbInputDialog.PreviewSize.DialogMessage"));
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

  private void getCollectionNames() {

    try {
      String connectionName = variables.resolve(wConnection.getText());

      String current = wCollection.getText();
      wCollection.removeAll();

      MongoDbConnection connection =
          metadataProvider.getSerializer(MongoDbConnection.class).load(connectionName);
      String databaseName = variables.resolve(connection.getDbName());

      if (!StringUtils.isEmpty(connectionName)) {

        final MongoDbInputMeta meta = new MongoDbInputMeta();
        getInfo(meta);
        try {
          MongoClientWrapper wrapper = connection.createWrapper(variables, log);
          Set<String> collections;
          try {
            collections = wrapper.getCollectionsNames(databaseName);
          } finally {
            wrapper.dispose();
          }

          for (String c : collections) {
            wCollection.add(c);
          }
        } catch (Exception e) {
          logError(
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"), e);
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
              e);
        }
      } else {
        // popup some feedback

        String missingConnDetails = "";
        if (StringUtils.isEmpty(connectionName)) {
          missingConnDetails += "connection name";
        }
        ShowMessageDialog smd =
            new ShowMessageDialog(
                shell,
                SWT.ICON_WARNING | SWT.OK,
                BaseMessages.getString(
                    PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails.Title"),
                BaseMessages.getString(
                    PKG,
                    "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails",
                    missingConnDetails));
        smd.open();
      }

      if (!StringUtils.isEmpty(current)) {
        wCollection.setText(current);
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting collections", e);
    }
  }

  public static boolean discoverFields(
      final MongoDbInputMeta meta,
      final IVariables variables,
      final int docsToSample,
      IHopMetadataProvider metadataProvider)
      throws HopException {

    String connectionName = variables.resolve(meta.getConnectionName());

    try {
      MongoDbConnection connection =
          metadataProvider.getSerializer(MongoDbConnection.class).load(connectionName);
      if (connection == null) {
        throw new HopException("Unable to find connection " + connectionName);
      }
      String collection = variables.resolve(meta.getCollection());
      String query = variables.resolve(meta.getJsonQuery());
      String fields = variables.resolve(meta.getFieldsName());
      int numDocsToSample = docsToSample;
      if (numDocsToSample < 1) {
        numDocsToSample = 100; // default
      }

      MongodbInputDiscoverFieldsImpl discoverFields = new MongodbInputDiscoverFieldsImpl();

      List<MongoField> discoveredFields =
        discoverFields
              .discoverFields(
                  variables,
                  connection,
                  collection,
                  query,
                  fields,
                  meta.isQueryIsPipeline(),
                  numDocsToSample,
                  meta);

      // return true if query resulted in documents being returned and fields
      // getting extracted
      if (discoveredFields.size() > 0) {
        meta.setMongoFields(discoveredFields);
        return true;
      }
    } catch (Exception e) {
      if (e instanceof HopException) {
        throw (HopException) e;
      } else {
        throw new HopException("Unable to discover fields from MongoDB", e);
      }
    }
    return false;
  }
}
