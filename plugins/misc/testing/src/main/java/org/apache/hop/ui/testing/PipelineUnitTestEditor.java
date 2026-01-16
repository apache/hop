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

package org.apache.hop.ui.testing;

import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestDatabaseReplacement;
import org.apache.hop.testing.VariableValue;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class PipelineUnitTestEditor extends MetadataEditor<PipelineUnitTest> {
  private static final Class<?> PKG = PipelineUnitTestEditor.class;

  private Text wName;
  private Text wDescription;
  private Combo wTestType;
  private Text wPipelineFilename;
  private TextVar wFilename;
  private TextVar wBasePath;
  private Button wAutoOpen;
  private TableView wDbReplacements;
  private TableView wVariableValues;

  private final PropsUi props;

  protected IHopMetadataProvider metadataProvider;

  public PipelineUnitTestEditor(
      HopGui hopGui, MetadataManager<PipelineUnitTest> manager, PipelineUnitTest metadata) {
    super(hopGui, manager, metadata);

    this.metadataProvider = manager.getMetadataProvider();
    props = PropsUi.getInstance();
  }

  @Override
  public void createControl(Composite parent) {

    PipelineUnitTest pipelineUnitTest = this.getMetadata();

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // The name of the unit test...
    //
    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "PipelineUnitTestDialog.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(0, margin);
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    // The description of the test...
    //
    Label wlDescription = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlDescription);
    wlDescription.setText(BaseMessages.getString(PKG, "PipelineUnitTestDialog.Description.Label"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(lastControl, margin);
    fdlDescription.left = new FormAttachment(0, 0);
    fdlDescription.right = new FormAttachment(middle, -margin);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(lastControl, margin);
    fdDescription.left = new FormAttachment(middle, 0);
    fdDescription.right = new FormAttachment(100, 0);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    // The type of test...
    //
    Label wlTestType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlTestType);
    wlTestType.setText(BaseMessages.getString(PKG, "PipelineUnitTestDialog.TestType.Label"));
    FormData fdlTestType = new FormData();
    fdlTestType.top = new FormAttachment(lastControl, margin);
    fdlTestType.left = new FormAttachment(0, 0);
    fdlTestType.right = new FormAttachment(middle, -margin);
    wlTestType.setLayoutData(fdlTestType);
    wTestType = new Combo(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTestType);
    FormData fdTestType = new FormData();
    fdTestType.top = new FormAttachment(lastControl, margin);
    fdTestType.left = new FormAttachment(middle, 0);
    fdTestType.right = new FormAttachment(100, 0);
    wTestType.setLayoutData(fdTestType);
    wTestType.setItems(DataSetConst.getTestTypeDescriptions());
    lastControl = wTestType;

    // The filename of the pipeline to test
    //
    Label wlPipelineFilename = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPipelineFilename);
    wlPipelineFilename.setText(
        BaseMessages.getString(PKG, "PipelineUnitTestDialog.PipelineFilename.Label"));
    FormData fdlPipelineFilename = new FormData();
    fdlPipelineFilename.top = new FormAttachment(lastControl, margin);
    fdlPipelineFilename.left = new FormAttachment(0, 0);
    fdlPipelineFilename.right = new FormAttachment(middle, -margin);
    wlPipelineFilename.setLayoutData(fdlPipelineFilename);

    Button wbPipelineFilename = new Button(parent, SWT.PUSH);
    PropsUi.setLook(wbPipelineFilename);
    wbPipelineFilename.setText(BaseMessages.getString(PKG, "PipelineUnitTestDialog.Button.Browse"));
    FormData fdbPipelineFilename = new FormData();
    fdbPipelineFilename.right = new FormAttachment(100, 0);
    fdbPipelineFilename.top = new FormAttachment(lastControl, margin);
    wbPipelineFilename.setLayoutData(fdbPipelineFilename);
    wbPipelineFilename.addListener(SWT.Selection, this::browsePipelineFilename);
    wPipelineFilename = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPipelineFilename);
    FormData fdPipelineFilename = new FormData();
    fdPipelineFilename.top = new FormAttachment(lastControl, margin);
    fdPipelineFilename.left = new FormAttachment(middle, 0);
    fdPipelineFilename.right = new FormAttachment(wbPipelineFilename, -margin);
    wPipelineFilename.setLayoutData(fdPipelineFilename);
    lastControl = wbPipelineFilename;

    // The optional filename of the test result...
    //
    Label wlFilename = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlFilename);
    wlFilename.setText(BaseMessages.getString(PKG, "PipelineUnitTestDialog.Filename.Label"));
    FormData fdlFilename = new FormData();
    fdlFilename.top = new FormAttachment(lastControl, margin);
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);
    wFilename = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.top = new FormAttachment(lastControl, margin);
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(100, 0);
    wFilename.setLayoutData(fdFilename);
    lastControl = wFilename;

    // The base path for relative test path resolution
    //
    Label wlBasePath = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlBasePath);
    wlBasePath.setText(BaseMessages.getString(PKG, "PipelineUnitTestDialog.BasePath.Label"));
    FormData fdlBasePath = new FormData();
    fdlBasePath.top = new FormAttachment(lastControl, margin);
    fdlBasePath.left = new FormAttachment(0, 0);
    fdlBasePath.right = new FormAttachment(middle, -margin);
    wlBasePath.setLayoutData(fdlBasePath);

    Button wbBasePath = new Button(parent, SWT.PUSH);
    PropsUi.setLook(wbBasePath);
    wbBasePath.setText(BaseMessages.getString(PKG, "PipelineUnitTestDialog.Button.Browse"));
    FormData fdbBasePath = new FormData();
    fdbBasePath.right = new FormAttachment(100, 0);
    fdbBasePath.top = new FormAttachment(lastControl, margin);
    wbBasePath.setLayoutData(fdbBasePath);
    wbBasePath.addListener(SWT.Selection, this::browseTestPathDir);

    wBasePath = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBasePath);
    FormData fdBasePath = new FormData();
    fdBasePath.top = new FormAttachment(lastControl, margin);
    fdBasePath.left = new FormAttachment(middle, 0);
    fdBasePath.right = new FormAttachment(wbBasePath, -margin);
    wBasePath.setLayoutData(fdBasePath);
    lastControl = wbBasePath;

    // Auto-open checkbox
    //
    Label wlAutoOpen = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlAutoOpen);
    wlAutoOpen.setText(BaseMessages.getString(PKG, "PipelineUnitTestDialog.AutoOpen.Label"));
    FormData fdlAutoOpen = new FormData();
    fdlAutoOpen.top = new FormAttachment(lastControl, margin);
    fdlAutoOpen.left = new FormAttachment(0, 0);
    fdlAutoOpen.right = new FormAttachment(middle, -margin);
    wlAutoOpen.setLayoutData(fdlAutoOpen);
    wAutoOpen = new Button(parent, SWT.CHECK);
    PropsUi.setLook(wAutoOpen);
    FormData fdAutoOpen = new FormData();
    fdAutoOpen.top = new FormAttachment(wlAutoOpen, 0, SWT.CENTER);
    fdAutoOpen.left = new FormAttachment(middle, 0);
    wAutoOpen.setLayoutData(fdAutoOpen);
    lastControl = wAutoOpen;

    // The list of database replacements in the unit test pipeline
    //
    Label wlDbReplacements = new Label(parent, SWT.NONE);
    wlDbReplacements.setText(
        BaseMessages.getString(PKG, "PipelineUnitTestDialog.DbReplacements.Label"));
    PropsUi.setLook(wlDbReplacements);
    FormData fdlDbReplacements = new FormData();
    fdlDbReplacements.left = new FormAttachment(0, 0);
    fdlDbReplacements.top = new FormAttachment(lastControl, 2 * margin);
    wlDbReplacements.setLayoutData(fdlDbReplacements);
    lastControl = wlDbReplacements;

    // the database replacements
    //
    List<String> dbNames;
    try {
      dbNames = metadataProvider.getSerializer(DatabaseMeta.class).listObjectNames();
      Collections.sort(dbNames);
    } catch (HopException e) {
      LogChannel.UI.logError("Error getting list of databases", e);
      dbNames = Collections.emptyList();
    }
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "PipelineUnitTestDialog.DbReplacement.ColumnInfo.OriginalDb"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              dbNames.toArray(new String[0]),
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "PipelineUnitTestDialog.DbReplacement.ColumnInfo.ReplacementDb"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              dbNames.toArray(new String[0]),
              false),
        };
    columns[0].setUsingVariables(true);
    columns[1].setUsingVariables(true);

    wDbReplacements =
        new TableView(
            new Variables(),
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            columns,
            pipelineUnitTest.getTweaks().size(),
            null,
            props);

    FormData fdDbReplacements = new FormData();
    fdDbReplacements.left = new FormAttachment(0, 0);
    fdDbReplacements.top = new FormAttachment(lastControl, margin);
    fdDbReplacements.right = new FormAttachment(100, 0);
    fdDbReplacements.bottom = new FormAttachment(50, -margin);
    wDbReplacements.setLayoutData(fdDbReplacements);
    lastControl = wDbReplacements;

    Label wlVariableValues = new Label(parent, SWT.NONE);
    wlVariableValues.setText(
        BaseMessages.getString(PKG, "PipelineUnitTestDialog.VariableValues.Label"));
    PropsUi.setLook(wlVariableValues);
    FormData fdlVariableValues = new FormData();
    fdlVariableValues.left = new FormAttachment(0, 0);
    fdlVariableValues.top = new FormAttachment(lastControl, 2 * margin);
    wlVariableValues.setLayoutData(fdlVariableValues);
    lastControl = wlVariableValues;

    ColumnInfo[] varValColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "PipelineUnitTestDialog.VariableValues.ColumnInfo.VariableName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "PipelineUnitTestDialog.VariableValues.ColumnInfo.VariableValue"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };
    varValColumns[0].setUsingVariables(true);
    varValColumns[1].setUsingVariables(true);

    wVariableValues =
        new TableView(
            new Variables(),
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            varValColumns,
            pipelineUnitTest.getVariableValues().size(),
            null,
            props);

    FormData fdVariableValues = new FormData();
    fdVariableValues.left = new FormAttachment(0, 0);
    fdVariableValues.top = new FormAttachment(lastControl, margin);
    fdVariableValues.right = new FormAttachment(100, 0);
    fdVariableValues.bottom = new FormAttachment(100, -2 * margin);
    wVariableValues.setLayoutData(fdVariableValues);
    lastControl = wVariableValues;

    setWidgetsContent();

    // Add listener to detect change after loading data
    Listener listener = e -> setChanged();
    ModifyListener modifyListener = e -> setChanged();
    wName.addListener(SWT.Modify, listener);
    wDescription.addListener(SWT.Modify, listener);
    wTestType.addListener(SWT.Modify, listener);
    wPipelineFilename.addListener(SWT.Modify, listener);
    wFilename.addListener(SWT.Modify, listener);
    wBasePath.addListener(SWT.Modify, listener);
    wAutoOpen.addListener(SWT.Selection, listener);
    wDbReplacements.addModifyListener(modifyListener);
    wVariableValues.addModifyListener(modifyListener);
  }

  private void browsePipelineFilename(Event event) {

    String pipelineFilename = BaseDialog.presentFileDialog(this.getShell(), null, null, false);

    // Set the name to the base folder if the name is empty
    //
    try {
      if (pipelineFilename != null && StringUtils.isEmpty(wPipelineFilename.getText())) {
        PipelineUnitTest pipelineUnitTest = this.getMetadata();
        pipelineUnitTest.setRelativeFilename(manager.getVariables(), pipelineFilename);
        wPipelineFilename.setText(Const.NVL(pipelineUnitTest.getPipelineFilename(), ""));
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting pipelineFilename", e);
      // Don't change the name
    }
  }

  private void browseTestPathDir(Event event) {

    String testPathDir = BaseDialog.presentFileDialog(this.getShell(), null, null, true);

    // Set the name to the base folder if the name is empty
    //
    try {
      if (testPathDir != null && StringUtils.isEmpty(wPipelineFilename.getText())) {
        /* PipelineUnitTest pipelineUnitTest = this.getMetadata();
        pipelineUnitTest.setRelativeFilename(manager.getVariables(), testPathDir);*/
        wBasePath.setText(Const.NVL(testPathDir, ""));
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting testPathDir", e);
      // Don't change the name
    }
  }

  @Override
  public void setWidgetsContent() {
    PipelineUnitTest pipelineUnitTest = this.getMetadata();

    wName.setText(Const.NVL(pipelineUnitTest.getName(), ""));
    wDescription.setText(Const.NVL(pipelineUnitTest.getDescription(), ""));
    wTestType.setText(
        Const.NVL(DataSetConst.getTestTypeDescription(pipelineUnitTest.getType()), ""));
    wPipelineFilename.setText(Const.NVL(pipelineUnitTest.getPipelineFilename(), ""));
    wFilename.setText(Const.NVL(pipelineUnitTest.getFilename(), ""));
    wBasePath.setText(Const.NVL(pipelineUnitTest.getBasePath(), ""));
    wAutoOpen.setSelection(pipelineUnitTest.isAutoOpening());

    for (int i = 0; i < pipelineUnitTest.getDatabaseReplacements().size(); i++) {
      PipelineUnitTestDatabaseReplacement dbReplacement =
          pipelineUnitTest.getDatabaseReplacements().get(i);
      wDbReplacements.setText(Const.NVL(dbReplacement.getOriginalDatabaseName(), ""), 1, i);
      wDbReplacements.setText(Const.NVL(dbReplacement.getReplacementDatabaseName(), ""), 2, i);
    }

    for (int i = 0; i < pipelineUnitTest.getVariableValues().size(); i++) {
      VariableValue variableValue = pipelineUnitTest.getVariableValues().get(i);
      wVariableValues.setText(Const.NVL(variableValue.getKey(), ""), 1, i);
      wVariableValues.setText(Const.NVL(variableValue.getValue(), ""), 2, i);
    }

    wDbReplacements.removeEmptyRows();
    wDbReplacements.setRowNums();
  }

  @Override
  public void getWidgetsContent(PipelineUnitTest test) {

    test.setName(wName.getText());
    test.setDescription(wDescription.getText());
    test.setType(DataSetConst.getTestTypeForDescription(wTestType.getText()));
    test.setPipelineFilename(wPipelineFilename.getText());
    test.setFilename(wFilename.getText());
    test.setBasePath(wBasePath.getText());
    test.setAutoOpening(wAutoOpen.getSelection());

    test.getDatabaseReplacements().clear();
    int nrFields = wDbReplacements.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wDbReplacements.getNonEmpty(i);
      String sourceDb = item.getText(1);
      String replaceDb = item.getText(2);
      PipelineUnitTestDatabaseReplacement dbReplacement =
          new PipelineUnitTestDatabaseReplacement(sourceDb, replaceDb);
      test.getDatabaseReplacements().add(dbReplacement);
    }
    test.getVariableValues().clear();
    int nrVars = wVariableValues.nrNonEmpty();
    for (int i = 0; i < nrVars; i++) {
      TableItem item = wVariableValues.getNonEmpty(i);
      String key = item.getText(1);
      String value = item.getText(2);
      VariableValue variableValue = new VariableValue(key, value);
      test.getVariableValues().add(variableValue);
    }
  }
}
