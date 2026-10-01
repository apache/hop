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

package org.apache.hop.pipeline.transforms.tableinput;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.PreviewTableSettingsDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.SQLStyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class TableInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = TableInputMeta.class;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextComposite wSqlComposite;

  private CCombo wDataFrom;

  private TextVar wLimit;

  private Label wlEachRow;
  private Button wEachRow;

  private Button wVariables;

  private final TableInputMeta input;

  private Label wlPosition;

  private TextVar wSqlFromFile;

  private Button wInsertField;

  private Button wUseNamedParameters;

  private Button wSpecifyFields;

  private Button wValidateSpecifiedFields;

  private TableView wFields;

  private Button wGetFields;

  public TableInputDialog(
      Shell parent, IVariables variables, TableInputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "TableInput.Name"));
    changed = input.hasChanged();

    buildButtonBar().ok(e -> ok()).preview(e -> preview()).cancel(e -> cancel()).build();

    wConnection = addConnectionLine(shell, wSpacer, input.getConnection(), lsMod);
    wConnection.addListener(SWT.Selection, e -> getSqlReservedWords());

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addSqlTab(wTabFolder);
    addOptionsTab(wTabFolder);
    addFieldsTab(wTabFolder);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wConnection, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wDataFrom.addListener(SWT.Selection, e -> setFlags());
    wDataFrom.addListener(SWT.FocusOut, e -> setFlags());

    getData();
    input.setChanged(changed);
    wTabFolder.setSelection(0);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addSqlTab(CTabFolder wTabFolder) {
    CTabItem wSqlTab = new CTabItem(wTabFolder, SWT.NONE);
    wSqlTab.setFont(GuiResource.getInstance().getFontDefault());
    wSqlTab.setText(BaseMessages.getString(PKG, "TableInputDialog.SqlTab.Title"));

    Composite wSqlComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wSqlComp);
    FormLayout sqlLayout = new FormLayout();
    sqlLayout.marginWidth = PropsUi.getFormMargin();
    sqlLayout.marginHeight = PropsUi.getFormMargin();
    wSqlComp.setLayout(sqlLayout);

    Label wlSqlFromFile = new Label(wSqlComp, SWT.RIGHT);
    wlSqlFromFile.setText(BaseMessages.getString(PKG, "TableInputDialog.LoadSqlFromFile"));
    PropsUi.setLook(wlSqlFromFile);
    FormData fdlSqlFromFile = new FormData();
    fdlSqlFromFile.left = new FormAttachment(0, 0);
    fdlSqlFromFile.right = new FormAttachment(middle, -margin);
    fdlSqlFromFile.top = new FormAttachment(0, margin);
    wlSqlFromFile.setLayoutData(fdlSqlFromFile);
    Button wbSqlFromFile = new Button(wSqlComp, SWT.PUSH);
    PropsUi.setLook(wbSqlFromFile);
    wbSqlFromFile.setText(BaseMessages.getString(PKG, "TableInputDialog.Browse"));
    FormData fdbSqlFromFile = new FormData();
    fdbSqlFromFile.right = new FormAttachment(100, 0);
    fdbSqlFromFile.top = new FormAttachment(wlSqlFromFile, 0, SWT.CENTER);
    wbSqlFromFile.setLayoutData(fdbSqlFromFile);

    wSqlFromFile = new TextVar(variables, wSqlComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSqlFromFile);
    wSqlFromFile.addModifyListener(lsMod);
    FormData fdSqlFromFile = new FormData();
    fdSqlFromFile.left = new FormAttachment(middle, 0);
    fdSqlFromFile.right = new FormAttachment(wbSqlFromFile, -margin);
    fdSqlFromFile.top = new FormAttachment(wlSqlFromFile, 0, SWT.CENTER);
    wSqlFromFile.setLayoutData(fdSqlFromFile);
    wbSqlFromFile.addListener(
        SWT.Selection,
        e -> {
          String path =
              BaseDialog.presentFileDialog(
                  shell,
                  wSqlFromFile,
                  variables,
                  new String[] {"*.sql", "*"},
                  new String[] {
                    BaseMessages.getString(PKG, "TableInputDialog.SqlFiles"),
                    BaseMessages.getString(PKG, "System.FileType.AllFiles")
                  },
                  false);
          if (path != null) {
            loadSqlFromFileAndSetReadOnly();
          }
        });
    wSqlFromFile.addModifyListener(
        e -> {
          if (Utils.isEmpty(wSqlFromFile.getText())) {
            wSqlComposite.setEditable(true);
          }
        });

    Label wlSql = new Label(wSqlComp, SWT.NONE);
    wlSql.setText(BaseMessages.getString(PKG, "TableInputDialog.SQL"));
    PropsUi.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.top = new FormAttachment(wbSqlFromFile, margin);
    wlSql.setLayoutData(fdlSql);

    Button wbTable = new Button(wSqlComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "TableInputDialog.GetSQLAndSelectStatement"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSqlFromFile, margin);
    wbTable.setLayoutData(fdbTable);
    wbTable.addListener(SWT.Selection, e -> getSql());

    wInsertField = new Button(wSqlComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wInsertField);
    wInsertField.setText(BaseMessages.getString(PKG, "TableInputDialog.InsertField"));
    FormData fdInsertField = new FormData();
    fdInsertField.right = new FormAttachment(wbTable, -margin);
    fdInsertField.top = new FormAttachment(wbSqlFromFile, margin);
    wInsertField.setLayoutData(fdInsertField);
    wInsertField.addListener(SWT.Selection, e -> insertField());

    wlPosition = new Label(wSqlComp, SWT.NONE);
    PropsUi.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.right = new FormAttachment(100, 0);
    fdlPosition.bottom = new FormAttachment(100, 0);
    wlPosition.setLayoutData(fdlPosition);

    wUseNamedParameters = new Button(wSqlComp, SWT.CHECK);
    wUseNamedParameters.setText(BaseMessages.getString(PKG, "TableInputDialog.UseNamedParameters"));
    wUseNamedParameters.setToolTipText(
        BaseMessages.getString(PKG, "TableInputDialog.UseNamedParameters.Tooltip"));
    PropsUi.setLook(wUseNamedParameters);
    FormData fdUseNamedParameters = new FormData();
    fdUseNamedParameters.left = new FormAttachment(0, 0);
    fdUseNamedParameters.right = new FormAttachment(100, 0);
    fdUseNamedParameters.bottom = new FormAttachment(wlPosition, -margin);
    wUseNamedParameters.setLayoutData(fdUseNamedParameters);
    wUseNamedParameters.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            if (wUseNamedParameters.getSelection()) {
              suggestLookupAndExecuteEachRow();
            }
            setFlags();
          }
        });

    if (EnvironmentUtils.getInstance().isWeb()) {
      wSqlComposite =
          new StyledTextComp(
              variables,
              wSqlComp,
              SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL,
              TextComposite.STYLE_TYPE_SQL);
    } else {
      wSqlComposite =
          new SQLStyledTextComp(
              variables, wSqlComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    }
    PropsUi.setLook(wSqlComposite, Props.WIDGET_STYLE_FIXED);
    wSqlComposite.addModifyListener(lsMod);
    FormData fdSql = new FormData();
    fdSql.left = new FormAttachment(0, 0);
    fdSql.top = new FormAttachment(wbTable, margin);
    fdSql.right = new FormAttachment(100, 0);
    fdSql.bottom = new FormAttachment(wUseNamedParameters, -margin);
    wSqlComposite.setLayoutData(fdSql);
    wSqlComposite.addModifyListener(
        arg0 -> {
          setSqlToolTip();
          setPosition();
        });

    wSqlComposite.addKeyListener(
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          @Override
          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });
    wSqlComposite.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          @Override
          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });
    wSqlComposite.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });

    wSqlComposite.addLineStyleListener(getSqlReservedWords());
    wSqlTab.setControl(wSqlComp);
  }

  private void addOptionsTab(CTabFolder wTabFolder) {
    CTabItem wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOptionsTab.setFont(GuiResource.getInstance().getFontDefault());
    wOptionsTab.setText(BaseMessages.getString(PKG, "TableInputDialog.OptionsTab.Title"));

    Composite wOptionsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wOptionsComp);
    FormLayout optionsLayout = new FormLayout();
    optionsLayout.marginWidth = PropsUi.getFormMargin();
    optionsLayout.marginHeight = PropsUi.getFormMargin();
    wOptionsComp.setLayout(optionsLayout);

    Label wlVariables = new Label(wOptionsComp, SWT.RIGHT);
    wlVariables.setText(BaseMessages.getString(PKG, "TableInputDialog.ReplaceVariables"));
    PropsUi.setLook(wlVariables);
    FormData fdlVariables = new FormData();
    fdlVariables.left = new FormAttachment(0, 0);
    fdlVariables.right = new FormAttachment(middle, -margin);
    fdlVariables.top = new FormAttachment(0, margin);
    wlVariables.setLayoutData(fdlVariables);
    wVariables = new Button(wOptionsComp, SWT.CHECK);
    PropsUi.setLook(wVariables);
    FormData fdVariables = new FormData();
    fdVariables.left = new FormAttachment(middle, 0);
    fdVariables.right = new FormAttachment(100, 0);
    fdVariables.top = new FormAttachment(wlVariables, 0, SWT.CENTER);
    wVariables.setLayoutData(fdVariables);
    wVariables.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
            setSqlToolTip();
          }
        });

    Label wlDatefrom = new Label(wOptionsComp, SWT.RIGHT);
    wlDatefrom.setText(BaseMessages.getString(PKG, "TableInputDialog.InsertDataFromTransform"));
    wlDatefrom.setToolTipText(
        BaseMessages.getString(PKG, "TableInputDialog.InsertDataFromTransform.Tooltip"));
    PropsUi.setLook(wlDatefrom);
    FormData fdlDatefrom = new FormData();
    fdlDatefrom.left = new FormAttachment(0, 0);
    fdlDatefrom.right = new FormAttachment(middle, -margin);
    fdlDatefrom.top = new FormAttachment(wVariables, margin);
    wlDatefrom.setLayoutData(fdlDatefrom);
    wDataFrom = new CCombo(wOptionsComp, SWT.BORDER);
    PropsUi.setLook(wDataFrom);

    List<TransformMeta> previousTransforms =
        pipelineMeta.findPreviousTransforms(pipelineMeta.findTransform(transformName));
    for (TransformMeta transformMeta : previousTransforms) {
      wDataFrom.add(transformMeta.getName());
    }

    wDataFrom.setToolTipText(wlDatefrom.getToolTipText());
    wDataFrom.addModifyListener(lsMod);
    FormData fdDatefrom = new FormData();
    fdDatefrom.left = new FormAttachment(middle, 0);
    fdDatefrom.right = new FormAttachment(100, 0);
    fdDatefrom.top = new FormAttachment(wlDatefrom, 0, SWT.CENTER);
    wDataFrom.setLayoutData(fdDatefrom);

    wlEachRow = new Label(wOptionsComp, SWT.RIGHT);
    wlEachRow.setText(BaseMessages.getString(PKG, "TableInputDialog.ExecuteForEachRow"));
    wlEachRow.setToolTipText(
        BaseMessages.getString(PKG, "TableInputDialog.ExecuteForEachRow.Tooltip"));
    PropsUi.setLook(wlEachRow);
    FormData fdlEachRow = new FormData();
    fdlEachRow.left = new FormAttachment(0, 0);
    fdlEachRow.right = new FormAttachment(middle, -margin);
    fdlEachRow.top = new FormAttachment(wDataFrom, margin);
    wlEachRow.setLayoutData(fdlEachRow);
    wEachRow = new Button(wOptionsComp, SWT.CHECK);
    wEachRow.setToolTipText(wlEachRow.getToolTipText());
    PropsUi.setLook(wEachRow);
    FormData fdEachRow = new FormData();
    fdEachRow.left = new FormAttachment(middle, 0);
    fdEachRow.right = new FormAttachment(100, 0);
    fdEachRow.top = new FormAttachment(wlEachRow, 0, SWT.CENTER);
    wEachRow.setLayoutData(fdEachRow);
    wEachRow.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        });

    Label wlLimit = new Label(wOptionsComp, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "TableInputDialog.LimitSize"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.right = new FormAttachment(middle, -margin);
    fdlLimit.top = new FormAttachment(wEachRow, margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wLimit.enableExpandedInteger();
    PropsUi.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.right = new FormAttachment(100, 0);
    fdLimit.top = new FormAttachment(wlLimit, 0, SWT.CENTER);
    wLimit.setLayoutData(fdLimit);

    wOptionsTab.setControl(wOptionsComp);
  }

  private void addFieldsTab(CTabFolder wTabFolder) {
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "TableInputDialog.FieldsTab.Title"));

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();
    wFieldsComp.setLayout(fieldsLayout);

    Label wlSpecifyFields = new Label(wFieldsComp, SWT.RIGHT);
    wlSpecifyFields.setText(BaseMessages.getString(PKG, "TableInputDialog.SpecifyFields"));
    PropsUi.setLook(wlSpecifyFields);
    FormData fdlSpecifyFields = new FormData();
    fdlSpecifyFields.left = new FormAttachment(0, 0);
    fdlSpecifyFields.right = new FormAttachment(middle, -margin);
    fdlSpecifyFields.top = new FormAttachment(0, margin);
    wlSpecifyFields.setLayoutData(fdlSpecifyFields);
    wSpecifyFields = new Button(wFieldsComp, SWT.CHECK);
    PropsUi.setLook(wSpecifyFields);
    FormData fdSpecifyFields = new FormData();
    fdSpecifyFields.left = new FormAttachment(middle, 0);
    fdSpecifyFields.right = new FormAttachment(100, 0);
    fdSpecifyFields.top = new FormAttachment(wlSpecifyFields, 0, SWT.CENTER);
    wSpecifyFields.setLayoutData(fdSpecifyFields);
    wSpecifyFields.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setSpecifyFieldsEnabled();
          }
        });

    Label wlValidate = new Label(wFieldsComp, SWT.RIGHT);
    wlValidate.setText(BaseMessages.getString(PKG, "TableInputDialog.ValidateSpecifiedFields"));
    wlValidate.setToolTipText(
        BaseMessages.getString(PKG, "TableInputDialog.ValidateSpecifiedFields.Tooltip"));
    PropsUi.setLook(wlValidate);
    FormData fdlValidate = new FormData();
    fdlValidate.left = new FormAttachment(0, 0);
    fdlValidate.right = new FormAttachment(middle, -margin);
    fdlValidate.top = new FormAttachment(wSpecifyFields, margin);
    wlValidate.setLayoutData(fdlValidate);
    wValidateSpecifiedFields = new Button(wFieldsComp, SWT.CHECK);
    wValidateSpecifiedFields.setToolTipText(wlValidate.getToolTipText());
    PropsUi.setLook(wValidateSpecifiedFields);
    FormData fdValidate = new FormData();
    fdValidate.left = new FormAttachment(middle, 0);
    fdValidate.right = new FormAttachment(100, 0);
    fdValidate.top = new FormAttachment(wlValidate, 0, SWT.CENTER);
    wValidateSpecifiedFields.setLayoutData(fdValidate);
    wValidateSpecifiedFields.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    wGetFields = new Button(wFieldsComp, SWT.PUSH);
    wGetFields.setText(BaseMessages.getString(PKG, "TableInputDialog.GetFields"));
    FormData fdGetFields = new FormData();
    fdGetFields.top = new FormAttachment(wValidateSpecifiedFields, margin);
    fdGetFields.right = new FormAttachment(100, 0);
    wGetFields.setLayoutData(fdGetFields);
    wGetFields.addListener(SWT.Selection, e -> getOutputFields());

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "TableInputDialog.ColumnInfo.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TableInputDialog.ColumnInfo.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TableInputDialog.ColumnInfo.Format"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TableInputDialog.ColumnInfo.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TableInputDialog.ColumnInfo.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };

    int fieldRows =
        input.getFields() != null && !input.getFields().isEmpty() ? input.getFields().size() : 1;
    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            colinf,
            fieldRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wGetFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);

    wFieldsTab.setControl(wFieldsComp);
  }

  private List<String> getSqlReservedWords() {
    String connectionName = wConnection != null ? wConnection.getText() : input.getConnection();
    if (Utils.isEmpty(connectionName)) {
      return List.of();
    }

    if (variables.resolve(connectionName).startsWith("${")) {
      return List.of();
    }

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
    if (databaseMeta != null) {
      return Arrays.stream(databaseMeta.getReservedWords()).toList();
    } else {
      return Collections.emptyList();
    }
  }

  public void setPosition() {
    int lineNumber = wSqlComposite.getLineNumber();
    int columnNumber = wSqlComposite.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "TableInputDialog.Position.Label", "" + lineNumber, "" + columnNumber));
  }

  private void loadSqlFromFileAndSetReadOnly() {
    String path = variables.resolve(wSqlFromFile.getText());
    if (Utils.isEmpty(path)) {
      wSqlComposite.setEditable(true);
      return;
    }
    try {
      String content = HopVfs.getTextFileContent(path, StandardCharsets.UTF_8);
      wSqlComposite.setText(content);
      wSqlComposite.setEditable(false);
    } catch (HopFileException e) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError"));
      mb.setMessage(
          BaseMessages.getString(PKG, "TableInputDialog.CouldNotLoadSqlFromFile", path)
              + Const.CR
              + e.getMessage());
      mb.open();
      wSqlComposite.setEditable(true);
    }
  }

  protected void setSqlToolTip() {
    if (wVariables.getSelection()) {
      wSqlComposite.setToolTipText(variables.resolve(wSqlComposite.getText()));
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {

    if (input.getSql() != null) {
      wSqlComposite.setText(input.getSql());
    }
    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }

    wSqlFromFile.setText(Const.NVL(input.getSqlFromFile(), ""));
    if (!Utils.isEmpty(wSqlFromFile.getText())) {
      loadSqlFromFileAndSetReadOnly();
    } else {
      wSqlComposite.setEditable(true);
    }

    wLimit.setText(Const.NVL(input.getRowLimit(), ""));
    wDataFrom.setText(Const.NVL(input.getLookup(), ""));
    wEachRow.setSelection(input.isExecuteEachInputRow());
    wVariables.setSelection(input.isVariableReplacementActive());
    wUseNamedParameters.setSelection(input.isUseNamedParameters());
    wSpecifyFields.setSelection(input.isSpecifyFields());
    wValidateSpecifiedFields.setSelection(input.isValidateSpecifiedFields());

    if (input.getFields() != null) {
      for (int i = 0; i < input.getFields().size(); i++) {
        TableInputField field = input.getFields().get(i);
        TableItem item = wFields.table.getItem(i);
        if (field.getName() != null) {
          item.setText(1, field.getName());
        }
        item.setText(2, ValueMetaFactory.getValueMetaName(field.getType()));
        item.setText(3, Const.NVL(field.getFormat(), ""));
        item.setText(4, field.getLength() < 0 ? "" : Integer.toString(field.getLength()));
        item.setText(5, field.getPrecision() < 0 ? "" : Integer.toString(field.getPrecision()));
      }
    }
    wFields.setRowNums();
    wFields.optWidth(true);

    setSqlToolTip();
    if (wUseNamedParameters.getSelection()) {
      suggestLookupTransform();
    }
    setFlags();
    setSpecifyFieldsEnabled();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(TableInputMeta meta, boolean preview) {

    meta.setConnection(wConnection.getText());

    meta.setSql(
        preview && !Utils.isEmpty(wSqlComposite.getSelectionText())
            ? wSqlComposite.getSelectionText()
            : wSqlComposite.getText());
    meta.setSqlFromFile(wSqlFromFile.getText());

    meta.setRowLimit(wLimit.getText());
    meta.setExecuteEachInputRow(wEachRow.getSelection());
    meta.setVariableReplacementActive(wVariables.getSelection());
    meta.setUseNamedParameters(wUseNamedParameters.getSelection());
    meta.setLookup(wDataFrom.getText());
    meta.setSpecifyFields(wSpecifyFields.getSelection());
    meta.setValidateSpecifiedFields(wValidateSpecifiedFields.getSelection());

    List<TableInputField> fields = new ArrayList<>();
    int nrFields = wFields.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      TableInputField field = new TableInputField();
      field.setName(item.getText(1));
      field.setType(ValueMetaFactory.getIdForValueMeta(item.getText(2)));
      field.setFormat(item.getText(3));
      field.setLength(Const.toInt(item.getText(4), -1));
      field.setPrecision(Const.toInt(item.getText(5), -1));
      fields.add(field);
    }
    meta.setFields(fields);

    // Force recreate TransformIOMeta and update info stream
    meta.resetTransformIoMeta();
    meta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value
    if (Utils.isEmpty(wConnection.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "TableInputDialog.SelectValidConnection"));
      mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError"));
      mb.open();
      return;
    }

    getInfo(input, false);
    dispose();
  }

  private IRowMeta incomingParameterFields() throws HopException {
    if (!Utils.isEmpty(wDataFrom.getText())) {
      IRowMeta selected = pipelineMeta.getTransformFields(variables, wDataFrom.getText());
      if (selected != null && !selected.isEmpty()) {
        return selected;
      }
    }
    return pipelineMeta.getPrevTransformFields(variables, transformName);
  }

  private void insertField() {
    try {
      IRowMeta prev = incomingParameterFields();
      if (prev == null || prev.isEmpty()) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        mb.setMessage(BaseMessages.getString(PKG, "TableInputDialog.NoIncomingFields"));
        mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError"));
        mb.open();
        return;
      }
      EnterSelectionDialog dialog =
          new EnterSelectionDialog(
              shell,
              prev.getFieldNames(),
              BaseMessages.getString(PKG, "TableInputDialog.InsertField.Title"),
              BaseMessages.getString(PKG, "TableInputDialog.InsertField.Message"));
      String fieldName = dialog.open();
      if (fieldName != null) {
        wSqlComposite.insert("{" + fieldName + "}");
        input.setChanged();
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError"),
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
          e);
    }
  }

  private void getSql() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases(), false, true);
      if (std.open()) {
        String sql =
            "SELECT *"
                + Const.CR
                + "FROM "
                + databaseMeta.getQuotedSchemaTableCombination(
                    variables, std.getSchemaName(), std.getTableName())
                + Const.CR;
        wSqlComposite.setText(sql);

        MessageBox yn = new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION);
        yn.setMessage(BaseMessages.getString(PKG, "TableInputDialog.IncludeFieldNamesInSQL"));
        yn.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionQuestion"));
        int id = yn.open();
        switch (id) {
          case SWT.CANCEL:
            break;
          case SWT.NO:
            wSqlComposite.setText(sql);
            break;
          case SWT.YES:
            Database db = new Database(loggingObject, variables, databaseMeta);
            try {
              db.connect();
              IRowMeta fields = db.getQueryFields(sql, false);
              if (fields != null) {
                sql = "SELECT" + Const.CR;
                for (int i = 0; i < fields.size(); i++) {
                  IValueMeta field = fields.getValueMeta(i);
                  if (i == 0) {
                    sql += "  ";
                  } else {
                    sql += ", ";
                  }
                  sql += databaseMeta.quoteField(field.getName()) + Const.CR;
                }
                sql +=
                    "FROM "
                        + databaseMeta.getQuotedSchemaTableCombination(
                            variables, std.getSchemaName(), std.getTableName())
                        + Const.CR;
                wSqlComposite.setText(sql);
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(
                    BaseMessages.getString(PKG, "TableInputDialog.ERROR_CouldNotRetrieveFields")
                        + Const.CR
                        + BaseMessages.getString(PKG, "TableInputDialog.PerhapsNoPermissions"));
                mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError2"));
                mb.open();
              }
            } catch (HopException e) {
              MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
              mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError3"));
              mb.setMessage(
                  BaseMessages.getString(PKG, "TableInputDialog.AnErrorOccurred")
                      + Const.CR
                      + e.getMessage());
              mb.open();
            } finally {
              db.close();
            }
            break;
          default:
            break;
        }
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "TableInputDialog.ConnectionNoLongerAvailable"));
      mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError4"));
      mb.open();
    }
  }

  private void getOutputFields() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "TableInputDialog.ConnectionNoLongerAvailable"));
      mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError4"));
      mb.open();
      return;
    }

    Database db = new Database(loggingObject, variables, databaseMeta);
    try {
      db.connect();
      String sql = wSqlComposite.getText();
      if (wVariables.getSelection()) {
        sql = variables.resolve(sql);
      }
      IRowMeta paramMeta = incomingParameterFields();
      Object[] paramData = null;
      if (paramMeta != null && !paramMeta.isEmpty()) {
        paramData = RowDataUtil.allocateRowData(paramMeta.size());
      } else {
        paramMeta = null;
      }
      TableInputSql.Bound bound =
          TableInputSql.prepare(wUseNamedParameters.getSelection(), sql, paramMeta, paramData);
      boolean param = bound.getParameterMeta() != null && !bound.getParameterMeta().isEmpty();
      IRowMeta fields =
          db.getQueryFields(
              bound.getJdbcSql(), param, bound.getParameterMeta(), bound.getParameterData());
      if (fields == null) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "TableInputDialog.ERROR_CouldNotRetrieveFields")
                + Const.CR
                + BaseMessages.getString(PKG, "TableInputDialog.PerhapsNoPermissions"));
        mb.setText(BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError2"));
        mb.open();
        return;
      }
      wFields.clearAll(false);
      for (IValueMeta valueMeta : fields.getValueMetaList()) {
        TableItem item = new TableItem(wFields.table, SWT.NONE);
        item.setText(1, Const.NVL(valueMeta.getName(), ""));
        item.setText(2, valueMeta.getTypeDesc());
        item.setText(3, Const.NVL(valueMeta.getConversionMask(), ""));
        item.setText(4, valueMeta.getLength() < 0 ? "" : Integer.toString(valueMeta.getLength()));
        item.setText(
            5, valueMeta.getPrecision() < 0 ? "" : Integer.toString(valueMeta.getPrecision()));
      }
      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth(true);
      input.setChanged();
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "TableInputDialog.DialogCaptionError3"),
          BaseMessages.getString(PKG, "TableInputDialog.AnErrorOccurred")
              + Const.CR
              + e.getMessage(),
          e);
    } finally {
      db.close();
    }
  }

  private boolean hasIncomingHops() {
    String[] sources = wDataFrom.getItems();
    return sources != null && sources.length > 0;
  }

  private void setFlags() {
    boolean hasIncoming = hasIncomingHops();
    boolean hasLookup = !Utils.isEmpty(wDataFrom.getText());
    wEachRow.setEnabled(hasIncoming);
    wlEachRow.setEnabled(hasIncoming);
    if (!hasIncoming) {
      wEachRow.setSelection(false);
    }
    wPreview.setEnabled(!hasLookup);
    wInsertField.setEnabled(hasIncoming && wUseNamedParameters.getSelection());
  }

  /** Fill "Insert data from transform" when there is a single incoming hop and none is selected. */
  private void suggestLookupTransform() {
    if (Utils.isEmpty(wDataFrom.getText())) {
      String[] sources = wDataFrom.getItems();
      if (sources != null && sources.length == 1) {
        wDataFrom.setText(sources[0]);
      }
    }
  }

  /**
   * Named parameters read incoming fields, so run the query once per input row and pick the source
   * transform when there is only one hop. The lookup remains optional: any incoming hop is a
   * parameter source.
   */
  private void suggestLookupAndExecuteEachRow() {
    suggestLookupTransform();
    if (hasIncomingHops()) {
      wEachRow.setSelection(true);
    }
  }

  private void setSpecifyFieldsEnabled() {
    boolean enabled = wSpecifyFields.getSelection();
    wFields.setEnabled(enabled);
    wGetFields.setEnabled(enabled);
    wValidateSpecifiedFields.setEnabled(enabled);
    if (!enabled) {
      wValidateSpecifiedFields.setSelection(false);
    }
  }

  /**
   * Preview the data generated by this transform. This generates a pipeline using this transform &
   * a dummy and previews it.
   */
  private void preview() {
    // Create the table input reader transform...
    TableInputMeta oneMeta = new TableInputMeta();
    getInfo(oneMeta, true);

    int defaultRows = props.getDefaultPreviewSize();
    PreviewTableSettingsDialog settingsDialog =
        new PreviewTableSettingsDialog(shell, Math.max(1, defaultRows), variables, true);
    PreviewTableSettingsDialog.Settings settings = settingsDialog.open();
    if (settings == null) {
      return;
    }
    int previewRows = settings.rowLimit > 0 ? settings.rowLimit : Math.max(1, defaultRows);
    oneMeta.setRowLimit(Integer.toString(previewRows));

    IVariables previewVariables = settingsDialog.getPreviewExecutionVariables();

    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
            pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

    PipelinePreviewProgressDialog progressDialog =
        new PipelinePreviewProgressDialog(
            shell,
            previewVariables,
            previewMeta,
            new String[] {wTransformName.getText()},
            new int[] {previewRows});
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
      } else {
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
}
