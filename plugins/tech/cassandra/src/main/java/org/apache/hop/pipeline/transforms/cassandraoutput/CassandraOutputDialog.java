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
 *
 */
package org.apache.hop.pipeline.transforms.cassandraoutput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databases.cassandra.metadata.CassandraConnection;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.databases.cassandra.spi.ITableMetaData;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CassandraUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.List;

/** Dialog class for the CassandraOutput transform. */
public class CassandraOutputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = CassandraOutputMeta.class;

  private final CassandraOutputMeta input;

  /** various UI bits and pieces for the dialog */
  private CTabFolder wTabFolder;

  private MetaSelectionLine<CassandraConnection> wConnection;

  private CCombo wTable;

  private TextVar wConsistency;

  private TextVar wBatchSize;

  private TextVar wBatchInsertTimeout;
  private TextVar wSubBatchSize;

  private Button wUnloggedBatch;

  private Label wlKeyField;
  private TextVar wKeyField;

  private Button wbGetFields;

  private Button wbCreateTable;

  private TextVar wWithClause;

  private Button wTruncateTable;

  private Button wUpdateTableMetaData;

  private Button wInsertFieldsNotInTableMeta;

  private CCombo wTtlUnits;
  private TextVar wTtlValue;

  public CassandraOutputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String name) {

    super(parent, variables, (BaseTransformMeta) in, tr, name);

    input = (CassandraOutputMeta) in;
  }

  @Override
  public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Buttons at the bottom of the dialog
    //
    wOk = new Button(shell, SWT.PUSH | SWT.CENTER);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH | SWT.CENTER);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, wTabFolder);

    // transformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.transformName.Label"));
    props.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.right = new FormAttachment(100, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    wTransformName.setLayoutData(fdTransformName);

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    wTabFolder.setSimple(false);

    // start of the connection tab
    CTabItem wConnectionTab = new CTabItem(wTabFolder, SWT.BORDER);
    wConnectionTab.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.Tab.Connection"));

    Composite wConnectionComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wConnectionComp);

    FormLayout connectionLayout = new FormLayout();
    connectionLayout.marginWidth = 3;
    connectionLayout.marginHeight = 3;
    wConnectionComp.setLayout(connectionLayout);

    // Connection line
    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            CassandraConnection.class,
            wConnectionComp,
            SWT.NONE,
            BaseMessages.getString(PKG, "CassandraOutputDialog.Connection.Label"),
            BaseMessages.getString(PKG, "CassandraOutputDialog.Connection.Tooltip"));
    props.setLook(wConnection);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(0, margin);
    wConnection.setLayoutData(fdConnection);

    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error listing Cassandra connection metadata objects", e);
    }

    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wConnectionComp.setLayoutData(fd);

    wConnectionComp.layout();
    wConnectionTab.setControl(wConnectionComp);

    // --- start of the write tab ---
    CTabItem wWriteTab = new CTabItem(wTabFolder, SWT.NONE);
    wWriteTab.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.Tab.Write"));
    Composite wWriteComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wWriteComp);

    FormLayout writeLayout = new FormLayout();
    writeLayout.marginWidth = 3;
    writeLayout.marginHeight = 3;
    wWriteComp.setLayout(writeLayout);

    // table line
    Label wlTable = new Label(wWriteComp, SWT.RIGHT);
    props.setLook(wlTable);
    wlTable.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.Table.Label"));
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.top = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    wlTable.setLayoutData(fdlTable);

    Button wbGetTables = new Button(wWriteComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetTables);
    wbGetTables.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.GetTable.Button"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wlTable, 0, SWT.CENTER);
    wbGetTables.setLayoutData(fdbTable);
    wbGetTables.addListener(SWT.Selection, e -> setupTablesCombo());

    wTable = new CCombo(wWriteComp, SWT.BORDER);
    props.setLook(wTable);
    wTable.addModifyListener(e -> wTable.setToolTipText(variables.resolve(wTable.getText())));
    FormData fdTable = new FormData();
    fdTable.right = new FormAttachment(wbGetTables, -margin);
    fdTable.top = new FormAttachment(wlTable, 0, SWT.CENTER);
    fdTable.left = new FormAttachment(middle, 0);
    wTable.setLayoutData(fdTable);

    // consistency line
    Label wlConsistency = new Label(wWriteComp, SWT.RIGHT);
    props.setLook(wlConsistency);
    wlConsistency.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.Consistency.Label"));
    wlConsistency.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.Consistency.Label.TipText"));
    FormData fdlConsistency = new FormData();
    fdlConsistency.left = new FormAttachment(0, 0);
    fdlConsistency.top = new FormAttachment(wTable, margin);
    fdlConsistency.right = new FormAttachment(middle, -margin);
    wlConsistency.setLayoutData(fdlConsistency);

    wConsistency = new TextVar(variables, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wConsistency);
    wConsistency.addModifyListener(
        e -> wConsistency.setToolTipText(variables.resolve(wConsistency.getText())));
    FormData fdConsistency = new FormData();
    fdConsistency.right = new FormAttachment(100, 0);
    fdConsistency.top = new FormAttachment(wTable, margin);
    fdConsistency.left = new FormAttachment(middle, 0);
    wConsistency.setLayoutData(fdConsistency);

    // batch size line
    Label wlBatchSize = new Label(wWriteComp, SWT.RIGHT);
    props.setLook(wlBatchSize);
    wlBatchSize.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.BatchSize.Label"));
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment(0, 0);
    fdlBatchSize.top = new FormAttachment(wConsistency, margin);
    fdlBatchSize.right = new FormAttachment(middle, -margin);
    wlBatchSize.setLayoutData(fdlBatchSize);
    wlBatchSize.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.BatchSize.TipText"));

    wBatchSize = new TextVar(variables, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBatchSize);
    wBatchSize.addModifyListener(
        e -> wBatchSize.setToolTipText(variables.resolve(wBatchSize.getText())));
    FormData fdBatchSize = new FormData();
    fdBatchSize.right = new FormAttachment(100, 0);
    fdBatchSize.top = new FormAttachment(wConsistency, margin);
    fdBatchSize.left = new FormAttachment(middle, 0);
    wBatchSize.setLayoutData(fdBatchSize);

    // batch insert timeout
    Label wlBatchInsertTimeout = new Label(wWriteComp, SWT.RIGHT);
    props.setLook(wlBatchInsertTimeout);
    wlBatchInsertTimeout.setText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.BatchInsertTimeout.Label"));
    FormData fdlBatchInsertTimeout = new FormData();
    fdlBatchInsertTimeout.left = new FormAttachment(0, 0);
    fdlBatchInsertTimeout.top = new FormAttachment(wBatchSize, margin);
    fdlBatchInsertTimeout.right = new FormAttachment(middle, -margin);
    wlBatchInsertTimeout.setLayoutData(fdlBatchInsertTimeout);
    wlBatchInsertTimeout.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.BatchInsertTimeout.TipText"));

    wBatchInsertTimeout = new TextVar(variables, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBatchInsertTimeout);
    wBatchInsertTimeout.addModifyListener(
        e -> wBatchInsertTimeout.setToolTipText(variables.resolve(wBatchInsertTimeout.getText())));
    FormData fdBatchInsertTimeout = new FormData();
    fdBatchInsertTimeout.right = new FormAttachment(100, 0);
    fdBatchInsertTimeout.top = new FormAttachment(wBatchSize, margin);
    fdBatchInsertTimeout.left = new FormAttachment(middle, 0);
    wBatchInsertTimeout.setLayoutData(fdBatchInsertTimeout);

    // sub-batch size
    Label wlSubBatchSize = new Label(wWriteComp, SWT.RIGHT);
    props.setLook(wlSubBatchSize);
    wlSubBatchSize.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.SubBatchSize.Label"));
    wlSubBatchSize.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.SubBatchSize.TipText"));
    FormData fdlSubBatchSize = new FormData();
    fdlSubBatchSize.left = new FormAttachment(0, 0);
    fdlSubBatchSize.top = new FormAttachment(wBatchInsertTimeout, margin);
    fdlSubBatchSize.right = new FormAttachment(middle, -margin);
    wlSubBatchSize.setLayoutData(fdlSubBatchSize);

    wSubBatchSize = new TextVar(variables, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSubBatchSize);
    wSubBatchSize.addModifyListener(
        e -> wSubBatchSize.setToolTipText(variables.resolve(wSubBatchSize.getText())));
    FormData fdSubBatchSize = new FormData();
    fdSubBatchSize.right = new FormAttachment(100, 0);
    fdSubBatchSize.top = new FormAttachment(wBatchInsertTimeout, margin);
    fdSubBatchSize.left = new FormAttachment(middle, 0);
    wSubBatchSize.setLayoutData(fdSubBatchSize);

    // unlogged batch line
    Label wlUnloggedBatch = new Label(wWriteComp, SWT.RIGHT);
    wlUnloggedBatch.setText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.UnloggedBatch.Label"));
    wlUnloggedBatch.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.UnloggedBatch.TipText"));
    props.setLook(wlUnloggedBatch);
    FormData fdlUnloggedBatch = new FormData();
    fdlUnloggedBatch.left = new FormAttachment(0, 0);
    fdlUnloggedBatch.top = new FormAttachment(wSubBatchSize, margin);
    fdlUnloggedBatch.right = new FormAttachment(middle, -margin);
    wlUnloggedBatch.setLayoutData(fdlUnloggedBatch);

    wUnloggedBatch = new Button(wWriteComp, SWT.CHECK);
    props.setLook(wUnloggedBatch);
    FormData fdUnloggedBatch = new FormData();
    fdUnloggedBatch.right = new FormAttachment(100, 0);
    fdUnloggedBatch.top = new FormAttachment(wlUnloggedBatch, 0, SWT.CENTER);
    fdUnloggedBatch.left = new FormAttachment(middle, 0);
    wUnloggedBatch.setLayoutData(fdUnloggedBatch);

    // TTL line
    Label wlTtl = new Label(wWriteComp, SWT.RIGHT);
    props.setLook(wlTtl);
    wlTtl.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.TTL.Label"));
    FormData fdlTtl = new FormData();
    fdlTtl.left = new FormAttachment(0, 0);
    fdlTtl.top = new FormAttachment(wlUnloggedBatch, 2 * margin);
    fdlTtl.right = new FormAttachment(middle, -margin);
    wlTtl.setLayoutData(fdlTtl);

    wTtlUnits = new CCombo(wWriteComp, SWT.BORDER);
    wTtlUnits.setEditable(false);
    props.setLook(wTtlUnits);
    FormData fdTtl = new FormData();
    fdTtl.right = new FormAttachment(100, 0);
    fdTtl.top = new FormAttachment(wlTtl, 0, SWT.CENTER);
    wTtlUnits.setLayoutData(fdTtl);
    for (CassandraOutputMeta.TtlUnits u : CassandraOutputMeta.TtlUnits.values()) {
      wTtlUnits.add(u.toString());
    }
    wTtlUnits.select(0);
    wTtlUnits.addListener(
        SWT.Selection,
        e -> {
          if (wTtlUnits.getSelectionIndex() == 0) {
            wTtlValue.setEnabled(false);
            wTtlValue.setText("");
          } else {
            wTtlValue.setEnabled(true);
          }
        });

    wTtlValue = new TextVar(variables, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTtlValue);
    FormData fdTtlValue = new FormData();
    fdTtlValue.right = new FormAttachment(wTtlUnits, -2 * margin);
    fdTtlValue.top = new FormAttachment(wlTtl, 0, SWT.CENTER);
    fdTtlValue.left = new FormAttachment(middle, 0);
    wTtlValue.setLayoutData(fdTtlValue);
    wTtlValue.setEnabled(false);
    wTtlValue.addModifyListener(
        e -> wTtlValue.setToolTipText(variables.resolve(wTtlValue.getText())));

    // key field line
    wlKeyField = new Label(wWriteComp, SWT.RIGHT);
    props.setLook(wlKeyField);
    wlKeyField.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.KeyField.Label"));
    FormData fdlKeyField = new FormData();
    fdlKeyField.left = new FormAttachment(0, 0);
    fdlKeyField.top = new FormAttachment(wTtlValue, 2 * margin);
    fdlKeyField.right = new FormAttachment(middle, -margin);
    wlKeyField.setLayoutData(fdlKeyField);

    wbGetFields = new Button(wWriteComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetFields);
    wbGetFields.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.GetFields.Button"));
    FormData fdbGetFields = new FormData();
    fdbGetFields.right = new FormAttachment(100, 0);
    fdbGetFields.top = new FormAttachment(wlKeyField, 0, SWT.CENTER);
    wbGetFields.setLayoutData(fdbGetFields);
    wbGetFields.addListener(SWT.Selection, e -> showEnterSelectionDialog());

    wKeyField = new TextVar(variables, wWriteComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wKeyField.addModifyListener(
        e -> wKeyField.setToolTipText(variables.resolve(wKeyField.getText())));
    FormData fdKeyField = new FormData();
    fdKeyField.right = new FormAttachment(wbGetFields, -margin);
    fdKeyField.top = new FormAttachment(wlKeyField, 0, SWT.CENTER);
    fdKeyField.left = new FormAttachment(middle, 0);
    wKeyField.setLayoutData(fdKeyField);

    FormData fdWriteComp = new FormData();
    fdWriteComp.left = new FormAttachment(0, 0);
    fdWriteComp.top = new FormAttachment(0, 0);
    fdWriteComp.right = new FormAttachment(100, 0);
    fdWriteComp.bottom = new FormAttachment(100, 0);
    wWriteComp.setLayoutData(fdWriteComp);

    wWriteTab.setControl(wWriteComp);

    // show schema button
    Button wbShowSchema = new Button(wWriteComp, SWT.PUSH | SWT.CENTER);
    wbShowSchema.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.Schema.Button"));
    props.setLook(wbShowSchema);
    FormData fdbShowSchema = new FormData();
    fdbShowSchema.right = new FormAttachment(100, 0);
    fdbShowSchema.bottom = new FormAttachment(100, -margin * 2);
    wbShowSchema.setLayoutData(fdbShowSchema);
    wbShowSchema.addListener(SWT.Selection, e -> popupSchemaInfo());

    // ---- start of the schema options tab ----
    CTabItem wSchemaTab = new CTabItem(wTabFolder, SWT.NONE);
    wSchemaTab.setText(BaseMessages.getString(PKG, "CassandraOutputData.Tab.Schema"));

    Composite wSchemaComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSchemaComp);

    FormLayout schemaLayout = new FormLayout();
    schemaLayout.marginWidth = 3;
    schemaLayout.marginHeight = 3;
    wSchemaComp.setLayout(schemaLayout);

    // create table line
    Label wlCreateTable = new Label(wSchemaComp, SWT.RIGHT);
    props.setLook(wlCreateTable);
    wlCreateTable.setText(BaseMessages.getString(PKG, "CassandraOutputDialog.CreateTable.Label"));
    wlCreateTable.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.CreateTable.TipText"));
    FormData fdlCreateTable = new FormData();
    fdlCreateTable.left = new FormAttachment(0, 0);
    fdlCreateTable.top = new FormAttachment(0, margin);
    fdlCreateTable.right = new FormAttachment(middle, -margin);
    wlCreateTable.setLayoutData(fdlCreateTable);

    wbCreateTable = new Button(wSchemaComp, SWT.CHECK);
    wbCreateTable.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.CreateTable.TipText"));
    props.setLook(wbCreateTable);
    FormData fdCreateTable = new FormData();
    fdCreateTable.right = new FormAttachment(100, 0);
    fdCreateTable.top = new FormAttachment(wlCreateTable, 0, SWT.CENTER);
    fdCreateTable.left = new FormAttachment(middle, 0);
    wbCreateTable.setLayoutData(fdCreateTable);

    // table creation with clause line
    Label wlWithClause = new Label(wSchemaComp, SWT.RIGHT);
    wlWithClause.setText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.CreateTableWithClause.Label"));
    wlWithClause.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.CreateTableWithClause.TipText"));
    props.setLook(wlWithClause);
    FormData fdlWithClause = new FormData();
    fdlWithClause.left = new FormAttachment(0, 0);
    fdlWithClause.top = new FormAttachment(wlCreateTable, 2 * margin);
    fdlWithClause.right = new FormAttachment(middle, -margin);
    wlWithClause.setLayoutData(fdlWithClause);

    wWithClause = new TextVar(variables, wSchemaComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wWithClause);
    wWithClause.addModifyListener(
        e -> wWithClause.setToolTipText(variables.resolve(wWithClause.getText())));
    FormData fdWithClause = new FormData();
    fdWithClause.right = new FormAttachment(100, 0);
    fdWithClause.top = new FormAttachment(wlWithClause, 0, SWT.CENTER);
    fdWithClause.left = new FormAttachment(middle, 0);
    wWithClause.setLayoutData(fdWithClause);

    // truncate table line
    Label wlTruncateTable = new Label(wSchemaComp, SWT.RIGHT);
    props.setLook(wlTruncateTable);
    wlTruncateTable.setText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.TruncateTable.Label"));
    wlTruncateTable.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.TruncateTable.TipText"));
    FormData fdlTruncateTable = new FormData();
    fdlTruncateTable.left = new FormAttachment(0, 0);
    fdlTruncateTable.top = new FormAttachment(wWithClause, margin);
    fdlTruncateTable.right = new FormAttachment(middle, -margin);
    wlTruncateTable.setLayoutData(fdlTruncateTable);

    wTruncateTable = new Button(wSchemaComp, SWT.CHECK);
    wTruncateTable.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.TruncateTable.TipText"));
    props.setLook(wTruncateTable);
    FormData fdTruncateTable = new FormData();
    fdTruncateTable.right = new FormAttachment(100, 0);
    fdTruncateTable.top = new FormAttachment(wlTruncateTable, 0, SWT.CENTER);
    fdTruncateTable.left = new FormAttachment(middle, 0);
    wTruncateTable.setLayoutData(fdTruncateTable);

    // update table meta data line
    Label wlUpdateTableMetaData = new Label(wSchemaComp, SWT.RIGHT);
    props.setLook(wlUpdateTableMetaData);
    wlUpdateTableMetaData.setText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.UpdateTableMetaData.Label"));
    wlUpdateTableMetaData.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.UpdateTableMetaData.TipText"));
    FormData fdlUpdateTableMetaData = new FormData();
    fdlUpdateTableMetaData.left = new FormAttachment(0, 0);
    fdlUpdateTableMetaData.top = new FormAttachment(wlTruncateTable, 2 * margin);
    fdlUpdateTableMetaData.right = new FormAttachment(middle, -margin);
    wlUpdateTableMetaData.setLayoutData(fdlUpdateTableMetaData);

    wUpdateTableMetaData = new Button(wSchemaComp, SWT.CHECK);
    wUpdateTableMetaData.setToolTipText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.UpdateTableMetaData.TipText"));
    props.setLook(wUpdateTableMetaData);
    FormData fdUpdateTableMetaData = new FormData();
    fdUpdateTableMetaData.right = new FormAttachment(100, 0);
    fdUpdateTableMetaData.top = new FormAttachment(wlUpdateTableMetaData, 0, SWT.CENTER);
    fdUpdateTableMetaData.left = new FormAttachment(middle, 0);
    wUpdateTableMetaData.setLayoutData(fdUpdateTableMetaData);

    // insert fields not in meta line
    Label wlInsertFieldsNotInTableMeta = new Label(wSchemaComp, SWT.RIGHT);
    props.setLook(wlInsertFieldsNotInTableMeta);
    wlInsertFieldsNotInTableMeta.setText(
        BaseMessages.getString(PKG, "CassandraOutputDialog.InsertFieldsNotInTableMetaData.Label"));
    wlInsertFieldsNotInTableMeta.setToolTipText(
        BaseMessages.getString(
            PKG, "CassandraOutputDialog.InsertFieldsNotInTableMetaData.TipText"));
    FormData fdlInsertFieldsNotInTableMeta = new FormData();
    fdlInsertFieldsNotInTableMeta.left = new FormAttachment(0, 0);
    fdlInsertFieldsNotInTableMeta.top = new FormAttachment(wlUpdateTableMetaData, 2 * margin);
    fdlInsertFieldsNotInTableMeta.right = new FormAttachment(middle, -margin);
    wlInsertFieldsNotInTableMeta.setLayoutData(fdlInsertFieldsNotInTableMeta);

    wInsertFieldsNotInTableMeta = new Button(wSchemaComp, SWT.CHECK);
    wInsertFieldsNotInTableMeta.setToolTipText(
        BaseMessages.getString(
            PKG, "CassandraOutputDialog.InsertFieldsNotInTableMetaData.TipText"));
    props.setLook(wInsertFieldsNotInTableMeta);
    FormData fdInsertFieldsNotInTableMeta = new FormData();
    fdInsertFieldsNotInTableMeta.right = new FormAttachment(100, 0);
    fdInsertFieldsNotInTableMeta.top =
        new FormAttachment(wlInsertFieldsNotInTableMeta, 0, SWT.CENTER);
    fdInsertFieldsNotInTableMeta.left = new FormAttachment(middle, 0);
    wInsertFieldsNotInTableMeta.setLayoutData(fdInsertFieldsNotInTableMeta);

    FormData fdSchemaComp = new FormData();
    fdSchemaComp.left = new FormAttachment(0, 0);
    fdSchemaComp.top = new FormAttachment(0, 0);
    fdSchemaComp.right = new FormAttachment(100, 0);
    fdSchemaComp.bottom = new FormAttachment(100, 0);
    wSchemaComp.setLayoutData(fdSchemaComp);

    wSchemaComp.layout();
    wSchemaTab.setControl(wSchemaComp);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wlTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // If you hit enter in a text field: confirm and close
    //
    wTransformName.addListener(SWT.DefaultSelection, e -> ok());

    wConnection.addListener(SWT.DefaultSelection, e -> ok());

    wTable.addListener(SWT.DefaultSelection, e -> ok());
    wConsistency.addListener(SWT.DefaultSelection, e -> ok());
    wBatchSize.addListener(SWT.DefaultSelection, e -> ok());
    wBatchInsertTimeout.addListener(SWT.DefaultSelection, e -> ok());
    wSubBatchSize.addListener(SWT.DefaultSelection, e -> ok());
    wTtlValue.addListener(SWT.DefaultSelection, e -> ok());
    wKeyField.addListener(SWT.DefaultSelection, e -> ok());

    wWithClause.addListener(SWT.DefaultSelection, e -> ok());

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    wTabFolder.setSelection(0);
    setSize();

    getData();

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return transformName;
  }

  protected void setupTablesCombo() {
    Connection conn = null;
    Keyspace kSpace = null;

    try {
      String connectionName = variables.resolve(wConnection.getText());
      if (StringUtils.isEmpty(connectionName)) {
        return;
      }
      CassandraConnection cassandraConnection =
          metadataProvider.getSerializer(CassandraConnection.class).load(connectionName);

      try {
        conn = cassandraConnection.createConnection(variables, false);
        kSpace = cassandraConnection.lookupKeyspace(conn, variables);
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                    PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message")
                + ":\n\n"
                + e.getLocalizedMessage(),
            e);
        new ErrorDialog(
            shell,
            BaseMessages.getString(
                PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Title"),
            BaseMessages.getString(
                    PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message")
                + ":\n\n"
                + e.getLocalizedMessage(),
            e);
        return;
      }

      List<String> tables = kSpace.getTableNamesCQL3();
      wTable.removeAll();
      for (String famName : tables) {
        wTable.add(famName);
      }

    } catch (Exception ex) {
      logError(
          BaseMessages.getString(
                  PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message")
              + ":\n\n"
              + ex.getMessage(),
          ex);
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Title"),
          BaseMessages.getString(
                  PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message")
              + ":\n\n"
              + ex.getMessage(),
          ex);
    } finally {
      if (conn != null) {
        try {
          conn.closeConnection();
        } catch (Exception e) {
          // TODO popup another error dialog
          e.printStackTrace();
        }
      }
    }
  }

  protected void showEnterSelectionDialog() {
    TransformMeta transformMeta = pipelineMeta.findTransform(transformName);

    String[] choices = null;
    if (transformMeta != null) {
      try {
        IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

        if (row.size() == 0) {
          MessageDialog.openError(
              shell,
              BaseMessages.getString(PKG, "CassandraOutputData.Message.NoIncomingFields.Title"),
              BaseMessages.getString(PKG, "CassandraOutputData.Message.NoIncomingFields"));

          return;
        }

        choices = new String[row.size()];
        for (int i = 0; i < row.size(); i++) {
          IValueMeta vm = row.getValueMeta(i);
          choices[i] = vm.getName();
        }

        EnterSelectionDialog dialog =
            new EnterSelectionDialog(
                shell,
                choices,
                BaseMessages.getString(PKG, "CassandraOutputDialog.SelectKeyFieldsDialog.Title"),
                BaseMessages.getString(PKG, "CassandraOutputDialog.SelectKeyFieldsDialog.Message"));
        dialog.setMulti(true);
        if (!Utils.isEmpty(wKeyField.getText())) {
          String current = wKeyField.getText();
          String[] parts = current.split(",");
          int[] currentSelection = new int[parts.length];
          int count = 0;
          for (String s : parts) {
            int index = row.indexOfValue(s.trim());
            if (index >= 0) {
              currentSelection[count++] = index;
            }
          }

          dialog.setSelectedNrs(currentSelection);
        }

        dialog.open();

        int[] selected = dialog.getSelectionIndeces(); // SIC
        if (selected != null && selected.length > 0) {
          StringBuilder newSelection = new StringBuilder();
          boolean first = true;
          for (int i : selected) {
            if (first) {
              newSelection.append(choices[i]);
              first = false;
            } else {
              newSelection.append(",").append(choices[i]);
            }
          }

          wKeyField.setText(newSelection.toString());
        }
      } catch (HopException ex) {
        MessageDialog.openError(
            shell,
            BaseMessages.getString(PKG, "CassandraOutputData.Message.NoIncomingFields.Title"),
            BaseMessages.getString(PKG, "CassandraOutputData.Message.NoIncomingFields"));
      }
    }
  }

  protected void setupFieldsCombo() {
    // try and set up from incoming fields from previous transform

    TransformMeta transformMeta = pipelineMeta.findTransform(transformName);

    if (transformMeta != null) {
      try {
        IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

        if (row.size() == 0) {
          MessageDialog.openError(
              shell,
              BaseMessages.getString(PKG, "CassandraOutputData.Message.NoIncomingFields.Title"),
              BaseMessages.getString(PKG, "CassandraOutputData.Message.NoIncomingFields"));

          return;
        }
      } catch (HopException ex) {
        MessageDialog.openError(
            shell,
            BaseMessages.getString(PKG, "CassandraOutputData.Message.NoIncomingFields.Title"),
            BaseMessages.getString(PKG, "CassandraOutputData.Message.NoIncomingFields"));
      }
    }
  }

  protected void ok() {
    if (Utils.isEmpty(wlTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText();
    input.setConnectionName(wConnection.getText());
    input.setTableName(wTable.getText());
    input.setConsistency(wConsistency.getText());
    input.setBatchSize(wBatchSize.getText());
    input.setCqlBatchInsertTimeout(wBatchInsertTimeout.getText());
    input.setCqlSubBatchSize(wSubBatchSize.getText());
    input.setKeyField(wKeyField.getText());

    input.setCreateTable(wbCreateTable.getSelection());
    input.setTruncateTable(wTruncateTable.getSelection());
    input.setUpdateCassandraMeta(wUpdateTableMetaData.getSelection());
    input.setInsertFieldsNotInMeta(wInsertFieldsNotInTableMeta.getSelection());
    input.setCreateTableWithClause(wWithClause.getText());
    input.setUseUnloggedBatch(wUnloggedBatch.getSelection());

    input.setTtl(wTtlValue.getText());
    input.setTtlUnit(wTtlUnits.getText());

    input.setChanged();

    dispose();
  }

  protected void cancel() {
    transformName = null;
    dispose();
  }

  protected void popupSchemaInfo() {

    Connection conn = null;
    Keyspace kSpace = null;
    try {

      String connectionName = variables.resolve(wConnection.getText());
      if (StringUtils.isEmpty(connectionName)) {
        return;
      }
      CassandraConnection cassandraConnection =
          metadataProvider.getSerializer(CassandraConnection.class).load(connectionName);

      try {
        conn = cassandraConnection.createConnection(variables, false);
        kSpace = cassandraConnection.lookupKeyspace(conn, variables);
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                    PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message")
                + ":\n\n"
                + e.getLocalizedMessage(),
            e);
        new ErrorDialog(
            shell,
            BaseMessages.getString(
                PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Title"),
            BaseMessages.getString(
                    PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message")
                + ":\n\n"
                + e.getLocalizedMessage(),
            e);
        return;
      }

      String table = variables.resolve(wTable.getText());
      if (Utils.isEmpty(table)) {
        throw new Exception("No table name specified!");
      }
      table = CassandraUtils.cql3MixedCaseQuote(table);

      // if (!CassandraColumnMetaData.tableExists(conn, table)) {
      if (!kSpace.tableExists(table)) {
        throw new Exception(
            "The table '"
                + table
                + "' does not "
                + "seem to exist in the keyspace '"
                + cassandraConnection.getKeyspace());
      }

      ITableMetaData cassMeta = kSpace.getTableMetaData(table);
      // CassandraColumnMetaData cassMeta = new CassandraColumnMetaData(conn,
      // table);
      String schemaDescription = cassMeta.describe();
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell, SWT.ICON_INFORMATION | SWT.OK, "Schema info", schemaDescription, true);
      smd.open();
    } catch (Exception e1) {
      logError(
          BaseMessages.getString(
                  PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message")
              + ":\n\n"
              + e1.getMessage(),
          e1);
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Title"),
          BaseMessages.getString(
                  PKG, "CassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message")
              + ":\n\n"
              + e1.getMessage(),
          e1);
    } finally {
      if (conn != null) {
        try {
          conn.closeConnection();
        } catch (Exception e) {
          // TODO popup another error dialog
          e.printStackTrace();
        }
      }
    }
  }

  protected void getData() {
    wTransformName.setText(Const.NVL(transformName, ""));
    wConnection.setText(Const.NVL(input.getConnectionName(), ""));
    wTable.setText(Const.NVL(input.getTableName(), ""));
    wConsistency.setText(Const.NVL(input.getConsistency(), ""));
    wBatchSize.setText(Const.NVL(input.getBatchSize(), ""));
    wBatchInsertTimeout.setText(Const.NVL(input.getCqlBatchInsertTimeout(), ""));
    wSubBatchSize.setText(Const.NVL(input.getBatchSize(), ""));
    wKeyField.setText(Const.NVL(input.getKeyField(), ""));
    wWithClause.setText(Const.NVL(input.getCreateTableWithClause(), ""));

    wbCreateTable.setSelection(input.isCreateTable());
    wTruncateTable.setSelection(input.isTruncateTable());
    wUpdateTableMetaData.setSelection(input.isUpdateCassandraMeta());
    wInsertFieldsNotInTableMeta.setSelection(input.isInsertFieldsNotInMeta());
    wUnloggedBatch.setSelection(input.isUseUnloggedBatch());
    
    if (!Utils.isEmpty(input.getTtl())) {
      wTtlValue.setText(input.getTtl());
      wTtlUnits.setText(input.getTtlUnit());
      wTtlValue.setEnabled(wTtlUnits.getSelectionIndex() > 0);
    }
  }
}
