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

package org.apache.hop.workflow.actions.snowflake;

import java.sql.ResultSet;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

public class WarehouseManagerDialog extends ActionDialog implements IActionDialog {

  private static final Class<?> PKG =
      WarehouseManager.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private static final String[] MANAGEMENT_ACTION_DESCS =
      new String[] {
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Action.Create"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Action.Drop"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Action.Resume"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Action.Suspend"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Action.Alter")
      };

  private static final String[] WAREHOUSE_SIZE_DESCS =
      new String[] {
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Size.Xsmall"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Size.Small"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Size.Medium"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Size.Large"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Size.Xlarge"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Size.Xxlarge"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Size.Xxxlarge")
      };

  private static final String[] WAREHOUSE_TYPE_DESCS =
      new String[] {
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Type.Standard"),
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Type.Enterprise")
      };

  private WarehouseManager warehouseManager;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private ComboVar wWarehouseName;

  private CCombo wAction;

  private Group wCreateGroup;

  private Button wCreateReplace;

  private Button wCreateFailIfExists;

  private ComboVar wCreateWarehouseSize;

  private ComboVar wCreateWarehouseType;

  private TextVar wCreateMaxClusterSize;

  private TextVar wCreateMinClusterSize;

  private TextVar wCreateAutoSuspend;

  private Button wCreateAutoResume;

  private Button wCreateInitialSuspend;

  private ComboVar wCreateResourceMonitor;

  private TextVar wCreateComment;

  private Group wDropGroup;

  private Button wDropFailIfNotExists;

  private Group wResumeGroup;

  private Button wResumeFailIfNotExists;

  private Group wSuspendGroup;

  private Button wSuspendFailIfNotExists;

  private Group wAlterGroup;

  private Button wAlterFailIfNotExists;

  private ComboVar wAlterWarehouseSize;

  private ComboVar wAlterWarehouseType;

  private TextVar wAlterMaxClusterSize;

  private TextVar wAlterMinClusterSize;

  private TextVar wAlterAutoSuspend;

  private Button wAlterAutoResume;

  private ComboVar wAlterResourceMonitor;

  private TextVar wAlterComment;

  private boolean backupChanged;

  public WarehouseManagerDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    warehouseManager = (WarehouseManager) action;
  }

  public IAction open() {
    createShell(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Title"), warehouseManager);
    ModifyListener lsMod = e -> warehouseManager.setChanged();
    backupChanged = warehouseManager.hasChanged();

    int middle = this.middle;
    int margin = this.margin;

    DatabaseMeta databaseMeta =
        workflowMeta.findDatabase(warehouseManager.getConnection(), variables);
    wConnection = addConnectionLine(shell, wSpacer, databaseMeta, lsMod);
    //    if (warehouseManager.getDatabaseMeta() == null && workflowMeta.nrDatabases() == 1) {
    //      wConnection.select(0);
    //    }

    // Warehouse name line
    //
    Label wlWarehouseName = new Label(shell, SWT.RIGHT);
    wlWarehouseName.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.WarehouseName.Label"));
    PropsUi.setLook(wlWarehouseName);
    FormData fdlWarehouseName = new FormData();
    fdlWarehouseName.left = new FormAttachment(0, 0);
    fdlWarehouseName.top = new FormAttachment(wConnection, margin);
    fdlWarehouseName.right = new FormAttachment(middle, -margin);
    wlWarehouseName.setLayoutData(fdlWarehouseName);

    wWarehouseName = new ComboVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wWarehouseName);
    FormData fdWarehouseName = new FormData();
    fdWarehouseName.left = new FormAttachment(middle, 0);
    fdWarehouseName.top = new FormAttachment(wConnection, margin);
    fdWarehouseName.right = new FormAttachment(100, 0);
    wWarehouseName.setLayoutData(fdWarehouseName);
    wWarehouseName.addFocusListener(
        new FocusAdapter() {
          /**
           * Get the list of stages for the schema, and populate the stage name drop down.
           *
           * @param focusEvent The event
           */
          @Override
          public void focusGained(FocusEvent focusEvent) {
            DatabaseMeta databaseMeta = workflowMeta.findDatabase(wConnection.getText(), variables);
            if (databaseMeta != null) {
              String warehouseName = wWarehouseName.getText();
              wWarehouseName.removeAll();
              Database db = null;
              try {
                db = new Database(loggingObject, variables, databaseMeta);
                db.connect();
                try (ResultSet resultSet =
                    db.openQuery("show warehouses;", null, null, ResultSet.FETCH_FORWARD, false)) {
                  IRowMeta rowMeta = db.getReturnRowMeta();
                  Object[] row = db.getRow(resultSet);
                  int nameField = rowMeta.indexOfValue("NAME");
                  if (nameField >= 0) {
                    while (row != null) {
                      String name = rowMeta.getString(row, nameField);
                      wWarehouseName.add(name);
                      row = db.getRow(resultSet);
                    }
                  } else {
                    throw new HopException("Unable to find warehouse name field in result");
                  }
                  db.closeQuery(resultSet);
                }
                if (warehouseName != null) {
                  wWarehouseName.setText(warehouseName);
                }
              } catch (Exception ex) {
                warehouseManager.logDebug("Error getting warehouses", ex);
              } finally {
                if (db != null) {
                  db.disconnect();
                }
              }
            }
          }
        });

    // ///////////////////
    // Action line
    // ///////////////////
    Label wlAction = new Label(shell, SWT.RIGHT);
    wlAction.setText(BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Action.Label"));
    PropsUi.setLook(wlAction);
    FormData fdlAction = new FormData();
    fdlAction.left = new FormAttachment(0, 0);
    fdlAction.right = new FormAttachment(middle, -margin);
    fdlAction.top = new FormAttachment(wWarehouseName, margin);
    wlAction.setLayoutData(fdlAction);

    wAction = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wAction.setItems(MANAGEMENT_ACTION_DESCS);
    PropsUi.setLook(wAction);
    FormData fdAction = new FormData();
    fdAction.left = new FormAttachment(middle, 0);
    fdAction.top = new FormAttachment(wWarehouseName, margin);
    fdAction.right = new FormAttachment(100, 0);
    wAction.setLayoutData(fdAction);
    wAction.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            setFlags();
          }
        });

    /////////////////////
    // Start Create Warehouse Group
    /////////////////////
    wCreateGroup = new Group(shell, SWT.SHADOW_ETCHED_IN);
    wCreateGroup.setText(
        BaseMessages.getString(
            PKG, "SnowflakeWarehouseManager.Dialog.Group.CreateWarehouse.Label"));
    FormLayout createWarehouseLayout = new FormLayout();
    createWarehouseLayout.marginWidth = 3;
    createWarehouseLayout.marginHeight = 3;
    wCreateGroup.setLayout(createWarehouseLayout);
    PropsUi.setLook(wCreateGroup);

    FormData fdgCreateGroup = new FormData();
    fdgCreateGroup.left = new FormAttachment(0, 0);
    fdgCreateGroup.right = new FormAttachment(100, 0);
    fdgCreateGroup.top = new FormAttachment(wAction, margin);
    wCreateGroup.setLayoutData(fdgCreateGroup);

    // //////////////////////
    // Replace line
    // /////////////////////
    Label wlCreateReplace = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateReplace.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Create.Replace.Label"));
    wlCreateReplace.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Create.Replace.Tooltip"));
    PropsUi.setLook(wlCreateReplace);
    FormData fdlCreateReplace = new FormData();
    fdlCreateReplace.left = new FormAttachment(0, 0);
    fdlCreateReplace.top = new FormAttachment(0, margin);
    fdlCreateReplace.right = new FormAttachment(middle, -margin);
    wlCreateReplace.setLayoutData(fdlCreateReplace);

    wCreateReplace = new Button(wCreateGroup, SWT.CHECK);
    PropsUi.setLook(wCreateReplace);
    FormData fdCreateReplace = new FormData();
    fdCreateReplace.left = new FormAttachment(middle, 0);
    fdCreateReplace.top = new FormAttachment(0, margin);
    fdCreateReplace.right = new FormAttachment(100, 0);
    wCreateReplace.setLayoutData(fdCreateReplace);
    wCreateReplace.addListener(SWT.Selection, e -> warehouseManager.setChanged());
    wCreateReplace.addListener(SWT.Selection, e -> setFlags());

    // /////////////////////
    // Fail if exists line
    // /////////////////////
    Label wlCreateFailIfExists = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateFailIfExists.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Create.FailIfExists.Label"));
    PropsUi.setLook(wlCreateFailIfExists);
    FormData fdlCreateFailIfExists = new FormData();
    fdlCreateFailIfExists.left = new FormAttachment(0, 0);
    fdlCreateFailIfExists.top = new FormAttachment(wCreateReplace, margin);
    fdlCreateFailIfExists.right = new FormAttachment(middle, -margin);
    wlCreateFailIfExists.setLayoutData(fdlCreateFailIfExists);

    wCreateFailIfExists = new Button(wCreateGroup, SWT.CHECK);
    PropsUi.setLook(wCreateFailIfExists);
    FormData fdCreateFailIfExists = new FormData();
    fdCreateFailIfExists.left = new FormAttachment(middle, 0);
    fdCreateFailIfExists.top = new FormAttachment(wCreateReplace, margin);
    fdCreateFailIfExists.right = new FormAttachment(100, 0);
    wCreateFailIfExists.setLayoutData(fdCreateFailIfExists);
    wCreateFailIfExists.addListener(SWT.Selection, e -> warehouseManager.setChanged());

    // Warehouse Size
    //
    Label wlCreateWarehouseSize = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateWarehouseSize.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.CreateWarehouseSize.Label"));
    PropsUi.setLook(wlCreateWarehouseSize);
    FormData fdlCreateWarehouseSize = new FormData();
    fdlCreateWarehouseSize.left = new FormAttachment(0, 0);
    fdlCreateWarehouseSize.top = new FormAttachment(wCreateFailIfExists, margin);
    fdlCreateWarehouseSize.right = new FormAttachment(middle, -margin);
    wlCreateWarehouseSize.setLayoutData(fdlCreateWarehouseSize);

    wCreateWarehouseSize =
        new ComboVar(variables, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCreateWarehouseSize);
    wCreateWarehouseSize.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdCreateWarehouseSize = new FormData();
    fdCreateWarehouseSize.left = new FormAttachment(middle, 0);
    fdCreateWarehouseSize.top = new FormAttachment(wCreateFailIfExists, margin);
    fdCreateWarehouseSize.right = new FormAttachment(100, 0);
    wCreateWarehouseSize.setLayoutData(fdCreateWarehouseSize);
    wCreateWarehouseSize.setItems(WAREHOUSE_SIZE_DESCS);

    // Warehouse Type
    //
    Label wlCreateWarehouseType = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateWarehouseType.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.CreateWarehouseType.Label"));
    PropsUi.setLook(wlCreateWarehouseType);
    FormData fdlCreateWarehouseType = new FormData();
    fdlCreateWarehouseType.left = new FormAttachment(0, 0);
    fdlCreateWarehouseType.top = new FormAttachment(wCreateWarehouseSize, margin);
    fdlCreateWarehouseType.right = new FormAttachment(middle, -margin);
    wlCreateWarehouseType.setLayoutData(fdlCreateWarehouseType);

    wCreateWarehouseType =
        new ComboVar(variables, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCreateWarehouseType);
    wCreateWarehouseType.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdCreateWarehouseType = new FormData();
    fdCreateWarehouseType.left = new FormAttachment(middle, 0);
    fdCreateWarehouseType.top = new FormAttachment(wCreateWarehouseSize, margin);
    fdCreateWarehouseType.right = new FormAttachment(100, 0);
    wCreateWarehouseType.setLayoutData(fdCreateWarehouseType);
    wCreateWarehouseType.setItems(WAREHOUSE_TYPE_DESCS);

    // /////////////////////
    // Max Cluster Size
    // /////////////////////
    Label wlCreateMaxClusterSize = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateMaxClusterSize.setText(
        BaseMessages.getString(
            PKG, "SnowflakeWarehouseManager.Dialog.Create.MaxClusterSize.Label"));
    PropsUi.setLook(wlCreateMaxClusterSize);
    FormData fdlCreateMaxClusterSize = new FormData();
    fdlCreateMaxClusterSize.left = new FormAttachment(0, 0);
    fdlCreateMaxClusterSize.top = new FormAttachment(wCreateWarehouseType, margin);
    fdlCreateMaxClusterSize.right = new FormAttachment(middle, -margin);
    wlCreateMaxClusterSize.setLayoutData(fdlCreateMaxClusterSize);

    wCreateMaxClusterSize =
        new TextVar(variables, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCreateGroup);
    wCreateMaxClusterSize.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdCreateMaxClusterSize = new FormData();
    fdCreateMaxClusterSize.left = new FormAttachment(middle, 0);
    fdCreateMaxClusterSize.right = new FormAttachment(100, 0);
    fdCreateMaxClusterSize.top = new FormAttachment(wCreateWarehouseType, margin);
    wCreateMaxClusterSize.setLayoutData(fdCreateMaxClusterSize);

    // /////////////////////
    // Min Cluster Size
    // /////////////////////
    Label wlCreateMinClusterSize = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateMinClusterSize.setText(
        BaseMessages.getString(
            PKG, "SnowflakeWarehouseManager.Dialog.Create.MinClusterSize.Label"));
    PropsUi.setLook(wlCreateMinClusterSize);
    FormData fdlCreateMinClusterSize = new FormData();
    fdlCreateMinClusterSize.left = new FormAttachment(0, 0);
    fdlCreateMinClusterSize.top = new FormAttachment(wCreateMaxClusterSize, margin);
    fdlCreateMinClusterSize.right = new FormAttachment(middle, -margin);
    wlCreateMinClusterSize.setLayoutData(fdlCreateMinClusterSize);

    wCreateMinClusterSize =
        new TextVar(variables, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCreateGroup);
    wCreateMinClusterSize.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdCreateMinClusterSize = new FormData();
    fdCreateMinClusterSize.left = new FormAttachment(middle, 0);
    fdCreateMinClusterSize.right = new FormAttachment(100, 0);
    fdCreateMinClusterSize.top = new FormAttachment(wCreateMaxClusterSize, margin);
    wCreateMinClusterSize.setLayoutData(fdCreateMinClusterSize);

    // /////////////////////
    // Auto Suspend Size
    // /////////////////////
    Label wlCreateAutoSuspend = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateAutoSuspend.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Create.AutoSuspend.Label"));
    wlCreateAutoSuspend.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Create.AutoSuspend.Tooltip"));
    PropsUi.setLook(wlCreateAutoSuspend);
    FormData fdlCreateAutoSuspend = new FormData();
    fdlCreateAutoSuspend.left = new FormAttachment(0, 0);
    fdlCreateAutoSuspend.top = new FormAttachment(wCreateMinClusterSize, margin);
    fdlCreateAutoSuspend.right = new FormAttachment(middle, -margin);
    wlCreateAutoSuspend.setLayoutData(fdlCreateAutoSuspend);

    wCreateAutoSuspend = new TextVar(variables, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCreateGroup);
    wCreateAutoSuspend.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdCreateAutoSuspend = new FormData();
    fdCreateAutoSuspend.left = new FormAttachment(middle, 0);
    fdCreateAutoSuspend.right = new FormAttachment(100, 0);
    fdCreateAutoSuspend.top = new FormAttachment(wCreateMinClusterSize, margin);
    wCreateAutoSuspend.setLayoutData(fdCreateAutoSuspend);

    // /////////////////////
    // Auto-resume
    // /////////////////////
    Label wlCreateAutoResume = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateAutoResume.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Create.AutoResume.Label"));
    PropsUi.setLook(wlCreateAutoResume);
    FormData fdlCreateAutoResume = new FormData();
    fdlCreateAutoResume.left = new FormAttachment(0, 0);
    fdlCreateAutoResume.top = new FormAttachment(wCreateAutoSuspend, margin);
    fdlCreateAutoResume.right = new FormAttachment(middle, -margin);
    wlCreateAutoResume.setLayoutData(fdlCreateAutoResume);

    wCreateAutoResume = new Button(wCreateGroup, SWT.CHECK);
    PropsUi.setLook(wCreateAutoResume);
    FormData fdCreateAutoResume = new FormData();
    fdCreateAutoResume.left = new FormAttachment(middle, 0);
    fdCreateAutoResume.top = new FormAttachment(wCreateAutoSuspend, margin);
    fdCreateAutoResume.right = new FormAttachment(100, 0);
    wCreateAutoResume.setLayoutData(fdCreateAutoResume);
    wCreateAutoResume.addListener(SWT.Selection, e -> warehouseManager.setChanged());

    // /////////////////////
    // Auto-resume
    // /////////////////////
    Label wlCreateInitialSuspend = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateInitialSuspend.setText(
        BaseMessages.getString(
            PKG, "SnowflakeWarehouseManager.Dialog.Create.InitialSuspend.Label"));
    wlCreateInitialSuspend.setToolTipText(
        BaseMessages.getString(
            PKG, "SnowflakeWarehouseManager.Dialog.Create.InitialSuspend.Tooltip"));
    PropsUi.setLook(wlCreateInitialSuspend);
    FormData fdlCreateInitialSuspend = new FormData();
    fdlCreateInitialSuspend.left = new FormAttachment(0, 0);
    fdlCreateInitialSuspend.top = new FormAttachment(wCreateAutoResume, margin);
    fdlCreateInitialSuspend.right = new FormAttachment(middle, -margin);
    wlCreateInitialSuspend.setLayoutData(fdlCreateInitialSuspend);

    wCreateInitialSuspend = new Button(wCreateGroup, SWT.CHECK);
    PropsUi.setLook(wCreateInitialSuspend);
    FormData fdCreateInitialSuspend = new FormData();
    fdCreateInitialSuspend.left = new FormAttachment(middle, 0);
    fdCreateInitialSuspend.top = new FormAttachment(wCreateAutoResume, margin);
    fdCreateInitialSuspend.right = new FormAttachment(100, 0);
    wCreateInitialSuspend.setLayoutData(fdCreateInitialSuspend);
    wCreateInitialSuspend.addListener(SWT.Selection, e -> warehouseManager.setChanged());

    // Resource monitor line
    //
    Label wlCreateResourceMonitor = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateResourceMonitor.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.ResourceMonitor.Label"));
    PropsUi.setLook(wlCreateResourceMonitor);
    FormData fdlCreateResourceMonitor = new FormData();
    fdlCreateResourceMonitor.left = new FormAttachment(0, 0);
    fdlCreateResourceMonitor.top = new FormAttachment(wCreateInitialSuspend, margin);
    fdlCreateResourceMonitor.right = new FormAttachment(middle, -margin);
    wlCreateResourceMonitor.setLayoutData(fdlCreateResourceMonitor);

    wCreateResourceMonitor =
        new ComboVar(variables, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCreateResourceMonitor);
    wCreateResourceMonitor.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdCreateResourceMonitor = new FormData();
    fdCreateResourceMonitor.left = new FormAttachment(middle, 0);
    fdCreateResourceMonitor.top = new FormAttachment(wCreateInitialSuspend, margin);
    fdCreateResourceMonitor.right = new FormAttachment(100, 0);
    wCreateResourceMonitor.setLayoutData(fdCreateResourceMonitor);
    wCreateResourceMonitor.addFocusListener(
        new FocusAdapter() {
          /**
           * Get the list of stages for the schema, and populate the stage name drop down.
           *
           * @param focusEvent The event
           */
          @Override
          public void focusGained(FocusEvent focusEvent) {
            getResourceMonitors();
          }
        });

    // /////////////////////
    // Comment Line
    // /////////////////////
    Label wlCreateComment = new Label(wCreateGroup, SWT.RIGHT);
    wlCreateComment.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Create.Comment.Label"));
    PropsUi.setLook(wlCreateComment);
    FormData fdlCreateComment = new FormData();
    fdlCreateComment.left = new FormAttachment(0, 0);
    fdlCreateComment.top = new FormAttachment(wCreateResourceMonitor, margin);
    fdlCreateComment.right = new FormAttachment(middle, -margin);
    wlCreateComment.setLayoutData(fdlCreateComment);

    wCreateComment = new TextVar(variables, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCreateGroup);
    wCreateComment.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdCreateComment = new FormData();
    fdCreateComment.left = new FormAttachment(middle, 0);
    fdCreateComment.right = new FormAttachment(100, 0);
    fdCreateComment.top = new FormAttachment(wCreateResourceMonitor, margin);
    wCreateComment.setLayoutData(fdCreateComment);

    /////////////////////
    // Start Drop Warehouse Group
    /////////////////////
    wDropGroup = new Group(shell, SWT.SHADOW_ETCHED_IN);
    wDropGroup.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Group.DropWarehouse.Label"));
    FormLayout dropWarehouseLayout = new FormLayout();
    dropWarehouseLayout.marginWidth = 3;
    dropWarehouseLayout.marginHeight = 3;
    wDropGroup.setLayout(dropWarehouseLayout);
    PropsUi.setLook(wDropGroup);

    FormData fdgDropGroup = new FormData();
    fdgDropGroup.left = new FormAttachment(0, 0);
    fdgDropGroup.right = new FormAttachment(100, 0);
    fdgDropGroup.top = new FormAttachment(wAction, margin);
    wDropGroup.setLayoutData(fdgDropGroup);

    // //////////////////////
    // Fail if Not exists line
    // /////////////////////
    Label wlDropFailIfNotExists = new Label(wDropGroup, SWT.RIGHT);
    wlDropFailIfNotExists.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Drop.FailIfNotExists.Label"));
    PropsUi.setLook(wlDropFailIfNotExists);
    FormData fdlDropFailIfNotExists = new FormData();
    fdlDropFailIfNotExists.left = new FormAttachment(0, 0);
    fdlDropFailIfNotExists.top = new FormAttachment(0, margin);
    fdlDropFailIfNotExists.right = new FormAttachment(middle, -margin);
    wlDropFailIfNotExists.setLayoutData(fdlDropFailIfNotExists);

    wDropFailIfNotExists = new Button(wDropGroup, SWT.CHECK);
    PropsUi.setLook(wDropFailIfNotExists);
    FormData fdDropFailIfNotExists = new FormData();
    fdDropFailIfNotExists.left = new FormAttachment(middle, 0);
    fdDropFailIfNotExists.top = new FormAttachment(0, margin);
    fdDropFailIfNotExists.right = new FormAttachment(100, 0);
    wDropFailIfNotExists.setLayoutData(fdDropFailIfNotExists);
    wDropFailIfNotExists.addListener(SWT.Selection, e -> warehouseManager.setChanged());

    /////////////////////
    // Start Resume Warehouse Group
    /////////////////////
    wResumeGroup = new Group(shell, SWT.SHADOW_ETCHED_IN);
    wResumeGroup.setText(
        BaseMessages.getString(
            PKG, "SnowflakeWarehouseManager.Dialog.Group.ResumeWarehouse.Label"));
    FormLayout resumeWarehouseLayout = new FormLayout();
    resumeWarehouseLayout.marginWidth = 3;
    resumeWarehouseLayout.marginHeight = 3;
    wResumeGroup.setLayout(resumeWarehouseLayout);
    PropsUi.setLook(wResumeGroup);

    FormData fdgResumeGroup = new FormData();
    fdgResumeGroup.left = new FormAttachment(0, 0);
    fdgResumeGroup.right = new FormAttachment(100, 0);
    fdgResumeGroup.top = new FormAttachment(wAction, margin);
    wResumeGroup.setLayoutData(fdgResumeGroup);

    // //////////////////////
    // Fail if Not exists line
    // /////////////////////
    Label wlResumeFailIfNotExists = new Label(wResumeGroup, SWT.RIGHT);
    wlResumeFailIfNotExists.setText(
        BaseMessages.getString(
            PKG, "SnowflakeWarehouseManager.Dialog.Resume.FailIfNotExists.Label"));
    PropsUi.setLook(wlResumeFailIfNotExists);
    FormData fdlResumeFailIfNotExists = new FormData();
    fdlResumeFailIfNotExists.left = new FormAttachment(0, 0);
    fdlResumeFailIfNotExists.top = new FormAttachment(0, margin);
    fdlResumeFailIfNotExists.right = new FormAttachment(middle, -margin);
    wlResumeFailIfNotExists.setLayoutData(fdlResumeFailIfNotExists);

    wResumeFailIfNotExists = new Button(wResumeGroup, SWT.CHECK);
    PropsUi.setLook(wResumeFailIfNotExists);
    FormData fdResumeFailIfNotExists = new FormData();
    fdResumeFailIfNotExists.left = new FormAttachment(middle, 0);
    fdResumeFailIfNotExists.top = new FormAttachment(0, margin);
    fdResumeFailIfNotExists.right = new FormAttachment(100, 0);
    wResumeFailIfNotExists.setLayoutData(fdResumeFailIfNotExists);
    wResumeFailIfNotExists.addListener(SWT.Selection, e -> warehouseManager.setChanged());

    /////////////////////
    // Start Suspend Warehouse Group
    /////////////////////
    wSuspendGroup = new Group(shell, SWT.SHADOW_ETCHED_IN);
    wSuspendGroup.setText(
        BaseMessages.getString(
            PKG, "SnowflakeWarehouseManager.Dialog.Group.SuspendWarehouse.Label"));
    FormLayout suspendWarehouseLayout = new FormLayout();
    suspendWarehouseLayout.marginWidth = 3;
    suspendWarehouseLayout.marginHeight = 3;
    wSuspendGroup.setLayout(suspendWarehouseLayout);
    PropsUi.setLook(wSuspendGroup);

    FormData fdgSuspendGroup = new FormData();
    fdgSuspendGroup.left = new FormAttachment(0, 0);
    fdgSuspendGroup.right = new FormAttachment(100, 0);
    fdgSuspendGroup.top = new FormAttachment(wAction, margin);
    wSuspendGroup.setLayoutData(fdgSuspendGroup);

    // //////////////////////
    // Fail if Not exists line
    // /////////////////////
    Label wlSuspendFailIfNotExists = new Label(wSuspendGroup, SWT.RIGHT);
    wlSuspendFailIfNotExists.setText(
        BaseMessages.getString(
            PKG, "SnowflakeWarehouseManager.Dialog.Suspend.FailIfNotExists.Label"));
    PropsUi.setLook(wlSuspendFailIfNotExists);
    FormData fdlSuspendFailIfNotExists = new FormData();
    fdlSuspendFailIfNotExists.left = new FormAttachment(0, 0);
    fdlSuspendFailIfNotExists.top = new FormAttachment(0, margin);
    fdlSuspendFailIfNotExists.right = new FormAttachment(middle, -margin);
    wlSuspendFailIfNotExists.setLayoutData(fdlSuspendFailIfNotExists);

    wSuspendFailIfNotExists = new Button(wSuspendGroup, SWT.CHECK);
    PropsUi.setLook(wSuspendFailIfNotExists);
    FormData fdSuspendFailIfNotExists = new FormData();
    fdSuspendFailIfNotExists.left = new FormAttachment(middle, 0);
    fdSuspendFailIfNotExists.top = new FormAttachment(0, margin);
    fdSuspendFailIfNotExists.right = new FormAttachment(100, 0);
    wSuspendFailIfNotExists.setLayoutData(fdSuspendFailIfNotExists);
    wSuspendFailIfNotExists.addListener(SWT.Selection, e -> warehouseManager.setChanged());

    /////////////////////
    // Start Alter Warehouse Group
    /////////////////////
    wAlterGroup = new Group(shell, SWT.SHADOW_ETCHED_IN);
    wAlterGroup.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Group.AlterWarehouse.Label"));
    FormLayout alterWarehouseLayout = new FormLayout();
    alterWarehouseLayout.marginWidth = 3;
    alterWarehouseLayout.marginHeight = 3;
    wAlterGroup.setLayout(alterWarehouseLayout);
    PropsUi.setLook(wAlterGroup);

    FormData fdgAlterGroup = new FormData();
    fdgAlterGroup.left = new FormAttachment(0, 0);
    fdgAlterGroup.right = new FormAttachment(100, 0);
    fdgAlterGroup.top = new FormAttachment(wAction, margin);
    wAlterGroup.setLayoutData(fdgAlterGroup);

    // //////////////////////
    // Fail if Not exists line
    // /////////////////////
    Label wlAlterFailIfNotExists = new Label(wAlterGroup, SWT.RIGHT);
    wlAlterFailIfNotExists.setText(
        BaseMessages.getString(
            PKG, "SnowflakeWarehouseManager.Dialog.Alter.FailIfNotExists.Label"));
    PropsUi.setLook(wlAlterFailIfNotExists);
    FormData fdlAlterFailIfNotExists = new FormData();
    fdlAlterFailIfNotExists.left = new FormAttachment(0, 0);
    fdlAlterFailIfNotExists.top = new FormAttachment(0, margin);
    fdlAlterFailIfNotExists.right = new FormAttachment(middle, -margin);
    wlAlterFailIfNotExists.setLayoutData(fdlAlterFailIfNotExists);

    wAlterFailIfNotExists = new Button(wAlterGroup, SWT.CHECK);
    PropsUi.setLook(wAlterFailIfNotExists);
    FormData fdAlterFailIfNotExists = new FormData();
    fdAlterFailIfNotExists.left = new FormAttachment(middle, 0);
    fdAlterFailIfNotExists.top = new FormAttachment(0, margin);
    fdAlterFailIfNotExists.right = new FormAttachment(100, 0);
    wAlterFailIfNotExists.setLayoutData(fdAlterFailIfNotExists);
    wAlterFailIfNotExists.addListener(SWT.Selection, e -> warehouseManager.setChanged());

    // Warehouse Size
    //
    Label wlAlterWarehouseSize = new Label(wAlterGroup, SWT.RIGHT);
    wlAlterWarehouseSize.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.AlterWarehouseSize.Label"));
    PropsUi.setLook(wlAlterWarehouseSize);
    FormData fdlAlterWarehouseSize = new FormData();
    fdlAlterWarehouseSize.left = new FormAttachment(0, 0);
    fdlAlterWarehouseSize.top = new FormAttachment(wAlterFailIfNotExists, margin);
    fdlAlterWarehouseSize.right = new FormAttachment(middle, -margin);
    wlAlterWarehouseSize.setLayoutData(fdlAlterWarehouseSize);

    wAlterWarehouseSize = new ComboVar(variables, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAlterWarehouseSize);
    wAlterWarehouseSize.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdAlterWarehouseSize = new FormData();
    fdAlterWarehouseSize.left = new FormAttachment(middle, 0);
    fdAlterWarehouseSize.top = new FormAttachment(wAlterFailIfNotExists, margin);
    fdAlterWarehouseSize.right = new FormAttachment(100, 0);
    wAlterWarehouseSize.setLayoutData(fdAlterWarehouseSize);
    wAlterWarehouseSize.setItems(WAREHOUSE_SIZE_DESCS);

    // Warehouse Type
    //
    Label wlAlterWarehouseType = new Label(wAlterGroup, SWT.RIGHT);
    wlAlterWarehouseType.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.AlterWarehouseType.Label"));
    PropsUi.setLook(wlAlterWarehouseType);
    FormData fdlAlterWarehouseType = new FormData();
    fdlAlterWarehouseType.left = new FormAttachment(0, 0);
    fdlAlterWarehouseType.top = new FormAttachment(wAlterWarehouseSize, margin);
    fdlAlterWarehouseType.right = new FormAttachment(middle, -margin);
    wlAlterWarehouseType.setLayoutData(fdlAlterWarehouseType);

    wAlterWarehouseType = new ComboVar(variables, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAlterWarehouseType);
    wAlterWarehouseType.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdAlterWarehouseType = new FormData();
    fdAlterWarehouseType.left = new FormAttachment(middle, 0);
    fdAlterWarehouseType.top = new FormAttachment(wAlterWarehouseSize, margin);
    fdAlterWarehouseType.right = new FormAttachment(100, 0);
    wAlterWarehouseType.setLayoutData(fdAlterWarehouseType);
    wAlterWarehouseType.setItems(WAREHOUSE_TYPE_DESCS);

    // /////////////////////
    // Max Cluster Size
    // /////////////////////
    Label wlAlterMaxClusterSize = new Label(wAlterGroup, SWT.RIGHT);
    wlAlterMaxClusterSize.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Alter.MaxClusterSize.Label"));
    PropsUi.setLook(wlAlterMaxClusterSize);
    FormData fdlAlterMaxClusterSize = new FormData();
    fdlAlterMaxClusterSize.left = new FormAttachment(0, 0);
    fdlAlterMaxClusterSize.top = new FormAttachment(wAlterWarehouseType, margin);
    fdlAlterMaxClusterSize.right = new FormAttachment(middle, -margin);
    wlAlterMaxClusterSize.setLayoutData(fdlAlterMaxClusterSize);

    wAlterMaxClusterSize = new TextVar(variables, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAlterGroup);
    wAlterMaxClusterSize.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdAlterMaxClusterSize = new FormData();
    fdAlterMaxClusterSize.left = new FormAttachment(middle, 0);
    fdAlterMaxClusterSize.right = new FormAttachment(100, 0);
    fdAlterMaxClusterSize.top = new FormAttachment(wAlterWarehouseType, margin);
    wAlterMaxClusterSize.setLayoutData(fdAlterMaxClusterSize);

    // /////////////////////
    // Min Cluster Size
    // /////////////////////
    Label wlAlterMinClusterSize = new Label(wAlterGroup, SWT.RIGHT);
    wlAlterMinClusterSize.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Alter.MinClusterSize.Label"));
    PropsUi.setLook(wlAlterMinClusterSize);
    FormData fdlAlterMinClusterSize = new FormData();
    fdlAlterMinClusterSize.left = new FormAttachment(0, 0);
    fdlAlterMinClusterSize.top = new FormAttachment(wAlterMaxClusterSize, margin);
    fdlAlterMinClusterSize.right = new FormAttachment(middle, -margin);
    wlAlterMinClusterSize.setLayoutData(fdlAlterMinClusterSize);

    wAlterMinClusterSize = new TextVar(variables, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAlterGroup);
    wAlterMinClusterSize.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdAlterMinClusterSize = new FormData();
    fdAlterMinClusterSize.left = new FormAttachment(middle, 0);
    fdAlterMinClusterSize.right = new FormAttachment(100, 0);
    fdAlterMinClusterSize.top = new FormAttachment(wAlterMaxClusterSize, margin);
    wAlterMinClusterSize.setLayoutData(fdAlterMinClusterSize);

    // /////////////////////
    // Auto Suspend Size
    // /////////////////////
    Label wlAlterAutoSuspend = new Label(wAlterGroup, SWT.RIGHT);
    wlAlterAutoSuspend.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Alter.AutoSuspend.Label"));
    wlAlterAutoSuspend.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Alter.AutoSuspend.Tooltip"));
    PropsUi.setLook(wlAlterAutoSuspend);
    FormData fdlAlterAutoSuspend = new FormData();
    fdlAlterAutoSuspend.left = new FormAttachment(0, 0);
    fdlAlterAutoSuspend.top = new FormAttachment(wAlterMinClusterSize, margin);
    fdlAlterAutoSuspend.right = new FormAttachment(middle, -margin);
    wlAlterAutoSuspend.setLayoutData(fdlAlterAutoSuspend);

    wAlterAutoSuspend = new TextVar(variables, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAlterGroup);
    wAlterAutoSuspend.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdAlterAutoSuspend = new FormData();
    fdAlterAutoSuspend.left = new FormAttachment(middle, 0);
    fdAlterAutoSuspend.right = new FormAttachment(100, 0);
    fdAlterAutoSuspend.top = new FormAttachment(wAlterMinClusterSize, margin);
    wAlterAutoSuspend.setLayoutData(fdAlterAutoSuspend);

    // /////////////////////
    // Auto-resume
    // /////////////////////
    Label wlAlterAutoResume = new Label(wAlterGroup, SWT.RIGHT);
    wlAlterAutoResume.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Alter.AutoResume.Label"));
    PropsUi.setLook(wlAlterAutoResume);
    FormData fdlAlterAutoResume = new FormData();
    fdlAlterAutoResume.left = new FormAttachment(0, 0);
    fdlAlterAutoResume.top = new FormAttachment(wAlterAutoSuspend, margin);
    fdlAlterAutoResume.right = new FormAttachment(middle, -margin);
    wlAlterAutoResume.setLayoutData(fdlAlterAutoResume);

    wAlterAutoResume = new Button(wAlterGroup, SWT.CHECK);
    PropsUi.setLook(wAlterAutoResume);
    FormData fdAlterAutoResume = new FormData();
    fdAlterAutoResume.left = new FormAttachment(middle, 0);
    fdAlterAutoResume.top = new FormAttachment(wAlterAutoSuspend, margin);
    fdAlterAutoResume.right = new FormAttachment(100, 0);
    wAlterAutoResume.setLayoutData(fdAlterAutoResume);
    wAlterAutoResume.addListener(SWT.Selection, e -> warehouseManager.setChanged());

    // Resource monitor line
    //
    Label wlAlterResourceMonitor = new Label(wAlterGroup, SWT.RIGHT);
    wlAlterResourceMonitor.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.ResourceMonitor.Label"));
    PropsUi.setLook(wlAlterResourceMonitor);
    FormData fdlAlterResourceMonitor = new FormData();
    fdlAlterResourceMonitor.left = new FormAttachment(0, 0);
    fdlAlterResourceMonitor.top = new FormAttachment(wAlterAutoResume, margin);
    fdlAlterResourceMonitor.right = new FormAttachment(middle, -margin);
    wlAlterResourceMonitor.setLayoutData(fdlAlterResourceMonitor);

    wAlterResourceMonitor =
        new ComboVar(variables, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAlterResourceMonitor);
    wAlterResourceMonitor.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdAlterResourceMonitor = new FormData();
    fdAlterResourceMonitor.left = new FormAttachment(middle, 0);
    fdAlterResourceMonitor.top = new FormAttachment(wAlterAutoResume, margin);
    fdAlterResourceMonitor.right = new FormAttachment(100, 0);
    wAlterResourceMonitor.setLayoutData(fdAlterResourceMonitor);
    wAlterResourceMonitor.addFocusListener(
        new FocusAdapter() {
          /**
           * Get the list of stages for the schema, and populate the stage name drop down.
           *
           * @param focusEvent The event
           */
          @Override
          public void focusGained(FocusEvent focusEvent) {
            getResourceMonitors();
          }
        });

    // /////////////////////
    // Comment Line
    // /////////////////////
    Label wlAlterComment = new Label(wAlterGroup, SWT.RIGHT);
    wlAlterComment.setText(
        BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Dialog.Alter.Comment.Label"));
    PropsUi.setLook(wlAlterComment);
    FormData fdlAlterComment = new FormData();
    fdlAlterComment.left = new FormAttachment(0, 0);
    fdlAlterComment.top = new FormAttachment(wAlterResourceMonitor, margin);
    fdlAlterComment.right = new FormAttachment(middle, -margin);
    wlAlterComment.setLayoutData(fdlAlterComment);

    wAlterComment = new TextVar(variables, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAlterGroup);
    wAlterComment.addListener(SWT.Modify, e -> warehouseManager.setChanged());
    FormData fdAlterComment = new FormData();
    fdAlterComment.left = new FormAttachment(middle, 0);
    fdAlterComment.right = new FormAttachment(100, 0);
    fdAlterComment.top = new FormAttachment(wAlterResourceMonitor, margin);
    wAlterComment.setLayoutData(fdAlterComment);

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build(wCreateGroup);

    getData();
    setFlags();
    BaseTransformDialog.setSize(shell);
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return warehouseManager;
  }

  public void setFlags() {
    wCreateFailIfExists.setEnabled(!wCreateReplace.getSelection());
    wCreateGroup.setVisible(
        wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_CREATE);
    wDropGroup.setVisible(wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_DROP);
    wResumeGroup.setVisible(
        wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_RESUME);
    wSuspendGroup.setVisible(
        wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_SUSPEND);
    wAlterGroup.setVisible(wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_ALTER);
  }

  @Override
  public void dispose() {
    WindowProperty winprop = new WindowProperty(shell);
    props.setScreen(winprop);
    shell.dispose();
  }

  @Override
  protected void onActionNameModified() {
    warehouseManager.setChanged();
  }

  public void getData() {
    wName.setText(Const.NVL(warehouseManager.getName(), ""));
    wConnection.setText(Const.NVL(warehouseManager.getConnection(), ""));
    wWarehouseName.setText(Const.NVL(warehouseManager.getWarehouseName(), ""));
    int actionId = warehouseManager.getManagementActionId();
    if (actionId >= 0 && actionId < MANAGEMENT_ACTION_DESCS.length) {
      wAction.setText(MANAGEMENT_ACTION_DESCS[actionId]);
    }
    wCreateReplace.setSelection(warehouseManager.isReplace());
    wCreateFailIfExists.setSelection(warehouseManager.isFailIfExists());
    int warehouseSizeId = warehouseManager.getWarehouseSizeId();
    if (warehouseSizeId >= 0 && warehouseSizeId < WAREHOUSE_SIZE_DESCS.length) {
      wCreateWarehouseSize.setText(WAREHOUSE_SIZE_DESCS[warehouseSizeId]);
    } else {
      wCreateWarehouseSize.setText(Const.NVL(warehouseManager.getWarehouseSize(), ""));
    }
    int warehouseTypeId = warehouseManager.getWarehouseTypeId();
    if (warehouseTypeId >= 0 && warehouseTypeId < WAREHOUSE_TYPE_DESCS.length) {
      wCreateWarehouseType.setText(WAREHOUSE_TYPE_DESCS[warehouseTypeId]);
    } else {
      wCreateWarehouseType.setText(Const.NVL(warehouseManager.getWarehouseType(), ""));
    }
    wCreateMaxClusterSize.setText(Const.NVL(warehouseManager.getMaxClusterCount(), ""));
    wCreateMinClusterSize.setText(Const.NVL(warehouseManager.getMinClusterCount(), ""));
    wCreateAutoSuspend.setText(Const.NVL(warehouseManager.getAutoSuspend(), ""));
    wCreateAutoResume.setSelection(warehouseManager.isAutoResume());
    wCreateInitialSuspend.setSelection(warehouseManager.isInitiallySuspended());
    wCreateResourceMonitor.setText(Const.NVL(warehouseManager.getResourceMonitor(), ""));
    wCreateComment.setText(Const.NVL(warehouseManager.getComment(), ""));

    wDropFailIfNotExists.setSelection(warehouseManager.isFailIfNotExists());
    wResumeFailIfNotExists.setSelection(warehouseManager.isFailIfNotExists());
    wSuspendFailIfNotExists.setSelection(warehouseManager.isFailIfNotExists());

    wAlterFailIfNotExists.setSelection(warehouseManager.isFailIfNotExists());
    if (warehouseSizeId >= 0 && warehouseSizeId < WAREHOUSE_SIZE_DESCS.length) {
      wAlterWarehouseSize.setText(WAREHOUSE_SIZE_DESCS[warehouseSizeId]);
    } else {
      wAlterWarehouseSize.setText(Const.NVL(warehouseManager.getWarehouseSize(), ""));
    }
    if (warehouseTypeId >= 0 && warehouseTypeId < WAREHOUSE_TYPE_DESCS.length) {
      wAlterWarehouseType.setText(WAREHOUSE_TYPE_DESCS[warehouseTypeId]);
    } else {
      wAlterWarehouseType.setText(Const.NVL(warehouseManager.getWarehouseType(), ""));
    }
    wAlterMaxClusterSize.setText(Const.NVL(warehouseManager.getMaxClusterCount(), ""));
    wAlterMinClusterSize.setText(Const.NVL(warehouseManager.getMinClusterCount(), ""));
    wAlterAutoSuspend.setText(Const.NVL(warehouseManager.getAutoSuspend(), ""));
    wAlterAutoResume.setSelection(warehouseManager.isAutoResume());
    wAlterResourceMonitor.setText(Const.NVL(warehouseManager.getResourceMonitor(), ""));
    wAlterComment.setText(Const.NVL(warehouseManager.getComment(), ""));

    if (StringUtil.isEmpty(wAction.getText())) {
      wAction.setText(MANAGEMENT_ACTION_DESCS[0]);
    }
  }

  private void cancel() {
    warehouseManager.setChanged(backupChanged);

    warehouseManager = null;
    dispose();
  }

  private void ok() {
    if (StringUtil.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.ActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    warehouseManager.setName(wName.getText());
    warehouseManager.setConnection(wConnection.getText());
    warehouseManager.setWarehouseName(wWarehouseName.getText());
    warehouseManager.setManagementActionById(wAction.getSelectionIndex());
    if (wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_CREATE) {
      warehouseManager.setReplace(wCreateReplace.getSelection());
      warehouseManager.setFailIfExists(wCreateFailIfExists.getSelection());
      boolean warehouseSizeFound = false;
      for (int i = 0; i < WAREHOUSE_SIZE_DESCS.length; i++) {
        if (wCreateWarehouseSize.getText().equals(WAREHOUSE_SIZE_DESCS[i])) {
          warehouseSizeFound = true;
          warehouseManager.setWarehouseSizeById(i);
          break;
        }
      }
      if (!warehouseSizeFound) {
        warehouseManager.setWarehouseSize(wCreateWarehouseSize.getText());
      }

      boolean warehouseTypeFound = false;
      for (int i = 0; i < WAREHOUSE_TYPE_DESCS.length; i++) {
        if (wCreateWarehouseType.getText().equals(WAREHOUSE_TYPE_DESCS[i])) {
          warehouseTypeFound = true;
          warehouseManager.setWarehouseTypeById(i);
          break;
        }
      }
      if (!warehouseTypeFound) {
        warehouseManager.setWarehouseType(wCreateWarehouseType.getText());
      }

      warehouseManager.setMaxClusterCount(wCreateMaxClusterSize.getText());
      warehouseManager.setMinClusterCount(wCreateMinClusterSize.getText());
      warehouseManager.setAutoResume(wCreateAutoResume.getSelection());
      warehouseManager.setAutoSuspend(wCreateAutoSuspend.getText());
      warehouseManager.setInitiallySuspended(wCreateInitialSuspend.getSelection());
      warehouseManager.setResourceMonitor(wCreateResourceMonitor.getText());
      warehouseManager.setComment(wCreateComment.getText());
    } else if (wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_DROP) {
      warehouseManager.setFailIfNotExists(wDropFailIfNotExists.getSelection());
    } else if (wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_RESUME) {
      warehouseManager.setFailIfNotExists(wResumeFailIfNotExists.getSelection());
    } else if (wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_SUSPEND) {
      warehouseManager.setFailIfNotExists(wSuspendFailIfNotExists.getSelection());
    } else if (wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_ALTER) {
      warehouseManager.setFailIfNotExists(wAlterFailIfNotExists.getSelection());
      boolean warehouseSizeFound = false;
      for (int i = 0; i < WAREHOUSE_SIZE_DESCS.length; i++) {
        if (wAlterWarehouseSize.getText().equals(WAREHOUSE_SIZE_DESCS[i])) {
          warehouseSizeFound = true;
          warehouseManager.setWarehouseSizeById(i);
          break;
        }
      }
      if (!warehouseSizeFound) {
        warehouseManager.setWarehouseSize(wAlterWarehouseSize.getText());
      }

      boolean warehouseTypeFound = false;
      for (int i = 0; i < WAREHOUSE_TYPE_DESCS.length; i++) {
        if (wAlterWarehouseType.getText().equals(WAREHOUSE_TYPE_DESCS[i])) {
          warehouseTypeFound = true;
          warehouseManager.setWarehouseTypeById(i);
          break;
        }
      }
      if (!warehouseTypeFound) {
        warehouseManager.setWarehouseType(wAlterWarehouseType.getText());
      }

      warehouseManager.setMaxClusterCount(wAlterMaxClusterSize.getText());
      warehouseManager.setMinClusterCount(wAlterMinClusterSize.getText());
      warehouseManager.setAutoResume(wAlterAutoResume.getSelection());
      warehouseManager.setAutoSuspend(wAlterAutoSuspend.getText());
      warehouseManager.setResourceMonitor(wAlterResourceMonitor.getText());
      warehouseManager.setComment(wAlterComment.getText());
    }

    dispose();
  }

  public void getResourceMonitors() {
    DatabaseMeta databaseMeta = workflowMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      String warehouseName = wWarehouseName.getText();
      wWarehouseName.removeAll();
      Database db = null;
      try {
        db = new Database(loggingObject, variables, databaseMeta);
        db.connect();
        ResultSet resultSet =
            db.openQuery("show resource monitors;", null, null, ResultSet.FETCH_FORWARD, false);
        IRowMeta rowMeta = db.getReturnRowMeta();
        Object[] row = db.getRow(resultSet);
        int nameField = rowMeta.indexOfValue("NAME");
        if (nameField >= 0) {
          while (row != null) {
            String name = rowMeta.getString(row, nameField);
            wWarehouseName.add(name);
            row = db.getRow(resultSet);
          }
        } else {
          throw new HopException("Unable to find resource monitor name field in result");
        }
        db.closeQuery(resultSet);
        if (warehouseName != null) {
          wWarehouseName.setText(warehouseName);
        }
      } catch (Exception ex) {
        warehouseManager.logDebug("Error getting resource monitors", ex);
      } finally {
        if (db != null) {
          db.disconnect();
        }
      }
    }
  }
}
