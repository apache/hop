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

package org.apache.hop.pipeline.transforms.dorisbulkloader;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.*;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

import static org.apache.hop.pipeline.transforms.dorisbulkloader.LoadConstants.CSV;
import static org.apache.hop.pipeline.transforms.dorisbulkloader.LoadConstants.JSON;

public class DorisBulkLoaderDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = DorisBulkLoaderMeta.class; // For Translator

  private Label wlFeHost;
  private TextVar wFeHost;

  private Label wlFeHttpPort;
  private TextVar wFeHttpPort;

  private Label wlDatabaseName;
  private TextVar wDatabaseName;

  private Label wlTableName;
  private TextVar wTableName;

  private Label wlHttpLogin;
  private TextVar wHttpLogin;

  private Label wlHttpPassword;
  private TextVar wHttpPassword;

  private Label wlDataField;
  private ComboVar wDataField;

  private Label wlFormat;
  private ComboVar wFormat;

  private Label wlLineDelimiter;
  private TextVar wLineDelimiter;

  private Label wlColumnDelimiter;
  private TextVar wColumnDelimiter;

  private Label wlBufferSize;
  private TextVar wBufferSize;

  private Label wlBufferCount;
  private TextVar wBufferCount;

  private TableView wHeaders;
  private ColumnInfo[] colinf;

  private final DorisBulkLoaderMeta input;

  private String[] inputFieldNames;

  public DorisBulkLoaderDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (DorisBulkLoaderMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // THE BUTTONS: at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.TransformName.Label"));
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

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, PropsUi.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.GeneralTab.Title"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout(fileLayout);

    // ////////////////////////
    // START Settings GROUP
    Group gConnections = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gConnections.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.ConnectionsGroup.Label"));
    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 3;
    settingsLayout.marginHeight = 3;
    gConnections.setLayout(settingsLayout);
    props.setLook(gConnections);

    wlFeHost = new Label(gConnections, SWT.RIGHT);
    wlFeHost.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.FeHost.Label"));
    props.setLook(wlFeHost);
    FormData fdlFeHost = new FormData();
    fdlFeHost.left = new FormAttachment(0, 0);
    fdlFeHost.right = new FormAttachment(middle, -margin);
    fdlFeHost.top = new FormAttachment(wGeneralComp, margin * 2);
    wlFeHost.setLayoutData(fdlFeHost);
    wFeHost = new TextVar(variables, gConnections, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFeHost.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.FeHost.Tooltip"));
    props.setLook(wFeHost);
    wFeHost.addModifyListener(lsMod);
    FormData fdFeHost = new FormData();
    fdFeHost.left = new FormAttachment(middle, 0);
    fdFeHost.top = new FormAttachment(wGeneralComp, margin * 2);
    fdFeHost.right = new FormAttachment(100, 0);
    wFeHost.setLayoutData(fdFeHost);

    wlFeHttpPort = new Label(gConnections, SWT.RIGHT);
    wlFeHttpPort.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.FeRestPort.Label"));
    props.setLook(wlFeHttpPort);
    FormData fdlFeHttpPort = new FormData();
    fdlFeHttpPort.left = new FormAttachment(0, 0);
    fdlFeHttpPort.right = new FormAttachment(middle, -margin);
    fdlFeHttpPort.top = new FormAttachment(wFeHost, margin);
    wlFeHttpPort.setLayoutData(fdlFeHttpPort);
    wFeHttpPort = new TextVar(variables, gConnections, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFeHttpPort.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.FeRestPort.Tooltip"));
    props.setLook(wFeHttpPort);
    wFeHttpPort.addModifyListener(lsMod);
    FormData fdFeHttpPort = new FormData();
    fdFeHttpPort.left = new FormAttachment(middle, 0);
    fdFeHttpPort.top = new FormAttachment(wFeHost, margin);
    fdFeHttpPort.right = new FormAttachment(100, 0);
    wFeHttpPort.setLayoutData(fdFeHttpPort);

    wlDatabaseName = new Label(gConnections, SWT.RIGHT);
    wlDatabaseName.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.DatabaseName.Label"));
    props.setLook(wlDatabaseName);
    FormData fdLblDatabaseName = new FormData();
    fdLblDatabaseName.left = new FormAttachment(0, 0);
    fdLblDatabaseName.right = new FormAttachment(middle, -margin);
    fdLblDatabaseName.top = new FormAttachment(wFeHttpPort, margin);
    wlDatabaseName.setLayoutData(fdLblDatabaseName);
    wDatabaseName = new TextVar(variables, gConnections, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDatabaseName.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.DatabaseName.Tooltip"));
    props.setLook(wDatabaseName);
    wDatabaseName.addModifyListener(lsMod);
    FormData fdDatabaseName = new FormData();
    fdDatabaseName.left = new FormAttachment(middle, 0);
    fdDatabaseName.top = new FormAttachment(wFeHttpPort, margin);
    fdDatabaseName.right = new FormAttachment(100, 0);
    wDatabaseName.setLayoutData(fdDatabaseName);

    wlTableName = new Label(gConnections, SWT.RIGHT);
    wlTableName.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.TableName.Label"));
    props.setLook(wlTableName);
    FormData fdlTableName = new FormData();
    fdlTableName.left = new FormAttachment(0, 0);
    fdlTableName.right = new FormAttachment(middle, -margin);
    fdlTableName.top = new FormAttachment(wDatabaseName, margin);
    wlTableName.setLayoutData(fdlTableName);
    wTableName = new TextVar(variables, gConnections, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTableName.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.TableName.Tooltip"));
    props.setLook(wTableName);
    wTableName.addModifyListener(lsMod);
    FormData fdTableName = new FormData();
    fdTableName.left = new FormAttachment(middle, 0);
    fdTableName.top = new FormAttachment(wDatabaseName, margin);
    fdTableName.right = new FormAttachment(100, 0);
    wTableName.setLayoutData(fdTableName);

    // HTTP Login
    wlHttpLogin = new Label(gConnections, SWT.RIGHT);
    wlHttpLogin.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.HttpLogin.Label"));
    props.setLook(wlHttpLogin);
    FormData fdlHttpLogin = new FormData();
    fdlHttpLogin.top = new FormAttachment(wTableName, margin);
    fdlHttpLogin.left = new FormAttachment(0, 0);
    fdlHttpLogin.right = new FormAttachment(middle, -margin);
    wlHttpLogin.setLayoutData(fdlHttpLogin);
    wHttpLogin = new TextVar(variables, gConnections, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHttpLogin.addModifyListener(lsMod);
    wHttpLogin.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.HttpLogin.Tooltip"));
    props.setLook(wHttpLogin);
    FormData fdHttpLogin = new FormData();
    fdHttpLogin.top = new FormAttachment(wTableName, margin);
    fdHttpLogin.left = new FormAttachment(middle, 0);
    fdHttpLogin.right = new FormAttachment(100, 0);
    wHttpLogin.setLayoutData(fdHttpLogin);

    // HTTP Password
    wlHttpPassword = new Label(gConnections, SWT.RIGHT);
    wlHttpPassword.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.HttpPassword.Label"));
    props.setLook(wlHttpPassword);
    FormData fdLblHttpPassword = new FormData();
    fdLblHttpPassword.top = new FormAttachment(wHttpLogin, margin);
    fdLblHttpPassword.left = new FormAttachment(0, 0);
    fdLblHttpPassword.right = new FormAttachment(middle, -margin);
    wlHttpPassword.setLayoutData(fdLblHttpPassword);
    wHttpPassword = new PasswordTextVar(variables, gConnections, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHttpPassword.addModifyListener(lsMod);
    wHttpPassword.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.HttpPassword.Tooltip"));
    props.setLook(wHttpPassword);
    FormData fdHttpPassword = new FormData();
    fdHttpPassword.top = new FormAttachment(wHttpLogin, margin);
    fdHttpPassword.left = new FormAttachment(middle, 0);
    fdHttpPassword.right = new FormAttachment(100, 0);
    wHttpPassword.setLayoutData(fdHttpPassword);

    FormData fdConnections = new FormData();
    fdConnections.left = new FormAttachment(0, 0);
    fdConnections.right = new FormAttachment(100, 0);
    fdConnections.top = new FormAttachment(wTransformName, margin);
    gConnections.setLayoutData(fdConnections);
    // END Connections GROUP
    // ////////////////////////


    // ////////////////////////
    // START Bulk Data GROUP
    Group gBulkData = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gBulkData.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.BulkDataGroup.Label"));
    FormLayout bulkDataLayout = new FormLayout();
    bulkDataLayout.marginWidth = 3;
    bulkDataLayout.marginHeight = 3;
    gBulkData.setLayout(bulkDataLayout);
    props.setLook(gBulkData);

    // Data Field
    wlDataField = new Label(gBulkData, SWT.RIGHT);
    wlDataField.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.BulkData.Label"));
    props.setLook(wlDataField);
    FormData fdlBody = new FormData();
    fdlBody.top = new FormAttachment(wHttpPassword, margin);
    fdlBody.left = new FormAttachment(0, 0);
    fdlBody.right = new FormAttachment(middle, -margin);
    wlDataField.setLayoutData(fdlBody);
    wDataField = new ComboVar(variables, gBulkData, SWT.BORDER | SWT.READ_ONLY);
    wDataField.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.BulkData.Tooltip"));
    wDataField.setEditable(true);
    props.setLook(wDataField);
    wDataField.addModifyListener(lsMod);
    FormData fdDataField = new FormData();
    fdDataField.top = new FormAttachment(wHttpPassword, margin);
    fdDataField.left = new FormAttachment(middle, 0);
    fdDataField.right = new FormAttachment(100, 0);
    wDataField.setLayoutData(fdDataField);
    wDataField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            String bodyField = wDataField.getText();
            wDataField.setItems(inputFieldNames);
            wDataField.setText(bodyField);
            shell.setCursor(null);
            busy.dispose();
          }
        });


    // format
    wlFormat = new Label(gBulkData, SWT.RIGHT);
    wlFormat.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.Format.Label"));
    props.setLook(wlFormat);
    FormData fdlFormat = new FormData();
    fdlFormat.top = new FormAttachment(wDataField, margin);
    fdlFormat.left = new FormAttachment(0, 0);
    fdlFormat.right = new FormAttachment(middle, -margin);
    wlFormat.setLayoutData(fdlFormat);
    wFormat = new ComboVar(variables, gBulkData, SWT.BORDER | SWT.READ_ONLY);
    wFormat.addModifyListener(lsMod);
    wFormat.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.Format.Tooltip"));
    props.setLook(wFormat);
    FormData fdFormat = new FormData();
    fdFormat.top = new FormAttachment(wDataField, margin);
    fdFormat.left = new FormAttachment(middle, 0);
    fdFormat.right = new FormAttachment(100, 0);
    wFormat.setLayoutData(fdFormat);
    wFormat.addFocusListener(
            new FocusListener() {
              @Override
              public void focusLost(FocusEvent e) {}

              @Override
              public void focusGained(FocusEvent e) {
                Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
                shell.setCursor(busy);
                String format = wFormat.getText();
                wFormat.setItems(new String[] {JSON,CSV});
                wFormat.setText(format);
                shell.setCursor(null);
                busy.dispose();
              }
            });
    wFormat.addSelectionListener(new SelectionListener() {
      @Override
      public void widgetSelected(SelectionEvent selectionEvent) {
        if (JSON.equals(wFormat.getText())) {
          wLineDelimiter.setText(LoadConstants.LINE_DELIMITER_JSON);
          wLineDelimiter.setEnabled(false);
          wColumnDelimiter.setText(LoadConstants.FIELD_DELIMITER_DEFAULT);
          wColumnDelimiter.setEnabled(false);
        } else {
          wLineDelimiter.setEnabled(true);
          wLineDelimiter.setText(LoadConstants.LINE_DELIMITER_DEFAULT);
          wColumnDelimiter.setEnabled(true);
          wColumnDelimiter.setText(LoadConstants.FIELD_DELIMITER_DEFAULT);
        }
      }

      @Override
      public void widgetDefaultSelected(SelectionEvent selectionEvent) {

      }
    });


    // lineDelimiter
    wlLineDelimiter = new Label(gBulkData, SWT.RIGHT);
    wlLineDelimiter.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.LineDelimiter.Label"));
    props.setLook(wlLineDelimiter);
    FormData fdlLineDelimiter = new FormData();
    fdlLineDelimiter.top = new FormAttachment(wFormat, margin);
    fdlLineDelimiter.left = new FormAttachment(0, 0);
    fdlLineDelimiter.right = new FormAttachment(middle, -margin);
    wlLineDelimiter.setLayoutData(fdlLineDelimiter);
    wLineDelimiter = new TextVar(variables, gBulkData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wLineDelimiter.addModifyListener(lsMod);
    wLineDelimiter.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.LineDelimiter.Tooltip"));
    props.setLook(wLineDelimiter);
    FormData fdLineDelimiter = new FormData();
    fdLineDelimiter.top = new FormAttachment(wFormat, margin);
    fdLineDelimiter.left = new FormAttachment(middle, 0);
    fdLineDelimiter.right = new FormAttachment(100, 0);
    wLineDelimiter.setLayoutData(fdLineDelimiter);

    // columnDelimiter
    wlColumnDelimiter = new Label(gBulkData, SWT.RIGHT);
    wlColumnDelimiter.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.ColumnDelimiter.Label"));
    props.setLook(wlColumnDelimiter);
    FormData fdlColumnDelimiter = new FormData();
    fdlColumnDelimiter.top = new FormAttachment(wLineDelimiter, margin);
    fdlColumnDelimiter.left = new FormAttachment(0, 0);
    fdlColumnDelimiter.right = new FormAttachment(middle, -margin);
    wlColumnDelimiter.setLayoutData(fdlColumnDelimiter);
    wColumnDelimiter = new TextVar(variables, gBulkData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wColumnDelimiter.addModifyListener(lsMod);
    wColumnDelimiter.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.ColumnDelimiter.Tooltip"));
    props.setLook(wColumnDelimiter);
    FormData fdColumnDelimiter = new FormData();
    fdColumnDelimiter.top = new FormAttachment(wLineDelimiter, margin);
    fdColumnDelimiter.left = new FormAttachment(middle, 0);
    fdColumnDelimiter.right = new FormAttachment(100, 0);
    wColumnDelimiter.setLayoutData(fdColumnDelimiter);

    // bufferSize
    wlBufferSize = new Label(gBulkData, SWT.RIGHT);
    wlBufferSize.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.BufferSize.Label"));
    props.setLook(wlBufferSize);
    FormData fdlBufferSize = new FormData();
    fdlBufferSize.top = new FormAttachment(wColumnDelimiter, margin);
    fdlBufferSize.left = new FormAttachment(0, 0);
    fdlBufferSize.right = new FormAttachment(middle, -margin);
    wlBufferSize.setLayoutData(fdlBufferSize);
    wBufferSize = new TextVar(variables, gBulkData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBufferSize.addModifyListener(lsMod);
    wBufferSize.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.BufferSize.Tooltip"));
    props.setLook(wBufferSize);
    FormData fdBufferSize = new FormData();
    fdBufferSize.top = new FormAttachment(wColumnDelimiter, margin);
    fdBufferSize.left = new FormAttachment(middle, 0);
    fdBufferSize.right = new FormAttachment(100, 0);
    wBufferSize.setLayoutData(fdBufferSize);


    // bufferCount
    wlBufferCount = new Label(gBulkData, SWT.RIGHT);
    wlBufferCount.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.BufferCount.Label"));
    props.setLook(wlBufferCount);
    FormData fdlBufferCount = new FormData();
    fdlBufferCount.top = new FormAttachment(wBufferSize, margin);
    fdlBufferCount.left = new FormAttachment(0, 0);
    fdlBufferCount.right = new FormAttachment(middle, -margin);
    wlBufferCount.setLayoutData(fdlBufferCount);
    wBufferCount = new TextVar(variables, gBulkData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBufferCount.addModifyListener(lsMod);
    wBufferCount.setToolTipText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.BufferCount.Tooltip"));
    props.setLook(wBufferCount);
    FormData fdBufferCount = new FormData();
    fdBufferCount.top = new FormAttachment(wBufferSize, margin);
    fdBufferCount.left = new FormAttachment(middle, 0);
    fdBufferCount.right = new FormAttachment(100, 0);
    wBufferCount.setLayoutData(fdBufferCount);

    FormData fdBulkData = new FormData();
    fdBulkData.left = new FormAttachment(0, 0);
    fdBulkData.right = new FormAttachment(100, 0);
    fdBulkData.top = new FormAttachment(gConnections, margin);
    gBulkData.setLayoutData(fdBulkData);
    // END Bulk Data GROUP
    // ////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(wTransformName, margin);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);
    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////


    // ///////////////////////////////////////////////////////////
    // / START OF HEADER TAB
    // ///////////////////////////////////////////////////////////
    CTabItem wAdditionalTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalTab.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.Headers.Title"));

    Composite wAdditionalComp = new Composite(wTabFolder, SWT.NONE);
    FormLayout addLayout = new FormLayout();
    addLayout.marginWidth = Const.FORM_MARGIN;
    addLayout.marginHeight = Const.FORM_MARGIN;
    wAdditionalComp.setLayout(addLayout);
    props.setLook(wAdditionalComp);

    Label wlFields = new Label(wAdditionalComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.Headers.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wTransformName, margin);
    wlFields.setLayoutData(fdlFields);

    final int FieldsRows = input.getHeaderNames() != null ? input.getHeaderNames().length : 0;

    colinf =
            new ColumnInfo[] {
                    new ColumnInfo(
                            BaseMessages.getString(PKG, "DorisBulkLoaderDialog.ColumnInfo.Header"),
                            ColumnInfo.COLUMN_TYPE_CCOMBO,
                            new String[] {""},
                            false),
                    new ColumnInfo(
                            BaseMessages.getString(PKG, "DorisBulkLoaderDialog.ColumnInfo.Value"),
                            ColumnInfo.COLUMN_TYPE_TEXT,
                            false)
            };

    colinf[1].setUsingVariables(true);
    wHeaders =
            new TableView(
                    variables,
                    wAdditionalComp,
                    SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
                    colinf,
                    FieldsRows,
                    lsMod,
                    props);

    FormData fdHeaders = new FormData();
    fdHeaders.left = new FormAttachment(0, 0);
    fdHeaders.top = new FormAttachment(wlFields, margin);
    fdHeaders.right = new FormAttachment(100, -margin);
    fdHeaders.bottom = new FormAttachment(100, -margin);
    wHeaders.setLayoutData(fdHeaders);

    FormData fdAdditionalComp = new FormData();
    fdAdditionalComp.left = new FormAttachment(0, 0);
    fdAdditionalComp.top = new FormAttachment(wTransformName, margin);
    fdAdditionalComp.right = new FormAttachment(100, -margin);
    fdAdditionalComp.bottom = new FormAttachment(100, 0);
    wAdditionalComp.setLayoutData(fdAdditionalComp);

    wAdditionalComp.layout();
    wAdditionalTab.setControl(wAdditionalComp);
    // ///////////////////////////////////////////////////////////
    // / END OF HEADER TAB
    // ///////////////////////////////////////////////////////////


    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);
    wTabFolder.setSelection(0);


    //
    // Search and set the fields in the background
    //

    final Runnable runnable =
        new Runnable() {
          @Override
          public void run() {
            TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
            if (transformMeta != null) {
              try {
                IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

                // Remember these fields...
                Map<String, Integer> inputFields = new HashMap<>();
                for (int i = 0; i < row.size(); i++) {
                  inputFields.put(row.getValueMeta(i).getName(), i);
                }

                Set<String> keySet = inputFields.keySet();
                List<String> entries = new ArrayList<>(keySet);
                inputFieldNames = entries.toArray(new String[entries.size()]);
                Const.sortStrings(inputFieldNames);
              } catch (HopException e) {
                log.logError(
                    toString(),
                    BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
              }
            }
          }
        };
    new Thread(runnable).start();

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "DorisBulkLoaderDialog.Log.GettingKeyInfo"));
    }

    if (input.getFeHost() != null) {
      wFeHost.setText(input.getFeHost());
    }
    if (input.getFeHttpPort() != null) {
      wFeHttpPort.setText(input.getFeHttpPort());
    }
    if (input.getDatabaseName() != null) {
      wDatabaseName.setText(input.getDatabaseName());
    }
    if (input.getTableName() != null) {
      wTableName.setText(input.getTableName());
    }
    if (input.getLoginUser() != null) {
      wHttpLogin.setText(input.getLoginUser());
    }
    if (input.getLoginPassword() != null) {
      wHttpPassword.setText(input.getLoginPassword());
    }
    if (input.getDataField() != null) {
      wDataField.setText(input.getDataField());
    }
    if (input.getFormat() != null) {
      wFormat.setText(input.getFormat());

      if (JSON.equals(wFormat.getText())) {
        wLineDelimiter.setText(",");
        wLineDelimiter.setEnabled(false);
        wColumnDelimiter.setText(",");
        wColumnDelimiter.setEnabled(false);
      }
    }
    if (input.getLineDelimiter() != null) {
      wLineDelimiter.setText(input.getLineDelimiter());
    }
    if (input.getColumnDelimiter() != null) {
      wColumnDelimiter.setText(input.getColumnDelimiter());
    }

    if (input.getHeaderNames() != null) {
      for (int i = 0; i < input.getHeaderNames().length; i++) {
        TableItem item = wHeaders.table.getItem(i);
        if (input.getHeaderNames()[i] != null) {
          item.setText(1, input.getHeaderNames()[i]);
        }
        if (input.getHeaderValues()[i] != null) {
          item.setText(2, input.getHeaderValues()[i]);
        }
      }
    }

    wBufferSize.setText(Integer.toString(input.getBufferSize()));
    wBufferCount.setText(Integer.toString(input.getBufferSize()));

    wTransformName.selectAll();
    wTransformName.setFocus();

    input.setChanged(changed);
  }

  /**
   * It will be called when click cancel button
   */
  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  /**
   * It will be called when click ok button
   */
  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    input.setFeHost(wFeHost.getText());
    input.setFeHttpPort(wFeHttpPort.getText());
    input.setDatabaseName(wDatabaseName.getText());
    input.setTableName(wTableName.getText());
    input.setDataField(wDataField.getText());
    input.setLoginUser(wHttpLogin.getText());
    input.setLoginPassword(wHttpPassword.getText());
    input.setFormat(wFormat.getText());
    input.setLineDelimiter(wLineDelimiter.getText());
    input.setColumnDelimiter(wColumnDelimiter.getText());

    int headerCount = wHeaders.nrNonEmpty();
    input.allocate(headerCount);
    for (int i = 0; i < headerCount; i++) {
      TableItem item = wHeaders.getNonEmpty(i);
      input.getHeaderNames()[i] = item.getText(1);
      input.getHeaderValues()[i] = item.getText(2);
    }

    input.setBufferSize(Integer.parseInt(wBufferSize.getText()));
    input.setBufferCount(Integer.parseInt(wBufferCount.getText()));
    transformName = wTransformName.getText(); // return value

    dispose();
  }

}
