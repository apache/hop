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

package org.apache.hop.pipeline.transforms.execsqlrow;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class ExecSqlRowDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ExecSqlRowMeta.class;

  private boolean gotPreviousFields = false;
  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Text wInsertField;

  private Text wUpdateField;

  private Text wDeleteField;

  private Text wReadField;

  private CCombo wSqlFieldName;

  private Text wCommit;

  private final ExecSqlRowMeta input;

  private Button wSqlFromFile;

  private Button wSendOneStatement;

  public ExecSqlRowDialog(
      Shell parent, IVariables variables, ExecSqlRowMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ExecSqlRowDialog.Shell.Label"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        };
    changed = input.hasChanged();

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolledComposite = new FormData();
    fdScrolledComposite.left = new FormAttachment(0, 0);
    fdScrolledComposite.top = new FormAttachment(wSpacer, 0);
    fdScrolledComposite.right = new FormAttachment(100, 0);
    fdScrolledComposite.bottom = new FormAttachment(wOk, -margin);
    scrolledComposite.setLayoutData(fdScrolledComposite);
    scrolledComposite.setLayout(new FillLayout());

    Composite wContent = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    // Connection line
    wConnection = addConnectionLine(wContent, null, input.getConnection(), lsMod);
    wConnection.addSelectionListener(lsSelection);

    // Commit line
    Label wlCommit = new Label(wContent, SWT.RIGHT);
    wlCommit.setText(BaseMessages.getString(PKG, "ExecSqlRowDialog.Commit.Label"));
    PropsUi.setLook(wlCommit);
    FormData fdlCommit = new FormData();
    fdlCommit.left = new FormAttachment(0, 0);
    fdlCommit.top = new FormAttachment(wConnection, margin);
    fdlCommit.right = new FormAttachment(middle, -margin);
    wlCommit.setLayoutData(fdlCommit);
    wCommit = new Text(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCommit);
    wCommit.addModifyListener(lsMod);
    FormData fdCommit = new FormData();
    fdCommit.left = new FormAttachment(middle, 0);
    fdCommit.top = new FormAttachment(wConnection, margin);
    fdCommit.right = new FormAttachment(100, 0);
    wCommit.setLayoutData(fdCommit);

    Label wlSendOneStatement = new Label(wContent, SWT.RIGHT);
    wlSendOneStatement.setText(
        BaseMessages.getString(PKG, "ExecSqlRowDialog.SendOneStatement.Label"));
    PropsUi.setLook(wlSendOneStatement);
    FormData fdlSendOneStatement = new FormData();
    fdlSendOneStatement.left = new FormAttachment(0, 0);
    fdlSendOneStatement.top = new FormAttachment(wCommit, margin);
    fdlSendOneStatement.right = new FormAttachment(middle, -margin);
    wlSendOneStatement.setLayoutData(fdlSendOneStatement);
    wSendOneStatement = new Button(wContent, SWT.CHECK);
    wSendOneStatement.setToolTipText(
        BaseMessages.getString(PKG, "ExecSqlRowDialog.SendOneStatement.Tooltip"));
    PropsUi.setLook(wSendOneStatement);
    FormData fdSendOneStatement = new FormData();
    fdSendOneStatement.left = new FormAttachment(middle, 0);
    fdSendOneStatement.top = new FormAttachment(wlSendOneStatement, 0, SWT.CENTER);
    fdSendOneStatement.right = new FormAttachment(100, 0);
    wSendOneStatement.setLayoutData(fdSendOneStatement);
    wSendOneStatement.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // SQLFieldName field
    Label wlSqlFieldName = new Label(wContent, SWT.RIGHT);
    wlSqlFieldName.setText(BaseMessages.getString(PKG, "ExecSqlRowDialog.SQLFieldName.Label"));
    PropsUi.setLook(wlSqlFieldName);
    FormData fdlSqlFieldName = new FormData();
    fdlSqlFieldName.left = new FormAttachment(0, 0);
    fdlSqlFieldName.right = new FormAttachment(middle, -margin);
    fdlSqlFieldName.top = new FormAttachment(wSendOneStatement, margin);
    wlSqlFieldName.setLayoutData(fdlSqlFieldName);
    wSqlFieldName = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    wSqlFieldName.setEditable(true);
    PropsUi.setLook(wSqlFieldName);
    wSqlFieldName.addModifyListener(lsMod);
    FormData fdSqlFieldName = new FormData();
    fdSqlFieldName.left = new FormAttachment(middle, 0);
    fdSqlFieldName.top = new FormAttachment(wSendOneStatement, margin);
    fdSqlFieldName.right = new FormAttachment(100, -margin);
    wSqlFieldName.setLayoutData(fdSqlFieldName);
    wSqlFieldName.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Ignore focusLost
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            get();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    Label wlSqlFromFile = new Label(wContent, SWT.RIGHT);
    wlSqlFromFile.setText(BaseMessages.getString(PKG, "ExecSqlRowDialog.SQLFromFile.Label"));
    PropsUi.setLook(wlSqlFromFile);
    FormData fdlSqlFromFile = new FormData();
    fdlSqlFromFile.left = new FormAttachment(0, 0);
    fdlSqlFromFile.top = new FormAttachment(wSqlFieldName, margin);
    fdlSqlFromFile.right = new FormAttachment(middle, -margin);
    wlSqlFromFile.setLayoutData(fdlSqlFromFile);
    wSqlFromFile = new Button(wContent, SWT.CHECK);
    wSqlFromFile.setToolTipText(
        BaseMessages.getString(PKG, "ExecSqlRowDialog.SQLFromFile.Tooltip"));
    PropsUi.setLook(wSqlFromFile);
    FormData fdSqlFromFile = new FormData();
    fdSqlFromFile.left = new FormAttachment(middle, 0);
    fdSqlFromFile.top = new FormAttachment(wlSqlFromFile, 0, SWT.CENTER);
    fdSqlFromFile.right = new FormAttachment(100, 0);
    wSqlFromFile.setLayoutData(fdSqlFromFile);
    wSqlFromFile.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // ///////////////////////////////
    // START OF Additional Fields GROUP //
    // ///////////////////////////////

    Group wAdditionalFields = new Group(wContent, SWT.SHADOW_NONE);
    PropsUi.setLook(wAdditionalFields);
    wAdditionalFields.setText(
        BaseMessages.getString(PKG, "ExecSqlRowDialog.wAdditionalFields.Label"));

    FormLayout additionalFieldsgroupLayout = new FormLayout();
    additionalFieldsgroupLayout.marginWidth = 10;
    additionalFieldsgroupLayout.marginHeight = 10;
    wAdditionalFields.setLayout(additionalFieldsgroupLayout);

    // insert field
    Label wlInsertField = new Label(wAdditionalFields, SWT.RIGHT);
    wlInsertField.setText(BaseMessages.getString(PKG, "ExecSqlRowDialog.InsertField.Label"));
    PropsUi.setLook(wlInsertField);
    FormData fdlInsertField = new FormData();
    fdlInsertField.left = new FormAttachment(0, margin);
    fdlInsertField.right = new FormAttachment(middle, -margin);
    fdlInsertField.top = new FormAttachment(wSqlFromFile, margin);
    wlInsertField.setLayoutData(fdlInsertField);
    wInsertField = new Text(wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInsertField);
    wInsertField.addModifyListener(lsMod);
    FormData fdInsertField = new FormData();
    fdInsertField.left = new FormAttachment(middle, 0);
    fdInsertField.top = new FormAttachment(wSqlFromFile, margin);
    fdInsertField.right = new FormAttachment(100, 0);
    wInsertField.setLayoutData(fdInsertField);

    // Update field
    Label wlUpdateField = new Label(wAdditionalFields, SWT.RIGHT);
    wlUpdateField.setText(BaseMessages.getString(PKG, "ExecSqlRowDialog.UpdateField.Label"));
    PropsUi.setLook(wlUpdateField);
    FormData fdlUpdateField = new FormData();
    fdlUpdateField.left = new FormAttachment(0, margin);
    fdlUpdateField.right = new FormAttachment(middle, -margin);
    fdlUpdateField.top = new FormAttachment(wInsertField, margin);
    wlUpdateField.setLayoutData(fdlUpdateField);
    wUpdateField = new Text(wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUpdateField);
    wUpdateField.addModifyListener(lsMod);
    FormData fdUpdateField = new FormData();
    fdUpdateField.left = new FormAttachment(middle, 0);
    fdUpdateField.top = new FormAttachment(wInsertField, margin);
    fdUpdateField.right = new FormAttachment(100, 0);
    wUpdateField.setLayoutData(fdUpdateField);

    // Delete field
    Label wlDeleteField = new Label(wAdditionalFields, SWT.RIGHT);
    wlDeleteField.setText(BaseMessages.getString(PKG, "ExecSqlRowDialog.DeleteField.Label"));
    PropsUi.setLook(wlDeleteField);
    FormData fdlDeleteField = new FormData();
    fdlDeleteField.left = new FormAttachment(0, margin);
    fdlDeleteField.right = new FormAttachment(middle, -margin);
    fdlDeleteField.top = new FormAttachment(wUpdateField, margin);
    wlDeleteField.setLayoutData(fdlDeleteField);
    wDeleteField = new Text(wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDeleteField);
    wDeleteField.addModifyListener(lsMod);
    FormData fdDeleteField = new FormData();
    fdDeleteField.left = new FormAttachment(middle, 0);
    fdDeleteField.top = new FormAttachment(wUpdateField, margin);
    fdDeleteField.right = new FormAttachment(100, 0);
    wDeleteField.setLayoutData(fdDeleteField);

    // Read field
    Label wlReadField = new Label(wAdditionalFields, SWT.RIGHT);
    wlReadField.setText(BaseMessages.getString(PKG, "ExecSqlRowDialog.ReadField.Label"));
    PropsUi.setLook(wlReadField);
    FormData fdlReadField = new FormData();
    fdlReadField.left = new FormAttachment(0, 0);
    fdlReadField.right = new FormAttachment(middle, -margin);
    fdlReadField.top = new FormAttachment(wDeleteField, margin);
    wlReadField.setLayoutData(fdlReadField);
    wReadField = new Text(wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wReadField);
    wReadField.addModifyListener(lsMod);
    FormData fdReadField = new FormData();
    fdReadField.left = new FormAttachment(middle, 0);
    fdReadField.top = new FormAttachment(wDeleteField, margin);
    fdReadField.right = new FormAttachment(100, 0);
    wReadField.setLayoutData(fdReadField);

    FormData fdAdditionalFields = new FormData();
    fdAdditionalFields.left = new FormAttachment(0, margin);
    fdAdditionalFields.top = new FormAttachment(wSqlFromFile, margin);
    fdAdditionalFields.right = new FormAttachment(100, -margin);
    wAdditionalFields.setLayoutData(fdAdditionalFields);

    // ///////////////////////////////
    // END OF Additional Fields GROUP //
    // ///////////////////////////////

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wCommit.setText("" + input.getCommitSize());
    if (input.getSqlFieldName() != null) {
      wSqlFieldName.setText(input.getSqlFieldName());
    }
    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }

    if (input.getUpdateField() != null) {
      wUpdateField.setText(input.getUpdateField());
    }
    if (input.getInsertField() != null) {
      wInsertField.setText(input.getInsertField());
    }
    if (input.getDeleteField() != null) {
      wDeleteField.setText(input.getDeleteField());
    }
    if (input.getReadField() != null) {
      wReadField.setText(input.getReadField());
    }
    wSqlFromFile.setSelection(input.isSqlFromfile());
    wSendOneStatement.setSelection(input.isSendOneStatement());
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

    if (wConnection.getText() == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "ExecSqlRowDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "ExecSqlRowDialog.InvalidConnection.DialogTitle"));
      mb.open();
      return;
    }

    input.setCommitSize(Const.toInt(wCommit.getText(), 0));
    transformName = wTransformName.getText(); // return value
    input.setSqlFieldName(wSqlFieldName.getText());
    // copy info to TextFileInputMeta class (input)
    input.setConnection(wConnection.getText());

    input.setInsertField(wInsertField.getText());
    input.setUpdateField(wUpdateField.getText());
    input.setDeleteField(wDeleteField.getText());
    input.setReadField(wReadField.getText());
    input.setSqlFromfile(wSqlFromFile.getSelection());
    input.setSendOneStatement(wSendOneStatement.getSelection());

    dispose();
  }

  private void get() {
    if (!gotPreviousFields) {
      gotPreviousFields = true;
      try {
        String sqlfield = wSqlFieldName.getText();
        wSqlFieldName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wSqlFieldName.removeAll();
          wSqlFieldName.setItems(r.getFieldNames());
        }
        if (sqlfield != null) {
          wSqlFieldName.setText(sqlfield);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "ExecSqlRowDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "ExecSqlRowDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
    }
  }
}
