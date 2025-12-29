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

package org.apache.hop.avro.transforms.avrooutput;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class AvroOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG =
      AvroOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String[] YES_NO_COMBO =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  private static final String[] OUTPUT_TYPE_DESC =
      new String[] {
        BaseMessages.getString(PKG, "AvroOutputDialog.OutputType.BinaryFile"),
        BaseMessages.getString(PKG, "AvroOutputDialog.OutputType.BinaryField"),
        BaseMessages.getString(PKG, "AvroOutputDialog.OutputType.JsonField")
      };
  public static final String CONST_SYSTEM_DIALOG_GET_FIELDS_FAILED_MESSAGE =
      "System.Dialog.GetFieldsFailed.Message";

  private CCombo wOutputType;
  private TextVar wFilename;
  private TextVar wOutputField;
  private Button wbSchema;
  private Label wlSchema;
  private TextVar wSchema;
  private Button wCreateParentFolder;
  private Button wAddTransformNr;
  private Button wAddPartNr;
  private Label wlAddDate;
  private Button wAddDate;
  private Label wlAddTime;
  private Button wAddTime;
  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;
  private CCombo wCompression;
  private Button wSpecifyFormat;
  private Button wCreateSchemaFile;
  private Label wlWriteSchemaFile;
  private Button wWriteSchemaFile;
  private Label wlRecordName;
  private TextVar wRecordName;
  private Label wlNamespace;
  private TextVar wNamespace;
  private Label wlDoc;
  private TextVar wDoc;
  private TableView wFields;
  private final AvroOutputMeta input;
  private Button wAddToResult;
  private ColumnInfo[] colinf;
  private final List<String> inputFields = new ArrayList<>();
  private Schema avroSchema;
  private boolean validSchema = false;
  private String[] avroFieldNames = null;

  public AvroOutputDialog(
      Shell parent, IVariables variables, AvroOutputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "AvroOutputDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Add the buttons at the bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF FILE TAB///
    // /
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wFileTab.setText(BaseMessages.getString(PKG, "AvroOutputDialog.FileTab.TabTitle"));

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout(fileLayout);

    //
    // Output type
    Label wlOutputType = new Label(wFileComp, SWT.RIGHT);
    wlOutputType.setText(BaseMessages.getString(PKG, "AvroOutputDialog.OutputType.Label"));
    PropsUi.setLook(wlOutputType);
    FormData fdlOutputType = new FormData();
    fdlOutputType.left = new FormAttachment(0, 0);
    fdlOutputType.top = new FormAttachment(0, margin);
    fdlOutputType.right = new FormAttachment(middle, -margin);
    wlOutputType.setLayoutData(fdlOutputType);

    wOutputType = new CCombo(wFileComp, SWT.BORDER | SWT.READ_ONLY);
    wOutputType.setEditable(false);
    PropsUi.setLook(wOutputType);
    wOutputType.addListener(SWT.Selection, e -> enableFields());
    FormData fdOutputType = new FormData();
    fdOutputType.left = new FormAttachment(middle, 0);
    fdOutputType.top = new FormAttachment(0, margin);
    fdOutputType.right = new FormAttachment(75, 0);
    wOutputType.setLayoutData(fdOutputType);
    for (String outputTypeDesc : OUTPUT_TYPE_DESC) {
      wOutputType.add(outputTypeDesc);
    }

    // Filename line
    Label wlFilename = new Label(wFileComp, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "AvroOutputDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wOutputType, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wOutputType, margin);
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(wOutputType, margin);
    fdFilename.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener(
        e -> wFilename.setToolTipText(variables.resolve(wFilename.getText())));

    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                true,
                shell,
                wFilename,
                variables,
                new String[] {"*.avro", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "AvroOutputDialog.AvroFiles"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));

    // Fieldname line
    Label wlOutputField = new Label(wFileComp, SWT.RIGHT);
    wlOutputField.setText(BaseMessages.getString(PKG, "AvroOutputDialog.OutputField.Label"));
    PropsUi.setLook(wlOutputField);
    FormData fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment(0, 0);
    fdlOutputField.top = new FormAttachment(wFilename, margin);
    fdlOutputField.right = new FormAttachment(middle, -margin);
    wlOutputField.setLayoutData(fdlOutputField);

    wOutputField = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wOutputField);
    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(middle, 0);
    fdOutputField.top = new FormAttachment(wFilename, margin);
    fdOutputField.right = new FormAttachment(100, -margin);
    wOutputField.setLayoutData(fdOutputField);

    // Automatically create schema?
    //
    Label wlCreateSchemaFile = new Label(wFileComp, SWT.RIGHT);
    wlCreateSchemaFile.setText(
        BaseMessages.getString(PKG, "AvroOutputDialog.CreateSchemaFile.Label"));
    PropsUi.setLook(wlCreateSchemaFile);
    FormData fdlCreateSchemaFile = new FormData();
    fdlCreateSchemaFile.left = new FormAttachment(0, 0);
    fdlCreateSchemaFile.top = new FormAttachment(wOutputField, margin);
    fdlCreateSchemaFile.right = new FormAttachment(middle, -margin);
    wlCreateSchemaFile.setLayoutData(fdlCreateSchemaFile);
    wCreateSchemaFile = new Button(wFileComp, SWT.CHECK);
    wCreateSchemaFile.setToolTipText(
        BaseMessages.getString(PKG, "AvroOutputDialog.CreateSchemaFile.Tooltip"));
    PropsUi.setLook(wCreateSchemaFile);
    FormData fdCreateSchemaFile = new FormData();
    fdCreateSchemaFile.left = new FormAttachment(middle, 0);
    fdCreateSchemaFile.top = new FormAttachment(wlCreateSchemaFile, 0, SWT.CENTER);
    fdCreateSchemaFile.right = new FormAttachment(100, 0);
    wCreateSchemaFile.setLayoutData(fdCreateSchemaFile);
    wCreateSchemaFile.addListener(SWT.Selection, e -> enableFields());

    // Write Schema File
    //
    wlWriteSchemaFile = new Label(wFileComp, SWT.RIGHT);
    wlWriteSchemaFile.setText(
        BaseMessages.getString(PKG, "AvroOutputDialog.WriteSchemaFile.Label"));
    PropsUi.setLook(wlWriteSchemaFile);
    FormData fdlWriteSchemaFile = new FormData();
    fdlWriteSchemaFile.left = new FormAttachment(0, 0);
    fdlWriteSchemaFile.top = new FormAttachment(wlCreateSchemaFile, 2 * margin);
    fdlWriteSchemaFile.right = new FormAttachment(middle, -margin);
    wlWriteSchemaFile.setLayoutData(fdlWriteSchemaFile);
    wWriteSchemaFile = new Button(wFileComp, SWT.CHECK);
    wWriteSchemaFile.setToolTipText(
        BaseMessages.getString(PKG, "AvroOutputDialog.WriteSchemaFile.Tooltip"));
    PropsUi.setLook(wWriteSchemaFile);
    FormData fdWriteSchemaFile = new FormData();
    fdWriteSchemaFile.left = new FormAttachment(middle, 0);
    fdWriteSchemaFile.top = new FormAttachment(wlWriteSchemaFile, 0, SWT.CENTER);
    fdWriteSchemaFile.right = new FormAttachment(100, 0);
    wWriteSchemaFile.setLayoutData(fdWriteSchemaFile);
    wWriteSchemaFile.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableFields();
          }
        });

    // Namespace
    //
    wlNamespace = new Label(wFileComp, SWT.RIGHT);
    wlNamespace.setText(BaseMessages.getString(PKG, "AvroOutputDialog.Namespace.Label"));
    PropsUi.setLook(wlNamespace);
    FormData fdlNamespace = new FormData();
    fdlNamespace.left = new FormAttachment(0, 0);
    fdlNamespace.top = new FormAttachment(wlWriteSchemaFile, 2 * margin);
    fdlNamespace.right = new FormAttachment(middle, -margin);
    wlNamespace.setLayoutData(fdlNamespace);
    wNamespace = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wNamespace);
    FormData fdNamespace = new FormData();
    fdNamespace.left = new FormAttachment(middle, 0);
    fdNamespace.top = new FormAttachment(wlNamespace, 0, SWT.CENTER);
    fdNamespace.right = new FormAttachment(100, 0);
    wNamespace.setLayoutData(fdNamespace);
    wNamespace.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Record Name
    //
    wlRecordName = new Label(wFileComp, SWT.RIGHT);
    wlRecordName.setText(BaseMessages.getString(PKG, "AvroOutputDialog.RecordName.Label"));
    PropsUi.setLook(wlRecordName);
    FormData fdlRecordName = new FormData();
    fdlRecordName.left = new FormAttachment(0, 0);
    fdlRecordName.top = new FormAttachment(wNamespace, margin);
    fdlRecordName.right = new FormAttachment(middle, -margin);
    wlRecordName.setLayoutData(fdlRecordName);
    wRecordName = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRecordName);
    FormData fdRecordName = new FormData();
    fdRecordName.left = new FormAttachment(middle, 0);
    fdRecordName.top = new FormAttachment(wNamespace, margin);
    fdRecordName.right = new FormAttachment(100, 0);
    wRecordName.setLayoutData(fdRecordName);
    wRecordName.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Doc
    //
    wlDoc = new Label(wFileComp, SWT.RIGHT);
    wlDoc.setText(BaseMessages.getString(PKG, "AvroOutputDialog.Doc.Label"));
    PropsUi.setLook(wlDoc);
    FormData fdlDoc = new FormData();
    fdlDoc.left = new FormAttachment(0, 0);
    fdlDoc.top = new FormAttachment(wRecordName, margin);
    fdlDoc.right = new FormAttachment(middle, -margin);
    wlDoc.setLayoutData(fdlDoc);
    wDoc = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDoc);
    FormData fdDoc = new FormData();
    fdDoc.left = new FormAttachment(middle, 0);
    fdDoc.top = new FormAttachment(wRecordName, margin);
    fdDoc.right = new FormAttachment(100, 0);
    wDoc.setLayoutData(fdDoc);
    wDoc.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Schema Filename line

    ModifyListener lsSchema = e -> updateSchema();

    wlSchema = new Label(wFileComp, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "AvroOutputDialog.Schema.Label"));
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.top = new FormAttachment(wDoc, margin);
    fdlSchema.right = new FormAttachment(middle, -margin);
    wlSchema.setLayoutData(fdlSchema);

    wbSchema = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSchema = new FormData();
    fdbSchema.right = new FormAttachment(100, 0);
    fdbSchema.top = new FormAttachment(wDoc, 0);
    wbSchema.setLayoutData(fdbSchema);

    wSchema = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsSchema);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wDoc, margin);
    fdSchema.right = new FormAttachment(wbSchema, -margin);
    wSchema.setLayoutData(fdSchema);

    wbSchema.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                true,
                shell,
                wSchema,
                variables,
                new String[] {"*.avsc", "*.json", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "AvroOutputDialog.AvroFilesSchema"),
                  BaseMessages.getString(PKG, "AvroOutputDialog.AvroFilesSchemaJson"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));

    // Create Parent Folder?
    //
    Label wlCreateParentFolder = new Label(wFileComp, SWT.RIGHT);
    wlCreateParentFolder.setText(
        BaseMessages.getString(PKG, "AvroOutputDialog.CreateParentFolder.Label"));
    PropsUi.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment(0, 0);
    fdlCreateParentFolder.top = new FormAttachment(wSchema, margin);
    fdlCreateParentFolder.right = new FormAttachment(middle, -margin);
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(wFileComp, SWT.CHECK);
    wCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "AvroOutputDialog.CreateParentFolder.Tooltip"));
    PropsUi.setLook(wCreateParentFolder);
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment(middle, 0);
    fdCreateParentFolder.top = new FormAttachment(wlCreateParentFolder, 0, SWT.CENTER);
    fdCreateParentFolder.right = new FormAttachment(100, 0);
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Compression
    //
    Label wlCompression = new Label(wFileComp, SWT.RIGHT);
    wlCompression.setText(BaseMessages.getString(PKG, "AvroOutputDialog.Compression.Label"));
    PropsUi.setLook(wlCompression);
    FormData fdlCompression = new FormData();
    fdlCompression.left = new FormAttachment(0, 0);
    fdlCompression.top = new FormAttachment(wlCreateParentFolder, 2 * margin);
    fdlCompression.right = new FormAttachment(middle, -margin);
    wlCompression.setLayoutData(fdlCompression);
    wCompression = new CCombo(wFileComp, SWT.BORDER | SWT.READ_ONLY);
    wCompression.setEditable(true);
    PropsUi.setLook(wCompression);
    FormData fdCompression = new FormData();
    fdCompression.left = new FormAttachment(middle, 0);
    fdCompression.top = new FormAttachment(wlCompression, 0, SWT.CENTER);
    fdCompression.right = new FormAttachment(75, 0);
    wCompression.setLayoutData(fdCompression);
    String[] compressions = AvroOutputMeta.compressionTypes;
    for (String compression : compressions) {
      wCompression.add(compression);
    }

    // Add transform number?
    //
    Label wlAddTransformNr = new Label(wFileComp, SWT.RIGHT);
    wlAddTransformNr.setText(BaseMessages.getString(PKG, "AvroOutputDialog.AddTransformNr.Label"));
    PropsUi.setLook(wlAddTransformNr);
    FormData fdlAddTransformNr = new FormData();
    fdlAddTransformNr.left = new FormAttachment(0, 0);
    fdlAddTransformNr.top = new FormAttachment(wCompression, margin);
    fdlAddTransformNr.right = new FormAttachment(middle, -margin);
    wlAddTransformNr.setLayoutData(fdlAddTransformNr);
    wAddTransformNr = new Button(wFileComp, SWT.CHECK);
    PropsUi.setLook(wAddTransformNr);
    FormData fdAddTransformNr = new FormData();
    fdAddTransformNr.left = new FormAttachment(middle, 0);
    fdAddTransformNr.top = new FormAttachment(wlAddTransformNr, 0, SWT.CENTER);
    fdAddTransformNr.right = new FormAttachment(100, 0);
    wAddTransformNr.setLayoutData(fdAddTransformNr);
    wAddTransformNr.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Add part number to filename?
    //
    Label wlAddPartNr = new Label(wFileComp, SWT.RIGHT);
    wlAddPartNr.setText(BaseMessages.getString(PKG, "AvroOutputDialog.AddPartNr.Label"));
    PropsUi.setLook(wlAddPartNr);
    FormData fdlAddPartNr = new FormData();
    fdlAddPartNr.left = new FormAttachment(0, 0);
    fdlAddPartNr.top = new FormAttachment(wlAddTransformNr, 2 * margin);
    fdlAddPartNr.right = new FormAttachment(middle, -margin);
    wlAddPartNr.setLayoutData(fdlAddPartNr);
    wAddPartNr = new Button(wFileComp, SWT.CHECK);
    PropsUi.setLook(wAddPartNr);
    FormData fdAddPartNr = new FormData();
    fdAddPartNr.left = new FormAttachment(middle, 0);
    fdAddPartNr.top = new FormAttachment(wlAddPartNr, 0, SWT.CENTER);
    fdAddPartNr.right = new FormAttachment(100, 0);
    wAddPartNr.setLayoutData(fdAddPartNr);
    wAddPartNr.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Add date to filename?
    //
    wlAddDate = new Label(wFileComp, SWT.RIGHT);
    wlAddDate.setText(BaseMessages.getString(PKG, "AvroOutputDialog.AddDate.Label"));
    PropsUi.setLook(wlAddDate);
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment(0, 0);
    fdlAddDate.top = new FormAttachment(wlAddPartNr, 2 * margin);
    fdlAddDate.right = new FormAttachment(middle, -margin);
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wFileComp, SWT.CHECK);
    PropsUi.setLook(wAddDate);
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment(middle, 0);
    fdAddDate.top = new FormAttachment(wlAddDate, 0, SWT.CENTER);
    fdAddDate.right = new FormAttachment(100, 0);
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Add time to the filename?
    //
    wlAddTime = new Label(wFileComp, SWT.RIGHT);
    wlAddTime.setText(BaseMessages.getString(PKG, "AvroOutputDialog.AddTime.Label"));
    PropsUi.setLook(wlAddTime);
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment(0, 0);
    fdlAddTime.top = new FormAttachment(wlAddDate, 2 * margin);
    fdlAddTime.right = new FormAttachment(middle, -margin);
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wFileComp, SWT.CHECK);
    PropsUi.setLook(wAddTime);
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment(middle, 0);
    fdAddTime.top = new FormAttachment(wlAddTime, 0, SWT.CENTER);
    fdAddTime.right = new FormAttachment(100, 0);
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Specify a date time format?
    //
    Label wlSpecifyFormat = new Label(wFileComp, SWT.RIGHT);
    wlSpecifyFormat.setText(BaseMessages.getString(PKG, "AvroOutputDialog.SpecifyFormat.Label"));
    PropsUi.setLook(wlSpecifyFormat);
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment(0, 0);
    fdlSpecifyFormat.top = new FormAttachment(wlAddTime, 2 * margin);
    fdlSpecifyFormat.right = new FormAttachment(middle, -margin);
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button(wFileComp, SWT.CHECK);
    PropsUi.setLook(wSpecifyFormat);
    wSpecifyFormat.setToolTipText(
        BaseMessages.getString(PKG, "AvroOutputDialog.SpecifyFormat.Tooltip"));
    FormData fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment(middle, 0);
    fdSpecifyFormat.top = new FormAttachment(wlSpecifyFormat, 0, SWT.CENTER);
    fdSpecifyFormat.right = new FormAttachment(100, 0);
    wSpecifyFormat.setLayoutData(fdSpecifyFormat);
    wSpecifyFormat.addListener(SWT.Selection, e -> enableFields());

    // The date-time format
    //
    wlDateTimeFormat = new Label(wFileComp, SWT.RIGHT);
    wlDateTimeFormat.setText(BaseMessages.getString(PKG, "AvroOutputDialog.DateTimeFormat.Label"));
    PropsUi.setLook(wlDateTimeFormat);
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment(0, 0);
    fdlDateTimeFormat.top = new FormAttachment(wlSpecifyFormat, 2 * margin);
    fdlDateTimeFormat.right = new FormAttachment(middle, -margin);
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo(wFileComp, SWT.BORDER | SWT.READ_ONLY);
    wDateTimeFormat.setEditable(true);
    PropsUi.setLook(wDateTimeFormat);
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment(middle, 0);
    fdDateTimeFormat.top = new FormAttachment(wlDateTimeFormat, 0, SWT.CENTER);
    fdDateTimeFormat.right = new FormAttachment(75, 0);
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    String[] dats = Const.getDateFormats();
    for (String dat : dats) {
      wDateTimeFormat.add(dat);
    }

    // Add filename to the result files name
    //
    Label wlAddToResult = new Label(wFileComp, SWT.RIGHT);
    wlAddToResult.setText(BaseMessages.getString(PKG, "AvroOutputDialog.AddFileToResult.Label"));
    PropsUi.setLook(wlAddToResult);
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment(0, 0);
    fdlAddToResult.top = new FormAttachment(wDateTimeFormat, 2 * margin);
    fdlAddToResult.right = new FormAttachment(middle, -margin);
    wlAddToResult.setLayoutData(fdlAddToResult);
    wAddToResult = new Button(wFileComp, SWT.CHECK);
    wAddToResult.setToolTipText(
        BaseMessages.getString(PKG, "AvroOutputDialog.AddFileToResult.Tooltip"));
    PropsUi.setLook(wAddToResult);
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment(middle, 0);
    fdAddToResult.top = new FormAttachment(wlAddToResult, 0, SWT.CENTER);
    fdAddToResult.right = new FormAttachment(100, 0);
    wAddToResult.setLayoutData(fdAddToResult);
    SelectionAdapter lsSelR =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wAddToResult.addSelectionListener(lsSelR);

    FormData fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment(0, 0);
    fdFileComp.top = new FormAttachment(0, 0);
    fdFileComp.right = new FormAttachment(100, 0);
    fdFileComp.bottom = new FormAttachment(100, 0);
    wFileComp.setLayoutData(fdFileComp);

    wFileComp.layout();
    wFileTab.setControl(wFileComp);

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "AvroOutputDialog.FieldsTab.TabTitle"));

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout(fieldsLayout);
    PropsUi.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.GetFields"));
    wGet.addListener(SWT.Selection, e -> get());
    fdGet = new FormData();
    fdGet.right = new FormAttachment(50, -margin);
    fdGet.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);

    Button wUpdateTypes = new Button(wFieldsComp, SWT.PUSH);
    wUpdateTypes.setText(BaseMessages.getString(PKG, "AvroOutputDialog.Button.UpdateTypes"));
    wUpdateTypes.setToolTipText(
        BaseMessages.getString(PKG, "AvroOutputDialog.Tooltip.UpdateTypes"));
    wUpdateTypes.addListener(SWT.Selection, e -> updateTypes());
    FormData fdUpdateTypes = new FormData();
    fdUpdateTypes.left = new FormAttachment(wGet, margin * 2);
    fdUpdateTypes.bottom = new FormAttachment(100, 0);
    wUpdateTypes.setLayoutData(fdUpdateTypes);

    final int nrFieldsCols = 4;
    final int nrFieldsRows = input.getOutputFields().size();

    colinf = new ColumnInfo[nrFieldsCols];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "AvroOutputDialog.StreamColumn.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "AvroOutputDialog.AvroColumn.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            getSchemaFields(),
            false);
    colinf[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "AvroOutputDialog.AvroType.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            AvroOutputField.getAvroTypeArraySorted(),
            false);
    colinf[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "AvroOutputDialog.Nullable.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO,
            false);

    colinf[2].setComboValuesSelectionListener(
        (tableItem, rowNr, colNr) -> {
          String[] comboValues;
          if (!(wCreateSchemaFile.getSelection()) && validSchema) {
            String avroColumn = tableItem.getText(colNr - 1);
            String streamColumn = tableItem.getText(colNr - 2);
            avroColumn = (avroColumn != null ? avroColumn : streamColumn);

            Schema fieldSchema = getFieldSchema(avroColumn);
            if (fieldSchema != null) {
              String[] combo = AvroOutputField.mapAvroType(fieldSchema, fieldSchema.getType());
              comboValues = combo;
            } else {
              comboValues = AvroOutputField.getAvroTypeArraySorted();
            }
          } else {
            comboValues = AvroOutputField.getAvroTypeArraySorted();
          }
          return comboValues;
        });

    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            nrFieldsRows,
            null,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wGet, -margin);
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.add(row.getValueMeta(i).getName());
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, CONST_SYSTEM_DIALOG_GET_FIELDS_FAILED_MESSAGE));
            }
          }
        };
    new Thread(runnable).start();

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    input.setChanged(changed);
    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private Schema getFieldSchema(String avroName) {
    if (avroSchema != null) {
      if (avroName.startsWith("$.")) {
        avroName = avroName.substring(2);
      }

      Schema recordSchema = avroSchema;
      while (avroName.contains(".")) {
        String parentRecord = avroName.substring(0, avroName.indexOf("."));
        avroName = avroName.substring(avroName.indexOf(".") + 1);
        Schema.Field field = recordSchema.getField(parentRecord);
        if (field == null) {
          return null;
        } else {
          recordSchema = field.schema();
        }
      }
      if (log.isDetailed()) {
        logDetailed("Avro name is " + avroName);
        logDetailed("Record Schema is " + recordSchema.toString(true));
      }
      Schema.Field f = recordSchema.getField(avroName);
      if (f == null) {
        return null;
      }
      return f.schema();
    }
    return null;
  }

  private void updateSchema() {

    try {
      avroSchema = null;
      avroSchema = new Schema.Parser().parse(new File(variables.resolve(wSchema.getText())));

      validSchema = true;

      wFields.setColumnInfo(
          1,
          new ColumnInfo(
              BaseMessages.getString(PKG, "AvroOutputDialog.AvroColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              getSchemaFields(),
              false));
    } catch (Exception ex) {
      validSchema = false;
      avroSchema = null;
    }
  }

  private ArrayList<String> getFieldsList(Schema schema, String parent) {
    ArrayList<String> result = new ArrayList<>();
    if (schema.getType() != Schema.Type.RECORD) {
      return result;
    }

    List<Schema.Field> fields = schema.getFields();

    for (Schema.Field f : fields) {
      List<Schema> fieldSchemas;
      if (f.schema().getType() == Schema.Type.UNION) {
        fieldSchemas = f.schema().getTypes();
      } else {
        fieldSchemas = new ArrayList<>();
        fieldSchemas.add(f.schema());
      }

      Iterator<Schema> itTypes = fieldSchemas.iterator();
      boolean added = false;
      while (itTypes.hasNext()) {
        Schema type = itTypes.next();
        if (type.getType() == Schema.Type.NULL) {
          // do nothing
        } else if (type.getType() == Schema.Type.RECORD) {
          String child = "$." + f.name();
          if (!Utils.isEmpty(parent) && !parent.equals("$.")) {
            child = parent + "." + f.name();
          }
          ArrayList<String> children = getFieldsList(type, child);
          result.addAll(children);
        } else if (!added) {
          added = true;
          String name = "$." + f.name();
          if (!Utils.isEmpty(parent) && !parent.equals("$.")) {
            name = parent + "." + f.name();
          }
          result.add(name);
        }
      }
    }

    return result;
  }

  private String[] getSchemaFields() {
    String[] avroFields;

    if (validSchema) {
      ArrayList<String> fields = getFieldsList(avroSchema, "$.");
      avroFields = new String[fields.size() + 1];
      avroFields[0] = "";
      Iterator<String> itNames = fields.iterator();
      int i = 1;
      while (itNames.hasNext()) {
        avroFields[i] = itNames.next();
        i++;
      }
    } else {
      avroFields = new String[1];
      avroFields[0] = "";
    }
    avroFieldNames = avroFields;
    return avroFields;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
  }

  private void enableFields() {
    boolean createSchema = wCreateSchemaFile.getSelection();

    wlWriteSchemaFile.setEnabled(createSchema);
    wWriteSchemaFile.setEnabled(createSchema);

    boolean enableSchema = createSchema && wWriteSchemaFile.getSelection();
    wlDoc.setEnabled(enableSchema);
    wDoc.setEnabled(enableSchema);
    wlNamespace.setEnabled(enableSchema);
    wNamespace.setEnabled(enableSchema);
    wlRecordName.setEnabled(enableSchema);
    wRecordName.setEnabled(enableSchema);
    wlSchema.setEnabled(enableSchema);
    wSchema.setEnabled(enableSchema);
    wbSchema.setEnabled(enableSchema);

    int outputTypeId = wOutputType.getSelectionIndex();
    if (outputTypeId == AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE) {
      wFilename.setEnabled(true);
      wCompression.setEnabled(true);
      wAddTransformNr.setEnabled(true);
      wAddPartNr.setEnabled(true);
      wAddDate.setEnabled(true);
      wAddTime.setEnabled(true);
      wSpecifyFormat.setEnabled(true);
      wDateTimeFormat.setEnabled(true);
      wOutputField.setEnabled(false);
    } else if (outputTypeId == AvroOutputMeta.OUTPUT_TYPE_FIELD
        || outputTypeId == AvroOutputMeta.OUTPUT_TYPE_JSON_FIELD) {
      wFilename.setEnabled(false);
      wCompression.setEnabled(false);
      wAddTransformNr.setEnabled(false);
      wAddPartNr.setEnabled(false);
      wAddDate.setEnabled(false);
      wAddTime.setEnabled(false);
      wSpecifyFormat.setEnabled(false);
      wDateTimeFormat.setEnabled(false);
      wOutputField.setEnabled(true);
    }

    if (wSpecifyFormat.getSelection()) {
      wAddDate.setSelection(false);
      wAddTime.setSelection(false);
    }

    wDateTimeFormat.setEnabled(wSpecifyFormat.getSelection());
    wlDateTimeFormat.setEnabled(wSpecifyFormat.getSelection());
    wAddDate.setEnabled(!(wSpecifyFormat.getSelection()));
    wlAddDate.setEnabled(!(wSpecifyFormat.getSelection()));
    wAddTime.setEnabled(!(wSpecifyFormat.getSelection()));
    wlAddTime.setEnabled(!(wSpecifyFormat.getSelection()));
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {

    if (input.getOutputTypeId() >= 0 && input.getOutputTypeId() < OUTPUT_TYPE_DESC.length) {
      wOutputType.setText(OUTPUT_TYPE_DESC[input.getOutputTypeId()]);
    }
    if (input.getFileName() != null && !wFilename.isDisposed()) {
      wFilename.setText(input.getFileName());
    }
    wOutputField.setText(Const.NVL(input.getOutputFieldName(), ""));
    if (input.getSchemaFileName() != null && !wSchema.isDisposed()) {
      wSchema.setText(input.getSchemaFileName());
      updateSchema();
    }

    if (input.getNamespace() != null && !wNamespace.isDisposed()) {
      wNamespace.setText(input.getNamespace());
    }
    if (input.getDoc() != null && !wDoc.isDisposed()) {
      wDoc.setText(input.getDoc());
    }
    if (input.getRecordName() != null && !wRecordName.isDisposed()) {
      wRecordName.setText(input.getRecordName());
    }

    wCreateParentFolder.setSelection(input.isCreateParentFolder());
    wCreateSchemaFile.setSelection(input.isCreateSchemaFile());
    wWriteSchemaFile.setSelection(input.isWriteSchemaFile());
    wCompression.setText(Const.NVL(input.getCompressionType(), ""));

    wAddDate.setSelection(input.isDateInFilename());
    wAddTime.setSelection(input.isTimeInFilename());
    wDateTimeFormat.setText(Const.NVL(input.getDateTimeFormat(), ""));
    wSpecifyFormat.setSelection(input.isSpecifyingFormat());

    wAddTransformNr.setSelection(input.isTransformNrInFilename());
    wAddPartNr.setSelection(input.isPartNrInFilename());
    wAddToResult.setSelection(input.isAddToResultFilenames());
    logDebug("getting fields info...");

    for (int i = 0; i < input.getOutputFields().size(); i++) {
      AvroOutputField field = input.getOutputFields().get(i);

      TableItem item = wFields.table.getItem(i);
      if (field.getName() != null) {
        item.setText(1, field.getName());
      }
      if (field.getAvroName() != null) {
        item.setText(2, field.getAvroName());
      }
      if (field.getAvroTypeDesc() != null) {
        item.setText(3, field.getAvroTypeDesc());
      }
      if (field.isNullable()) {
        item.setText(4, "Y");
      } else {
        item.setText(4, "N");
      }
    }

    wFields.optWidth(true);

    enableFields();
    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    avroSchema = null;

    dispose();
  }

  private void getInfo(AvroOutputMeta tfoi) {
    tfoi.setOutputTypeById(wOutputType.getSelectionIndex());
    tfoi.setFileName(wFilename.getText());
    tfoi.setOutputFieldName(wOutputField.getText());
    tfoi.setSchemaFileName(wSchema.getText());
    tfoi.setCreateSchemaFile(wCreateSchemaFile.getSelection());
    tfoi.setWriteSchemaFile(wWriteSchemaFile.getSelection());
    tfoi.setNamespace(wNamespace.getText());
    tfoi.setRecordName(wRecordName.getText());
    tfoi.setDoc(wDoc.getText());
    tfoi.setCompressionType(wCompression.getText());
    tfoi.setCreateParentFolder(wCreateParentFolder.getSelection());
    tfoi.setTransformNrInFilename(wAddTransformNr.getSelection());
    tfoi.setPartNrInFilename(wAddPartNr.getSelection());
    tfoi.setDateInFilename(wAddDate.getSelection());
    tfoi.setTimeInFilename(wAddTime.getSelection());
    tfoi.setDateTimeFormat(wDateTimeFormat.getText());
    tfoi.setSpecifyingFormat(wSpecifyFormat.getSelection());
    tfoi.setAddToResultFilenames(wAddToResult.getSelection());

    tfoi.getOutputFields().clear();

    for (TableItem item : wFields.getNonEmptyItems()) {
      AvroOutputField field = new AvroOutputField();

      field.setName(item.getText(1));
      field.setAvroName(item.getText(2));
      field.setAvroType(item.getText(3));
      boolean nullable = "Y".equalsIgnoreCase(item.getText(4));
      if (item.getText(4).isEmpty()) {
        nullable = true;
      }
      field.setNullable(nullable);

      tfoi.getOutputFields().add(field);
    }
  }

  private void ok() {
    avroSchema = null;
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    input.setChanged(true);

    dispose();
  }

  private void updateTypes() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        updateTypes(r, wFields, 1, 2, new int[] {3});
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, CONST_SYSTEM_DIALOG_GET_FIELDS_FAILED_MESSAGE),
          ke);
    }
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener = (tableItem, v) -> true;
        getFieldsFromPreviousAvro(
            r, wFields, 1, new int[] {1}, new int[] {2}, new int[] {3}, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, CONST_SYSTEM_DIALOG_GET_FIELDS_FAILED_MESSAGE),
          ke);
    }
  }

  public void updateTypes(
      IRowMeta row, TableView tableView, int nameColumn, int avroNameColumn, int[] typeColumn) {
    boolean createSchemaFile = wCreateSchemaFile.getSelection();
    if (validSchema || createSchemaFile) {
      Table table = tableView.table;

      boolean typesPopulated = false;
      for (int i = 0; i < table.getItemCount(); i++) {
        TableItem tableItem = table.getItem(i);

        if (!typesPopulated) {
          for (int j : typeColumn) {
            if (!Utils.isEmpty(tableItem.getText(j))) {
              typesPopulated = true;
              break;
            }
          }
          if (typesPopulated) {
            break;
          }
        }
      }

      int choice = 0;

      if (typesPopulated) {
        // Ask what we should sdo with the existing data
        MessageDialogWithToggle md =
            new MessageDialogWithToggle(
                tableView.getShell(),
                BaseMessages.getString(PKG, "AvroOutputDialog.GetTypesChoice.Title"),
                BaseMessages.getString(PKG, "AvroOutputDialog.GetTypesChoice.Message"),
                SWT.ICON_WARNING,
                new String[] {
                  BaseMessages.getString(PKG, "AvroOutputDialog.GetTypesChoice.AddNew"),
                  BaseMessages.getString(PKG, "AvroOutputDialog.GetTypesChoice.ReplaceAll"),
                  BaseMessages.getString(PKG, "AvroOutputDialog.GetTypesChoice.Cancel")
                },
                "",
                true);
        int idx = md.open();
        choice = idx & 0xFF;
      }

      if (choice == 2 || choice == 255) {
        return; // nothing to do
      }

      for (int i = 0; i < table.getItemCount(); i++) {
        TableItem tableItem = table.getItem(i);
        String avroName =
            !(tableItem.getText(2).isEmpty()) ? tableItem.getText(2) : tableItem.getText(1);
        String streamName = tableItem.getText(1);
        logBasic("Stream name is " + streamName);
        if (!(avroName.isEmpty()) && !(createSchemaFile) && avroSchema != null) {
          Schema fieldSchema = getFieldSchema(avroName);
          if (fieldSchema != null) {
            String[] types = AvroOutputField.mapAvroType(fieldSchema, fieldSchema.getType());
            if (types.length > 0) {
              for (int j : typeColumn) {
                if (choice == 0 && !Utils.isEmpty(tableItem.getText(j))) {
                  // do nothing
                } else {
                  tableItem.setText(j, types[0]);
                }
              }
            } else if (choice == 1) {
              for (int j : typeColumn) {
                tableItem.setText(j, "");
              }
            }
          } else if (choice == 1) {
            for (int j : typeColumn) {
              tableItem.setText(j, "");
            }
          }
        } else if (createSchemaFile && !(streamName.isEmpty())) { // create schema file
          logBasic("Create File");
          IValueMeta v = row.searchValueMeta(streamName);
          logBasic(v != null ? v.getName() : "Value Meta is null");
          String avroType = "";
          if (v != null) {
            avroType =
                AvroOutputField.getAvroTypeDesc(AvroOutputField.getDefaultAvroType(v.getType()));
          }
          for (int aTypeColumn : typeColumn) {
            if (choice == 1 || (choice == 0 && (tableItem.getText(aTypeColumn).isEmpty()))) {
              tableItem.setText(aTypeColumn, avroType);
            }
          }
        }
      }
    } else {
      MessageDialogWithToggle md =
          new MessageDialogWithToggle(
              tableView.getShell(),
              BaseMessages.getString(PKG, "AvroOutputDialog.GetTypesChoice.BadSchema.Title"),
              BaseMessages.getString(PKG, "AvroOutputDialog.GetTypesChoice.BadSchema.Message"),
              SWT.ICON_WARNING,
              new String[] {BaseMessages.getString(PKG, "System.Button.OK")},
              "",
              true);
      int idx = md.open();
    }
  }

  public void getFieldsFromPreviousAvro(
      IRowMeta row,
      TableView tableView,
      int keyColumn,
      int[] nameColumn,
      int[] avroNameColumn,
      int[] avroTypeColumn,
      ITableItemInsertListener listener) {
    if (row == null || row.isEmpty()) {
      return; // nothing to do
    }

    Table table = tableView.table;

    // get a list of all the non-empty keys (names)
    //
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < table.getItemCount(); i++) {
      TableItem tableItem = table.getItem(i);
      String key = tableItem.getText(keyColumn);
      if (!Utils.isEmpty(key) && keys.indexOf(key) < 0) {
        keys.add(key);
      }
    }

    int choice = 0;

    if (!keys.isEmpty()) {
      // Ask what we should do with the existing data in the Transform.
      //
      MessageDialogWithToggle md =
          new MessageDialogWithToggle(
              tableView.getShell(),
              BaseMessages.getString(
                  PKG, "BaseTransformDialog.GetFieldsChoice.Title"), // "Warning!"
              BaseMessages.getString(
                  PKG,
                  "BaseTransformDialog.GetFieldsChoice.Message",
                  "" + keys.size(),
                  "" + row.size()),
              SWT.ICON_WARNING,
              new String[] {
                BaseMessages.getString(PKG, "BaseTransformDialog.AddNew"),
                BaseMessages.getString(PKG, "BaseTransformDialog.Add"),
                BaseMessages.getString(PKG, "BaseTransformDialog.ClearAndAdd"),
                BaseMessages.getString(PKG, "BaseTransformDialog.Cancel"),
              },
              "Y",
              true);
      int idx = md.open();
      choice = idx & 0xFF;
    }

    if (choice == 3 || choice == 255) {
      return; // Cancel clicked
    }

    if (choice == 2) {
      tableView.clearAll(false);
    }

    for (int i = 0; i < row.size(); i++) {
      IValueMeta v = row.getValueMeta(i);

      boolean add = true;

      // hang on, see if it's not yet in the table view
      if (choice == 0 && keys.contains(v.getName())) {
        add = false;
      }

      if (add) {
        TableItem tableItem = new TableItem(table, SWT.NONE);

        for (int aNameColumn : nameColumn) {
          tableItem.setText(aNameColumn, Const.NVL(v.getName(), ""));
        }
        if (validSchema && !wCreateSchemaFile.getSelection()) {
          for (int anAvroNameColumn : avroNameColumn) {
            for (String avroFieldName : avroFieldNames) {
              String fieldName = avroFieldName;
              String matchName = fieldName;
              if (fieldName != null && fieldName.startsWith("$.")) {
                matchName = fieldName.substring(2);
              }
              if (matchName.equalsIgnoreCase(Const.NVL(v.getName(), ""))) {
                if (!Utils.isEmpty(fieldName)) {
                  fieldName = "$." + matchName;
                }
                tableItem.setText(anAvroNameColumn, Const.NVL(fieldName, ""));
                try {

                  Schema fieldSchema = getFieldSchema(fieldName);
                  String[] avroType =
                      AvroOutputField.mapAvroType(fieldSchema, fieldSchema.getType());
                  if (avroType.length > 0) {
                    for (int anAvroTypeColumn : avroTypeColumn) {
                      tableItem.setText(anAvroTypeColumn, avroType[0]);
                    }
                  }
                } catch (Exception ex) {
                  // ignore exception
                }
                break;
              }
            }
          }
        } else if (wCreateSchemaFile.getSelection()) {
          for (int aNameColumn : nameColumn) {
            tableItem.setText(aNameColumn, Const.NVL(v.getName(), ""));
          }

          String avroName = v.getName();
          if (!Utils.isEmpty(avroName)) {
            avroName = "$." + avroName;
          }
          for (int anAvroNameColumn : avroNameColumn) {
            tableItem.setText(anAvroNameColumn, Const.NVL(avroName, ""));
          }

          String avroType =
              AvroOutputField.getAvroTypeDesc(AvroOutputField.getDefaultAvroType(v.getType()));
          for (int anAvroTypeColumn : avroTypeColumn) {
            tableItem.setText(anAvroTypeColumn, Const.NVL(avroType, ""));
          }
        }

        if (listener != null && !listener.tableItemInserted(tableItem, v)) {
          tableItem.dispose(); // remove it again
        }
      }
    }
    tableView.removeEmptyRows();
    tableView.setRowNums();
    tableView.optWidth(true);
  }
}
