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
package org.apache.hop.pipeline.transforms.vcardoutput;

import static org.apache.hop.core.util.Utils.isEmpty;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.vcard.VCardFieldMapping;
import org.apache.hop.pipeline.transforms.vcard.VCardMappingDialogUtil;
import org.apache.hop.pipeline.transforms.vcard.VCardPropertyType;
import org.apache.hop.pipeline.transforms.vcard.VCardStreamFieldMatcher;
import org.apache.hop.pipeline.transforms.vcard.VCardVersionOption;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class VCardOutputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = VCardOutputDialog.class;
  private final VCardOutputMeta input;
  private CTabFolder wTabFolder;
  private CCombo wOperationType;
  private CCombo wVcardVersion;
  private TextVar wOutputField;
  private Label wlOutputField;
  private Button wPassInputFields;
  private Label wlPassInputFields;
  private Button wAddProdId;
  private Button wAddRevision;
  private Group wFileGroup;
  private Button wFileNameInField;
  private CCombo wFileNameField;
  private Label wlFileNameField;
  private TextVar wFileName;
  private Label wlFileName;
  private Button wbBrowse;
  private TextVar wExtension;
  private Label wlExtension;
  private Button wCreateParentFolder;
  private Label wlCreateParentFolder;
  private Button wDoNotOpenNewFileInit;
  private Label wlDoNotOpenNewFileInit;
  private ComboVar wEncoding;
  private Label wlEncoding;
  private Button wAddDate;
  private Label wlAddDate;
  private Button wAddTime;
  private Label wlAddTime;
  private Button wbShowFiles;
  private Button wAddToResult;
  private Label wlAddToResult;
  private Button wAppend;
  private Label wlAppend;
  private TableView wMappings;
  private ModifyListener lsMod;
  private boolean gotEncodings;

  public VCardOutputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (VCardOutputMeta) in;
  }

  @Override
  public String open() {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    lsMod = e -> input.setChanged();

    Control lastControl = createShell(BaseMessages.getString(PKG, "VCardOutputDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancelDialog()).build();

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wFileTab.setText(BaseMessages.getString(PKG, "VCardOutputDialog.FileTab.Label"));
    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFileComp);
    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = PropsUi.getFormMargin();
    fileLayout.marginHeight = PropsUi.getFormMargin();
    wFileComp.setLayout(fileLayout);
    wFileTab.setControl(wFileComp);

    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "VCardOutputDialog.FieldsTab.Label"));
    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();
    wFieldsComp.setLayout(fieldsLayout);
    wFieldsTab.setControl(wFieldsComp);

    buildFileTab(wFileComp, middle, margin);
    buildFieldsTab(wFieldsComp, margin, props);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, margin);
    fdTabFolder.right = new FormAttachment(100, -margin);
    fdTabFolder.top = new FormAttachment(lastControl, margin);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wTabFolder.setSelection(0);

    populateStreamFieldCombos();
    getData();
    updateOperation();
    input.setChanged(changed);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancelDialog());
    return transformName;
  }

  private void buildFileTab(Composite parent, int middle, int margin) {
    Label wlOperation = new Label(parent, SWT.RIGHT);
    wlOperation.setText(BaseMessages.getString(PKG, "VCardOutputDialog.OperationType.Label"));
    PropsUi.setLook(wlOperation);
    FormData fdlOperation = new FormData();
    fdlOperation.left = new FormAttachment(0, 0);
    fdlOperation.right = new FormAttachment(middle, -margin);
    fdlOperation.top = new FormAttachment(0, 0);
    wlOperation.setLayoutData(fdlOperation);

    wOperationType = new CCombo(parent, SWT.BORDER | SWT.READ_ONLY);
    wOperationType.setItems(VCardOutputMeta.OperationType.getDescriptions());
    PropsUi.setLook(wOperationType);
    FormData fdOperation = new FormData();
    fdOperation.left = new FormAttachment(middle, 0);
    fdOperation.right = new FormAttachment(100, 0);
    fdOperation.top = new FormAttachment(wlOperation, 0, SWT.CENTER);
    wOperationType.setLayoutData(fdOperation);
    wOperationType.addModifyListener(lsMod);
    wOperationType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            updateOperation();
          }
        });

    Group wSettingsGroup = new Group(parent, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettingsGroup);
    wSettingsGroup.setText(BaseMessages.getString(PKG, "VCardOutputDialog.Group.Settings.Label"));
    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 10;
    settingsLayout.marginHeight = 10;
    wSettingsGroup.setLayout(settingsLayout);

    Label wlVersion = new Label(wSettingsGroup, SWT.RIGHT);
    wlVersion.setText(BaseMessages.getString(PKG, "VCardOutputDialog.VCardVersion.Label"));
    PropsUi.setLook(wlVersion);
    FormData fdlVersion = new FormData();
    fdlVersion.left = new FormAttachment(0, 0);
    fdlVersion.right = new FormAttachment(middle, -margin);
    fdlVersion.top = new FormAttachment(0, 0);
    wlVersion.setLayoutData(fdlVersion);

    wVcardVersion = new CCombo(wSettingsGroup, SWT.BORDER | SWT.READ_ONLY);
    wVcardVersion.setItems(VCardVersionOption.getDescriptions());
    PropsUi.setLook(wVcardVersion);
    FormData fdVersion = new FormData();
    fdVersion.left = new FormAttachment(middle, 0);
    fdVersion.right = new FormAttachment(100, 0);
    fdVersion.top = new FormAttachment(wlVersion, 0, SWT.CENTER);
    wVcardVersion.setLayoutData(fdVersion);
    wVcardVersion.addModifyListener(lsMod);

    wlOutputField = new Label(wSettingsGroup, SWT.RIGHT);
    wlOutputField.setText(BaseMessages.getString(PKG, "VCardOutputDialog.OutputField.Label"));
    PropsUi.setLook(wlOutputField);
    FormData fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment(0, 0);
    fdlOutputField.right = new FormAttachment(middle, -margin);
    fdlOutputField.top = new FormAttachment(wVcardVersion, margin);
    wlOutputField.setLayoutData(fdlOutputField);

    wOutputField = new TextVar(variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wOutputField);
    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(middle, 0);
    fdOutputField.right = new FormAttachment(100, 0);
    fdOutputField.top = new FormAttachment(wlOutputField, 0, SWT.CENTER);
    wOutputField.setLayoutData(fdOutputField);
    wOutputField.addModifyListener(lsMod);

    wlPassInputFields = new Label(wSettingsGroup, SWT.RIGHT);
    wlPassInputFields.setText(
        BaseMessages.getString(PKG, "VCardOutputDialog.PassInputFields.Label"));
    PropsUi.setLook(wlPassInputFields);
    FormData fdlPass = new FormData();
    fdlPass.left = new FormAttachment(0, 0);
    fdlPass.right = new FormAttachment(middle, -margin);
    fdlPass.top = new FormAttachment(wOutputField, margin);
    wlPassInputFields.setLayoutData(fdlPass);

    wPassInputFields = new Button(wSettingsGroup, SWT.CHECK);
    PropsUi.setLook(wPassInputFields);
    FormData fdPass = new FormData();
    fdPass.left = new FormAttachment(middle, 0);
    fdPass.top = new FormAttachment(wlPassInputFields, 0, SWT.CENTER);
    wPassInputFields.setLayoutData(fdPass);
    wPassInputFields.addListener(SWT.Selection, e -> input.setChanged());

    wAddProdId = new Button(wSettingsGroup, SWT.CHECK);
    wAddProdId.setText(BaseMessages.getString(PKG, "VCardOutputDialog.AddProdId.Label"));
    PropsUi.setLook(wAddProdId);
    FormData fdProdId = new FormData();
    fdProdId.left = new FormAttachment(middle, 0);
    fdProdId.top = new FormAttachment(wPassInputFields, margin);
    wAddProdId.setLayoutData(fdProdId);
    wAddProdId.addListener(SWT.Selection, e -> input.setChanged());

    wAddRevision = new Button(wSettingsGroup, SWT.CHECK);
    wAddRevision.setText(BaseMessages.getString(PKG, "VCardOutputDialog.AddRevision.Label"));
    PropsUi.setLook(wAddRevision);
    FormData fdRevision = new FormData();
    fdRevision.left = new FormAttachment(middle, 0);
    fdRevision.top = new FormAttachment(wAddProdId, margin);
    wAddRevision.setLayoutData(fdRevision);
    wAddRevision.addListener(SWT.Selection, e -> input.setChanged());

    FormData fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment(0, 0);
    fdSettingsGroup.top = new FormAttachment(wOperationType, margin);
    fdSettingsGroup.right = new FormAttachment(100, 0);
    wSettingsGroup.setLayoutData(fdSettingsGroup);

    wFileGroup = new Group(parent, SWT.SHADOW_NONE);
    PropsUi.setLook(wFileGroup);
    wFileGroup.setText(BaseMessages.getString(PKG, "VCardOutputDialog.Group.File.Label"));
    FormLayout fileGroupLayout = new FormLayout();
    fileGroupLayout.marginWidth = 10;
    fileGroupLayout.marginHeight = 10;
    wFileGroup.setLayout(fileGroupLayout);

    wFileNameInField = new Button(wFileGroup, SWT.CHECK);
    wFileNameInField.setText(
        BaseMessages.getString(PKG, "VCardOutputDialog.FileNameInField.Label"));
    PropsUi.setLook(wFileNameInField);
    FormData fdFileNameInField = new FormData();
    fdFileNameInField.left = new FormAttachment(middle, 0);
    fdFileNameInField.top = new FormAttachment(0, 0);
    wFileNameInField.setLayoutData(fdFileNameInField);

    wlFileNameField = new Label(wFileGroup, SWT.RIGHT);
    wlFileNameField.setText(BaseMessages.getString(PKG, "VCardOutputDialog.FileNameField.Label"));
    PropsUi.setLook(wlFileNameField);
    FormData fdlFileNameField = new FormData();
    fdlFileNameField.left = new FormAttachment(0, 0);
    fdlFileNameField.right = new FormAttachment(middle, -margin);
    fdlFileNameField.top = new FormAttachment(wFileNameInField, margin);
    wlFileNameField.setLayoutData(fdlFileNameField);

    wFileNameField = new CCombo(wFileGroup, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wFileNameField);
    FormData fdFileNameField = new FormData();
    fdFileNameField.left = new FormAttachment(middle, 0);
    fdFileNameField.right = new FormAttachment(100, 0);
    fdFileNameField.top = new FormAttachment(wlFileNameField, 0, SWT.CENTER);
    wFileNameField.setLayoutData(fdFileNameField);

    wlFileName = new Label(wFileGroup, SWT.RIGHT);
    wlFileName.setText(BaseMessages.getString(PKG, "VCardOutputDialog.FileName.Label"));
    PropsUi.setLook(wlFileName);
    FormData fdlFileName = new FormData();
    fdlFileName.left = new FormAttachment(0, 0);
    fdlFileName.right = new FormAttachment(middle, -margin);
    fdlFileName.top = new FormAttachment(wFileNameField, margin);
    wlFileName.setLayoutData(fdlFileName);

    wbBrowse = new Button(wFileGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbBrowse);
    wbBrowse.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbBrowse = new FormData();
    fdbBrowse.right = new FormAttachment(100, 0);
    fdbBrowse.top = new FormAttachment(wFileNameField, margin);
    wbBrowse.setLayoutData(fdbBrowse);
    wbBrowse.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                true,
                shell,
                wFileName,
                variables,
                new String[] {"*.vcf", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "VCardOutputDialog.FileType.VCard"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));

    wFileName = new TextVar(variables, wFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFileName);
    FormData fdFileName = new FormData();
    fdFileName.left = new FormAttachment(middle, 0);
    fdFileName.right = new FormAttachment(wbBrowse, -margin);
    fdFileName.top = new FormAttachment(wlFileName, 0, SWT.CENTER);
    wFileName.setLayoutData(fdFileName);

    wlAppend = new Label(wFileGroup, SWT.RIGHT);
    wlAppend.setText(BaseMessages.getString(PKG, "VCardOutputDialog.Append.Label"));
    PropsUi.setLook(wlAppend);
    wlAppend.setToolTipText(BaseMessages.getString(PKG, "VCardOutputDialog.Append.Tooltip"));
    FormData fdlAppend = new FormData();
    fdlAppend.left = new FormAttachment(0, 0);
    fdlAppend.right = new FormAttachment(middle, -margin);
    fdlAppend.top = new FormAttachment(wFileName, margin);
    wlAppend.setLayoutData(fdlAppend);

    wAppend = new Button(wFileGroup, SWT.CHECK);
    wAppend.setToolTipText(BaseMessages.getString(PKG, "VCardOutputDialog.Append.Tooltip"));
    PropsUi.setLook(wAppend);
    FormData fdAppend = new FormData();
    fdAppend.left = new FormAttachment(middle, 0);
    fdAppend.top = new FormAttachment(wlAppend, 0, SWT.CENTER);
    wAppend.setLayoutData(fdAppend);

    wlCreateParentFolder = new Label(wFileGroup, SWT.RIGHT);
    wlCreateParentFolder.setText(
        BaseMessages.getString(PKG, "VCardOutputDialog.CreateParentFolder.Label"));
    PropsUi.setLook(wlCreateParentFolder);
    wlCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "VCardOutputDialog.CreateParentFolder.Tooltip"));
    FormData fdlCreateParent = new FormData();
    fdlCreateParent.left = new FormAttachment(0, 0);
    fdlCreateParent.right = new FormAttachment(middle, -margin);
    fdlCreateParent.top = new FormAttachment(wAppend, margin);
    wlCreateParentFolder.setLayoutData(fdlCreateParent);

    wCreateParentFolder = new Button(wFileGroup, SWT.CHECK);
    wCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "VCardOutputDialog.CreateParentFolder.Tooltip"));
    PropsUi.setLook(wCreateParentFolder);
    FormData fdCreateParent = new FormData();
    fdCreateParent.left = new FormAttachment(middle, 0);
    fdCreateParent.top = new FormAttachment(wlCreateParentFolder, 0, SWT.CENTER);
    wCreateParentFolder.setLayoutData(fdCreateParent);

    wlDoNotOpenNewFileInit = new Label(wFileGroup, SWT.RIGHT);
    wlDoNotOpenNewFileInit.setText(
        BaseMessages.getString(PKG, "VCardOutputDialog.DoNotOpenNewFileInit.Label"));
    PropsUi.setLook(wlDoNotOpenNewFileInit);
    wlDoNotOpenNewFileInit.setToolTipText(
        BaseMessages.getString(PKG, "VCardOutputDialog.DoNotOpenNewFileInit.Tooltip"));
    FormData fdlDoNotOpenNewFileInit = new FormData();
    fdlDoNotOpenNewFileInit.left = new FormAttachment(0, 0);
    fdlDoNotOpenNewFileInit.right = new FormAttachment(middle, -margin);
    fdlDoNotOpenNewFileInit.top = new FormAttachment(wCreateParentFolder, margin);
    wlDoNotOpenNewFileInit.setLayoutData(fdlDoNotOpenNewFileInit);

    wDoNotOpenNewFileInit = new Button(wFileGroup, SWT.CHECK);
    wDoNotOpenNewFileInit.setToolTipText(
        BaseMessages.getString(PKG, "VCardOutputDialog.DoNotOpenNewFileInit.Tooltip"));
    PropsUi.setLook(wDoNotOpenNewFileInit);
    FormData fdDoNotOpenNewFileInit = new FormData();
    fdDoNotOpenNewFileInit.left = new FormAttachment(middle, 0);
    fdDoNotOpenNewFileInit.top = new FormAttachment(wlDoNotOpenNewFileInit, 0, SWT.CENTER);
    wDoNotOpenNewFileInit.setLayoutData(fdDoNotOpenNewFileInit);

    wlExtension = new Label(wFileGroup, SWT.RIGHT);
    wlExtension.setText(BaseMessages.getString(PKG, "System.Label.Extension"));
    PropsUi.setLook(wlExtension);
    FormData fdlExtension = new FormData();
    fdlExtension.left = new FormAttachment(0, 0);
    fdlExtension.right = new FormAttachment(middle, -margin);
    fdlExtension.top = new FormAttachment(wDoNotOpenNewFileInit, margin);
    wlExtension.setLayoutData(fdlExtension);

    wExtension = new TextVar(variables, wFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wExtension);
    FormData fdExtension = new FormData();
    fdExtension.left = new FormAttachment(middle, 0);
    fdExtension.right = new FormAttachment(100, 0);
    fdExtension.top = new FormAttachment(wlExtension, 0, SWT.CENTER);
    wExtension.setLayoutData(fdExtension);

    wlEncoding = new Label(wFileGroup, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "VCardOutputDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    fdlEncoding.top = new FormAttachment(wExtension, margin);
    wlEncoding.setLayoutData(fdlEncoding);

    wEncoding = new ComboVar(variables, wFileGroup, SWT.BORDER | SWT.READ_ONLY);
    wEncoding.setEditable(true);
    PropsUi.setLook(wEncoding);
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.right = new FormAttachment(100, 0);
    fdEncoding.top = new FormAttachment(wlEncoding, 0, SWT.CENTER);
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // ignore
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            setEncodings();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    wlAddDate = new Label(wFileGroup, SWT.RIGHT);
    wlAddDate.setText(BaseMessages.getString(PKG, "VCardOutputDialog.AddDate.Label"));
    PropsUi.setLook(wlAddDate);
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment(0, 0);
    fdlAddDate.right = new FormAttachment(middle, -margin);
    fdlAddDate.top = new FormAttachment(wEncoding, margin);
    wlAddDate.setLayoutData(fdlAddDate);

    wAddDate = new Button(wFileGroup, SWT.CHECK);
    PropsUi.setLook(wAddDate);
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment(middle, 0);
    fdAddDate.top = new FormAttachment(wlAddDate, 0, SWT.CENTER);
    wAddDate.setLayoutData(fdAddDate);

    wlAddTime = new Label(wFileGroup, SWT.RIGHT);
    wlAddTime.setText(BaseMessages.getString(PKG, "VCardOutputDialog.AddTime.Label"));
    PropsUi.setLook(wlAddTime);
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment(0, 0);
    fdlAddTime.right = new FormAttachment(middle, -margin);
    fdlAddTime.top = new FormAttachment(wAddDate, margin);
    wlAddTime.setLayoutData(fdlAddTime);

    wAddTime = new Button(wFileGroup, SWT.CHECK);
    PropsUi.setLook(wAddTime);
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment(middle, 0);
    fdAddTime.top = new FormAttachment(wlAddTime, 0, SWT.CENTER);
    wAddTime.setLayoutData(fdAddTime);

    wbShowFiles = new Button(wFileGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "VCardOutputDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.top = new FormAttachment(wAddTime, margin);
    wbShowFiles.setLayoutData(fdbShowFiles);
    wbShowFiles.addListener(SWT.Selection, e -> showFilenames());

    wlAddToResult = new Label(wFileGroup, SWT.RIGHT);
    wlAddToResult.setText(BaseMessages.getString(PKG, "VCardOutputDialog.AddFileToResult.Label"));
    PropsUi.setLook(wlAddToResult);
    wlAddToResult.setToolTipText(
        BaseMessages.getString(PKG, "VCardOutputDialog.AddFileToResult.Tooltip"));
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment(0, 0);
    fdlAddToResult.right = new FormAttachment(middle, -margin);
    fdlAddToResult.top = new FormAttachment(wbShowFiles, margin);
    wlAddToResult.setLayoutData(fdlAddToResult);

    wAddToResult = new Button(wFileGroup, SWT.CHECK);
    wAddToResult.setToolTipText(
        BaseMessages.getString(PKG, "VCardOutputDialog.AddFileToResult.Tooltip"));
    PropsUi.setLook(wAddToResult);
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment(middle, 0);
    fdAddToResult.top = new FormAttachment(wlAddToResult, 0, SWT.CENTER);
    wAddToResult.setLayoutData(fdAddToResult);

    FormData fdFileGroup = new FormData();
    fdFileGroup.left = new FormAttachment(0, 0);
    fdFileGroup.top = new FormAttachment(wSettingsGroup, margin);
    fdFileGroup.right = new FormAttachment(100, 0);
    wFileGroup.setLayoutData(fdFileGroup);

    SelectionAdapter changeListener =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        };

    wFileNameInField.addListener(SWT.Selection, e -> toggleFileControls());
    wFileNameInField.addListener(SWT.Selection, e -> input.setChanged());
    wFileNameField.addModifyListener(lsMod);
    wFileName.addModifyListener(lsMod);
    wExtension.addModifyListener(lsMod);
    wEncoding.addModifyListener(lsMod);
    wCreateParentFolder.addSelectionListener(changeListener);
    wDoNotOpenNewFileInit.addSelectionListener(changeListener);
    wAppend.addSelectionListener(changeListener);
    wAddDate.addSelectionListener(changeListener);
    wAddTime.addSelectionListener(changeListener);
    wAddToResult.addSelectionListener(changeListener);
  }

  private void buildFieldsTab(Composite parent, int margin, PropsUi props) {
    Label wlMappings = new Label(parent, SWT.LEFT);
    wlMappings.setText(BaseMessages.getString(PKG, "VCardOutputDialog.Mappings.Label"));
    PropsUi.setLook(wlMappings);
    FormData fdlMappings = new FormData();
    fdlMappings.left = new FormAttachment(0, 0);
    fdlMappings.top = new FormAttachment(0, 0);
    wlMappings.setLayoutData(fdlMappings);

    Button wGetFields = new Button(parent, SWT.PUSH);
    wGetFields.setText(BaseMessages.getString(PKG, "VCardOutputDialog.GetFields.Button"));
    wGetFields.addListener(SWT.Selection, e -> getFieldsFromStream());

    Button wDoMapping = new Button(parent, SWT.PUSH);
    wDoMapping.setText(BaseMessages.getString(PKG, "VCardOutputDialog.DoMapping.Button"));
    wDoMapping.addListener(SWT.Selection, e -> generateMappings());

    setButtonPositions(new Button[] {wGetFields, wDoMapping}, margin, null);

    ColumnInfo[] columns = VCardMappingDialogUtil.createOutputMappingColumns();
    wMappings =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            columns,
            Math.max(1, input.getFieldMappings().size()),
            lsMod,
            props);
    PropsUi.setLook(wMappings);
    FormData fdMappings = new FormData();
    fdMappings.left = new FormAttachment(0, 0);
    fdMappings.right = new FormAttachment(100, 0);
    fdMappings.top = new FormAttachment(wlMappings, margin);
    fdMappings.bottom = new FormAttachment(wGetFields, -margin);
    wMappings.setLayoutData(fdMappings);
  }

  private void updateOperation() {
    VCardOutputMeta.OperationType operationType =
        VCardOutputMeta.OperationType.lookupDescription(wOperationType.getText());
    boolean activeFile = operationType != VCardOutputMeta.OperationType.OUTPUT_VALUE;
    boolean activeOutputValue = operationType != VCardOutputMeta.OperationType.WRITE_TO_FILE;

    wFileGroup.setEnabled(activeFile);
    wlOutputField.setEnabled(activeOutputValue);
    wOutputField.setEnabled(activeOutputValue);
    wlPassInputFields.setEnabled(activeOutputValue);
    wPassInputFields.setEnabled(activeOutputValue);

    if (activeFile) {
      toggleFileControls();
    }
  }

  private void toggleFileControls() {
    if (!wFileGroup.isEnabled()) {
      return;
    }
    boolean inField = wFileNameInField.getSelection();
    wlFileNameField.setEnabled(inField);
    wFileNameField.setEnabled(inField);
    wlFileName.setEnabled(!inField);
    wFileName.setEnabled(!inField);
    wbBrowse.setEnabled(!inField);
    wlExtension.setEnabled(!inField);
    wExtension.setEnabled(!inField);
    wlAddDate.setEnabled(!inField);
    wAddDate.setEnabled(!inField);
    wlAddTime.setEnabled(!inField);
    wAddTime.setEnabled(!inField);
    wbShowFiles.setEnabled(!inField);
  }

  private void setEncodings() {
    if (!gotEncodings) {
      gotEncodings = true;
      wEncoding.removeAll();
      for (Charset charset : Charset.availableCharsets().values()) {
        wEncoding.add(charset.displayName());
      }
    }
  }

  private void showFilenames() {
    VCardOutputMeta probe = new VCardOutputMeta();
    saveFileSettings(probe);
    String[] files = probe.getFileSettings().getFiles(variables, true);
    if (files != null && files.length > 0) {
      EnterSelectionDialog dialog =
          new EnterSelectionDialog(
              shell,
              files,
              BaseMessages.getString(PKG, "VCardOutputDialog.SelectOutputFiles.DialogTitle"),
              BaseMessages.getString(PKG, "VCardOutputDialog.SelectOutputFiles.DialogMessage"));
      dialog.setViewOnly();
      dialog.open();
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "VCardOutputDialog.NoFilesFound.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
    }
  }

  private void saveFileSettings(VCardOutputMeta target) {
    target.setFileNameInField(wFileNameInField.getSelection());
    target.setFileNameField(wFileNameField.getText());
    target.getFileSettings().setFileName(wFileName.getText());
    target.getFileSettings().setExtension(wExtension.getText());
    target.getFileSettings().setCreateParentFolder(wCreateParentFolder.getSelection());
    target.getFileSettings().setAppend(wAppend.getSelection());
    target.getFileSettings().setDoNotOpenNewFileInit(wDoNotOpenNewFileInit.getSelection());
    target.getFileSettings().setDateInFileName(wAddDate.getSelection());
    target.getFileSettings().setTimeInFileName(wAddTime.getSelection());
    target.setEncoding(wEncoding.getText());
    target.setAddingToResult(wAddToResult.getSelection());
  }

  private void populateStreamFieldCombos() {
    try {
      IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (row != null) {
        String[] names = row.getFieldNames();
        String savedFileNameField = wFileNameField.getText();
        wFileNameField.setItems(names);
        if (!isEmpty(savedFileNameField)) {
          wFileNameField.setText(savedFileNameField);
        }
        ColumnInfo streamFieldColumn = wMappings.getColumns()[1];
        streamFieldColumn.setComboValues(names);
        wMappings.setColumnInfo(1, streamFieldColumn);
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell, BaseMessages.getString(PKG, "System.Dialog.Error.Title"), e.getMessage(), e);
    }
  }

  private void getFieldsFromStream() {
    try {
      IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (row == null || row.isEmpty()) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        mb.setMessage(BaseMessages.getString(PKG, "VCardOutputDialog.GetFields.NoInput"));
        mb.setText(BaseMessages.getString(PKG, "VCardOutputDialog.GetFields.DialogTitle"));
        mb.open();
        return;
      }
      populateStreamFieldCombos();
      List<VCardFieldMapping> mappings = VCardStreamFieldMatcher.suggestFromStream(row);
      if (mappings.isEmpty()) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        mb.setMessage(BaseMessages.getString(PKG, "VCardOutputDialog.GetFields.NoMatches"));
        mb.setText(BaseMessages.getString(PKG, "VCardOutputDialog.GetFields.DialogTitle"));
        mb.open();
        return;
      }
      wMappings.clearAll();
      VCardMappingDialogUtil.loadOutputMappings(wMappings, mappings);
      input.setChanged();
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "VCardOutputDialog.GetFields.DialogTitle"),
          BaseMessages.getString(PKG, "VCardOutputDialog.GetFields.Failed", e.getMessage()),
          e);
    }
  }

  private void generateMappings() {
    IRowMeta sourceFields;
    try {
      sourceFields = pipelineMeta.getPrevTransformFields(variables, transformName);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "VCardOutputDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "VCardOutputDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }
    if (sourceFields == null || sourceFields.isEmpty()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage(BaseMessages.getString(PKG, "VCardOutputDialog.GetFields.NoInput"));
      mb.setText(
          BaseMessages.getString(
              PKG, "VCardOutputDialog.DoMapping.UnableToFindSourceFields.Title"));
      mb.open();
      return;
    }

    populateStreamFieldCombos();
    String[] targetNames = VCardPropertyType.getDescriptions();
    Map<String, String> parameterTypesByTarget = existingParameterTypesByTarget();

    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuilder missingSourceFields = new StringBuilder();
    StringBuilder missingTargetFields = new StringBuilder();

    int nrFields = wMappings.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wMappings.getNonEmpty(i);
      String target = item.getText(1);
      String source = item.getText(2);

      int sourceIndex = sourceFields.indexOfValue(source);
      if (sourceIndex < 0) {
        missingSourceFields
            .append(Const.CR)
            .append("   ")
            .append(source)
            .append(" --> ")
            .append(target);
      }
      int targetIndex = indexOfDescription(targetNames, target);
      if (targetIndex < 0) {
        missingTargetFields
            .append(Const.CR)
            .append("   ")
            .append(source)
            .append(" --> ")
            .append(target);
      }
      if (sourceIndex < 0 || targetIndex < 0) {
        continue;
      }
      mappings.add(new SourceToTargetMapping(sourceIndex, targetIndex));
    }

    if (!missingSourceFields.isEmpty() || !missingTargetFields.isEmpty()) {
      String message = "";
      if (!missingSourceFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "VCardOutputDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (!missingTargetFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "VCardOutputDialog.DoMapping.SomeTargetFieldsNotFound",
                    missingTargetFields.toString())
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString(PKG, "VCardOutputDialog.DoMapping.SomeFieldsNotFoundContinue");
      int answer =
          BaseDialog.openMessageBox(
              shell,
              BaseMessages.getString(PKG, "VCardOutputDialog.DoMapping.SomeFieldsNotFoundTitle"),
              message,
              SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      if ((answer & SWT.YES) == 0) {
        return;
      }
    }

    EnterMappingDialog dialog =
        new EnterMappingDialog(shell, sourceFields.getFieldNames(), targetNames, mappings);
    mappings = dialog.open();
    if (mappings == null) {
      return;
    }

    wMappings.table.removeAll();
    wMappings.table.setItemCount(mappings.size());
    for (int i = 0; i < mappings.size(); i++) {
      SourceToTargetMapping mapping = mappings.get(i);
      TableItem item = wMappings.table.getItem(i);
      String targetDescription = targetNames[mapping.getTargetPosition()];
      item.setText(1, targetDescription);
      item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
      item.setText(3, parameterTypesByTarget.getOrDefault(targetDescription, ""));
    }
    wMappings.setRowNums();
    wMappings.optWidth(true);
    input.setChanged();
  }

  private Map<String, String> existingParameterTypesByTarget() {
    Map<String, String> parameterTypesByTarget = new HashMap<>();
    for (int i = 0; i < wMappings.nrNonEmpty(); i++) {
      TableItem item = wMappings.getNonEmpty(i);
      parameterTypesByTarget.put(item.getText(1), item.getText(3));
    }
    return parameterTypesByTarget;
  }

  private static int indexOfDescription(String[] descriptions, String description) {
    for (int i = 0; i < descriptions.length; i++) {
      if (descriptions[i].equals(description)) {
        return i;
      }
    }
    return -1;
  }

  private void getData() {
    wOperationType.setText(
        input.getOperationType() == null
            ? VCardOutputMeta.OperationType.BOTH.getDescription()
            : input.getOperationType().getDescription());
    wVcardVersion.setText(
        input.getVcardVersion() == null
            ? VCardVersionOption.V3_0.getDescription()
            : input.getVcardVersion().getDescription());
    wOutputField.setText(Const.NVL(input.getOutputField(), ""));
    wPassInputFields.setSelection(input.isPassInputFields());
    wAddProdId.setSelection(input.isAddProdId());
    wAddRevision.setSelection(input.isAddRevision());
    wFileNameInField.setSelection(input.isFileNameInField());
    wFileNameField.setText(Const.NVL(input.getFileNameField(), ""));
    wFileName.setText(Const.NVL(input.getFileSettings().getFileName(), ""));
    wExtension.setText(Const.NVL(input.getFileSettings().getExtension(), "vcf"));
    wCreateParentFolder.setSelection(input.getFileSettings().isCreateParentFolder());
    wAppend.setSelection(input.getFileSettings().isAppend());
    wDoNotOpenNewFileInit.setSelection(input.getFileSettings().isDoNotOpenNewFileInit());
    wAddDate.setSelection(input.getFileSettings().isDateInFileName());
    wAddTime.setSelection(input.getFileSettings().isTimeInFileName());
    wEncoding.setText(Const.NVL(input.getEncoding(), Const.UTF_8));
    wAddToResult.setSelection(input.isAddingToResult());
    wMappings.clearAll();
    VCardMappingDialogUtil.loadOutputMappings(wMappings, input.getFieldMappings());
  }

  private void ok() {
    if (isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText();
    input.setOperationType(
        VCardOutputMeta.OperationType.lookupDescription(wOperationType.getText()));
    input.setVcardVersion(VCardVersionOption.lookupDescription(wVcardVersion.getText()));
    input.setOutputField(wOutputField.getText());
    input.setPassInputFields(wPassInputFields.getSelection());
    input.setAddProdId(wAddProdId.getSelection());
    input.setAddRevision(wAddRevision.getSelection());
    saveFileSettings(input);
    input.setFieldMappings(VCardMappingDialogUtil.saveOutputMappings(wMappings));
    input.setChanged();
    dispose();
  }

  private void cancelDialog() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }
}
