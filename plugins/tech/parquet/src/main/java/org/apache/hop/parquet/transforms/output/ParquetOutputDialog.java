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

package org.apache.hop.parquet.transforms.output;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class ParquetOutputDialog extends BaseTransformDialog implements ITransformDialog {

  public static final Class<?> PKG = ParquetOutputMeta.class;

  protected ParquetOutputMeta input;

  private TextVar wFilenameBase;
  private TextVar wFilenameExtension;
  private Button wFilenameIncludeDate;
  private Button wFilenameIncludeTime;
  private Button wFilenameIncludeDateTime;
  private Label wlFilenameDateTimeFormat;
  private TextVar wFilenameDateTimeFormat;
  private Button wFilenameIncludeCopyNr;
  private Button wFilenameIncludeSplitNr;

  private Button wFilenameInField;
  private Label wlFilenameField;
  private ComboVar wFilenameField;
  private Label wlFilenameSplitSize;
  private TextVar wFilenameSplitSize;
  private Button wFilenameCreateFolders;
  private Label wlAddToResult;
  private Button wAddToResult;
  private Combo wCompressionCodec;
  private Combo wVersion;
  private TextVar wRowGroupSize;
  private TextVar wDataPageSize;
  private TextVar wDictionaryPageSize;
  private TableView wFields;

  private String returnValue;
  private Label wlFilenameBase;
  private Label wlFilenameIncludeDateTime;
  private Label wlFilenameIncludeTime;
  private Label wlFilenameIncludeDate;
  private Label wlFilenameIncludeSplitNr;
  private Label wlFilenameIncludeCopyNr;
  private boolean gotPreviousFields = false;

  public ParquetOutputDialog(
      Shell parent,
      IVariables variables,
      Object in,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, transformName);
    input = (ParquetOutputMeta) in;
  }

  @Override
  public String open() {

    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ParquetOutput.Name"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Some buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.addListener(SWT.Selection, e -> getFields());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wGet, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addGeneralTab(wTabFolder, middle, margin);
    addOptionsTab(wTabFolder, middle, margin);
    addFieldsTab(wTabFolder, middle, margin);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    toggleUponFilenameInFieldSelection();
    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return returnValue;
  }

  private void addGeneralTab(CTabFolder wTabFolder, int middle, int margin) {

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.GeneralTab.Title"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    wlFilenameBase = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameBase.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameBase.Label"));
    PropsUi.setLook(wlFilenameBase);
    FormData fdlFilenameBase = new FormData();
    fdlFilenameBase.left = new FormAttachment(0, 0);
    fdlFilenameBase.right = new FormAttachment(middle, -margin);
    fdlFilenameBase.top = new FormAttachment(0, 0);
    wlFilenameBase.setLayoutData(fdlFilenameBase);
    wFilenameBase = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilenameBase);
    FormData fdFilenameBase = new FormData();
    fdFilenameBase.left = new FormAttachment(middle, 0);
    fdFilenameBase.top = new FormAttachment(wlFilenameBase, 0, SWT.CENTER);
    fdFilenameBase.right = new FormAttachment(100, 0);
    wFilenameBase.setLayoutData(fdFilenameBase);
    Control lastControl = wFilenameBase;

    Label wlFilenameExtension = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameExtension.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameExtension.Label"));
    PropsUi.setLook(wlFilenameExtension);
    FormData fdlFilenameExtension = new FormData();
    fdlFilenameExtension.left = new FormAttachment(0, 0);
    fdlFilenameExtension.right = new FormAttachment(middle, -margin);
    fdlFilenameExtension.top = new FormAttachment(lastControl, margin);
    wlFilenameExtension.setLayoutData(fdlFilenameExtension);
    wFilenameExtension = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilenameExtension);
    FormData fdFilenameExtension = new FormData();
    fdFilenameExtension.left = new FormAttachment(middle, 0);
    fdFilenameExtension.top = new FormAttachment(wlFilenameExtension, 0, SWT.CENTER);
    fdFilenameExtension.right = new FormAttachment(100, 0);
    wFilenameExtension.setLayoutData(fdFilenameExtension);
    lastControl = wFilenameExtension;

    Label wlFilenameCreateFolders = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameCreateFolders.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameCreateFolders.Label"));
    PropsUi.setLook(wlFilenameCreateFolders);
    FormData fdlFilenameCreateFolders = new FormData();
    fdlFilenameCreateFolders.left = new FormAttachment(0, 0);
    fdlFilenameCreateFolders.right = new FormAttachment(middle, -margin);
    fdlFilenameCreateFolders.top = new FormAttachment(lastControl, margin);
    wlFilenameCreateFolders.setLayoutData(fdlFilenameCreateFolders);
    wFilenameCreateFolders = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wFilenameCreateFolders);
    FormData fdFilenameCreateFolders = new FormData();
    fdFilenameCreateFolders.left = new FormAttachment(middle, 0);
    fdFilenameCreateFolders.top = new FormAttachment(wlFilenameCreateFolders, 0, SWT.CENTER);
    fdFilenameCreateFolders.right = new FormAttachment(100, 0);
    wFilenameCreateFolders.setLayoutData(fdFilenameCreateFolders);

    lastControl = wFilenameCreateFolders;

    Label wlFileNameInField = new Label(wGeneralComp, SWT.RIGHT);
    wlFileNameInField.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameInField.Label"));
    PropsUi.setLook(wlFileNameInField);
    FormData fdlFileNameInField = new FormData();
    fdlFileNameInField.left = new FormAttachment(0, 0);
    fdlFileNameInField.top = new FormAttachment(lastControl, margin);
    fdlFileNameInField.right = new FormAttachment(middle, -margin);
    wlFileNameInField.setLayoutData(fdlFileNameInField);
    wFilenameInField = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wFilenameInField);
    FormData fdFileNameInField = new FormData();
    fdFileNameInField.left = new FormAttachment(middle, 0);
    fdFileNameInField.top = new FormAttachment(wlFileNameInField, 0, SWT.CENTER);
    fdFileNameInField.right = new FormAttachment(100, 0);
    wFilenameInField.setLayoutData(fdFileNameInField);
    wFilenameInField.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            toggleUponFilenameInFieldSelection();
          }
        });

    lastControl = wFilenameInField;

    ModifyListener lsMod = e -> input.setChanged();

    // FileNameField Line
    wlFilenameField = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameField.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameField.Label"));
    PropsUi.setLook(wlFilenameField);
    FormData fdlFileNameField = new FormData();
    fdlFileNameField.left = new FormAttachment(0, 0);
    fdlFileNameField.right = new FormAttachment(middle, -margin);
    fdlFileNameField.top = new FormAttachment(lastControl, margin);
    wlFilenameField.setLayoutData(fdlFileNameField);

    wFilenameField = new ComboVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilenameField);
    wFilenameField.addModifyListener(lsMod);
    FormData fdFileNameField = new FormData();
    fdFileNameField.left = new FormAttachment(middle, 0);
    fdFileNameField.top = new FormAttachment(lastControl, margin);
    fdFileNameField.right = new FormAttachment(100, 0);
    wFilenameField.setLayoutData(fdFileNameField);
    wFilenameField.setEnabled(false);
    wFilenameField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);

            getPrevFields();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    lastControl = wFilenameField;

    wlFilenameIncludeDate = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameIncludeDate.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameIncludeDate.Label"));
    PropsUi.setLook(wlFilenameIncludeDate);
    FormData fdlFilenameIncludeDate = new FormData();
    fdlFilenameIncludeDate.left = new FormAttachment(0, 0);
    fdlFilenameIncludeDate.right = new FormAttachment(middle, -margin);
    fdlFilenameIncludeDate.top = new FormAttachment(lastControl, margin);
    wlFilenameIncludeDate.setLayoutData(fdlFilenameIncludeDate);
    wFilenameIncludeDate = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wFilenameIncludeDate);
    FormData fdFilenameIncludeDate = new FormData();
    fdFilenameIncludeDate.left = new FormAttachment(middle, 0);
    fdFilenameIncludeDate.top = new FormAttachment(wlFilenameIncludeDate, 0, SWT.CENTER);
    fdFilenameIncludeDate.right = new FormAttachment(100, 0);
    wFilenameIncludeDate.setLayoutData(fdFilenameIncludeDate);
    lastControl = wlFilenameIncludeDate;

    wlFilenameIncludeTime = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameIncludeTime.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameIncludeTime.Label"));
    PropsUi.setLook(wlFilenameIncludeTime);
    FormData fdlFilenameIncludeTime = new FormData();
    fdlFilenameIncludeTime.left = new FormAttachment(0, 0);
    fdlFilenameIncludeTime.right = new FormAttachment(middle, -margin);
    fdlFilenameIncludeTime.top = new FormAttachment(lastControl, margin);
    wlFilenameIncludeTime.setLayoutData(fdlFilenameIncludeTime);
    wFilenameIncludeTime = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wFilenameIncludeTime);
    FormData fdFilenameIncludeTime = new FormData();
    fdFilenameIncludeTime.left = new FormAttachment(middle, 0);
    fdFilenameIncludeTime.top = new FormAttachment(wlFilenameIncludeTime, 0, SWT.CENTER);
    fdFilenameIncludeTime.right = new FormAttachment(100, 0);
    wFilenameIncludeTime.setLayoutData(fdFilenameIncludeTime);
    lastControl = wlFilenameIncludeTime;

    wlFilenameIncludeDateTime = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameIncludeDateTime.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameIncludeDateTime.Label"));
    PropsUi.setLook(wlFilenameIncludeDateTime);
    FormData fdlFilenameIncludeDateTime = new FormData();
    fdlFilenameIncludeDateTime.left = new FormAttachment(0, 0);
    fdlFilenameIncludeDateTime.right = new FormAttachment(middle, -margin);
    fdlFilenameIncludeDateTime.top = new FormAttachment(lastControl, margin);
    wlFilenameIncludeDateTime.setLayoutData(fdlFilenameIncludeDateTime);
    wFilenameIncludeDateTime = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wFilenameIncludeDateTime);
    FormData fdFilenameIncludeDateTime = new FormData();
    fdFilenameIncludeDateTime.left = new FormAttachment(middle, 0);
    fdFilenameIncludeDateTime.top = new FormAttachment(wlFilenameIncludeDateTime, 0, SWT.CENTER);
    fdFilenameIncludeDateTime.right = new FormAttachment(100, 0);
    wFilenameIncludeDateTime.setLayoutData(fdFilenameIncludeDateTime);
    wFilenameIncludeDateTime.addListener(SWT.Selection, e -> toggleUponIncludeDateTimeSelection());
    lastControl = wlFilenameIncludeDateTime;

    wlFilenameDateTimeFormat = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameDateTimeFormat.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameDateTimeFormat.Label"));
    PropsUi.setLook(wlFilenameDateTimeFormat);
    FormData fdlFilenameDateTimeFormat = new FormData();
    fdlFilenameDateTimeFormat.left = new FormAttachment(0, 0);
    fdlFilenameDateTimeFormat.right = new FormAttachment(middle, -margin);
    fdlFilenameDateTimeFormat.top = new FormAttachment(lastControl, margin);
    wlFilenameDateTimeFormat.setLayoutData(fdlFilenameDateTimeFormat);
    wFilenameDateTimeFormat =
        new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilenameDateTimeFormat);
    FormData fdFilenameDateTimeFormat = new FormData();
    fdFilenameDateTimeFormat.left = new FormAttachment(middle, 0);
    fdFilenameDateTimeFormat.top = new FormAttachment(wlFilenameDateTimeFormat, 0, SWT.CENTER);
    fdFilenameDateTimeFormat.right = new FormAttachment(100, 0);
    wFilenameDateTimeFormat.setLayoutData(fdFilenameDateTimeFormat);
    lastControl = wFilenameDateTimeFormat;

    wlFilenameIncludeCopyNr = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameIncludeCopyNr.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameIncludeCopyNr.Label"));
    PropsUi.setLook(wlFilenameIncludeCopyNr);
    FormData fdlFilenameIncludeCopyNr = new FormData();
    fdlFilenameIncludeCopyNr.left = new FormAttachment(0, 0);
    fdlFilenameIncludeCopyNr.right = new FormAttachment(middle, -margin);
    fdlFilenameIncludeCopyNr.top = new FormAttachment(lastControl, margin);
    wlFilenameIncludeCopyNr.setLayoutData(fdlFilenameIncludeCopyNr);
    wFilenameIncludeCopyNr = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wFilenameIncludeCopyNr);
    FormData fdFilenameIncludeCopyNr = new FormData();
    fdFilenameIncludeCopyNr.left = new FormAttachment(middle, 0);
    fdFilenameIncludeCopyNr.top = new FormAttachment(wlFilenameIncludeCopyNr, 0, SWT.CENTER);
    fdFilenameIncludeCopyNr.right = new FormAttachment(100, 0);
    wFilenameIncludeCopyNr.setLayoutData(fdFilenameIncludeCopyNr);
    lastControl = wlFilenameIncludeCopyNr;

    wlFilenameIncludeSplitNr = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameIncludeSplitNr.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameIncludeSplitNr.Label"));
    PropsUi.setLook(wlFilenameIncludeSplitNr);
    FormData fdlFilenameIncludeSplitNr = new FormData();
    fdlFilenameIncludeSplitNr.left = new FormAttachment(0, 0);
    fdlFilenameIncludeSplitNr.right = new FormAttachment(middle, -margin);
    fdlFilenameIncludeSplitNr.top = new FormAttachment(lastControl, margin);
    wlFilenameIncludeSplitNr.setLayoutData(fdlFilenameIncludeSplitNr);
    wFilenameIncludeSplitNr = new Button(wGeneralComp, SWT.CHECK);
    PropsUi.setLook(wFilenameIncludeSplitNr);
    FormData fdFilenameIncludeSplitNr = new FormData();
    fdFilenameIncludeSplitNr.left = new FormAttachment(middle, 0);
    fdFilenameIncludeSplitNr.top = new FormAttachment(wlFilenameIncludeSplitNr, 0, SWT.CENTER);
    fdFilenameIncludeSplitNr.right = new FormAttachment(100, 0);
    wFilenameIncludeSplitNr.setLayoutData(fdFilenameIncludeSplitNr);
    wFilenameIncludeSplitNr.addListener(SWT.Selection, e -> toggleUponIncludeDateTimeSelection());
    lastControl = wlFilenameIncludeSplitNr;

    wlFilenameSplitSize = new Label(wGeneralComp, SWT.RIGHT);
    wlFilenameSplitSize.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameSplitSize.Label"));
    PropsUi.setLook(wlFilenameSplitSize);
    FormData fdlFilenameSplitSize = new FormData();
    fdlFilenameSplitSize.left = new FormAttachment(0, 0);
    fdlFilenameSplitSize.right = new FormAttachment(middle, -margin);
    fdlFilenameSplitSize.top = new FormAttachment(lastControl, margin);
    wlFilenameSplitSize.setLayoutData(fdlFilenameSplitSize);
    wFilenameSplitSize = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilenameSplitSize);
    FormData fdFilenameSplitSize = new FormData();
    fdFilenameSplitSize.left = new FormAttachment(middle, 0);
    fdFilenameSplitSize.top = new FormAttachment(wlFilenameSplitSize, 0, SWT.CENTER);
    fdFilenameSplitSize.right = new FormAttachment(100, 0);
    wFilenameSplitSize.setLayoutData(fdFilenameSplitSize);

    lastControl = wFilenameSplitSize;

    // Add File to the result files name
    wlAddToResult = new Label(wGeneralComp, SWT.RIGHT);
    wlAddToResult.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.AddFileToResult.Label"));
    PropsUi.setLook(wlAddToResult);
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment(0, 0);
    fdlAddToResult.top = new FormAttachment(lastControl, 2 * margin);
    fdlAddToResult.right = new FormAttachment(middle, -margin);
    wlAddToResult.setLayoutData(fdlAddToResult);
    wAddToResult = new Button(wGeneralComp, SWT.CHECK);
    wAddToResult.setToolTipText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.AddFileToResult.Tooltip"));
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

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
  }

  private void getPrevFields() {
    if (!gotPreviousFields) {
      try {
        String field = wFilenameField.getText();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wFilenameField.setItems(r.getFieldNames());
        }
        if (field != null) {
          wFilenameField.setText(field);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "ParquetOutputDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "ParquetOutputDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
    }
  }

  private void toggleUponFilenameInFieldSelection() {

    wlFilenameField.setEnabled(wFilenameInField.getSelection());
    wFilenameField.setEnabled(wFilenameInField.getSelection());
    wlFilenameBase.setEnabled(!wFilenameInField.getSelection());
    wFilenameBase.setEnabled(!wFilenameInField.getSelection());
    wlFilenameDateTimeFormat.setEnabled(!wFilenameInField.getSelection());
    wFilenameDateTimeFormat.setEnabled(!wFilenameInField.getSelection());
    wlFilenameIncludeDateTime.setEnabled(!wFilenameInField.getSelection());
    wFilenameIncludeDateTime.setEnabled(!wFilenameInField.getSelection());
    wlFilenameIncludeTime.setEnabled(!wFilenameInField.getSelection());
    wFilenameIncludeTime.setEnabled(!wFilenameInField.getSelection());
    wlFilenameIncludeDate.setEnabled(!wFilenameInField.getSelection());
    wFilenameIncludeDate.setEnabled(!wFilenameInField.getSelection());
    wlFilenameIncludeSplitNr.setEnabled(!wFilenameInField.getSelection());
    wFilenameIncludeSplitNr.setEnabled(!wFilenameInField.getSelection());
    wlFilenameSplitSize.setEnabled(!wFilenameInField.getSelection());
    wFilenameSplitSize.setEnabled(!wFilenameInField.getSelection());
    wFilenameIncludeCopyNr.setEnabled(!wFilenameInField.getSelection());
    wlFilenameIncludeCopyNr.setEnabled(!wFilenameInField.getSelection());

    if (wFilenameInField.getSelection()) {
      wFilenameSplitSize.setText("");
      wFilenameIncludeSplitNr.setSelection(false);
      wFilenameIncludeCopyNr.setSelection(false);
    }
  }

  private void addOptionsTab(CTabFolder wTabFolder, int middle, int margin) {

    CTabItem wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOptionsTab.setFont(GuiResource.getInstance().getFontDefault());
    wOptionsTab.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.OptionsTab.Title"));

    Composite wOptionsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wOptionsComp);

    FormLayout optionsLayout = new FormLayout();
    optionsLayout.marginWidth = 3;
    optionsLayout.marginHeight = 3;
    wOptionsComp.setLayout(optionsLayout);

    Label wlCompressionCodec = new Label(wOptionsComp, SWT.RIGHT);
    wlCompressionCodec.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.CompressionCodec.Label"));
    PropsUi.setLook(wlCompressionCodec);
    FormData fdlCompressionCodec = new FormData();
    fdlCompressionCodec.left = new FormAttachment(0, 0);
    fdlCompressionCodec.right = new FormAttachment(middle, -margin);
    fdlCompressionCodec.top = new FormAttachment(0, margin);
    wlCompressionCodec.setLayoutData(fdlCompressionCodec);
    wCompressionCodec = new Combo(wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    for (CompressionCodecName codecName : CompressionCodecName.values()) {
      wCompressionCodec.add(codecName.name());
    }
    PropsUi.setLook(wCompressionCodec);
    FormData fdCompressionCodec = new FormData();
    fdCompressionCodec.left = new FormAttachment(middle, 0);
    fdCompressionCodec.top = new FormAttachment(wlCompressionCodec, 0, SWT.CENTER);
    fdCompressionCodec.right = new FormAttachment(100, 0);
    wCompressionCodec.setLayoutData(fdCompressionCodec);
    Control lastControl = wCompressionCodec;

    Label wlVersion = new Label(wOptionsComp, SWT.RIGHT);
    wlVersion.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.Version.Label"));
    PropsUi.setLook(wlVersion);
    FormData fdlVersion = new FormData();
    fdlVersion.left = new FormAttachment(0, 0);
    fdlVersion.right = new FormAttachment(middle, -margin);
    fdlVersion.top = new FormAttachment(lastControl, margin);
    wlVersion.setLayoutData(fdlVersion);
    wVersion = new Combo(wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    for (ParquetVersion version : ParquetVersion.values()) {
      wVersion.add(version.getDescription());
    }
    PropsUi.setLook(wVersion);
    FormData fdVersion = new FormData();
    fdVersion.left = new FormAttachment(middle, 0);
    fdVersion.top = new FormAttachment(wlVersion, 0, SWT.CENTER);
    fdVersion.right = new FormAttachment(100, 0);
    wVersion.setLayoutData(fdVersion);
    lastControl = wVersion;

    Label wlRowGroupSize = new Label(wOptionsComp, SWT.RIGHT);
    wlRowGroupSize.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.RowGroupSize.Label"));
    PropsUi.setLook(wlRowGroupSize);
    FormData fdlRowGroupSize = new FormData();
    fdlRowGroupSize.left = new FormAttachment(0, 0);
    fdlRowGroupSize.right = new FormAttachment(middle, -margin);
    fdlRowGroupSize.top = new FormAttachment(lastControl, margin);
    wlRowGroupSize.setLayoutData(fdlRowGroupSize);
    wRowGroupSize = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRowGroupSize);
    FormData fdRowGroupSize = new FormData();
    fdRowGroupSize.left = new FormAttachment(middle, 0);
    fdRowGroupSize.top = new FormAttachment(wlRowGroupSize, 0, SWT.CENTER);
    fdRowGroupSize.right = new FormAttachment(100, 0);
    wRowGroupSize.setLayoutData(fdRowGroupSize);
    lastControl = wRowGroupSize;

    Label wlDataPageSize = new Label(wOptionsComp, SWT.RIGHT);
    wlDataPageSize.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.DataPageSize.Label"));
    PropsUi.setLook(wlDataPageSize);
    FormData fdlDataPageSize = new FormData();
    fdlDataPageSize.left = new FormAttachment(0, 0);
    fdlDataPageSize.right = new FormAttachment(middle, -margin);
    fdlDataPageSize.top = new FormAttachment(lastControl, margin);
    wlDataPageSize.setLayoutData(fdlDataPageSize);
    wDataPageSize = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDataPageSize);
    FormData fdDataPageSize = new FormData();
    fdDataPageSize.left = new FormAttachment(middle, 0);
    fdDataPageSize.top = new FormAttachment(wlDataPageSize, 0, SWT.CENTER);
    fdDataPageSize.right = new FormAttachment(100, 0);
    wDataPageSize.setLayoutData(fdDataPageSize);
    lastControl = wDataPageSize;

    Label wlDictionaryPageSize = new Label(wOptionsComp, SWT.RIGHT);
    wlDictionaryPageSize.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.DictionaryPageSize.Label"));
    PropsUi.setLook(wlDictionaryPageSize);
    FormData fdlDictionaryPageSize = new FormData();
    fdlDictionaryPageSize.left = new FormAttachment(0, 0);
    fdlDictionaryPageSize.right = new FormAttachment(middle, -margin);
    fdlDictionaryPageSize.top = new FormAttachment(lastControl, margin);
    wlDictionaryPageSize.setLayoutData(fdlDictionaryPageSize);
    wDictionaryPageSize = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDictionaryPageSize);
    FormData fdDictionaryPageSize = new FormData();
    fdDictionaryPageSize.left = new FormAttachment(middle, 0);
    fdDictionaryPageSize.top = new FormAttachment(wlDictionaryPageSize, 0, SWT.CENTER);
    fdDictionaryPageSize.right = new FormAttachment(100, 0);
    wDictionaryPageSize.setLayoutData(fdDictionaryPageSize);

    FormData fdOptionsComp = new FormData();
    fdOptionsComp.left = new FormAttachment(0, 0);
    fdOptionsComp.top = new FormAttachment(0, 0);
    fdOptionsComp.right = new FormAttachment(100, 0);
    fdOptionsComp.bottom = new FormAttachment(100, 0);
    wOptionsComp.setLayoutData(fdOptionsComp);

    wOptionsComp.layout();
    wOptionsTab.setControl(wOptionsComp);
  }

  private void addFieldsTab(CTabFolder wTabFolder, int middle, int margin) {

    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.FieldsTab.Title"));

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout(fieldsLayout);

    Group wManualSchemaDefinitionGroup = new Group(wFieldsComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wManualSchemaDefinitionGroup);
    wManualSchemaDefinitionGroup.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FieldsSchema.Label"));

    FormLayout manualSchemaDefinitionLayout = new FormLayout();
    manualSchemaDefinitionLayout.marginWidth = 10;
    manualSchemaDefinitionLayout.marginHeight = 10;
    wManualSchemaDefinitionGroup.setLayout(manualSchemaDefinitionLayout);

    //    Label wlFields = new Label(wFieldsComp, SWT.LEFT);
    //    wlFields.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.Fields.Label"));
    //    PropsUi.setLook(wlFields);
    //    FormData fdlFields = new FormData();
    //    fdlFields.left = new FormAttachment(0, 0);
    //    fdlFields.right = new FormAttachment(middle, -margin);
    //    fdlFields.top = new FormAttachment(0, margin);
    //    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetOutputDialog.FieldsColumn.SourceField.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[0]),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetOutputDialog.FieldsColumn.TargetField.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    wFields =
        new TableView(
            variables,
            wManualSchemaDefinitionGroup,
            SWT.BORDER,
            columns,
            input.getFields().size(),
            false,
            null,
            props);
    PropsUi.setLook(wFields);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);

    FormData fdManualSchemaDefinitionComp = new FormData();
    fdManualSchemaDefinitionComp.left = new FormAttachment(0, 0);
    fdManualSchemaDefinitionComp.top = new FormAttachment(0, 0);
    fdManualSchemaDefinitionComp.right = new FormAttachment(100, 0);
    fdManualSchemaDefinitionComp.bottom = new FormAttachment(100, 0);
    wManualSchemaDefinitionGroup.setLayoutData(fdManualSchemaDefinitionComp);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);
  }

  private void toggleUponIncludeDateTimeSelection() {
    wlFilenameDateTimeFormat.setEnabled(wFilenameIncludeDateTime.getSelection());
    wFilenameDateTimeFormat.setEnabled(wFilenameIncludeDateTime.getSelection());

    wlFilenameSplitSize.setEnabled(wFilenameIncludeSplitNr.getSelection());
    wFilenameSplitSize.setEnabled(wFilenameIncludeSplitNr.getSelection());
  }

  private void getFields() {
    // Populate the wFields grid
    //
    try {
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformName);
      BaseTransformDialog.getFieldsFromPrevious(
          rowMeta, wFields, 2, new int[] {1, 2}, new int[0], -1, -1, true, null);
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting fields", e);
    }
  }

  private void getData() {
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
      wFields.getColumns()[0].setComboValues(fields.getFieldNames());
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting source fields", e);
    }

    wTransformName.setText(Const.NVL(transformName, ""));
    wFilenameBase.setText(Const.NVL(input.getFilenameBase(), ""));
    wFilenameExtension.setText(Const.NVL(input.getFilenameExtension(), ""));
    wFilenameIncludeDate.setSelection(input.isFilenameIncludingDate());
    wFilenameIncludeTime.setSelection(input.isFilenameIncludingTime());

    wFilenameInField.setSelection(input.isFilenameInField());
    wFilenameField.setText(Const.NVL(input.getFilenameField(), ""));
    wFilenameIncludeDateTime.setSelection(input.isFilenameIncludingDateTime());
    wFilenameDateTimeFormat.setText(Const.NVL(input.getFilenameDateTimeFormat(), ""));
    wFilenameIncludeCopyNr.setSelection(input.isFilenameIncludingCopyNr());
    wFilenameIncludeSplitNr.setSelection(input.isFilenameIncludingSplitNr());
    wFilenameSplitSize.setText(Const.NVL(input.getFileSplitSize(), ""));
    wAddToResult.setSelection(input.isAddToResultFilenames());
    wFilenameCreateFolders.setSelection(input.isFilenameCreatingParentFolders());
    wCompressionCodec.setText(input.getCompressionCodec().name());
    wVersion.setText(input.getVersion().getDescription());
    wRowGroupSize.setText(Const.NVL(input.getRowGroupSize(), ""));
    wDataPageSize.setText(Const.NVL(input.getDataPageSize(), ""));
    wDictionaryPageSize.setText(Const.NVL(input.getDictionaryPageSize(), ""));
    for (int i = 0; i < input.getFields().size(); i++) {
      ParquetField field = input.getFields().get(i);
      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(field.getSourceFieldName(), ""));
      item.setText(2, Const.NVL(field.getTargetFieldName(), ""));
    }
    wFields.optimizeTableView();
    toggleUponIncludeDateTimeSelection();
  }

  private void ok() {
    returnValue = wTransformName.getText();

    input.setFilenameBase(wFilenameBase.getText());
    input.setFilenameExtension(wFilenameExtension.getText());
    input.setFilenameIncludingDate(wFilenameIncludeDate.getSelection());
    input.setFilenameIncludingTime(wFilenameIncludeTime.getSelection());
    input.setFilenameIncludingDateTime(wFilenameIncludeDateTime.getSelection());
    input.setFilenameDateTimeFormat(wFilenameDateTimeFormat.getText());
    input.setFilenameField(wFilenameField.getText());
    input.setFilenameInField(wFilenameInField.getSelection());
    input.setAddToResultFilenames(wAddToResult.getSelection());
    input.setFilenameIncludingCopyNr(wFilenameIncludeCopyNr.getSelection());
    input.setFilenameIncludingSplitNr(wFilenameIncludeSplitNr.getSelection());
    input.setFileSplitSize(wFilenameSplitSize.getText());
    input.setFilenameCreatingParentFolders(wFilenameCreateFolders.getSelection());

    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
    try {
      codec = CompressionCodecName.valueOf(wCompressionCodec.getText());
    } catch (Exception e) {
      // Uncompressed it is.
    }
    input.setCompressionCodec(codec);

    input.setVersion(ParquetVersion.getVersionFromDescription(wVersion.getText()));
    input.setRowGroupSize(wRowGroupSize.getText());
    input.setDataPageSize(wDataPageSize.getText());
    input.setDictionaryPageSize(wDictionaryPageSize.getText());
    input.getFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      input.getFields().add(new ParquetField(item.getText(1), item.getText(2)));
    }

    dispose();
  }

  private void cancel() {
    returnValue = null;
    dispose();
  }

  @Override
  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
