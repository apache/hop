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
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class ParquetOutputDialog extends BaseTransformDialog implements ITransformDialog {

  public static final Class<?> PKG = ParquetOutputMeta.class;

  protected ParquetOutputMeta input;

  private Shell shell;
  private TextVar wFilenameBase;
  private TextVar wFilenameExtension;
  private Button wFilenameIncludeDate;
  private Button wFilenameIncludeTime;
  private Button wFilenameIncludeDateTime;
  private Label wlFilenameDateTimeFormat;
  private TextVar wFilenameDateTimeFormat;
  private Button wFilenameIncludeCopyNr;
  private Button wFilenameIncludeSplitNr;
  private Label wlFilenameSplitSize;
  private TextVar wFilenameSplitSize;
  private Button wFilenameCreateFolders;
  private Combo wCompressionCodec;
  private Combo wVersion;
  private TextVar wRowGroupSize;
  private TextVar wDataPageSize;
  private TextVar wDictionaryPageSize;
  private TableView wFields;

  private String returnValue;

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
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

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
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    Group wFileGroup = new Group(shell, SWT.SHADOW_ETCHED_IN);
    wFileGroup.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameGroup.Label"));
    wFileGroup.setLayout(new FormLayout());
    FormData fdFileGroup = new FormData();
    fdFileGroup.left = new FormAttachment(0, 0);
    fdFileGroup.right = new FormAttachment(100, 0);
    fdFileGroup.top = new FormAttachment(lastControl, margin);
    wFileGroup.setLayoutData(fdFileGroup);

    Label wlFilenameBase = new Label(wFileGroup, SWT.RIGHT);
    wlFilenameBase.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameBase.Label"));
    props.setLook(wlFilenameBase);
    FormData fdlFilenameBase = new FormData();
    fdlFilenameBase.left = new FormAttachment(0, 0);
    fdlFilenameBase.right = new FormAttachment(middle, -margin);
    fdlFilenameBase.top = new FormAttachment(0, 0);
    wlFilenameBase.setLayoutData(fdlFilenameBase);
    wFilenameBase = new TextVar(variables, wFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilenameBase);
    FormData fdFilenameBase = new FormData();
    fdFilenameBase.left = new FormAttachment(middle, 0);
    fdFilenameBase.top = new FormAttachment(wlFilenameBase, 0, SWT.CENTER);
    fdFilenameBase.right = new FormAttachment(100, 0);
    wFilenameBase.setLayoutData(fdFilenameBase);
    lastControl = wFilenameBase;

    Label wlFilenameExtension = new Label(wFileGroup, SWT.RIGHT);
    wlFilenameExtension.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameExtension.Label"));
    props.setLook(wlFilenameExtension);
    FormData fdlFilenameExtension = new FormData();
    fdlFilenameExtension.left = new FormAttachment(0, 0);
    fdlFilenameExtension.right = new FormAttachment(middle, -margin);
    fdlFilenameExtension.top = new FormAttachment(lastControl, margin);
    wlFilenameExtension.setLayoutData(fdlFilenameExtension);
    wFilenameExtension = new TextVar(variables, wFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilenameExtension);
    FormData fdFilenameExtension = new FormData();
    fdFilenameExtension.left = new FormAttachment(middle, 0);
    fdFilenameExtension.top = new FormAttachment(wlFilenameExtension, 0, SWT.CENTER);
    fdFilenameExtension.right = new FormAttachment(100, 0);
    wFilenameExtension.setLayoutData(fdFilenameExtension);
    lastControl = wFilenameExtension;

    Label wlFilenameIncludeDate = new Label(wFileGroup, SWT.RIGHT);
    wlFilenameIncludeDate.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameIncludeDate.Label"));
    props.setLook(wlFilenameIncludeDate);
    FormData fdlFilenameIncludeDate = new FormData();
    fdlFilenameIncludeDate.left = new FormAttachment(0, 0);
    fdlFilenameIncludeDate.right = new FormAttachment(middle, -margin);
    fdlFilenameIncludeDate.top = new FormAttachment(lastControl, margin);
    wlFilenameIncludeDate.setLayoutData(fdlFilenameIncludeDate);
    wFilenameIncludeDate = new Button(wFileGroup, SWT.CHECK);
    props.setLook(wFilenameIncludeDate);
    FormData fdFilenameIncludeDate = new FormData();
    fdFilenameIncludeDate.left = new FormAttachment(middle, 0);
    fdFilenameIncludeDate.top = new FormAttachment(wlFilenameIncludeDate, 0, SWT.CENTER);
    fdFilenameIncludeDate.right = new FormAttachment(100, 0);
    wFilenameIncludeDate.setLayoutData(fdFilenameIncludeDate);
    lastControl = wlFilenameIncludeDate;

    Label wlFilenameIncludeTime = new Label(wFileGroup, SWT.RIGHT);
    wlFilenameIncludeTime.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameIncludeTime.Label"));
    props.setLook(wlFilenameIncludeTime);
    FormData fdlFilenameIncludeTime = new FormData();
    fdlFilenameIncludeTime.left = new FormAttachment(0, 0);
    fdlFilenameIncludeTime.right = new FormAttachment(middle, -margin);
    fdlFilenameIncludeTime.top = new FormAttachment(lastControl, margin);
    wlFilenameIncludeTime.setLayoutData(fdlFilenameIncludeTime);
    wFilenameIncludeTime = new Button(wFileGroup, SWT.CHECK);
    props.setLook(wFilenameIncludeTime);
    FormData fdFilenameIncludeTime = new FormData();
    fdFilenameIncludeTime.left = new FormAttachment(middle, 0);
    fdFilenameIncludeTime.top = new FormAttachment(wlFilenameIncludeTime, 0, SWT.CENTER);
    fdFilenameIncludeTime.right = new FormAttachment(100, 0);
    wFilenameIncludeTime.setLayoutData(fdFilenameIncludeTime);
    lastControl = wlFilenameIncludeTime;

    Label wlFilenameIncludeDateTime = new Label(wFileGroup, SWT.RIGHT);
    wlFilenameIncludeDateTime.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameIncludeDateTime.Label"));
    props.setLook(wlFilenameIncludeDateTime);
    FormData fdlFilenameIncludeDateTime = new FormData();
    fdlFilenameIncludeDateTime.left = new FormAttachment(0, 0);
    fdlFilenameIncludeDateTime.right = new FormAttachment(middle, -margin);
    fdlFilenameIncludeDateTime.top = new FormAttachment(lastControl, margin);
    wlFilenameIncludeDateTime.setLayoutData(fdlFilenameIncludeDateTime);
    wFilenameIncludeDateTime = new Button(wFileGroup, SWT.CHECK);
    props.setLook(wFilenameIncludeDateTime);
    FormData fdFilenameIncludeDateTime = new FormData();
    fdFilenameIncludeDateTime.left = new FormAttachment(middle, 0);
    fdFilenameIncludeDateTime.top = new FormAttachment(wlFilenameIncludeDateTime, 0, SWT.CENTER);
    fdFilenameIncludeDateTime.right = new FormAttachment(100, 0);
    wFilenameIncludeDateTime.setLayoutData(fdFilenameIncludeDateTime);
    wFilenameIncludeDateTime.addListener(SWT.Selection, e -> enableFields());
    lastControl = wlFilenameIncludeDateTime;

    wlFilenameDateTimeFormat = new Label(wFileGroup, SWT.RIGHT);
    wlFilenameDateTimeFormat.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameDateTimeFormat.Label"));
    props.setLook(wlFilenameDateTimeFormat);
    FormData fdlFilenameDateTimeFormat = new FormData();
    fdlFilenameDateTimeFormat.left = new FormAttachment(0, 0);
    fdlFilenameDateTimeFormat.right = new FormAttachment(middle, -margin);
    fdlFilenameDateTimeFormat.top = new FormAttachment(lastControl, margin);
    wlFilenameDateTimeFormat.setLayoutData(fdlFilenameDateTimeFormat);
    wFilenameDateTimeFormat =
        new TextVar(variables, wFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilenameDateTimeFormat);
    FormData fdFilenameDateTimeFormat = new FormData();
    fdFilenameDateTimeFormat.left = new FormAttachment(middle, 0);
    fdFilenameDateTimeFormat.top = new FormAttachment(wlFilenameDateTimeFormat, 0, SWT.CENTER);
    fdFilenameDateTimeFormat.right = new FormAttachment(100, 0);
    wFilenameDateTimeFormat.setLayoutData(fdFilenameDateTimeFormat);
    lastControl = wFilenameDateTimeFormat;

    Label wlFilenameIncludeCopyNr = new Label(wFileGroup, SWT.RIGHT);
    wlFilenameIncludeCopyNr.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameIncludeCopyNr.Label"));
    props.setLook(wlFilenameIncludeCopyNr);
    FormData fdlFilenameIncludeCopyNr = new FormData();
    fdlFilenameIncludeCopyNr.left = new FormAttachment(0, 0);
    fdlFilenameIncludeCopyNr.right = new FormAttachment(middle, -margin);
    fdlFilenameIncludeCopyNr.top = new FormAttachment(lastControl, margin);
    wlFilenameIncludeCopyNr.setLayoutData(fdlFilenameIncludeCopyNr);
    wFilenameIncludeCopyNr = new Button(wFileGroup, SWT.CHECK);
    props.setLook(wFilenameIncludeCopyNr);
    FormData fdFilenameIncludeCopyNr = new FormData();
    fdFilenameIncludeCopyNr.left = new FormAttachment(middle, 0);
    fdFilenameIncludeCopyNr.top = new FormAttachment(wlFilenameIncludeCopyNr, 0, SWT.CENTER);
    fdFilenameIncludeCopyNr.right = new FormAttachment(100, 0);
    wFilenameIncludeCopyNr.setLayoutData(fdFilenameIncludeCopyNr);
    lastControl = wlFilenameIncludeCopyNr;

    Label wlFilenameIncludeSplitNr = new Label(wFileGroup, SWT.RIGHT);
    wlFilenameIncludeSplitNr.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameIncludeSplitNr.Label"));
    props.setLook(wlFilenameIncludeSplitNr);
    FormData fdlFilenameIncludeSplitNr = new FormData();
    fdlFilenameIncludeSplitNr.left = new FormAttachment(0, 0);
    fdlFilenameIncludeSplitNr.right = new FormAttachment(middle, -margin);
    fdlFilenameIncludeSplitNr.top = new FormAttachment(lastControl, margin);
    wlFilenameIncludeSplitNr.setLayoutData(fdlFilenameIncludeSplitNr);
    wFilenameIncludeSplitNr = new Button(wFileGroup, SWT.CHECK);
    props.setLook(wFilenameIncludeSplitNr);
    FormData fdFilenameIncludeSplitNr = new FormData();
    fdFilenameIncludeSplitNr.left = new FormAttachment(middle, 0);
    fdFilenameIncludeSplitNr.top = new FormAttachment(wlFilenameIncludeSplitNr, 0, SWT.CENTER);
    fdFilenameIncludeSplitNr.right = new FormAttachment(100, 0);
    wFilenameIncludeSplitNr.setLayoutData(fdFilenameIncludeSplitNr);
    wFilenameIncludeSplitNr.addListener(SWT.Selection, e -> enableFields());
    lastControl = wlFilenameIncludeSplitNr;

    wlFilenameSplitSize = new Label(wFileGroup, SWT.RIGHT);
    wlFilenameSplitSize.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameSplitSize.Label"));
    props.setLook(wlFilenameSplitSize);
    FormData fdlFilenameSplitSize = new FormData();
    fdlFilenameSplitSize.left = new FormAttachment(0, 0);
    fdlFilenameSplitSize.right = new FormAttachment(middle, -margin);
    fdlFilenameSplitSize.top = new FormAttachment(lastControl, margin);
    wlFilenameSplitSize.setLayoutData(fdlFilenameSplitSize);
    wFilenameSplitSize = new TextVar(variables, wFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilenameSplitSize);
    FormData fdFilenameSplitSize = new FormData();
    fdFilenameSplitSize.left = new FormAttachment(middle, 0);
    fdFilenameSplitSize.top = new FormAttachment(wlFilenameSplitSize, 0, SWT.CENTER);
    fdFilenameSplitSize.right = new FormAttachment(100, 0);
    wFilenameSplitSize.setLayoutData(fdFilenameSplitSize);
    lastControl = wFilenameSplitSize;

    Label wlFilenameCreateFolders = new Label(wFileGroup, SWT.RIGHT);
    wlFilenameCreateFolders.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.FilenameCreateFolders.Label"));
    props.setLook(wlFilenameCreateFolders);
    FormData fdlFilenameCreateFolders = new FormData();
    fdlFilenameCreateFolders.left = new FormAttachment(0, 0);
    fdlFilenameCreateFolders.right = new FormAttachment(middle, -margin);
    fdlFilenameCreateFolders.top = new FormAttachment(lastControl, margin);
    wlFilenameCreateFolders.setLayoutData(fdlFilenameCreateFolders);
    wFilenameCreateFolders = new Button(wFileGroup, SWT.CHECK);
    props.setLook(wFilenameCreateFolders);
    FormData fdFilenameCreateFolders = new FormData();
    fdFilenameCreateFolders.left = new FormAttachment(middle, 0);
    fdFilenameCreateFolders.top = new FormAttachment(wlFilenameCreateFolders, 0, SWT.CENTER);
    fdFilenameCreateFolders.right = new FormAttachment(100, 0);
    wFilenameCreateFolders.setLayoutData(fdFilenameCreateFolders);

    // End of the file group
    //
    lastControl = wFileGroup;

    Label wlCompressionCodec = new Label(shell, SWT.RIGHT);
    wlCompressionCodec.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.CompressionCodec.Label"));
    props.setLook(wlCompressionCodec);
    FormData fdlCompressionCodec = new FormData();
    fdlCompressionCodec.left = new FormAttachment(0, 0);
    fdlCompressionCodec.right = new FormAttachment(middle, -margin);
    fdlCompressionCodec.top = new FormAttachment(lastControl, margin);
    wlCompressionCodec.setLayoutData(fdlCompressionCodec);
    wCompressionCodec = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    for (CompressionCodecName codecName : CompressionCodecName.values()) {
      wCompressionCodec.add(codecName.name());
    }
    props.setLook(wCompressionCodec);
    FormData fdCompressionCodec = new FormData();
    fdCompressionCodec.left = new FormAttachment(middle, 0);
    fdCompressionCodec.top = new FormAttachment(wlCompressionCodec, 0, SWT.CENTER);
    fdCompressionCodec.right = new FormAttachment(100, 0);
    wCompressionCodec.setLayoutData(fdCompressionCodec);
    lastControl = wCompressionCodec;

    Label wlVersion = new Label(shell, SWT.RIGHT);
    wlVersion.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.Version.Label"));
    props.setLook(wlVersion);
    FormData fdlVersion = new FormData();
    fdlVersion.left = new FormAttachment(0, 0);
    fdlVersion.right = new FormAttachment(middle, -margin);
    fdlVersion.top = new FormAttachment(lastControl, margin);
    wlVersion.setLayoutData(fdlVersion);
    wVersion = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    for (ParquetVersion version : ParquetVersion.values()) {
      wVersion.add(version.getDescription());
    }
    props.setLook(wVersion);
    FormData fdVersion = new FormData();
    fdVersion.left = new FormAttachment(middle, 0);
    fdVersion.top = new FormAttachment(wlVersion, 0, SWT.CENTER);
    fdVersion.right = new FormAttachment(100, 0);
    wVersion.setLayoutData(fdVersion);
    lastControl = wVersion;

    Label wlRowGroupSize = new Label(shell, SWT.RIGHT);
    wlRowGroupSize.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.RowGroupSize.Label"));
    props.setLook(wlRowGroupSize);
    FormData fdlRowGroupSize = new FormData();
    fdlRowGroupSize.left = new FormAttachment(0, 0);
    fdlRowGroupSize.right = new FormAttachment(middle, -margin);
    fdlRowGroupSize.top = new FormAttachment(lastControl, margin);
    wlRowGroupSize.setLayoutData(fdlRowGroupSize);
    wRowGroupSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRowGroupSize);
    FormData fdRowGroupSize = new FormData();
    fdRowGroupSize.left = new FormAttachment(middle, 0);
    fdRowGroupSize.top = new FormAttachment(wlRowGroupSize, 0, SWT.CENTER);
    fdRowGroupSize.right = new FormAttachment(100, 0);
    wRowGroupSize.setLayoutData(fdRowGroupSize);
    lastControl = wRowGroupSize;

    Label wlDataPageSize = new Label(shell, SWT.RIGHT);
    wlDataPageSize.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.DataPageSize.Label"));
    props.setLook(wlDataPageSize);
    FormData fdlDataPageSize = new FormData();
    fdlDataPageSize.left = new FormAttachment(0, 0);
    fdlDataPageSize.right = new FormAttachment(middle, -margin);
    fdlDataPageSize.top = new FormAttachment(lastControl, margin);
    wlDataPageSize.setLayoutData(fdlDataPageSize);
    wDataPageSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wDataPageSize);
    FormData fdDataPageSize = new FormData();
    fdDataPageSize.left = new FormAttachment(middle, 0);
    fdDataPageSize.top = new FormAttachment(wlDataPageSize, 0, SWT.CENTER);
    fdDataPageSize.right = new FormAttachment(100, 0);
    wDataPageSize.setLayoutData(fdDataPageSize);
    lastControl = wDataPageSize;

    Label wlDictionaryPageSize = new Label(shell, SWT.RIGHT);
    wlDictionaryPageSize.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.DictionaryPageSize.Label"));
    props.setLook(wlDictionaryPageSize);
    FormData fdlDictionaryPageSize = new FormData();
    fdlDictionaryPageSize.left = new FormAttachment(0, 0);
    fdlDictionaryPageSize.right = new FormAttachment(middle, -margin);
    fdlDictionaryPageSize.top = new FormAttachment(lastControl, margin);
    wlDictionaryPageSize.setLayoutData(fdlDictionaryPageSize);
    wDictionaryPageSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wDictionaryPageSize);
    FormData fdDictionaryPageSize = new FormData();
    fdDictionaryPageSize.left = new FormAttachment(middle, 0);
    fdDictionaryPageSize.top = new FormAttachment(wlDictionaryPageSize, 0, SWT.CENTER);
    fdDictionaryPageSize.right = new FormAttachment(100, 0);
    wDictionaryPageSize.setLayoutData(fdDictionaryPageSize);
    lastControl = wDictionaryPageSize;

    Label wlFields = new Label(shell, SWT.LEFT);
    wlFields.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(lastControl, margin);
    wlFields.setLayoutData(fdlFields);

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
            variables, shell, SWT.NONE, columns, input.getFields().size(), false, null, props);
    props.setLook(wFields);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return returnValue;
  }

  private void enableFields() {
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
    wFilenameIncludeDateTime.setSelection(input.isFilenameIncludingDateTime());
    wFilenameDateTimeFormat.setText(Const.NVL(input.getFilenameDateTimeFormat(), ""));
    wFilenameIncludeCopyNr.setSelection(input.isFilenameIncludingCopyNr());
    wFilenameIncludeSplitNr.setSelection(input.isFilenameIncludingSplitNr());
    wFilenameSplitSize.setText(Const.NVL(input.getFileSplitSize(), ""));
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
    enableFields();
  }

  private void ok() {
    returnValue = wTransformName.getText();

    input.setFilenameBase(wFilenameBase.getText());
    input.setFilenameExtension(wFilenameExtension.getText());
    input.setFilenameIncludingDate(wFilenameIncludeDate.getSelection());
    input.setFilenameIncludingTime(wFilenameIncludeTime.getSelection());
    input.setFilenameIncludingDateTime(wFilenameIncludeDateTime.getSelection());
    input.setFilenameDateTimeFormat(wFilenameDateTimeFormat.getText());
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
    input.setChanged();
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
