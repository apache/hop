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

package org.apache.hop.pipeline.transforms.tika;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.*;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class TikaDialog extends BaseTransformDialog implements ITransformDialog {
  public static final int[] dateLengths = new int[] {23, 19, 14, 10, 10, 10, 10, 8, 8, 8, 8, 6, 6};
  private static final Class<?> PKG = TikaMeta.class; // for Translator
  private static final String[] YES_NO_COMBO =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  private final TikaMeta input;
  private CTabFolder wTabFolder;
  private Label wlFilename;
  private Button wbbFilename; // Browse: add file or directory
  private Button wbdFilename; // Delete
  private Button wbeFilename; // Edit
  private Button wbaFilename; // Add or change
  private TextVar wFilename;
  private Label wlFilenameList;
  private TableView wFilenameList;
  private Label wlFilemask;
  private TextVar wFilemask;
  private Label wlExcludeFilemask;
  private TextVar wExcludeFilemask;
  private Button wbShowFiles;
  private CCombo wFilenameField;
  private Button wFilenameInField;
  private Label wlAddResult;
  private Button wAddResult;
  private Button wIgnoreEmptyFile;
  private TextVar wInclFilenameField;
  private TextVar wRowNumField;
  private Text wLimit;
  private Label wlEncoding;
  private CCombo wEncoding;
  private CCombo wOutputFormat;
  private boolean gotEncodings = false;
  private boolean gotPreviousFields = false;
  private TextVar wContentFieldName;
  private TextVar wFileSizeFieldName;
  private TextVar wShortFileFieldName;
  private TextVar wPathFieldName;

  private TextVar wIsHiddenName;
  private TextVar wLastModificationTimeName;
  private TextVar wUriName;
  private TextVar wRootUriName;
  private TextVar wExtensionFieldName;
  private TextVar wMetadataFieldName;

  private int middle;
  private int margin;

  public TikaDialog(
      final Shell parent,
      IVariables variables,
      final Object baseTransformMeta,
      final PipelineMeta pipelineMeta,
      final String transformName) {
    super(parent, variables, (BaseTransformMeta) baseTransformMeta, pipelineMeta, transformName);
    input = (TikaMeta) baseTransformMeta;
  }

  public String open() {
    Shell parent = getParent();

    ClassLoader loader = input.getClass().getClassLoader();
    TikaOutput tikaOutput;
    try {
      tikaOutput = new TikaOutput(loader, LogChannel.UI, variables);
    } catch (Exception e) {
      new ErrorDialog(shell, "Tika error", "Tika error", e);
      return null;
    }

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "TikaDialog.DialogTitle"));

    middle = props.getMiddlePct();
    margin = (int) (Const.MARGIN * props.getZoomFactor());

    // Buttons at the bottom:
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "TikaDialog.Button.PreviewRows"));
    wPreview.addListener(SWT.Selection, e -> preview());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    //////////////////////////
    // START OF FILE TAB   ///
    //////////////////////////
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setText(BaseMessages.getString(PKG, "TikaDialog.File.Tab"));

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout(fileLayout);

    // ///////////////////////////////
    // START OF Output Field GROUP  //
    /////////////////////////////////

    Group wOutputField = new Group(wFileComp, SWT.SHADOW_NONE);
    props.setLook(wOutputField);
    wOutputField.setText(BaseMessages.getString(PKG, "TikaDialog.wOutputField.Label"));

    FormLayout outputfieldgroupLayout = new FormLayout();
    outputfieldgroupLayout.marginWidth = 10;
    outputfieldgroupLayout.marginHeight = 10;
    wOutputField.setLayout(outputfieldgroupLayout);

    // Is filename defined in a Field
    Label wlFilenameInField = new Label(wOutputField, SWT.RIGHT);
    wlFilenameInField.setText(BaseMessages.getString(PKG, "TikaDialog.FilenameInField.Label"));
    props.setLook(wlFilenameInField);
    FormData fdlFilenameInField = new FormData();
    fdlFilenameInField.left = new FormAttachment(0, -margin);
    fdlFilenameInField.top = new FormAttachment(0, margin);
    fdlFilenameInField.right = new FormAttachment(middle, -2 * margin);
    wlFilenameInField.setLayoutData(fdlFilenameInField);

    wFilenameInField = new Button(wOutputField, SWT.CHECK);
    props.setLook(wFilenameInField);
    wFilenameInField.setToolTipText(
        BaseMessages.getString(PKG, "TikaDialog.FilenameInField.Tooltip"));
    FormData fdFileNameInField = new FormData();
    fdFileNameInField.left = new FormAttachment(middle, -margin);
    fdFileNameInField.top = new FormAttachment(wlFilenameInField, 0, SWT.CENTER);
    wFilenameInField.setLayoutData(fdFileNameInField);
    wFilenameInField.addListener(
        SWT.Selection,
        e -> {
          enableFields();
          input.setChanged();
        });

    // If Filename defined in a Field
    Label wlFilenameField = new Label(wOutputField, SWT.RIGHT);
    wlFilenameField.setText(BaseMessages.getString(PKG, "TikaDialog.FilenameField.Label"));
    props.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, margin);
    fdlFilenameField.top = new FormAttachment(wlFilenameInField, 2 * margin);
    fdlFilenameField.right = new FormAttachment(middle, -2 * margin);
    wlFilenameField.setLayoutData(fdlFilenameField);

    wFilenameField = new CCombo(wOutputField, SWT.BORDER | SWT.READ_ONLY);
    wFilenameField.setEditable(true);
    props.setLook(wFilenameField);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, -margin);
    fdFilenameField.top = new FormAttachment(wlFilenameField, 0, SWT.CENTER);
    fdFilenameField.right = new FormAttachment(100, -margin);
    wFilenameField.setLayoutData(fdFilenameField);
    wFilenameField.addFocusListener(
        new FocusListener() {
          public void focusLost(org.eclipse.swt.events.FocusEvent e) {}

          public void focusGained(org.eclipse.swt.events.FocusEvent e) {
            setDynamicFilenameField();
          }
        });

    // End of group
    //
    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(0, margin);
    fdOutputField.top = new FormAttachment(wFilenameList, margin);
    fdOutputField.right = new FormAttachment(100, -margin);
    wOutputField.setLayoutData(fdOutputField);

    // ///////////////////////////////////////////////////////////
    // / END OF Output Field GROUP
    // ///////////////////////////////////////////////////////////

    middle = middle / 2;
    // Filename line
    wlFilename = new Label(wFileComp, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "TikaDialog.Filename.Label"));
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wOutputField, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "TikaDialog.FilenameBrowse.Button"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wOutputField, margin);
    wbbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbaFilename);
    wbaFilename.setText(BaseMessages.getString(PKG, "TikaDialog.FilenameAdd.Button"));
    wbaFilename.setToolTipText(BaseMessages.getString(PKG, "TikaDialog.FilenameAdd.Tooltip"));
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment(wbbFilename, -margin);
    fdbaFilename.top = new FormAttachment(wOutputField, margin);
    wbaFilename.setLayoutData(fdbaFilename);

    wFilename = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbaFilename, -margin);
    fdFilename.top = new FormAttachment(wOutputField, margin);
    wFilename.setLayoutData(fdFilename);

    wlFilemask = new Label(wFileComp, SWT.RIGHT);
    wlFilemask.setText(BaseMessages.getString(PKG, "TikaDialog.RegExp.Label"));
    props.setLook(wlFilemask);
    FormData fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment(0, 0);
    fdlFilemask.top = new FormAttachment(wFilename, 2 * margin);
    fdlFilemask.right = new FormAttachment(middle, -margin);
    wlFilemask.setLayoutData(fdlFilemask);
    wFilemask = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilemask);
    FormData fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment(middle, 0);
    fdFilemask.top = new FormAttachment(wFilename, 2 * margin);
    fdFilemask.right = new FormAttachment(100, 0);
    wFilemask.setLayoutData(fdFilemask);

    wlExcludeFilemask = new Label(wFileComp, SWT.RIGHT);
    wlExcludeFilemask.setText(BaseMessages.getString(PKG, "TikaDialog.ExcludeFilemask.Label"));
    props.setLook(wlExcludeFilemask);
    FormData fdlExcludeFilemask = new FormData();
    fdlExcludeFilemask.left = new FormAttachment(0, 0);
    fdlExcludeFilemask.top = new FormAttachment(wFilemask, margin);
    fdlExcludeFilemask.right = new FormAttachment(middle, -margin);
    wlExcludeFilemask.setLayoutData(fdlExcludeFilemask);
    wExcludeFilemask = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wExcludeFilemask);
    FormData fdExcludeFilemask = new FormData();
    fdExcludeFilemask.left = new FormAttachment(middle, 0);
    fdExcludeFilemask.top = new FormAttachment(wFilemask, margin);
    fdExcludeFilemask.right = new FormAttachment(wFilename, 0, SWT.RIGHT);
    wExcludeFilemask.setLayoutData(fdExcludeFilemask);

    // Filename list line
    wlFilenameList = new Label(wFileComp, SWT.RIGHT);
    wlFilenameList.setText(BaseMessages.getString(PKG, "TikaDialog.FilenameList.Label"));
    props.setLook(wlFilenameList);
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment(0, 0);
    fdlFilenameList.top = new FormAttachment(wExcludeFilemask, margin);
    fdlFilenameList.right = new FormAttachment(middle, -margin);
    wlFilenameList.setLayoutData(fdlFilenameList);

    // Buttons to the right of the screen...
    wbdFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbdFilename);
    wbdFilename.setText(BaseMessages.getString(PKG, "TikaDialog.FilenameRemove.Label"));
    wbdFilename.setToolTipText(BaseMessages.getString(PKG, "TikaDialog.FilenameRemove.Tooltip"));
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment(100, 0);
    fdbdFilename.top = new FormAttachment(wExcludeFilemask, 40);
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbeFilename);
    wbeFilename.setText(BaseMessages.getString(PKG, "TikaDialog.FilenameEdit.Label"));
    wbeFilename.setToolTipText(BaseMessages.getString(PKG, "TikaDialog.FilenameEdit.Tooltip"));
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment(100, 0);
    fdbeFilename.top = new FormAttachment(wbdFilename, margin);
    wbeFilename.setLayoutData(fdbeFilename);

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "TikaDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.bottom = new FormAttachment(100, 0);
    wbShowFiles.setLayoutData(fdbShowFiles);

    ColumnInfo[] colinfo = new ColumnInfo[5];
    colinfo[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "TikaDialog.Files.Filename.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "TikaDialog.Files.Wildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "TikaDialog.Files.ExcludeWildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    colinfo[0].setUsingVariables(true);
    colinfo[1].setUsingVariables(true);
    colinfo[1].setToolTip(BaseMessages.getString(PKG, "TikaDialog.Files.Wildcard.Tooltip"));
    colinfo[2].setUsingVariables(true);
    colinfo[2].setToolTip(BaseMessages.getString(PKG, "TikaDialog.Files.ExcludeWildcard.Tooltip"));

    colinfo[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "TikaDialog.Required.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);
    colinfo[3].setToolTip(BaseMessages.getString(PKG, "TikaDialog.Required.Tooltip"));
    colinfo[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "TikaDialog.IncludeSubDirs.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);
    colinfo[4].setToolTip(BaseMessages.getString(PKG, "TikaDialog.IncludeSubDirs.Tooltip"));

    wFilenameList =
        new TableView(
            variables,
            wFileComp,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            colinfo,
            2,
            null,
            props);
    props.setLook(wFilenameList);
    FormData fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment(middle, 0);
    fdFilenameList.right = new FormAttachment(wbdFilename, -margin);
    fdFilenameList.top = new FormAttachment(wExcludeFilemask, margin);
    fdFilenameList.bottom = new FormAttachment(wbShowFiles, -margin);
    wFilenameList.setLayoutData(fdFilenameList);

    FormData fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment(0, 0);
    fdFileComp.top = new FormAttachment(0, 0);
    fdFileComp.right = new FormAttachment(100, 0);
    fdFileComp.bottom = new FormAttachment(100, 0);
    wFileComp.setLayoutData(fdFileComp);

    wFileComp.layout();
    wFileTab.setControl(wFileComp);

    /////////////////////////////////////////////////////////////
    /// END OF FILE TAB
    /////////////////////////////////////////////////////////////

    middle = props.getMiddlePct();
    //////////////////////////
    // START OF CONTENT TAB///
    ///
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setText(BaseMessages.getString(PKG, "TikaDialog.Content.Tab"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    // ///////////////////////////////
    // START OF FileConf Field GROUP  //
    /////////////////////////////////

    Group wFileConf = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wFileConf);
    wFileConf.setText(BaseMessages.getString(PKG, "TikaDialog.FileConf.Label"));

    FormLayout xmlConfgroupLayout = new FormLayout();
    xmlConfgroupLayout.marginWidth = 10;
    xmlConfgroupLayout.marginHeight = 10;
    wFileConf.setLayout(xmlConfgroupLayout);

    wlEncoding = new Label(wFileConf, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "TikaDialog.Encoding.Label"));
    props.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.top = new FormAttachment(0, margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new CCombo(wFileConf, SWT.BORDER | SWT.READ_ONLY);
    wEncoding.setEditable(true);
    props.setLook(wEncoding);
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.top = new FormAttachment(0, margin);
    fdEncoding.right = new FormAttachment(100, 0);
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener(
        new FocusListener() {
          public void focusLost(org.eclipse.swt.events.FocusEvent e) {}

          public void focusGained(org.eclipse.swt.events.FocusEvent e) {
            setEncodings();
          }
        });

    // Ignore Empty File
    Label wlIgnoreEmptyFile = new Label(wFileConf, SWT.RIGHT);
    wlIgnoreEmptyFile.setText(BaseMessages.getString(PKG, "TikaDialog.IgnoreEmptyFile.Label"));
    props.setLook(wlIgnoreEmptyFile);
    FormData fdlIgnoreEmptyFile = new FormData();
    fdlIgnoreEmptyFile.left = new FormAttachment(0, 0);
    fdlIgnoreEmptyFile.top = new FormAttachment(wEncoding, margin);
    fdlIgnoreEmptyFile.right = new FormAttachment(middle, -margin);
    wlIgnoreEmptyFile.setLayoutData(fdlIgnoreEmptyFile);
    wIgnoreEmptyFile = new Button(wFileConf, SWT.CHECK);
    props.setLook(wIgnoreEmptyFile);
    wIgnoreEmptyFile.setToolTipText(
        BaseMessages.getString(PKG, "TikaDialog.IgnoreEmptyFile.Tooltip"));
    FormData fdIgnoreEmptyFile = new FormData();
    fdIgnoreEmptyFile.left = new FormAttachment(middle, 0);
    fdIgnoreEmptyFile.top = new FormAttachment(wlIgnoreEmptyFile, 0, SWT.CENTER);
    wIgnoreEmptyFile.setLayoutData(fdIgnoreEmptyFile);

    // preview limit
    Label wlLimit = new Label(wFileConf, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "TikaDialog.Limit.Label"));
    props.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.top = new FormAttachment(wlIgnoreEmptyFile, 2 * margin);
    fdlLimit.right = new FormAttachment(middle, -margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wFileConf, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLimit);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.top = new FormAttachment(wlLimit, 0, SWT.CENTER);
    fdLimit.right = new FormAttachment(100, 0);
    wLimit.setLayoutData(fdLimit);

    Label wlOutputFormat = new Label(wFileConf, SWT.RIGHT);
    wlOutputFormat.setText(BaseMessages.getString(PKG, "TikaDialog.OutputFormat.Label"));
    props.setLook(wlOutputFormat);
    FormData fdlOutputFormat = new FormData();
    fdlOutputFormat.left = new FormAttachment(0, 0);
    fdlOutputFormat.top = new FormAttachment(wLimit, margin);
    fdlOutputFormat.right = new FormAttachment(middle, -margin);
    wlOutputFormat.setLayoutData(fdlOutputFormat);
    wOutputFormat = new CCombo(wFileConf, SWT.BORDER | SWT.READ_ONLY);
    wOutputFormat.setText(BaseMessages.getString(PKG, "TikaDialog.OutputFormat.Label"));
    props.setLook(wOutputFormat);

    wOutputFormat.setItems(tikaOutput.getFileOutputTypeCodes().keySet().toArray(new String[] {}));
    FormData fdOutputFormat = new FormData();
    fdOutputFormat.left = new FormAttachment(middle, 0);
    fdOutputFormat.top = new FormAttachment(wLimit, margin);
    fdOutputFormat.right = new FormAttachment(100, 0);
    wOutputFormat.setLayoutData(fdOutputFormat);

    FormData fdFileConf = new FormData();
    fdFileConf.left = new FormAttachment(0, margin);
    fdFileConf.top = new FormAttachment(0, margin);
    fdFileConf.right = new FormAttachment(100, -margin);
    wFileConf.setLayoutData(fdFileConf);

    // ///////////////////////////////
    // START OF AddFileResult GROUP  //
    /////////////////////////////////

    Group wAddFileResultGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wAddFileResultGroup);
    wAddFileResultGroup.setText(BaseMessages.getString(PKG, "TikaDialog.wAddFileResult.Label"));

    FormLayout addFileResultGroupLayout = new FormLayout();
    addFileResultGroupLayout.marginWidth = 10;
    addFileResultGroupLayout.marginHeight = 10;
    wAddFileResultGroup.setLayout(addFileResultGroupLayout);

    wlAddResult = new Label(wAddFileResultGroup, SWT.RIGHT);
    wlAddResult.setText(BaseMessages.getString(PKG, "TikaDialog.AddResult.Label"));
    props.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment(0, 0);
    fdlAddResult.top = new FormAttachment(0, margin);
    fdlAddResult.right = new FormAttachment(middle, -margin);
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wAddFileResultGroup, SWT.CHECK);
    props.setLook(wAddResult);
    wAddResult.setToolTipText(BaseMessages.getString(PKG, "TikaDialog.AddResult.Tooltip"));
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(middle, 0);
    fdAddResult.top = new FormAttachment(wlAddResult, 0, SWT.CENTER);
    wAddResult.setLayoutData(fdAddResult);

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment(0, margin);
    fdAddFileResult.top = new FormAttachment(wFileConf, margin);
    fdAddFileResult.right = new FormAttachment(100, -margin);
    wAddFileResultGroup.setLayoutData(fdAddFileResult);

    // ///////////////////////////////////////////////////////////
    // / END OF AddFileResult GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment(0, 0);
    fdContentComp.top = new FormAttachment(0, 0);
    fdContentComp.right = new FormAttachment(100, 0);
    fdContentComp.bottom = new FormAttachment(100, 0);
    wContentComp.setLayoutData(fdContentComp);

    wContentComp.layout();
    wContentTab.setControl(wContentComp);

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    addOutputFieldsTab();

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add the file to the list of files...
    SelectionAdapter selA =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            wFilenameList.add(
                wFilename.getText(), wFilemask.getText(), wExcludeFilemask.getText(), "Y", "Y");
            wFilename.setText("");
            wFilemask.setText("");
            wExcludeFilemask.setText("");
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
            wFilenameList.optWidth(true);
          }
        };
    wbaFilename.addSelectionListener(selA);
    wFilename.addSelectionListener(selA);

    // Delete files from the list of files...
    wbdFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            int[] idx = wFilenameList.getSelectionIndices();
            wFilenameList.remove(idx);
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
          }
        });

    // Edit the selected file & remove from the list...
    wbeFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            int idx = wFilenameList.getSelectionIndex();
            if (idx >= 0) {
              String[] string = wFilenameList.getItem(idx);
              wFilename.setText(string[0]);
              wFilemask.setText(string[1]);
              wExcludeFilemask.setText(string[2]);
              wFilenameList.remove(idx);
            }
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
          }
        });

    // Show the files that are selected at this time...
    wbShowFiles.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            try {
              TikaMeta tikaMeta = new TikaMeta();
              getInfo(tikaMeta);
              FileInputList fileInputList = tikaMeta.getFiles(variables);
              String[] files = fileInputList.getFileStrings();
              if (files != null && files.length > 0) {
                EnterSelectionDialog esd =
                    new EnterSelectionDialog(
                        shell,
                        files,
                        BaseMessages.getString(
                            PKG, "TikaDialog" + ".FilesReadSelection.DialogTitle"),
                        BaseMessages.getString(PKG, "TikaDialog.FilesReadSelection.DialogMessage"));
                esd.setViewOnly();
                esd.open();
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(BaseMessages.getString(PKG, "TikaDialog.NoFileFound.DialogMessage"));
                mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
                mb.open();
              }
            } catch (Exception ex) {
              new ErrorDialog(
                  shell,
                  BaseMessages.getString(PKG, "TikaDialog.ErrorParsingData" + ".DialogTitle"),
                  BaseMessages.getString(PKG, "TikaDialog.ErrorParsingData.DialogMessage"),
                  ex);
            }
          }
        });

    // Whenever something changes, set the tooltip to the expanded version of the filename:
    wFilename.addModifyListener(e -> wFilename.setToolTipText(wFilename.getText()));

    // Listen to the Browse... button
    wbbFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            if (!StringUtils.isEmpty(wFilemask.getText())
                || !StringUtils.isEmpty(wExcludeFilemask.getText())) // A mask: a directory!
            {
              DirectoryDialog dialog = new DirectoryDialog(shell, SWT.OPEN);
              if (wFilename.getText() != null) {
                String fpath = variables.resolve(wFilename.getText());
                dialog.setFilterPath(fpath);
              }

              if (dialog.open() != null) {
                String str = dialog.getFilterPath();
                wFilename.setText(str);
              }
            } else {
              FileDialog dialog = new FileDialog(shell, SWT.OPEN);

              dialog.setFilterExtensions(new String[] {"*"});

              if (wFilename.getText() != null) {
                String filename = variables.resolve(wFilename.getText());
                dialog.setFileName(filename);
              }

              dialog.setFilterNames(
                  new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")});

              if (dialog.open() != null) {
                String str =
                    dialog.getFilterPath()
                        + System.getProperty("file.separator")
                        + dialog.getFileName();
                wFilename.setText(str);
              }
            }
          }
        });

    wTabFolder.setSelection(0);

    // Set the shell size, based upon previous time...
    setSize();
    getData(input);
    enableFields();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return this.transformName;
  }

  private void setDynamicFilenameField() {
    if (!gotPreviousFields) {
      try {
        String field = wFilenameField.getText();
        wFilenameField.removeAll();
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
            BaseMessages.getString(PKG, "TikaDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "TikaDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
    }
  }

  private void enableFields() {
    wlFilename.setEnabled(!wFilenameInField.getSelection());
    wbbFilename.setEnabled(!wFilenameInField.getSelection());
    wbaFilename.setEnabled(!wFilenameInField.getSelection());
    wFilename.setEnabled(!wFilenameInField.getSelection());
    wlFilemask.setEnabled(!wFilenameInField.getSelection());
    wFilemask.setEnabled(!wFilenameInField.getSelection());
    wlExcludeFilemask.setEnabled(!wFilenameInField.getSelection());
    wExcludeFilemask.setEnabled(!wFilenameInField.getSelection());
    wlFilenameList.setEnabled(!wFilenameInField.getSelection());
    wbdFilename.setEnabled(!wFilenameInField.getSelection());
    wbeFilename.setEnabled(!wFilenameInField.getSelection());
    wbShowFiles.setEnabled(!wFilenameInField.getSelection());
    wlFilenameList.setEnabled(!wFilenameInField.getSelection());
    wFilenameList.setEnabled(!wFilenameInField.getSelection());

    if (wFilenameInField.getSelection()) {
      wEncoding.setEnabled(false);
      wlEncoding.setEnabled(false);
    } else {
      wEncoding.setEnabled(true);
      wlEncoding.setEnabled(true);
    }
    wAddResult.setEnabled(!wFilenameInField.getSelection());
    wlAddResult.setEnabled(!wFilenameInField.getSelection());
    wLimit.setEnabled(!wFilenameInField.getSelection());
    wPreview.setEnabled(!wFilenameInField.getSelection());
  }

  private void setEncodings() {
    // Encoding of the text file:
    if (!gotEncodings) {
      gotEncodings = true;
      String encoding = wEncoding.getText();
      wEncoding.removeAll();
      ArrayList<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (int i = 0; i < values.size(); i++) {
        Charset charSet = (Charset) values.get(i);
        wEncoding.add(charSet.displayName());
      }

      if (!StringUtils.isEmpty(encoding)) {
        wEncoding.setText(encoding);
      }
    }
  }

  /**
   * Read the data from the TextFileInputMeta object and show it meta this dialog.
   *
   * @param meta The TextFileInputMeta object to obtain the data from.
   */
  public void getData(TikaMeta meta) {
    wFilenameList.removeAll();

    for (TikaFile file : meta.getFiles()) {
      wFilenameList.add(
          file.getName(),
          file.getMask(),
          file.getExcludeMask(),
          file.isRequired() ? "Y" : "N",
          file.isIncludingSubFolders() ? "Y" : "N");
    }

    wFilenameList.optimizeTableView();

    wAddResult.setSelection(meta.isAddingResultFile());
    ;
    wIgnoreEmptyFile.setSelection(meta.isIgnoreEmptyFile());

    wFilenameInField.setSelection(meta.isFileInField());

    wFilenameField.setText(Const.NVL(meta.getDynamicFilenameField(), ""));
    wLimit.setText("" + meta.getRowLimit());
    wEncoding.setText(Const.NVL(meta.getEncoding(), ""));
    wOutputFormat.setText((meta.getOutputFormat() != null) ? meta.getOutputFormat() : "Plain text");

    wContentFieldName.setText(Const.NVL(meta.getContentFieldName(), ""));
    wFileSizeFieldName.setText(Const.NVL(meta.getFileSizeFieldName(), ""));
    wMetadataFieldName.setText(Const.NVL(meta.getMetadataFieldName(), ""));
    wInclFilenameField.setText(Const.NVL(meta.getFilenameField(), ""));
    wRowNumField.setText(Const.NVL(meta.getRowNumberField(), ""));
    wShortFileFieldName.setText(Const.NVL(meta.getShortFileFieldName(), ""));
    wPathFieldName.setText(Const.NVL(meta.getPathFieldName(), ""));
    wIsHiddenName.setText(Const.NVL(meta.getHiddenFieldName(), ""));
    wLastModificationTimeName.setText(Const.NVL(meta.getLastModificationTimeFieldName(), ""));
    wUriName.setText(Const.NVL(meta.getUriFieldName(), ""));
    wRootUriName.setText(Const.NVL(meta.getRootUriNameFieldName(), ""));
    wExtensionFieldName.setText(Const.NVL(meta.getExtensionFieldName(), ""));

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    try {
      getInfo(input);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "TikaDialog.ErrorParsingData.DialogTitle"),
          BaseMessages.getString(PKG, "TikaDialog.ErrorParsingData.DialogMessage"),
          e);
    }
    dispose();
  }

  private void getInfo(TikaMeta in) throws HopException {
    transformName = wTransformName.getText(); // return value

    // copy info to TikaMeta class (input)
    in.setRowLimit(Const.toLong(wLimit.getText(), 0L));
    in.setEncoding(wEncoding.getText());
    in.setOutputFormat(wOutputFormat.getText());
    in.setAddingResultFile(wAddResult.getSelection());
    in.setIgnoreEmptyFile(wIgnoreEmptyFile.getSelection());

    in.setFileInField(wFilenameInField.getSelection());
    in.setDynamicFilenameField(wFilenameField.getText());

    List<TableItem> fileItems = wFilenameList.getNonEmptyItems();
    in.getFiles().clear();
    for (TableItem item : fileItems) {
      TikaFile file = new TikaFile();
      int col = 1;
      file.setName(item.getText(col++));
      file.setMask(item.getText(col++));
      file.setExcludeMask(item.getText(col++));
      file.setRequired("Y".equalsIgnoreCase(item.getText(col++)));
      file.setIncludingSubFolders("Y".equalsIgnoreCase(item.getText(col)));
      in.getFiles().add(file);
    }

    in.setContentFieldName(wContentFieldName.getText());
    in.setFileSizeFieldName(wFileSizeFieldName.getText());
    in.setMetadataFieldName(wMetadataFieldName.getText());
    in.setFilenameField(wInclFilenameField.getText());
    in.setRowNumberField(wRowNumField.getText());
    in.setShortFileFieldName(wShortFileFieldName.getText());
    in.setPathFieldName(wPathFieldName.getText());
    in.setHiddenFieldName(wIsHiddenName.getText());
    in.setLastModificationTimeFieldName(wLastModificationTimeName.getText());
    in.setUriFieldName(wUriName.getText());
    in.setRootUriNameFieldName(wRootUriName.getText());
    in.setExtensionFieldName(wExtensionFieldName.getText());
  }

  // Preview the data
  private void preview() {
    try {
      // Create the Xml input step
      TikaMeta oneMeta = new TikaMeta();
      getInfo(oneMeta);

      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
              metadataProvider, oneMeta, wTransformName.getText());

      EnterNumberDialog numberDialog =
          new EnterNumberDialog(
              shell,
              props.getDefaultPreviewSize(),
              BaseMessages.getString(PKG, "TikaDialog.NumberRows.DialogTitle"),
              BaseMessages.getString(PKG, "TikaDialog.NumberRows.DialogMessage"));

      int previewSize = numberDialog.open();
      if (previewSize > 0) {
        PipelinePreviewProgressDialog progressDialog =
            new PipelinePreviewProgressDialog(
                shell,
                variables,
                previewMeta,
                new String[] {wTransformName.getText()},
                new int[] {previewSize});
        progressDialog.open();

        if (!progressDialog.isCancelled()) {
          Pipeline pipeline = progressDialog.getPipeline();
          String loggingText = progressDialog.getLoggingText();

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
          }

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
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "TikaDialog.ErrorPreviewingData.DialogTitle"),
          BaseMessages.getString(PKG, "TikaDialog.ErrorPreviewingData.DialogMessage"),
          e);
    }
  }

  private void addOutputFieldsTab() {
    CTabItem wOutputFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOutputFieldsTab.setText(BaseMessages.getString(PKG, "TikaDialog.OutputFieldsTab.TabTitle"));

    Composite wOutputFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wOutputFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wOutputFieldsComp.setLayout(fieldsLayout);

    Control lastControl;

    {
      // ContentFieldName line
      Label wlContentFieldName = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlContentFieldName.setText(BaseMessages.getString(PKG, "TikaDialog.ContentFieldName.Label"));
      props.setLook(wlContentFieldName);
      FormData fdlContentFieldName = new FormData();
      fdlContentFieldName.left = new FormAttachment(0, 0);
      fdlContentFieldName.top = new FormAttachment(0, margin);
      fdlContentFieldName.right = new FormAttachment(middle, -margin);
      wlContentFieldName.setLayoutData(fdlContentFieldName);
      wContentFieldName =
          new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wContentFieldName);
      FormData fdContentFieldName = new FormData();
      fdContentFieldName.left = new FormAttachment(middle, 0);
      fdContentFieldName.right = new FormAttachment(100, -margin);
      fdContentFieldName.top = new FormAttachment(wlContentFieldName, 0, SWT.CENTER);
      wContentFieldName.setLayoutData(fdContentFieldName);
      lastControl = wContentFieldName;
    }

    {
      // FileSizeFieldName line
      Label wlFileSizeFieldName = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlFileSizeFieldName.setText(
          BaseMessages.getString(PKG, "TikaDialog.FileSizeFieldName.Label"));
      props.setLook(wlFileSizeFieldName);
      FormData fdlFileSizeFieldName = new FormData();
      fdlFileSizeFieldName.left = new FormAttachment(0, 0);
      fdlFileSizeFieldName.top = new FormAttachment(lastControl, margin);
      fdlFileSizeFieldName.right = new FormAttachment(middle, -margin);
      wlFileSizeFieldName.setLayoutData(fdlFileSizeFieldName);

      wFileSizeFieldName =
          new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wFileSizeFieldName);
      FormData fdFileSizeFieldName = new FormData();
      fdFileSizeFieldName.left = new FormAttachment(middle, 0);
      fdFileSizeFieldName.right = new FormAttachment(100, -margin);
      fdFileSizeFieldName.top = new FormAttachment(wlFileSizeFieldName, 0, SWT.CENTER);
      wFileSizeFieldName.setLayoutData(fdFileSizeFieldName);
      lastControl = wFileSizeFieldName;
    }

    {
      Label wlMetadataFieldName = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlMetadataFieldName.setText(
          BaseMessages.getString(PKG, "TikaDialog.MetadataFieldName.Label"));
      props.setLook(wlMetadataFieldName);
      FormData fdlMetadataFieldName = new FormData();
      fdlMetadataFieldName.left = new FormAttachment(0, 0);
      fdlMetadataFieldName.top = new FormAttachment(lastControl, margin);
      fdlMetadataFieldName.right = new FormAttachment(middle, -margin);
      wlMetadataFieldName.setLayoutData(fdlMetadataFieldName);

      wMetadataFieldName =
          new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wMetadataFieldName);
      FormData fdMetadataFieldName = new FormData();
      fdMetadataFieldName.left = new FormAttachment(middle, 0);
      fdMetadataFieldName.right = new FormAttachment(100, -margin);
      fdMetadataFieldName.top = new FormAttachment(wlMetadataFieldName, 0, SWT.CENTER);
      wMetadataFieldName.setLayoutData(fdMetadataFieldName);
      lastControl = wMetadataFieldName;
    }

    {
      Label wlInclFilenameField = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlInclFilenameField.setText(
          BaseMessages.getString(PKG, "TikaDialog.InclFilenameField.Label"));
      props.setLook(wlInclFilenameField);
      FormData fdlInclFilenameField = new FormData();
      fdlInclFilenameField.left = new FormAttachment(0, 0);
      fdlInclFilenameField.top = new FormAttachment(lastControl, margin);
      fdlInclFilenameField.right = new FormAttachment(middle, -margin);
      wlInclFilenameField.setLayoutData(fdlInclFilenameField);

      wInclFilenameField =
          new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wInclFilenameField);
      FormData fdInclFilenameField = new FormData();
      fdInclFilenameField.left = new FormAttachment(middle, 0);
      fdInclFilenameField.top = new FormAttachment(wlInclFilenameField, 0, SWT.CENTER);
      fdInclFilenameField.right = new FormAttachment(100, -margin);
      wInclFilenameField.setLayoutData(fdInclFilenameField);
      lastControl = wInclFilenameField;
    }

    {
      Label wlRowNumField = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlRowNumField.setText(BaseMessages.getString(PKG, "TikaDialog.RowNum.Label"));
      props.setLook(wlRowNumField);
      FormData fdlRowNumField = new FormData();
      fdlRowNumField.left = new FormAttachment(0, 0);
      fdlRowNumField.top = new FormAttachment(lastControl, margin);
      fdlRowNumField.right = new FormAttachment(middle, -margin);
      wlRowNumField.setLayoutData(fdlRowNumField);

      wRowNumField = new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wRowNumField);
      FormData fdRowNumField = new FormData();
      fdRowNumField.left = new FormAttachment(middle, 0);
      fdRowNumField.right = new FormAttachment(100, -margin);
      fdRowNumField.top = new FormAttachment(wlRowNumField, 0, SWT.CENTER);
      wRowNumField.setLayoutData(fdRowNumField);
      lastControl = wRowNumField;
    }

    {
      // ShortFileFieldName line
      Label wlShortFileFieldName = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlShortFileFieldName.setText(
          BaseMessages.getString(PKG, "TikaDialog.ShortFileFieldName.Label"));
      props.setLook(wlShortFileFieldName);
      FormData fdlShortFileFieldName = new FormData();
      fdlShortFileFieldName.left = new FormAttachment(0, 0);
      fdlShortFileFieldName.top = new FormAttachment(lastControl, margin);
      fdlShortFileFieldName.right = new FormAttachment(middle, -margin);
      wlShortFileFieldName.setLayoutData(fdlShortFileFieldName);

      wShortFileFieldName =
          new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wShortFileFieldName);
      FormData fdShortFileFieldName = new FormData();
      fdShortFileFieldName.left = new FormAttachment(middle, 0);
      fdShortFileFieldName.right = new FormAttachment(100, -margin);
      fdShortFileFieldName.top = new FormAttachment(wlShortFileFieldName, 0, SWT.CENTER);
      wShortFileFieldName.setLayoutData(fdShortFileFieldName);
      lastControl = wShortFileFieldName;
    }

    {
      // ExtensionFieldName line
      Label wlExtensionFieldName = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlExtensionFieldName.setText(
          BaseMessages.getString(PKG, "TikaDialog.ExtensionFieldName.Label"));
      props.setLook(wlExtensionFieldName);
      FormData fdlExtensionFieldName = new FormData();
      fdlExtensionFieldName.left = new FormAttachment(0, 0);
      fdlExtensionFieldName.top = new FormAttachment(lastControl, margin);
      fdlExtensionFieldName.right = new FormAttachment(middle, -margin);
      wlExtensionFieldName.setLayoutData(fdlExtensionFieldName);

      wExtensionFieldName =
          new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wExtensionFieldName);
      FormData fdExtensionFieldName = new FormData();
      fdExtensionFieldName.left = new FormAttachment(middle, 0);
      fdExtensionFieldName.right = new FormAttachment(100, -margin);
      fdExtensionFieldName.top = new FormAttachment(wlExtensionFieldName, 0, SWT.CENTER);
      wExtensionFieldName.setLayoutData(fdExtensionFieldName);
      lastControl = wExtensionFieldName;
    }

    {
      // PathFieldName line
      Label wlPathFieldName = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlPathFieldName.setText(BaseMessages.getString(PKG, "TikaDialog.PathFieldName.Label"));
      props.setLook(wlPathFieldName);
      FormData fdlPathFieldName = new FormData();
      fdlPathFieldName.left = new FormAttachment(0, 0);
      fdlPathFieldName.top = new FormAttachment(lastControl, margin);
      fdlPathFieldName.right = new FormAttachment(middle, -margin);
      wlPathFieldName.setLayoutData(fdlPathFieldName);

      wPathFieldName =
          new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wPathFieldName);
      FormData fdPathFieldName = new FormData();
      fdPathFieldName.left = new FormAttachment(middle, 0);
      fdPathFieldName.right = new FormAttachment(100, -margin);
      fdPathFieldName.top = new FormAttachment(wlPathFieldName, 0, SWT.CENTER);
      wPathFieldName.setLayoutData(fdPathFieldName);
      lastControl = wPathFieldName;
    }

    {
      // IsHiddenName line
      Label wlIsHiddenName = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlIsHiddenName.setText(BaseMessages.getString(PKG, "TikaDialog.IsHiddenName.Label"));
      props.setLook(wlIsHiddenName);
      FormData fdlIsHiddenName = new FormData();
      fdlIsHiddenName.left = new FormAttachment(0, 0);
      fdlIsHiddenName.top = new FormAttachment(lastControl, margin);
      fdlIsHiddenName.right = new FormAttachment(middle, -margin);
      wlIsHiddenName.setLayoutData(fdlIsHiddenName);

      wIsHiddenName = new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wIsHiddenName);
      FormData fdIsHiddenName = new FormData();
      fdIsHiddenName.left = new FormAttachment(middle, 0);
      fdIsHiddenName.right = new FormAttachment(100, -margin);
      fdIsHiddenName.top = new FormAttachment(wlIsHiddenName, 0, SWT.CENTER);
      wIsHiddenName.setLayoutData(fdIsHiddenName);
      lastControl = wIsHiddenName;
    }

    {
      // LastModificationTimeName line
      Label wlLastModificationTimeName = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlLastModificationTimeName.setText(
          BaseMessages.getString(PKG, "TikaDialog.LastModificationTimeName.Label"));
      props.setLook(wlLastModificationTimeName);
      FormData fdlLastModificationTimeName = new FormData();
      fdlLastModificationTimeName.left = new FormAttachment(0, 0);
      fdlLastModificationTimeName.top = new FormAttachment(lastControl, margin);
      fdlLastModificationTimeName.right = new FormAttachment(middle, -margin);
      wlLastModificationTimeName.setLayoutData(fdlLastModificationTimeName);

      wLastModificationTimeName =
          new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wLastModificationTimeName);
      FormData fdLastModificationTimeName = new FormData();
      fdLastModificationTimeName.left = new FormAttachment(middle, 0);
      fdLastModificationTimeName.right = new FormAttachment(100, -margin);
      fdLastModificationTimeName.top =
          new FormAttachment(wlLastModificationTimeName, 0, SWT.CENTER);
      wLastModificationTimeName.setLayoutData(fdLastModificationTimeName);
      lastControl = wLastModificationTimeName;
    }

    {
      // UriName line
      Label wlUriName = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlUriName.setText(BaseMessages.getString(PKG, "TikaDialog.UriName.Label"));
      props.setLook(wlUriName);
      FormData fdlUriName = new FormData();
      fdlUriName.left = new FormAttachment(0, 0);
      fdlUriName.top = new FormAttachment(lastControl, margin);
      fdlUriName.right = new FormAttachment(middle, -margin);
      wlUriName.setLayoutData(fdlUriName);

      wUriName = new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wUriName);
      FormData fdUriName = new FormData();
      fdUriName.left = new FormAttachment(middle, 0);
      fdUriName.right = new FormAttachment(100, -margin);
      fdUriName.top = new FormAttachment(wlUriName, 0, SWT.CENTER);
      wUriName.setLayoutData(fdUriName);
      lastControl = wUriName;
    }

    {
      // RootUriName line
      Label wlRootUriName = new Label(wOutputFieldsComp, SWT.RIGHT);
      wlRootUriName.setText(BaseMessages.getString(PKG, "TikaDialog.RootUriName.Label"));
      props.setLook(wlRootUriName);
      FormData fdlRootUriName = new FormData();
      fdlRootUriName.left = new FormAttachment(0, 0);
      fdlRootUriName.top = new FormAttachment(lastControl, margin);
      fdlRootUriName.right = new FormAttachment(middle, -margin);
      wlRootUriName.setLayoutData(fdlRootUriName);

      wRootUriName = new TextVar(variables, wOutputFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wRootUriName);
      FormData fdRootUriName = new FormData();
      fdRootUriName.left = new FormAttachment(middle, 0);
      fdRootUriName.right = new FormAttachment(100, -margin);
      fdRootUriName.top = new FormAttachment(wlRootUriName, 0, SWT.CENTER);
      wRootUriName.setLayoutData(fdRootUriName);
      lastControl = wRootUriName;
    }

    FormData fdOutputFieldsComp = new FormData();
    fdOutputFieldsComp.left = new FormAttachment(0, 0);
    fdOutputFieldsComp.top = new FormAttachment(wTransformName, margin);
    fdOutputFieldsComp.right = new FormAttachment(100, 0);
    fdOutputFieldsComp.bottom = new FormAttachment(100, 0);
    wOutputFieldsComp.setLayoutData(fdOutputFieldsComp);

    wOutputFieldsComp.layout();
    wOutputFieldsTab.setControl(wOutputFieldsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF ADDITIONAL FIELDS TAB
    // ///////////////////////////////////////////////////////////

  }
}
