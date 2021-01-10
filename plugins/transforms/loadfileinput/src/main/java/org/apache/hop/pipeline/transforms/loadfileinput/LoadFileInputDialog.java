/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.loadfileinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
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
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.nio.charset.Charset;
import java.util.ArrayList;

public class LoadFileInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = LoadFileInputMeta.class; // For Translator

  private static final String[] YES_NO_COMBO =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"), BaseMessages.getString(PKG, "System.Combo.Yes")
      };

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

  private Label wlFilenameField;
  private CCombo wFilenameField;
  private Button wFilenameInField;

  private Label wlInclFilename;
  private Button wInclFilename, wAddResult;

  private Label wlInclFilenameField;
  private TextVar wInclFilenameField;

  private Label wlAddResult;
  private Button wInclRownum;

  private Button wIgnoreEmptyFile;

  private Button wIgnoreMissingPath;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;

  private Text wLimit;

  private Label wlEncoding;
  private CCombo wEncoding;

  private TableView wFields;

  private final LoadFileInputMeta input;

  private boolean gotEncodings = false;

  private boolean gotPreviousFields = false;

  public static final int[] dateLengths = new int[] {23, 19, 14, 10, 10, 10, 10, 8, 8, 8, 8, 6, 6};

  private TextVar wShortFileFieldName;
  private TextVar wPathFieldName;

  private TextVar wIsHiddenName;
  private TextVar wLastModificationTimeName;
  private TextVar wUriName;
  private TextVar wRootUriName;
  private TextVar wExtensionFieldName;

  private int middle;
  private int margin;

  private ModifyListener lsMod;

  public LoadFileInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (LoadFileInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.DialogTitle"));

    middle = props.getMiddlePct();
    margin = props.getMargin();

    // Buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.Button.PreviewRows"));
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
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.File.Tab"));

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout(fileLayout);

    // ///////////////////////////////
    // START OF Output Field GROUP //
    // ///////////////////////////////

    Group wOutputField = new Group(wFileComp, SWT.SHADOW_NONE);
    props.setLook(wOutputField);
    wOutputField.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.wOutputField.Label"));

    FormLayout outputfieldgroupLayout = new FormLayout();
    outputfieldgroupLayout.marginWidth = 10;
    outputfieldgroupLayout.marginHeight = 10;
    wOutputField.setLayout(outputfieldgroupLayout);

    // Is filename defined in a Field
    Label wlFilenameInField = new Label(wOutputField, SWT.RIGHT);
    wlFilenameInField.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameInField.Label"));
    props.setLook(wlFilenameInField);
    FormData fdlFilenameInField = new FormData();
    fdlFilenameInField.left = new FormAttachment(0, -margin);
    fdlFilenameInField.top = new FormAttachment(0, margin);
    fdlFilenameInField.right = new FormAttachment(middle, -2 * margin);
    wlFilenameInField.setLayoutData(fdlFilenameInField);
    wFilenameInField = new Button(wOutputField, SWT.CHECK);
    props.setLook(wFilenameInField);
    wFilenameInField.setToolTipText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameInField.Tooltip"));
    FormData fdFileNameInField = new FormData();
    fdFileNameInField.left = new FormAttachment(middle, -margin);
    fdFileNameInField.top = new FormAttachment(wlFilenameInField, 0, SWT.CENTER);
    wFilenameInField.setLayoutData(fdFileNameInField);
    SelectionAdapter lsxmlstream =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            ActiveXmlStreamField();
            input.setChanged();
          }
        };
    wFilenameInField.addSelectionListener(lsxmlstream);

    // If Filename defined in a Field
    wlFilenameField = new Label(wOutputField, SWT.RIGHT);
    wlFilenameField.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameField.Label"));
    props.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, margin);
    fdlFilenameField.top = new FormAttachment(wFilenameInField, margin);
    fdlFilenameField.right = new FormAttachment(middle, -2 * margin);
    wlFilenameField.setLayoutData(fdlFilenameField);

    wFilenameField = new CCombo(wOutputField, SWT.BORDER | SWT.READ_ONLY);
    wFilenameField.setEditable(true);
    props.setLook(wFilenameField);
    wFilenameField.addModifyListener(lsMod);
    FormData fdXMLField = new FormData();
    fdXMLField.left = new FormAttachment(middle, -margin);
    fdXMLField.top = new FormAttachment(wFilenameInField, margin);
    fdXMLField.right = new FormAttachment(100, -margin);
    wFilenameField.setLayoutData(fdXMLField);
    wFilenameField.addFocusListener(
        new FocusListener() {
          public void focusLost(org.eclipse.swt.events.FocusEvent e) {}

          public void focusGained(org.eclipse.swt.events.FocusEvent e) {
            setDynamicFilenameField();
          }
        });

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
    wlFilename.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.Filename.Label"));
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wOutputField, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameBrowse.Button"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wOutputField, margin);
    wbbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbaFilename);
    wbaFilename.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameAdd.Button"));
    wbaFilename.setToolTipText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameAdd.Tooltip"));
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment(wbbFilename, -margin);
    fdbaFilename.top = new FormAttachment(wOutputField, margin);
    wbaFilename.setLayoutData(fdbaFilename);

    wFilename = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbaFilename, -margin);
    fdFilename.top = new FormAttachment(wOutputField, margin);
    wFilename.setLayoutData(fdFilename);

    wlFilemask = new Label(wFileComp, SWT.RIGHT);
    wlFilemask.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.RegExp.Label"));
    props.setLook(wlFilemask);
    FormData fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment(0, 0);
    fdlFilemask.top = new FormAttachment(wFilename, 2 * margin);
    fdlFilemask.right = new FormAttachment(middle, -margin);
    wlFilemask.setLayoutData(fdlFilemask);
    wFilemask = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilemask);
    wFilemask.addModifyListener(lsMod);
    FormData fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment(middle, 0);
    fdFilemask.top = new FormAttachment(wFilename, 2 * margin);
    fdFilemask.right = new FormAttachment(100, 0);
    wFilemask.setLayoutData(fdFilemask);

    wlExcludeFilemask = new Label(wFileComp, SWT.RIGHT);
    wlExcludeFilemask.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.ExcludeFilemask.Label"));
    props.setLook(wlExcludeFilemask);
    FormData fdlExcludeFilemask = new FormData();
    fdlExcludeFilemask.left = new FormAttachment(0, 0);
    fdlExcludeFilemask.top = new FormAttachment(wFilemask, margin);
    fdlExcludeFilemask.right = new FormAttachment(middle, -margin);
    wlExcludeFilemask.setLayoutData(fdlExcludeFilemask);
    wExcludeFilemask = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wExcludeFilemask);
    wExcludeFilemask.addModifyListener(lsMod);
    FormData fdExcludeFilemask = new FormData();
    fdExcludeFilemask.left = new FormAttachment(middle, 0);
    fdExcludeFilemask.top = new FormAttachment(wFilemask, margin);
    fdExcludeFilemask.right = new FormAttachment(wFilename, 0, SWT.RIGHT);
    wExcludeFilemask.setLayoutData(fdExcludeFilemask);

    // Filename list line
    wlFilenameList = new Label(wFileComp, SWT.RIGHT);
    wlFilenameList.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameList.Label"));
    props.setLook(wlFilenameList);
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment(0, 0);
    fdlFilenameList.top = new FormAttachment(wExcludeFilemask, margin);
    fdlFilenameList.right = new FormAttachment(middle, -margin);
    wlFilenameList.setLayoutData(fdlFilenameList);

    // Buttons to the right of the screen...
    wbdFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbdFilename);
    wbdFilename.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameRemove.Label"));
    wbdFilename.setToolTipText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameRemove.Tooltip"));
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment(100, 0);
    fdbdFilename.top = new FormAttachment(wExcludeFilemask, 40);
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbeFilename);
    wbeFilename.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameEdit.Label"));
    wbeFilename.setToolTipText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.FilenameEdit.Tooltip"));
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment(100, 0);
    fdbeFilename.top = new FormAttachment(wbdFilename, margin);
    wbeFilename.setLayoutData(fdbeFilename);

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.bottom = new FormAttachment(100, 0);
    wbShowFiles.setLayoutData(fdbShowFiles);

    ColumnInfo[] colinfo = new ColumnInfo[5];
    colinfo[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LoadFileInputDialog.Files.Filename.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LoadFileInputDialog.Files.Wildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LoadFileInputDialog.Files.ExcludeWildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    colinfo[0].setUsingVariables(true);
    colinfo[1].setUsingVariables(true);
    colinfo[1].setToolTip(
        BaseMessages.getString(PKG, "LoadFileInputDialog.Files.Wildcard.Tooltip"));
    colinfo[2].setUsingVariables(true);
    colinfo[2].setToolTip(
        BaseMessages.getString(PKG, "LoadFileInputDialog.Files.ExcludeWildcard.Tooltip"));

    colinfo[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LoadFileInputDialog.Required.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);
    colinfo[3].setToolTip(BaseMessages.getString(PKG, "LoadFileInputDialog.Required.Tooltip"));
    colinfo[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LoadFileInputDialog.IncludeSubDirs.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);
    colinfo[4].setToolTip(
        BaseMessages.getString(PKG, "LoadFileInputDialog.IncludeSubDirs.Tooltip"));

    wFilenameList =
        new TableView(
            variables,
            wFileComp,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            colinfo,
            2,
            lsMod,
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

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    middle = props.getMiddlePct();
    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.Content.Tab"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    // ///////////////////////////////
    // START OF FileConf Field GROUP //
    // ///////////////////////////////

    Group wFileConf = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wFileConf);
    wFileConf.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.FileConf.Label"));

    FormLayout XmlConfgroupLayout = new FormLayout();
    XmlConfgroupLayout.marginWidth = 10;
    XmlConfgroupLayout.marginHeight = 10;
    wFileConf.setLayout(XmlConfgroupLayout);

    wlEncoding = new Label(wFileConf, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.Encoding.Label"));
    props.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.top = new FormAttachment(0, margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new CCombo(wFileConf, SWT.BORDER | SWT.READ_ONLY);
    wEncoding.setEditable(true);
    props.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);
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
    wlIgnoreEmptyFile.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.IgnoreEmptyFile.Label"));
    props.setLook(wlIgnoreEmptyFile);
    FormData fdlIgnoreEmptyFile = new FormData();
    fdlIgnoreEmptyFile.left = new FormAttachment(0, 0);
    fdlIgnoreEmptyFile.top = new FormAttachment(wEncoding, margin);
    fdlIgnoreEmptyFile.right = new FormAttachment(middle, -margin);
    wlIgnoreEmptyFile.setLayoutData(fdlIgnoreEmptyFile);
    wIgnoreEmptyFile = new Button(wFileConf, SWT.CHECK);
    props.setLook(wIgnoreEmptyFile);
    wIgnoreEmptyFile.setToolTipText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.IgnoreEmptyFile.Tooltip"));
    FormData fdIgnoreEmptyFile = new FormData();
    fdIgnoreEmptyFile.left = new FormAttachment(middle, 0);
    fdIgnoreEmptyFile.top = new FormAttachment(wlIgnoreEmptyFile, 0, SWT.CENTER);
    wIgnoreEmptyFile.setLayoutData(fdIgnoreEmptyFile);
    wIgnoreEmptyFile.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Ignore missing path
    Label wlIgnoreMissingPath = new Label(wFileConf, SWT.RIGHT);
    wlIgnoreMissingPath.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.IgnoreMissingPath.Label"));
    props.setLook(wlIgnoreMissingPath);
    FormData fdlIgnoreMissingPath = new FormData();
    fdlIgnoreMissingPath.left = new FormAttachment(0, 0);
    fdlIgnoreMissingPath.top = new FormAttachment(wIgnoreEmptyFile, margin);
    fdlIgnoreMissingPath.right = new FormAttachment(middle, -margin);
    wlIgnoreMissingPath.setLayoutData(fdlIgnoreMissingPath);
    wIgnoreMissingPath = new Button(wFileConf, SWT.CHECK);
    props.setLook(wIgnoreMissingPath);
    wIgnoreMissingPath.setToolTipText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.IgnoreMissingPath.Tooltip"));
    FormData fdIgnoreMissingPath = new FormData();
    fdIgnoreMissingPath.left = new FormAttachment(middle, 0);
    fdIgnoreMissingPath.top = new FormAttachment(wlIgnoreMissingPath, 0, SWT.CENTER);
    wIgnoreMissingPath.setLayoutData(fdIgnoreMissingPath);
    wIgnoreMissingPath.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // preview limit
    Label wlLimit = new Label(wFileConf, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.Limit.Label"));
    props.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.top = new FormAttachment(wIgnoreMissingPath, margin);
    fdlLimit.right = new FormAttachment(middle, -margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wFileConf, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.top = new FormAttachment(wIgnoreMissingPath, margin);
    fdLimit.right = new FormAttachment(100, 0);
    wLimit.setLayoutData(fdLimit);

    FormData fdXmlConf = new FormData();
    fdXmlConf.left = new FormAttachment(0, margin);
    fdXmlConf.top = new FormAttachment(0, margin);
    fdXmlConf.right = new FormAttachment(100, -margin);
    wFileConf.setLayoutData(fdXmlConf);

    // ///////////////////////////////////////////////////////////
    // / END OF XmlConf Field GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF Additional Fields GROUP //
    // ///////////////////////////////

    Group wAdditionalFields = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wAdditionalFields);
    wAdditionalFields.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.wAdditionalFields.Label"));

    FormLayout AdditionalFieldsgroupLayout = new FormLayout();
    AdditionalFieldsgroupLayout.marginWidth = 10;
    AdditionalFieldsgroupLayout.marginHeight = 10;
    wAdditionalFields.setLayout(AdditionalFieldsgroupLayout);

    wlInclFilename = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclFilename.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.InclFilename.Label"));
    props.setLook(wlInclFilename);
    FormData fdlInclFilename = new FormData();
    fdlInclFilename.left = new FormAttachment(0, 0);
    fdlInclFilename.top = new FormAttachment(wFileConf, 4 * margin);
    fdlInclFilename.right = new FormAttachment(middle, -margin);
    wlInclFilename.setLayoutData(fdlInclFilename);
    wInclFilename = new Button(wAdditionalFields, SWT.CHECK);
    props.setLook(wInclFilename);
    wInclFilename.setToolTipText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.InclFilename.Tooltip"));
    FormData fdInclFilename = new FormData();
    fdInclFilename.left = new FormAttachment(middle, 0);
    fdInclFilename.top = new FormAttachment(wlInclFilename, 0, SWT.CENTER);
    wInclFilename.setLayoutData(fdInclFilename);

    wlInclFilenameField = new Label(wAdditionalFields, SWT.LEFT);
    wlInclFilenameField.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.InclFilenameField.Label"));
    props.setLook(wlInclFilenameField);
    FormData fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment(wInclFilename, margin);
    fdlInclFilenameField.top = new FormAttachment(wLimit, 4 * margin);
    wlInclFilenameField.setLayoutData(fdlInclFilenameField);
    wInclFilenameField =
        new TextVar(variables, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wInclFilenameField);
    wInclFilenameField.addModifyListener(lsMod);
    FormData fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment(wlInclFilenameField, margin);
    fdInclFilenameField.top = new FormAttachment(wLimit, 4 * margin);
    fdInclFilenameField.right = new FormAttachment(100, 0);
    wInclFilenameField.setLayoutData(fdInclFilenameField);

    Label wlInclRownum = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclRownum.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.InclRownum.Label"));
    props.setLook(wlInclRownum);
    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment(0, 0);
    fdlInclRownum.top = new FormAttachment(wInclFilenameField, margin);
    fdlInclRownum.right = new FormAttachment(middle, -margin);
    wlInclRownum.setLayoutData(fdlInclRownum);
    wInclRownum = new Button(wAdditionalFields, SWT.CHECK);
    props.setLook(wInclRownum);
    wInclRownum.setToolTipText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.InclRownum.Tooltip"));
    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment(middle, 0);
    fdRownum.top = new FormAttachment(wlInclRownum, 0, SWT.CENTER);
    wInclRownum.setLayoutData(fdRownum);

    wlInclRownumField = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclRownumField.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.InclRownumField.Label"));
    props.setLook(wlInclRownumField);
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment(wInclRownum, margin);
    fdlInclRownumField.top = new FormAttachment(wInclFilenameField, margin);
    wlInclRownumField.setLayoutData(fdlInclRownumField);
    wInclRownumField =
        new TextVar(variables, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wInclRownumField);
    wInclRownumField.addModifyListener(lsMod);
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment(wlInclRownumField, margin);
    fdInclRownumField.top = new FormAttachment(wInclFilenameField, margin);
    fdInclRownumField.right = new FormAttachment(100, 0);
    wInclRownumField.setLayoutData(fdInclRownumField);

    FormData fdAdditionalFields = new FormData();
    fdAdditionalFields.left = new FormAttachment(0, margin);
    fdAdditionalFields.top = new FormAttachment(wFileConf, margin);
    fdAdditionalFields.right = new FormAttachment(100, -margin);
    wAdditionalFields.setLayoutData(fdAdditionalFields);

    // ///////////////////////////////////////////////////////////
    // / END OF Additional Fields GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    Group wAddFileResult = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wAddFileResult);
    wAddFileResult.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.wAddFileResult.Label"));

    FormLayout AddFileResultgroupLayout = new FormLayout();
    AddFileResultgroupLayout.marginWidth = 10;
    AddFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout(AddFileResultgroupLayout);

    wlAddResult = new Label(wAddFileResult, SWT.RIGHT);
    wlAddResult.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.AddResult.Label"));
    props.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment(0, 0);
    fdlAddResult.top = new FormAttachment(wAdditionalFields, margin);
    fdlAddResult.right = new FormAttachment(middle, -margin);
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wAddFileResult, SWT.CHECK);
    props.setLook(wAddResult);
    wAddResult.setToolTipText(BaseMessages.getString(PKG, "LoadFileInputDialog.AddResult.Tooltip"));
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(middle, 0);
    fdAddResult.top = new FormAttachment(wlAddResult, 0, SWT.CENTER);
    wAddResult.setLayoutData(fdAddResult);
    wAddResult.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment(0, margin);
    fdAddFileResult.top = new FormAttachment(wAdditionalFields, margin);
    fdAddFileResult.right = new FormAttachment(100, -margin);
    wAddFileResult.setLayoutData(fdAddFileResult);

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

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.Fields.Tab"));

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout(fieldsLayout);
    props.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.GetFields.Button"));
    fdGet = new FormData();
    fdGet.left = new FormAttachment(50, 0);
    fdGet.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);

    final int FieldsRows = input.getInputFields().length;

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Name.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Element.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              LoadFileInputField.ElementTypeDesc,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Type.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Format.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              Const.getConversionFormats()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Currency.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Decimal.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Group.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.TrimType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              LoadFileInputField.trimTypeDesc,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Repeat.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, "System.Combo.Yes"),
                BaseMessages.getString(PKG, "System.Combo.No")
              },
              true),
        };

    colinf[0].setUsingVariables(true);
    colinf[0].setToolTip(
        BaseMessages.getString(PKG, "LoadFileInputDialog.FieldsTable.Name.Column.Tooltip"));
    colinfo[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LoadFileInputDialog.Required.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);
    colinfo[2].setToolTip(BaseMessages.getString(PKG, "LoadFileInputDialog.Required.Tooltip"));
    colinfo[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LoadFileInputDialog.IncludeSubDirs.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);
    colinfo[3].setToolTip(
        BaseMessages.getString(PKG, "LoadFileInputDialog.IncludeSubDirs.Tooltip"));

    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wGet, -margin);
    wFields.setLayoutData(fdFields);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    addAdditionalFieldsTab();

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    wGet.addListener(SWT.Selection, e -> get());

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wLimit.addSelectionListener(lsDef);
    wInclRownumField.addSelectionListener(lsDef);
    wInclFilenameField.addSelectionListener(lsDef);

    // Add the file to the list of files...
    SelectionAdapter selA =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            wFilenameList.add(
                new String[] {
                  wFilename.getText(),
                  wFilemask.getText(),
                  wExcludeFilemask.getText(),
                  LoadFileInputMeta.RequiredFilesCode[0],
                  LoadFileInputMeta.RequiredFilesCode[0]
                });
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
          public void widgetSelected(SelectionEvent e) {
            try {
              LoadFileInputMeta tfii = new LoadFileInputMeta();
              getInfo(tfii);
              FileInputList fileInputList = tfii.getFiles(variables);
              String[] files = fileInputList.getFileStrings();
              if (files != null && files.length > 0) {
                EnterSelectionDialog esd =
                    new EnterSelectionDialog(
                        shell,
                        files,
                        BaseMessages.getString(
                            PKG, "LoadFileInputDialog.FilesReadSelection.DialogTitle"),
                        BaseMessages.getString(
                            PKG, "LoadFileInputDialog.FilesReadSelection.DialogMessage"));
                esd.setViewOnly();
                esd.open();
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(
                    BaseMessages.getString(PKG, "LoadFileInputDialog.NoFileFound.DialogMessage"));
                mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
                mb.open();
              }
            } catch (Exception ex) {
              new ErrorDialog(
                  shell,
                  BaseMessages.getString(PKG, "LoadFileInputDialog.ErrorParsingData.DialogTitle"),
                  BaseMessages.getString(PKG, "LoadFileInputDialog.ErrorParsingData.DialogMessage"),
                  ex);
            }
          }
        });
    // Enable/disable the right fields to allow a filename to be added to each row...
    wInclFilename.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            setIncludeFilename();
            input.setChanged();
          }
        });

    // Enable/disable the right fields to allow a row number to be added to each row...
    wInclRownum.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            setIncludeRownum();
            input.setChanged();
          }
        });

    // Whenever something changes, set the tooltip to the expanded version of the filename:
    wFilename.addModifyListener(e -> wFilename.setToolTipText(wFilename.getText()));

    // Listen to the Browse... button
    wbbFilename.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            if (!Utils.isEmpty(wFilemask.getText())
                || !Utils.isEmpty(wExcludeFilemask.getText())) { // A mask: a directory!
              BaseDialog.presentDirectoryDialog(shell, wFilename, variables);
            } else {
              BaseDialog.presentFileDialog(
                  shell,
                  wFilename,
                  variables,
                  new String[] {"*.txt;", "*.csv", "*.TRT", "*"},
                  new String[] {
                    BaseMessages.getString(PKG, "System.FileType.TextFiles"),
                    BaseMessages.getString(PKG, "LoadFileInputDialog.FileType.TextAndCSVFiles"),
                    BaseMessages.getString(PKG, "LoadFileInput.FileType.TRTFiles"),
                    BaseMessages.getString(PKG, "System.FileType.AllFiles")
                  },
                  true);
            }
          }
        });

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    wTabFolder.setSelection(0);

    // Set the shell size, based upon previous time...
    setSize();
    getData(input);
    ActiveXmlStreamField();
    input.setChanged(changed);
    wFields.optWidth(true);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
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
            BaseMessages.getString(PKG, "LoadFileInputDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "LoadFileInputDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
    }
  }

  private void ActiveXmlStreamField() {
    wlFilenameField.setEnabled(wFilenameInField.getSelection());
    wFilenameField.setEnabled(wFilenameInField.getSelection());

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
    wInclFilename.setEnabled(!wFilenameInField.getSelection());
    wlInclFilename.setEnabled(!wFilenameInField.getSelection());

    if (wFilenameInField.getSelection()) {
      wInclFilename.setSelection(false);
      wlInclFilenameField.setEnabled(false);
      wInclFilenameField.setEnabled(false);
    } else {
      wlInclFilenameField.setEnabled(wInclFilename.getSelection());
      wInclFilenameField.setEnabled(wInclFilename.getSelection());
    }

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

  private void get() {

    int clearFields = SWT.NO;
    int nrInputFields = wFields.nrNonEmpty();

    if (nrInputFields > 0) {
      MessageBox mb = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      mb.setMessage(
          BaseMessages.getString(PKG, "LoadFileInputDialog.ClearFieldList.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.ClearFieldList.DialogTitle"));
      clearFields = mb.open();
    }

    if (clearFields == SWT.YES) {
      // Clear Fields Grid
      wFields.table.removeAll();
    }

    TableItem item = new TableItem(wFields.table, SWT.NONE);
    item.setText(1, LoadFileInputField.ElementTypeDesc[0]);
    item.setText(2, LoadFileInputField.ElementTypeDesc[0]);
    item.setText(3, "String");
    // file size
    item = new TableItem(wFields.table, SWT.NONE);
    item.setText(1, LoadFileInputField.ElementTypeDesc[1]);
    item.setText(2, LoadFileInputField.ElementTypeDesc[1]);
    item.setText(3, "Integer");

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void setEncodings() {
    // Encoding of the text file:
    if (!gotEncodings) {
      gotEncodings = true;
      String encoding = wEncoding.getText();
      wEncoding.removeAll();
      ArrayList<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
      }

      if (!Utils.isEmpty(encoding)) {
        wEncoding.setText(encoding);
      } /*
         * else { // Now select the default! String defEncoding = Const.getEnvironmentVariable("file.encoding",
         * "UTF-8"); int idx = Const.indexOfString(defEncoding, wEncoding.getItems() ); if (idx>=0) wEncoding.select(
         * idx ); }
         */
    }
  }

  public void setIncludeFilename() {
    wlInclFilenameField.setEnabled(wInclFilename.getSelection());
    wInclFilenameField.setEnabled(wInclFilename.getSelection());
  }

  public void setIncludeRownum() {
    wlInclRownumField.setEnabled(wInclRownum.getSelection());
    wInclRownumField.setEnabled(wInclRownum.getSelection());
  }

  /**
   * Read the data from the TextFileInputMeta object and show it in this dialog.
   *
   * @param in The TextFileInputMeta object to obtain the data from.
   */
  public void getData(LoadFileInputMeta in) {
    if (in.getFileName() != null) {
      wFilenameList.removeAll();

      for (int i = 0; i < in.getFileName().length; i++) {
        wFilenameList.add(
            new String[] {
              in.getFileName()[i],
              in.getFileMask()[i],
              in.getExludeFileMask()[i],
              in.getRequiredFilesDesc(in.getFileRequired()[i]),
              in.getRequiredFilesDesc(in.getIncludeSubFolders()[i])
            });
      }

      wFilenameList.removeEmptyRows();
      wFilenameList.setRowNums();
      wFilenameList.optWidth(true);
    }
    wInclFilename.setSelection(in.includeFilename());
    wInclRownum.setSelection(in.includeRowNumber());
    wAddResult.setSelection(in.addResultFile());

    wIgnoreEmptyFile.setSelection(in.isIgnoreEmptyFile());
    wIgnoreMissingPath.setSelection(in.isIgnoreMissingPath());
    wFilenameInField.setSelection(in.getIsInFields());

    if (in.getDynamicFilenameField() != null) {
      wFilenameField.setText(in.getDynamicFilenameField());
    }

    if (in.getFilenameField() != null) {
      wInclFilenameField.setText(in.getFilenameField());
    }
    if (in.getRowNumberField() != null) {
      wInclRownumField.setText(in.getRowNumberField());
    }
    wLimit.setText("" + in.getRowLimit());
    wEncoding.setText(Const.NVL(in.getEncoding(), ""));

    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "LoadFileInputDialog.Log.GettingFieldsInfo"));
    }
    for (int i = 0; i < in.getInputFields().length; i++) {
      LoadFileInputField field = in.getInputFields()[i];

      if (field != null) {
        TableItem item = wFields.table.getItem(i);
        String name = field.getName();
        String element = field.getElementTypeDesc();
        String type = field.getTypeDesc();
        String format = field.getFormat();
        String length = "" + field.getLength();
        String prec = "" + field.getPrecision();
        String curr = field.getCurrencySymbol();
        String group = field.getGroupSymbol();
        String decim = field.getDecimalSymbol();
        String trim = field.getTrimTypeDesc();
        String rep =
            field.isRepeated()
                ? BaseMessages.getString(PKG, "System.Combo.Yes")
                : BaseMessages.getString(PKG, "System.Combo.No");

        if (name != null) {
          item.setText(1, name);
        }
        if (element != null) {
          item.setText(2, element);
        }
        if (type != null) {
          item.setText(3, type);
        }
        if (format != null) {
          item.setText(4, format);
        }
        if (length != null && !"-1".equals(length)) {
          item.setText(5, length);
        }
        if (prec != null && !"-1".equals(prec)) {
          item.setText(6, prec);
        }
        if (curr != null) {
          item.setText(7, curr);
        }
        if (decim != null) {
          item.setText(8, decim);
        }
        if (group != null) {
          item.setText(9, group);
        }
        if (trim != null) {
          item.setText(10, trim);
        }
        if (rep != null) {
          item.setText(11, rep);
        }
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
    if (in.getShortFileNameField() != null) {
      wShortFileFieldName.setText(in.getShortFileNameField());
    }
    if (in.getPathField() != null) {
      wPathFieldName.setText(in.getPathField());
    }
    if (in.isHiddenField() != null) {
      wIsHiddenName.setText(in.isHiddenField());
    }
    if (in.getLastModificationDateField() != null) {
      wLastModificationTimeName.setText(in.getLastModificationDateField());
    }
    if (in.getUriField() != null) {
      wUriName.setText(in.getUriField());
    }
    if (in.getRootUriField() != null) {
      wRootUriName.setText(in.getRootUriField());
    }
    if (in.getExtensionField() != null) {
      wExtensionFieldName.setText(in.getExtensionField());
    }

    setIncludeFilename();
    setIncludeRownum();
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
          BaseMessages.getString(PKG, "LoadFileInputDialog.ErrorParsingData.DialogTitle"),
          BaseMessages.getString(PKG, "LoadFileInputDialog.ErrorParsingData.DialogMessage"),
          e);
    }
    dispose();
  }

  private void getInfo(LoadFileInputMeta in) throws HopException {
    transformName = wTransformName.getText(); // return value

    // copy info to TextFileInputMeta class (input)
    in.setRowLimit(Const.toLong(wLimit.getText(), 0L));
    in.setEncoding(wEncoding.getText());
    in.setRowNumberField(wInclRownumField.getText());
    in.setAddResultFile(wAddResult.getSelection());
    in.setIgnoreEmptyFile(wIgnoreEmptyFile.getSelection());
    in.setIgnoreMissingPath(wIgnoreMissingPath.getSelection());

    in.setIncludeFilename(wInclFilename.getSelection());
    in.setFilenameField(wInclFilenameField.getText());
    in.setIncludeRowNumber(wInclRownum.getSelection());

    in.setIsInFields(wFilenameInField.getSelection());
    in.setDynamicFilenameField(wFilenameField.getText());

    int nrFields = wFields.nrNonEmpty();

    if (wFilenameInField.getSelection()) {
      in.allocate(0, nrFields);

      in.setFileName(new String[0]);
      in.setFileMask(new String[0]);
      in.setExcludeFileMask(new String[0]);
      in.setFileRequired(new String[0]);
      in.setIncludeSubFolders(new String[0]);
    } else {
      in.allocate(wFilenameList.getItemCount(), nrFields);

      in.setFileName(wFilenameList.getItems(0));
      in.setFileMask(wFilenameList.getItems(1));
      in.setExcludeFileMask(wFilenameList.getItems(2));
      in.setFileRequired(wFilenameList.getItems(3));
      in.setIncludeSubFolders(wFilenameList.getItems(4));
    }

    for (int i = 0; i < nrFields; i++) {
      LoadFileInputField field = new LoadFileInputField();

      TableItem item = wFields.getNonEmpty(i);

      field.setName(item.getText(1));
      field.setElementType(LoadFileInputField.getElementTypeByDesc(item.getText(2)));
      field.setType(ValueMetaFactory.getIdForValueMeta(item.getText(3)));
      field.setFormat(item.getText(4));
      field.setLength(Const.toInt(item.getText(5), -1));
      field.setPrecision(Const.toInt(item.getText(6), -1));
      field.setCurrencySymbol(item.getText(7));
      field.setDecimalSymbol(item.getText(8));
      field.setGroupSymbol(item.getText(9));
      field.setTrimType(LoadFileInputField.getTrimTypeByDesc(item.getText(10)));
      field.setRepeated(
          BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(item.getText(11)));

      // CHECKSTYLE:Indentation:OFF
      in.getInputFields()[i] = field;
    }
    in.setShortFileNameField(wShortFileFieldName.getText());
    in.setPathField(wPathFieldName.getText());
    in.setIsHiddenField(wIsHiddenName.getText());
    in.setLastModificationDateField(wLastModificationTimeName.getText());
    in.setUriField(wUriName.getText());
    in.setRootUriField(wRootUriName.getText());
    in.setExtensionField(wExtensionFieldName.getText());
  }

  // Preview the data
  private void preview() {
    try {
      // Create the XML input transform
      LoadFileInputMeta oneMeta = new LoadFileInputMeta();
      getInfo(oneMeta);

      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
              variables, pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

      EnterNumberDialog numberDialog =
          new EnterNumberDialog(
              shell,
              props.getDefaultPreviewSize(),
              BaseMessages.getString(PKG, "LoadFileInputDialog.NumberRows.DialogTitle"),
              BaseMessages.getString(PKG, "LoadFileInputDialog.NumberRows.DialogMessage"));

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
          BaseMessages.getString(PKG, "LoadFileInputDialog.ErrorPreviewingData.DialogTitle"),
          BaseMessages.getString(PKG, "LoadFileInputDialog.ErrorPreviewingData.DialogMessage"),
          e);
    }
  }

  private void addAdditionalFieldsTab() {
    // ////////////////////////
    // START OF ADDITIONAL FIELDS TAB ///
    // ////////////////////////
    CTabItem wAdditionalFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalFieldsTab.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.AdditionalFieldsTab.TabTitle"));

    Composite wAdditionalFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdditionalFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wAdditionalFieldsComp.setLayout(fieldsLayout);
    // ShortFileFieldName line
    Label wlShortFileFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlShortFileFieldName.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.ShortFileFieldName.Label"));
    props.setLook(wlShortFileFieldName);
    FormData fdlShortFileFieldName = new FormData();
    fdlShortFileFieldName.left = new FormAttachment(0, 0);
    fdlShortFileFieldName.top = new FormAttachment(wInclRownumField, margin);
    fdlShortFileFieldName.right = new FormAttachment(middle, -margin);
    wlShortFileFieldName.setLayoutData(fdlShortFileFieldName);

    wShortFileFieldName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wShortFileFieldName);
    wShortFileFieldName.addModifyListener(lsMod);
    FormData fdShortFileFieldName = new FormData();
    fdShortFileFieldName.left = new FormAttachment(middle, 0);
    fdShortFileFieldName.right = new FormAttachment(100, -margin);
    fdShortFileFieldName.top = new FormAttachment(wInclRownumField, margin);
    wShortFileFieldName.setLayoutData(fdShortFileFieldName);

    // ExtensionFieldName line
    Label wlExtensionFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlExtensionFieldName.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.ExtensionFieldName.Label"));
    props.setLook(wlExtensionFieldName);
    FormData fdlExtensionFieldName = new FormData();
    fdlExtensionFieldName.left = new FormAttachment(0, 0);
    fdlExtensionFieldName.top = new FormAttachment(wShortFileFieldName, margin);
    fdlExtensionFieldName.right = new FormAttachment(middle, -margin);
    wlExtensionFieldName.setLayoutData(fdlExtensionFieldName);

    wExtensionFieldName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wExtensionFieldName);
    wExtensionFieldName.addModifyListener(lsMod);
    FormData fdExtensionFieldName = new FormData();
    fdExtensionFieldName.left = new FormAttachment(middle, 0);
    fdExtensionFieldName.right = new FormAttachment(100, -margin);
    fdExtensionFieldName.top = new FormAttachment(wShortFileFieldName, margin);
    wExtensionFieldName.setLayoutData(fdExtensionFieldName);

    // PathFieldName line
    Label wlPathFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlPathFieldName.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.PathFieldName.Label"));
    props.setLook(wlPathFieldName);
    FormData fdlPathFieldName = new FormData();
    fdlPathFieldName.left = new FormAttachment(0, 0);
    fdlPathFieldName.top = new FormAttachment(wExtensionFieldName, margin);
    fdlPathFieldName.right = new FormAttachment(middle, -margin);
    wlPathFieldName.setLayoutData(fdlPathFieldName);

    wPathFieldName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPathFieldName);
    wPathFieldName.addModifyListener(lsMod);
    FormData fdPathFieldName = new FormData();
    fdPathFieldName.left = new FormAttachment(middle, 0);
    fdPathFieldName.right = new FormAttachment(100, -margin);
    fdPathFieldName.top = new FormAttachment(wExtensionFieldName, margin);
    wPathFieldName.setLayoutData(fdPathFieldName);

    // IsHiddenName line
    Label wlIsHiddenName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlIsHiddenName.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.IsHiddenName.Label"));
    props.setLook(wlIsHiddenName);
    FormData fdlIsHiddenName = new FormData();
    fdlIsHiddenName.left = new FormAttachment(0, 0);
    fdlIsHiddenName.top = new FormAttachment(wPathFieldName, margin);
    fdlIsHiddenName.right = new FormAttachment(middle, -margin);
    wlIsHiddenName.setLayoutData(fdlIsHiddenName);

    wIsHiddenName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wIsHiddenName);
    wIsHiddenName.addModifyListener(lsMod);
    FormData fdIsHiddenName = new FormData();
    fdIsHiddenName.left = new FormAttachment(middle, 0);
    fdIsHiddenName.right = new FormAttachment(100, -margin);
    fdIsHiddenName.top = new FormAttachment(wPathFieldName, margin);
    wIsHiddenName.setLayoutData(fdIsHiddenName);

    // LastModificationTimeName line
    Label wlLastModificationTimeName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlLastModificationTimeName.setText(
        BaseMessages.getString(PKG, "LoadFileInputDialog.LastModificationTimeName.Label"));
    props.setLook(wlLastModificationTimeName);
    FormData fdlLastModificationTimeName = new FormData();
    fdlLastModificationTimeName.left = new FormAttachment(0, 0);
    fdlLastModificationTimeName.top = new FormAttachment(wIsHiddenName, margin);
    fdlLastModificationTimeName.right = new FormAttachment(middle, -margin);
    wlLastModificationTimeName.setLayoutData(fdlLastModificationTimeName);

    wLastModificationTimeName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLastModificationTimeName);
    wLastModificationTimeName.addModifyListener(lsMod);
    FormData fdLastModificationTimeName = new FormData();
    fdLastModificationTimeName.left = new FormAttachment(middle, 0);
    fdLastModificationTimeName.right = new FormAttachment(100, -margin);
    fdLastModificationTimeName.top = new FormAttachment(wIsHiddenName, margin);
    wLastModificationTimeName.setLayoutData(fdLastModificationTimeName);

    // UriName line
    Label wlUriName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlUriName.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.UriName.Label"));
    props.setLook(wlUriName);
    FormData fdlUriName = new FormData();
    fdlUriName.left = new FormAttachment(0, 0);
    fdlUriName.top = new FormAttachment(wLastModificationTimeName, margin);
    fdlUriName.right = new FormAttachment(middle, -margin);
    wlUriName.setLayoutData(fdlUriName);

    wUriName = new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUriName);
    wUriName.addModifyListener(lsMod);
    FormData fdUriName = new FormData();
    fdUriName.left = new FormAttachment(middle, 0);
    fdUriName.right = new FormAttachment(100, -margin);
    fdUriName.top = new FormAttachment(wLastModificationTimeName, margin);
    wUriName.setLayoutData(fdUriName);

    // RootUriName line
    Label wlRootUriName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlRootUriName.setText(BaseMessages.getString(PKG, "LoadFileInputDialog.RootUriName.Label"));
    props.setLook(wlRootUriName);
    FormData fdlRootUriName = new FormData();
    fdlRootUriName.left = new FormAttachment(0, 0);
    fdlRootUriName.top = new FormAttachment(wUriName, margin);
    fdlRootUriName.right = new FormAttachment(middle, -margin);
    wlRootUriName.setLayoutData(fdlRootUriName);

    wRootUriName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRootUriName);
    wRootUriName.addModifyListener(lsMod);
    FormData fdRootUriName = new FormData();
    fdRootUriName.left = new FormAttachment(middle, 0);
    fdRootUriName.right = new FormAttachment(100, -margin);
    fdRootUriName.top = new FormAttachment(wUriName, margin);
    wRootUriName.setLayoutData(fdRootUriName);

    FormData fdAdditionalFieldsComp = new FormData();
    fdAdditionalFieldsComp.left = new FormAttachment(0, 0);
    fdAdditionalFieldsComp.top = new FormAttachment(wTransformName, margin);
    fdAdditionalFieldsComp.right = new FormAttachment(100, 0);
    fdAdditionalFieldsComp.bottom = new FormAttachment(100, 0);
    wAdditionalFieldsComp.setLayoutData(fdAdditionalFieldsComp);

    wAdditionalFieldsComp.layout();
    wAdditionalFieldsTab.setControl(wAdditionalFieldsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF ADDITIONAL FIELDS TAB
    // ///////////////////////////////////////////////////////////

  }
}
