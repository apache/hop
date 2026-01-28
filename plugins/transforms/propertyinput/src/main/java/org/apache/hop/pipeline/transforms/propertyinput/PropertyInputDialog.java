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

package org.apache.hop.pipeline.transforms.propertyinput;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.ini4j.Wini;

public class PropertyInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = PropertyInputMeta.class;
  public static final String CONST_SYSTEM_COMBO_YES = "System.Combo.Yes";
  public static final String CONST_SYSTEM_COMBO_NO = "System.Combo.No";
  private static final String[] YES_NO_COMBO =
      new String[] {
        BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO),
        BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
      };
  public static final String CONST_SYSTEM_DIALOG_ERROR_TITLE = "System.Dialog.Error.Title";

  private CTabFolder wTabFolder;

  private Label wlFilename;
  private Button wbbFilename; // Browse: add file or directory

  private Button wbdFilename; // Delete
  private Button wbeFilename; // Edit
  private Button wbaFilename; // Add or change
  private TextVar wFilename;

  private Label wlFilenameList;
  private TableView wFilenameList;

  private Label wlFileMask;
  private TextVar wFileMask;

  private Label wlExcludeFileMask;
  private TextVar wExcludeFileMask;

  private Button wbShowFiles;

  private Label wlInclFilename;
  private Button wInclFilename;

  private Label wlInclFilenameField;
  private TextVar wInclFilenameField;

  private Button wInclRowNum;

  private boolean gotEncodings;

  private Label wlInclRowNumField;
  private TextVar wInclRowNumField;

  private Label wlInclIniSection;
  private Button wInclIniSection;

  private Label wlInclIniSectionField;
  private TextVar wInclIniSectionField;

  private Button wResetRowNum;

  private Button wResolveValueVariable;

  private Label wlLimit;
  private Text wLimit;

  private Label wlSection;
  private TextVar wSection;
  private Button wbbSection;

  private TableView wFields;

  private final PropertyInputMeta input;

  private Button wFileField;
  private Button wAddResult;

  private Label wlEncoding;
  private ComboVar wEncoding;

  private CCombo wFileType;

  private Label wlFilenameField;
  private CCombo wFilenameField;

  private TextVar wShortFileFieldName;
  private TextVar wPathFieldName;

  private TextVar wIsHiddenName;
  private TextVar wLastModificationTimeName;
  private TextVar wUriName;
  private TextVar wRootUriName;
  private TextVar wExtensionFieldName;
  private TextVar wSizeFieldName;

  private boolean gotPreviousFields = false;

  public PropertyInputDialog(
      Shell parent,
      IVariables variables,
      PropertyInputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "PropertyInputDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).preview(e -> preview()).cancel(e -> cancel()).build();

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wFileTab.setText(BaseMessages.getString(PKG, "PropertyInputDialog.File.Tab"));

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout(fileLayout);

    // ///////////////////////////////
    // START OF Origin files GROUP //
    // ///////////////////////////////

    Group wOriginFiles = new Group(wFileComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wOriginFiles);
    wOriginFiles.setText(BaseMessages.getString(PKG, "PropertyInputDialog.wOriginFiles.Label"));

    FormLayout originFilesGroupLayout = new FormLayout();
    originFilesGroupLayout.marginWidth = 10;
    originFilesGroupLayout.marginHeight = 10;
    wOriginFiles.setLayout(originFilesGroupLayout);

    // Is Filename defined in a Field
    Label wlFileField = new Label(wOriginFiles, SWT.RIGHT);
    wlFileField.setText(BaseMessages.getString(PKG, "PropertyInputDialog.FileField.Label"));
    PropsUi.setLook(wlFileField);
    FormData fdlFileField = new FormData();
    fdlFileField.left = new FormAttachment(0, 0);
    fdlFileField.top = new FormAttachment(0, margin);
    fdlFileField.right = new FormAttachment(middle, -margin);
    wlFileField.setLayoutData(fdlFileField);
    wFileField = new Button(wOriginFiles, SWT.CHECK);
    PropsUi.setLook(wFileField);
    wFileField.setToolTipText(BaseMessages.getString(PKG, "PropertyInputDialog.FileField.Tooltip"));
    FormData fdFileField = new FormData();
    fdFileField.left = new FormAttachment(middle, margin);
    fdFileField.top = new FormAttachment(wlFileField, 0, SWT.CENTER);
    wFileField.setLayoutData(fdFileField);
    wFileField.addListener(SWT.Selection, e -> activateFileField());

    // Filename field
    wlFilenameField = new Label(wOriginFiles, SWT.RIGHT);
    wlFilenameField.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.wlFilenameField.Label"));
    PropsUi.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, 0);
    fdlFilenameField.top = new FormAttachment(wFileField, margin);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    wlFilenameField.setLayoutData(fdlFilenameField);

    wFilenameField = new CCombo(wOriginFiles, SWT.BORDER | SWT.READ_ONLY);
    wFilenameField.setEditable(true);
    PropsUi.setLook(wFilenameField);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, margin);
    fdFilenameField.top = new FormAttachment(wFileField, margin);
    fdFilenameField.right = new FormAttachment(100, -margin);
    wFilenameField.setLayoutData(fdFilenameField);
    wFilenameField.addListener(SWT.FocusIn, e -> setFileField());

    FormData fdOriginFiles = new FormData();
    fdOriginFiles.left = new FormAttachment(0, margin);
    fdOriginFiles.top = new FormAttachment(wFilenameList, margin);
    fdOriginFiles.right = new FormAttachment(100, -margin);
    wOriginFiles.setLayoutData(fdOriginFiles);

    // ///////////////////////////////////////////////////////////
    // / END OF Origin files GROUP
    // ///////////////////////////////////////////////////////////

    middle = middle / 2;

    // Filename line
    wlFilename = new Label(wFileComp, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "PropertyInputDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wOriginFiles, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "PropertyInputDialog.FilenameBrowse.Button"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wbbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbaFilename);
    wbaFilename.setText(BaseMessages.getString(PKG, "PropertyInputDialog.FilenameAdd.Button"));
    wbaFilename.setToolTipText(
        BaseMessages.getString(PKG, "PropertyInputDialog.FilenameAdd.Tooltip"));
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment(wbbFilename, -margin);
    fdbaFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wbaFilename.setLayoutData(fdbaFilename);

    wFilename = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbaFilename, -margin);
    fdFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wFilename.setLayoutData(fdFilename);

    wlFileMask = new Label(wFileComp, SWT.RIGHT);
    wlFileMask.setText(BaseMessages.getString(PKG, "PropertyInputDialog.RegExp.Label"));
    PropsUi.setLook(wlFileMask);
    FormData fdlFileMask = new FormData();
    fdlFileMask.left = new FormAttachment(0, 0);
    fdlFileMask.top = new FormAttachment(wFilename, margin);
    fdlFileMask.right = new FormAttachment(middle, -margin);
    wlFileMask.setLayoutData(fdlFileMask);
    wFileMask = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFileMask);
    FormData fdFileMask = new FormData();
    fdFileMask.left = new FormAttachment(middle, 0);
    fdFileMask.top = new FormAttachment(wlFileMask, 0, SWT.CENTER);
    fdFileMask.right = new FormAttachment(wbaFilename, -margin);
    wFileMask.setLayoutData(fdFileMask);

    wlExcludeFileMask = new Label(wFileComp, SWT.RIGHT);
    wlExcludeFileMask.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.ExcludeFileMask.Label"));
    PropsUi.setLook(wlExcludeFileMask);
    FormData fdlExcludeFileMask = new FormData();
    fdlExcludeFileMask.left = new FormAttachment(0, 0);
    fdlExcludeFileMask.top = new FormAttachment(wFileMask, margin);
    fdlExcludeFileMask.right = new FormAttachment(middle, -margin);
    wlExcludeFileMask.setLayoutData(fdlExcludeFileMask);
    wExcludeFileMask = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wExcludeFileMask);
    FormData fdExcludeFileMask = new FormData();
    fdExcludeFileMask.left = new FormAttachment(middle, 0);
    fdExcludeFileMask.top = new FormAttachment(wlExcludeFileMask, 0, SWT.CENTER);
    fdExcludeFileMask.right = new FormAttachment(wFilename, 0, SWT.RIGHT);
    wExcludeFileMask.setLayoutData(fdExcludeFileMask);

    // Filename list line
    wlFilenameList = new Label(wFileComp, SWT.RIGHT);
    wlFilenameList.setText(BaseMessages.getString(PKG, "PropertyInputDialog.FilenameList.Label"));
    PropsUi.setLook(wlFilenameList);
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment(0, 0);
    fdlFilenameList.top = new FormAttachment(wExcludeFileMask, margin);
    fdlFilenameList.right = new FormAttachment(middle, -margin);
    wlFilenameList.setLayoutData(fdlFilenameList);

    // Buttons to the right of the screen...
    wbdFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbdFilename);
    wbdFilename.setText(BaseMessages.getString(PKG, "PropertyInputDialog.FilenameRemove.Button"));
    wbdFilename.setToolTipText(
        BaseMessages.getString(PKG, "PropertyInputDialog.FilenameRemove.Tooltip"));
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment(100, 0);
    fdbdFilename.top = new FormAttachment(wExcludeFileMask, margin);
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbeFilename);
    wbeFilename.setText(BaseMessages.getString(PKG, "PropertyInputDialog.FilenameEdit.Button"));
    wbeFilename.setToolTipText(
        BaseMessages.getString(PKG, "PropertyInputDialog.FilenameEdit.Tooltip"));
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment(100, 0);
    fdbeFilename.left = new FormAttachment(wbdFilename, 0, SWT.LEFT);
    fdbeFilename.top = new FormAttachment(wbdFilename, margin);
    wbeFilename.setLayoutData(fdbeFilename);

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "PropertyInputDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.bottom = new FormAttachment(100, 0);
    wbShowFiles.setLayoutData(fdbShowFiles);

    ColumnInfo[] colinfo = new ColumnInfo[5];
    colinfo[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "PropertyInputDialog.Files.Filename.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "PropertyInputDialog.Files.Wildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "PropertyInputDialog.Files.ExcludeWildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "PropertyInputDialog.Required.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);
    colinfo[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "PropertyInputDialog.IncludeSubDirs.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);

    colinfo[0].setUsingVariables(true);
    colinfo[1].setUsingVariables(true);
    colinfo[1].setToolTip(
        BaseMessages.getString(PKG, "PropertyInputDialog.Files.Wildcard.Tooltip"));
    colinfo[2].setUsingVariables(true);
    colinfo[2].setToolTip(
        BaseMessages.getString(PKG, "PropertyInputDialog.Files.ExcludeWildcard.Tooltip"));
    colinfo[3].setToolTip(BaseMessages.getString(PKG, "PropertyInputDialog.Required.Tooltip"));
    colinfo[4].setToolTip(
        BaseMessages.getString(PKG, "PropertyInputDialog.IncludeSubDirs.Tooltip"));

    wFilenameList =
        new TableView(
            variables,
            wFileComp,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colinfo,
            2,
            null,
            props);
    PropsUi.setLook(wFilenameList);

    FormData fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment(middle, 0);
    fdFilenameList.right = new FormAttachment(wbdFilename, -margin);
    fdFilenameList.top = new FormAttachment(wExcludeFileMask, margin);
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
    wContentTab.setFont(GuiResource.getInstance().getFontDefault());
    wContentTab.setText(BaseMessages.getString(PKG, "PropertyInputDialog.Content.Tab"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    // ///////////////////////////////
    // START OF SettingsGroup GROUP //
    // ///////////////////////////////

    Group wSettingsGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettingsGroup);
    wSettingsGroup.setText(BaseMessages.getString(PKG, "PropertyInputDialog.SettingsGroup.Label"));

    FormLayout settingsGroupLayout = new FormLayout();
    settingsGroupLayout.marginWidth = 10;
    settingsGroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout(settingsGroupLayout);

    Label wlFileType = new Label(wSettingsGroup, SWT.RIGHT);
    wlFileType.setText(BaseMessages.getString(PKG, "PropertyInputDialog.FileType.Label"));
    PropsUi.setLook(wlFileType);
    FormData fdlFileType = new FormData();
    fdlFileType.left = new FormAttachment(0, 0);
    fdlFileType.top = new FormAttachment(0, margin);
    fdlFileType.right = new FormAttachment(middle, -margin);
    wlFileType.setLayoutData(fdlFileType);
    wFileType = new CCombo(wSettingsGroup, SWT.BORDER | SWT.READ_ONLY);
    wFileType.setEditable(true);
    wFileType.setItems(ResultFile.FileType.getDescriptions());
    PropsUi.setLook(wFileType);
    FormData fdFileType = new FormData();
    fdFileType.left = new FormAttachment(middle, 0);
    fdFileType.top = new FormAttachment(0, margin);
    fdFileType.right = new FormAttachment(100, 0);
    wFileType.setLayoutData(fdFileType);
    wFileType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setFileType();
          }
        });

    wlEncoding = new Label(wSettingsGroup, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "PropertyInputDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.top = new FormAttachment(wFileType, margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new ComboVar(variables, wSettingsGroup, SWT.BORDER | SWT.READ_ONLY);
    wEncoding.setEditable(true);
    PropsUi.setLook(wEncoding);
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.top = new FormAttachment(wFileType, margin);
    fdEncoding.right = new FormAttachment(100, 0);
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addListener(SWT.FocusIn, e -> setEncodings());

    wlSection = new Label(wSettingsGroup, SWT.RIGHT);
    wlSection.setText(BaseMessages.getString(PKG, "PropertyInputDialog.Section.Label"));
    PropsUi.setLook(wlSection);
    FormData fdlSection = new FormData();
    fdlSection.left = new FormAttachment(0, 0);
    fdlSection.top = new FormAttachment(wEncoding, margin);
    fdlSection.right = new FormAttachment(middle, -margin);
    wlSection.setLayoutData(fdlSection);

    wbbSection = new Button(wSettingsGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbSection);
    wbbSection.setText(BaseMessages.getString(PKG, "PropertyInputDialog.SectionBrowse.Button"));
    FormData fdbSection = new FormData();
    fdbSection.right = new FormAttachment(100, 0);
    fdbSection.top = new FormAttachment(wEncoding, margin);
    wbbSection.setLayoutData(fdbSection);
    wbbSection.addListener(SWT.Selection, e -> getSections());

    wSection = new TextVar(variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSection.setToolTipText(BaseMessages.getString(PKG, "PropertyInputDialog.Section.Tooltip"));
    PropsUi.setLook(wSection);
    FormData fdSection = new FormData();
    fdSection.left = new FormAttachment(middle, 0);
    fdSection.top = new FormAttachment(wEncoding, margin);
    fdSection.right = new FormAttachment(wbbSection, -margin);
    wSection.setLayoutData(fdSection);

    wlLimit = new Label(wSettingsGroup, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "PropertyInputDialog.Limit.Label"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.top = new FormAttachment(wbbSection, margin);
    fdlLimit.right = new FormAttachment(middle, -margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLimit);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.top = new FormAttachment(wbbSection, margin);
    fdLimit.right = new FormAttachment(100, 0);
    wLimit.setLayoutData(fdLimit);

    Label wlResolveValueVariable = new Label(wSettingsGroup, SWT.RIGHT);
    wlResolveValueVariable.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.resolveValueVariable.Label"));
    PropsUi.setLook(wlResolveValueVariable);
    FormData fdlResolveValueVariable = new FormData();
    fdlResolveValueVariable.left = new FormAttachment(0, 0);
    fdlResolveValueVariable.top = new FormAttachment(wLimit, margin);
    fdlResolveValueVariable.right = new FormAttachment(middle, -margin);
    wlResolveValueVariable.setLayoutData(fdlResolveValueVariable);
    wResolveValueVariable = new Button(wSettingsGroup, SWT.CHECK);
    PropsUi.setLook(wResolveValueVariable);
    wResolveValueVariable.setToolTipText(
        BaseMessages.getString(PKG, "PropertyInputDialog.resolveValueVariable.Tooltip"));
    FormData fdResolveValueVariable = new FormData();
    fdResolveValueVariable.left = new FormAttachment(middle, 0);
    fdResolveValueVariable.top = new FormAttachment(wlResolveValueVariable, 0, SWT.CENTER);
    wResolveValueVariable.setLayoutData(fdResolveValueVariable);
    wResolveValueVariable.addSelectionListener(new ComponentSelectionListener(input));

    FormData fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment(0, margin);
    fdSettingsGroup.top = new FormAttachment(wResolveValueVariable, margin);
    fdSettingsGroup.right = new FormAttachment(100, -margin);
    wSettingsGroup.setLayoutData(fdSettingsGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF SettingsGroup GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    Group wAdditionalGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAdditionalGroup);
    wAdditionalGroup.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.Group.AdditionalGroup.Label"));

    FormLayout additionalGroupLayout = new FormLayout();
    additionalGroupLayout.marginWidth = 10;
    additionalGroupLayout.marginHeight = 10;
    wAdditionalGroup.setLayout(additionalGroupLayout);

    wlInclFilename = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclFilename.setText(BaseMessages.getString(PKG, "PropertyInputDialog.InclFilename.Label"));
    PropsUi.setLook(wlInclFilename);
    FormData fdlInclFilename = new FormData();
    fdlInclFilename.left = new FormAttachment(0, 0);
    fdlInclFilename.top = new FormAttachment(wSettingsGroup, margin);
    fdlInclFilename.right = new FormAttachment(middle, -margin);
    wlInclFilename.setLayoutData(fdlInclFilename);
    wInclFilename = new Button(wAdditionalGroup, SWT.CHECK);
    PropsUi.setLook(wInclFilename);
    wInclFilename.setToolTipText(
        BaseMessages.getString(PKG, "PropertyInputDialog.InclFilename.Tooltip"));
    FormData fdInclFilename = new FormData();
    fdInclFilename.left = new FormAttachment(middle, 0);
    fdInclFilename.top = new FormAttachment(wlInclFilename, 0, SWT.CENTER);
    wInclFilename.setLayoutData(fdInclFilename);
    wInclFilename.addSelectionListener(new ComponentSelectionListener(input));

    wlInclFilenameField = new Label(wAdditionalGroup, SWT.LEFT);
    wlInclFilenameField.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.InclFilenameField.Label"));
    PropsUi.setLook(wlInclFilenameField);
    FormData fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment(wInclFilename, margin);
    fdlInclFilenameField.top = new FormAttachment(wSettingsGroup, margin);
    wlInclFilenameField.setLayoutData(fdlInclFilenameField);
    wInclFilenameField =
        new TextVar(variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclFilenameField);
    FormData fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment(wlInclFilenameField, margin);
    fdInclFilenameField.top = new FormAttachment(wSettingsGroup, margin);
    fdInclFilenameField.right = new FormAttachment(100, 0);
    wInclFilenameField.setLayoutData(fdInclFilenameField);

    Label wlInclRowNum = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclRowNum.setText(BaseMessages.getString(PKG, "PropertyInputDialog.InclRowNum.Label"));
    PropsUi.setLook(wlInclRowNum);
    FormData fdlInclRowNum = new FormData();
    fdlInclRowNum.left = new FormAttachment(0, 0);
    fdlInclRowNum.top = new FormAttachment(wInclFilenameField, margin);
    fdlInclRowNum.right = new FormAttachment(middle, -margin);
    wlInclRowNum.setLayoutData(fdlInclRowNum);
    wInclRowNum = new Button(wAdditionalGroup, SWT.CHECK);
    PropsUi.setLook(wInclRowNum);
    wInclRowNum.setToolTipText(
        BaseMessages.getString(PKG, "PropertyInputDialog.InclRowNum.Tooltip"));
    FormData fdRowNum = new FormData();
    fdRowNum.left = new FormAttachment(middle, 0);
    fdRowNum.top = new FormAttachment(wlInclRowNum, 0, SWT.CENTER);
    wInclRowNum.setLayoutData(fdRowNum);
    wInclRowNum.addSelectionListener(new ComponentSelectionListener(input));

    wlInclRowNumField = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclRowNumField.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.InclRowNumField.Label"));
    PropsUi.setLook(wlInclRowNumField);
    FormData fdlInclRowNumField = new FormData();
    fdlInclRowNumField.left = new FormAttachment(wInclRowNum, margin);
    fdlInclRowNumField.top = new FormAttachment(wInclFilenameField, margin);
    wlInclRowNumField.setLayoutData(fdlInclRowNumField);
    wInclRowNumField = new TextVar(variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclRowNumField);
    FormData fdInclRowNumField = new FormData();
    fdInclRowNumField.left = new FormAttachment(wlInclRowNumField, margin);
    fdInclRowNumField.top = new FormAttachment(wInclFilenameField, margin);
    fdInclRowNumField.right = new FormAttachment(100, 0);
    wInclRowNumField.setLayoutData(fdInclRowNumField);

    wResetRowNum = new Button(wAdditionalGroup, SWT.CHECK);
    PropsUi.setLook(wResetRowNum);
    wResetRowNum.setText(BaseMessages.getString(PKG, "PropertyInputDialog.ResetRowNum.Label"));
    wResetRowNum.setToolTipText(
        BaseMessages.getString(PKG, "PropertyInputDialog.ResetRowNum.Tooltip"));
    fdRowNum = new FormData();
    fdRowNum.left = new FormAttachment(wlInclRowNum, margin);
    fdRowNum.top = new FormAttachment(wInclRowNumField, margin);
    wResetRowNum.setLayoutData(fdRowNum);
    wResetRowNum.addSelectionListener(new ComponentSelectionListener(input));

    wlInclIniSection = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclIniSection.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.InclIniSection.Label"));
    PropsUi.setLook(wlInclIniSection);
    FormData fdlInclIniSection = new FormData();
    fdlInclIniSection.left = new FormAttachment(0, 0);
    fdlInclIniSection.top = new FormAttachment(wResetRowNum, margin);
    fdlInclIniSection.right = new FormAttachment(middle, -margin);
    wlInclIniSection.setLayoutData(fdlInclIniSection);
    wInclIniSection = new Button(wAdditionalGroup, SWT.CHECK);
    PropsUi.setLook(wInclIniSection);
    wInclIniSection.setToolTipText(
        BaseMessages.getString(PKG, "PropertyInputDialog.InclIniSection.Tooltip"));
    fdRowNum = new FormData();
    fdRowNum.left = new FormAttachment(middle, 0);
    fdRowNum.top = new FormAttachment(wlInclIniSection, 0, SWT.CENTER);
    wInclIniSection.setLayoutData(fdRowNum);
    wInclIniSection.addSelectionListener(new ComponentSelectionListener(input));

    wlInclIniSectionField = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclIniSectionField.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.InclIniSectionField.Label"));
    PropsUi.setLook(wlInclIniSectionField);
    FormData fdlInclIniSectionField = new FormData();
    fdlInclIniSectionField.left = new FormAttachment(wInclIniSection, margin);
    fdlInclIniSectionField.top = new FormAttachment(wResetRowNum, margin);
    wlInclIniSectionField.setLayoutData(fdlInclIniSectionField);
    wInclIniSectionField =
        new TextVar(variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclIniSectionField);
    FormData fdInclIniSectionField = new FormData();
    fdInclIniSectionField.left = new FormAttachment(wlInclIniSectionField, margin);
    fdInclIniSectionField.top = new FormAttachment(wResetRowNum, margin);
    fdInclIniSectionField.right = new FormAttachment(100, 0);
    wInclIniSectionField.setLayoutData(fdInclIniSectionField);

    FormData fdAdditionalGroup = new FormData();
    fdAdditionalGroup.left = new FormAttachment(0, margin);
    fdAdditionalGroup.top = new FormAttachment(wSettingsGroup, margin);
    fdAdditionalGroup.right = new FormAttachment(100, -margin);
    wAdditionalGroup.setLayoutData(fdAdditionalGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION ADDRESS GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    Group wAddFileResult = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAddFileResult);
    wAddFileResult.setText(BaseMessages.getString(PKG, "PropertyInputDialog.wAddFileResult.Label"));

    FormLayout addFileResultGroupLayout = new FormLayout();
    addFileResultGroupLayout.marginWidth = 10;
    addFileResultGroupLayout.marginHeight = 10;
    wAddFileResult.setLayout(addFileResultGroupLayout);

    Label wlAddResult = new Label(wAddFileResult, SWT.RIGHT);
    wlAddResult.setText(BaseMessages.getString(PKG, "PropertyInputDialog.AddResult.Label"));
    PropsUi.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment(0, 0);
    fdlAddResult.top = new FormAttachment(wAdditionalGroup, margin);
    fdlAddResult.right = new FormAttachment(middle, -margin);
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wAddFileResult, SWT.CHECK);
    PropsUi.setLook(wAddResult);
    wAddResult.setToolTipText(BaseMessages.getString(PKG, "PropertyInputDialog.AddResult.Tooltip"));
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(middle, 0);
    fdAddResult.top = new FormAttachment(wlAddResult, 0, SWT.CENTER);
    wAddResult.setLayoutData(fdAddResult);
    wAddResult.addSelectionListener(new ComponentSelectionListener(input));

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment(0, margin);
    fdAddFileResult.top = new FormAttachment(wAdditionalGroup, margin);
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
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "PropertyInputDialog.Fields.Tab"));

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout(fieldsLayout);
    PropsUi.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "PropertyInputDialog.GetFields.Button"));
    fdGet = new FormData();
    fdGet.left = new FormAttachment(50, 0);
    fdGet.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);

    final int FieldsRows = input.getInputFields().size();

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Name.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Attribute.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              PropertyInputMeta.KeyValue.getCodes(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Type.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Format.Column"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              3),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Currency.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Decimal.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Group.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.TrimType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              IValueMeta.TrimType.getDescriptions(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Repeat.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO)
              },
              true),
        };

    colinf[0].setUsingVariables(true);
    colinf[0].setToolTip(
        BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Name.Column.Tooltip"));
    colinf[1].setToolTip(
        BaseMessages.getString(PKG, "PropertyInputDialog.FieldsTable.Attribute.Column.Tooltip"));
    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            null,
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
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, -50);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    wGet.addListener(SWT.Selection, e -> get());

    // Add the file to the list of files...
    Listener selA =
        e -> {
          wFilenameList.add(
              wFilename.getText(),
              wFileMask.getText(),
              wExcludeFileMask.getText(),
              PropertyInputMeta.RequiredFilesCode[0],
              PropertyInputMeta.RequiredFilesCode[0]);
          wFilename.setText("");
          wFileMask.setText("");
          wExcludeFileMask.setText("");
          wFilenameList.removeEmptyRows();
          wFilenameList.setRowNums();
          wFilenameList.optWidth(true);
        };
    wbaFilename.addListener(SWT.Selection, selA);
    wFilename.addListener(SWT.Selection, selA);

    // Delete files from the list of files...
    wbdFilename.addListener(
        SWT.Selection,
        e -> {
          int[] idx = wFilenameList.getSelectionIndices();
          wFilenameList.remove(idx);
          wFilenameList.removeEmptyRows();
          wFilenameList.setRowNums();
        });

    // Edit the selected file & remove from the list...
    wbeFilename.addListener(
        SWT.Selection,
        e -> {
          int idx = wFilenameList.getSelectionIndex();
          if (idx >= 0) {
            String[] string = wFilenameList.getItem(idx);
            wFilename.setText(string[0]);
            wFileMask.setText(string[1]);
            wExcludeFileMask.setText(string[2]);
            wFilenameList.remove(idx);
          }
          wFilenameList.removeEmptyRows();
          wFilenameList.setRowNums();
        });

    // Show the files that are selected at this time...
    wbShowFiles.addListener(SWT.Selection, e -> showInputFiles());

    // Enable/disable the right fields to allow a filename to be added to each row...
    wInclFilename.addListener(SWT.Selection, e -> setIncludeFilename());
    // Enable/disable the right fields to allow a filename to be added to each row...
    wInclIniSection.addListener(SWT.Selection, e -> setIncludeSection());

    // Enable/disable the right fields to allow a row number to be added to each row...
    wInclRowNum.addListener(SWT.Selection, e -> setIncludeRowNum());

    // Whenever something changes, set the tooltip to the expanded version of the filename:
    wFilename.addModifyListener(e -> wFilename.setToolTipText(""));

    // Listen to the Browse... button
    wbbFilename.addListener(SWT.Selection, e -> browseFiles());

    wTabFolder.setSelection(0);

    getData(input);
    setFileType();
    input.setChanged(changed);
    activateFileField();
    wFields.optWidth(true);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void browseFiles() {
    final PropertyInputMeta.FileType fileType =
        PropertyInputMeta.FileType.lookupDescription(wFileType.getText());

    BaseDialog.presentFileDialog(
        shell,
        wFilename,
        variables,
        fileType == PropertyInputMeta.FileType.PROPERTY
            ? new String[] {"*.properties;*.PROPERTIES", "*"}
            : new String[] {"*.ini;*.INI", "*"},
        fileType == PropertyInputMeta.FileType.PROPERTY
            ? new String[] {
              BaseMessages.getString(PKG, "PropertyInputDialog.FileType.PropertiesFiles"),
              BaseMessages.getString(PKG, "System.FileType.AllFiles")
            }
            : new String[] {
              BaseMessages.getString(PKG, "PropertyInputDialog.FileType.IniFiles"),
              BaseMessages.getString(PKG, "System.FileType.AllFiles")
            },
        true);
  }

  private void showInputFiles() {
    try {
      PropertyInputMeta tfii = new PropertyInputMeta();
      getInfo(tfii);
      FileInputList fileInputList = tfii.getFiles(variables);
      String[] files = fileInputList.getFileStrings();

      if (files.length > 0) {
        EnterSelectionDialog esd =
            new EnterSelectionDialog(
                shell,
                files,
                BaseMessages.getString(PKG, "PropertyInputDialog.FilesReadSelection.DialogTitle"),
                BaseMessages.getString(
                    PKG, "PropertyInputDialog.FilesReadSelection.DialogMessage"));
        esd.setViewOnly();
        esd.open();
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(BaseMessages.getString(PKG, "PropertyInputDialog.NoFileFound.DialogMessage"));
        mb.setText(BaseMessages.getString(PKG, CONST_SYSTEM_DIALOG_ERROR_TITLE));
        mb.open();
      }
    } catch (Exception ex) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "PropertyInputDialog.ErrorParsingData.DialogTitle"),
          BaseMessages.getString(PKG, "PropertyInputDialog.ErrorParsingData.DialogMessage"),
          ex);
    }
  }

  private void setFileType() {
    PropertyInputMeta.FileType fileType =
        PropertyInputMeta.FileType.lookupDescription(wFileType.getText());
    boolean active = fileType == PropertyInputMeta.FileType.INI;
    wlSection.setEnabled(active);
    wSection.setEnabled(active);
    wbbSection.setEnabled(active);
    wlEncoding.setEnabled(active);
    wEncoding.setEnabled(active);
    if (!active && wInclIniSection.getSelection()) {
      wInclIniSection.setSelection(false);
    }
    wlInclIniSection.setEnabled(active);
    wInclIniSection.setEnabled(active);
    setIncludeSection();
  }

  private void setEncodings() {
    // Encoding of the text file:
    if (!gotEncodings) {
      gotEncodings = true;

      wEncoding.removeAll();
      ArrayList<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
      }

      // Now select the default!
      String defEncoding =
          Const.getEnvironmentVariable("file.encoding", PropertyInputMeta.DEFAULT_ENCODING);
      int idx = Const.indexOfString(defEncoding, wEncoding.getItems());
      if (idx >= 0) {
        wEncoding.select(idx);
      }
    }
  }

  private void setFileField() {
    try {
      String value = wFilenameField.getText();
      wFilenameField.removeAll();
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        r.getFieldNames();

        for (int i = 0; i < r.getFieldNames().length; i++) {
          wFilenameField.add(r.getFieldNames()[i]);
        }
      }
      if (value != null) {
        wFilenameField.setText(value);
      }

    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "PropertyInputDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "PropertyInputDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void get() {
    if (!gotPreviousFields) {
      gotPreviousFields = true;
      IRowMeta fields = new RowMeta();

      try {
        PropertyInputMeta meta = new PropertyInputMeta();
        getInfo(meta);

        FileInputList inputList = meta.getFiles(variables);

        if (!inputList.getFiles().isEmpty()) {
          IValueMeta field =
              new ValueMetaString(BaseMessages.getString(PKG, "PropertyInputField.Column.Key"));
          fields.addValueMeta(field);
          field =
              new ValueMetaString(BaseMessages.getString(PKG, "PropertyInputField.Column.Value"));
          fields.addValueMeta(field);
        }

      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, CONST_SYSTEM_DIALOG_ERROR_TITLE),
            BaseMessages.getString(
                PKG, "PropertyInputDialog.ErrorReadingFile.DialogMessage", e.toString()),
            e);
      }

      if (!fields.isEmpty()) {

        // Clear Fields Grid
        wFields.removeAll();

        for (int j = 0; j < fields.size(); j++) {
          IValueMeta field = fields.getValueMeta(j);
          wFields.add(
              field.getName(),
              field.getName(),
              field.getTypeDesc(),
              "",
              "-1",
              "",
              "",
              "",
              "",
              "none",
              "N");
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth(true);
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        mb.setMessage(
            BaseMessages.getString(PKG, "PropertyInputDialog.UnableToFindFields.DialogTitle"));
        mb.setText(
            BaseMessages.getString(PKG, "PropertyInputDialog.UnableToFindFields.DialogMessage"));
        mb.open();
      }
    }
  }

  private void activateFileField() {
    wlFilenameField.setEnabled(wFileField.getSelection());
    wFilenameField.setEnabled(wFileField.getSelection());

    wlFilename.setEnabled(!wFileField.getSelection());
    wbbFilename.setEnabled(!wFileField.getSelection());
    wbaFilename.setEnabled(!wFileField.getSelection());
    wFilename.setEnabled(!wFileField.getSelection());
    wlFileMask.setEnabled(!wFileField.getSelection());
    wFileMask.setEnabled(!wFileField.getSelection());
    wlExcludeFileMask.setEnabled(!wFileField.getSelection());
    wExcludeFileMask.setEnabled(!wFileField.getSelection());
    wlFilenameList.setEnabled(!wFileField.getSelection());
    wbdFilename.setEnabled(!wFileField.getSelection());
    wbeFilename.setEnabled(!wFileField.getSelection());
    wbShowFiles.setEnabled(!wFileField.getSelection());
    wlFilenameList.setEnabled(!wFileField.getSelection());
    wFilenameList.setEnabled(!wFileField.getSelection());
    if (wFileField.getSelection()) {
      wInclFilename.setSelection(false);
    }
    wInclFilename.setEnabled(!wFileField.getSelection());
    wlInclFilename.setEnabled(!wFileField.getSelection());
    wlLimit.setEnabled(!wFileField.getSelection());
    wLimit.setEnabled(!wFileField.getSelection());
    wPreview.setEnabled(!wFileField.getSelection());
    wGet.setEnabled(!wFileField.getSelection());
  }

  public void setIncludeFilename() {
    wlInclFilenameField.setEnabled(wInclFilename.getSelection());
    wInclFilenameField.setEnabled(wInclFilename.getSelection());
  }

  public void setIncludeSection() {
    boolean active =
        PropertyInputMeta.FileType.lookupDescription(wFileType.getText())
            == PropertyInputMeta.FileType.INI;
    wlInclIniSectionField.setEnabled(active && wInclIniSection.getSelection());
    wInclIniSectionField.setEnabled(active && wInclIniSection.getSelection());
  }

  public void setIncludeRowNum() {
    wlInclRowNumField.setEnabled(wInclRowNum.getSelection());
    wInclRowNumField.setEnabled(wInclRowNum.getSelection());
    wResetRowNum.setEnabled(wInclRowNum.getSelection());
  }

  /**
   * Read the data from the TextFileInputMeta object and show it in this dialog.
   *
   * @param in The TextFileInputMeta object to obtain the data from.
   */
  public void getData(PropertyInputMeta in) {

    wFilenameList.removeAll();

    for (int i = 0; i < in.getFiles().size(); i++) {
      PropertyInputMeta.PIFile file = in.getFiles().get(i);
      wFilenameList.add(
          file.getName(),
          file.getMask(),
          file.getExcludeMask(),
          file.isRequired() ? "Y" : "N",
          file.isIncludingSubFolders() ? "Y" : "N");
    }
    wFilenameList.optimizeTableView();

    if (in.getFileType() != null) {
      wFileType.setText(in.getFileType().getDescription());
    }
    wInclFilename.setSelection(in.isIncludingFilename());
    wInclRowNum.setSelection(in.isIncludeRowNumber());
    wInclIniSection.setSelection(in.isIncludeIniSection());
    wAddResult.setSelection(in.isAddResult());
    wResolveValueVariable.setSelection(in.isResolvingValueVariable());
    wFileField.setSelection(in.isFileField());
    wEncoding.setText(Const.NVL(in.getEncoding(), ""));
    wInclFilenameField.setText(Const.NVL(in.getFilenameField(), ""));
    wFilenameField.setText(Const.NVL(in.getDynamicFilenameField(), ""));
    wInclIniSectionField.setText(Const.NVL(in.getIniSectionField(), ""));
    wSection.setText(Const.NVL(in.getSection(), ""));
    wInclRowNumField.setText(Const.NVL(in.getRowNumberField(), ""));
    wResetRowNum.setSelection(in.isResettingRowNumber());
    wLimit.setText("" + in.getRowLimit());

    if (log.isDebug()) {
      log.logDebug(BaseMessages.getString(PKG, "PropertyInputDialog.Log.GettingFieldsInfo"));
    }
    for (int i = 0; i < in.getInputFields().size(); i++) {
      PropertyInputMeta.PIField field = in.getInputFields().get(i);

      if (field != null) {
        TableItem item = wFields.table.getItem(i);
        String name = field.getName();
        String column = field.getColumn().getCode();
        String type = field.getType();
        String format = field.getFormat();
        String length = "" + field.getLength();
        String prec = "" + field.getPrecision();
        String curr = field.getCurrency();
        String group = field.getGroup();
        String decim = field.getDecimal();
        String trim = field.getTrimType().getDescription();
        String rep =
            field.isRepeating()
                ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
                : BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO);

        item.setText(1, Const.NVL(name, ""));
        item.setText(2, Const.NVL(column, ""));
        item.setText(3, Const.NVL(type, ""));
        item.setText(4, Const.NVL(format, ""));
        if (!"-1".equals(length)) {
          item.setText(5, length);
        }
        if (!"-1".equals(prec)) {
          item.setText(6, prec);
        }
        item.setText(7, Const.NVL(curr, ""));
        item.setText(8, Const.NVL(decim, ""));
        item.setText(9, Const.NVL(group, ""));
        item.setText(10, Const.NVL(trim, ""));
        item.setText(11, Const.NVL(rep, ""));
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
    if (!StringUtils.isEmpty(in.getShortFileFieldName())) {
      wShortFileFieldName.setText(in.getShortFileFieldName());
    }
    if (!StringUtils.isEmpty(in.getPathFieldName())) {
      wPathFieldName.setText(in.getPathFieldName());
    }
    if (!StringUtils.isEmpty(in.getHiddenFieldName())) {
      wIsHiddenName.setText(in.getHiddenFieldName());
    }
    if (!StringUtils.isEmpty(in.getLastModificationTimeFieldName())) {
      wLastModificationTimeName.setText(in.getLastModificationTimeFieldName());
    }
    if (!StringUtils.isEmpty(in.getUriNameFieldName())) {
      wUriName.setText(in.getUriNameFieldName());
    }
    if (!StringUtils.isEmpty(in.getRootUriNameFieldName())) {
      wRootUriName.setText(in.getRootUriNameFieldName());
    }
    if (!StringUtils.isEmpty(in.getExtensionFieldName())) {
      wExtensionFieldName.setText(in.getExtensionFieldName());
    }
    if (!StringUtils.isEmpty(in.getSizeFieldName())) {
      wSizeFieldName.setText(in.getSizeFieldName());
    }

    setIncludeFilename();
    setIncludeRowNum();
    setIncludeSection();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    try {
      getInfo(input);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "PropertyInputDialog.ErrorParsingData.DialogTitle"),
          BaseMessages.getString(PKG, "PropertyInputDialog.ErrorParsingData.DialogMessage"),
          e);
    }
    input.setChanged();
    dispose();
  }

  private void getInfo(PropertyInputMeta in) {
    transformName = wTransformName.getText(); // return value

    // copy info to PropertyInputMeta class (input)
    in.setRowLimit(Const.toLong(wLimit.getText(), 0L));
    in.setIncludingFilename(wInclFilename.getSelection());
    in.setFilenameField(wInclFilenameField.getText());
    in.setIncludeRowNumber(wInclRowNum.getSelection());
    in.setIncludeIniSection(wInclIniSection.getSelection());
    in.setAddResult(wAddResult.getSelection());
    in.setEncoding(wEncoding.getText());
    in.setDynamicFilenameField(wFilenameField.getText());
    in.setFileField(wFileField.getSelection());
    in.setRowNumberField(wInclRowNumField.getText());
    in.setIniSectionField(wInclIniSectionField.getText());
    in.setResettingRowNumber(wResetRowNum.getSelection());
    in.setResolvingValueVariable(wResolveValueVariable.getSelection());

    in.getFiles().clear();
    for (TableItem item : wFilenameList.getNonEmptyItems()) {
      PropertyInputMeta.PIFile file = new PropertyInputMeta.PIFile();
      in.getFiles().add(file);

      file.setName(item.getText(1));
      file.setMask(item.getText(2));
      file.setExcludeMask(item.getText(3));
      file.setRequired(Const.toBoolean(item.getText(4)));
      file.setIncludingSubFolders(Const.toBoolean(item.getText(5)));
    }

    in.setSection(wSection.getText());

    in.setFileType(PropertyInputMeta.FileType.lookupDescription(wFileType.getText()));

    in.getInputFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      PropertyInputMeta.PIField field = new PropertyInputMeta.PIField();
      in.getInputFields().add(field);

      field.setName(item.getText(1));
      field.setColumn(PropertyInputMeta.KeyValue.lookupCode(item.getText(2)));
      field.setType(item.getText(3));
      field.setFormat(item.getText(4));
      field.setLength(Const.toInt(item.getText(5), -1));
      field.setPrecision(Const.toInt(item.getText(6), -1));
      field.setCurrency(item.getText(7));
      field.setDecimal(item.getText(8));
      field.setGroup(item.getText(9));
      field.setTrimType(IValueMeta.TrimType.lookupDescription(item.getText(10)));
      field.setRepeating(
          BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES).equalsIgnoreCase(item.getText(11)));
    }
    in.setShortFileFieldName(wShortFileFieldName.getText());
    in.setPathFieldName(wPathFieldName.getText());
    in.setHiddenFieldName(wIsHiddenName.getText());
    in.setLastModificationTimeFieldName(wLastModificationTimeName.getText());
    in.setUriNameFieldName(wUriName.getText());
    in.setRootUriNameFieldName(wRootUriName.getText());
    in.setExtensionFieldName(wExtensionFieldName.getText());
    in.setSizeFieldName(wSizeFieldName.getText());
  }

  // Preview the data
  private void preview() {
    try {

      PropertyInputMeta oneMeta = new PropertyInputMeta();
      getInfo(oneMeta);

      EnterNumberDialog numberDialog =
          new EnterNumberDialog(
              shell,
              props.getDefaultPreviewSize(),
              BaseMessages.getString(PKG, "PropertyInputDialog.NumberRows.DialogTitle"),
              BaseMessages.getString(PKG, "PropertyInputDialog.NumberRows.DialogMessage"));

      int previewSize = numberDialog.open();
      if (previewSize > 0) {
        oneMeta.setRowLimit(previewSize);
        PipelineMeta previewMeta =
            PipelinePreviewFactory.generatePreviewPipeline(
                pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

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
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "PropertyInputDialog.ErrorPreviewingData.DialogTitle"),
          BaseMessages.getString(PKG, "PropertyInputDialog.ErrorPreviewingData.DialogMessage"),
          e);
    }
  }

  private void getSections() {
    Wini wini = new Wini();
    PropertyInputMeta meta = new PropertyInputMeta();
    try {
      getInfo(meta);

      FileInputList fileInputList = meta.getFiles(variables);

      if (fileInputList.nrOfFiles() > 0) {
        // Check the first file
        if (fileInputList.getFile(0).exists()) {
          // Open the file (only first file) in readOnly ...
          //
          wini = new Wini(HopVfs.getInputStream(fileInputList.getFile(0)));
          Iterator<String> itSection = wini.keySet().iterator();
          String[] sectionsList = new String[wini.keySet().size()];
          int i = 0;
          while (itSection.hasNext()) {
            sectionsList[i] = itSection.next();
            i++;
          }
          Const.sortStrings(sectionsList);
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  sectionsList,
                  BaseMessages.getString(PKG, "PropertyInputDialog.Dialog.SelectASection.Title"),
                  BaseMessages.getString(PKG, "PropertyInputDialog.Dialog.SelectASection.Message"));
          String sectionname = dialog.open();
          if (sectionname != null) {
            wSection.setText(sectionname);
          }
        } else {
          // The file not exists !
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "PropertyInputDialog.Exception.FileDoesNotExist",
                  HopVfs.getFilename(fileInputList.getFile(0))));
        }
      } else {
        // No file specified
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "PropertyInputDialog.FilesMissing.DialogMessage"));
        mb.setText(BaseMessages.getString(PKG, CONST_SYSTEM_DIALOG_ERROR_TITLE));
        mb.open();
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "PropertyInputDialog.UnableToGetListOfSections.Title"),
          BaseMessages.getString(PKG, "PropertyInputDialog.UnableToGetListOfSections.Message"),
          e);
    } finally {
      wini.clear();
    }
  }

  private void addAdditionalFieldsTab() {
    // ////////////////////////
    // START OF ADDITIONAL FIELDS TAB ///
    // ////////////////////////
    CTabItem wAdditionalFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdditionalFieldsTab.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.AdditionalFieldsTab.TabTitle"));

    Composite wAdditionalFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdditionalFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wAdditionalFieldsComp.setLayout(fieldsLayout);

    // ShortFileFieldName line
    Label wlShortFileFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlShortFileFieldName.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.ShortFileFieldName.Label"));
    PropsUi.setLook(wlShortFileFieldName);
    FormData fdlShortFileFieldName = new FormData();
    fdlShortFileFieldName.left = new FormAttachment(0, 0);
    fdlShortFileFieldName.top = new FormAttachment(wInclRowNumField, margin);
    fdlShortFileFieldName.right = new FormAttachment(middle, -margin);
    wlShortFileFieldName.setLayoutData(fdlShortFileFieldName);

    wShortFileFieldName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wShortFileFieldName);
    FormData fdShortFileFieldName = new FormData();
    fdShortFileFieldName.left = new FormAttachment(middle, 0);
    fdShortFileFieldName.right = new FormAttachment(100, -margin);
    fdShortFileFieldName.top = new FormAttachment(wInclRowNumField, margin);
    wShortFileFieldName.setLayoutData(fdShortFileFieldName);

    // ExtensionFieldName line
    Label wlExtensionFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlExtensionFieldName.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.ExtensionFieldName.Label"));
    PropsUi.setLook(wlExtensionFieldName);
    FormData fdlExtensionFieldName = new FormData();
    fdlExtensionFieldName.left = new FormAttachment(0, 0);
    fdlExtensionFieldName.top = new FormAttachment(wShortFileFieldName, margin);
    fdlExtensionFieldName.right = new FormAttachment(middle, -margin);
    wlExtensionFieldName.setLayoutData(fdlExtensionFieldName);

    wExtensionFieldName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wExtensionFieldName);
    FormData fdExtensionFieldName = new FormData();
    fdExtensionFieldName.left = new FormAttachment(middle, 0);
    fdExtensionFieldName.right = new FormAttachment(100, -margin);
    fdExtensionFieldName.top = new FormAttachment(wShortFileFieldName, margin);
    wExtensionFieldName.setLayoutData(fdExtensionFieldName);

    // PathFieldName line
    Label wlPathFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlPathFieldName.setText(BaseMessages.getString(PKG, "PropertyInputDialog.PathFieldName.Label"));
    PropsUi.setLook(wlPathFieldName);
    FormData fdlPathFieldName = new FormData();
    fdlPathFieldName.left = new FormAttachment(0, 0);
    fdlPathFieldName.top = new FormAttachment(wExtensionFieldName, margin);
    fdlPathFieldName.right = new FormAttachment(middle, -margin);
    wlPathFieldName.setLayoutData(fdlPathFieldName);

    wPathFieldName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPathFieldName);
    FormData fdPathFieldName = new FormData();
    fdPathFieldName.left = new FormAttachment(middle, 0);
    fdPathFieldName.right = new FormAttachment(100, -margin);
    fdPathFieldName.top = new FormAttachment(wExtensionFieldName, margin);
    wPathFieldName.setLayoutData(fdPathFieldName);

    // SizeFieldName line
    Label wlSizeFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlSizeFieldName.setText(BaseMessages.getString(PKG, "PropertyInputDialog.SizeFieldName.Label"));
    PropsUi.setLook(wlSizeFieldName);
    FormData fdlSizeFieldName = new FormData();
    fdlSizeFieldName.left = new FormAttachment(0, 0);
    fdlSizeFieldName.top = new FormAttachment(wPathFieldName, margin);
    fdlSizeFieldName.right = new FormAttachment(middle, -margin);
    wlSizeFieldName.setLayoutData(fdlSizeFieldName);

    wSizeFieldName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSizeFieldName);
    FormData fdSizeFieldName = new FormData();
    fdSizeFieldName.left = new FormAttachment(middle, 0);
    fdSizeFieldName.right = new FormAttachment(100, -margin);
    fdSizeFieldName.top = new FormAttachment(wPathFieldName, margin);
    wSizeFieldName.setLayoutData(fdSizeFieldName);

    // IsHiddenName line
    Label wlIsHiddenName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlIsHiddenName.setText(BaseMessages.getString(PKG, "PropertyInputDialog.IsHiddenName.Label"));
    PropsUi.setLook(wlIsHiddenName);
    FormData fdlIsHiddenName = new FormData();
    fdlIsHiddenName.left = new FormAttachment(0, 0);
    fdlIsHiddenName.top = new FormAttachment(wSizeFieldName, margin);
    fdlIsHiddenName.right = new FormAttachment(middle, -margin);
    wlIsHiddenName.setLayoutData(fdlIsHiddenName);

    wIsHiddenName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wIsHiddenName);
    FormData fdIsHiddenName = new FormData();
    fdIsHiddenName.left = new FormAttachment(middle, 0);
    fdIsHiddenName.right = new FormAttachment(100, -margin);
    fdIsHiddenName.top = new FormAttachment(wSizeFieldName, margin);
    wIsHiddenName.setLayoutData(fdIsHiddenName);

    // LastModificationTimeName line
    Label wlLastModificationTimeName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlLastModificationTimeName.setText(
        BaseMessages.getString(PKG, "PropertyInputDialog.LastModificationTimeName.Label"));
    PropsUi.setLook(wlLastModificationTimeName);
    FormData fdlLastModificationTimeName = new FormData();
    fdlLastModificationTimeName.left = new FormAttachment(0, 0);
    fdlLastModificationTimeName.top = new FormAttachment(wIsHiddenName, margin);
    fdlLastModificationTimeName.right = new FormAttachment(middle, -margin);
    wlLastModificationTimeName.setLayoutData(fdlLastModificationTimeName);

    wLastModificationTimeName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLastModificationTimeName);
    FormData fdLastModificationTimeName = new FormData();
    fdLastModificationTimeName.left = new FormAttachment(middle, 0);
    fdLastModificationTimeName.right = new FormAttachment(100, -margin);
    fdLastModificationTimeName.top = new FormAttachment(wIsHiddenName, margin);
    wLastModificationTimeName.setLayoutData(fdLastModificationTimeName);

    // UriName line
    Label wlUriName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlUriName.setText(BaseMessages.getString(PKG, "PropertyInputDialog.UriName.Label"));
    PropsUi.setLook(wlUriName);
    FormData fdlUriName = new FormData();
    fdlUriName.left = new FormAttachment(0, 0);
    fdlUriName.top = new FormAttachment(wLastModificationTimeName, margin);
    fdlUriName.right = new FormAttachment(middle, -margin);
    wlUriName.setLayoutData(fdlUriName);

    wUriName = new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUriName);
    FormData fdUriName = new FormData();
    fdUriName.left = new FormAttachment(middle, 0);
    fdUriName.right = new FormAttachment(100, -margin);
    fdUriName.top = new FormAttachment(wLastModificationTimeName, margin);
    wUriName.setLayoutData(fdUriName);

    // RootUriName line
    Label wlRootUriName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlRootUriName.setText(BaseMessages.getString(PKG, "PropertyInputDialog.RootUriName.Label"));
    PropsUi.setLook(wlRootUriName);
    FormData fdlRootUriName = new FormData();
    fdlRootUriName.left = new FormAttachment(0, 0);
    fdlRootUriName.top = new FormAttachment(wUriName, margin);
    fdlRootUriName.right = new FormAttachment(middle, -margin);
    wlRootUriName.setLayoutData(fdlRootUriName);

    wRootUriName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRootUriName);
    FormData fdRootUriName = new FormData();
    fdRootUriName.left = new FormAttachment(middle, 0);
    fdRootUriName.right = new FormAttachment(100, -margin);
    fdRootUriName.top = new FormAttachment(wUriName, margin);
    wRootUriName.setLayoutData(fdRootUriName);

    FormData fdAdditionalFieldsComp = new FormData();
    fdAdditionalFieldsComp.left = new FormAttachment(0, 0);
    fdAdditionalFieldsComp.top = new FormAttachment(wSpacer, margin);
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
