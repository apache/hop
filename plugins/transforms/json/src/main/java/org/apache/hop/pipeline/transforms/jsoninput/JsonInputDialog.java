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

package org.apache.hop.pipeline.transforms.jsoninput;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class JsonInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = JsonInputMeta.class; // For Translator

  private CTabFolder wTabFolder;

  private Label wlFilename, wlSourceIsAFile;
  private Button wbbFilename; // Browse: add file or directory
  private Button wbdFilename; // Delete
  private Button wbeFilename; // Edit
  private Button wbaFilename; // Add or change
  private TextVar wFilename;

  private Label wlFilenameList;
  private TableView wFilenameList;

  private Label wlFilemask;
  private TextVar wFilemask;

  private Button wbShowFiles;

  private Label wlSourceField;
  private CCombo wFieldValue;
  private Button wSourceStreamField, wSourceIsAFile;

  private Label wlInclFilename;
  private Button wInclFilename, wAddResult;

  private Label wlReadUrl;
  private Button wReadUrl;

  private Label wlRemoveSourceField;
  private Button wRemoveSourceField;
  private Label wlInclFilenameField;
  private TextVar wInclFilenameField;

  private Label wlAddResult;
  private Button wInclRownum;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;

  private Label wlLimit;
  private Text wLimit;

  private TableView wFields;

  private Label wlExcludeFilemask;
  private TextVar wExcludeFilemask;

  private Button wIgnoreEmptyFile;

  private Button wIgnoreMissingPath;

  private Button wDefaultPathLeafToNull;

  private Button wdoNotFailIfNoFile;

  private TextVar wShortFileFieldName;
  private TextVar wPathFieldName;

  private TextVar wIsHiddenName;
  private TextVar wLastModificationTimeName;
  private TextVar wUriName;
  private TextVar wRootUriName;
  private TextVar wExtensionFieldName;
  private TextVar wSizeFieldName;

  private final JsonInputMeta input;

  private int middle;
  private int margin;
  private ModifyListener lsMod;

  public JsonInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (JsonInputMeta) in;
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
    shell.setText(BaseMessages.getString(PKG, "JsonInputDialog.DialogTitle"));

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    // Buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "JsonInputDialog.Button.PreviewRows"));
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

    addFileTab();

    addContentTab();

    addFieldsTab();

    addAdditionalFieldsTab();

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

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
                  JsonInputMeta.RequiredFilesCode[0],
                  JsonInputMeta.RequiredFilesCode[0]
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
              JsonInputMeta tfii = new JsonInputMeta();
              getInfo(tfii);
              FileInputList fileInputList = tfii.getFiles(variables);
              String[] files = fileInputList.getFileStrings();
              if (files != null && files.length > 0) {
                EnterSelectionDialog esd =
                    new EnterSelectionDialog(
                        shell,
                        files,
                        BaseMessages.getString(
                            PKG, "JsonInputDialog.FilesReadSelection.DialogTitle"),
                        BaseMessages.getString(
                            PKG, "JsonInputDialog.FilesReadSelection.DialogMessage"));
                esd.setViewOnly();
                esd.open();
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(
                    BaseMessages.getString(PKG, "JsonInputDialog.NoFileFound.DialogMessage"));
                mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
                mb.open();
              }
            } catch (HopException ex) {
              new ErrorDialog(
                  shell,
                  BaseMessages.getString(PKG, "JsonInputDialog.ErrorParsingData.DialogTitle"),
                  BaseMessages.getString(PKG, "JsonInputDialog.ErrorParsingData.DialogMessage"),
                  ex);
            }
          }
        });
    // Enable/disable the right fields to allow a filename to be added to each row...
    wInclFilename.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            setIncludeFilename();
          }
        });

    // Enable/disable the right fields to allow a row number to be added to each row...
    wInclRownum.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            setIncludeRownum();
          }
        });

    // Whenever something changes, set the tooltip to the expanded version of the filename:
    wFilename.addModifyListener(e -> wFilename.setToolTipText(wFilename.getText()));

    wbbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, variables,
            new String[] { "*.js", "*.json", "*" },
            new String[] { BaseMessages.getString( PKG, "System.FileType.JsonFiles" ),
                    BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
            true )
    );
    // Listen to the Browse... button
    /* wbbFilename.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            if (!Utils.isEmpty(wFilemask.getText())
                || !Utils.isEmpty(wExcludeFilemask.getText())) { // A mask: a
              // directory!
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
              dialog.setFilterExtensions(new String[] {"*.js;*.JS;*.json;*.JSON", "*"});
              if (wFilename.getText() != null) {
                String fname = variables.resolve(wFilename.getText());
                dialog.setFileName(fname);
              }

              dialog.setFilterNames(
                  new String[] {
                    BaseMessages.getString(PKG, "System.FileType.JsonFiles"),
                    BaseMessages.getString(PKG, "System.FileType.AllFiles")
                  });

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
*/
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
    activeStreamField();
    setIncludeFilename();
    setIncludeRownum();
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

  private void addFieldsTab() {
    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText(BaseMessages.getString(PKG, "JsonInputDialog.Fields.Tab"));

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout(fieldsLayout);
    props.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "JsonInputDialog.Button.SelectFields"));
    fdGet = new FormData();
    fdGet.left = new FormAttachment(50, 0);
    fdGet.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);
    wGet.addListener(SWT.Selection, e -> get());

    setButtonPositions(new Button[] {wGet}, margin, null);

    final int FieldsRows = input.getInputFields().length;

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Name.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Path.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Type.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Format.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              Const.getConversionFormats()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Currency.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Decimal.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Group.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.TrimType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaBase.trimTypeDesc,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Repeat.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, "System.Combo.Yes"),
                BaseMessages.getString(PKG, "System.Combo.No")
              },
              true),
        };

    colinf[0].setUsingVariables(true);
    colinf[0].setToolTip(
        BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Name.Column.Tooltip"));
    colinf[1].setUsingVariables(true);
    colinf[1].setToolTip(
        BaseMessages.getString(PKG, "JsonInputDialog.FieldsTable.Path.Column.Tooltip"));

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
  }

  private void addContentTab() {
    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setText(BaseMessages.getString(PKG, "JsonInputDialog.Content.Tab"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    // ///////////////////////////////
    // START OF Conf Field GROUP //
    // ///////////////////////////////

    Group wConf = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wConf);
    wConf.setText(BaseMessages.getString(PKG, "JsonInputDialog.wConf.Label"));

    FormLayout ConfgroupLayout = new FormLayout();
    ConfgroupLayout.marginWidth = 10;
    ConfgroupLayout.marginHeight = 10;
    wConf.setLayout(ConfgroupLayout);

    // Ignore Empty File
    // ignore empty files flag
    Label wlIgnoreEmptyFile = new Label(wConf, SWT.RIGHT);
    wlIgnoreEmptyFile.setText(BaseMessages.getString(PKG, "JsonInputDialog.IgnoreEmptyFile.Label"));
    props.setLook(wlIgnoreEmptyFile);
    FormData fdlIgnoreEmptyFile = new FormData();
    fdlIgnoreEmptyFile.left = new FormAttachment(0, 0);
    fdlIgnoreEmptyFile.top = new FormAttachment(0, margin);
    fdlIgnoreEmptyFile.right = new FormAttachment(middle, -margin);
    wlIgnoreEmptyFile.setLayoutData(fdlIgnoreEmptyFile);
    wIgnoreEmptyFile = new Button(wConf, SWT.CHECK);
    props.setLook(wIgnoreEmptyFile);
    wIgnoreEmptyFile.setToolTipText(
        BaseMessages.getString(PKG, "JsonInputDialog.IgnoreEmptyFile.Tooltip"));
    FormData fdIgnoreEmptyFile = new FormData();
    fdIgnoreEmptyFile.left = new FormAttachment(middle, 0);
    fdIgnoreEmptyFile.top = new FormAttachment(wlIgnoreEmptyFile, 0, SWT.CENTER);
    wIgnoreEmptyFile.setLayoutData(fdIgnoreEmptyFile);

    // do not fail if no files?
    //
    Label wlDoNotFailIfNoFile = new Label(wConf, SWT.RIGHT);
    wlDoNotFailIfNoFile.setText(
        BaseMessages.getString(PKG, "JsonInputDialog.doNotFailIfNoFile.Label"));
    props.setLook(wlDoNotFailIfNoFile);
    FormData fdldoNotFailIfNoFile = new FormData();
    fdldoNotFailIfNoFile.left = new FormAttachment(0, 0);
    fdldoNotFailIfNoFile.top = new FormAttachment(wIgnoreEmptyFile, margin);
    fdldoNotFailIfNoFile.right = new FormAttachment(middle, -margin);
    wlDoNotFailIfNoFile.setLayoutData(fdldoNotFailIfNoFile);
    wdoNotFailIfNoFile = new Button(wConf, SWT.CHECK);
    props.setLook(wdoNotFailIfNoFile);
    wdoNotFailIfNoFile.setToolTipText(
        BaseMessages.getString(PKG, "JsonInputDialog.doNotFailIfNoFile.Tooltip"));
    FormData fddoNotFailIfNoFile = new FormData();
    fddoNotFailIfNoFile.left = new FormAttachment(middle, 0);
    fddoNotFailIfNoFile.top = new FormAttachment(wlDoNotFailIfNoFile, 0, SWT.CENTER);
    wdoNotFailIfNoFile.setLayoutData(fddoNotFailIfNoFile);

    // Ignore missing path
    //
    Label wlIgnoreMissingPath = new Label(wConf, SWT.RIGHT);
    wlIgnoreMissingPath.setText(
        BaseMessages.getString(PKG, "JsonInputDialog.IgnoreMissingPath.Label"));
    props.setLook(wlIgnoreMissingPath);
    FormData fdlIgnoreMissingPath = new FormData();
    fdlIgnoreMissingPath.left = new FormAttachment(0, 0);
    fdlIgnoreMissingPath.top = new FormAttachment(wdoNotFailIfNoFile, margin);
    fdlIgnoreMissingPath.right = new FormAttachment(middle, -margin);
    wlIgnoreMissingPath.setLayoutData(fdlIgnoreMissingPath);
    wIgnoreMissingPath = new Button(wConf, SWT.CHECK);
    props.setLook(wIgnoreMissingPath);
    wIgnoreMissingPath.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });
    wIgnoreMissingPath.setToolTipText(
        BaseMessages.getString(PKG, "JsonInputDialog.IgnoreMissingPath.Tooltip"));
    FormData fdIgnoreMissingPath = new FormData();
    fdIgnoreMissingPath.left = new FormAttachment(middle, 0);
    fdIgnoreMissingPath.top = new FormAttachment(wlIgnoreMissingPath, 0, SWT.CENTER);
    wIgnoreMissingPath.setLayoutData(fdIgnoreMissingPath);

    // default path leaf to null
    //
    Label wlDefaultPathLeafToNull = new Label(wConf, SWT.RIGHT);
    wlDefaultPathLeafToNull.setText(
        BaseMessages.getString(PKG, "JsonInputDialog.DefaultPathLeafToNull.Label"));
    props.setLook(wlDefaultPathLeafToNull);
    FormData fdlDefaultPathLeafToNull = new FormData();
    fdlDefaultPathLeafToNull.left = new FormAttachment(0, 0);
    fdlDefaultPathLeafToNull.top = new FormAttachment(wIgnoreMissingPath, margin);
    fdlDefaultPathLeafToNull.right = new FormAttachment(middle, -margin);
    wlDefaultPathLeafToNull.setLayoutData(fdlDefaultPathLeafToNull);
    wDefaultPathLeafToNull = new Button(wConf, SWT.CHECK);
    props.setLook(wDefaultPathLeafToNull);
    wDefaultPathLeafToNull.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });
    wDefaultPathLeafToNull.setToolTipText(
        BaseMessages.getString(PKG, "JsonInputDialog.DefaultPathLeafToNull.Tooltip"));
    FormData fdDefaultPathLeafToNull = new FormData();
    fdDefaultPathLeafToNull.left = new FormAttachment(middle, 0);
    fdDefaultPathLeafToNull.top = new FormAttachment(wlDefaultPathLeafToNull, 0, SWT.CENTER);
    wDefaultPathLeafToNull.setLayoutData(fdDefaultPathLeafToNull);
    // default path leaf to null - end

    wlLimit = new Label(wConf, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "JsonInputDialog.Limit.Label"));
    props.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.top = new FormAttachment(wDefaultPathLeafToNull, margin);
    fdlLimit.right = new FormAttachment(middle, -margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wConf, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.top = new FormAttachment(wDefaultPathLeafToNull, margin);
    fdLimit.right = new FormAttachment(100, 0);
    wLimit.setLayoutData(fdLimit);

    FormData fdConf = new FormData();
    fdConf.left = new FormAttachment(0, margin);
    fdConf.top = new FormAttachment(0, margin);
    fdConf.right = new FormAttachment(100, -margin);
    wConf.setLayoutData(fdConf);

    // ///////////////////////////////////////////////////////////
    // / END OF Conf Field GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF Additional Fields GROUP //
    // ///////////////////////////////

    Group wAdditionalFields = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wAdditionalFields);
    wAdditionalFields.setText(
        BaseMessages.getString(PKG, "JsonInputDialog.wAdditionalFields.Label"));

    FormLayout AdditionalFieldsgroupLayout = new FormLayout();
    AdditionalFieldsgroupLayout.marginWidth = 10;
    AdditionalFieldsgroupLayout.marginHeight = 10;
    wAdditionalFields.setLayout(AdditionalFieldsgroupLayout);

    wlInclFilename = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclFilename.setText(BaseMessages.getString(PKG, "JsonInputDialog.InclFilename.Label"));
    props.setLook(wlInclFilename);
    FormData fdlInclFilename = new FormData();
    fdlInclFilename.left = new FormAttachment(0, 0);
    fdlInclFilename.top = new FormAttachment(wConf, 4 * margin);
    fdlInclFilename.right = new FormAttachment(middle, -margin);
    wlInclFilename.setLayoutData(fdlInclFilename);
    wInclFilename = new Button(wAdditionalFields, SWT.CHECK);
    props.setLook(wInclFilename);
    wInclFilename.setToolTipText(
        BaseMessages.getString(PKG, "JsonInputDialog.InclFilename.Tooltip"));
    FormData fdInclFilename = new FormData();
    fdInclFilename.left = new FormAttachment(middle, 0);
    fdInclFilename.top = new FormAttachment(wlInclFilename, 0, SWT.CENTER);
    wInclFilename.setLayoutData(fdInclFilename);

    wlInclFilenameField = new Label(wAdditionalFields, SWT.LEFT);
    wlInclFilenameField.setText(
        BaseMessages.getString(PKG, "JsonInputDialog.InclFilenameField.Label"));
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
    wlInclRownum.setText(BaseMessages.getString(PKG, "JsonInputDialog.InclRownum.Label"));
    props.setLook(wlInclRownum);
    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment(0, 0);
    fdlInclRownum.top = new FormAttachment(wInclFilenameField, margin);
    fdlInclRownum.right = new FormAttachment(middle, -margin);
    wlInclRownum.setLayoutData(fdlInclRownum);
    wInclRownum = new Button(wAdditionalFields, SWT.CHECK);
    props.setLook(wInclRownum);
    wInclRownum.setToolTipText(BaseMessages.getString(PKG, "JsonInputDialog.InclRownum.Tooltip"));
    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment(middle, 0);
    fdRownum.top = new FormAttachment(wlInclRownum, 0, SWT.CENTER);
    wInclRownum.setLayoutData(fdRownum);

    wlInclRownumField = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclRownumField.setText(BaseMessages.getString(PKG, "JsonInputDialog.InclRownumField.Label"));
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
    fdAdditionalFields.top = new FormAttachment(wConf, margin);
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
    wAddFileResult.setText(BaseMessages.getString(PKG, "JsonInputDialog.wAddFileResult.Label"));

    FormLayout AddFileResultgroupLayout = new FormLayout();
    AddFileResultgroupLayout.marginWidth = 10;
    AddFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout(AddFileResultgroupLayout);

    wlAddResult = new Label(wAddFileResult, SWT.RIGHT);
    wlAddResult.setText(BaseMessages.getString(PKG, "JsonInputDialog.AddResult.Label"));
    props.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment(0, 0);
    fdlAddResult.top = new FormAttachment(wAdditionalFields, margin);
    fdlAddResult.right = new FormAttachment(middle, -margin);
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wAddFileResult, SWT.CHECK);
    props.setLook(wAddResult);
    wAddResult.setToolTipText(BaseMessages.getString(PKG, "JsonInputDialog.AddResult.Tooltip"));
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(middle, 0);
    fdAddResult.top = new FormAttachment(wAdditionalFields, margin);
    wAddResult.setLayoutData(fdAddResult);

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
  }

  private void addFileTab() {
    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setText(BaseMessages.getString(PKG, "JsonInputDialog.File.Tab"));

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
    wOutputField.setText(BaseMessages.getString(PKG, "JsonInputDialog.wOutputField.Label"));

    FormLayout outputfieldgroupLayout = new FormLayout();
    outputfieldgroupLayout.marginWidth = 10;
    outputfieldgroupLayout.marginHeight = 10;
    wOutputField.setLayout(outputfieldgroupLayout);

    // Is source string defined in a Field
    Label wlSourceStreamField = new Label(wOutputField, SWT.RIGHT);
    wlSourceStreamField.setText(
        BaseMessages.getString(PKG, "JsonInputDialog.wlSourceStreamField.Label"));
    props.setLook(wlSourceStreamField);
    FormData fdlSourceStreamField = new FormData();
    fdlSourceStreamField.left = new FormAttachment(0, -margin);
    fdlSourceStreamField.top = new FormAttachment(0, margin);
    fdlSourceStreamField.right = new FormAttachment(middle, -2 * margin);
    wlSourceStreamField.setLayoutData(fdlSourceStreamField);
    wSourceStreamField = new Button(wOutputField, SWT.CHECK);
    props.setLook(wSourceStreamField);
    wSourceStreamField.setToolTipText(
        BaseMessages.getString(PKG, "JsonInputDialog.wSourceStreamField.Tooltip"));
    FormData fdSourceStreamField = new FormData();
    fdSourceStreamField.left = new FormAttachment(middle, -margin);
    fdSourceStreamField.top = new FormAttachment(wlSourceStreamField, 0, SWT.CENTER);
    wSourceStreamField.setLayoutData(fdSourceStreamField);
    SelectionAdapter lsstream =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            activeStreamField();
            input.setChanged();
          }
        };
    wSourceStreamField.addSelectionListener(lsstream);

    // If source string defined in a Field
    wlSourceField = new Label(wOutputField, SWT.RIGHT);
    wlSourceField.setText(BaseMessages.getString(PKG, "JsonInputDialog.wlSourceField.Label"));
    props.setLook(wlSourceField);
    FormData fdlFieldValue = new FormData();
    fdlFieldValue.left = new FormAttachment(0, -margin);
    fdlFieldValue.top = new FormAttachment(wSourceStreamField, margin);
    fdlFieldValue.right = new FormAttachment(middle, -2 * margin);
    wlSourceField.setLayoutData(fdlFieldValue);

    wFieldValue = new CCombo(wOutputField, SWT.BORDER | SWT.READ_ONLY);
    wFieldValue.setEditable(true);
    props.setLook(wFieldValue);
    wFieldValue.addModifyListener(lsMod);
    FormData fdFieldValue = new FormData();
    fdFieldValue.left = new FormAttachment(middle, -margin);
    fdFieldValue.top = new FormAttachment(wSourceStreamField, margin);
    fdFieldValue.right = new FormAttachment(100, -margin);
    wFieldValue.setLayoutData(fdFieldValue);
    setSourceStreamField();

    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(0, margin);
    fdOutputField.top = new FormAttachment(wFilenameList, margin);
    fdOutputField.right = new FormAttachment(100, -margin);
    wOutputField.setLayoutData(fdOutputField);

    // Is source is a file?
    wlSourceIsAFile = new Label(wOutputField, SWT.RIGHT);
    wlSourceIsAFile.setText(BaseMessages.getString(PKG, "JsonInputDialog.SourceIsAFile.Label"));
    props.setLook(wlSourceIsAFile);
    FormData fdlSourceIsAFile = new FormData();
    fdlSourceIsAFile.left = new FormAttachment(0, -margin);
    fdlSourceIsAFile.top = new FormAttachment(wFieldValue, margin);
    fdlSourceIsAFile.right = new FormAttachment(middle, -2 * margin);
    wlSourceIsAFile.setLayoutData(fdlSourceIsAFile);
    wSourceIsAFile = new Button(wOutputField, SWT.CHECK);
    props.setLook(wSourceIsAFile);
    wSourceIsAFile.setToolTipText(
        BaseMessages.getString(PKG, "JsonInputDialog.SourceIsAFile.Tooltip"));
    FormData fdSourceIsAFile = new FormData();
    fdSourceIsAFile.left = new FormAttachment(middle, -margin);
    fdSourceIsAFile.top = new FormAttachment(wlSourceIsAFile, 0, SWT.CENTER);
    wSourceIsAFile.setLayoutData(fdSourceIsAFile);
    SelectionAdapter lssourceisafile =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            if (wSourceIsAFile.getSelection()) {
              wReadUrl.setSelection(false);
            }
            input.setChanged();
          }
        };
    wSourceIsAFile.addSelectionListener(lssourceisafile);

    // read url as source ?
    wlReadUrl = new Label(wOutputField, SWT.RIGHT);
    wlReadUrl.setText(BaseMessages.getString(PKG, "JsonInputDialog.readUrl.Label"));
    props.setLook(wlReadUrl);
    FormData fdlreadUrl = new FormData();
    fdlreadUrl.left = new FormAttachment(0, -margin);
    fdlreadUrl.top = new FormAttachment(wlSourceIsAFile, margin);
    fdlreadUrl.right = new FormAttachment(middle, -2 * margin);
    wlReadUrl.setLayoutData(fdlreadUrl);
    wReadUrl = new Button(wOutputField, SWT.CHECK);
    props.setLook(wReadUrl);
    wReadUrl.setToolTipText(BaseMessages.getString(PKG, "JsonInputDialog.readUrl.Tooltip"));
    FormData fdreadUrl = new FormData();
    fdreadUrl.left = new FormAttachment(middle, -margin);
    fdreadUrl.top = new FormAttachment(wlReadUrl, 0, SWT.CENTER);
    wReadUrl.setLayoutData(fdreadUrl);
    SelectionAdapter lsreadurl =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            if (wReadUrl.getSelection()) {
              wSourceIsAFile.setSelection(false);
            }
            input.setChanged();
          }
        };
    wReadUrl.addSelectionListener(lsreadurl);

    // Remove source field from output stream?
    wlRemoveSourceField = new Label(wOutputField, SWT.RIGHT);
    wlRemoveSourceField.setText(
        BaseMessages.getString(PKG, "JsonInputDialog.removeSourceField.Label"));
    props.setLook(wlRemoveSourceField);
    FormData fdlremoveSourceField = new FormData();
    fdlremoveSourceField.left = new FormAttachment(0, -margin);
    fdlremoveSourceField.top = new FormAttachment(wlReadUrl, margin);
    fdlremoveSourceField.right = new FormAttachment(middle, -2 * margin);
    wlRemoveSourceField.setLayoutData(fdlremoveSourceField);
    wRemoveSourceField = new Button(wOutputField, SWT.CHECK);
    props.setLook(wRemoveSourceField);
    FormData fdremoveSourceField = new FormData();
    fdremoveSourceField.left = new FormAttachment(middle, -margin);
    fdremoveSourceField.top = new FormAttachment(wlRemoveSourceField, 0, SWT.CENTER);
    wRemoveSourceField.setLayoutData(fdremoveSourceField);
    SelectionAdapter removeSourceFieldAdapter =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            input.setChanged();
          }
        };
    wRemoveSourceField.addSelectionListener(removeSourceFieldAdapter);

    // ///////////////////////////////////////////////////////////
    // / END OF Output Field GROUP
    // ///////////////////////////////////////////////////////////

    // Filename line
    wlFilename = new Label(wFileComp, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "JsonInputDialog.Filename.Label"));
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wOutputField, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "JsonInputDialog.FilenameBrowse.Button"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wOutputField, margin);
    wbbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbaFilename);
    wbaFilename.setText(BaseMessages.getString(PKG, "JsonInputDialog.FilenameAdd.Button"));
    wbaFilename.setToolTipText(BaseMessages.getString(PKG, "JsonInputDialog.FilenameAdd.Tooltip"));
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
    wlFilemask.setText(BaseMessages.getString(PKG, "JsonInputDialog.RegExp.Label"));
    props.setLook(wlFilemask);
    FormData fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment(0, 0);
    fdlFilemask.top = new FormAttachment(wFilename, margin);
    fdlFilemask.right = new FormAttachment(middle, -margin);
    wlFilemask.setLayoutData(fdlFilemask);
    wFilemask = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilemask);
    wFilemask.addModifyListener(lsMod);
    FormData fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment(middle, 0);
    fdFilemask.top = new FormAttachment(wFilename, margin);
    fdFilemask.right = new FormAttachment(100, 0);
    wFilemask.setLayoutData(fdFilemask);

    wlExcludeFilemask = new Label(wFileComp, SWT.RIGHT);
    wlExcludeFilemask.setText(BaseMessages.getString(PKG, "JsonInputDialog.ExcludeFilemask.Label"));
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
    wlFilenameList.setText(BaseMessages.getString(PKG, "JsonInputDialog.FilenameList.Label"));
    props.setLook(wlFilenameList);
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment(0, 0);
    fdlFilenameList.top = new FormAttachment(wExcludeFilemask, margin);
    fdlFilenameList.right = new FormAttachment(middle, -margin);
    wlFilenameList.setLayoutData(fdlFilenameList);

    // Buttons to the right of the screen...
    wbdFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbdFilename);
    wbdFilename.setText(BaseMessages.getString(PKG, "JsonInputDialog.FilenameRemove.Button"));
    wbdFilename.setToolTipText(
        BaseMessages.getString(PKG, "JsonInputDialog.FilenameRemove.Tooltip"));
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment(100, 0);
    fdbdFilename.top = new FormAttachment(wExcludeFilemask, 40);
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbeFilename);
    wbeFilename.setText(BaseMessages.getString(PKG, "JsonInputDialog.FilenameEdit.Button"));
    wbeFilename.setToolTipText(BaseMessages.getString(PKG, "JsonInputDialog.FilenameEdit.Tooltip"));
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment(100, 0);
    fdbeFilename.left = new FormAttachment(wbdFilename, 0, SWT.LEFT);
    fdbeFilename.top = new FormAttachment(wbdFilename, margin);
    wbeFilename.setLayoutData(fdbeFilename);

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "JsonInputDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.bottom = new FormAttachment(100, 0);
    wbShowFiles.setLayoutData(fdbShowFiles);

    ColumnInfo[] colinfo = new ColumnInfo[5];
    colinfo[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "JsonInputDialog.Files.Filename.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "JsonInputDialog.Files.Wildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "JsonInputDialog.Files.ExcludeWildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    colinfo[0].setUsingVariables(true);
    colinfo[1].setUsingVariables(true);
    colinfo[1].setToolTip(BaseMessages.getString(PKG, "JsonInputDialog.Files.Wildcard.Tooltip"));
    colinfo[2].setUsingVariables(true);
    colinfo[2].setToolTip(
        BaseMessages.getString(PKG, "JsonInputDialog.Files.ExcludeWildcard.Tooltip"));
    colinfo[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "JsonInputDialog.Required.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            JsonInputMeta.RequiredFilesDesc);
    colinfo[3].setToolTip(BaseMessages.getString(PKG, "JsonInputDialog.Required.Tooltip"));
    colinfo[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "JsonInputDialog.IncludeSubDirs.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            JsonInputMeta.RequiredFilesDesc);
    colinfo[4].setToolTip(BaseMessages.getString(PKG, "JsonInputDialog.IncludeSubDirs.Tooltip"));

    wFilenameList =
        new TableView(
            variables,
            wFileComp,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
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
  }

  private void setSourceStreamField() {
    try {
      String value = wFieldValue.getText();
      wFieldValue.removeAll();

      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        wFieldValue.setItems(r.getFieldNames());
      }
      if (value != null) {
        wFieldValue.setText(value);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "JsonInputDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "JsonInputDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void activeStreamField() {
    wlSourceField.setEnabled(wSourceStreamField.getSelection());
    wFieldValue.setEnabled(wSourceStreamField.getSelection());
    wlSourceIsAFile.setEnabled(wSourceStreamField.getSelection());
    wSourceIsAFile.setEnabled(wSourceStreamField.getSelection());
    wlReadUrl.setEnabled(wSourceStreamField.getSelection());
    wReadUrl.setEnabled(wSourceStreamField.getSelection());
    wlRemoveSourceField.setEnabled(wSourceStreamField.getSelection());
    wRemoveSourceField.setEnabled(wSourceStreamField.getSelection());

    wlFilename.setEnabled(!wSourceStreamField.getSelection());
    wbbFilename.setEnabled(!wSourceStreamField.getSelection());
    wbaFilename.setEnabled(!wSourceStreamField.getSelection());
    wFilename.setEnabled(!wSourceStreamField.getSelection());
    wlExcludeFilemask.setEnabled(!wSourceStreamField.getSelection());
    wExcludeFilemask.setEnabled(!wSourceStreamField.getSelection());
    wlFilemask.setEnabled(!wSourceStreamField.getSelection());
    wFilemask.setEnabled(!wSourceStreamField.getSelection());
    wlFilenameList.setEnabled(!wSourceStreamField.getSelection());
    wbdFilename.setEnabled(!wSourceStreamField.getSelection());
    wbeFilename.setEnabled(!wSourceStreamField.getSelection());
    wbShowFiles.setEnabled(!wSourceStreamField.getSelection());
    wlFilenameList.setEnabled(!wSourceStreamField.getSelection());

    wFilenameList.setEnabled(!wSourceStreamField.getSelection());
    setCompositeEnabled(wFilenameList, !wSourceStreamField.getSelection());

    wInclFilename.setEnabled(!wSourceStreamField.getSelection());
    wlInclFilename.setEnabled(!wSourceStreamField.getSelection());

    if (wSourceStreamField.getSelection()) {
      wInclFilename.setSelection(false);
      wlInclFilenameField.setEnabled(false);
      wInclFilenameField.setEnabled(false);
    } else {
      wlInclFilenameField.setEnabled(wInclFilename.getSelection());
      wInclFilenameField.setEnabled(wInclFilename.getSelection());
    }

    wAddResult.setEnabled(!wSourceStreamField.getSelection());
    wlAddResult.setEnabled(!wSourceStreamField.getSelection());
    wLimit.setEnabled(!wSourceStreamField.getSelection());
    wlLimit.setEnabled(!wSourceStreamField.getSelection());
    wPreview.setEnabled(!wSourceStreamField.getSelection());
  }

  private void setCompositeEnabled(Composite comp, boolean enabled) {
    // TODO: move to TableView?
    comp.setEnabled(enabled);
    for (Control child : comp.getChildren()) {
      child.setEnabled(enabled);
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
  public void getData(JsonInputMeta in) {
    if (in.getFileName() != null) {
      wFilenameList.removeAll();

      for (int i = 0; i < in.getFileName().length; i++) {
        wFilenameList.add(
            new String[] {
              in.getFileName()[i],
              in.getFileMask()[i],
              in.getExcludeFileMask()[i],
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
    wReadUrl.setSelection(in.isReadUrl());
    wIgnoreEmptyFile.setSelection(in.isIgnoreEmptyFile());
    wdoNotFailIfNoFile.setSelection(in.isDoNotFailIfNoFile());
    wIgnoreMissingPath.setSelection(in.isIgnoreMissingPath());
    wDefaultPathLeafToNull.setSelection(in.isDefaultPathLeafToNull());
    wRemoveSourceField.setSelection(in.isRemoveSourceField());
    wSourceStreamField.setSelection(in.isInFields());
    wSourceIsAFile.setSelection(in.getIsAFile());

    if (in.getFieldValue() != null) {
      wFieldValue.setText(in.getFieldValue());
    }

    if (in.getFilenameField() != null) {
      wInclFilenameField.setText(in.getFilenameField());
    }
    if (in.getRowNumberField() != null) {
      wInclRownumField.setText(in.getRowNumberField());
    }
    wLimit.setText("" + in.getRowLimit());

    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "JsonInputDialog.Log.GettingFieldsInfo"));
    }
    for (int i = 0; i < in.getInputFields().length; i++) {
      JsonInputField field = in.getInputFields()[i];

      if (field != null) {
        TableItem item = wFields.table.getItem(i);
        String name = field.getName();
        String xpath = field.getPath();
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
        if (xpath != null) {
          item.setText(2, xpath);
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
    setSourceStreamField();

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
    if (in.getSizeField() != null) {
      wSizeFieldName.setText(in.getSizeField());
    }

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
          BaseMessages.getString(PKG, "JsonInputDialog.ErrorParsingData.DialogTitle"),
          BaseMessages.getString(PKG, "JsonInputDialog.ErrorParsingData.DialogMessage"),
          e);
    }
    dispose();
  }

  /** dialog -&gt; meta */
  private void getInfo(JsonInputMeta in) throws HopException {
    transformName = wTransformName.getText(); // return value

    in.setRowLimit(Const.toLong(wLimit.getText(), 0L));
    in.setFilenameField(wInclFilenameField.getText());
    in.setRowNumberField(wInclRownumField.getText());
    in.setAddResultFile(wAddResult.getSelection());
    in.setIncludeFilename(wInclFilename.getSelection());
    in.setIncludeRowNumber(wInclRownum.getSelection());
    in.setReadUrl(wReadUrl.getSelection());
    in.setIgnoreEmptyFile(wIgnoreEmptyFile.getSelection());
    in.setDoNotFailIfNoFile(wdoNotFailIfNoFile.getSelection());
    in.setIgnoreMissingPath(wIgnoreMissingPath.getSelection());
    in.setDefaultPathLeafToNull(wDefaultPathLeafToNull.getSelection());
    in.setRemoveSourceField(wRemoveSourceField.getSelection());
    in.setInFields(wSourceStreamField.getSelection());
    in.setIsAFile(wSourceIsAFile.getSelection());
    in.setFieldValue(wFieldValue.getText());

    int nrFiles = wFilenameList.getItemCount();
    int nrFields = wFields.nrNonEmpty();

    in.allocate(nrFiles, nrFields);
    in.setFileName(wFilenameList.getItems(0));
    in.setFileMask(wFilenameList.getItems(1));
    in.setExcludeFileMask(wFilenameList.getItems(2));
    in.setFileRequired(wFilenameList.getItems(3));
    in.setIncludeSubFolders(wFilenameList.getItems(4));

    for (int i = 0; i < nrFields; i++) {
      JsonInputField field = new JsonInputField();

      TableItem item = wFields.getNonEmpty(i);

      field.setName(item.getText(1));
      field.setPath(item.getText(2));
      field.setType(ValueMetaFactory.getIdForValueMeta(item.getText(3)));
      field.setFormat(item.getText(4));
      field.setLength(Const.toInt(item.getText(5), -1));
      field.setPrecision(Const.toInt(item.getText(6), -1));
      field.setCurrencySymbol(item.getText(7));
      field.setDecimalSymbol(item.getText(8));
      field.setGroupSymbol(item.getText(9));
      field.setTrimType(ValueMetaBase.getTrimTypeByDesc(item.getText(10)));
      field.setRepeated(
          BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(item.getText(11)));

      in.getInputFields()[i] = field;
    }
    in.setShortFileNameField(wShortFileFieldName.getText());
    in.setPathField(wPathFieldName.getText());
    in.setIsHiddenField(wIsHiddenName.getText());
    in.setLastModificationDateField(wLastModificationTimeName.getText());
    in.setUriField(wUriName.getText());
    in.setRootUriField(wRootUriName.getText());
    in.setExtensionField(wExtensionFieldName.getText());
    in.setSizeField(wSizeFieldName.getText());
  }

  private void get() {
    try {
      JsonInputMeta meta = new JsonInputMeta();
      getInfo(meta);
      FileInputList files = meta.getFiles(variables);
      for (FileObject fileObject : files.getFiles()) {
        try (InputStream inputStream = HopVfs.getInputStream(fileObject)) {
          // Extract all useful paths from the file...
          //
          Set<String> paths = extractPaths(inputStream);
          List<String> pathsList = new ArrayList<>(paths);
          Collections.sort(pathsList);

          for (String path : pathsList) {
            int dotIndex = path.lastIndexOf( '.' );
            if (dotIndex>0) {
              String name = path.substring( dotIndex+1 );
              TableItem item = new TableItem(wFields.table, SWT.NONE);
              item.setText(1, name);
              item.setText(2, path);
              item.setText(3, "String");
            }
          }
        }
      }
      wFields.optimizeTableView();

    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error sampling JSON file(s)", e);
    }
  }

  private Set<String> extractPaths(InputStream inputStream) throws Exception {
    Set<String> paths = new HashSet<>();
    LinkedList<String> currentPath = new LinkedList<>();

    JsonFactory jsonFactory = new MappingJsonFactory();
    JsonParser parser = jsonFactory.createParser(inputStream);

    while (parser.nextToken() != null) {
      JsonToken jsonToken = parser.currentToken();
      String name = parser.currentName();
      switch (jsonToken) {
        case START_OBJECT:
          currentPath.push(name); // {
          break;
        case END_OBJECT:
          currentPath.pop(); // }
          break;
        case START_ARRAY:
          currentPath.push(name); // name : [
          currentPath.push("*"); // path wildcard
          break;
        case END_ARRAY:
          currentPath.pop(); // *
          currentPath.pop(); // name
          break;
        case FIELD_NAME:
          currentPath.push(name);
          addToPaths(paths, currentPath);
          currentPath.pop();
          break;
        default:
          // Ignore the rest
          break;
      }
      if (currentPath.size()>100) {
        throw new HopException("Path too long");
      }
    }

    return paths;
  }

  private void addToPaths(Set<String> paths, LinkedList<String> currentPath) {
    StringBuilder path = new StringBuilder("$");

    Iterator<String> iterator = currentPath.descendingIterator();
    while (iterator.hasNext()) {
      String string = iterator.next();
      if (string!=null) {
        path.append(".").append(string);
      }
    }
    paths.add(path.toString());
  }

  // Preview the data
  private void preview() {
    try {
      JsonInputMeta oneMeta = new JsonInputMeta();
      getInfo(oneMeta);

      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
              variables, metadataProvider, oneMeta, wTransformName.getText());

      EnterNumberDialog numberDialog =
          new EnterNumberDialog(
              shell,
              props.getDefaultPreviewSize(),
              BaseMessages.getString(PKG, "JsonInputDialog.NumberRows.DialogTitle"),
              BaseMessages.getString(PKG, "JsonInputDialog.NumberRows.DialogMessage"));

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
          BaseMessages.getString(PKG, "JsonInputDialog.ErrorPreviewingData.DialogTitle"),
          BaseMessages.getString(PKG, "JsonInputDialog.ErrorPreviewingData.DialogMessage"),
          e);
    }
  }

  private void addAdditionalFieldsTab() {
    // ////////////////////////
    // START OF ADDITIONAL FIELDS TAB ///
    // ////////////////////////
    CTabItem wAdditionalFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalFieldsTab.setText(
        BaseMessages.getString(PKG, "JsonInputDialog.AdditionalFieldsTab.TabTitle"));

    Composite wAdditionalFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdditionalFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wAdditionalFieldsComp.setLayout(fieldsLayout);
    // ShortFileFieldName line
    Label wlShortFileFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlShortFileFieldName.setText(
        BaseMessages.getString(PKG, "JsonInputDialog.ShortFileFieldName.Label"));
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
        BaseMessages.getString(PKG, "JsonInputDialog.ExtensionFieldName.Label"));
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
    wlPathFieldName.setText(BaseMessages.getString(PKG, "JsonInputDialog.PathFieldName.Label"));
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

    // SizeFieldName line
    Label wlSizeFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlSizeFieldName.setText(BaseMessages.getString(PKG, "JsonInputDialog.SizeFieldName.Label"));
    props.setLook(wlSizeFieldName);
    FormData fdlSizeFieldName = new FormData();
    fdlSizeFieldName.left = new FormAttachment(0, 0);
    fdlSizeFieldName.top = new FormAttachment(wPathFieldName, margin);
    fdlSizeFieldName.right = new FormAttachment(middle, -margin);
    wlSizeFieldName.setLayoutData(fdlSizeFieldName);

    wSizeFieldName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSizeFieldName);
    wSizeFieldName.addModifyListener(lsMod);
    FormData fdSizeFieldName = new FormData();
    fdSizeFieldName.left = new FormAttachment(middle, 0);
    fdSizeFieldName.right = new FormAttachment(100, -margin);
    fdSizeFieldName.top = new FormAttachment(wPathFieldName, margin);
    wSizeFieldName.setLayoutData(fdSizeFieldName);

    // IsHiddenName line
    Label wlIsHiddenName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlIsHiddenName.setText(BaseMessages.getString(PKG, "JsonInputDialog.IsHiddenName.Label"));
    props.setLook(wlIsHiddenName);
    FormData fdlIsHiddenName = new FormData();
    fdlIsHiddenName.left = new FormAttachment(0, 0);
    fdlIsHiddenName.top = new FormAttachment(wSizeFieldName, margin);
    fdlIsHiddenName.right = new FormAttachment(middle, -margin);
    wlIsHiddenName.setLayoutData(fdlIsHiddenName);

    wIsHiddenName =
        new TextVar(variables, wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wIsHiddenName);
    wIsHiddenName.addModifyListener(lsMod);
    FormData fdIsHiddenName = new FormData();
    fdIsHiddenName.left = new FormAttachment(middle, 0);
    fdIsHiddenName.right = new FormAttachment(100, -margin);
    fdIsHiddenName.top = new FormAttachment(wSizeFieldName, margin);
    wIsHiddenName.setLayoutData(fdIsHiddenName);

    // LastModificationTimeName line
    Label wlLastModificationTimeName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlLastModificationTimeName.setText(
        BaseMessages.getString(PKG, "JsonInputDialog.LastModificationTimeName.Label"));
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
    wlUriName.setText(BaseMessages.getString(PKG, "JsonInputDialog.UriName.Label"));
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
    wlRootUriName.setText(BaseMessages.getString(PKG, "JsonInputDialog.RootUriName.Label"));
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
