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
package org.apache.hop.pipeline.transforms.vcardinput;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** File tab widgets for {@link VCardInputDialog}, aligned with text file input. */
public final class VCardInputFileDialogSection {

  private static final Class<?> PKG = VCardInputFileDialogSection.class;
  private static final String CONST_SYSTEM_COMBO_YES = BaseMessages.getString("System.Combo.Yes");

  private final Shell shell;
  private final IVariables variables;
  private final PipelineMeta pipelineMeta;
  private final String transformName;
  private final ModifyListener lsMod;

  private Label wlFilename;
  private Label wlFilemask;
  private Label wlExcludeFilemask;
  private Label wlFilenameList;
  private Label wlAccFilenames;
  private Label wlPassThruFields;
  private Label wlAccTransform;
  private Label wlAccField;
  private Label wlIncludeFilenameField;
  private Label wlEncoding;

  public TextVar wFilename;
  public TextVar wFilemask;
  public TextVar wExcludeFilemask;
  public TableView wFilenameList;
  public Button wAccFilenames;
  public Button wPassThruFields;
  public CCombo wAccTransform;
  public Text wAccField;
  public Button wIncludeFilename;
  public Text wIncludeFilenameField;
  public TextVar wEncoding;
  public Button wIgnoreEmptyFile;
  public Button wDoNotFailIfNoFile;
  public Button wAddResult;

  private Button wbbFilename;
  private Button wbaFilename;
  private Button wbdFilename;
  private Button wbeFilename;
  private Button wbShowFiles;
  private Group gAccepting;
  private Group gOptions;

  public VCardInputFileDialogSection(
      Shell shell,
      IVariables variables,
      PipelineMeta pipelineMeta,
      String transformName,
      ModifyListener lsMod) {
    this.shell = shell;
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.transformName = transformName;
    this.lsMod = lsMod;
  }

  public void build(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    wlFilename = new Label(parent, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(parent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(0, 0);
    wbbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(parent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbaFilename);
    wbaFilename.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.FilenameAdd.Button"));
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment(wbbFilename, -margin);
    fdbaFilename.top = new FormAttachment(0, 0);
    wbaFilename.setLayoutData(fdbaFilename);

    wFilename = new TextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbaFilename, -margin);
    fdFilename.top = new FormAttachment(0, 0);
    wFilename.setLayoutData(fdFilename);

    wlFilemask = new Label(parent, SWT.RIGHT);
    wlFilemask.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.Filemask.Label"));
    PropsUi.setLook(wlFilemask);
    FormData fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment(0, 0);
    fdlFilemask.top = new FormAttachment(wFilename, margin);
    fdlFilemask.right = new FormAttachment(middle, -margin);
    wlFilemask.setLayoutData(fdlFilemask);

    wFilemask = new TextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilemask);
    wFilemask.addModifyListener(lsMod);
    FormData fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment(middle, 0);
    fdFilemask.top = new FormAttachment(wFilename, margin);
    fdFilemask.right = new FormAttachment(wbaFilename, -margin);
    wFilemask.setLayoutData(fdFilemask);

    wlExcludeFilemask = new Label(parent, SWT.RIGHT);
    wlExcludeFilemask.setText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.ExcludeFilemask.Label"));
    PropsUi.setLook(wlExcludeFilemask);
    FormData fdlExcludeFilemask = new FormData();
    fdlExcludeFilemask.left = new FormAttachment(0, 0);
    fdlExcludeFilemask.top = new FormAttachment(wFilemask, margin);
    fdlExcludeFilemask.right = new FormAttachment(middle, -margin);
    wlExcludeFilemask.setLayoutData(fdlExcludeFilemask);

    wExcludeFilemask = new TextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wExcludeFilemask);
    wExcludeFilemask.addModifyListener(lsMod);
    FormData fdExcludeFilemask = new FormData();
    fdExcludeFilemask.left = new FormAttachment(middle, 0);
    fdExcludeFilemask.top = new FormAttachment(wFilemask, margin);
    fdExcludeFilemask.right = new FormAttachment(wFilename, 0, SWT.RIGHT);
    wExcludeFilemask.setLayoutData(fdExcludeFilemask);

    wlFilenameList = new Label(parent, SWT.RIGHT);
    wlFilenameList.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.FilenameList.Label"));
    PropsUi.setLook(wlFilenameList);
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment(0, 0);
    fdlFilenameList.top = new FormAttachment(wExcludeFilemask, margin);
    fdlFilenameList.right = new FormAttachment(middle, -margin);
    wlFilenameList.setLayoutData(fdlFilenameList);

    wbdFilename = new Button(parent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbdFilename);
    wbdFilename.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.FilenameRemove.Button"));
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment(100, 0);
    fdbdFilename.top = new FormAttachment(wExcludeFilemask, margin);
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(parent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbeFilename);
    wbeFilename.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.FilenameEdit.Button"));
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment(100, 0);
    fdbeFilename.left = new FormAttachment(wbdFilename, 0, SWT.LEFT);
    fdbeFilename.top = new FormAttachment(wbdFilename, margin);
    wbeFilename.setLayoutData(fdbeFilename);

    wbShowFiles = new Button(parent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.bottom = new FormAttachment(100, 0);
    wbShowFiles.setLayoutData(fdbShowFiles);

    gAccepting = new Group(parent, SWT.SHADOW_NONE);
    gAccepting.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.AcceptingGroup.Label"));
    FormLayout acceptingLayout = new FormLayout();
    acceptingLayout.marginWidth = 3;
    acceptingLayout.marginHeight = 3;
    gAccepting.setLayout(acceptingLayout);
    PropsUi.setLook(gAccepting);

    wlAccFilenames = new Label(gAccepting, SWT.RIGHT);
    wlAccFilenames.setText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.AcceptFilenames.Label"));
    PropsUi.setLook(wlAccFilenames);
    FormData fdlAccFilenames = new FormData();
    fdlAccFilenames.top = new FormAttachment(0, margin);
    fdlAccFilenames.left = new FormAttachment(0, 0);
    fdlAccFilenames.right = new FormAttachment(middle, -margin);
    wlAccFilenames.setLayoutData(fdlAccFilenames);

    wAccFilenames = new Button(gAccepting, SWT.CHECK);
    wAccFilenames.setToolTipText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.AcceptFilenames.Tooltip"));
    PropsUi.setLook(wAccFilenames);
    FormData fdAccFilenames = new FormData();
    fdAccFilenames.top = new FormAttachment(wlAccFilenames, 0, SWT.CENTER);
    fdAccFilenames.left = new FormAttachment(middle, 0);
    fdAccFilenames.right = new FormAttachment(100, 0);
    wAccFilenames.setLayoutData(fdAccFilenames);

    wlPassThruFields = new Label(gAccepting, SWT.RIGHT);
    wlPassThruFields.setText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.PassThruFields.Label"));
    PropsUi.setLook(wlPassThruFields);
    FormData fdlPassThruFields = new FormData();
    fdlPassThruFields.top = new FormAttachment(wAccFilenames, margin);
    fdlPassThruFields.left = new FormAttachment(0, 0);
    fdlPassThruFields.right = new FormAttachment(middle, -margin);
    wlPassThruFields.setLayoutData(fdlPassThruFields);

    wPassThruFields = new Button(gAccepting, SWT.CHECK);
    wPassThruFields.setToolTipText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.PassThruFields.Tooltip"));
    PropsUi.setLook(wPassThruFields);
    FormData fdPassThruFields = new FormData();
    fdPassThruFields.top = new FormAttachment(wlPassThruFields, 0, SWT.CENTER);
    fdPassThruFields.left = new FormAttachment(middle, 0);
    fdPassThruFields.right = new FormAttachment(100, 0);
    wPassThruFields.setLayoutData(fdPassThruFields);

    wlAccTransform = new Label(gAccepting, SWT.RIGHT);
    wlAccTransform.setText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.AcceptTransform.Label"));
    PropsUi.setLook(wlAccTransform);
    FormData fdlAccTransform = new FormData();
    fdlAccTransform.top = new FormAttachment(wPassThruFields, margin);
    fdlAccTransform.left = new FormAttachment(0, 0);
    fdlAccTransform.right = new FormAttachment(middle, -margin);
    wlAccTransform.setLayoutData(fdlAccTransform);

    wAccTransform = new CCombo(gAccepting, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAccTransform.setToolTipText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.AcceptTransform.Tooltip"));
    PropsUi.setLook(wAccTransform);
    wAccTransform.addModifyListener(lsMod);
    FormData fdAccTransform = new FormData();
    fdAccTransform.top = new FormAttachment(wPassThruFields, margin);
    fdAccTransform.left = new FormAttachment(middle, 0);
    fdAccTransform.right = new FormAttachment(100, 0);
    wAccTransform.setLayoutData(fdAccTransform);

    wlAccField = new Label(gAccepting, SWT.RIGHT);
    wlAccField.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.AcceptField.Label"));
    PropsUi.setLook(wlAccField);
    FormData fdlAccField = new FormData();
    fdlAccField.top = new FormAttachment(wAccTransform, margin);
    fdlAccField.left = new FormAttachment(0, 0);
    fdlAccField.right = new FormAttachment(middle, -margin);
    wlAccField.setLayoutData(fdlAccField);

    wAccField = new Text(gAccepting, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAccField.setToolTipText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.AcceptField.Tooltip"));
    PropsUi.setLook(wAccField);
    wAccField.addModifyListener(lsMod);
    FormData fdAccField = new FormData();
    fdAccField.top = new FormAttachment(wAccTransform, margin);
    fdAccField.left = new FormAttachment(middle, 0);
    fdAccField.right = new FormAttachment(100, 0);
    wAccField.setLayoutData(fdAccField);

    TransformMeta thisTransform = pipelineMeta.findTransform(transformName);
    if (thisTransform != null) {
      for (TransformMeta prevTransform : pipelineMeta.findPreviousTransforms(thisTransform)) {
        wAccTransform.add(prevTransform.getName());
      }
    }

    gOptions = new Group(parent, SWT.SHADOW_NONE);
    gOptions.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.OptionsGroup.Label"));
    FormLayout optionsLayout = new FormLayout();
    optionsLayout.marginWidth = 3;
    optionsLayout.marginHeight = 3;
    gOptions.setLayout(optionsLayout);
    PropsUi.setLook(gOptions);

    wIncludeFilename = new Button(gOptions, SWT.CHECK);
    wIncludeFilename.setText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.IncludeFilename.Label"));
    PropsUi.setLook(wIncludeFilename);
    wIncludeFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeIncludeFilename();
          }
        });
    FormData fdIncludeFilename = new FormData();
    fdIncludeFilename.left = new FormAttachment(0, margin);
    fdIncludeFilename.top = new FormAttachment(0, margin);
    wIncludeFilename.setLayoutData(fdIncludeFilename);

    wlIncludeFilenameField = new Label(gOptions, SWT.RIGHT);
    wlIncludeFilenameField.setText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.IncludeFilenameField.Label"));
    PropsUi.setLook(wlIncludeFilenameField);
    FormData fdlIncludeFilenameField = new FormData();
    fdlIncludeFilenameField.left = new FormAttachment(0, 0);
    fdlIncludeFilenameField.right = new FormAttachment(middle, -margin);
    fdlIncludeFilenameField.top = new FormAttachment(wIncludeFilename, margin);
    wlIncludeFilenameField.setLayoutData(fdlIncludeFilenameField);

    wIncludeFilenameField = new Text(gOptions, SWT.BORDER | SWT.SINGLE);
    PropsUi.setLook(wIncludeFilenameField);
    wIncludeFilenameField.addModifyListener(lsMod);
    FormData fdIncludeFilenameField = new FormData();
    fdIncludeFilenameField.left = new FormAttachment(middle, 0);
    fdIncludeFilenameField.right = new FormAttachment(100, 0);
    fdIncludeFilenameField.top = new FormAttachment(wlIncludeFilenameField, 0, SWT.CENTER);
    wIncludeFilenameField.setLayoutData(fdIncludeFilenameField);

    wlEncoding = new Label(gOptions, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    fdlEncoding.top = new FormAttachment(wIncludeFilenameField, margin);
    wlEncoding.setLayoutData(fdlEncoding);

    wEncoding = new TextVar(variables, gOptions, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.right = new FormAttachment(100, 0);
    fdEncoding.top = new FormAttachment(wlEncoding, 0, SWT.CENTER);
    wEncoding.setLayoutData(fdEncoding);

    wIgnoreEmptyFile = new Button(gOptions, SWT.CHECK);
    wIgnoreEmptyFile.setText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.IgnoreEmptyFile.Label"));
    PropsUi.setLook(wIgnoreEmptyFile);
    FormData fdIgnoreEmpty = new FormData();
    fdIgnoreEmpty.left = new FormAttachment(0, margin);
    fdIgnoreEmpty.top = new FormAttachment(wEncoding, margin);
    wIgnoreEmptyFile.setLayoutData(fdIgnoreEmpty);

    wDoNotFailIfNoFile = new Button(gOptions, SWT.CHECK);
    wDoNotFailIfNoFile.setText(
        BaseMessages.getString(PKG, "VCardInputFileDialog.DoNotFailIfNoFile.Label"));
    PropsUi.setLook(wDoNotFailIfNoFile);
    FormData fdDoNotFail = new FormData();
    fdDoNotFail.left = new FormAttachment(middle, 0);
    fdDoNotFail.top = new FormAttachment(wEncoding, margin);
    wDoNotFailIfNoFile.setLayoutData(fdDoNotFail);

    wAddResult = new Button(gOptions, SWT.CHECK);
    wAddResult.setText(BaseMessages.getString(PKG, "VCardInputFileDialog.AddResult.Label"));
    PropsUi.setLook(wAddResult);
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(0, margin);
    fdAddResult.top = new FormAttachment(wIgnoreEmptyFile, margin);
    wAddResult.setLayoutData(fdAddResult);

    FormData fdOptions = new FormData();
    fdOptions.left = new FormAttachment(0, 0);
    fdOptions.right = new FormAttachment(100, 0);
    fdOptions.bottom = new FormAttachment(wbShowFiles, -margin);
    gOptions.setLayoutData(fdOptions);

    FormData fdAccepting = new FormData();
    fdAccepting.left = new FormAttachment(0, 0);
    fdAccepting.right = new FormAttachment(100, 0);
    fdAccepting.bottom = new FormAttachment(gOptions, -margin);
    gAccepting.setLayoutData(fdAccepting);

    ColumnInfo[] colinfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardInputFileDialog.Column.Filename"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardInputFileDialog.Column.Filemask"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardInputFileDialog.Column.ExcludeFilemask"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardInputFileDialog.Column.Required"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              VCardInputMeta.REQUIRED_FILES_DESC),
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardInputFileDialog.Column.IncludeSubfolders"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              VCardInputMeta.REQUIRED_FILES_DESC)
        };
    colinfo[0].setUsingVariables(true);
    colinfo[1].setUsingVariables(true);
    colinfo[2].setUsingVariables(true);

    wFilenameList =
        new TableView(
            variables,
            parent,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colinfo,
            4,
            lsMod,
            props);
    PropsUi.setLook(wFilenameList);
    FormData fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment(middle, 0);
    fdFilenameList.right = new FormAttachment(wbdFilename, -margin);
    fdFilenameList.top = new FormAttachment(wExcludeFilemask, margin);
    fdFilenameList.bottom = new FormAttachment(gAccepting, -margin);
    wFilenameList.setLayoutData(fdFilenameList);

    SelectionAdapter addFile =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            if (org.apache.hop.core.util.Utils.isEmpty(wFilename.getText())) {
              return;
            }
            wFilenameList.add(
                wFilename.getText(),
                wFilemask.getText(),
                wExcludeFilemask.getText(),
                BaseMessages.getString("System.Combo.No"),
                BaseMessages.getString("System.Combo.No"));
            wFilename.setText("");
            wFilemask.setText("");
            wExcludeFilemask.setText("");
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
            wFilenameList.optWidth(true);
          }
        };
    wbaFilename.addSelectionListener(addFile);
    wFilename.addSelectionListener(addFile);

    wbbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wFilename,
                variables,
                new String[] {"*.vcf", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "VCardInputFileDialog.FileType.VCard"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));

    wbdFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            wFilenameList.remove(wFilenameList.getSelectionIndices());
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
          }
        });

    wbeFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
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

    wbShowFiles.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            VCardInputMeta probe = new VCardInputMeta();
            save(probe);
            try {
              FileInputList fileInputList = probe.getFileInputList(variables);
              String[] files = fileInputList.getFileStrings();
              if (files != null && files.length > 0) {
                EnterSelectionDialog esd =
                    new EnterSelectionDialog(
                        shell,
                        files,
                        BaseMessages.getString(PKG, "VCardInputFileDialog.ShowFiles.DialogTitle"),
                        BaseMessages.getString(
                            PKG, "VCardInputFileDialog.ShowFiles.DialogMessage"));
                esd.setViewOnly();
                esd.open();
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(
                    BaseMessages.getString(PKG, "VCardInputFileDialog.ShowFiles.NoFiles"));
                mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
                mb.open();
              }
            } catch (Exception ex) {
              new ErrorDialog(
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
                  ex.getMessage(),
                  ex);
            }
          }
        });

    wAccFilenames.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setFlags();
          }
        });
  }

  private void activeIncludeFilename() {
    boolean include = wIncludeFilename.getSelection();
    wlIncludeFilenameField.setEnabled(include);
    wIncludeFilenameField.setEnabled(include);
  }

  public void setFlags() {
    gAccepting.setEnabled(true);
    wlAccFilenames.setEnabled(true);
    wAccFilenames.setEnabled(true);

    boolean accept = wAccFilenames.getSelection();
    wlPassThruFields.setEnabled(accept);
    wPassThruFields.setEnabled(accept);
    if (!accept) {
      wPassThruFields.setSelection(false);
    }
    wlAccField.setEnabled(accept);
    wAccField.setEnabled(accept);
    wlAccTransform.setEnabled(accept);
    wAccTransform.setEnabled(accept);

    wlFilename.setEnabled(!accept);
    wbbFilename.setEnabled(!accept);
    wbdFilename.setEnabled(!accept);
    wbeFilename.setEnabled(!accept);
    wbaFilename.setEnabled(!accept);
    wFilename.setEnabled(!accept);
    wlFilenameList.setEnabled(!accept);
    wFilenameList.setEnabled(!accept);
    wlFilemask.setEnabled(!accept);
    wFilemask.setEnabled(!accept);
    wlExcludeFilemask.setEnabled(!accept);
    wExcludeFilemask.setEnabled(!accept);
    wbShowFiles.setEnabled(!accept);
  }

  public void load(VCardInputMeta meta) {
    wFilenameList.removeAll();
    for (InputFile inputFile : meta.getFileInput().getInputFiles()) {
      wFilenameList.add(
          inputFile.getFileName(),
          inputFile.getFileMask(),
          inputFile.getExcludeFileMask(),
          inputFile.getFileRequiredDesc(),
          inputFile.getIncludeSubFoldersDesc());
    }
    wFilenameList.optimizeTableView();
    wAccFilenames.setSelection(meta.getFileInput().isAcceptingFilenames());
    wAccTransform.setText(Const.NVL(meta.getFileInput().getAcceptingTransformName(), ""));
    wAccField.setText(Const.NVL(meta.getFileInput().getAcceptingField(), ""));
    wPassThruFields.setSelection(meta.getFileInput().isPassingThruFields());
    wIncludeFilename.setSelection(meta.isIncludeFilename());
    wIncludeFilenameField.setText(Const.NVL(meta.getFilenameField(), ""));
    wEncoding.setText(Const.NVL(meta.getEncoding(), Const.UTF_8));
    wIgnoreEmptyFile.setSelection(meta.isIgnoringEmptyFile());
    wDoNotFailIfNoFile.setSelection(meta.isDoNotFailIfNoFile());
    wAddResult.setSelection(meta.getFileInput().isAddingResult());
    activeIncludeFilename();
    setFlags();
  }

  public void save(VCardInputMeta meta) {
    List<InputFile> inputFiles = new ArrayList<>();
    for (int i = 0; i < wFilenameList.nrNonEmpty(); i++) {
      TableItem item = wFilenameList.getNonEmpty(i);
      InputFile inputFile = new InputFile();
      inputFile.setFileName(item.getText(1));
      inputFile.setFileMask(item.getText(2));
      inputFile.setExcludeFileMask(item.getText(3));
      inputFile.setFileRequired(CONST_SYSTEM_COMBO_YES.equalsIgnoreCase(item.getText(4)));
      inputFile.setIncludeSubFolders(CONST_SYSTEM_COMBO_YES.equalsIgnoreCase(item.getText(5)));
      inputFiles.add(inputFile);
    }
    meta.getFileInput().setInputFiles(inputFiles);
    meta.getFileInput().setAcceptingFilenames(wAccFilenames.getSelection());
    meta.getFileInput().setAcceptingTransformName(wAccTransform.getText());
    meta.getFileInput().setAcceptingField(wAccField.getText());
    meta.getFileInput().setPassingThruFields(wPassThruFields.getSelection());
    meta.setIncludeFilename(wIncludeFilename.getSelection());
    meta.setFilenameField(wIncludeFilenameField.getText());
    meta.setEncoding(wEncoding.getText());
    meta.setIgnoringEmptyFile(wIgnoreEmptyFile.getSelection());
    meta.setDoNotFailIfNoFile(wDoNotFailIfNoFile.getSelection());
    meta.getFileInput().setAddingResult(wAddResult.getSelection());
  }
}
