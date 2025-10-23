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

package org.apache.hop.pipeline.transforms.getfilenames;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.WidgetUtils;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
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
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class GetFileNamesDialog extends BaseTransformDialog {
  private static final Class<?> PKG = GetFileNamesMeta.class;

  private static final String[] YES_NO_COMBO =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  private Button wDoNotFailIfNoFile;
  private Label wlDoNotFailIfNoFile;

  private Label wlRaiseAnExceptionIfNoFile;
  private Button wRaiseAnExceptionIfNoFile;

  private TextVar wFilename;
  private TableView wFilenameList;
  private TextVar wExcludeFilemask;
  private TextVar wFilemask;
  private CCombo wFilterFileType;
  private final GetFileNamesMeta input;
  private Button wFileField;
  private CCombo wFilenameField;
  private CCombo wWildcardField;
  private CCombo wExcludeWildcardField;
  private Button wIncludeSubFolder;
  private Button wAddResult;
  private Text wLimit;
  private Button wInclRownum;
  private TextVar wInclRownumField;
  private boolean getPreviousFields = false;
  private Group groupAsDefined;
  private Group groupFromField;
  private Label wFileFieldWarning;

  public GetFileNamesDialog(
      Shell parent,
      IVariables variables,
      GetFileNamesMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  private void createTransformName(Shell shell, int margin, int middle, ModifyListener lsMod) {
    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    wlTransformName.setLayoutData(
        new FormDataBuilder().left(0, margin).top(0, margin).right(middle, -2 * margin).result());

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    wTransformName.setLayoutData(
        new FormDataBuilder().left(middle, -margin).top(0, margin).right(100, -margin).result());
  }

  private void createDialogButtons(Shell shell, int margin) {
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());

    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.Preview.Button"));
    wPreview.addListener(SWT.Selection, e -> preview());

    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);
  }

  private Group createGroupOpMode(Composite parent, int margin, int middle) {
    Group group = new Group(parent, SWT.SHADOW_NONE);
    PropsUi.setLook(group);
    group.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.Group.OpMode.Label"));
    WidgetUtils.setFormLayout(group, 10);

    Label label = new Label(group, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.FileField.Label"));
    PropsUi.setLook(label);
    label.setLayoutData(
        new FormDataBuilder()
            .left(0, -margin)
            .top(group, margin)
            .right(middle, -2 * margin)
            .result());

    wFileField = new Button(group, SWT.CHECK);
    PropsUi.setLook(wFileField);
    wFileField.setToolTipText(BaseMessages.getString(PKG, "GetFileNamesDialog.FileField.Tooltip"));
    wFileField.setLayoutData(new FormDataBuilder().left(middle, -margin).top(0, margin).result());

    wFileField.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            activateFileField();
            setFileField();
            input.setChanged();
          }
        });

    wFileFieldWarning = new Label(group, SWT.LEFT);
    wFileFieldWarning.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.FileField.Warning"));
    PropsUi.setLook(wFileFieldWarning);
    wFileFieldWarning.setLayoutData(
        new FormDataBuilder()
            .left(wFileField, margin)
            .top(label, 0, SWT.CENTER)
            .right(100, -margin)
            .result());

    return group;
  }

  private Group createGroupFromField(
      Composite parent, int margin, int middle, ModifyListener lsMod) {
    Group group = new Group(parent, SWT.SHADOW_NONE);
    PropsUi.setLook(group);
    group.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.Group.FromField.Label"));
    WidgetUtils.setFormLayout(group, 10);

    // Filename field
    Label wlFilenameField = new Label(group, SWT.RIGHT);
    wlFilenameField.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.FilenameField.Label"));
    PropsUi.setLook(wlFilenameField);
    wlFilenameField.setLayoutData(
        new FormDataBuilder()
            .left(0, -margin)
            .top(wFileField, margin)
            .right(middle, -2 * margin)
            .result());

    wFilenameField = new CCombo(group, SWT.BORDER | SWT.READ_ONLY);
    wFilenameField.setEditable(true);
    PropsUi.setLook(wFilenameField);
    wFilenameField.addModifyListener(lsMod);
    wFilenameField.setLayoutData(
        new FormDataBuilder()
            .left(middle, -margin)
            .top(wFileField, margin)
            .right(100, -margin)
            .result());

    // Wildcard field
    Label wlWildcardField = new Label(group, SWT.RIGHT);
    wlWildcardField.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.WildcardField.Label"));
    PropsUi.setLook(wlWildcardField);
    wlWildcardField.setLayoutData(
        new FormDataBuilder()
            .left(0, -margin)
            .top(wFilenameField, margin)
            .right(middle, -2 * margin)
            .result());

    wWildcardField = new CCombo(group, SWT.BORDER | SWT.READ_ONLY);
    wWildcardField.setEditable(true);
    PropsUi.setLook(wWildcardField);
    wWildcardField.addModifyListener(lsMod);
    wWildcardField.setLayoutData(
        new FormDataBuilder()
            .left(middle, -margin)
            .top(wFilenameField, margin)
            .right(100, -margin)
            .result());

    // ExcludeWildcard field
    Label wlExcludeWildcardField = new Label(group, SWT.RIGHT);
    wlExcludeWildcardField.setText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.ExcludeWildcardField.Label"));
    PropsUi.setLook(wlExcludeWildcardField);
    wlExcludeWildcardField.setLayoutData(
        new FormDataBuilder()
            .left(0, -margin)
            .top(wWildcardField, margin)
            .right(middle, -2 * margin)
            .result());

    wExcludeWildcardField = new CCombo(group, SWT.BORDER | SWT.READ_ONLY);
    wExcludeWildcardField.setEditable(true);
    PropsUi.setLook(wExcludeWildcardField);
    wExcludeWildcardField.addModifyListener(lsMod);
    wExcludeWildcardField.setLayoutData(
        new FormDataBuilder()
            .left(middle, -margin)
            .top(wWildcardField, margin)
            .right(100, -margin)
            .result());

    // Is includeSubFoldername defined in a Field
    Label wlIncludeSubFolder = new Label(group, SWT.RIGHT);
    wlIncludeSubFolder.setText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.IncludeSubFolder.Label"));
    PropsUi.setLook(wlIncludeSubFolder);
    wlIncludeSubFolder.setLayoutData(
        new FormDataBuilder()
            .left(0, -margin)
            .top(wExcludeWildcardField, margin)
            .right(middle, -2 * margin)
            .result());

    wIncludeSubFolder = new Button(group, SWT.CHECK);
    PropsUi.setLook(wIncludeSubFolder);
    wIncludeSubFolder.setToolTipText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.IncludeSubFolder.Tooltip"));
    wIncludeSubFolder.setLayoutData(
        new FormDataBuilder()
            .left(middle, -margin)
            .top(wlIncludeSubFolder, 0, SWT.CENTER)
            .result());

    wIncludeSubFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            input.setChanged();
          }
        });

    return group;
  }

  private Group createGroupAsDefined(
      Composite parent, int margin, int middle, ModifyListener lsMod) {
    Button wbShowFiles;
    Label wlFilemask;
    Label wlExcludeFilemask;
    Label wlFilenameList;
    Button wbaFilename;
    Button wbeFilename;
    Button wbdFilename;
    Button wbbFilename;
    Label wlFilename;
    Group group = new Group(parent, SWT.SHADOW_NONE);
    PropsUi.setLook(group);
    group.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.Group.ByDefinition.Label"));
    WidgetUtils.setFormLayout(group, 10);

    // File or directory
    wlFilename = new Label(group, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    wlFilename.setLayoutData(
        new FormDataBuilder()
            .left(0, -margin)
            .top(wFileField, margin)
            .right(middle, -2 * margin)
            .result());

    // Button: Browse...
    wbbFilename = new Button(group, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    wbbFilename.setLayoutData(
        new FormDataBuilder().top(wlFilename, 0, SWT.CENTER).right(100, 0).result());
    wbbFilename.addListener(
        SWT.Selection,
        e -> {
          if (!Utils.isEmpty(wFilemask.getText()) || !Utils.isEmpty(wExcludeFilemask.getText())) {
            BaseDialog.presentDirectoryDialog(shell, wFilename, variables);
          } else {
            BaseDialog.presentFileDialog(
                shell,
                wFilename,
                variables,
                new String[] {"*.txt;*.csv", "*.csv", "*.txt", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "GetFileNamesDialog.FileType.TextAndCSVFiles"),
                  BaseMessages.getString(PKG, "System.FileType.CSVFiles"),
                  BaseMessages.getString(PKG, "System.FileType.TextFiles"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true);
          }
        });

    // Button: Add
    wbaFilename = new Button(group, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbaFilename);
    wbaFilename.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.FilenameAdd.Button"));
    wbaFilename.setToolTipText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.FilenameAdd.Tooltip"));
    wbaFilename.setLayoutData(
        new FormDataBuilder().top(wlFilename, 0, SWT.CENTER).right(wbbFilename, -margin).result());

    wFilename = new TextVar(variables, group, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    wFilename.setLayoutData(
        new FormDataBuilder()
            .left(middle, 0)
            .top(wlFilename, 0, SWT.CENTER)
            .right(wbaFilename, -margin)
            .result());

    SelectionAdapter addFileToListAdapter =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            wFilenameList.add(
                wFilename.getText(),
                wFilemask.getText(),
                wExcludeFilemask.getText(),
                GetFileNamesMeta.RequiredFilesCode[0],
                GetFileNamesMeta.RequiredFilesCode[0]);
            wFilename.setText("");
            wFilemask.setText("");
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
            wFilenameList.optWidth(true);
          }
        };
    wbaFilename.addSelectionListener(addFileToListAdapter);
    wFilename.addSelectionListener(addFileToListAdapter);

    // Inclusion wildcard (RegExp)
    wlFilemask = new Label(group, SWT.RIGHT);
    wlFilemask.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.Filemask.Label"));
    PropsUi.setLook(wlFilemask);
    wlFilemask.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wFilename, margin)
            .right(middle, -2 * margin)
            .result());

    wFilemask = new TextVar(variables, group, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilemask);
    wFilemask.addModifyListener(lsMod);
    wFilemask.setLayoutData(
        new FormDataBuilder()
            .left(middle, 0)
            .top(wlFilemask, 0, SWT.CENTER)
            .right(wFilename, 0, SWT.RIGHT)
            .result());

    // Exclusion wildcard (RegExp)
    wlExcludeFilemask = new Label(group, SWT.RIGHT);
    wlExcludeFilemask.setText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.ExcludeFilemask.Label"));
    PropsUi.setLook(wlExcludeFilemask);
    wlExcludeFilemask.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wlFilemask, margin)
            .right(middle, -2 * margin)
            .result());

    wExcludeFilemask = new TextVar(variables, group, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wExcludeFilemask);
    wExcludeFilemask.addModifyListener(lsMod);
    wExcludeFilemask.setLayoutData(
        new FormDataBuilder()
            .left(middle, 0)
            .top(wlExcludeFilemask, 0, SWT.CENTER)
            .right(wFilename, 0, SWT.RIGHT)
            .result());

    // Selected files:
    wlFilenameList = new Label(group, SWT.RIGHT);
    wlFilenameList.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.FilenameList.Label"));
    PropsUi.setLook(wlFilenameList);
    wlFilenameList.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wExcludeFilemask, margin)
            .right(middle, -2 * margin)
            .result());

    // Button: Delete
    wbdFilename = new Button(group, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbdFilename);
    wbdFilename.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.FilenameDelete.Button"));
    wbdFilename.setToolTipText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.FilenameDelete.Tooltip"));
    wbdFilename.setLayoutData(
        new FormDataBuilder().top(wExcludeFilemask, margin).right(100, 0).result());
    wbdFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            int[] idx = wFilenameList.getSelectionIndices();
            wFilenameList.remove(idx);
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
            input.setChanged();
          }
        });

    // Button: Edit
    wbeFilename = new Button(group, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbeFilename);
    wbeFilename.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.FilenameEdit.Button"));
    wbeFilename.setToolTipText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.FilenameEdit.Tooltip"));
    wbeFilename.setLayoutData(
        new FormDataBuilder()
            .left(wbdFilename, 0, SWT.LEFT)
            .top(wbdFilename, margin)
            .right(100, 0)
            .result());
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
            input.setChanged();
          }
        });

    // Button: Show filename(s)...
    wbShowFiles = new Button(group, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.ShowFiles.Button"));
    wbShowFiles.setLayoutData(new FormDataBuilder().left(middle, 0).bottom(100, 0).result());
    wbShowFiles.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            GetFileNamesMeta tfii = new GetFileNamesMeta();
            getInfo(tfii);
            String[] files = tfii.getFilePaths(variables);
            if (files != null && files.length > 0) {
              EnterSelectionDialog esd =
                  new EnterSelectionDialog(shell, files, "Files read", "Files read:");
              esd.setViewOnly();
              esd.open();
            } else {
              String elementTypeToGet =
                  FileInputList.FileTypeFilter.getByOrdinal(wFilterFileType.getSelectionIndex())
                      .toString();
              MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
              mb.setMessage(
                  BaseMessages.getString(
                      PKG, "GetFileNamesDialog.NoFilesFound.DialogMessage", elementTypeToGet));
              mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
              mb.open();
            }
          }
        });

    ColumnInfo[] colinfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "GetFileNamesDialog.FileDirColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "GetFileNamesDialog.WildcardColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "GetFileNamesDialog.ExcludeWildcardColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "GetFileNamesDialog.Required.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              YES_NO_COMBO),
          new ColumnInfo(
              BaseMessages.getString(PKG, "GetFileNamesDialog.IncludeSubDirs.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              YES_NO_COMBO)
        };

    colinfo[0].setUsingVariables(true);
    colinfo[1].setUsingVariables(true);
    colinfo[1].setToolTip(BaseMessages.getString(PKG, "GetFileNamesDialog.RegExpColumn.Column"));
    colinfo[2].setUsingVariables(true);
    colinfo[2].setToolTip(
        BaseMessages.getString(PKG, "GetFileNamesDialog.ExcludeRegExpColumn.Column"));
    colinfo[3].setToolTip(BaseMessages.getString(PKG, "GetFileNamesDialog.Required.Tooltip"));
    colinfo[4].setToolTip(BaseMessages.getString(PKG, "GetFileNamesDialog.IncludeSubDirs.ToolTip"));

    wFilenameList =
        new TableView(
            variables,
            group,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colinfo,
            colinfo.length,
            lsMod,
            props);
    PropsUi.setLook(wFilenameList);
    wFilenameList.setLayoutData(
        new FormDataBuilder()
            .left(middle, 0)
            .top(wExcludeFilemask, margin)
            .right(wbdFilename, -margin)
            .bottom(wbShowFiles, -margin)
            .result());

    return group;
  }

  private void createTabFile(CTabFolder wTabFolder, int margin, int middle, ModifyListener lsMod) {
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wFileTab.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.FileTab.TabTitle"));

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    composite.setLayoutData(
        new FormDataBuilder().left(0, 0).top(0, 0).right(100, 0).bottom(100, 0));
    PropsUi.setLook(composite);
    WidgetUtils.setFormLayout(composite, 3);

    Group opModeGroup = createGroupOpMode(composite, margin, middle);
    opModeGroup.setLayoutData(
        new FormDataBuilder().left(0, margin).top(0, margin).right(100, -margin).result());

    FormData belowOpModeGroupLayout =
        new FormDataBuilder()
            .left(0, margin)
            .top(opModeGroup, margin)
            .right(100, -margin)
            .bottom(100, -margin)
            .result();

    groupAsDefined = createGroupAsDefined(composite, margin, middle, lsMod);
    groupAsDefined.setLayoutData(belowOpModeGroupLayout);

    groupFromField = createGroupFromField(composite, margin, middle, lsMod);
    groupFromField.setLayoutData(belowOpModeGroupLayout);

    composite.layout();
    wFileTab.setControl(composite);
  }

  private void createTabFilter(
      CTabFolder wTabFolder, int margin, int middle, ModifyListener lsMod) {

    CTabItem wFilterTab = new CTabItem(wTabFolder, SWT.NONE);
    wFilterTab.setFont(GuiResource.getInstance().getFontDefault());
    wFilterTab.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.FilterTab.TabTitle"));

    Composite wFilterComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFilterComp);
    WidgetUtils.setFormLayout(wFilterComp, 3);

    // Filter File Type
    Label wlFilterFileType = new Label(wFilterComp, SWT.RIGHT);
    wlFilterFileType.setText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.FilterTab.FileType.Label"));
    PropsUi.setLook(wlFilterFileType);
    wlFilterFileType.setLayoutData(
        new FormDataBuilder().left(0, margin).top(0, 3 * margin).right(middle, -margin).result());

    wFilterFileType = new CCombo(wFilterComp, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wFilterFileType.add(
        BaseMessages.getString(PKG, "GetFileNamesDialog.FilterTab.FileType.All.Label"));
    wFilterFileType.add(
        BaseMessages.getString(PKG, "GetFileNamesDialog.FilterTab.FileType.OnlyFile.Label"));
    wFilterFileType.add(
        BaseMessages.getString(PKG, "GetFileNamesDialog.FilterTab.FileType.OnlyFolder.Label"));
    PropsUi.setLook(wFilterFileType);
    wFilterFileType.setLayoutData(
        new FormDataBuilder().left(middle, 0).top(0, 3 * margin).right(100, -3 * margin).result());
    wFilterFileType.addModifyListener(lsMod);

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    Group wAdditionalGroup = new Group(wFilterComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAdditionalGroup);
    wAdditionalGroup.setText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.Group.AdditionalGroup.Label"));
    WidgetUtils.setFormLayout(wAdditionalGroup, 10);
    wAdditionalGroup.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wFilterFileType, margin)
            .right(100, -margin)
            .result());

    Label wlInclRownum = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclRownum.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.InclRownum.Label"));
    PropsUi.setLook(wlInclRownum);
    wlInclRownum.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wFilterFileType, margin)
            .right(middle, -2 * margin)
            .result());

    wInclRownum = new Button(wAdditionalGroup, SWT.CHECK);
    PropsUi.setLook(wInclRownum);
    wInclRownum.setToolTipText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.InclRownum.Tooltip"));
    wInclRownum.setLayoutData(
        new FormDataBuilder()
            .left(middle, -margin)
            .top(wlInclRownum, 0, SWT.CENTER)
            .right(100, -margin)
            .result());
    wInclRownum.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            input.setChanged();
            wInclRownumField.setEnabled(wInclRownum.getSelection());
          }
        });

    Label wlInclRownumField = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclRownumField.setText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.InclRownumField.Label"));
    PropsUi.setLook(wlInclRownumField);
    wlInclRownumField.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wlInclRownum, margin)
            .right(middle, -2 * margin)
            .result());

    wInclRownumField = new TextVar(variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclRownumField);
    wInclRownumField.addModifyListener(lsMod);
    wInclRownumField.setLayoutData(
        new FormDataBuilder()
            .left(middle, -margin)
            .top(wInclRownum, margin)
            .right(100, -margin)
            .result());

    // ///////////////////////////////////////////////////////////
    // / END OF Additional Fields GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF No Files Folder GROUP
    // /////////////////////////////////

    Group wNoFilesFolderGroup = new Group(wFilterComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wNoFilesFolderGroup);
    wNoFilesFolderGroup.setText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.Group.NoFilesFolder.Label"));
    WidgetUtils.setFormLayout(wNoFilesFolderGroup, 10);

    // do not fail if no files?
    wlDoNotFailIfNoFile = new Label(wNoFilesFolderGroup, SWT.RIGHT);
    wlDoNotFailIfNoFile.setText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.DoNotFailIfNoFile.Label"));
    PropsUi.setLook(wlDoNotFailIfNoFile);
    wlDoNotFailIfNoFile.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wAdditionalGroup, margin)
            .right(middle, -2 * margin)
            .result());
    wDoNotFailIfNoFile = new Button(wNoFilesFolderGroup, SWT.CHECK);
    PropsUi.setLook(wDoNotFailIfNoFile);
    wDoNotFailIfNoFile.setToolTipText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.DoNotFailIfNoFile.Tooltip"));
    wDoNotFailIfNoFile.setLayoutData(
        new FormDataBuilder()
            .left(middle, -margin)
            .top(wlDoNotFailIfNoFile, 0, SWT.CENTER)
            .result());

    // Raise an exception if no file?
    wlRaiseAnExceptionIfNoFile = new Label(wNoFilesFolderGroup, SWT.RIGHT);
    wlRaiseAnExceptionIfNoFile.setText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.RaiseAnExceptionIfNoFiles.Label"));
    PropsUi.setLook(wlRaiseAnExceptionIfNoFile);
    wlRaiseAnExceptionIfNoFile.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wlDoNotFailIfNoFile, margin)
            .right(middle, -2 * margin)
            .result());

    wRaiseAnExceptionIfNoFile = new Button(wNoFilesFolderGroup, SWT.CHECK);
    PropsUi.setLook(wRaiseAnExceptionIfNoFile);
    wRaiseAnExceptionIfNoFile.setToolTipText(
        BaseMessages.getString(PKG, "GetFileNamesDialog.RaiseAnExceptionIfNoFiles.Tooltip"));
    wRaiseAnExceptionIfNoFile.setLayoutData(
        new FormDataBuilder()
            .left(middle, -margin)
            .top(wlRaiseAnExceptionIfNoFile, 0, SWT.CENTER)
            .result());
    wRaiseAnExceptionIfNoFile.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            wDoNotFailIfNoFile.setSelection(false);
            wlDoNotFailIfNoFile.setEnabled(!wRaiseAnExceptionIfNoFile.getSelection());
            wDoNotFailIfNoFile.setEnabled(!wRaiseAnExceptionIfNoFile.getSelection());
            input.setChanged();
          }
        });

    wDoNotFailIfNoFile.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            wlRaiseAnExceptionIfNoFile.setEnabled(!wDoNotFailIfNoFile.getSelection());
            wRaiseAnExceptionIfNoFile.setEnabled(!wDoNotFailIfNoFile.getSelection());
            input.setChanged();
          }
        });

    wNoFilesFolderGroup.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wAdditionalGroup, margin)
            .right(100, -margin)
            .result());

    // /////////////////////////////////
    // END OF No Files Folder GROUP
    // /////////////////////////////////

    Label wlLimit = new Label(wFilterComp, SWT.RIGHT);
    PropsUi.setLook(wlLimit);
    wlLimit.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.Limit.Label"));
    wlLimit.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wNoFilesFolderGroup, margin)
            .right(middle, -margin)
            .result());

    wLimit = new Text(wFilterComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    wLimit.setLayoutData(
        new FormDataBuilder()
            .left(middle, 0)
            .top(wNoFilesFolderGroup, margin)
            .right(100, -3 * margin)
            .result());

    Label wlAddResult = new Label(wFilterComp, SWT.RIGHT);
    PropsUi.setLook(wlAddResult);
    wlAddResult.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.AddResult.Label"));
    wlAddResult.setLayoutData(
        new FormDataBuilder().left(0, margin).top(wLimit, margin).right(middle, -margin).result());

    wAddResult = new Button(wFilterComp, SWT.CHECK);
    PropsUi.setLook(wAddResult);
    wAddResult.setToolTipText(BaseMessages.getString(PKG, "GetFileNamesDialog.AddResult.Tooltip"));
    wAddResult.setLayoutData(
        new FormDataBuilder().left(middle, 0).top(wlAddResult, 0, SWT.CENTER).result());
    wAddResult.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            input.setChanged();
          }
        });

    wFilterComp.setLayoutData(
        new FormDataBuilder().left(0, 0).top(0, 0).right(100, 0).bottom(100, 0).result());
    wFilterComp.layout();
    wFilterTab.setControl(wFilterComp);
  }

  @Override
  public String open() {
    Shell parentShell = getParent();

    shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    shell.setText(BaseMessages.getString(PKG, "GetFileNamesDialog.DialogTitle"));
    PropsUi.setLook(shell);
    setShellImage(shell, input);
    WidgetUtils.setFormLayout(shell, PropsUi.getFormMargin());

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    createTransformName(shell, margin, middle, lsMod);
    createDialogButtons(shell, margin);

    CTabFolder tabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(tabFolder, Props.WIDGET_STYLE_TAB);

    tabFolder.setLayoutData(
        new FormDataBuilder()
            .left(0, 0)
            .top(wTransformName, margin)
            .right(100, 0)
            .bottom(wOk, -2 * margin)
            .result());

    createTabFile(tabFolder, margin, middle, lsMod);
    createTabFilter(tabFolder, margin, middle, lsMod);
    tabFolder.setSelection(0);

    setFileField();
    getData(input);
    activateFileField();
    setErrorsMgmtCheckboxesStatus();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setFileField() {
    try {
      if (!getPreviousFields) {
        getPreviousFields = true;
        String filename = wFilenameField.getText();
        String wildcard = wWildcardField.getText();
        String excludewildcard = wExcludeWildcardField.getText();

        wFilenameField.removeAll();
        wWildcardField.removeAll();
        wExcludeWildcardField.removeAll();

        IRowMeta r = null;

        try {
          r = pipelineMeta.getPrevTransformFields(variables, transformName);
        } catch (NullPointerException e) {
          // thrown when previous transform is not yet properly configured
          LogChannel.UI.logBasic("Failed to read previous transform fields", e);
        }

        if (r != null) {
          wFilenameField.setItems(r.getFieldNames());
          wWildcardField.setItems(r.getFieldNames());
          wExcludeWildcardField.setItems(r.getFieldNames());
        }
        if (filename != null) {
          wFilenameField.setText(filename);
        }
        if (wildcard != null) {
          wWildcardField.setText(wildcard);
        }
        if (excludewildcard != null) {
          wExcludeWildcardField.setText(excludewildcard);
        }
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GetFileNamesDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "GetFileNamesDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void activateFileField() {
    boolean isFromField = wFileField.getSelection();
    groupFromField.setVisible(isFromField);

    for (Control c : groupFromField.getChildren()) {
      c.setEnabled(isFromField);
    }

    groupAsDefined.setVisible(!isFromField);

    for (Control c : groupAsDefined.getChildren()) {
      c.setEnabled(!isFromField);
    }

    wPreview.setEnabled(!isFromField);
    wFileFieldWarning.setVisible(!isFromField && hasPreviousTransform());
  }

  private boolean hasPreviousTransform() {
    return pipelineMeta.findPipelineHopTo(transformMeta) != null;
  }

  protected void setErrorsMgmtCheckboxesStatus() {

    if (wRaiseAnExceptionIfNoFile.getSelection()) {
      wDoNotFailIfNoFile.setSelection(false);
    }
    wlRaiseAnExceptionIfNoFile.setEnabled(!wDoNotFailIfNoFile.getSelection());
    wRaiseAnExceptionIfNoFile.setEnabled(!wDoNotFailIfNoFile.getSelection());

    if (wDoNotFailIfNoFile.getSelection()) {
      wRaiseAnExceptionIfNoFile.setSelection(false);
    }
    wlDoNotFailIfNoFile.setEnabled(!wRaiseAnExceptionIfNoFile.getSelection());
    wDoNotFailIfNoFile.setEnabled(!wRaiseAnExceptionIfNoFile.getSelection());
  }

  /**
   * Read the data from the GetFileNamesMeta object and show it in this dialog.
   *
   * @param in The TextFileInputMeta object to obtain the data from.
   */
  public void getData(final GetFileNamesMeta in) {
    if (!in.getFilesList().isEmpty()) {
      wFilenameList.removeAll();

      for (int i = 0; i < in.getFilesList().size(); i++) {
        FileItem fi = in.getFilesList().get(i);
        wFilenameList.add(
            fi.getFileName(),
            fi.getFileMask(),
            fi.getExcludeFileMask(),
            fi.getFileRequired(),
            fi.getIncludeSubFolders());
      }
    }

    wDoNotFailIfNoFile.setSelection(in.isDoNotFailIfNoFile());
    wRaiseAnExceptionIfNoFile.setSelection(in.isRaiseAnExceptionIfNoFile());
    wFilenameList.removeEmptyRows();
    wFilenameList.setRowNums();
    wFilenameList.optWidth(true);

    FileInputList.FileTypeFilter elementTypeToGet =
        FileInputList.FileTypeFilter.getByName(
            in.getFilterItemList().get(0).getFileTypeFilterSelection());
    if (elementTypeToGet != null) {
      wFilterFileType.select(elementTypeToGet.ordinal());
    } else {
      wFilterFileType.select(0);
    }

    wInclRownum.setSelection(in.isIncludeRowNumber());
    wInclRownumField.setEnabled(wInclRownum.getSelection());
    wAddResult.setSelection(in.isAddResultFile());
    wFileField.setSelection(in.isFileField());
    if (in.getRowNumberField() != null) {
      wInclRownumField.setText(in.getRowNumberField());
    }
    if (in.getDynamicFilenameField() != null) {
      wFilenameField.setText(in.getDynamicFilenameField());
    }
    if (in.getDynamicWildcardField() != null) {
      wWildcardField.setText(in.getDynamicWildcardField());
    }
    if (in.getDynamicExcludeWildcardField() != null) {
      wExcludeWildcardField.setText(in.getDynamicExcludeWildcardField());
    }
    wLimit.setText("" + in.getRowLimit());
    wIncludeSubFolder.setSelection(in.isDynamicIncludeSubFolders());

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    getInfo(input);
    dispose();
  }

  private void getInfo(GetFileNamesMeta in) {

    transformName = wTransformName.getText(); // return value
    int itemsNum = wFilenameList.getItemCount();
    in.getFilesList().clear();

    for (int i = 0; i < itemsNum; i++) {

      FileItem fi =
          new FileItem(
              wFilenameList.getItem(i, 1),
              wFilenameList.getItem(i, 2),
              wFilenameList.getItem(i, 3),
              wFilenameList.getItem(i, 4),
              wFilenameList.getItem(i, 5));
      in.getFilesList().add(fi);
    }

    in.getFilterItemList().clear();
    in.getFilterItemList()
        .add(
            new FilterItem(
                FileInputList.FileTypeFilter.getByOrdinal(wFilterFileType.getSelectionIndex())
                    .toString()));

    in.setIncludeRowNumber(wInclRownum.getSelection());
    in.setAddResultFile(wAddResult.getSelection());
    in.setDynamicFilenameField(wFilenameField.getText());
    in.setDynamicWildcardField(wWildcardField.getText());
    in.setDynamicExcludeWildcardField(wExcludeWildcardField.getText());
    in.setFileField(wFileField.getSelection());
    in.setRowNumberField(wInclRownum.getSelection() ? wInclRownumField.getText() : "");
    in.setRowLimit(Const.toLong(wLimit.getText(), 0L));
    in.setDynamicIncludeSubFolders(wIncludeSubFolder.getSelection());
    in.setDoNotFailIfNoFile(wDoNotFailIfNoFile.getSelection());
    in.setRaiseAnExceptionIfNoFile(wRaiseAnExceptionIfNoFile.getSelection());
  }

  // Preview the data
  private void preview() {
    // Create the XML input transform
    GetFileNamesMeta oneMeta = new GetFileNamesMeta();
    getInfo(oneMeta);

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "GetFileNamesDialog.PreviewSize.DialogTitle"),
            BaseMessages.getString(PKG, "GetFileNamesDialog.PreviewSize.DialogMessage"));
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
                  BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
                  BaseMessages.getString(PKG, "GetFileNamesDialog.ErrorInPreview.DialogMessage"),
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
  }
}
