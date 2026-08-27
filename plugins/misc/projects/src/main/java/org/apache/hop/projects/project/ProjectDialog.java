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

package org.apache.hop.projects.project;

import java.io.File;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.gui.ProjectsGuiPlugin;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class ProjectDialog extends Dialog {
  private static final Class<?> PKG = ProjectDialog.class;
  public static final String CONST_PROJECT = "Project '";
  private static final String[] YES_NO = {"Y", "N"};

  private final Project project;
  private final ProjectConfig projectConfig;

  private String returnValue;

  private Shell shell;
  private final PropsUi props;

  private Text wName;
  private TextVar wHome;
  private Button wReadOnly;
  private ComboVar wParentProject;
  private TextVar wConfigFile;
  private Button wbConfigFile;
  private Text wDescription;
  private Text wCompany;
  private Text wDepartment;
  private Text wVersion;

  private TextVar wMetadataBaseFolder;
  private Button wAutoExportMetadata;
  private TextVar wAutoExportMetadataFilename;
  private TextVar wUnitTestsBasePath;
  private TextVar wDataSetCsvFolder;
  private Button wEnforceHomeExecution;
  private TableView wVariables;
  private TableView wParentFolders;

  private final IVariables variables;

  @Getter @Setter private boolean needingProjectRefresh;

  private final boolean editMode;

  public ProjectDialog(
      Shell parent,
      Project project,
      ProjectConfig projectConfig,
      IVariables variables,
      boolean editMode) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);

    this.project = project;
    this.projectConfig = projectConfig;
    this.editMode = editMode;

    props = PropsUi.getInstance();

    this.variables = new Variables();
    this.variables.initializeFrom(null);
    try {
      project.modifyVariables(variables, projectConfig, Collections.emptyList(), null);
    } catch (Exception e) {
      if (ProjectsGuiPlugin.extractMissingProjectPath(e) == null) {
        new ErrorDialog(
            parent,
            BaseMessages.getString(PKG, "ProjectDialog.ProjectDefinitionError.Error.Dialog.Header"),
            BaseMessages.getString(
                PKG, "ProjectDialog.ProjectDefinitionError.Error.Dialog.Message"),
            e);
      }
      // When the project folder does not exist, allow the dialog to open so the user can
      // update the path in the configuration.
    }
  }

  public String open() {

    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setImage(
        GuiResource.getInstance()
            .getImage(
                "project.svg",
                PKG.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));

    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin() + 2;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ProjectDialog.Shell.Name"));

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, event -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin * 3, null);
    HelpUtils.createHelpButton(shell, Const.getDocUrl(Defaults.DOCUMENTATION_URI));

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.top = new FormAttachment(0, 0);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.bottom = new FormAttachment(wOk, -margin * 2);
    wTabFolder.setLayoutData(fdTabs);

    createBasicTab(wTabFolder, margin);
    createFoldersTab(wTabFolder, margin);
    createParentProjectTab(wTabFolder, margin);
    createVariablesTab(wTabFolder, margin);

    wParentProject.addModifyListener(
        e -> {
          needingProjectRefresh = true;
          updateParentFolderWidgets();
        });
    wHome.addModifyListener(
        e -> {
          needingProjectRefresh = true;
          autoSetReadOnlyFromHome();
        });

    getData();
    updateReadOnlyWidgets();
    updateAutoExportMetadataWidgets();
    updateParentFolderWidgets();

    wTabFolder.setSelection(0);
    shell.setMinimumSize(700, 450);
    shell.setDefaultButton(wOk);
    wName.setFocus();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return returnValue;
  }

  private Composite createTab(CTabFolder folder, String messageKey) {
    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, messageKey));
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);
    return comp;
  }

  private void createBasicTab(CTabFolder folder, int margin) {
    Composite comp = createTab(folder, "ProjectDialog.Tab.Basic");
    int middle = props.getMiddlePct();

    Label wlName = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "ProjectDialog.Label.ProjectName"));
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(100, 0);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    Label wlHome = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlHome);
    wlHome.setText(BaseMessages.getString(PKG, "ProjectDialog.Label.HomeFolder"));
    FormData fdlHome = new FormData();
    fdlHome.left = new FormAttachment(0, 0);
    fdlHome.right = new FormAttachment(middle, 0);
    fdlHome.top = new FormAttachment(lastControl, margin);
    wlHome.setLayoutData(fdlHome);
    Button wbHome = new Button(comp, SWT.PUSH);
    PropsUi.setLook(wbHome);
    wbHome.setText(BaseMessages.getString(PKG, "ProjectDialog.Button.Browse"));
    FormData fdbHome = new FormData();
    fdbHome.right = new FormAttachment(100, 0);
    fdbHome.top = new FormAttachment(wlHome, 0, SWT.CENTER);
    wbHome.setLayoutData(fdbHome);
    wbHome.addListener(SWT.Selection, this::browseHomeFolder);
    wHome = new TextVar(variables, comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wHome);
    FormData fdHome = new FormData();
    fdHome.left = new FormAttachment(middle, margin);
    fdHome.right = new FormAttachment(wbHome, -margin);
    fdHome.top = new FormAttachment(wlHome, 0, SWT.CENTER);
    wHome.setLayoutData(fdHome);
    lastControl = wHome;

    wReadOnly = new Button(comp, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wReadOnly);
    wReadOnly.setText(BaseMessages.getString(PKG, "ProjectDialog.Label.ReadOnly"));
    FormData fdReadOnly = new FormData();
    fdReadOnly.left = new FormAttachment(middle, margin);
    fdReadOnly.right = new FormAttachment(100, 0);
    fdReadOnly.top = new FormAttachment(lastControl, margin);
    wReadOnly.setLayoutData(fdReadOnly);
    wReadOnly.addListener(SWT.Selection, e -> updateReadOnlyWidgets());
    lastControl = wReadOnly;

    Label wlConfigFile = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlConfigFile);
    wlConfigFile.setText(BaseMessages.getString(PKG, "ProjectDialog.Label.ConfigurationFile"));
    FormData fdlConfigFile = new FormData();
    fdlConfigFile.left = new FormAttachment(0, 0);
    fdlConfigFile.right = new FormAttachment(middle, 0);
    fdlConfigFile.top = new FormAttachment(lastControl, margin);
    wlConfigFile.setLayoutData(fdlConfigFile);
    wbConfigFile = new Button(comp, SWT.PUSH);
    PropsUi.setLook(wbConfigFile);
    wbConfigFile.setText(BaseMessages.getString(PKG, "ProjectDialog.Button.Browse"));
    FormData fdbConfigFile = new FormData();
    fdbConfigFile.right = new FormAttachment(100, 0);
    fdbConfigFile.top = new FormAttachment(wlConfigFile, 0, SWT.CENTER);
    wbConfigFile.setLayoutData(fdbConfigFile);
    wbConfigFile.addListener(SWT.Selection, this::browseConfigFolder);
    wConfigFile = new TextVar(variables, comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wConfigFile);
    FormData fdConfigFile = new FormData();
    fdConfigFile.left = new FormAttachment(middle, margin);
    fdConfigFile.right = new FormAttachment(wbConfigFile, -margin);
    fdConfigFile.top = new FormAttachment(wlConfigFile, 0, SWT.CENTER);
    wConfigFile.setLayoutData(fdConfigFile);
    lastControl = wConfigFile;

    lastControl =
        addLabeledText(
            comp,
            middle,
            margin,
            lastControl,
            "ProjectDialog.Label.Description",
            wDescription = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT));
    lastControl =
        addLabeledText(
            comp,
            middle,
            margin,
            lastControl,
            "ProjectDialog.Label.Company",
            wCompany = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT));
    lastControl =
        addLabeledText(
            comp,
            middle,
            margin,
            lastControl,
            "ProjectDialog.Label.Department",
            wDepartment = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT));
    lastControl =
        addLabeledText(
            comp,
            middle,
            margin,
            lastControl,
            "ProjectDialog.Label.Version",
            wVersion = new Text(comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT));

    Label wlAutoExportMetadata = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlAutoExportMetadata);
    wlAutoExportMetadata.setText(
        BaseMessages.getString(PKG, "ProjectDialog.Label.AutoExportMetadata"));
    FormData fdlAutoExportMetadata = new FormData();
    fdlAutoExportMetadata.left = new FormAttachment(0, 0);
    fdlAutoExportMetadata.right = new FormAttachment(middle, 0);
    fdlAutoExportMetadata.top = new FormAttachment(lastControl, margin);
    wlAutoExportMetadata.setLayoutData(fdlAutoExportMetadata);
    wAutoExportMetadata = new Button(comp, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wAutoExportMetadata);
    wAutoExportMetadata.setText(
        BaseMessages.getString(PKG, "ProjectDialog.Label.AutoExportMetadata.Enable"));
    FormData fdAutoExportMetadata = new FormData();
    fdAutoExportMetadata.left = new FormAttachment(middle, margin);
    fdAutoExportMetadata.right = new FormAttachment(100, 0);
    fdAutoExportMetadata.top = new FormAttachment(wlAutoExportMetadata, 0, SWT.CENTER);
    wAutoExportMetadata.setLayoutData(fdAutoExportMetadata);
    wAutoExportMetadata.addListener(SWT.Selection, e -> updateAutoExportMetadataWidgets());
    lastControl = wlAutoExportMetadata;

    Label wlAutoExportMetadataFilename = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlAutoExportMetadataFilename);
    wlAutoExportMetadataFilename.setText(
        BaseMessages.getString(PKG, "ProjectDialog.Label.AutoExportMetadataFilename"));
    FormData fdlAutoExportMetadataFilename = new FormData();
    fdlAutoExportMetadataFilename.left = new FormAttachment(0, 0);
    fdlAutoExportMetadataFilename.right = new FormAttachment(middle, 0);
    fdlAutoExportMetadataFilename.top = new FormAttachment(lastControl, margin);
    wlAutoExportMetadataFilename.setLayoutData(fdlAutoExportMetadataFilename);
    wAutoExportMetadataFilename = new TextVar(variables, comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wAutoExportMetadataFilename);
    FormData fdAutoExportMetadataFilename = new FormData();
    fdAutoExportMetadataFilename.left = new FormAttachment(middle, margin);
    fdAutoExportMetadataFilename.right = new FormAttachment(100, 0);
    fdAutoExportMetadataFilename.top =
        new FormAttachment(wlAutoExportMetadataFilename, 0, SWT.CENTER);
    wAutoExportMetadataFilename.setLayoutData(fdAutoExportMetadataFilename);
  }

  private Text addLabeledText(
      Composite comp, int middle, int margin, Control lastControl, String labelKey, Text widget) {
    Label label = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, 0);
    fdl.top = new FormAttachment(lastControl, margin);
    label.setLayoutData(fdl);
    PropsUi.setLook(widget);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, margin);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(label, 0, SWT.CENTER);
    widget.setLayoutData(fd);
    return widget;
  }

  private void createFoldersTab(CTabFolder folder, int margin) {
    Composite comp = createTab(folder, "ProjectDialog.Tab.Folders");
    int middle = props.getMiddlePct();
    Control lastControl = null;

    lastControl =
        addLabeledTextVar(
            comp,
            middle,
            margin,
            lastControl,
            "ProjectDialog.Label.MetadataBaseFolder",
            wMetadataBaseFolder = new TextVar(variables, comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT));
    wMetadataBaseFolder.addModifyListener(e -> updateIVariables());

    lastControl =
        addLabeledTextVar(
            comp,
            middle,
            margin,
            lastControl,
            "ProjectDialog.Label.UnitTestBaseFolder",
            wUnitTestsBasePath = new TextVar(variables, comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT));
    wUnitTestsBasePath.addModifyListener(e -> updateIVariables());

    lastControl =
        addLabeledTextVar(
            comp,
            middle,
            margin,
            lastControl,
            "ProjectDialog.Label.DatasetCSVFolder",
            wDataSetCsvFolder = new TextVar(variables, comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT));
    wDataSetCsvFolder.addModifyListener(e -> updateIVariables());

    Label wlEnforceHomeExecution = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlEnforceHomeExecution);
    wlEnforceHomeExecution.setText(
        BaseMessages.getString(PKG, "ProjectDialog.Label.EnforceExecutionInHome"));
    FormData fdlEnforceHomeExecution = new FormData();
    fdlEnforceHomeExecution.left = new FormAttachment(0, 0);
    fdlEnforceHomeExecution.right = new FormAttachment(middle, 0);
    fdlEnforceHomeExecution.top = new FormAttachment(lastControl, margin);
    wlEnforceHomeExecution.setLayoutData(fdlEnforceHomeExecution);
    wEnforceHomeExecution = new Button(comp, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wEnforceHomeExecution);
    FormData fdEnforceHomeExecution = new FormData();
    fdEnforceHomeExecution.left = new FormAttachment(middle, margin);
    fdEnforceHomeExecution.right = new FormAttachment(100, 0);
    fdEnforceHomeExecution.top = new FormAttachment(wlEnforceHomeExecution, 0, SWT.CENTER);
    wEnforceHomeExecution.setLayoutData(fdEnforceHomeExecution);
  }

  private TextVar addLabeledTextVar(
      Composite comp,
      int middle,
      int margin,
      Control lastControl,
      String labelKey,
      TextVar widget) {
    Label label = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, 0);
    if (lastControl == null) {
      fdl.top = new FormAttachment(0, margin);
    } else {
      fdl.top = new FormAttachment(lastControl, margin);
    }
    label.setLayoutData(fdl);
    PropsUi.setLook(widget);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, margin);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(label, 0, SWT.CENTER);
    widget.setLayoutData(fd);
    return widget;
  }

  private void createParentProjectTab(CTabFolder folder, int margin) {
    Composite comp = createTab(folder, "ProjectDialog.Tab.ParentProject");
    int middle = props.getMiddlePct();

    Label wlParentProject = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlParentProject);
    wlParentProject.setText(BaseMessages.getString(PKG, "ProjectDialog.Label.ParentProject"));
    FormData fdlParentProject = new FormData();
    fdlParentProject.left = new FormAttachment(0, 0);
    fdlParentProject.right = new FormAttachment(middle, 0);
    fdlParentProject.top = new FormAttachment(0, margin);
    wlParentProject.setLayoutData(fdlParentProject);
    wParentProject = new ComboVar(variables, comp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wParentProject);
    FormData fdParentProject = new FormData();
    fdParentProject.left = new FormAttachment(middle, margin);
    fdParentProject.right = new FormAttachment(100, 0);
    fdParentProject.top = new FormAttachment(wlParentProject, 0, SWT.CENTER);
    wParentProject.setLayoutData(fdParentProject);

    Label wlParentFolders = new Label(comp, SWT.LEFT);
    PropsUi.setLook(wlParentFolders);
    wlParentFolders.setText(
        BaseMessages.getString(PKG, "ProjectDialog.Label.ParentProjectFolders"));
    FormData fdlParentFolders = new FormData();
    fdlParentFolders.left = new FormAttachment(0, 0);
    fdlParentFolders.right = new FormAttachment(100, 0);
    fdlParentFolders.top = new FormAttachment(wParentProject, 2 * margin);
    wlParentFolders.setLayoutData(fdlParentFolders);

    ColumnInfo[] columnInfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Label.Folder"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Label.CopyOnce"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              YES_NO,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Label.CopyOnEnable"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              YES_NO,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Label.Overwrite"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              YES_NO,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Label.ExclusionWildcard"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    columnInfo[0].setUsingVariables(true);
    columnInfo[0].setToolTip(
        BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Tooltip.Folder"));
    columnInfo[1].setToolTip(
        BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Tooltip.CopyOnce"));
    columnInfo[2].setToolTip(
        BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Tooltip.CopyOnEnable"));
    columnInfo[3].setToolTip(
        BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Tooltip.Overwrite"));
    columnInfo[4].setToolTip(
        BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Tooltip.ExclusionWildcard"));

    wParentFolders =
        new TableView(
            variables,
            comp,
            SWT.BORDER,
            columnInfo,
            Math.max(project.getParentProjectFolders().size(), 3),
            e -> needingProjectRefresh = true,
            props);
    PropsUi.setLook(wParentFolders);
    FormData fdParentFolders = new FormData();
    fdParentFolders.left = new FormAttachment(0, 0);
    fdParentFolders.right = new FormAttachment(100, 0);
    fdParentFolders.top = new FormAttachment(wlParentFolders, margin);
    fdParentFolders.bottom = new FormAttachment(100, 0);
    wParentFolders.setLayoutData(fdParentFolders);
  }

  private void createVariablesTab(CTabFolder folder, int margin) {
    Composite comp = createTab(folder, "ProjectDialog.Tab.Variables");

    Label wlVariables = new Label(comp, SWT.LEFT);
    PropsUi.setLook(wlVariables);
    wlVariables.setText(
        BaseMessages.getString(PKG, "ProjectDialog.Group.Label.ProjectVariablesToSet"));
    FormData fdlVariables = new FormData();
    fdlVariables.left = new FormAttachment(0, 0);
    fdlVariables.right = new FormAttachment(100, 0);
    fdlVariables.top = new FormAttachment(0, 0);
    wlVariables.setLayoutData(fdlVariables);

    ColumnInfo[] columnInfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Label.VariableName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Label.VariableValue"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ProjectDialog.DetailTable.Label.VariableDescription"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    columnInfo[0].setUsingVariables(true);
    columnInfo[1].setUsingVariables(true);

    wVariables =
        new TableView(
            new Variables(),
            comp,
            SWT.BORDER,
            columnInfo,
            Math.max(project.getDescribedVariables().size(), 3),
            e -> needingProjectRefresh = true,
            props);
    PropsUi.setLook(wVariables);
    FormData fdVariables = new FormData();
    fdVariables.left = new FormAttachment(0, 0);
    fdVariables.right = new FormAttachment(100, 0);
    fdVariables.top = new FormAttachment(wlVariables, margin);
    fdVariables.bottom = new FormAttachment(100, 0);
    wVariables.setLayoutData(fdVariables);
  }

  /**
   * Automatically select read-only when the home folder is a VFS archive URI (zip/jar/tar/...). The
   * user can still uncheck the option, or check it manually for other cases (http://, read-only
   * folders, ...).
   */
  private void autoSetReadOnlyFromHome() {
    if (ProjectConfig.isArchiveUri(variables.resolve(wHome.getText()))) {
      if (!wReadOnly.getSelection()) {
        wReadOnly.setSelection(true);
        updateReadOnlyWidgets();
      }
    }
  }

  /**
   * Enable or disable project settings that are stored in project-config.json. When the project is
   * read-only those cannot be persisted, so the widgets are disabled. Name, home folder, config
   * path and the read-only flag remain editable (they live in hop-config.json).
   */
  private void updateReadOnlyWidgets() {
    boolean editable = !wReadOnly.getSelection();

    wbConfigFile.setEnabled(editable);
    wParentProject.setEnabled(editable);
    wDescription.setEnabled(editable);
    wCompany.setEnabled(editable);
    wDepartment.setEnabled(editable);
    wVersion.setEnabled(editable);
    wMetadataBaseFolder.setEnabled(editable);
    wAutoExportMetadata.setEnabled(editable);
    wUnitTestsBasePath.setEnabled(editable);
    wDataSetCsvFolder.setEnabled(editable);
    wEnforceHomeExecution.setEnabled(editable);
    wVariables.setEnabled(editable);
    wVariables.setReadonly(!editable);
    updateAutoExportMetadataWidgets();
    updateParentFolderWidgets();
  }

  private void updateParentFolderWidgets() {
    if (wParentFolders == null || wParentProject == null) {
      return;
    }
    boolean editable =
        !wReadOnly.getSelection() && StringUtils.isNotEmpty(wParentProject.getText());
    wParentFolders.setEnabled(editable);
    wParentFolders.setReadonly(!editable);
  }

  /** Filename is only meaningful when auto-export is enabled (and the project is not read-only). */
  private void updateAutoExportMetadataWidgets() {
    boolean editable = !wReadOnly.getSelection() && wAutoExportMetadata.getSelection();
    wAutoExportMetadataFilename.setEnabled(editable);
  }

  private void browseHomeFolder(Event event) {
    String homeFolder = BaseDialog.presentDirectoryDialog(shell, wHome, variables);

    try {
      if (homeFolder != null && StringUtils.isEmpty(wName.getText())) {
        FileObject file = HopVfs.getFileObject(homeFolder);
        wName.setText(Const.NVL(file.getName().getBaseName(), ""));
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting base filename of home folder: " + homeFolder, e);
    }
  }

  private void browseConfigFolder(Event event) {
    String configFileStr = null;
    String rootPath = wHome.getText();

    File configFile =
        new File(
            wHome.getText()
                + File.separator
                + "config"
                + File.separator
                + ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
    wConfigFile.setText(rootPath);

    if (configFile.exists()) {
      configFileStr =
          BaseDialog.presentFileDialog(
              shell,
              wConfigFile,
              variables,
              new String[] {"*.json", "*.*"},
              new String[] {
                BaseMessages.getString(PKG, "ProjectDialog.FileList.PrjFiles.Text"),
                BaseMessages.getString(PKG, "ProjectDialog.FileList.AllFiles.Text")
              },
              true);
    } else {
      String configDir = BaseDialog.presentDirectoryDialog(shell, wConfigFile, variables);
      configFileStr =
          (configDir != null ? configDir : "")
              + File.separator
              + ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME;
    }

    if (configFileStr != null) {
      String relativeConfigFile = null;
      if (!configFileStr.startsWith(rootPath)) {
        MessageBox box = new MessageBox(shell, SWT.ICON_QUESTION | SWT.OK);
        box.setText(BaseMessages.getString(PKG, "ProjectGuiPlugin.WrongConfigPath.Dialog.Header"));
        box.setMessage(
            BaseMessages.getString(PKG, "ProjectGuiPlugin.WrongConfigPath.Dialog.Message"));
        box.open();
      } else {
        String tmpConfigFile = StringUtils.difference(rootPath + File.separator, configFileStr);
        relativeConfigFile =
            (tmpConfigFile.startsWith("/") ? tmpConfigFile.substring(1) : tmpConfigFile);
        relativeConfigFile.replace("\\", "/");
      }
      wConfigFile.setText(Const.NVL(relativeConfigFile, ""));
    }
  }

  private void updateIVariables() {
    Project env = new Project();
    ProjectConfig pc = new ProjectConfig();
    try {
      getInfo(env, pc);
      env.modifyVariables(variables, pc, Collections.emptyList(), null);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ProjectDialog.ProjectConfigError.Error.Dialog.Header"),
          BaseMessages.getString(PKG, "ProjectDialog.ProjectConfigError.Error.Dialog.Message"),
          e);
    }
  }

  /** Sanitize the path by removing leading/trailing whitespace and any trailing file separator. */
  protected String sanitizePath(String path) {
    if (path == null) {
      return null;
    }
    path = path.trim();
    while (path.endsWith("/") || path.endsWith("\\")) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  private void ok() {
    try {
      String oriProjectName = projectConfig.getProjectName();
      String oriProjectHome = projectConfig.getProjectHome();

      String homeFolder = sanitizePath(wHome.getText());
      boolean projectHomeFolderChanged = this.editMode && !oriProjectHome.equals(homeFolder);
      boolean readOnly = wReadOnly.getSelection();

      if (StringUtils.isEmpty(variables.resolve(homeFolder))) {
        throw new HopException("Please specify a home folder for your project");
      }

      if (projectHomeFolderChanged) {
        MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText(BaseMessages.getString(PKG, "ProjectDialog.ChangeHome.Dialog.Header"));
        box.setMessage(
            BaseMessages.getString(
                PKG, "ProjectDialog.ChangeHome.Dialog.Message", oriProjectHome, homeFolder));
        int anwser = box.open();
        if ((anwser & SWT.NO) != 0) {
          wHome.setText(oriProjectHome);
          projectHomeFolderChanged = false;
        }
      }

      FileObject homeFolderObject = HopVfs.getFileObject(variables.resolve(homeFolder));
      if (!homeFolderObject.exists()) {
        if (readOnly) {
          throw new HopException(
              BaseMessages.getString(PKG, "ProjectDialog.ReadOnly.HomeMissing.Error", homeFolder));
        }
        if (!this.editMode || projectHomeFolderChanged) {
          MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
          box.setText(BaseMessages.getString(PKG, "ProjectDialog.CreateHome.Dialog.Header"));
          box.setMessage(
              BaseMessages.getString(PKG, "ProjectDialog.CreateHome.Dialog.Message", homeFolder));
          int anwser = box.open();
          if ((anwser & SWT.YES) != 0) {
            HopVfs.getFileObject(homeFolder).createFolder();
          }
        }
      }

      String projectName = wName.getText();
      if (StringUtils.isEmpty(projectName)) {
        throw new HopException("Please give your new project a name");
      }

      if (Utils.isEmpty(wHome.getText())) {
        throw new HopException("Please specify project's home directory path!");
      }

      if (Utils.isEmpty(wConfigFile.getText())) {
        throw new HopException("Please specify project's configuration file relative path!");
      }

      if (readOnly) {
        ProjectConfig verifyConfig =
            new ProjectConfig(projectName, homeFolder, wConfigFile.getText());
        String configPath = verifyConfig.getActualProjectConfigFilename(variables);
        FileObject configFile = HopVfs.getFileObject(configPath);
        if (!configFile.exists()) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "ProjectDialog.ReadOnly.ConfigMissing.Error", configPath));
        }
      }

      if (wParentProject.getText() != null
          && !wParentProject.getText().isEmpty()
          && projectName.equals(wParentProject.getText())) {
        throw new HopException(
            CONST_PROJECT + projectName + "' cannot be set as a parent project of itself");
      }

      ProjectsConfig prjsCfg = ProjectsConfigSingleton.getConfig();
      List<String> prjs = prjsCfg.listProjectConfigNames();

      if (StringUtils.isEmpty(oriProjectName)
          || (StringUtils.isNotEmpty(oriProjectName) && !projectName.equals(oriProjectName))) {
        for (String prj : prjs) {
          if (projectName.equals(prj)) {
            throw new HopException(
                CONST_PROJECT + projectName + "' already exists. Project name must be unique!");
          }
        }
      }

      HopGui hopGui = HopGui.getInstance();
      if (!Utils.isEmpty(wParentProject.getText())) {

        boolean parentPrjExists = ProjectsUtil.projectExists(wParentProject.getText());
        if (!parentPrjExists)
          throw new HopException(
              CONST_PROJECT
                  + wParentProject.getText()
                  + "' cannot be set as parent project because it does not exists!");

        ProjectConfig parentPrjCfg = prjsCfg.findProjectConfig(wParentProject.getText());
        Project parentPrj = parentPrjCfg.loadProject(hopGui.getVariables());
        if (parentPrj.getParentProjectName() != null
            && parentPrj.getParentProjectName().equals(projectName))
          throw new HopException(
              CONST_PROJECT
                  + projectName
                  + "' cannot reference '"
                  + wParentProject.getText()
                  + "' as parent project because we are going to create a circular reference!");
      }

      if (this.editMode && !oriProjectName.equals(projectName)) {
        MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText(BaseMessages.getString(PKG, "ProjectDialog.ChangeProjectName.Dialog.Header"));
        box.setMessage(
            BaseMessages.getString(
                PKG,
                "ProjectDialog.ChangeProjectName.Dialog.Message",
                oriProjectName,
                projectName));
        int anwser = box.open();
        if ((anwser & SWT.NO) != 0) {
          wName.setText(oriProjectName);
        }
      }

      if (!oriProjectName.equals(projectName)) {
        List<String> refs = ProjectsUtil.getParentProjectReferences(oriProjectName);

        if (!refs.isEmpty()) {
          ProjectsUtil.changeParentProjectReferences(oriProjectName, projectName);
        }
      }

      getInfo(project, projectConfig);
      returnValue = projectConfig.getProjectName();
      dispose();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ProjectDialog.ProjectConfigError.Error.Dialog.Header"),
          BaseMessages.getString(PKG, "ProjectDialog.ProjectConfigError.Error.Dialog.Message"),
          e);
    }
  }

  private void cancel() {
    needingProjectRefresh = false;
    returnValue = null;

    dispose();
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  private void getData() {
    wName.setText(Const.NVL(projectConfig.getProjectName(), ""));
    wHome.setText(Const.NVL(projectConfig.getProjectHome(), ""));
    wConfigFile.setText(Const.NVL(projectConfig.getConfigFilename(), ""));
    wReadOnly.setSelection(
        projectConfig.isReadOnly()
            || ProjectConfig.isArchiveUri(variables.resolve(projectConfig.getProjectHome())));

    wDescription.setText(Const.NVL(project.getDescription(), ""));
    wCompany.setText(Const.NVL(project.getCompany(), ""));
    wDepartment.setText(Const.NVL(project.getDepartment(), ""));
    wVersion.setText(Const.NVL(project.getVersion(), ""));
    wMetadataBaseFolder.setText(Const.NVL(project.getMetadataBaseFolder(), ""));
    wAutoExportMetadata.setSelection(project.isAutoExportMetadata());
    String exportFilename = project.getAutoExportMetadataFilename();
    if (Utils.isEmpty(exportFilename)) {
      exportFilename = Defaults.DEFAULT_AUTO_EXPORT_METADATA_FILENAME;
    }
    wAutoExportMetadataFilename.setText(exportFilename);
    wUnitTestsBasePath.setText(Const.NVL(project.getUnitTestsBasePath(), ""));
    wDataSetCsvFolder.setText(Const.NVL(project.getDataSetsCsvFolder(), ""));
    wEnforceHomeExecution.setSelection(project.isEnforcingExecutionInHome());
    for (int i = 0; i < project.getDescribedVariables().size(); i++) {
      DescribedVariable describedVariable = project.getDescribedVariables().get(i);
      TableItem item = wVariables.table.getItem(i);
      item.setText(1, Const.NVL(describedVariable.getName(), ""));
      item.setText(2, Const.NVL(describedVariable.getValue(), ""));
      item.setText(3, Const.NVL(describedVariable.getDescription(), ""));
    }
    wVariables.setRowNums();
    wVariables.optWidth(true);

    try {
      wParentProject.setText(Const.NVL(project.getParentProjectName(), ""));

      List<String> names = ProjectsConfigSingleton.getConfig().listProjectConfigNames();
      if (projectConfig.getProjectName() != null) {
        names.remove(projectConfig.getProjectName());
      }
      wParentProject.setItems(names.toArray(new String[0]));
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ProjectDialog.ProjectList.Error.Dialog.Header"),
          BaseMessages.getString(PKG, "ProjectDialog.ProjectList.Error.Dialog.Message"),
          e);
    }

    List<ParentProjectFolder> parentFolders = project.getParentProjectFolders();
    for (int i = 0; i < parentFolders.size(); i++) {
      ParentProjectFolder parentFolder = parentFolders.get(i);
      TableItem item = wParentFolders.table.getItem(i);
      item.setText(1, Const.NVL(parentFolder.getFolder(), ""));
      item.setText(2, yesNo(parentFolder.isCopyOnce()));
      item.setText(3, yesNo(parentFolder.isCopyOnEnable()));
      item.setText(4, yesNo(parentFolder.isOverwrite()));
      item.setText(5, Const.NVL(parentFolder.getExclusionWildcard(), ""));
    }
    wParentFolders.setRowNums();
    wParentFolders.optWidth(true);
  }

  private void getInfo(Project project, ProjectConfig projectConfig) throws HopException {

    projectConfig.setProjectName(wName.getText());
    projectConfig.setProjectHome(sanitizePath(wHome.getText()));
    projectConfig.setConfigFilename(wConfigFile.getText());
    projectConfig.setReadOnly(wReadOnly.getSelection());

    project.setParentProjectName(wParentProject.getText());
    project.setDescription(wDescription.getText());
    project.setCompany(wCompany.getText());
    project.setDepartment(wDepartment.getText());
    project.setVersion(wVersion.getText());
    project.setMetadataBaseFolder(wMetadataBaseFolder.getText());
    project.setAutoExportMetadata(wAutoExportMetadata.getSelection());
    project.setAutoExportMetadataFilename(wAutoExportMetadataFilename.getText());
    project.setUnitTestsBasePath(wUnitTestsBasePath.getText());
    project.setDataSetsCsvFolder(wDataSetCsvFolder.getText());
    project.setEnforcingExecutionInHome(wEnforceHomeExecution.getSelection());
    project.getDescribedVariables().clear();
    for (int i = 0; i < wVariables.nrNonEmpty(); i++) {
      TableItem item = wVariables.getNonEmpty(i);
      DescribedVariable variable =
          new DescribedVariable(
              item.getText(1), // name
              item.getText(2), // value
              item.getText(3) // description
              );
      project.getDescribedVariables().add(variable);
    }

    project.getParentProjectFolders().clear();
    for (int i = 0; i < wParentFolders.nrNonEmpty(); i++) {
      TableItem item = wParentFolders.getNonEmpty(i);
      if (StringUtils.isEmpty(item.getText(1))) {
        continue;
      }
      ParentProjectFolder parentFolder = new ParentProjectFolder();
      parentFolder.setFolder(item.getText(1));
      parentFolder.setCopyOnce(isYes(item.getText(2)));
      parentFolder.setCopyOnEnable(isYes(item.getText(3)));
      parentFolder.setOverwrite(isYes(item.getText(4)));
      parentFolder.setExclusionWildcard(item.getText(5));
      project.getParentProjectFolders().add(parentFolder);
    }

    if (StringUtils.isNotEmpty(projectConfig.getProjectHome())
        && StringUtils.isNotEmpty(projectConfig.configFilename)) {
      try {
        project.setConfigFilename(projectConfig.getActualProjectConfigFilename(variables));
      } catch (Exception e) {
        if (ProjectsGuiPlugin.extractMissingProjectPath(e) == null) {
          throw new HopException(e);
        }
      }
    }

    try {
      project.verifyProjectsChain(projectConfig.getProjectName(), variables);
    } catch (Exception e) {
      if (ProjectsGuiPlugin.extractMissingProjectPath(e) == null) {
        throw new HopException(e);
      }
    }
  }

  private static boolean isYes(String value) {
    return "Y".equalsIgnoreCase(Const.NVL(value, ""));
  }

  private static String yesNo(boolean value) {
    return value ? "Y" : "N";
  }
}
