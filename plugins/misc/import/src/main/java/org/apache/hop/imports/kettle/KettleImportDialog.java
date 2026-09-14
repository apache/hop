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

package org.apache.hop.imports.kettle;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.SingletonUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.imp.HopImportBase;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;

public class KettleImportDialog extends Dialog {

  private static final Class<?> PKG = KettleImportDialog.class;

  public static final String LAST_USED_IMPORT_SOURCE_FOLDER = "ImportFolder";
  public static final String LAST_USED_IMPORT_INTO_PROJECT = "ImportInProject";
  public static final String LAST_USED_IMPORT_TARGET_PROJECT = "ImportProject";
  public static final String LAST_USED_IMPORT_TARGET_FOLDER = "ImportTarget";
  public static final String LAST_USED_IMPORT_PROPS_FILE = "ImportPropertiesFile";
  public static final String LAST_USED_IMPORT_SHARED_FILE = "ImportSharedFile";
  public static final String LAST_USED_IMPORT_JDBC_FILE = "ImportJdbcFile";
  public static final String LAST_USED_IMPORT_CONFIG_FILE = "ImportConfigFile";
  public static final String LAST_USED_IMPORT_SKIP_EXISTING = "ImportSkipExisting";
  public static final String LAST_USED_IMPORT_SKIP_HIDDEN = "ImportSkipHidden";
  public static final String LAST_USED_IMPORT_SKIP_FOLDERS = "ImportSkipFolders";
  public static final String LAST_USED_IMPORT_PIPELINE_RUN_CONFIGURATION =
      "ImportPipelineRunConfiguration";
  public static final String LAST_USED_IMPORT_WORKFLOW_RUN_CONFIGURATION =
      "ImportWorkflowRunConfiguration";
  public static final String LAST_USED_IMPORT_NAMING_SCHEME = "ImportNamingScheme";
  public static final String NAMING_SCHEME_METADATA_KEY = "naming-scheme";
  public static final String CONST_FALSE = "false";
  public static final String CONST_ALL_FILES = "All Files (*.*)";
  public static final String CONST_KETTLE_IMPORT_DIALOG_BUTTON_BROWSE =
      "KettleImportDialog.Button.Browse";

  /**
   * Directory prefix used by Hop Web File Browser uploads. Keep in sync with {@code
   * HopWebUserFilePlugin} session temp directories.
   */
  static final String WEB_USER_FILE_TEMP_DIRECTORY_PREFIX = "hop-web-user-files-";

  private final IVariables variables;

  private Shell shell;
  private final PropsUi props;

  private final KettleImport kettleImport;
  private final String configuredSourceFolder;
  private final List<String> projectNames;

  private TextVar wImportFrom;
  private TextVar wImportPath;
  private TextVar wKettleProps;
  private TextVar wShared;
  private TextVar wJdbcProps;
  private TextVar wTargetConfigFile;

  private MetaSelectionLine<PipelineRunConfiguration> wPipelineRunConfiguration;
  private MetaSelectionLine<WorkflowRunConfiguration> wWorkflowRunConfiguration;

  @SuppressWarnings("rawtypes")
  private MetaSelectionLine wNamingScheme;

  private MemoryMetadataProvider scratchMetadata;
  private IHopMetadataProvider dialogMetadataProvider;
  private String boundMetadataFolder;
  private Combo wImportProject;
  private Button wImportInExisting;
  private Button wbImportPath;
  private Button wSkipExisting;
  private Button wSkipHidden;
  private Button wSkipFolders;

  private int margin;
  private int middle;

  public KettleImportDialog(Shell parent, IVariables variables, KettleImport kettleImport)
      throws HopException {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);

    props = PropsUi.getInstance();

    this.variables = variables;
    this.kettleImport = kettleImport;
    this.configuredSourceFolder = kettleImport.getInputFolderName();
    this.scratchMetadata = new MemoryMetadataProvider(Encr.getEncoder(), variables);
    this.dialogMetadataProvider =
        new MultiMetadataProvider(Encr.getEncoder(), List.of(scratchMetadata), variables);

    try {
      projectNames =
          SingletonUtil.getValuesList(
              "org.apache.hop.projects.gui.ProjectsGuiPlugin",
              "org.apache.hop.projects.config.ProjectsConfigSingleton",
              "listProjectNames");
    } catch (HopException e) {
      throw new HopException("Error getting project names list", e);
    }
  }

  public void open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    PropsUi.setLook(shell);

    margin = PropsUi.getMargin() + 2;
    middle = props.getMiddlePct();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "KettleImportDialog.Shell.Name"));

    Button wImport = new Button(shell, SWT.PUSH);
    wImport.setText("Import");
    wImport.addListener(SWT.Selection, event -> doImport());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> dispose());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wImport, wCancel}, margin, null);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wImport, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    addSourceTab(wTabFolder);
    addTargetTab(wTabFolder);
    addMetadataTab(wTabFolder);
    wTabFolder.setSelection(0);

    // See if we need to remember previous settings...
    //
    wImportFrom.setText(
        initialSourceFolder(
            kettleImport.getInputFolderName(),
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_SOURCE_FOLDER)));
    wImportInExisting.setSelection(
        !CONST_FALSE.equalsIgnoreCase(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_INTO_PROJECT)));
    wImportProject.setText(
        Const.NVL(AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_TARGET_PROJECT), ""));
    wImportPath.setText(
        Const.NVL(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_TARGET_FOLDER),
            Const.NVL(kettleImport.getOutputFolderName(), "")));
    wKettleProps.setText(
        Const.NVL(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_PROPS_FILE),
            Const.NVL(kettleImport.getKettlePropertiesFilename(), "")));
    wShared.setText(
        Const.NVL(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_SHARED_FILE),
            Const.NVL(kettleImport.getSharedXmlFilename(), "")));
    wJdbcProps.setText(
        Const.NVL(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_JDBC_FILE),
            Const.NVL(kettleImport.getJdbcPropertiesFilename(), "")));
    wTargetConfigFile.setText(
        Const.NVL(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_CONFIG_FILE),
            Const.NVL(kettleImport.getTargetConfigFilename(), "")));
    wSkipExisting.setSelection(
        !CONST_FALSE.equalsIgnoreCase(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_SKIP_EXISTING)));
    wSkipHidden.setSelection(
        !CONST_FALSE.equalsIgnoreCase(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_SKIP_HIDDEN)));
    wSkipFolders.setSelection(
        !CONST_FALSE.equalsIgnoreCase(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_SKIP_FOLDERS)));

    showHideProjectFields(null);
    bindTargetMetadataProvider();
    wPipelineRunConfiguration.setText(
        Const.NVL(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_PIPELINE_RUN_CONFIGURATION), ""));
    wWorkflowRunConfiguration.setText(
        Const.NVL(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_WORKFLOW_RUN_CONFIGURATION), ""));
    if (wNamingScheme != null) {
      wNamingScheme.setText(
          Const.NVL(AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_NAMING_SCHEME), ""));
    }

    wImportFrom.setFocus();

    BaseDialog.defaultShellHandling(shell, c -> dispose(), c -> dispose());
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    String sourceFolder = wImportFrom.getText();
    if (shouldRememberSourceFolder(configuredSourceFolder, sourceFolder)) {
      AuditManagerGuiUtil.addLastUsedValue(LAST_USED_IMPORT_SOURCE_FOLDER, sourceFolder);
    }
    AuditManagerGuiUtil.addLastUsedValue(
        LAST_USED_IMPORT_INTO_PROJECT, wImportInExisting.getSelection() ? "true" : CONST_FALSE);
    AuditManagerGuiUtil.addLastUsedValue(LAST_USED_IMPORT_TARGET_PROJECT, wImportProject.getText());
    AuditManagerGuiUtil.addLastUsedValue(LAST_USED_IMPORT_TARGET_FOLDER, wImportPath.getText());
    AuditManagerGuiUtil.addLastUsedValue(LAST_USED_IMPORT_PROPS_FILE, wKettleProps.getText());
    AuditManagerGuiUtil.addLastUsedValue(LAST_USED_IMPORT_SHARED_FILE, wShared.getText());
    AuditManagerGuiUtil.addLastUsedValue(LAST_USED_IMPORT_JDBC_FILE, wJdbcProps.getText());
    AuditManagerGuiUtil.addLastUsedValue(LAST_USED_IMPORT_CONFIG_FILE, wTargetConfigFile.getText());
    AuditManagerGuiUtil.addLastUsedValue(
        LAST_USED_IMPORT_PIPELINE_RUN_CONFIGURATION, wPipelineRunConfiguration.getText());
    AuditManagerGuiUtil.addLastUsedValue(
        LAST_USED_IMPORT_WORKFLOW_RUN_CONFIGURATION, wWorkflowRunConfiguration.getText());
    if (wNamingScheme != null) {
      AuditManagerGuiUtil.addLastUsedValue(LAST_USED_IMPORT_NAMING_SCHEME, wNamingScheme.getText());
    }
    AuditManagerGuiUtil.addLastUsedValue(
        LAST_USED_IMPORT_SKIP_EXISTING, wSkipExisting.getSelection() ? "true" : CONST_FALSE);
    AuditManagerGuiUtil.addLastUsedValue(
        LAST_USED_IMPORT_SKIP_HIDDEN, wSkipHidden.getSelection() ? "true" : CONST_FALSE);
    AuditManagerGuiUtil.addLastUsedValue(
        LAST_USED_IMPORT_SKIP_FOLDERS, wSkipFolders.getSelection() ? "true" : CONST_FALSE);
    shell.dispose();
  }

  static String initialSourceFolder(String configuredSourceFolder, String lastUsedSourceFolder) {
    if (StringUtils.isNotBlank(configuredSourceFolder)) {
      return configuredSourceFolder;
    }
    if (StringUtils.isBlank(lastUsedSourceFolder)
        || isEphemeralWebUploadFolder(lastUsedSourceFolder)) {
      return "";
    }
    return lastUsedSourceFolder;
  }

  /**
   * File Browser ZIP uploads extract into a session temp folder that is deleted when the dialog
   * closes. Do not persist that path, and ignore it if it was already stored as last-used.
   */
  static boolean shouldRememberSourceFolder(
      String configuredSourceFolder, String currentSourceFolder) {
    if (isEphemeralWebUploadFolder(currentSourceFolder)) {
      return false;
    }
    if (StringUtils.isBlank(configuredSourceFolder)) {
      return true;
    }
    return StringUtils.isNotBlank(currentSourceFolder)
        && !sameSourceFolder(configuredSourceFolder, currentSourceFolder);
  }

  static boolean isEphemeralWebUploadFolder(String folder) {
    if (StringUtils.isBlank(folder)) {
      return false;
    }
    String normalized = folder.replace('\\', '/');
    return normalized.contains("/" + WEB_USER_FILE_TEMP_DIRECTORY_PREFIX)
        || normalized.startsWith(WEB_USER_FILE_TEMP_DIRECTORY_PREFIX);
  }

  private static boolean sameSourceFolder(String left, String right) {
    return StringUtils.equals(
        StringUtils.removeEnd(left.replace('\\', '/'), "/"),
        StringUtils.removeEnd(right.replace('\\', '/'), "/"));
  }

  private void browseHomeFolder(Event event) {
    BaseDialog.presentDirectoryDialog(shell, wImportFrom, variables);
  }

  private void browseTargetFolder(Event event) {
    BaseDialog.presentDirectoryDialog(shell, wImportPath, variables);
  }

  private void browseKettlePropsFile(Event event) {
    BaseDialog.presentFileDialog(
        shell,
        wKettleProps,
        variables,
        new String[] {"*.properties", "*.*"},
        new String[] {"Properties files (*.properties)", CONST_ALL_FILES},
        true);
  }

  private void browseJdbcPropsFile(Event event) {
    BaseDialog.presentFileDialog(
        shell,
        wJdbcProps,
        variables,
        new String[] {"*.properties", "*.*"},
        new String[] {"Properties files (*.properties)", CONST_ALL_FILES},
        true);
  }

  private void browseXmlFile(Event event) {
    BaseDialog.presentFileDialog(
        shell,
        wShared,
        variables,
        new String[] {"*.xml", "*.*"},
        new String[] {"XML files (*.xml)", CONST_ALL_FILES},
        true);
  }

  private void addSourceTab(CTabFolder folder) {
    Composite parent = addTab(folder, "KettleImportDialog.Tab.Source");
    wImportFrom =
        addTextRow(
            parent, null, "KettleImportDialog.Label.ImportFrom", true, this::browseHomeFolder);
    wKettleProps =
        addTextRow(
            parent,
            wImportFrom,
            "KettleImportDialog.Label.PathToKettleProperties",
            true,
            this::browseKettlePropsFile);
    wShared =
        addTextRow(
            parent,
            wKettleProps,
            "KettleImportDialog.Label.PathToSharedXml",
            true,
            this::browseXmlFile);
    wJdbcProps =
        addTextRow(
            parent,
            wShared,
            "KettleImportDialog.Label.PathToJDBCProperties",
            true,
            this::browseJdbcPropsFile);
    wSkipHidden =
        addCheckboxRow(parent, wJdbcProps, "KettleImportDialog.Label.SkipHiddenFiles", true);
    wSkipFolders =
        addCheckboxRow(parent, wSkipHidden, "KettleImportDialog.Label.SkipFolders", true);
  }

  private void addTargetTab(CTabFolder folder) {
    Composite parent = addTab(folder, "KettleImportDialog.Tab.Target");
    wImportInExisting =
        addCheckboxRow(parent, null, "KettleImportDialog.Label.ImportInExistingProject", true);
    wImportInExisting.addListener(SWT.Selection, this::showHideProjectFields);

    Label wlImportProject =
        addLabel(parent, wImportInExisting, "KettleImportDialog.Label.ImportInProject");
    wImportProject = new Combo(parent, SWT.READ_ONLY);
    wImportProject.setItems(projectNames.toArray(new String[0]));
    PropsUi.setLook(wImportProject);
    FormData fdImportProject = new FormData();
    fdImportProject.left = new FormAttachment(middle, margin);
    fdImportProject.right = new FormAttachment(100, 0);
    fdImportProject.top = new FormAttachment(wlImportProject, 0, SWT.CENTER);
    wImportProject.setLayoutData(fdImportProject);
    wImportProject.addListener(SWT.Selection, event -> bindTargetMetadataProvider());

    Label wlImportPath =
        addLabel(parent, wImportProject, "KettleImportDialog.Label.ImportToFolder");
    wbImportPath = addBrowseButton(parent, wlImportPath, this::browseTargetFolder);
    wbImportPath.setEnabled(false);
    wImportPath = addTextVar(parent, wlImportPath, wbImportPath);
    wImportPath.setEditable(false);
    wImportPath.addModifyListener(event -> bindTargetMetadataProvider());

    wSkipExisting =
        addCheckboxRow(
            parent, wImportPath, "KettleImportDialog.Label.SkipExistingTargetFiles", true);
    wTargetConfigFile =
        addTextRow(parent, wSkipExisting, "KettleImportDialog.Label.TargetConfigFile", false, null);
    wTargetConfigFile.setEditable(false);
  }

  private void addMetadataTab(CTabFolder folder) {
    Composite parent = addTab(folder, "KettleImportDialog.Tab.Metadata");
    wPipelineRunConfiguration =
        new MetaSelectionLine<>(
            variables,
            dialogMetadataProvider,
            PipelineRunConfiguration.class,
            parent,
            SWT.NONE,
            BaseMessages.getString(PKG, "KettleImportDialog.Pipeline.RunConfiguration.Label"),
            BaseMessages.getString(PKG, "KettleImportDialog.RunConfiguration.Tooltip"));
    wPipelineRunConfiguration.addToConnectionLine(parent, null, null, null);

    wWorkflowRunConfiguration =
        new MetaSelectionLine<>(
            variables,
            dialogMetadataProvider,
            WorkflowRunConfiguration.class,
            parent,
            SWT.NONE,
            BaseMessages.getString(PKG, "KettleImportDialog.Workflow.RunConfiguration.Label"),
            BaseMessages.getString(PKG, "KettleImportDialog.RunConfiguration.Tooltip"));
    wWorkflowRunConfiguration.addToConnectionLine(parent, wPipelineRunConfiguration, null, null);

    wNamingScheme =
        MetaSelectionLine.forMetadataKey(
            variables,
            dialogMetadataProvider,
            parent,
            SWT.NONE,
            NAMING_SCHEME_METADATA_KEY,
            BaseMessages.getString(PKG, "KettleImportDialog.NamingScheme.Label"),
            BaseMessages.getString(PKG, "KettleImportDialog.NamingScheme.Tooltip"));
    if (wNamingScheme != null) {
      wNamingScheme.addToConnectionLine(parent, wWorkflowRunConfiguration, null, null);
    }
  }

  private Composite addTab(CTabFolder folder, String i18nKey) {
    CTabItem item = new CTabItem(folder, SWT.NONE);
    item.setFont(GuiResource.getInstance().getFontDefault());
    item.setText(BaseMessages.getString(PKG, i18nKey));
    Composite composite = new Composite(folder, SWT.NONE);
    PropsUi.setLook(composite);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    composite.setLayout(layout);
    item.setControl(composite);
    return composite;
  }

  private Label addLabel(Composite parent, Control previous, String labelKey) {
    Label label = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, 0);
    if (previous != null) {
      fd.top = new FormAttachment(previous, margin);
    } else {
      fd.top = new FormAttachment(0, margin);
    }
    label.setLayoutData(fd);
    return label;
  }

  private Button addBrowseButton(Composite parent, Control alignTo, Listener browseListener) {
    Button browse = new Button(parent, SWT.PUSH);
    PropsUi.setLook(browse);
    browse.setText(BaseMessages.getString(PKG, CONST_KETTLE_IMPORT_DIALOG_BUTTON_BROWSE));
    FormData fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(alignTo, 0, SWT.CENTER);
    browse.setLayoutData(fd);
    browse.addListener(SWT.Selection, browseListener);
    return browse;
  }

  private TextVar addTextVar(Composite parent, Control alignTo, Control rightOf) {
    TextVar text = new TextVar(variables, parent, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(text);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, margin);
    fd.right = rightOf != null ? new FormAttachment(rightOf, -margin) : new FormAttachment(100, 0);
    fd.top = new FormAttachment(alignTo, 0, SWT.CENTER);
    text.setLayoutData(fd);
    return text;
  }

  private TextVar addTextRow(
      Composite parent,
      Control previous,
      String labelKey,
      boolean withBrowse,
      Listener browseListener) {
    Label label = addLabel(parent, previous, labelKey);
    Button browse = withBrowse ? addBrowseButton(parent, label, browseListener) : null;
    return addTextVar(parent, label, browse);
  }

  private Button addCheckboxRow(
      Composite parent, Control previous, String labelKey, boolean selected) {
    Label label = addLabel(parent, previous, labelKey);
    Button checkbox = new Button(parent, SWT.CHECK);
    PropsUi.setLook(checkbox);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, margin);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(label, 0, SWT.CENTER);
    checkbox.setLayoutData(fd);
    checkbox.setSelection(selected);
    return checkbox;
  }

  private void doImport() {

    try {
      String projectName = "";

      // we're importing to a new project, create by path
      if (!wImportInExisting.getSelection()) {
        projectName = "Hop Import Project";
        try {
          ExtensionPointHandler.callExtensionPoint(
              HopGui.getInstance().getLog(),
              variables,
              "HopImportCreateProject",
              wImportPath.getText());
        } catch (HopException e) {
          throw new HopException("Error creating project", e);
        }
      } else {
        projectName = wImportProject.getText();
      }

      // import jobs and transformations

      String sourceFolder = variables.resolve(wImportFrom.getText());
      String targetFolder = variables.resolve(wImportPath.getText());

      // See if we can pick up the target folder from a project reference...
      //
      if (wImportInExisting.getSelection()) {
        Object[] objects = new Object[2];
        objects[0] = projectName;
        objects[1] = targetFolder;
        try {
          ExtensionPointHandler.callExtensionPoint(
              HopGui.getInstance().getLog(), variables, "ProjectHome", objects);

          // Grab it back (or leave unchanged)
          targetFolder = (String) objects[1];
        } catch (HopException e) {
          throw new HopException("Error getting home folder of project " + projectName, e);
        }
      }

      kettleImport.setValidateInputFolder(sourceFolder);
      kettleImport.setValidateOutputFolder(targetFolder);
      persistDialogMetadataToTarget(kettleImport);
      kettleImport.setSharedXmlFilename(variables.resolve(wShared.getText()));
      kettleImport.setKettlePropertiesFilename(variables.resolve(wKettleProps.getText()));
      kettleImport.setJdbcPropertiesFilename(variables.resolve(wJdbcProps.getText()));
      kettleImport.setSkippingExistingTargetFiles(wSkipExisting.getSelection());
      kettleImport.setSkippingHiddenFilesAndFolders(wSkipHidden.getSelection());
      kettleImport.setSkippingFolders(wSkipFolders.getSelection());

      String defaultPRC = Const.NVL(wPipelineRunConfiguration.getText(), "");
      kettleImport.setDefaultPipelineRunConfiguration(defaultPRC);
      String defaultWRC = Const.NVL(wWorkflowRunConfiguration.getText(), "");
      kettleImport.setDefaultWorkflowRunConfiguration(defaultWRC);
      kettleImport.setApplyNamingSchemes(true);
      if (wNamingScheme != null) {
        kettleImport.setNamingSchemeName(Const.NVL(wNamingScheme.getText(), ""));
      }

      boolean goForImport = true;
      if ((Utils.isEmpty(defaultPRC) && Utils.isEmpty(defaultWRC))
          || Utils.isEmpty(defaultPRC)
          || Utils.isEmpty(defaultWRC)) {
        MessageBox box = new MessageBox(shell, SWT.ICON_WARNING | SWT.OK | SWT.CANCEL);
        box.setText(BaseMessages.getString(PKG, "KettleImportDialog.NoDefaultRC.Title"));
        box.setMessage(
            BaseMessages.getString(
                PKG,
                "KettleImportDialog.NoDefaultRC"
                    + ((Utils.isEmpty(defaultPRC) && Utils.isEmpty(defaultWRC))
                        ? "All"
                        : (Utils.isEmpty(defaultPRC)
                            ? "Prc"
                            : (Utils.isEmpty(defaultWRC) ? "Wrc" : "")))
                    + ".Message"));
        int answer = box.open();

        if (answer == SWT.CANCEL) goForImport = false;
      }

      if (goForImport) {
        // We're going to run the import in a progress dialog with a monitor...
        //
        ProgressMonitorDialog monitorDialog =
            new ProgressMonitorDialog(HopGui.getInstance().getShell());
        monitorDialog.run(
            true,
            monitor -> {
              try {
                monitor.beginTask("Importing files", 5);
                kettleImport.runImport(monitor);
                monitor.done();
              } catch (Throwable e) {
                throw new InvocationTargetException(
                    e, "Error importing " + Const.getStackTracker(e));
              }
            });

        // Show some statistics after the import...
        //
        MessageBox box = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
        box.setText(BaseMessages.getString(PKG, "KettleImportDialog.ImportSummary.Title"));
        box.setMessage(kettleImport.getImportReport());
        box.open();
      }
    } catch (Exception e) {
      String title = BaseMessages.getString(PKG, "KettleImportDialog.Error.Title");
      boolean web = EnvironmentUtils.getInstance().isWeb();
      String message =
          BaseMessages.getString(
              PKG,
              web ? "KettleImportDialog.Error.Web.Message" : "KettleImportDialog.Error.Message");
      LogChannel.UI.logError(message, e);
      new ErrorDialog(shell, title, message, web ? new HopException(message) : e);
    }
  }

  private void showHideProjectFields(Event event) {
    if (wImportInExisting.getSelection()) {
      wImportProject.setEnabled(true);
      wImportPath.setEditable(false);
      wbImportPath.setEnabled(false);
    } else {
      wImportProject.setEnabled(false);
      wImportPath.setEditable(true);
      wbImportPath.setEnabled(true);
    }
    bindTargetMetadataProvider();
  }

  /**
   * Rebuild the metadata lines so they can list objects from the target folder and the current
   * project. New and Edit write only to {@link #scratchMetadata}; nothing is saved to disk until
   * Import.
   */
  void bindTargetMetadataProvider() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    String metadataFolder = metadataFolderFor(peekTargetFolder());
    if (Objects.equals(metadataFolder, boundMetadataFolder) && dialogMetadataProvider != null) {
      return;
    }
    List<IHopMetadataProvider> providers = new ArrayList<>();
    if (metadataFolder != null) {
      providers.add(new JsonMetadataProvider(Encr.getEncoder(), metadataFolder, variables));
    }
    HopGui hopGui = HopGui.getInstance();
    if (hopGui != null && hopGui.getMetadataProvider() != null) {
      providers.add(hopGui.getMetadataProvider());
    }
    providers.add(scratchMetadata);
    applyMetadataProvider(
        new MultiMetadataProvider(Encr.getEncoder(), providers, variables), metadataFolder);
  }

  private String peekTargetFolder() {
    if (wImportInExisting != null && wImportInExisting.getSelection()) {
      String projectName = wImportProject != null ? wImportProject.getText() : "";
      if (Utils.isEmpty(projectName)) {
        return null;
      }
      Object[] objects = new Object[] {projectName, ""};
      try {
        ExtensionPointHandler.callExtensionPoint(
            HopGui.getInstance().getLog(), variables, "ProjectHome", objects);
        return (String) objects[1];
      } catch (Exception e) {
        return null;
      }
    }
    if (wImportPath == null) {
      return null;
    }
    return variables.resolve(wImportPath.getText());
  }

  private void applyMetadataProvider(IHopMetadataProvider provider, String metadataFolder) {
    this.dialogMetadataProvider = provider;
    this.boundMetadataFolder = metadataFolder;
    if (wPipelineRunConfiguration != null) {
      wPipelineRunConfiguration.setMetadataProvider(provider);
    }
    if (wWorkflowRunConfiguration != null) {
      wWorkflowRunConfiguration.setMetadataProvider(provider);
    }
    if (wNamingScheme != null) {
      wNamingScheme.setMetadataProvider(provider);
    }
  }

  IHopMetadataProvider getDialogMetadataProvider() {
    return dialogMetadataProvider;
  }

  static String metadataFolderFor(String targetFolder) {
    return HopImportBase.metadataFolderFor(targetFolder);
  }

  /**
   * Write only the selected metadata objects into the target folder, then hand that provider to the
   * importer. Objects created in the dialog live in {@link #scratchMetadata} until this point.
   */
  private void persistDialogMetadataToTarget(KettleImport kettleImport) throws HopException {
    String metadataFolder = metadataFolderFor(kettleImport.getOutputFolder().getName().getURI());
    JsonMetadataProvider json =
        new JsonMetadataProvider(Encr.getEncoder(), metadataFolder, variables);
    copyNamed(
        dialogMetadataProvider,
        json,
        PipelineRunConfiguration.class,
        wPipelineRunConfiguration.getText());
    copyNamed(
        dialogMetadataProvider,
        json,
        WorkflowRunConfiguration.class,
        wWorkflowRunConfiguration.getText());
    if (wNamingScheme != null) {
      copyNamedByKey(
          dialogMetadataProvider, json, NAMING_SCHEME_METADATA_KEY, wNamingScheme.getText());
    }
    applyMetadataProvider(json, metadataFolder);
    kettleImport.setMetadataTargetFolder(metadataFolder);
    kettleImport.setMetadataProvider(
        new MultiMetadataProvider(Encr.getEncoder(), List.of(json), variables));
  }

  static void copyNamed(
      IHopMetadataProvider from,
      IHopMetadataProvider to,
      Class<? extends IHopMetadata> type,
      String name)
      throws HopException {
    if (from == null || to == null || type == null || StringUtils.isBlank(name)) {
      return;
    }
    IHopMetadataSerializer<IHopMetadata> dest = serializer(to, type);
    if (dest.exists(name)) {
      return;
    }
    IHopMetadata object = serializer(from, type).load(name);
    if (object == null) {
      return;
    }
    dest.save(object);
  }

  static void copyNamedByKey(
      IHopMetadataProvider from, IHopMetadataProvider to, String metadataKey, String name)
      throws HopException {
    if (from == null || StringUtils.isBlank(metadataKey) || StringUtils.isBlank(name)) {
      return;
    }
    copyNamed(from, to, from.getMetadataClassForKey(metadataKey), name);
  }

  @SuppressWarnings("unchecked")
  private static IHopMetadataSerializer<IHopMetadata> serializer(
      IHopMetadataProvider provider, Class<? extends IHopMetadata> type) throws HopException {
    return (IHopMetadataSerializer<IHopMetadata>) provider.getSerializer(type);
  }
}
