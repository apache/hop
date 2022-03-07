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

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.SingletonUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

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

  private final IVariables variables;

  private Shell shell;
  private final PropsUi props;

  private final KettleImport kettleImport;
  private final List<String> projectNames;

  private TextVar wImportFrom;
  private TextVar wImportPath;
  private TextVar wKettleProps;
  private TextVar wShared;
  private TextVar wJdbcProps;
  private TextVar wTargetConfigFile;

  private Combo wPipelineRunConfiguration;
  private Combo wWorkflowRunConfiguration;
  private Combo wImportProject;
  private Button wImportInExisting;
  private Button wbImportPath;
  private Button wSkipExisting;
  private Button wSkipHidden;
  private Button wSkipFolders;

  public KettleImportDialog(Shell parent, IVariables variables, KettleImport kettleImport)
      throws HopException {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE);

    props = PropsUi.getInstance();

    this.variables = variables;
    this.kettleImport = kettleImport;

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
    props.setLook(shell);

    int margin = Const.MARGIN + 2;
    int middle = Const.MIDDLE_PCT;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "KettleImportDialog.Shell.Name"));

    // Select folder to import from
    Label wlImportFrom = new Label(shell, SWT.RIGHT);
    props.setLook(wlImportFrom);
    wlImportFrom.setText(BaseMessages.getString(PKG, "KettleImportDialog.Label.ImportFrom"));
    FormData fdlImportFrom = new FormData();
    fdlImportFrom.left = new FormAttachment(0, 0);
    fdlImportFrom.right = new FormAttachment(middle, 0);
    fdlImportFrom.top = new FormAttachment(0, margin);
    wlImportFrom.setLayoutData(fdlImportFrom);

    Button wbImportFrom = new Button(shell, SWT.PUSH);
    props.setLook(wbImportFrom);
    wbImportFrom.setText(BaseMessages.getString(PKG, "KettleImportDialog.Button.Browse"));
    FormData fdbImportFrom = new FormData();
    fdbImportFrom.right = new FormAttachment(100, 0);
    fdbImportFrom.top = new FormAttachment(wlImportFrom, 0, SWT.CENTER);
    wbImportFrom.setLayoutData(fdbImportFrom);
    wbImportFrom.addListener(SWT.Selection, this::browseHomeFolder);

    wImportFrom = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wImportFrom);
    FormData fdImportFrom = new FormData();
    fdImportFrom.left = new FormAttachment(middle, margin);
    fdImportFrom.right = new FormAttachment(wbImportFrom, -margin);
    fdImportFrom.top = new FormAttachment(wlImportFrom, 0, SWT.CENTER);
    wImportFrom.setLayoutData(fdImportFrom);
    Control lastControl = wImportFrom;

    // Import in existing project?
    Label wlImportInExisting = new Label(shell, SWT.RIGHT);
    props.setLook(wlImportInExisting);
    wlImportInExisting.setText(
        BaseMessages.getString(PKG, "KettleImportDialog.Label.ImportInExistingProject"));
    FormData fdlImportInExisting = new FormData();
    fdlImportInExisting.left = new FormAttachment(0, 0);
    fdlImportInExisting.right = new FormAttachment(middle, 0);
    fdlImportInExisting.top = new FormAttachment(lastControl, margin);
    wlImportInExisting.setLayoutData(fdlImportInExisting);

    wImportInExisting = new Button(shell, SWT.CHECK);
    wImportInExisting.setSelection(true);
    props.setLook(wImportInExisting);
    FormData fdcbImportInExisting = new FormData();
    fdcbImportInExisting.left = new FormAttachment(middle, margin);
    fdcbImportInExisting.right = new FormAttachment(100, 0);
    fdcbImportInExisting.top = new FormAttachment(wlImportInExisting, 0, SWT.CENTER);
    wImportInExisting.setLayoutData(fdcbImportInExisting);
    wImportInExisting.setSelection(true);
    wImportInExisting.addListener(SWT.Selection, this::showHideProjectFields);
    lastControl = wlImportInExisting;

    // Import in project
    Label wlImportProject = new Label(shell, SWT.RIGHT);
    props.setLook(wlImportProject);
    wlImportProject.setText(
        BaseMessages.getString(PKG, "KettleImportDialog.Label.ImportInProject"));
    FormData fdlImportProject = new FormData();
    fdlImportProject.left = new FormAttachment(0, 0);
    fdlImportProject.right = new FormAttachment(middle, 0);
    fdlImportProject.top = new FormAttachment(lastControl, margin);
    wlImportProject.setLayoutData(fdlImportProject);

    wImportProject = new Combo(shell, SWT.READ_ONLY);
    wImportProject.setItems(projectNames.toArray(new String[projectNames.size()]));
    props.setLook(wImportProject);
    FormData fdImportProject = new FormData();
    fdImportProject.left = new FormAttachment(middle, margin);
    fdImportProject.right = new FormAttachment(100, 0);
    fdImportProject.top = new FormAttachment(wlImportProject, 0, SWT.CENTER);
    wImportProject.setLayoutData(fdImportProject);
    lastControl = wlImportProject;

    // Import in path
    Label wlImportPath = new Label(shell, SWT.RIGHT);
    props.setLook(wlImportPath);
    wlImportPath.setText(BaseMessages.getString(PKG, "KettleImportDialog.Label.ImportToFolder"));
    FormData fdlImportPath = new FormData();
    fdlImportPath.left = new FormAttachment(0, 0);
    fdlImportPath.right = new FormAttachment(middle, 0);
    fdlImportPath.top = new FormAttachment(lastControl, margin);
    wlImportPath.setLayoutData(fdlImportPath);

    wbImportPath = new Button(shell, SWT.PUSH);
    props.setLook(wbImportPath);
    wbImportPath.setText(BaseMessages.getString(PKG, "KettleImportDialog.Button.Browse"));
    FormData fdbImportPath = new FormData();
    fdbImportPath.right = new FormAttachment(100, 0);
    fdbImportPath.top = new FormAttachment(wlImportPath, 0, SWT.CENTER);
    wbImportPath.setLayoutData(fdbImportPath);
    wbImportPath.setEnabled(false);
    wbImportPath.addListener(SWT.Selection, this::browseTargetFolder);

    wImportPath = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wImportPath);
    FormData fdImportPath = new FormData();
    fdImportPath.left = new FormAttachment(middle, margin);
    fdImportPath.right = new FormAttachment(wbImportPath, -margin);
    fdImportPath.top = new FormAttachment(wlImportPath, 0, SWT.CENTER);
    wImportPath.setLayoutData(fdImportPath);
    wImportPath.setEditable(false);
    lastControl = wImportPath;

    // Kettle properties path
    Label wlKettleProps = new Label(shell, SWT.RIGHT);
    props.setLook(wlKettleProps);
    wlKettleProps.setText(
        BaseMessages.getString(PKG, "KettleImportDialog.Label.PathToKettleProperties"));
    FormData fdlKettleProps = new FormData();
    fdlKettleProps.left = new FormAttachment(0, 0);
    fdlKettleProps.right = new FormAttachment(middle, 0);
    fdlKettleProps.top = new FormAttachment(lastControl, margin);
    wlKettleProps.setLayoutData(fdlKettleProps);

    Button wbKettleProps = new Button(shell, SWT.PUSH);
    props.setLook(wbKettleProps);
    wbKettleProps.setText(BaseMessages.getString(PKG, "KettleImportDialog.Button.Browse"));
    FormData fdbKettleProps = new FormData();
    fdbKettleProps.right = new FormAttachment(100, 0);
    fdbKettleProps.top = new FormAttachment(wlKettleProps, 0, SWT.CENTER);
    wbKettleProps.setLayoutData(fdbKettleProps);
    wbKettleProps.addListener(SWT.Selection, this::browseKettlePropsFile);

    wKettleProps = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wKettleProps);
    FormData fdKettleProps = new FormData();
    fdKettleProps.left = new FormAttachment(middle, margin);
    fdKettleProps.right = new FormAttachment(wbKettleProps, -margin);
    fdKettleProps.top = new FormAttachment(wlKettleProps, 0, SWT.CENTER);
    wKettleProps.setLayoutData(fdKettleProps);
    lastControl = wKettleProps;

    // Shared.xml path
    Label wlShared = new Label(shell, SWT.RIGHT);
    props.setLook(wlShared);
    wlShared.setText(BaseMessages.getString(PKG, "KettleImportDialog.Label.PathToSharedXml"));
    FormData fdlShared = new FormData();
    fdlShared.left = new FormAttachment(0, 0);
    fdlShared.right = new FormAttachment(middle, 0);
    fdlShared.top = new FormAttachment(lastControl, margin);
    wlShared.setLayoutData(fdlShared);

    Button wbShared = new Button(shell, SWT.PUSH);
    wbShared.setText(BaseMessages.getString(PKG, "KettleImportDialog.Button.Browse"));
    FormData fdbShared = new FormData();
    fdbShared.right = new FormAttachment(100, 0);
    fdbShared.top = new FormAttachment(wlShared, 0, SWT.CENTER);
    wbShared.setLayoutData(fdbShared);
    wbShared.addListener(SWT.Selection, this::browseXmlFile);

    wShared = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wShared);
    FormData fdShared = new FormData();
    fdShared.left = new FormAttachment(middle, margin);
    fdShared.right = new FormAttachment(wbShared, -margin);
    fdShared.top = new FormAttachment(wlShared, 0, SWT.CENTER);
    wShared.setLayoutData(fdShared);
    lastControl = wShared;

    // Jdbc properties path
    Label wlJdbcProps = new Label(shell, SWT.RIGHT);
    props.setLook(wlJdbcProps);
    wlJdbcProps.setText(
        BaseMessages.getString(PKG, "KettleImportDialog.Label.PathToJDBCProperties"));
    FormData fdlJdbcProps = new FormData();
    fdlJdbcProps.left = new FormAttachment(0, 0);
    fdlJdbcProps.right = new FormAttachment(middle, 0);
    fdlJdbcProps.top = new FormAttachment(lastControl, margin);
    wlJdbcProps.setLayoutData(fdlJdbcProps);

    Button wbJdbcProps = new Button(shell, SWT.PUSH);
    props.setLook(wbJdbcProps);
    wbJdbcProps.setText(BaseMessages.getString(PKG, "KettleImportDialog.Button.Browse"));
    FormData fdbJdbcProps = new FormData();
    fdbJdbcProps.right = new FormAttachment(100, 0);
    fdbJdbcProps.top = new FormAttachment(wlJdbcProps, 0, SWT.CENTER);
    wbJdbcProps.setLayoutData(fdbJdbcProps);
    wbJdbcProps.addListener(SWT.Selection, this::browseJdbcPropsFile);

    wJdbcProps = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wJdbcProps);
    FormData fdJdbcProps = new FormData();
    fdJdbcProps.left = new FormAttachment(middle, margin);
    fdJdbcProps.right = new FormAttachment(wbJdbcProps, -margin);
    fdJdbcProps.top = new FormAttachment(wlJdbcProps, 0, SWT.CENTER);
    wJdbcProps.setLayoutData(fdJdbcProps);
    lastControl = wJdbcProps;

    // Skip existing target files?
    Label wlSkipExisting = new Label(shell, SWT.RIGHT);
    props.setLook(wlSkipExisting);
    wlSkipExisting.setText(
        BaseMessages.getString(PKG, "KettleImportDialog.Label.SkipExistingTargetFiles"));
    FormData fdlSkipExisting = new FormData();
    fdlSkipExisting.left = new FormAttachment(0, 0);
    fdlSkipExisting.right = new FormAttachment(middle, 0);
    fdlSkipExisting.top = new FormAttachment(lastControl, margin);
    wlSkipExisting.setLayoutData(fdlSkipExisting);

    wSkipExisting = new Button(shell, SWT.CHECK);
    props.setLook(wSkipExisting);
    FormData fdSkipExisting = new FormData();
    fdSkipExisting.left = new FormAttachment(middle, margin);
    fdSkipExisting.right = new FormAttachment(100, 0);
    fdSkipExisting.top = new FormAttachment(wlSkipExisting, 0, SWT.CENTER);
    wSkipExisting.setLayoutData(fdSkipExisting);
    wSkipExisting.setSelection(true);
    lastControl = wlSkipExisting;

    // Skip existing target files?
    Label wlSkipHidden = new Label(shell, SWT.RIGHT);
    props.setLook(wlSkipHidden);
    wlSkipHidden.setText(BaseMessages.getString(PKG, "KettleImportDialog.Label.SkipHiddenFiles"));
    FormData fdlSkipHidden = new FormData();
    fdlSkipHidden.left = new FormAttachment(0, 0);
    fdlSkipHidden.right = new FormAttachment(middle, 0);
    fdlSkipHidden.top = new FormAttachment(lastControl, margin);
    wlSkipHidden.setLayoutData(fdlSkipHidden);

    wSkipHidden = new Button(shell, SWT.CHECK);
    props.setLook(wSkipHidden);
    FormData fdSkipHidden = new FormData();
    fdSkipHidden.left = new FormAttachment(middle, margin);
    fdSkipHidden.right = new FormAttachment(100, 0);
    fdSkipHidden.top = new FormAttachment(wlSkipHidden, 0, SWT.CENTER);
    wSkipHidden.setLayoutData(fdSkipHidden);
    wSkipHidden.setSelection(true);
    lastControl = wlSkipHidden;

    // Skip existing target files?
    Label wlSkipFolders = new Label(shell, SWT.RIGHT);
    props.setLook(wlSkipFolders);
    wlSkipFolders.setText(BaseMessages.getString(PKG, "KettleImportDialog.Label.SkipFolders"));
    FormData fdlSkipFolders = new FormData();
    fdlSkipFolders.left = new FormAttachment(0, 0);
    fdlSkipFolders.right = new FormAttachment(middle, 0);
    fdlSkipFolders.top = new FormAttachment(lastControl, margin);
    wlSkipFolders.setLayoutData(fdlSkipFolders);

    wSkipFolders = new Button(shell, SWT.CHECK);
    props.setLook(wSkipFolders);
    FormData fdSkipFolders = new FormData();
    fdSkipFolders.left = new FormAttachment(middle, margin);
    fdSkipFolders.right = new FormAttachment(100, 0);
    fdSkipFolders.top = new FormAttachment(wlSkipFolders, 0, SWT.CENTER);
    wSkipFolders.setLayoutData(fdSkipFolders);
    wSkipFolders.setSelection(true);
    wSkipFolders.addListener(SWT.Selection, this::showHideProjectFields);
    lastControl = wlSkipFolders;

    // Target environment configuration file
    Label wlTargetConfigFile = new Label(shell, SWT.RIGHT);
    props.setLook(wlTargetConfigFile);
    wlTargetConfigFile.setText(
        BaseMessages.getString(PKG, "KettleImportDialog.Label.TargetConfigFile"));
    FormData fdlTargetConfigFile = new FormData();
    fdlTargetConfigFile.left = new FormAttachment(0, 0);
    fdlTargetConfigFile.right = new FormAttachment(middle, 0);
    fdlTargetConfigFile.top = new FormAttachment(lastControl, margin);
    wlTargetConfigFile.setLayoutData(fdlTargetConfigFile);

    wTargetConfigFile = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wTargetConfigFile);
    FormData fdTargetConfigFile = new FormData();
    fdTargetConfigFile.left = new FormAttachment(middle, margin);
    fdTargetConfigFile.right = new FormAttachment(100, 0);
    fdTargetConfigFile.top = new FormAttachment(wlTargetConfigFile, 0, SWT.CENTER);
    wTargetConfigFile.setLayoutData(fdTargetConfigFile);
    wTargetConfigFile.setEditable(false);

    lastControl = wTargetConfigFile;

    Label wlPipelineRunConfiguration = new Label(shell, SWT.RIGHT);
    wlPipelineRunConfiguration.setText(
        BaseMessages.getString(PKG, "KettleImportDialog.Pipeline.RunConfiguration.Label"));
    props.setLook(wlPipelineRunConfiguration);
    FormData fdlPipelineRunConfiguration = new FormData();
    fdlPipelineRunConfiguration.left = new FormAttachment(0, 0);
    fdlPipelineRunConfiguration.right = new FormAttachment(middle, 0);
    fdlPipelineRunConfiguration.top = new FormAttachment(lastControl, margin);
    wlPipelineRunConfiguration.setLayoutData(fdlPipelineRunConfiguration);

    wPipelineRunConfiguration = new Combo(shell, SWT.READ_ONLY);
    props.setLook(wlPipelineRunConfiguration);
    FormData fdPipelineRunConfiguration = new FormData();
    fdPipelineRunConfiguration.left = new FormAttachment(middle, margin);
    fdPipelineRunConfiguration.top = new FormAttachment(wlPipelineRunConfiguration, 0, SWT.CENTER);
    fdPipelineRunConfiguration.right = new FormAttachment(100, 0);
    wPipelineRunConfiguration.setLayoutData(fdPipelineRunConfiguration);
    props.setLook(wPipelineRunConfiguration);

    HopGui hopGui = HopGui.getInstance();
    IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();

    try {
      List<String> runConfigurations =
          metadataProvider.getSerializer(PipelineRunConfiguration.class).listObjectNames();

      try {
        ExtensionPointHandler.callExtensionPoint(
            HopGui.getInstance().getLog(),
            variables,
            HopExtensionPoint.HopGuiRunConfiguration.id,
            new Object[] {runConfigurations, PipelineMeta.XML_TAG});
      } catch (HopException e) {
        // Ignore errors
      }

      wPipelineRunConfiguration.setItems(runConfigurations.toArray(new String[0]));
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting pipeline run configurations", e);
    }

    lastControl = wPipelineRunConfiguration;

    Label wlWorkflowRunConfiguration = new Label(shell, SWT.RIGHT);
    wlWorkflowRunConfiguration.setText(
        BaseMessages.getString(PKG, "KettleImportDialog.Workflow.RunConfiguration.Label"));
    props.setLook(wlWorkflowRunConfiguration);
    FormData fdlWorkflowRunConfiguration = new FormData();
    fdlWorkflowRunConfiguration.left = new FormAttachment(0, 0);
    fdlWorkflowRunConfiguration.right = new FormAttachment(middle, 0);
    fdlWorkflowRunConfiguration.top = new FormAttachment(lastControl, margin);
    wlWorkflowRunConfiguration.setLayoutData(fdlWorkflowRunConfiguration);

    wWorkflowRunConfiguration = new Combo(shell, SWT.READ_ONLY);
    props.setLook(wlWorkflowRunConfiguration);
    FormData fdWorkflowRunConfiguration = new FormData();
    fdWorkflowRunConfiguration.left = new FormAttachment(middle, margin);
    fdWorkflowRunConfiguration.top = new FormAttachment(wlWorkflowRunConfiguration, 0, SWT.CENTER);
    fdWorkflowRunConfiguration.right = new FormAttachment(100, 0);
    wWorkflowRunConfiguration.setLayoutData(fdWorkflowRunConfiguration);
    props.setLook(wWorkflowRunConfiguration);

    try {
      List<String> runConfigurations =
          metadataProvider.getSerializer(WorkflowRunConfiguration.class).listObjectNames();

      try {
        ExtensionPointHandler.callExtensionPoint(
            HopGui.getInstance().getLog(),
            variables,
            HopExtensionPoint.HopGuiRunConfiguration.id,
            new Object[] {runConfigurations, PipelineMeta.XML_TAG});
      } catch (HopException e) {
        // Ignore errors
      }

      wWorkflowRunConfiguration.setItems(runConfigurations.toArray(new String[0]));
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting workflow run configurations", e);
    }

    lastControl = wWorkflowRunConfiguration;

    Label separator = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdLine = new FormData();
    fdLine.height = 5;
    fdLine.left = new FormAttachment(0, 0);
    fdLine.right = new FormAttachment(100, 0);
    fdLine.top = new FormAttachment(lastControl, margin);
    separator.setLayoutData(fdLine);
    lastControl = separator;

    // Buttons go at the bottom of the dialog
    //
    Button wImport = new Button(shell, SWT.PUSH);
    wImport.setText("Import");
    wImport.addListener(SWT.Selection, event -> doImport());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, event -> dispose());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wImport, wCancel}, margin, lastControl);

    // See if we need to remember previous settings...
    //
    wImportFrom.setText(
        Const.NVL(
            AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_SOURCE_FOLDER),
            Const.NVL(kettleImport.getInputFolderName(), "")));
    wImportInExisting.setSelection(
        !"false"
            .equalsIgnoreCase(AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_INTO_PROJECT)));
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
        !"false"
            .equalsIgnoreCase(
                AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_SKIP_EXISTING)));
    wSkipHidden.setSelection(
        !"false"
            .equalsIgnoreCase(AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_SKIP_HIDDEN)));
    wSkipFolders.setSelection(
        !"false"
            .equalsIgnoreCase(AuditManagerGuiUtil.getLastUsedValue(LAST_USED_IMPORT_SKIP_FOLDERS)));

    wImportFrom.setFocus();

    BaseDialog.defaultShellHandling(shell, c -> dispose(), c -> dispose());
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    AuditManagerGuiUtil.addLastUsedValue(LAST_USED_IMPORT_SOURCE_FOLDER, wImportFrom.getText());
    AuditManagerGuiUtil.addLastUsedValue(
        LAST_USED_IMPORT_INTO_PROJECT, wImportInExisting.getSelection() ? "true" : "false");
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
    AuditManagerGuiUtil.addLastUsedValue(
        LAST_USED_IMPORT_SKIP_EXISTING, wSkipExisting.getSelection() ? "true" : "false");
    AuditManagerGuiUtil.addLastUsedValue(
        LAST_USED_IMPORT_SKIP_HIDDEN, wSkipHidden.getSelection() ? "true" : "false");
    AuditManagerGuiUtil.addLastUsedValue(
        LAST_USED_IMPORT_SKIP_FOLDERS, wSkipFolders.getSelection() ? "true" : "false");
    shell.dispose();
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
        new String[] {"Properties files (*.properties)", "All Files (*.*)"},
        true);
  }

  private void browseJdbcPropsFile(Event event) {
    BaseDialog.presentFileDialog(
        shell,
        wJdbcProps,
        variables,
        new String[] {"*.properties", "*.*"},
        new String[] {"Properties files (*.properties)", "All Files (*.*)"},
        true);
  }

  private void browseXmlFile(Event event) {
    BaseDialog.presentFileDialog(
        shell,
        wShared,
        variables,
        new String[] {"*.xml", "*.*"},
        new String[] {"XML files (*.xml)", "All Files (*.*)"},
        true);
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

      String sourceFolder = wImportFrom.getText();
      String targetFolder = wImportPath.getText();

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
      kettleImport.setSharedXmlFilename(wShared.getText());
      kettleImport.setKettlePropertiesFilename(wKettleProps.getText());
      kettleImport.setJdbcPropertiesFilename(wJdbcProps.getText());
      kettleImport.setSkippingExistingTargetFiles(wSkipExisting.getSelection());
      kettleImport.setSkippingHiddenFilesAndFolders(wSkipHidden.getSelection());
      kettleImport.setSkippingFolders(wSkipFolders.getSelection());
      kettleImport.setDefaultPipelineRunConfiguration(
          Const.NVL(wPipelineRunConfiguration.getText(), ""));
      kettleImport.setDefaultWorkflowRunConfiguration(
          Const.NVL(wWorkflowRunConfiguration.getText(), ""));

      // We're going to run the import in a progress dialog with a monitor...
      //
      ProgressMonitorDialog monitorDialog =
          new ProgressMonitorDialog(HopGui.getInstance().getShell());
      monitorDialog.run(
          true,
          monitor -> {
            try {
              monitor.beginTask("Importing files", 4);
              kettleImport.runImport(monitor);
              monitor.done();
            } catch (Throwable e) {
              throw new InvocationTargetException(e, "Error importing " + Const.getStackTracker(e));
            }
          });

      // Show some statistics after the import...
      //
      MessageBox box = new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_INFORMATION);
      box.setText("Import summary");
      box.setMessage(kettleImport.getImportReport());
      box.open();
    } catch (Throwable e) {
      new ErrorDialog(shell, "Error", "Error importing", e);
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
  }
}
