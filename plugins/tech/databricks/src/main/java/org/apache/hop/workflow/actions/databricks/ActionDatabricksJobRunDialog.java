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

package org.apache.hop.workflow.actions.databricks;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databricks.metadata.DatabricksConnection;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Dialog for {@link ActionDatabricksJobRun}. */
public class ActionDatabricksJobRunDialog extends ActionDialog {
  private static final Class<?> PKG = ActionDatabricksJobRun.class;

  private ActionDatabricksJobRun action;
  private boolean changed;

  private Text wName;
  private MetaSelectionLine<DatabricksConnection> wConnection;
  private CCombo wRunMode;
  private TextVar wJobId;
  private TextVar wJobName;
  private Button wUpdateJob;
  private StyledText wSubmitJson;
  private TextVar wFatJar;
  private Button wbFatJarBrowse;
  private TextVar wPipeline;
  private Button wbPipelineBrowse;
  private Button wbPipelineOpen;
  private MetaSelectionLine<PipelineRunConfiguration> wRunConfig;
  private TextVar wDbfsBase;
  private Button wUploadProjectPackage;
  private TextVar wProjectHome;
  private TextVar wProjectPackageFile;
  private Button wbProjectPackageBrowse;
  private TextVar wEnvironmentConfigFile;
  private Button wbEnvironmentConfigBrowse;
  private TextVar wClusterId;
  private TextVar wNewClusterSparkVersion;
  private TextVar wNewClusterNodeType;
  private TextVar wNewClusterNumWorkers;
  private StyledText wNewClusterJson;
  private TextVar wEnvironmentKey;
  private TextVar wEnvironmentClient;
  private CCombo wWaitMode;
  private TextVar wTimeout;
  private TextVar wPoll;
  private TextVar wVarJobId;
  private TextVar wVarRunId;
  private TextVar wVarStatus;
  private TextVar wVarPageUrl;
  private TextVar wVarError;

  private Label wlJobId;
  private Label wlSubmitJson;

  public ActionDatabricksJobRunDialog(
      Shell parent,
      ActionDatabricksJobRun action,
      WorkflowMeta workflowMeta,
      IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    changed = action.hasChanged();
    ModifyListener lsMod = e -> action.setChanged();

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Name.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder);
    FormData fdTab = new FormData();
    fdTab.left = new FormAttachment(0, 0);
    fdTab.top = new FormAttachment(wName, margin);
    fdTab.right = new FormAttachment(100, 0);
    fdTab.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTab);

    addJobTab(wTabFolder, middle, margin, lsMod);
    addDeployTab(wTabFolder, middle, margin, lsMod);
    addPackageTab(wTabFolder, middle, margin, lsMod);
    addComputeTab(wTabFolder, middle, margin, lsMod);
    addRunTab(wTabFolder, middle, margin, lsMod);

    wTabFolder.setSelection(0);
    getData();
    enableModeFields();
    wName.setFocus();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return action;
  }

  /** Connection, mode, job id/name, update flag, submit JSON. */
  private void addJobTab(CTabFolder wTabFolder, int middle, int margin, ModifyListener lsMod) {
    CTabItem tabJob = new CTabItem(wTabFolder, SWT.NONE);
    tabJob.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Tab.Job"));
    Composite wJobComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wJobComp);
    wJobComp.setLayout(new FormLayout());
    tabJob.setControl(wJobComp);

    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            DatabricksConnection.class,
            wJobComp,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionDatabricksJobRun.Connection.Label"),
            BaseMessages.getString(PKG, "ActionDatabricksJobRun.Connection.Tooltip"));
    PropsUi.setLook(wConnection);
    FormData fdConn = new FormData();
    fdConn.left = new FormAttachment(0, 0);
    fdConn.top = new FormAttachment(0, margin);
    fdConn.right = new FormAttachment(100, 0);
    wConnection.setLayoutData(fdConn);
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      // ignore empty metadata
    }
    wConnection.addModifyListener(lsMod);

    Label wlRunMode = new Label(wJobComp, SWT.RIGHT);
    wlRunMode.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.RunMode.Label"));
    PropsUi.setLook(wlRunMode);
    FormData fdlRunMode = new FormData();
    fdlRunMode.left = new FormAttachment(0, 0);
    fdlRunMode.right = new FormAttachment(middle, -margin);
    fdlRunMode.top = new FormAttachment(wConnection, margin);
    wlRunMode.setLayoutData(fdlRunMode);
    wRunMode = new CCombo(wJobComp, SWT.BORDER | SWT.READ_ONLY);
    wRunMode.setItems(
        new String[] {
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.RunMode.RunExisting"),
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.RunMode.SubmitOnce"),
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.RunMode.DeployAndRun")
        });
    PropsUi.setLook(wRunMode);
    FormData fdRunMode = new FormData();
    fdRunMode.left = new FormAttachment(middle, 0);
    fdRunMode.top = new FormAttachment(wConnection, margin);
    fdRunMode.right = new FormAttachment(100, 0);
    wRunMode.setLayoutData(fdRunMode);
    wRunMode.addModifyListener(lsMod);
    wRunMode.addListener(SWT.Selection, e -> enableModeFields());

    wlJobId = new Label(wJobComp, SWT.RIGHT);
    wlJobId.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.JobId.Label"));
    PropsUi.setLook(wlJobId);
    FormData fdlJobId = new FormData();
    fdlJobId.left = new FormAttachment(0, 0);
    fdlJobId.right = new FormAttachment(middle, -margin);
    fdlJobId.top = new FormAttachment(wRunMode, margin);
    wlJobId.setLayoutData(fdlJobId);
    wJobId = new TextVar(variables, wJobComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wJobId);
    wJobId.addModifyListener(lsMod);
    FormData fdJobId = new FormData();
    fdJobId.left = new FormAttachment(middle, 0);
    fdJobId.top = new FormAttachment(wRunMode, margin);
    fdJobId.right = new FormAttachment(100, 0);
    wJobId.setLayoutData(fdJobId);

    wJobName =
        addLabeledTextVar(
            wJobComp, wJobId, middle, margin, lsMod, "ActionDatabricksJobRun.JobName.Label");

    wUpdateJob = new Button(wJobComp, SWT.CHECK);
    PropsUi.setLook(wUpdateJob);
    wUpdateJob.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.UpdateJob.Label"));
    FormData fdUpdate = new FormData();
    fdUpdate.left = new FormAttachment(middle, 0);
    fdUpdate.top = new FormAttachment(wJobName, margin);
    wUpdateJob.setLayoutData(fdUpdate);
    wUpdateJob.addListener(SWT.Selection, e -> action.setChanged());

    wlSubmitJson = new Label(wJobComp, SWT.RIGHT);
    wlSubmitJson.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.SubmitJson.Label"));
    PropsUi.setLook(wlSubmitJson);
    FormData fdlSubmit = new FormData();
    fdlSubmit.left = new FormAttachment(0, 0);
    fdlSubmit.right = new FormAttachment(middle, -margin);
    fdlSubmit.top = new FormAttachment(wUpdateJob, margin);
    wlSubmitJson.setLayoutData(fdlSubmit);
    wSubmitJson =
        new StyledText(wJobComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wSubmitJson);
    wSubmitJson.addModifyListener(lsMod);
    FormData fdSubmit = new FormData();
    fdSubmit.left = new FormAttachment(middle, 0);
    fdSubmit.top = new FormAttachment(wUpdateJob, margin);
    fdSubmit.right = new FormAttachment(100, 0);
    fdSubmit.bottom = new FormAttachment(100, -margin);
    fdSubmit.height = 80;
    wSubmitJson.setLayoutData(fdSubmit);
  }

  /** Fat jar, pipeline, run configuration, upload base. */
  private void addDeployTab(CTabFolder wTabFolder, int middle, int margin, ModifyListener lsMod) {
    CTabItem tabDeploy = new CTabItem(wTabFolder, SWT.NONE);
    tabDeploy.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Tab.Deploy"));
    Composite wDeployComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wDeployComp);
    wDeployComp.setLayout(new FormLayout());
    tabDeploy.setControl(wDeployComp);

    Label wlFatJar = new Label(wDeployComp, SWT.RIGHT);
    wlFatJar.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.FatJar.Label"));
    PropsUi.setLook(wlFatJar);
    FormData fdlFatJar = new FormData();
    fdlFatJar.left = new FormAttachment(0, 0);
    fdlFatJar.right = new FormAttachment(middle, -margin);
    fdlFatJar.top = new FormAttachment(0, margin);
    wlFatJar.setLayoutData(fdlFatJar);
    wbFatJarBrowse = new Button(wDeployComp, SWT.PUSH);
    PropsUi.setLook(wbFatJarBrowse);
    wbFatJarBrowse.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFatJar = new FormData();
    fdbFatJar.right = new FormAttachment(100, 0);
    fdbFatJar.top = new FormAttachment(wlFatJar, 0, SWT.CENTER);
    wbFatJarBrowse.setLayoutData(fdbFatJar);
    wbFatJarBrowse.addListener(SWT.Selection, e -> browseFatJar());
    wFatJar = new TextVar(variables, wDeployComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFatJar);
    wFatJar.addModifyListener(lsMod);
    FormData fdFatJar = new FormData();
    fdFatJar.left = new FormAttachment(middle, 0);
    fdFatJar.top = new FormAttachment(wlFatJar, 0, SWT.CENTER);
    fdFatJar.right = new FormAttachment(wbFatJarBrowse, -margin);
    wFatJar.setLayoutData(fdFatJar);

    Label wlPipeline = new Label(wDeployComp, SWT.RIGHT);
    wlPipeline.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Pipeline.Label"));
    PropsUi.setLook(wlPipeline);
    FormData fdlPipeline = new FormData();
    fdlPipeline.left = new FormAttachment(0, 0);
    fdlPipeline.right = new FormAttachment(middle, -margin);
    fdlPipeline.top = new FormAttachment(wFatJar, margin);
    wlPipeline.setLayoutData(fdlPipeline);
    wbPipelineOpen = new Button(wDeployComp, SWT.PUSH);
    PropsUi.setLook(wbPipelineOpen);
    wbPipelineOpen.setText(BaseMessages.getString(PKG, "System.Button.Open"));
    FormData fdbPipelineOpen = new FormData();
    fdbPipelineOpen.right = new FormAttachment(100, 0);
    fdbPipelineOpen.top = new FormAttachment(wlPipeline, 0, SWT.CENTER);
    wbPipelineOpen.setLayoutData(fdbPipelineOpen);
    wbPipelineOpen.addListener(SWT.Selection, e -> openPipeline());
    wbPipelineBrowse = new Button(wDeployComp, SWT.PUSH);
    PropsUi.setLook(wbPipelineBrowse);
    wbPipelineBrowse.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbPipelineBrowse = new FormData();
    fdbPipelineBrowse.right = new FormAttachment(wbPipelineOpen, -margin);
    fdbPipelineBrowse.top = new FormAttachment(wlPipeline, 0, SWT.CENTER);
    wbPipelineBrowse.setLayoutData(fdbPipelineBrowse);
    wbPipelineBrowse.addListener(SWT.Selection, e -> browsePipeline());
    wPipeline = new TextVar(variables, wDeployComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPipeline);
    wPipeline.addModifyListener(lsMod);
    FormData fdPipeline = new FormData();
    fdPipeline.left = new FormAttachment(middle, 0);
    fdPipeline.top = new FormAttachment(wlPipeline, 0, SWT.CENTER);
    fdPipeline.right = new FormAttachment(wbPipelineBrowse, -margin);
    wPipeline.setLayoutData(fdPipeline);

    wRunConfig =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            PipelineRunConfiguration.class,
            wDeployComp,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionDatabricksJobRun.RunConfig.Label"),
            BaseMessages.getString(PKG, "ActionDatabricksJobRun.RunConfig.Tooltip"));
    PropsUi.setLook(wRunConfig);
    FormData fdRunConfig = new FormData();
    fdRunConfig.left = new FormAttachment(0, 0);
    fdRunConfig.top = new FormAttachment(wPipeline, margin);
    fdRunConfig.right = new FormAttachment(100, 0);
    wRunConfig.setLayoutData(fdRunConfig);
    try {
      wRunConfig.fillItems();
    } catch (Exception e) {
      // ignore empty metadata
    }
    wRunConfig.addModifyListener(lsMod);

    wDbfsBase =
        addLabeledTextVar(
            wDeployComp,
            wRunConfig,
            middle,
            margin,
            lsMod,
            "ActionDatabricksJobRun.DbfsBase.Label");
  }

  /** Optional Spark project package + environment config. */
  private void addPackageTab(CTabFolder wTabFolder, int middle, int margin, ModifyListener lsMod) {
    CTabItem tabPackage = new CTabItem(wTabFolder, SWT.NONE);
    tabPackage.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Tab.Package"));
    Composite wPackageComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wPackageComp);
    wPackageComp.setLayout(new FormLayout());
    tabPackage.setControl(wPackageComp);

    wUploadProjectPackage = new Button(wPackageComp, SWT.CHECK);
    PropsUi.setLook(wUploadProjectPackage);
    wUploadProjectPackage.setText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.UploadProjectPackage.Label"));
    wUploadProjectPackage.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.UploadProjectPackage.Tooltip"));
    FormData fdUploadPkg = new FormData();
    fdUploadPkg.left = new FormAttachment(middle, 0);
    fdUploadPkg.top = new FormAttachment(0, margin);
    fdUploadPkg.right = new FormAttachment(100, 0);
    wUploadProjectPackage.setLayoutData(fdUploadPkg);
    wUploadProjectPackage.addListener(SWT.Selection, e -> action.setChanged());

    wProjectHome =
        addLabeledTextVar(
            wPackageComp,
            wUploadProjectPackage,
            middle,
            margin,
            lsMod,
            "ActionDatabricksJobRun.ProjectHome.Label");
    wProjectHome.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.ProjectHome.Tooltip"));

    Label wlProjectPackage = new Label(wPackageComp, SWT.RIGHT);
    wlProjectPackage.setText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.ProjectPackageFile.Label"));
    PropsUi.setLook(wlProjectPackage);
    FormData fdlProjectPackage = new FormData();
    fdlProjectPackage.left = new FormAttachment(0, 0);
    fdlProjectPackage.right = new FormAttachment(middle, -margin);
    fdlProjectPackage.top = new FormAttachment(wProjectHome, margin);
    wlProjectPackage.setLayoutData(fdlProjectPackage);
    wbProjectPackageBrowse = new Button(wPackageComp, SWT.PUSH);
    PropsUi.setLook(wbProjectPackageBrowse);
    wbProjectPackageBrowse.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbProjectPackage = new FormData();
    fdbProjectPackage.right = new FormAttachment(100, 0);
    fdbProjectPackage.top = new FormAttachment(wlProjectPackage, 0, SWT.CENTER);
    wbProjectPackageBrowse.setLayoutData(fdbProjectPackage);
    wbProjectPackageBrowse.addListener(SWT.Selection, e -> browseProjectPackage());
    wProjectPackageFile = new TextVar(variables, wPackageComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProjectPackageFile);
    wProjectPackageFile.addModifyListener(lsMod);
    wProjectPackageFile.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.ProjectPackageFile.Tooltip"));
    FormData fdProjectPackage = new FormData();
    fdProjectPackage.left = new FormAttachment(middle, 0);
    fdProjectPackage.top = new FormAttachment(wlProjectPackage, 0, SWT.CENTER);
    fdProjectPackage.right = new FormAttachment(wbProjectPackageBrowse, -margin);
    wProjectPackageFile.setLayoutData(fdProjectPackage);

    Label wlEnvConfig = new Label(wPackageComp, SWT.RIGHT);
    wlEnvConfig.setText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.EnvironmentConfigFile.Label"));
    PropsUi.setLook(wlEnvConfig);
    FormData fdlEnvConfig = new FormData();
    fdlEnvConfig.left = new FormAttachment(0, 0);
    fdlEnvConfig.right = new FormAttachment(middle, -margin);
    fdlEnvConfig.top = new FormAttachment(wProjectPackageFile, margin);
    wlEnvConfig.setLayoutData(fdlEnvConfig);
    wbEnvironmentConfigBrowse = new Button(wPackageComp, SWT.PUSH);
    PropsUi.setLook(wbEnvironmentConfigBrowse);
    wbEnvironmentConfigBrowse.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbEnvConfig = new FormData();
    fdbEnvConfig.right = new FormAttachment(100, 0);
    fdbEnvConfig.top = new FormAttachment(wlEnvConfig, 0, SWT.CENTER);
    wbEnvironmentConfigBrowse.setLayoutData(fdbEnvConfig);
    wbEnvironmentConfigBrowse.addListener(SWT.Selection, e -> browseEnvironmentConfig());
    wEnvironmentConfigFile =
        new TextVar(variables, wPackageComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEnvironmentConfigFile);
    wEnvironmentConfigFile.addModifyListener(lsMod);
    wEnvironmentConfigFile.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.EnvironmentConfigFile.Tooltip"));
    FormData fdEnvConfig = new FormData();
    fdEnvConfig.left = new FormAttachment(middle, 0);
    fdEnvConfig.top = new FormAttachment(wlEnvConfig, 0, SWT.CENTER);
    fdEnvConfig.right = new FormAttachment(wbEnvironmentConfigBrowse, -margin);
    wEnvironmentConfigFile.setLayoutData(fdEnvConfig);
  }

  /** Cluster / new_cluster / serverless. */
  private void addComputeTab(CTabFolder wTabFolder, int middle, int margin, ModifyListener lsMod) {
    CTabItem tabCompute = new CTabItem(wTabFolder, SWT.NONE);
    tabCompute.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Tab.Compute"));
    Composite wComputeComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wComputeComp);
    wComputeComp.setLayout(new FormLayout());
    tabCompute.setControl(wComputeComp);

    wClusterId =
        addLabeledTextVar(
            wComputeComp,
            wComputeComp,
            middle,
            margin,
            lsMod,
            "ActionDatabricksJobRun.ClusterId.Label",
            true);
    wClusterId.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.ClusterId.Tooltip"));
    wNewClusterSparkVersion =
        addLabeledTextVar(
            wComputeComp,
            wClusterId,
            middle,
            margin,
            lsMod,
            "ActionDatabricksJobRun.NewClusterSparkVersion.Label");
    wNewClusterSparkVersion.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.NewClusterSparkVersion.Tooltip"));
    wNewClusterNodeType =
        addLabeledTextVar(
            wComputeComp,
            wNewClusterSparkVersion,
            middle,
            margin,
            lsMod,
            "ActionDatabricksJobRun.NewClusterNodeType.Label");
    wNewClusterNodeType.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.NewClusterNodeType.Tooltip"));
    wNewClusterNumWorkers =
        addLabeledTextVar(
            wComputeComp,
            wNewClusterNodeType,
            middle,
            margin,
            lsMod,
            "ActionDatabricksJobRun.NewClusterNumWorkers.Label");
    wNewClusterNumWorkers.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.NewClusterNumWorkers.Tooltip"));

    Label wlNewClusterJson = new Label(wComputeComp, SWT.RIGHT);
    wlNewClusterJson.setText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.NewClusterJson.Label"));
    PropsUi.setLook(wlNewClusterJson);
    FormData fdlNewClusterJson = new FormData();
    fdlNewClusterJson.left = new FormAttachment(0, 0);
    fdlNewClusterJson.right = new FormAttachment(middle, -margin);
    fdlNewClusterJson.top = new FormAttachment(wNewClusterNumWorkers, margin);
    wlNewClusterJson.setLayoutData(fdlNewClusterJson);
    wNewClusterJson =
        new StyledText(
            wComputeComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wNewClusterJson);
    wNewClusterJson.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.NewClusterJson.Tooltip"));
    wNewClusterJson.addModifyListener(lsMod);
    FormData fdNewClusterJson = new FormData();
    fdNewClusterJson.left = new FormAttachment(middle, 0);
    fdNewClusterJson.top = new FormAttachment(wNewClusterNumWorkers, margin);
    fdNewClusterJson.right = new FormAttachment(100, 0);
    fdNewClusterJson.height = 80;
    wNewClusterJson.setLayoutData(fdNewClusterJson);

    wEnvironmentKey =
        addLabeledTextVar(
            wComputeComp,
            wNewClusterJson,
            middle,
            margin,
            lsMod,
            "ActionDatabricksJobRun.EnvironmentKey.Label");
    wEnvironmentKey.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.EnvironmentKey.Tooltip"));
    wEnvironmentClient =
        addLabeledTextVar(
            wComputeComp,
            wEnvironmentKey,
            middle,
            margin,
            lsMod,
            "ActionDatabricksJobRun.EnvironmentClient.Label");
    wEnvironmentClient.setToolTipText(
        BaseMessages.getString(PKG, "ActionDatabricksJobRun.EnvironmentClient.Tooltip"));
  }

  /** Wait mode, timeout, poll, result variables. */
  private void addRunTab(CTabFolder wTabFolder, int middle, int margin, ModifyListener lsMod) {
    CTabItem tabRun = new CTabItem(wTabFolder, SWT.NONE);
    tabRun.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Tab.Run"));
    Composite wRunComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wRunComp);
    wRunComp.setLayout(new FormLayout());
    tabRun.setControl(wRunComp);

    Label wlWait = new Label(wRunComp, SWT.RIGHT);
    wlWait.setText(BaseMessages.getString(PKG, "ActionDatabricksJobRun.WaitMode.Label"));
    PropsUi.setLook(wlWait);
    FormData fdlWait = new FormData();
    fdlWait.left = new FormAttachment(0, 0);
    fdlWait.right = new FormAttachment(middle, -margin);
    fdlWait.top = new FormAttachment(0, margin);
    wlWait.setLayoutData(fdlWait);
    wWaitMode = new CCombo(wRunComp, SWT.BORDER | SWT.READ_ONLY);
    wWaitMode.setItems(
        new String[] {
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.WaitMode.Wait"),
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.WaitMode.FireAndForget")
        });
    PropsUi.setLook(wWaitMode);
    FormData fdWait = new FormData();
    fdWait.left = new FormAttachment(middle, 0);
    fdWait.top = new FormAttachment(0, margin);
    fdWait.right = new FormAttachment(100, 0);
    wWaitMode.setLayoutData(fdWait);
    wWaitMode.addModifyListener(lsMod);

    Control last = addTextVarRow(wRunComp, wWaitMode, middle, margin, lsMod, "Timeout");
    wTimeout = (TextVar) last;
    last = addTextVarRow(wRunComp, wTimeout, middle, margin, lsMod, "Poll");
    wPoll = (TextVar) last;
    last = addTextVarRow(wRunComp, wPoll, middle, margin, lsMod, "VarJobId");
    wVarJobId = (TextVar) last;
    last = addTextVarRow(wRunComp, wVarJobId, middle, margin, lsMod, "VarRunId");
    wVarRunId = (TextVar) last;
    last = addTextVarRow(wRunComp, wVarRunId, middle, margin, lsMod, "VarStatus");
    wVarStatus = (TextVar) last;
    last = addTextVarRow(wRunComp, wVarStatus, middle, margin, lsMod, "VarPageUrl");
    wVarPageUrl = (TextVar) last;
    last = addTextVarRow(wRunComp, wVarPageUrl, middle, margin, lsMod, "VarError");
    wVarError = (TextVar) last;
  }

  private TextVar addLabeledTextVar(
      Composite parent,
      Control above,
      int middle,
      int margin,
      ModifyListener lsMod,
      String labelKey) {
    return addLabeledTextVar(parent, above, middle, margin, lsMod, labelKey, false);
  }

  /**
   * @param firstOnTab when true, attach top to parent margin instead of {@code above}
   */
  private TextVar addLabeledTextVar(
      Composite parent,
      Control above,
      int middle,
      int margin,
      ModifyListener lsMod,
      String labelKey,
      boolean firstOnTab) {
    Label wl = new Label(parent, SWT.RIGHT);
    wl.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(wl);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    if (firstOnTab) {
      fdl.top = new FormAttachment(0, margin);
    } else {
      fdl.top = new FormAttachment(above, margin);
    }
    wl.setLayoutData(fdl);
    TextVar w = new TextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(w);
    w.addModifyListener(lsMod);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    if (firstOnTab) {
      fd.top = new FormAttachment(0, margin);
    } else {
      fd.top = new FormAttachment(above, margin);
    }
    fd.right = new FormAttachment(100, 0);
    w.setLayoutData(fd);
    return w;
  }

  private Control addTextVarRow(
      Composite parent, Control above, int middle, int margin, ModifyListener lsMod, String key) {
    return addLabeledTextVar(
        parent, above, middle, margin, lsMod, "ActionDatabricksJobRun." + key + ".Label");
  }

  private void enableModeFields() {
    int idx = wRunMode.getSelectionIndex();
    boolean existing = idx == 0;
    boolean submit = idx == 1;
    boolean deploy = idx == 2;
    wlJobId.setEnabled(existing || deploy);
    wJobId.setEnabled(existing || deploy);
    wlSubmitJson.setEnabled(submit);
    wSubmitJson.setEnabled(submit);
    wJobName.setEnabled(deploy);
    wUpdateJob.setEnabled(deploy);
    wFatJar.setEnabled(deploy);
    wbFatJarBrowse.setEnabled(deploy);
    wPipeline.setEnabled(deploy);
    wbPipelineBrowse.setEnabled(deploy);
    wbPipelineOpen.setEnabled(deploy);
    wRunConfig.setEnabled(deploy);
    wDbfsBase.setEnabled(deploy);
    wUploadProjectPackage.setEnabled(deploy);
    wProjectHome.setEnabled(deploy);
    wProjectPackageFile.setEnabled(deploy);
    wbProjectPackageBrowse.setEnabled(deploy);
    wEnvironmentConfigFile.setEnabled(deploy);
    wbEnvironmentConfigBrowse.setEnabled(deploy);
    wClusterId.setEnabled(deploy);
    wNewClusterSparkVersion.setEnabled(deploy);
    wNewClusterNodeType.setEnabled(deploy);
    wNewClusterNumWorkers.setEnabled(deploy);
    wNewClusterJson.setEnabled(deploy);
    wEnvironmentKey.setEnabled(deploy);
    wEnvironmentClient.setEnabled(deploy);
  }

  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wConnection.setText(Const.NVL(action.getConnectionName(), ""));
    if (ActionDatabricksJobRun.MODE_DEPLOY_AND_RUN.equalsIgnoreCase(action.getRunMode())) {
      wRunMode.select(2);
    } else if (ActionDatabricksJobRun.MODE_SUBMIT_ONCE.equalsIgnoreCase(action.getRunMode())) {
      wRunMode.select(1);
    } else {
      wRunMode.select(0);
    }
    wJobId.setText(Const.NVL(action.getJobId(), ""));
    wJobName.setText(Const.NVL(action.getJobName(), ""));
    wUpdateJob.setSelection(action.isUpdateExistingJob());
    wSubmitJson.setText(Const.NVL(action.getSubmitRunJson(), ""));
    wFatJar.setText(Const.NVL(action.getFatJarPath(), ""));
    wPipeline.setText(Const.NVL(action.getPipelineFilename(), ""));
    wRunConfig.setText(Const.NVL(action.getRunConfigurationName(), ""));
    wDbfsBase.setText(Const.NVL(action.getDbfsBasePath(), "dbfs:/FileStore/hop"));
    wUploadProjectPackage.setSelection(action.isUploadProjectPackage());
    wProjectHome.setText(Const.NVL(action.getProjectHome(), ""));
    wProjectPackageFile.setText(Const.NVL(action.getProjectPackageFile(), ""));
    wEnvironmentConfigFile.setText(Const.NVL(action.getEnvironmentConfigFile(), ""));
    wClusterId.setText(Const.NVL(action.getExistingClusterId(), ""));
    // Do not re-inject DEFAULT_* when the user cleared these (Const.NVL treats "" as empty).
    // Job create still applies factory defaults for new_cluster / serverless when blank.
    wNewClusterSparkVersion.setText(Const.NVL(action.getNewClusterSparkVersion(), ""));
    wNewClusterNodeType.setText(Const.NVL(action.getNewClusterNodeTypeId(), ""));
    wNewClusterNumWorkers.setText(Const.NVL(action.getNewClusterNumWorkers(), ""));
    wNewClusterJson.setText(Const.NVL(action.getNewClusterJson(), ""));
    wEnvironmentKey.setText(Const.NVL(action.getEnvironmentKey(), ""));
    wEnvironmentClient.setText(Const.NVL(action.getEnvironmentClient(), ""));
    if (ActionDatabricksJobRun.WAIT_FIRE_AND_FORGET.equalsIgnoreCase(action.getWaitMode())) {
      wWaitMode.select(1);
    } else {
      wWaitMode.select(0);
    }
    wTimeout.setText(Const.NVL(action.getTimeoutSeconds(), "3600"));
    wPoll.setText(Const.NVL(action.getPollIntervalSeconds(), "15"));
    wVarJobId.setText(Const.NVL(action.getResultVariableJobId(), "DatabricksJobId"));
    wVarRunId.setText(Const.NVL(action.getResultVariableRunId(), "DatabricksRunId"));
    wVarStatus.setText(Const.NVL(action.getResultVariableStatus(), "DatabricksStatus"));
    wVarPageUrl.setText(Const.NVL(action.getResultVariablePageUrl(), "DatabricksRunPageUrl"));
    wVarError.setText(Const.NVL(action.getResultVariableError(), "DatabricksError"));
  }

  private void browseFatJar() {
    BaseDialog.presentFileDialog(
        shell,
        wFatJar,
        variables,
        new String[] {"*.jar;*.JAR", "*"},
        new String[] {
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.FileType.Jar"),
          BaseMessages.getString(PKG, "System.FileType.AllFiles")
        },
        true);
  }

  private void browsePipeline() {
    HopPipelineFileType<?> pipelineFileType = new HopPipelineFileType<>();
    BaseDialog.presentFileDialog(
        shell,
        wPipeline,
        variables,
        pipelineFileType.getFilterExtensions(),
        pipelineFileType.getFilterNames(),
        true);
  }

  private void browseProjectPackage() {
    BaseDialog.presentFileDialog(
        shell,
        wProjectPackageFile,
        variables,
        new String[] {"*.zip;*.ZIP", "*"},
        new String[] {
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.FileType.Zip"),
          BaseMessages.getString(PKG, "System.FileType.AllFiles")
        },
        true);
  }

  private void browseEnvironmentConfig() {
    BaseDialog.presentFileDialog(
        shell,
        wEnvironmentConfigFile,
        variables,
        new String[] {"*.json;*.JSON", "*"},
        new String[] {
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.FileType.Json"),
          BaseMessages.getString(PKG, "System.FileType.AllFiles")
        },
        true);
  }

  private void openPipeline() {
    try {
      String filename = variables.resolve(wPipeline.getText());
      if (Utils.isEmpty(filename)) {
        return;
      }
      HopGui.getInstance().fileDelegate.fileOpen(filename);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.Error.OpenPipeline.Title"),
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.Error.OpenPipeline.Message"),
          e);
    }
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.setConnectionName(wConnection.getText());
    if (wRunMode.getSelectionIndex() == 2) {
      action.setRunMode(ActionDatabricksJobRun.MODE_DEPLOY_AND_RUN);
    } else if (wRunMode.getSelectionIndex() == 1) {
      action.setRunMode(ActionDatabricksJobRun.MODE_SUBMIT_ONCE);
    } else {
      action.setRunMode(ActionDatabricksJobRun.MODE_RUN_EXISTING);
    }
    action.setJobId(wJobId.getText());
    action.setSubmitRunJson(wSubmitJson.getText());
    action.setFatJarPath(wFatJar.getText());
    action.setPipelineFilename(wPipeline.getText());
    action.setRunConfigurationName(wRunConfig.getText());
    action.setDbfsBasePath(wDbfsBase.getText());
    action.setUploadProjectPackage(wUploadProjectPackage.getSelection());
    action.setProjectHome(wProjectHome.getText());
    action.setProjectPackageFile(wProjectPackageFile.getText());
    action.setEnvironmentConfigFile(wEnvironmentConfigFile.getText());
    action.setExistingClusterId(wClusterId.getText());
    action.setNewClusterSparkVersion(wNewClusterSparkVersion.getText());
    action.setNewClusterNodeTypeId(wNewClusterNodeType.getText());
    action.setNewClusterNumWorkers(wNewClusterNumWorkers.getText());
    action.setNewClusterJson(wNewClusterJson.getText());
    action.setEnvironmentKey(wEnvironmentKey.getText());
    action.setEnvironmentClient(wEnvironmentClient.getText());
    action.setJobName(wJobName.getText());
    action.setUpdateExistingJob(wUpdateJob.getSelection());
    action.setWaitMode(
        wWaitMode.getSelectionIndex() == 1
            ? ActionDatabricksJobRun.WAIT_FIRE_AND_FORGET
            : ActionDatabricksJobRun.WAIT_WAIT);
    action.setTimeoutSeconds(wTimeout.getText());
    action.setPollIntervalSeconds(wPoll.getText());
    action.setResultVariableJobId(wVarJobId.getText());
    action.setResultVariableRunId(wVarRunId.getText());
    action.setResultVariableStatus(wVarStatus.getText());
    action.setResultVariablePageUrl(wVarPageUrl.getText());
    action.setResultVariableError(wVarError.getText());
    dispose();
  }
}
