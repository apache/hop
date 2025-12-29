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

package org.apache.hop.ui.workflow.dialog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ConfigurationDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.local.LocalWorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class WorkflowExecutionConfigurationDialog extends ConfigurationDialog {
  private static final Class<?> PKG = WorkflowExecutionConfigurationDialog.class;

  public static final String AUDIT_LIST_TYPE_LAST_USED_RUN_CONFIGURATIONS =
      "last-workflow-run-configurations";
  public static final String MAP_TYPE_WORKFLOW_RUN_CONFIG_USAGE =
      "workflow-run-configuration-usage";

  private CCombo wStartAction;
  private MetaSelectionLine<WorkflowRunConfiguration> wRunConfiguration;

  public WorkflowExecutionConfigurationDialog(
      Shell parent, WorkflowExecutionConfiguration configuration, WorkflowMeta workflowMeta) {
    super(parent, configuration, workflowMeta);
  }

  @Override
  protected void optionsSectionControls() {

    wlLogLevel = new Label(gDetails, SWT.RIGHT);
    wlLogLevel.setText(
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.LogLevel.Label"));
    wlLogLevel.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.LogLevel.Tooltip"));
    PropsUi.setLook(wlLogLevel);
    FormData fdlLogLevel = new FormData();
    fdlLogLevel.top = new FormAttachment(0, 0);
    fdlLogLevel.left = new FormAttachment(0, 0);
    wlLogLevel.setLayoutData(fdlLogLevel);

    wLogLevel = new CCombo(gDetails, SWT.READ_ONLY | SWT.BORDER);
    wLogLevel.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.LogLevel.Tooltip"));
    PropsUi.setLook(wLogLevel);
    FormData fdLogLevel = new FormData();
    fdLogLevel.top = new FormAttachment(wlLogLevel, -2, SWT.TOP);
    fdLogLevel.right = new FormAttachment(100, 0);
    fdLogLevel.left = new FormAttachment(wlLogLevel, 6);
    wLogLevel.setLayoutData(fdLogLevel);
    wLogLevel.setItems(LogLevel.getLogLevelDescriptions());

    wClearLog = new Button(gDetails, SWT.CHECK);
    wClearLog.setText(
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.ClearLog.Label"));
    wClearLog.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.ClearLog.Tooltip"));
    PropsUi.setLook(wClearLog);
    FormData fdClearLog = new FormData();
    fdClearLog.top = new FormAttachment(wLogLevel, 10);
    fdClearLog.left = new FormAttachment(0, 0);
    wClearLog.setLayoutData(fdClearLog);

    Label wlStartAction = new Label(gDetails, SWT.RIGHT);
    wlStartAction.setText(
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.StartCopy.Label"));
    wlStartAction.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.StartCopy.Tooltip"));
    PropsUi.setLook(wlStartAction);
    FormData fdlStartAction = new FormData();
    fdlStartAction.top = new FormAttachment(wClearLog, 10);
    fdlStartAction.left = new FormAttachment(0, 0);
    wlStartAction.setLayoutData(fdlStartAction);

    wStartAction = new CCombo(gDetails, SWT.READ_ONLY | SWT.BORDER);
    wStartAction.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.StartCopy.Tooltip"));
    PropsUi.setLook(wStartAction);
    FormData fdStartActionAction = new FormData();
    fdStartActionAction.top = new FormAttachment(wlStartAction, 0, SWT.CENTER);
    fdStartActionAction.left = new FormAttachment(wlStartAction, PropsUi.getMargin());
    fdStartActionAction.right = new FormAttachment(100, 0);
    wStartAction.setLayoutData(fdStartActionAction);

    WorkflowMeta workflowMeta = (WorkflowMeta) super.abstractMeta;

    String[] names = new String[workflowMeta.getActions().size()];
    for (int i = 0; i < names.length; i++) {
      ActionMeta actionMeta = workflowMeta.getActions().get(i);
      names[i] = actionMeta.getName();
    }
    wStartAction.setItems(names);
  }

  public boolean open() {

    String shellTitle =
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.Shell.Title");
    mainLayout(shellTitle, GuiResource.getInstance().getImageWorkflow());

    addRunConfigurationSectionLayout();

    String alwaysShowOptionLabel =
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.AlwaysOption.Value");
    String alwaysShowOptionTooltip =
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.alwaysShowOption");
    String docUrl =
        Const.getDocUrl(
            BaseMessages.getString(
                HopGui.class, "HopGui.WorkflowExecutionConfigurationDialog.Help"));
    String docTitle = BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.docTitle");
    String docHeader =
        BaseMessages.getString(PKG, "WorkflowExecutionConfigurationDialog.docHeader");
    buttonsSectionLayout(
        alwaysShowOptionLabel, alwaysShowOptionTooltip, docTitle, docUrl, docHeader);

    optionsSectionLayout(PKG, "WorkflowExecutionConfigurationDialog");
    parametersSectionLayout(PKG, "WorkflowExecutionConfigurationDialog");

    getData();
    openDialog();
    return retval;
  }

  private void addRunConfigurationSectionLayout() {
    String runConfigLabel =
        BaseMessages.getString(PKG, "ConfigurationDialog.RunConfiguration.Label");
    String runConfigTooltip =
        BaseMessages.getString(PKG, "ConfigurationDialog.RunConfiguration.Tooltip");

    wRunConfiguration =
        new MetaSelectionLine<>(
            hopGui.getVariables(),
            hopGui.getMetadataProvider(),
            WorkflowRunConfiguration.class,
            shell,
            SWT.BORDER,
            runConfigLabel,
            runConfigTooltip,
            true);
    wRunConfigurationControl = wRunConfiguration;
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.right = new FormAttachment(100, 0);
    fdRunConfiguration.top = new FormAttachment(0, PropsUi.getMargin());
    fdRunConfiguration.left = new FormAttachment(0, 0);
    wRunConfiguration.setLayoutData(fdRunConfiguration);
  }

  private void getVariablesData() {
    wVariables.clearAll(false);
    List<String> variableNames = new ArrayList<>(configuration.getVariablesMap().keySet());
    Collections.sort(variableNames);

    List<String> paramNames = new ArrayList<>(configuration.getParametersMap().keySet());

    for (String variableName : variableNames) {
      String variableValue = configuration.getVariablesMap().get(variableName);

      if (!paramNames.contains(variableName)) {
        //
        // Do not put the parameters among the variables.
        //
        TableItem tableItem = new TableItem(wVariables.table, SWT.NONE);
        tableItem.setText(1, variableName);
        tableItem.setText(2, Const.NVL(variableValue, ""));
      }
    }
    wVariables.removeEmptyRows();
    wVariables.setRowNums();
    wVariables.optWidth(true);
  }

  public void getData() {
    wClearLog.setSelection(configuration.isClearingLog());

    try {
      wRunConfiguration.fillItems();
      if (Const.indexOfString(configuration.getRunConfiguration(), wRunConfiguration.getItems())
          < 0) {
        getConfiguration().setRunConfiguration(null);
      }
    } catch (Exception e) {
      hopGui.getLog().logError("Unable to obtain a list of workflow run configurations", e);
    }

    Map<String, String> workflowUsageMap = null;
    String lastGlobalRunConfig =
        AuditManagerGuiUtil.getLastUsedValue(AUDIT_LIST_TYPE_LAST_USED_RUN_CONFIGURATIONS);
    String selectedRunConfig = null;
    if (StringUtils.isNotEmpty(abstractMeta.getName())) {
      workflowUsageMap = AuditManagerGuiUtil.getUsageMap(MAP_TYPE_WORKFLOW_RUN_CONFIG_USAGE);
      selectedRunConfig = workflowUsageMap.get(abstractMeta.getName());
    }

    if (StringUtils.isEmpty(selectedRunConfig)) {
      // What is the default?
      WorkflowRunConfiguration defaultRunConfig = null;
      try {
        defaultRunConfig = WorkflowRunConfiguration.findDefault(hopGui.getMetadataProvider());
      } catch (HopException e) {
        LogChannel.UI.logError("Error finding default workflow run configuration", e);
      }
      if (defaultRunConfig != null) {
        selectedRunConfig = defaultRunConfig.getName();
      }
    }

    wRunConfiguration.setText(Const.NVL(selectedRunConfig, ""));

    if (StringUtils.isNotEmpty(selectedRunConfig)
        && StringUtils.isNotEmpty(lastGlobalRunConfig)
        && !selectedRunConfig.equals(lastGlobalRunConfig)) {
      wRunConfiguration
          .getLabelWidget()
          .setBackground(GuiResource.getInstance().getColorLightBlue());
      wRunConfiguration
          .getLabelWidget()
          .setToolTipText(
              BaseMessages.getString(
                  PKG, "WorkflowExecutionConfigurationDialog.VerifyRunConfigurationName.Warning"));
      wRunConfiguration
          .getComboWidget()
          .setBackground(GuiResource.getInstance().getColorLightBlue());
      wRunConfiguration
          .getComboWidget()
          .setToolTipText(
              BaseMessages.getString(
                  PKG, "WorkflowExecutionConfigurationDialog.VerifyRunConfigurationName.Warning"));
    }
    try {
      ExtensionPointHandler.callExtensionPoint(
          HopGui.getInstance().getLog(),
          hopGui.getVariables(),
          HopExtensionPoint.HopGuiRunConfiguration.id,
          wRunConfiguration);
    } catch (HopException e) {
      // Ignore errors
    }

    // If we don't have a run configuration from history or from a plugin,
    // set it from last execution or if there's only one, just pick that
    //
    if (StringUtil.isEmpty(wRunConfiguration.getText())) {
      if (StringUtils.isNotEmpty(getConfiguration().getRunConfiguration())) {
        wRunConfiguration.setText(getConfiguration().getRunConfiguration());
      } else if (wRunConfiguration.getItemCount() == 1) {
        wRunConfiguration.select(0);
      }
    }

    String startAction = "";
    if (!Utils.isEmpty(getConfiguration().getStartActionName())) {
      ActionMeta action =
          ((WorkflowMeta) abstractMeta).findAction(getConfiguration().getStartActionName());
      if (action != null) {
        startAction = action.getName();
      }
    }
    wStartAction.setText(startAction);

    if (workflowUsageMap != null && workflowUsageMap.containsKey(LOG_LEVEL)) {
      wLogLevel.select(Integer.parseInt(workflowUsageMap.get(LOG_LEVEL)));
    } else {
      wLogLevel.select(configuration.getLogLevel().getLevel());
    }

    getParamsData();
    getVariablesData();
  }

  @Override
  public boolean getInfo() {
    try {
      IHopMetadataSerializer<WorkflowRunConfiguration> serializer =
          hopGui.getMetadataProvider().getSerializer(WorkflowRunConfiguration.class);

      // See if there are any run configurations defined.  If not, ask about creating a local one.
      //
      if (serializer.listObjectNames().isEmpty()) {
        String name = createLocalWorkflowConfiguration(shell, serializer);
        wRunConfiguration.setText(name);
      }

      String runConfigurationName = wRunConfiguration.getText();
      if (StringUtils.isEmpty(runConfigurationName)) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        box.setText(
            BaseMessages.getString(
                PKG, "WorkflowExecutionConfigurationDialog.NoRunConfigurationSpecified.Title"));
        box.setMessage(
            BaseMessages.getString(
                PKG, "WorkflowExecutionConfigurationDialog.NoRunConfigurationSpecified.Message"));
        box.open();
        return false;
      }
      // See if the run configuration is available...
      //

      if (!serializer.exists(hopGui.getVariables().resolve(runConfigurationName))) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        box.setText(
            BaseMessages.getString(
                PKG, "WorkflowExecutionConfigurationDialog.RunConfigurationDoesNotExist.Title"));
        box.setMessage(
            BaseMessages.getString(
                PKG,
                "WorkflowExecutionConfigurationDialog.RunConfigurationDoesNotExist.Message",
                runConfigurationName));
        box.open();
        return false;
      }

      getConfiguration().setRunConfiguration(runConfigurationName);
      AuditManagerGuiUtil.addLastUsedValue(
          AUDIT_LIST_TYPE_LAST_USED_RUN_CONFIGURATIONS, runConfigurationName);
      if (StringUtils.isNotEmpty(abstractMeta.getName())) {
        Map<String, String> usageMap =
            AuditManagerGuiUtil.getUsageMap(MAP_TYPE_WORKFLOW_RUN_CONFIG_USAGE);
        usageMap.put(abstractMeta.getName(), runConfigurationName);
        usageMap.put("LOG_LEVEL", String.valueOf(wLogLevel.getSelectionIndex()));
        AuditManagerGuiUtil.saveUsageMap(MAP_TYPE_WORKFLOW_RUN_CONFIG_USAGE, usageMap);
      }

      // various settings
      //
      configuration.setClearingLog(wClearLog.getSelection());
      configuration.setLogLevel(LogLevel.values()[wLogLevel.getSelectionIndex()]);

      String startActionName = null;
      if (!Utils.isEmpty(wStartAction.getText()) && wStartAction.getSelectionIndex() >= 0) {

        ActionMeta action =
            ((WorkflowMeta) abstractMeta).getActions().get(wStartAction.getSelectionIndex());
        startActionName = action.getName();
      }
      getConfiguration().setStartActionName(startActionName);

      // The lower part of the dialog...
      getInfoParameters();
      getInfoVariables();

      return true;
    } catch (Exception e) {
      new ErrorDialog(shell, "Error in settings", "There is an error in the dialog settings", e);
    }
    return false;
  }

  public static final String createLocalWorkflowConfiguration(
      Shell shell, IHopMetadataSerializer<WorkflowRunConfiguration> prcSerializer) {
    try {
      MessageBox box =
          new MessageBox(HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      box.setText(
          BaseMessages.getString(
              PKG, "WorkflowExecutionConfigurationDialog.NoRunConfigurationDefined.Title"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "WorkflowExecutionConfigurationDialog.NoRunConfigurationDefined.Message"));
      int answer = box.open();
      if ((answer & SWT.YES) != 0) {
        LocalWorkflowRunConfiguration localWorkflowRunConfiguration =
            new LocalWorkflowRunConfiguration();
        localWorkflowRunConfiguration.setEnginePluginId("Local");
        WorkflowRunConfiguration local =
            new WorkflowRunConfiguration(
                "local",
                BaseMessages.getString(
                    PKG, "WorkflowExecutionConfigurationDialog.LocalRunConfiguration.Description"),
                null,
                localWorkflowRunConfiguration,
                true);
        prcSerializer.save(local);

        return local.getName();
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "WorkflowExecutionConfigurationDialog.ErrorSavingRunConfiguration.Title"),
          BaseMessages.getString(
              PKG, "WorkflowExecutionConfigurationDialog.ErrorSavingRunConfiguration.Message"),
          e);
    }
    return null;
  }

  /**
   * @return the configuration
   */
  public WorkflowExecutionConfiguration getConfiguration() {
    return (WorkflowExecutionConfiguration) configuration;
  }
}
