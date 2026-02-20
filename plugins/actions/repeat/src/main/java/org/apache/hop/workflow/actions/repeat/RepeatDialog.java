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

package org.apache.hop.workflow.actions.repeat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class RepeatDialog extends ActionDialog {

  private static final Class<?> PKG = RepeatDialog.class;

  private static final String COLON_SEPARATOR = " : ";

  private Repeat action;

  private TextVar wFilename;
  private TextVar wVariableName;
  private TextVar wVariableValue;
  private TextVar wDelay;
  private Button wKeepValues;
  private TableView wParameters;

  private Button wLogFileEnabled;
  private Label wlLogFileBase;
  private TextVar wLogFileBase;
  private Label wlLogFileExtension;
  private TextVar wLogFileExtension;
  private Label wlLogFileDateAdded;
  private Button wLogFileDateAdded;
  private Label wlLogFileTimeAdded;
  private Button wLogFileTimeAdded;
  private Label wlLogFileRepetitionAdded;
  private Button wLogFileRepetitionAdded;
  private Label wlLogFileAppended;
  private Button wLogFileAppended;
  private Label wlLogFileUpdateInterval;
  private TextVar wLogFileUpdateInterval;
  private ComboVar wRunConfiguration;

  public RepeatDialog(
      Shell parent, Repeat action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;

    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "Repeat.Name"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "Repeat.Name"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    Control lastControl = wSpacer;

    Label wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "Repeat.FileToRepeat.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    fdlFilename.top = new FormAttachment(lastControl, margin);
    wlFilename.setLayoutData(fdlFilename);

    // The filename browse button
    //
    // Browse for a file
    Button wbbFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    fdbFilename.right = new FormAttachment(100, 0);
    wbbFilename.setLayoutData(fdbFilename);
    wbbFilename.addListener(SWT.Selection, e -> browseForFile());

    wFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbbFilename, -margin);
    fdFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wFilename.setLayoutData(fdFilename);
    lastControl = wFilename;

    Label wlRunConfiguration = new Label(shell, SWT.RIGHT);
    wlRunConfiguration.setText(BaseMessages.getString(PKG, "Repeat.RunConfiguration.Label"));
    PropsUi.setLook(wlRunConfiguration);
    FormData fdlRunConfiguration = new FormData();
    fdlRunConfiguration.left = new FormAttachment(0, 0);
    fdlRunConfiguration.top = new FormAttachment(lastControl, margin);
    fdlRunConfiguration.right = new FormAttachment(middle, -margin);
    wlRunConfiguration.setLayoutData(fdlRunConfiguration);

    wRunConfiguration = new ComboVar(variables, shell, SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRunConfiguration);
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.left = new FormAttachment(middle, 0);
    fdRunConfiguration.top = new FormAttachment(wlRunConfiguration, 0, SWT.CENTER);
    fdRunConfiguration.right = new FormAttachment(100, 0);
    wRunConfiguration.setLayoutData(fdRunConfiguration);
    lastControl = wRunConfiguration;

    Label wlVariableName = new Label(shell, SWT.RIGHT);
    wlVariableName.setText(BaseMessages.getString(PKG, "Repeat.StopRepeatingVar.Label"));
    PropsUi.setLook(wlVariableName);
    FormData fdlVariableName = new FormData();
    fdlVariableName.left = new FormAttachment(0, 0);
    fdlVariableName.right = new FormAttachment(middle, -margin);
    fdlVariableName.top = new FormAttachment(lastControl, margin);
    wlVariableName.setLayoutData(fdlVariableName);
    wVariableName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wVariableName);
    FormData fdVariableName = new FormData();
    fdVariableName.left = new FormAttachment(middle, 0);
    fdVariableName.right = new FormAttachment(100, 0);
    fdVariableName.top = new FormAttachment(wlVariableName, 0, SWT.CENTER);
    wVariableName.setLayoutData(fdVariableName);
    lastControl = wVariableName;

    Label wlVariableValue = new Label(shell, SWT.RIGHT);
    wlVariableValue.setText(BaseMessages.getString(PKG, "Repeat.OptionalVarValue.Label"));
    PropsUi.setLook(wlVariableValue);
    FormData fdlVariableValue = new FormData();
    fdlVariableValue.left = new FormAttachment(0, 0);
    fdlVariableValue.right = new FormAttachment(middle, -margin);
    fdlVariableValue.top = new FormAttachment(lastControl, margin);
    wlVariableValue.setLayoutData(fdlVariableValue);
    wVariableValue = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wVariableValue);
    FormData fdVariableValue = new FormData();
    fdVariableValue.left = new FormAttachment(middle, 0);
    fdVariableValue.right = new FormAttachment(100, 0);
    fdVariableValue.top = new FormAttachment(wlVariableValue, 0, SWT.CENTER);
    wVariableValue.setLayoutData(fdVariableValue);
    lastControl = wVariableValue;

    Label wlDelay = new Label(shell, SWT.RIGHT);
    wlDelay.setText(BaseMessages.getString(PKG, "Repeat.Delay.Label"));
    PropsUi.setLook(wlDelay);
    FormData fdlDelay = new FormData();
    fdlDelay.left = new FormAttachment(0, 0);
    fdlDelay.right = new FormAttachment(middle, -margin);
    fdlDelay.top = new FormAttachment(lastControl, margin);
    wlDelay.setLayoutData(fdlDelay);
    wDelay = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDelay);
    FormData fdDelay = new FormData();
    fdDelay.left = new FormAttachment(middle, 0);
    fdDelay.right = new FormAttachment(100, 0);
    fdDelay.top = new FormAttachment(wlDelay, 0, SWT.CENTER);
    wDelay.setLayoutData(fdDelay);
    lastControl = wDelay;

    Label wlKeepValues = new Label(shell, SWT.RIGHT);
    wlKeepValues.setText(BaseMessages.getString(PKG, "Repeat.KeepVariableValues.Label"));
    PropsUi.setLook(wlKeepValues);
    FormData fdlKeepValues = new FormData();
    fdlKeepValues.left = new FormAttachment(0, 0);
    fdlKeepValues.right = new FormAttachment(middle, -margin);
    fdlKeepValues.top = new FormAttachment(lastControl, margin);
    wlKeepValues.setLayoutData(fdlKeepValues);
    wKeepValues = new Button(shell, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wKeepValues);
    FormData fdKeepValues = new FormData();
    fdKeepValues.left = new FormAttachment(middle, 0);
    fdKeepValues.right = new FormAttachment(100, 0);
    fdKeepValues.top = new FormAttachment(wlKeepValues, 0, SWT.CENTER);
    wKeepValues.setLayoutData(fdKeepValues);
    lastControl = wlKeepValues;

    Group wLogFileGroup = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wLogFileGroup);
    wLogFileGroup.setText(BaseMessages.getString(PKG, "Repeat.LoggingFileGroup.Label"));
    FormLayout logFileGroupLayout = new FormLayout();
    logFileGroupLayout.marginLeft = PropsUi.getMargin();
    logFileGroupLayout.marginRight = PropsUi.getMargin();
    logFileGroupLayout.marginTop = 2 * PropsUi.getMargin();
    logFileGroupLayout.marginBottom = 2 * PropsUi.getMargin();
    wLogFileGroup.setLayout(logFileGroupLayout);

    Label wlLogFileEnabled = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileEnabled.setText(BaseMessages.getString(PKG, "Repeat.LogToFile.Label"));
    PropsUi.setLook(wlLogFileEnabled);
    FormData fdlLogFileEnabled = new FormData();
    fdlLogFileEnabled.left = new FormAttachment(0, 0);
    fdlLogFileEnabled.right = new FormAttachment(middle, -margin);
    fdlLogFileEnabled.top = new FormAttachment(0, 0);
    wlLogFileEnabled.setLayoutData(fdlLogFileEnabled);
    wLogFileEnabled = new Button(wLogFileGroup, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wLogFileEnabled);
    FormData fdLogFileEnabled = new FormData();
    fdLogFileEnabled.left = new FormAttachment(middle, 0);
    fdLogFileEnabled.right = new FormAttachment(100, 0);
    fdLogFileEnabled.top = new FormAttachment(wlLogFileEnabled, 0, SWT.CENTER);
    wLogFileEnabled.setLayoutData(fdLogFileEnabled);
    wLogFileEnabled.addListener(SWT.Selection, e -> enableControls());
    Control lastLogControl = wlLogFileEnabled;

    wlLogFileBase = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileBase.setText(BaseMessages.getString(PKG, "Repeat.BaseLogFilename.Label"));
    PropsUi.setLook(wlLogFileBase);
    FormData fdlLogFileBase = new FormData();
    fdlLogFileBase.left = new FormAttachment(0, 0);
    fdlLogFileBase.right = new FormAttachment(middle, -margin);
    fdlLogFileBase.top = new FormAttachment(lastLogControl, margin);
    wlLogFileBase.setLayoutData(fdlLogFileBase);
    wLogFileBase = new TextVar(variables, wLogFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLogFileBase);
    FormData fdLogFileBase = new FormData();
    fdLogFileBase.left = new FormAttachment(middle, 0);
    fdLogFileBase.right = new FormAttachment(100, 0);
    fdLogFileBase.top = new FormAttachment(wlLogFileBase, 0, SWT.CENTER);
    wLogFileBase.setLayoutData(fdLogFileBase);
    lastLogControl = wLogFileBase;

    wlLogFileExtension = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileExtension.setText(BaseMessages.getString(PKG, "Repeat.LogFilenameExt.Label"));
    PropsUi.setLook(wlLogFileExtension);
    FormData fdlLogFileExtension = new FormData();
    fdlLogFileExtension.left = new FormAttachment(0, 0);
    fdlLogFileExtension.right = new FormAttachment(middle, -margin);
    fdlLogFileExtension.top = new FormAttachment(lastLogControl, margin);
    wlLogFileExtension.setLayoutData(fdlLogFileExtension);
    wLogFileExtension = new TextVar(variables, wLogFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLogFileExtension);
    FormData fdLogFileExtension = new FormData();
    fdLogFileExtension.left = new FormAttachment(middle, 0);
    fdLogFileExtension.right = new FormAttachment(100, 0);
    fdLogFileExtension.top = new FormAttachment(wlLogFileExtension, 0, SWT.CENTER);
    wLogFileExtension.setLayoutData(fdLogFileExtension);
    lastLogControl = wLogFileExtension;

    wlLogFileDateAdded = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileDateAdded.setText(BaseMessages.getString(PKG, "Repeat.AddDateToFilename.Label"));
    PropsUi.setLook(wlLogFileDateAdded);
    FormData fdlLogFileDateAdded = new FormData();
    fdlLogFileDateAdded.left = new FormAttachment(0, 0);
    fdlLogFileDateAdded.right = new FormAttachment(middle, -margin);
    fdlLogFileDateAdded.top = new FormAttachment(lastLogControl, margin);
    wlLogFileDateAdded.setLayoutData(fdlLogFileDateAdded);
    wLogFileDateAdded = new Button(wLogFileGroup, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wLogFileDateAdded);
    FormData fdLogFileDateAdded = new FormData();
    fdLogFileDateAdded.left = new FormAttachment(middle, 0);
    fdLogFileDateAdded.right = new FormAttachment(100, 0);
    fdLogFileDateAdded.top = new FormAttachment(wlLogFileDateAdded, 0, SWT.CENTER);
    wLogFileDateAdded.setLayoutData(fdLogFileDateAdded);
    lastLogControl = wlLogFileDateAdded;

    wlLogFileTimeAdded = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileTimeAdded.setText(BaseMessages.getString(PKG, "Repeat.AddTimeToFilename.Label"));
    PropsUi.setLook(wlLogFileTimeAdded);
    FormData fdlLogFileTimeAdded = new FormData();
    fdlLogFileTimeAdded.left = new FormAttachment(0, 0);
    fdlLogFileTimeAdded.right = new FormAttachment(middle, -margin);
    fdlLogFileTimeAdded.top = new FormAttachment(lastLogControl, margin);
    wlLogFileTimeAdded.setLayoutData(fdlLogFileTimeAdded);
    wLogFileTimeAdded = new Button(wLogFileGroup, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wLogFileTimeAdded);
    FormData fdLogFileTimeAdded = new FormData();
    fdLogFileTimeAdded.left = new FormAttachment(middle, 0);
    fdLogFileTimeAdded.right = new FormAttachment(100, 0);
    fdLogFileTimeAdded.top = new FormAttachment(wlLogFileTimeAdded, 0, SWT.CENTER);
    wLogFileTimeAdded.setLayoutData(fdLogFileTimeAdded);
    lastLogControl = wlLogFileTimeAdded;

    wlLogFileRepetitionAdded = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileRepetitionAdded.setText(
        BaseMessages.getString(PKG, "Repeat.AddReptNumToFilename.Label"));
    PropsUi.setLook(wlLogFileRepetitionAdded);
    FormData fdlLogFileRepetitionAdded = new FormData();
    fdlLogFileRepetitionAdded.left = new FormAttachment(0, 0);
    fdlLogFileRepetitionAdded.right = new FormAttachment(middle, -margin);
    fdlLogFileRepetitionAdded.top = new FormAttachment(lastLogControl, margin);
    wlLogFileRepetitionAdded.setLayoutData(fdlLogFileRepetitionAdded);
    wLogFileRepetitionAdded = new Button(wLogFileGroup, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wLogFileRepetitionAdded);
    FormData fdLogFileRepetitionAdded = new FormData();
    fdLogFileRepetitionAdded.left = new FormAttachment(middle, 0);
    fdLogFileRepetitionAdded.right = new FormAttachment(100, 0);
    fdLogFileRepetitionAdded.top = new FormAttachment(wlLogFileRepetitionAdded, 0, SWT.CENTER);
    wLogFileRepetitionAdded.setLayoutData(fdLogFileRepetitionAdded);
    lastLogControl = wlLogFileRepetitionAdded;

    wlLogFileAppended = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileAppended.setText(BaseMessages.getString(PKG, "Repeat.AppendToExistingFile.Label"));
    PropsUi.setLook(wlLogFileAppended);
    FormData fdlLogFileAppended = new FormData();
    fdlLogFileAppended.left = new FormAttachment(0, 0);
    fdlLogFileAppended.right = new FormAttachment(middle, -margin);
    fdlLogFileAppended.top = new FormAttachment(lastLogControl, margin);
    wlLogFileAppended.setLayoutData(fdlLogFileAppended);
    wLogFileAppended = new Button(wLogFileGroup, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wLogFileAppended);
    FormData fdLogFileAppended = new FormData();
    fdLogFileAppended.left = new FormAttachment(middle, 0);
    fdLogFileAppended.right = new FormAttachment(100, 0);
    fdLogFileAppended.top = new FormAttachment(wlLogFileAppended, 0, SWT.CENTER);
    wLogFileAppended.setLayoutData(fdLogFileAppended);
    lastLogControl = wlLogFileAppended;

    wlLogFileUpdateInterval = new Label(wLogFileGroup, SWT.RIGHT);
    wlLogFileUpdateInterval.setText(
        BaseMessages.getString(PKG, "Repeat.LogFileUpdateInterval.Label"));
    PropsUi.setLook(wlLogFileUpdateInterval);
    FormData fdlLogFileUpdateInterval = new FormData();
    fdlLogFileUpdateInterval.left = new FormAttachment(0, 0);
    fdlLogFileUpdateInterval.right = new FormAttachment(middle, -margin);
    fdlLogFileUpdateInterval.top = new FormAttachment(lastLogControl, margin);
    wlLogFileUpdateInterval.setLayoutData(fdlLogFileUpdateInterval);
    wLogFileUpdateInterval =
        new TextVar(variables, wLogFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLogFileUpdateInterval);
    FormData fdLogFileUpdateInterval = new FormData();
    fdLogFileUpdateInterval.left = new FormAttachment(middle, 0);
    fdLogFileUpdateInterval.right = new FormAttachment(100, 0);
    fdLogFileUpdateInterval.top = new FormAttachment(wlLogFileUpdateInterval, 0, SWT.CENTER);
    wLogFileUpdateInterval.setLayoutData(fdLogFileUpdateInterval);

    FormData fdLogFileGroup = new FormData();
    fdLogFileGroup.left = new FormAttachment(0, margin);
    fdLogFileGroup.right = new FormAttachment(100, -margin);
    fdLogFileGroup.top = new FormAttachment(lastControl, margin);
    wLogFileGroup.setLayoutData(fdLogFileGroup);
    wLogFileGroup.pack();
    lastControl = wLogFileGroup;

    // Parameters
    //
    Label wlParameters = new Label(shell, SWT.LEFT);
    wlParameters.setText(BaseMessages.getString(PKG, "Repeat.ParmsVarGroup.Label"));
    PropsUi.setLook(wlParameters);
    FormData fdlParameters = new FormData();
    fdlParameters.left = new FormAttachment(0, margin);
    fdlParameters.top = new FormAttachment(lastControl, margin);
    fdlParameters.right = new FormAttachment(100, 0);
    wlParameters.setLayoutData(fdlParameters);
    lastControl = wlParameters;

    ColumnInfo[] columnInfos =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "Repeat.ParmsVarGroup.Name.Column.Header"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Repeat.ParmsVarGroup.Value.Column.Header"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    columnInfos[1].setUsingVariables(true);

    wParameters =
        new TableView(
            variables, shell, SWT.BORDER, columnInfos, action.getParameters().size(), null, props);
    PropsUi.setLook(wParameters);
    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment(0, margin);
    fdParameters.right = new FormAttachment(100, -margin);
    fdParameters.top = new FormAttachment(lastControl, margin);
    fdParameters.bottom = new FormAttachment(wCancel, -margin);
    wParameters.setLayoutData(fdParameters);

    getData();
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void browseForFile() {
    HopPipelineFileType pipelineFileType = new HopPipelineFileType<>();
    HopWorkflowFileType workflowFileType = new HopWorkflowFileType<>();

    List<String> filterExtensions = new ArrayList<>();
    filterExtensions.add(
        pipelineFileType.getFilterExtensions()[0]
            + ";"
            + workflowFileType.getFilterExtensions()[0]);
    filterExtensions.addAll(Arrays.asList(pipelineFileType.getFilterExtensions()));
    filterExtensions.addAll(Arrays.asList(workflowFileType.getFilterExtensions()));
    filterExtensions.add("*.*");

    List<String> filterNames = new ArrayList<>();
    filterNames.add(
        pipelineFileType.getFilterNames()[0] + " and " + workflowFileType.getFilterNames()[0]);
    filterNames.addAll(Arrays.asList(pipelineFileType.getFilterNames()));
    filterNames.addAll(Arrays.asList(workflowFileType.getFilterNames()));
    filterNames.add(BaseMessages.getString(PKG, "System.FileType.AllFiles"));

    BaseDialog.presentFileDialog(
        shell,
        wFilename,
        variables,
        filterExtensions.toArray(new String[0]),
        filterNames.toArray(new String[0]),
        false);
  }

  private void enableControls() {
    boolean logEnabled = wLogFileEnabled.getSelection();

    wlLogFileBase.setEnabled(logEnabled);
    wLogFileBase.setEnabled(logEnabled);
    wlLogFileExtension.setEnabled(logEnabled);
    wLogFileExtension.setEnabled(logEnabled);
    wlLogFileDateAdded.setEnabled(logEnabled);
    wLogFileDateAdded.setEnabled(logEnabled);
    wlLogFileTimeAdded.setEnabled(logEnabled);
    wLogFileTimeAdded.setEnabled(logEnabled);
    wlLogFileRepetitionAdded.setEnabled(logEnabled);
    wLogFileRepetitionAdded.setEnabled(logEnabled);
    wlLogFileAppended.setEnabled(logEnabled);
    wLogFileAppended.setEnabled(logEnabled);
    wlLogFileUpdateInterval.setEnabled(logEnabled);
    wLogFileUpdateInterval.setEnabled(logEnabled);
  }

  private void cancel() {
    action = null;
    dispose();
  }

  private void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wFilename.setText(Const.NVL(action.getFilename(), ""));
    wVariableName.setText(Const.NVL(action.getVariableName(), ""));
    wVariableValue.setText(Const.NVL(action.getVariableValue(), ""));
    wDelay.setText(Const.NVL(action.getDelay(), ""));
    wKeepValues.setSelection(action.isKeepingValues());

    wLogFileEnabled.setSelection(action.isLogFileEnabled());
    wLogFileBase.setText(Const.NVL(action.getLogFileBase(), ""));
    wLogFileExtension.setText(Const.NVL(action.getLogFileExtension(), ""));
    wLogFileDateAdded.setSelection(action.isLogFileDateAdded());
    wLogFileTimeAdded.setSelection(action.isLogFileTimeAdded());
    wLogFileRepetitionAdded.setSelection(action.isLogFileRepetitionAdded());
    wLogFileAppended.setSelection(action.isLogFileAppended());
    wLogFileUpdateInterval.setText(Const.NVL(action.getLogFileUpdateInterval(), "5000"));

    int rowNr = 0;
    for (ParameterDetails parameter : action.getParameters()) {
      TableItem item = wParameters.table.getItem(rowNr++);
      item.setText(1, Const.NVL(parameter.getName(), ""));
      item.setText(2, Const.NVL(parameter.getField(), ""));
    }
    wParameters.setRowNums();
    wParameters.optWidth(true);

    // Get the run configurations for both pipelines and workflows
    //
    MetadataManager<PipelineRunConfiguration> prcManager =
        new MetadataManager<>(
            variables, getMetadataProvider(), PipelineRunConfiguration.class, shell);
    MetadataManager<WorkflowRunConfiguration> wrcManager =
        new MetadataManager<>(
            variables, getMetadataProvider(), WorkflowRunConfiguration.class, shell);
    List<String> entries = new ArrayList<>();
    try {
      prcManager.getNames().forEach(name -> entries.add("Pipeline" + COLON_SEPARATOR + name));
    } catch (Exception e) {
      // Ignore
    }
    try {
      wrcManager.getNames().forEach(name -> entries.add("Workflow" + COLON_SEPARATOR + name));
    } catch (Exception e) {
      // Ignore
    }
    wRunConfiguration.setItems(entries.toArray(new String[0]));
    if (StringUtils.isNotEmpty(action.getRunConfigurationName())) {
      String realFilename = variables.resolve(wFilename.getText());
      try {
        if (this.action.isPipeline(realFilename)) {
          wRunConfiguration.setText(
              "Pipeline" + COLON_SEPARATOR + action.getRunConfigurationName());
        } else if (this.action.isWorkflow(realFilename)) {
          wRunConfiguration.setText(
              "Workflow" + COLON_SEPARATOR + action.getRunConfigurationName());
        }
      } catch (Exception e) {
        // Ignore
      }
    }

    enableControls();
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "Repeat.Dialog.ActionMissing.Header"));
      mb.setMessage(BaseMessages.getString(PKG, "Repeat.Dialog.ActionMissing.Message"));
      mb.open();
      return;
    }
    action.setName(wName.getText());

    if (Utils.isEmpty(wFilename.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "Repeat.Dialog.FilenameMissing.Header"));
      mb.setMessage(BaseMessages.getString(PKG, "Repeat.Dialog.FilenameMissing.Message"));
      mb.open();
      return;
    }
    if (isSelfReferencing()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "Repeat.Dialog.SelfReference.Header"));
      mb.setMessage(BaseMessages.getString(PKG, "Repeat.Dialog.SelfReference.Message"));
      mb.open();
      return;
    }
    action.setFilename(wFilename.getText());
    action.setVariableName(wVariableName.getText());
    action.setVariableValue(wVariableValue.getText());
    action.setDelay(wDelay.getText());
    action.setKeepingValues(wKeepValues.getSelection());

    action.setLogFileEnabled(wLogFileEnabled.getSelection());
    action.setLogFileAppended(wLogFileAppended.getSelection());
    action.setLogFileBase(wLogFileBase.getText());
    action.setLogFileExtension(wLogFileExtension.getText());
    action.setLogFileDateAdded(wLogFileDateAdded.getSelection());
    action.setLogFileTimeAdded(wLogFileTimeAdded.getSelection());
    action.setLogFileRepetitionAdded(wLogFileRepetitionAdded.getSelection());
    action.setLogFileUpdateInterval(wLogFileUpdateInterval.getText());

    action.getParameters().clear();
    for (int i = 0; i < wParameters.nrNonEmpty(); i++) {
      TableItem item = wParameters.getNonEmpty(i);
      action.getParameters().add(new ParameterDetails(item.getText(1), item.getText(2)));
    }

    // Get the name of the run configuration:
    //
    String runConfigRaw = wRunConfiguration.getText();
    if (StringUtils.isEmpty(runConfigRaw)) {
      action.setRunConfigurationName(null);
    } else {
      int colonIndex = runConfigRaw.indexOf(COLON_SEPARATOR);
      if (colonIndex > 0 && colonIndex + COLON_SEPARATOR.length() < runConfigRaw.length()) {
        action.setRunConfigurationName(
            runConfigRaw.substring(colonIndex + COLON_SEPARATOR.length()));
      } else {
        action.setRunConfigurationName(runConfigRaw);
      }
    }

    action.setChanged();

    dispose();
  }

  private boolean isSelfReferencing() {
    return variables
        .resolve(wFilename.getText())
        .equals(variables.resolve(workflowMeta.getFilename()));
  }
}
