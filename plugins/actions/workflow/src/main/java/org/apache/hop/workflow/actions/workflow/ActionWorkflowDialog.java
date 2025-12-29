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

package org.apache.hop.workflow.actions.workflow;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.apache.hop.ui.workflow.actions.ActionBaseDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow.Parameter;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow.ParameterDefinition;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/** This dialog allows you to edit the workflow action (ActionWorkflow) */
public class ActionWorkflowDialog extends ActionBaseDialog {
  private static final Class<?> PKG = ActionWorkflow.class;

  private ActionWorkflow action;
  private MetaSelectionLine<WorkflowRunConfiguration> wRunConfiguration;

  private static final String[] FILE_FILTERLOGNAMES =
      new String[] {
        BaseMessages.getString(PKG, "ActionWorkflow.Fileformat.TXT"),
        BaseMessages.getString(PKG, "ActionWorkflow.Fileformat.LOG"),
        BaseMessages.getString(PKG, "ActionWorkflow.Fileformat.All")
      };

  public ActionWorkflowDialog(
      Shell parent, ActionWorkflow action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, action, workflowMeta, variables);
    this.action = action;
  }

  @Override
  public IAction open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    backupChanged = action.hasChanged();

    createElements();

    getData();
    setActive();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  @Override
  protected void createElements() {
    super.createElements();
    shell.setText(BaseMessages.getString(PKG, "ActionWorkflow.Header"));

    wlPath.setText(BaseMessages.getString(PKG, "ActionWorkflow.WorkflowFile.Label"));
    wPassParams.setText(BaseMessages.getString(PKG, "ActionWorkflow.PassAllParameters.Label"));

    wWaitingToFinish = new Button(gExecution, SWT.CHECK);
    PropsUi.setLook(wWaitingToFinish);
    wWaitingToFinish.setText(BaseMessages.getString(PKG, "ActionWorkflow.WaitToFinish.Label"));
    FormData fdWait = new FormData();
    fdWait.top = new FormAttachment(wEveryRow, 10);
    fdWait.left = new FormAttachment(0, 0);
    wWaitingToFinish.setLayoutData(fdWait);

    // force reload from file specification
    wbGetParams.addListener(SWT.Selection, e -> getParameters(null));

    wbBrowse.addListener(SWT.Selection, e -> pickFileVFS());

    wbLogFilename.addListener(SWT.Selection, e -> selectLogFile(FILE_FILTERLOGNAMES));
  }

  @Override
  protected ActionBase getAction() {
    return action;
  }

  @Override
  protected Image getImage() {
    return SwtSvgImageUtil.getImage(
        shell.getDisplay(),
        getClass().getClassLoader(),
        "ui/images/workflow.svg",
        ConstUi.LARGE_ICON_SIZE,
        ConstUi.LARGE_ICON_SIZE);
  }

  @Override
  protected int getParameterCount() {
    return action.getParameterDefinition().getParameters().size();
  }

  protected void getParameters(WorkflowMeta inputWorkflowMeta) {
    try {
      if (inputWorkflowMeta == null) {
        ActionWorkflow jej = new ActionWorkflow();
        getInfo(jej);
        inputWorkflowMeta = jej.getWorkflowMeta(this.getMetadataProvider(), variables);
      }
      String[] parameters = inputWorkflowMeta.listParameters();

      String[] existing = wParameters.getItems(1);

      for (String parameter : parameters) {
        if (Const.indexOfString(parameter, existing) < 0) {
          TableItem item = new TableItem(wParameters.table, SWT.NONE);
          item.setText(1, parameter);
        }
      }
      wParameters.removeEmptyRows();
      wParameters.setRowNums();
      wParameters.optWidth(true);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ActionWorkflowDialog.Exception.UnableToLoadWorkflow.Title"),
          BaseMessages.getString(
              PKG, "ActionWorkflowDialog.Exception.UnableToLoadWorkflow.Message"),
          e);
    }
  }

  @Override
  protected Control createRunConfigurationControl() {
    wRunConfiguration =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            WorkflowRunConfiguration.class,
            shell,
            SWT.BORDER,
            null,
            null,
            true);

    return wRunConfiguration;
  }

  protected void pickFileVFS() {
    HopWorkflowFileType<WorkflowMeta> workflowFileType = new HopWorkflowFileType<>();
    String filename =
        BaseDialog.presentFileDialog(
            shell,
            wPath,
            variables,
            workflowFileType.getFilterExtensions(),
            workflowFileType.getFilterNames(),
            false);
    if (filename != null) {
      replaceNameWithBaseFilename(filename);
    }
  }

  @Override
  public void setActive() {
    super.setActive();
  }

  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wPath.setText(Const.NVL(action.getFilename(), ""));

    // Parameters
    ParameterDefinition parameterDefinition = action.getParameterDefinition();
    if (action.getParameterDefinition() != null) {
      for (int i = 0; i < parameterDefinition.getParameters().size(); i++) {
        Parameter parameter = parameterDefinition.getParameters().get(i);
        TableItem item = wParameters.getTable().getItem(i);
        if (!Utils.isEmpty(parameter.getName())) {
          item.setText(1, Const.NVL(parameter.getName(), ""));
          item.setText(2, Const.NVL(parameter.getField(), ""));
          item.setText(3, Const.NVL(parameter.getValue(), ""));
        }
      }
      wParameters.setRowNums();
      wParameters.optWidth(true);
    }

    wPassParams.setSelection(parameterDefinition.isPassingAllParameters());

    wPrevToParams.setSelection(action.isParamsFromPrevious());
    wEveryRow.setSelection(action.isExecPerRow());
    wSetLogfile.setSelection(action.isSetLogfile());
    if (action.getLogfile() != null) {
      wLogfile.setText(action.getLogfile());
    }
    if (action.getLogext() != null) {
      wLogext.setText(action.getLogext());
    }
    wAddDate.setSelection(action.isAddDate());
    wAddTime.setSelection(action.isAddTime());

    if (action.getLogFileLevel() != null) {
      wLoglevel.select(action.getLogFileLevel().getLevel());
    } else {
      // Set the default log level
      wLoglevel.select(ActionWorkflow.DEFAULT_LOG_LEVEL.getLevel());
    }
    wAppendLogfile.setSelection(action.isSetAppendLogfile());
    wCreateParentFolder.setSelection(action.isCreateParentFolder());
    wWaitingToFinish.setSelection(action.isWaitingToFinish());

    try {
      List<String> runConfigurations =
          this.getMetadataProvider()
              .getSerializer(WorkflowRunConfiguration.class)
              .listObjectNames();

      try {
        ExtensionPointHandler.callExtensionPoint(
            HopGui.getInstance().getLog(),
            variables,
            HopExtensionPoint.HopGuiRunConfiguration.id,
            new Object[] {runConfigurations, WorkflowMeta.XML_TAG});
      } catch (HopException e) {
        // Ignore errors
      }

      wRunConfiguration.setItems(runConfigurations.toArray(new String[0]));
      if (Utils.isEmpty(action.getRunConfiguration())) {
        wRunConfiguration.select(0);
      } else {
        wRunConfiguration.setText(action.getRunConfiguration());
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting workflow run configurations", e);
    }

    setLogFileEnabled();

    wName.selectAll();
    wName.setFocus();
  }

  @Override
  protected void cancel() {
    action.setChanged(backupChanged);

    action = null;
    dispose();
  }

  @VisibleForTesting
  protected void getInfo(ActionWorkflow action) {
    action.setName(wName.getText());

    action.setFileName(wPath.getText());
    action.setRunConfiguration(wRunConfiguration.getText());

    ParameterDefinition parameterDefinition = action.getParameterDefinition();
    parameterDefinition.getParameters().clear();

    // Do the parameters
    int nrItems = wParameters.nrNonEmpty();
    for (int i = 0; i < nrItems; i++) {
      TableItem item = wParameters.getNonEmpty(i);

      Parameter parameter = new Parameter();
      parameter.setName(item.getText(1));

      String fieldName = Const.trim(item.getText(2));
      if (!Utils.isEmpty(fieldName)) {
        parameter.setField(fieldName);
      } else {
        parameter.setField("");
      }

      String value = Const.trim(item.getText(3));
      if (!Utils.isEmpty(value)) {
        parameter.setValue(value);
      } else {
        parameter.setValue("");
      }

      parameterDefinition.getParameters().add(parameter);
    }
    parameterDefinition.setPassingAllParameters(wPassParams.getSelection());

    action.setSetLogfile(wSetLogfile.getSelection());
    action.setAddDate(wAddDate.getSelection());
    action.setAddTime(wAddTime.getSelection());
    action.setLogfile(wLogfile.getText());
    action.setLogext(wLogext.getText());
    action.setLogFileLevel(LogLevel.lookupDescription(wLoglevel.getText()));
    action.setParamsFromPrevious(wPrevToParams.getSelection());
    action.setExecPerRow(wEveryRow.getSelection());
    action.setSetAppendLogfile(wAppendLogfile.getSelection());
    action.setWaitingToFinish(wWaitingToFinish.getSelection());
    action.setCreateParentFolder(wCreateParentFolder.getSelection());
    action.setRunConfiguration(wRunConfiguration.getText());
  }

  @Override
  public void ok() {
    if (Utils.isEmpty(getName())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    if (Utils.isEmpty(wPath.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "ActionWorkflowDialog.FilenameMissing.Header"));
      mb.setMessage(BaseMessages.getString(PKG, "ActionWorkflowDialog.FilenameMissing.Message"));
      mb.open();
      return;
    }
    if (isSelfReferencing()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "ActionWorkflowDialog.SelfReference.Header"));
      mb.setMessage(BaseMessages.getString(PKG, "ActionWorkflowDialog.SelfReference.Message"));
      mb.open();
      return;
    }

    getInfo(action);
    action.setChanged();
    dispose();
  }

  @VisibleForTesting
  protected String getName() {
    return wName.getText();
  }

  @VisibleForTesting
  protected String getPath() {
    return wPath.getText();
  }

  private boolean isSelfReferencing() {
    return variables.resolve(wPath.getText()).equals(variables.resolve(workflowMeta.getFilename()));
  }
}
