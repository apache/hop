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

package org.apache.hop.workflow.actions.pipeline;

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
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.apache.hop.ui.workflow.actions.ActionBaseDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline.Parameter;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline.ParameterDefinition;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/** This dialog allows you to edit the pipeline action (ActionPipeline) */
public class ActionPipelineDialog extends ActionBaseDialog {
  private static final Class<?> PKG = ActionPipeline.class;

  private ActionPipeline action;
  private MetaSelectionLine<PipelineRunConfiguration> wRunConfiguration;

  private static final String[] FILE_FILTERLOGNAMES =
      new String[] {
        BaseMessages.getString(PKG, "ActionPipeline.Fileformat.TXT"),
        BaseMessages.getString(PKG, "ActionPipeline.Fileformat.LOG"),
        BaseMessages.getString(PKG, "ActionPipeline.Fileformat.All")
      };

  public ActionPipelineDialog(
      Shell parent, ActionPipeline action, WorkflowMeta workflowMeta, IVariables variables) {
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
    shell.setText(BaseMessages.getString(PKG, "ActionPipeline.Header"));

    wlPath.setText(BaseMessages.getString(PKG, "ActionPipeline.PipelineFile.Label"));
    wPassParams.setText(BaseMessages.getString(PKG, "ActionPipeline.PassAllParameters.Label"));

    wClearRows = new Button(gExecution, SWT.CHECK);
    PropsUi.setLook(wClearRows);
    wClearRows.setText(BaseMessages.getString(PKG, "ActionPipeline.ClearResultList.Label"));
    FormData fdbClearRows = new FormData();
    fdbClearRows.left = new FormAttachment(0, 0);
    fdbClearRows.top = new FormAttachment(wEveryRow, 10);
    wClearRows.setLayoutData(fdbClearRows);

    wClearFiles = new Button(gExecution, SWT.CHECK);
    PropsUi.setLook(wClearFiles);
    wClearFiles.setText(BaseMessages.getString(PKG, "ActionPipeline.ClearResultFiles.Label"));
    FormData fdbClearFiles = new FormData();
    fdbClearFiles.left = new FormAttachment(0, 0);
    fdbClearFiles.top = new FormAttachment(wClearRows, 10);
    wClearFiles.setLayoutData(fdbClearFiles);

    wWaitingToFinish = new Button(gExecution, SWT.CHECK);
    PropsUi.setLook(wWaitingToFinish);
    wWaitingToFinish.setText(BaseMessages.getString(PKG, "ActionPipeline.WaitToFinish.Label"));
    FormData fdWait = new FormData();
    fdWait.top = new FormAttachment(wClearFiles, 10);
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
        "ui/images/pipeline.svg",
        ConstUi.LARGE_ICON_SIZE,
        ConstUi.LARGE_ICON_SIZE);
  }

  @Override
  protected int getParameterCount() {
    return action.getParameterDefinition().getParameters().size();
  }

  private void getParameters(PipelineMeta inputPipelineMeta) {
    try {
      if (inputPipelineMeta == null) {
        ActionPipeline jet = new ActionPipeline();
        getInfo(jet);
        inputPipelineMeta = jet.getPipelineMeta(this.getMetadataProvider(), variables);
      }
      String[] parameters = inputPipelineMeta.listParameters();

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
          BaseMessages.getString(PKG, "ActionPipelineDialog.Exception.UnableToLoadPipeline.Title"),
          BaseMessages.getString(
              PKG, "ActionPipelineDialog.Exception.UnableToLoadPipeline.Message"),
          e);
    }
  }

  @Override
  protected Control createRunConfigurationControl() {
    wRunConfiguration =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            PipelineRunConfiguration.class,
            shell,
            SWT.BORDER,
            null,
            null,
            true);

    return wRunConfiguration;
  }

  protected void pickFileVFS() {
    HopPipelineFileType<PipelineMeta> pipelineFileType = new HopPipelineFileType<>();
    String filename =
        BaseDialog.presentFileDialog(
            shell,
            wPath,
            variables,
            pipelineFileType.getFilterExtensions(),
            pipelineFileType.getFilterNames(),
            false);
    if (filename != null) {
      replaceNameWithBaseFilename(filename);
    }
  }

  public String getEntryName(String name) {
    return "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}/" + name;
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

    if (action.getLogfile() != null) {
      wLogfile.setText(action.getLogfile());
    }
    if (action.getLogext() != null) {
      wLogext.setText(action.getLogext());
    }

    wPrevToParams.setSelection(action.isParamsFromPrevious());
    wEveryRow.setSelection(action.isExecPerRow());
    wSetLogfile.setSelection(action.isSetLogfile());
    wAddDate.setSelection(action.isAddDate());
    wAddTime.setSelection(action.isAddTime());
    wClearRows.setSelection(action.isClearResultRows());
    wClearFiles.setSelection(action.isClearResultFiles());
    wWaitingToFinish.setSelection(action.isWaitingToFinish());
    wAppendLogfile.setSelection(action.isSetAppendLogfile());
    wCreateParentFolder.setSelection(action.isCreateParentFolder());
    if (action.getLogFileLevel() != null) {
      wLoglevel.select(action.getLogFileLevel().getLevel());
    }

    try {
      List<String> runConfigurations =
          this.getMetadataProvider()
              .getSerializer(PipelineRunConfiguration.class)
              .listObjectNames();

      try {
        ExtensionPointHandler.callExtensionPoint(
            HopGui.getInstance().getLog(),
            variables,
            HopExtensionPoint.HopGuiRunConfiguration.id,
            new Object[] {runConfigurations, PipelineMeta.XML_TAG});
      } catch (HopException e) {
        // Ignore errors
      }

      wRunConfiguration.setItems(runConfigurations.toArray(new String[0]));
      wRunConfiguration.setText(Const.NVL(action.getRunConfiguration(), ""));

      if (Utils.isEmpty(action.getRunConfiguration())) {
        wRunConfiguration.select(0);
      } else {
        wRunConfiguration.setText(action.getRunConfiguration());
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting pipeline run configurations", e);
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

  private void getInfo(ActionPipeline actionPipeline) throws HopException {
    actionPipeline.setName(wName.getText());
    actionPipeline.setRunConfiguration(wRunConfiguration.getText());
    actionPipeline.setFileName(wPath.getText());

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

    actionPipeline.setLogfile(wLogfile.getText());
    actionPipeline.setLogext(wLogext.getText());
    actionPipeline.setLogFileLevel(LogLevel.lookupDescription(wLoglevel.getText()));
    actionPipeline.setParamsFromPrevious(wPrevToParams.getSelection());
    actionPipeline.setExecPerRow(wEveryRow.getSelection());
    actionPipeline.setSetLogfile(wSetLogfile.getSelection());
    actionPipeline.setAddDate(wAddDate.getSelection());
    actionPipeline.setAddTime(wAddTime.getSelection());
    actionPipeline.setClearResultRows(wClearRows.getSelection());
    actionPipeline.setClearResultFiles(wClearFiles.getSelection());
    actionPipeline.setCreateParentFolder(wCreateParentFolder.getSelection());
    actionPipeline.setRunConfiguration(wRunConfiguration.getText());
    actionPipeline.setSetAppendLogfile(wAppendLogfile.getSelection());
    actionPipeline.setWaitingToFinish(wWaitingToFinish.getSelection());
  }

  @Override
  protected void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      box.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      box.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      box.open();
      return;
    }
    action.setName(wName.getText());

    try {
      getInfo(action);
    } catch (HopException e) {
      // suppress exceptions at this time - we will let the runtime report on any errors
    }
    action.setChanged();
    dispose();
  }
}
