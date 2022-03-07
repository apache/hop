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
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.apache.hop.ui.workflow.actions.ActionBaseDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;

/** This dialog allows you to edit the workflow action (ActionWorkflow) */
public class ActionWorkflowDialog extends ActionBaseDialog implements IActionDialog {
  private static final Class<?> PKG = ActionWorkflow.class; // For Translator

  protected ActionWorkflow action;

  private static final String[] FILE_FILTERLOGNAMES =
      new String[] {
        BaseMessages.getString(PKG, "ActionWorkflow.Fileformat.TXT"),
        BaseMessages.getString(PKG, "ActionWorkflow.Fileformat.LOG"),
        BaseMessages.getString(PKG, "ActionWorkflow.Fileformat.All")
      };

  public ActionWorkflowDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, action, workflowMeta, variables);
    this.action = (ActionWorkflow) action;
  }

  @Override
  public IAction open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    props.setLook(shell);
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
    props.setLook(wWaitingToFinish);
    wWaitingToFinish.setText(BaseMessages.getString(PKG, "ActionWorkflow.WaitToFinish.Label"));
    FormData fdWait = new FormData();
    fdWait.top = new FormAttachment(wEveryRow, 10);
    fdWait.left = new FormAttachment(0, 0);
    wWaitingToFinish.setLayoutData(fdWait);

    // End Server Section

    Composite cRunConfiguration = new Composite(wOptions, SWT.NONE);
    cRunConfiguration.setLayout(new FormLayout());
    props.setLook(cRunConfiguration);
    FormData fdLocal = new FormData();
    fdLocal.top = new FormAttachment(0);
    fdLocal.right = new FormAttachment(100);
    fdLocal.left = new FormAttachment(0);

    wbGetParams.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            getParameters(null); // force reload from file specification
          }
        });

    wbBrowse.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            pickFileVFS();
          }
        });

    wbLogFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            selectLogFile(FILE_FILTERLOGNAMES);
          }
        });
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
  protected String[] getParameters() {
    return action.parameters;
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

      for (int i = 0; i < parameters.length; i++) {
        if (Const.indexOfString(parameters[i], existing) < 0) {
          TableItem item = new TableItem(wParameters.table, SWT.NONE);
          item.setText(1, parameters[i]);
        }
      }
      wParameters.removeEmptyRows();
      wParameters.setRowNums();
      wParameters.optWidth(true);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ActionWorkflowDialog.Exception.UnableToLoadJob.Title"),
          BaseMessages.getString(PKG, "ActionWorkflowDialog.Exception.UnableToLoadJob.Message"),
          e);
    }
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
            true);
    if (filename != null) {
      replaceNameWithBaseFilename(filename);
    }
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  @Override
  public void setActive() {
    super.setActive();
  }

  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wPath.setText(Const.NVL(action.getFilename(), ""));

    // Parameters
    if (action.parameters != null) {
      for (int i = 0; i < action.parameters.length; i++) {
        TableItem ti = wParameters.table.getItem(i);
        if (!Utils.isEmpty(action.parameters[i])) {
          ti.setText(1, Const.NVL(action.parameters[i], ""));
          ti.setText(2, Const.NVL(action.parameterFieldNames[i], ""));
          ti.setText(3, Const.NVL(action.parameterValues[i], ""));
        }
      }
      wParameters.setRowNums();
      wParameters.optWidth(true);
    }

    wPassParams.setSelection(action.isPassingAllParameters());

    wPrevToParams.setSelection(action.paramsFromPrevious);
    wEveryRow.setSelection(action.execPerRow);
    wSetLogfile.setSelection(action.setLogfile);
    if (action.logfile != null) {
      wLogfile.setText(action.logfile);
    }
    if (action.logext != null) {
      wLogext.setText(action.logext);
    }
    wAddDate.setSelection(action.addDate);
    wAddTime.setSelection(action.addTime);

    if (action.logFileLevel != null) {
      wLoglevel.select(action.logFileLevel.getLevel());
    } else {
      // Set the default log level
      wLoglevel.select(ActionWorkflow.DEFAULT_LOG_LEVEL.getLevel());
    }
    wAppendLogfile.setSelection(action.setAppendLogfile);
    wCreateParentFolder.setSelection(action.createParentFolder);
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
      wRunConfiguration.setText(Const.NVL(action.getRunConfiguration(), ""));

      if (Utils.isEmpty(action.getRunConfiguration())) {
        wRunConfiguration.select(0);
      } else {
        wRunConfiguration.setText(action.getRunConfiguration());
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting workflow run configurations", e);
    }

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
  protected void getInfo(ActionWorkflow aw) {
    String jobPath = getPath();
    aw.setName(getName());
    aw.setFileName(jobPath);
    aw.setRunConfiguration(wRunConfiguration.getText());

    // Do the parameters
    int nrItems = wParameters.nrNonEmpty();
    int nr = 0;
    for (int i = 0; i < nrItems; i++) {
      String param = wParameters.getNonEmpty(i).getText(1);
      if (param != null && param.length() != 0) {
        nr++;
      }
    }
    aw.parameters = new String[nr];
    aw.parameterFieldNames = new String[nr];
    aw.parameterValues = new String[nr];
    nr = 0;
    for (int i = 0; i < nrItems; i++) {
      String param = wParameters.getNonEmpty(i).getText(1);
      String fieldName = wParameters.getNonEmpty(i).getText(2);
      String value = wParameters.getNonEmpty(i).getText(3);

      aw.parameters[nr] = param;

      if (!Utils.isEmpty(Const.trim(fieldName))) {
        aw.parameterFieldNames[nr] = fieldName;
      } else {
        aw.parameterFieldNames[nr] = "";
      }

      if (!Utils.isEmpty(Const.trim(value))) {
        aw.parameterValues[nr] = value;
      } else {
        aw.parameterValues[nr] = "";
      }

      nr++;
    }
    aw.setPassingAllParameters(wPassParams.getSelection());

    aw.setLogfile = wSetLogfile.getSelection();
    aw.addDate = wAddDate.getSelection();
    aw.addTime = wAddTime.getSelection();
    aw.logfile = wLogfile.getText();
    aw.logext = wLogext.getText();
    if (wLoglevel.getSelectionIndex() >= 0) {
      aw.logFileLevel = LogLevel.values()[wLoglevel.getSelectionIndex()];
    } else {
      aw.logFileLevel = LogLevel.BASIC;
    }
    aw.paramsFromPrevious = wPrevToParams.getSelection();
    aw.execPerRow = wEveryRow.getSelection();
    aw.setAppendLogfile = wAppendLogfile.getSelection();
    aw.setWaitingToFinish(wWaitingToFinish.getSelection());
    aw.createParentFolder = wCreateParentFolder.getSelection();
    aw.setRunConfiguration(wRunConfiguration.getText());
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
}
