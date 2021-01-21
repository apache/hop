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

package org.apache.hop.ui.workflow.actions.workflow;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.HelpUtils;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.apache.hop.ui.workflow.actions.pipeline.ActionBaseDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.actions.workflow.ActionWorkflow;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.util.List;

/**
 * This dialog allows you to edit the workflow action (ActionWorkflow)
 *
 * @author Matt
 * @since 19-06-2003
 */
public class ActionWorkflowDialog extends ActionBaseDialog implements IActionDialog {
  private static final Class<?> PKG = ActionWorkflow.class; // For Translator

  protected ActionWorkflow action;

  protected Button wPassExport;

  protected Button wExpandRemote;

  private static final String[] FILE_FILTERLOGNAMES =
      new String[] {
        BaseMessages.getString(PKG, "ActionWorkflow.Fileformat.TXT"),
        BaseMessages.getString(PKG, "ActionWorkflow.Fileformat.LOG"),
        BaseMessages.getString(PKG, "ActionWorkflow.Fileformat.All")
      };

  public ActionWorkflowDialog(Shell parent, IAction action, WorkflowMeta workflowMeta) {
    super(parent, action, workflowMeta);
    this.action = (ActionWorkflow) action;
  }

  public IAction open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);
    IPlugin plugin = PluginRegistry.getInstance().getPlugin(ActionPluginType.class, action);
    plugin.setDocumentationUrl(
        "https://hop.apache.org/manual/latest/plugins/actions/workflow.html");
    HelpUtils.createHelpButton(shell, HelpUtils.getHelpDialogTitle(plugin), plugin);

    backupChanged = action.hasChanged();

    createElements();

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();
    setActive();

    BaseTransformDialog.setSize(shell);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return action;
  }

  protected void createElements() {
    super.createElements();
    shell.setText(BaseMessages.getString(PKG, "ActionWorkflow.Header"));

    wlPath.setText(BaseMessages.getString(PKG, "ActionWorkflow.WorkflowFile.Label"));
    wPassParams.setText(BaseMessages.getString(PKG, "ActionWorkflow.PassAllParameters.Label"));

    // Start Server Section
    wPassExport = new Button(gExecution, SWT.CHECK);
    wPassExport.setText(
        BaseMessages.getString(PKG, "ActionWorkflowDialog.PassExportToServer.Label"));
    props.setLook(wPassExport);
    FormData fdPassExport = new FormData();
    fdPassExport.left = new FormAttachment(0, 0);
    fdPassExport.top = new FormAttachment(wEveryRow, 10);
    fdPassExport.right = new FormAttachment(100, 0);
    wPassExport.setLayoutData(fdPassExport);

    wExpandRemote = new Button(gExecution, SWT.CHECK);
    wExpandRemote.setText(
        BaseMessages.getString(PKG, "ActionWorkflowDialog.ExpandRemoteOnServer.Label"));
    props.setLook(wExpandRemote);
    FormData fdExpandRemote = new FormData();
    fdExpandRemote.top = new FormAttachment(wPassExport, 10);
    fdExpandRemote.left = new FormAttachment(0, 0);
    wExpandRemote.setLayoutData(fdExpandRemote);

    wWaitingToFinish = new Button(gExecution, SWT.CHECK);
    props.setLook(wWaitingToFinish);
    wWaitingToFinish.setText(BaseMessages.getString(PKG, "ActionWorkflow.WaitToFinish.Label"));
    FormData fdWait = new FormData();
    fdWait.top = new FormAttachment(wExpandRemote, 10);
    fdWait.left = new FormAttachment(0, 0);
    wWaitingToFinish.setLayoutData(fdWait);

    wFollowingAbortRemotely = new Button(gExecution, SWT.CHECK);
    props.setLook(wFollowingAbortRemotely);
    wFollowingAbortRemotely.setText(
        BaseMessages.getString(PKG, "ActionWorkflow.AbortRemote.Label"));
    FormData fdFollow = new FormData();
    fdFollow.top = new FormAttachment(wWaitingToFinish, 10);
    fdFollow.left = new FormAttachment(0, 0);
    wFollowingAbortRemotely.setLayoutData(fdFollow);
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
          public void widgetSelected(SelectionEvent e) {
            pickFileVFS();
          }
        });

    wbLogFilename.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            selectLogFile(FILE_FILTERLOGNAMES);
          }
        });
  }

  protected ActionBase getAction() {
    return action;
  }

  protected Image getImage() {
    return SwtSvgImageUtil.getImage(
        shell.getDisplay(),
        getClass().getClassLoader(),
        "ui/images/workflow.svg",
        ConstUi.LARGE_ICON_SIZE,
        ConstUi.LARGE_ICON_SIZE);
  }

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
    wPassExport.setSelection(action.isPassingExport());

    if (action.logFileLevel != null) {
      wLoglevel.select(action.logFileLevel.getLevel());
    } else {
      // Set the default log level
      wLoglevel.select(ActionWorkflow.DEFAULT_LOG_LEVEL.getLevel());
    }
    wAppendLogfile.setSelection(action.setAppendLogfile);
    wCreateParentFolder.setSelection(action.createParentFolder);
    wWaitingToFinish.setSelection(action.isWaitingToFinish());
    wFollowingAbortRemotely.setSelection(action.isFollowingAbortRemotely());

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
    aw.setPassingExport(wPassExport.getSelection());
    aw.setAppendLogfile = wAppendLogfile.getSelection();
    aw.setWaitingToFinish(wWaitingToFinish.getSelection());
    aw.createParentFolder = wCreateParentFolder.getSelection();
    aw.setFollowingAbortRemotely(wFollowingAbortRemotely.getSelection());
    aw.setRunConfiguration(wRunConfiguration.getText());
  }

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
