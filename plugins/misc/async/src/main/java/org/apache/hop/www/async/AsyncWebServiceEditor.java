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

package org.apache.hop.www.async;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;

/**
 * Editor that allows you to change Asynchronous Web Service metadata
 *
 * @see AsyncWebService
 */
public class AsyncWebServiceEditor extends MetadataEditor<AsyncWebService> {
  private static final Class<?> PKG = AsyncWebServiceEditor.class;

  private Text wName;
  private Button wEnabled;
  private TextVar wFilename;
  private MetaSelectionLine<WorkflowRunConfiguration> wRunConfiguration;
  private TextVar wStatusVars;
  private TextVar wContentVar;
  private TextVar wHeaderContentVariable;

  public AsyncWebServiceEditor(
      HopGui hopGui, MetadataManager<AsyncWebService> manager, AsyncWebService metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {

    PropsUi props = PropsUi.getInstance();

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlIcon = new FormData();
    fdlIcon.top = new FormAttachment(0, 0);
    fdlIcon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlIcon);
    PropsUi.setLook(wIcon);

    // What's the name
    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText("Name");
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(wIcon, margin);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    Label spacer = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wName, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);
    Control lastControl = spacer;

    // Enabled?
    //
    Label wlEnabled = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlEnabled);
    wlEnabled.setText(BaseMessages.getString(PKG, "System.Button.Enabled"));
    FormData fdlEnabled = new FormData();
    fdlEnabled.left = new FormAttachment(0, 0);
    fdlEnabled.right = new FormAttachment(middle, -margin);
    fdlEnabled.top = new FormAttachment(lastControl, margin);
    wlEnabled.setLayoutData(fdlEnabled);
    wEnabled = new Button(parent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wEnabled);
    FormData fdEnabled = new FormData();
    fdEnabled.left = new FormAttachment(middle, 0);
    fdEnabled.right = new FormAttachment(100, 0);
    fdEnabled.top = new FormAttachment(wlEnabled, 0, SWT.CENTER);
    wEnabled.setLayoutData(fdEnabled);
    lastControl = wlEnabled;

    Label wlFilename = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlFilename);
    wlFilename.setText("Filename");
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    fdlFilename.top = new FormAttachment(lastControl, 2 * margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbbFilename = new Button(parent, SWT.PUSH);
    PropsUi.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbbFilename = new FormData();
    fdbbFilename.right = new FormAttachment(100, 0);
    fdbbFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wbbFilename.setLayoutData(fdbbFilename);
    wbbFilename.addListener(SWT.Selection, e -> selectWorkflowFilename(parent));

    Button wbnFilename = new Button(parent, SWT.PUSH);
    PropsUi.setLook(wbnFilename);
    wbnFilename.setText(BaseMessages.getString(PKG, "System.Button.New"));
    FormData fdbnFilename = new FormData();
    fdbnFilename.right = new FormAttachment(wbbFilename, -margin);
    fdbnFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wbnFilename.setLayoutData(fdbnFilename);
    wbnFilename.addListener(SWT.Selection, e -> createWorkflowFile(parent));

    Button wboFilename = new Button(parent, SWT.PUSH);
    PropsUi.setLook(wboFilename);
    wboFilename.setText(BaseMessages.getString(PKG, "System.Button.Open"));
    FormData fdboFilename = new FormData();
    fdboFilename.right = new FormAttachment(wbnFilename, -margin);
    fdboFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wboFilename.setLayoutData(fdboFilename);
    wboFilename.addListener(SWT.Selection, e -> openWorkflowFile(parent));

    wFilename = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wboFilename, -margin);
    fdFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wFilename.setLayoutData(fdFilename);
    lastControl = wlFilename;

    wRunConfiguration =
        new MetaSelectionLine<>(
            manager.getVariables(),
            manager.getMetadataProvider(),
            WorkflowRunConfiguration.class,
            parent,
            SWT.NONE,
            BaseMessages.getString(PKG, "AsyncWebService.Runconfiguration.Label"),
            BaseMessages.getString(PKG, "AsyncWebService.Runconfiguration.Tooltip"));
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.left = new FormAttachment(0, 0);
    fdRunConfiguration.top = new FormAttachment(lastControl, margin);
    fdRunConfiguration.right = new FormAttachment(100, 0);
    wRunConfiguration.setLayoutData(fdRunConfiguration);
    lastControl = wRunConfiguration;

    // Status variables
    //
    Label wlStatusVars = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlStatusVars);
    wlStatusVars.setText(BaseMessages.getString(PKG, "AsyncWebService.StatusVariables.Label"));
    FormData fdlStatusVars = new FormData();
    fdlStatusVars.left = new FormAttachment(0, 0);
    fdlStatusVars.right = new FormAttachment(middle, -margin);
    fdlStatusVars.top = new FormAttachment(lastControl, 2 * margin);
    wlStatusVars.setLayoutData(fdlStatusVars);
    wStatusVars = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStatusVars);
    FormData fdStatusVars = new FormData();
    fdStatusVars.left = new FormAttachment(middle, 0);
    fdStatusVars.right = new FormAttachment(100, 0);
    fdStatusVars.top = new FormAttachment(wlStatusVars, 0, SWT.CENTER);
    wStatusVars.setLayoutData(fdStatusVars);
    lastControl = wlStatusVars;

    // body content variables
    Label wlContentVar = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlContentVar);
    wlContentVar.setText(BaseMessages.getString(PKG, "AsyncWebService.BodyContentVariable.Label"));
    FormData fdlContentVar = new FormData();
    fdlContentVar.left = new FormAttachment(0, 0);
    fdlContentVar.right = new FormAttachment(middle, -margin);
    fdlContentVar.top = new FormAttachment(lastControl, 2 * margin);
    wlContentVar.setLayoutData(fdlContentVar);
    wContentVar = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wContentVar);
    FormData fdContentVar = new FormData();
    fdContentVar.left = new FormAttachment(middle, 0);
    fdContentVar.right = new FormAttachment(100, 0);
    fdContentVar.top = new FormAttachment(wlContentVar, 0, SWT.CENTER);
    wContentVar.setLayoutData(fdContentVar);
    lastControl = wContentVar;

    // HeaderContentVariable to read from
    //
    Label wlHeaderContentVariable = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlHeaderContentVariable);
    wlHeaderContentVariable.setText(
        BaseMessages.getString(PKG, "AsyncWebService.HeaderContentVariable.Label"));
    wlHeaderContentVariable.setToolTipText(
        BaseMessages.getString(PKG, "AsyncWebService.HeaderContentVariable.Tooltip"));
    FormData fdlHeaderContentVariable = new FormData();
    fdlHeaderContentVariable.left = new FormAttachment(0, 0);
    fdlHeaderContentVariable.right = new FormAttachment(middle, -margin);
    fdlHeaderContentVariable.top = new FormAttachment(lastControl, 2 * margin);
    wlHeaderContentVariable.setLayoutData(fdlHeaderContentVariable);
    wHeaderContentVariable =
        new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHeaderContentVariable.setToolTipText(
        BaseMessages.getString(PKG, "AsyncWebService.HeaderContentVariable.Tooltip"));
    PropsUi.setLook(wHeaderContentVariable);
    FormData fdHeaderContentVariable = new FormData();
    fdHeaderContentVariable.left = new FormAttachment(middle, 0);
    fdHeaderContentVariable.right = new FormAttachment(100, 0);
    fdHeaderContentVariable.top = new FormAttachment(wlHeaderContentVariable, 0, SWT.CENTER);
    wHeaderContentVariable.setLayoutData(fdHeaderContentVariable);
    lastControl = wlHeaderContentVariable;

    setWidgetsContent();

    // Add listener to detect change after loading data
    Listener lsMod = e -> setChanged();
    wName.addListener(SWT.Modify, lsMod);
    wEnabled.addListener(SWT.Selection, lsMod);
    wFilename.addListener(SWT.Modify, lsMod);
    wRunConfiguration.addListener(SWT.Modify, lsMod);
    wStatusVars.addListener(SWT.Modify, lsMod);
    wContentVar.addListener(SWT.Modify, lsMod);
    wHeaderContentVariable.addListener(SWT.Modify, lsMod);
  }

  /**
   * Create a new workflow file with a note to explain what's going on
   *
   * @param parent
   */
  private void createWorkflowFile(Composite parent) {
    try {
      // Create an empty workflow...
      //
      WorkflowMeta workflowMeta = new WorkflowMeta();

      // Add a note explaining what's going on.
      //
      NotePadMeta note =
          new NotePadMeta(
              "This workflow can set status variables which are picked up when you request the status of this workflow."
                  + Const.CR
                  + "You can use service asyncStatus to query the status.",
              150,
              350,
              -1,
              -1);
      workflowMeta.addNote(note);

      // Save it...
      //
      HopWorkflowFileType<WorkflowMeta> type = new HopWorkflowFileType<>();
      String filename =
          BaseDialog.presentFileDialog(
              true, // save
              parent.getShell(),
              wFilename,
              manager.getVariables(),
              type.getFilterExtensions(),
              type.getFilterNames(),
              true);
      if (filename != null) {
        // User specified a pipeline filename
        //
        String realFilename = manager.getVariables().resolve(filename);
        workflowMeta.setFilename(realFilename);
        workflowMeta.clearChanged();

        HopDataOrchestrationPerspective perspective = HopGui.getDataOrchestrationPerspective();

        // Switch to the perspective
        //
        perspective.activate();

        // Open it in the Hop GUI
        //
        HopGui.getDataOrchestrationPerspective().addWorkflow(hopGui, workflowMeta, type);

        // Save the file
        hopGui.fileDelegate.fileSave();
      }
    } catch (Exception e) {
      new ErrorDialog(parent.getShell(), "Error", "Error creating workflow", e);
    }
  }

  /**
   * Open the specified file
   *
   * @param parent
   */
  private void openWorkflowFile(Composite parent) {
    try {
      String filename = manager.getVariables().resolve(wFilename.getText());
      if (StringUtils.isNotEmpty(filename)) {
        hopGui.fileDelegate.fileOpen(filename);
      }
    } catch (Exception e) {
      new ErrorDialog(parent.getShell(), "Error", "Error opening workflow", e);
    }
  }

  private void selectWorkflowFilename(Composite parent) {
    HopWorkflowFileType<?> type = new HopWorkflowFileType<>();
    BaseDialog.presentFileDialog(
        parent.getShell(),
        wFilename,
        manager.getVariables(),
        type.getFilterExtensions(),
        type.getFilterNames(),
        true);
  }

  @Override
  public void setWidgetsContent() {
    AsyncWebService ws = getMetadata();

    wName.setText(Const.NVL(ws.getName(), ""));
    wEnabled.setSelection(ws.isEnabled());
    wFilename.setText(Const.NVL(ws.getFilename(), ""));
    wStatusVars.setText(Const.NVL(ws.getStatusVariables(), ""));
    wContentVar.setText(Const.NVL(ws.getBodyContentVariable(), ""));
    wHeaderContentVariable.setText(Const.NVL(ws.getHeaderContentVariable(), ""));
    try {
      wRunConfiguration.fillItems();
      wRunConfiguration.setText(Const.NVL(ws.getRunConfigurationName(), ""));
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting workflow run configurations", e);
    }
  }

  @Override
  public void getWidgetsContent(AsyncWebService ws) {
    ws.setName(wName.getText());
    ws.setEnabled(wEnabled.getSelection());
    ws.setFilename(wFilename.getText());
    ws.setStatusVariables(wStatusVars.getText());
    ws.setBodyContentVariable(wContentVar.getText());
    ws.setRunConfigurationName(wRunConfiguration.getText());
    ws.setHeaderContentVariable(wHeaderContentVariable.getText());
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }
}
