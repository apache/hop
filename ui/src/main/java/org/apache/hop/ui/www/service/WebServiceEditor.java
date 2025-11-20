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
 *
 */

package org.apache.hop.ui.www.service;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.www.service.WebService;
import org.apache.http.entity.ContentType;
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
 * Editor that allows you to change Web Service metadata
 *
 * @see WebService
 */
public class WebServiceEditor extends MetadataEditor<WebService> {
  private static final Class<?> PKG = WebServiceEditor.class;
  public static final String CONST_ERROR = "Error";

  private Text wName;
  private Button wEnabled;
  private TextVar wFilename;
  private MetaSelectionLine<PipelineRunConfiguration> wRunConfiguration;
  private TextVar wTransform;
  private TextVar wField;
  private TextVar wStatusCode;
  private ComboVar wContentType;
  private Button wListStatus;
  private TextVar wBodyContentVariable;
  private TextVar wHeaderContentVariable;

  public WebServiceEditor(HopGui hopGui, MetadataManager<WebService> manager, WebService metadata) {
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
    wlName.setText(BaseMessages.getString(PKG, "WebServiceEditor.Name.Label"));
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
    wlEnabled.setText(BaseMessages.getString(PKG, "WebServiceEditor.Enabled.Label"));
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
    wlFilename.setText(BaseMessages.getString(PKG, "WebServiceEditor.Filename.Label"));
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
    wbbFilename.addListener(SWT.Selection, e -> selectPipelineFilename(parent));

    Button wbnFilename = new Button(parent, SWT.PUSH);
    PropsUi.setLook(wbnFilename);
    wbnFilename.setText(BaseMessages.getString(PKG, "System.Button.New"));
    FormData fdbnFilename = new FormData();
    fdbnFilename.right = new FormAttachment(wbbFilename, -margin);
    fdbnFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wbnFilename.setLayoutData(fdbnFilename);
    wbnFilename.addListener(SWT.Selection, e -> createPipelineFile(parent));

    Button wboFilename = new Button(parent, SWT.PUSH);
    PropsUi.setLook(wboFilename);
    wboFilename.setText(BaseMessages.getString(PKG, "System.Button.Open"));
    FormData fdboFilename = new FormData();
    fdboFilename.right = new FormAttachment(wbnFilename, -margin);
    fdboFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wboFilename.setLayoutData(fdboFilename);
    wboFilename.addListener(SWT.Selection, e -> openPipelineFile(parent));

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
            PipelineRunConfiguration.class,
            parent,
            SWT.NONE,
            BaseMessages.getString(PKG, "WebServiceEditor.Runconfiguration.Label"),
            BaseMessages.getString(PKG, "WebServiceEditor.Runconfiguration.Tooltip"));
    FormData fdRunConfiguration = new FormData();
    fdRunConfiguration.left = new FormAttachment(0, 0);
    fdRunConfiguration.top = new FormAttachment(lastControl, margin);
    fdRunConfiguration.right = new FormAttachment(100, 0);
    wRunConfiguration.setLayoutData(fdRunConfiguration);
    lastControl = wRunConfiguration;

    // The transform to read from
    //
    Label wlTransform = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlTransform);
    wlTransform.setText(BaseMessages.getString(PKG, "WebServiceEditor.Transform.Label"));
    FormData fdlTransform = new FormData();
    fdlTransform.left = new FormAttachment(0, 0);
    fdlTransform.right = new FormAttachment(middle, -margin);
    fdlTransform.top = new FormAttachment(lastControl, 2 * margin);
    wlTransform.setLayoutData(fdlTransform);
    wTransform = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wTransform);
    FormData fdTransform = new FormData();
    fdTransform.left = new FormAttachment(middle, 0);
    fdTransform.right = new FormAttachment(100, 0);
    fdTransform.top = new FormAttachment(wlTransform, 0, SWT.CENTER);
    wTransform.setLayoutData(fdTransform);
    lastControl = wlTransform;

    // Field to read from
    //
    Label wlField = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlField);
    wlField.setText(BaseMessages.getString(PKG, "WebServiceEditor.Field.Label"));
    FormData fdlField = new FormData();
    fdlField.left = new FormAttachment(0, 0);
    fdlField.right = new FormAttachment(middle, -margin);
    fdlField.top = new FormAttachment(lastControl, 2 * margin);
    wlField.setLayoutData(fdlField);
    wField = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wField);
    FormData fdField = new FormData();
    fdField.left = new FormAttachment(middle, 0);
    fdField.right = new FormAttachment(100, 0);
    fdField.top = new FormAttachment(wlField, 0, SWT.CENTER);
    wField.setLayoutData(fdField);
    lastControl = wlField;

    // Status code field
    //
    Label wlStatuscode = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlField);
    wlStatuscode.setText(BaseMessages.getString(PKG, "WebServiceEditor.StatusCodeField.Label"));
    FormData fdlStatusCode = new FormData();
    fdlStatusCode.left = new FormAttachment(0, 0);
    fdlStatusCode.right = new FormAttachment(middle, -margin);
    fdlStatusCode.top = new FormAttachment(lastControl, 2 * margin);
    wlStatuscode.setLayoutData(fdlStatusCode);
    wStatusCode = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStatusCode);
    FormData fdStatuscode = new FormData();
    fdStatuscode.left = new FormAttachment(middle, 0);
    fdStatuscode.right = new FormAttachment(100, 0);
    fdStatuscode.top = new FormAttachment(wlStatuscode, 0, SWT.CENTER);
    wStatusCode.setLayoutData(fdStatuscode);
    lastControl = wlStatuscode;

    // Content type
    //
    Label wlContentType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlContentType);
    wlContentType.setText(BaseMessages.getString(PKG, "WebServiceEditor.ContentType.Label"));
    FormData fdlContentType = new FormData();
    fdlContentType.left = new FormAttachment(0, 0);
    fdlContentType.right = new FormAttachment(middle, -margin);
    fdlContentType.top = new FormAttachment(lastControl, 2 * margin);
    wlContentType.setLayoutData(fdlContentType);
    wContentType = new ComboVar(manager.getVariables(), parent, SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wContentType);
    FormData fdContentType = new FormData();
    fdContentType.left = new FormAttachment(middle, 0);
    fdContentType.right = new FormAttachment(100, 0);
    fdContentType.top = new FormAttachment(wlContentType, 0, SWT.CENTER);
    wContentType.setLayoutData(fdContentType);
    wContentType.add(ContentType.TEXT_PLAIN.getMimeType());
    wContentType.add(ContentType.APPLICATION_JSON.getMimeType());
    wContentType.add(ContentType.APPLICATION_XML.getMimeType());
    wContentType.add(ContentType.TEXT_HTML.getMimeType());
    lastControl = wlContentType;

    // List status?
    //
    Label wlListStatus = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlListStatus);
    wlListStatus.setText(BaseMessages.getString(PKG, "WebServiceEditor.ListStatus.Label"));
    FormData fdlListStatus = new FormData();
    fdlListStatus.left = new FormAttachment(0, 0);
    fdlListStatus.right = new FormAttachment(middle, -margin);
    fdlListStatus.top = new FormAttachment(lastControl, margin);
    wlListStatus.setLayoutData(fdlListStatus);
    wListStatus = new Button(parent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wListStatus);
    FormData fdListStatus = new FormData();
    fdListStatus.left = new FormAttachment(middle, 0);
    fdListStatus.right = new FormAttachment(100, 0);
    fdListStatus.top = new FormAttachment(wlListStatus, 0, SWT.CENTER);
    wListStatus.setLayoutData(fdListStatus);
    lastControl = wListStatus;

    // BodyContentVariable to read from
    //
    Label wlBodyContentVariable = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlBodyContentVariable);
    wlBodyContentVariable.setText(
        BaseMessages.getString(PKG, "WebServiceEditor.BodyContentVariable.Label"));
    wlBodyContentVariable.setToolTipText(
        BaseMessages.getString(PKG, "WebServiceEditor.BodyContentVariable.Tooltip"));
    FormData fdlBodyContentVariable = new FormData();
    fdlBodyContentVariable.left = new FormAttachment(0, 0);
    fdlBodyContentVariable.right = new FormAttachment(middle, -margin);
    fdlBodyContentVariable.top = new FormAttachment(lastControl, 2 * margin);
    wlBodyContentVariable.setLayoutData(fdlBodyContentVariable);
    wBodyContentVariable =
        new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBodyContentVariable.setToolTipText(
        BaseMessages.getString(PKG, "WebServiceEditor.BodyContentVariable.Tooltip"));
    PropsUi.setLook(wBodyContentVariable);
    FormData fdBodyContentVariable = new FormData();
    fdBodyContentVariable.left = new FormAttachment(middle, 0);
    fdBodyContentVariable.right = new FormAttachment(100, 0);
    fdBodyContentVariable.top = new FormAttachment(wlBodyContentVariable, 0, SWT.CENTER);
    wBodyContentVariable.setLayoutData(fdBodyContentVariable);
    lastControl = wlBodyContentVariable;

    // HeaderContentVariable to read from
    //
    Label wlHeaderContentVariable = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlHeaderContentVariable);
    wlHeaderContentVariable.setText(
        BaseMessages.getString(PKG, "WebServiceEditor.HeaderContentVariable.Label"));
    wlHeaderContentVariable.setToolTipText(
        BaseMessages.getString(PKG, "WebServiceEditor.HeaderContentVariable.Tooltip"));
    FormData fdlHeaderContentVariable = new FormData();
    fdlHeaderContentVariable.left = new FormAttachment(0, 0);
    fdlHeaderContentVariable.right = new FormAttachment(middle, -margin);
    fdlHeaderContentVariable.top = new FormAttachment(lastControl, 2 * margin);
    wlHeaderContentVariable.setLayoutData(fdlHeaderContentVariable);
    wHeaderContentVariable =
        new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHeaderContentVariable.setToolTipText(
        BaseMessages.getString(PKG, "WebServiceEditor.HeaderContentVariable.Tooltip"));
    PropsUi.setLook(wHeaderContentVariable);
    FormData fdHeaderContentVariable = new FormData();
    fdHeaderContentVariable.left = new FormAttachment(middle, 0);
    fdHeaderContentVariable.right = new FormAttachment(100, 0);
    fdHeaderContentVariable.top = new FormAttachment(wlHeaderContentVariable, 0, SWT.CENTER);
    wHeaderContentVariable.setLayoutData(fdHeaderContentVariable);
    lastControl = wlHeaderContentVariable;

    setWidgetsContent();

    // Add listener to detect change after loading data
    Listener modifyListener = e -> setChanged();
    wName.addListener(SWT.Modify, modifyListener);
    wEnabled.addListener(SWT.Selection, modifyListener);
    wFilename.addListener(SWT.Modify, modifyListener);
    wTransform.addListener(SWT.Modify, modifyListener);
    wField.addListener(SWT.Modify, modifyListener);
    wStatusCode.addListener(SWT.Modify, modifyListener);
    wContentType.addListener(SWT.Modify, modifyListener);
    wListStatus.addListener(SWT.Selection, modifyListener);
    wBodyContentVariable.addListener(SWT.Modify, modifyListener);
    wHeaderContentVariable.addListener(SWT.Modify, modifyListener);
    wRunConfiguration.addListener(SWT.Selection, modifyListener);
  }

  /**
   * Create a new pipeline file with a note to explain what's going on
   *
   * @param parent
   */
  private void createPipelineFile(Composite parent) {
    try {

      // Create an empty pipeline...
      //
      PipelineMeta pipelineMeta = new PipelineMeta();

      // Add a note explaining what's going on.
      //
      NotePadMeta note =
          new NotePadMeta(
              "This pipeline can create output for a web service."
                  + Const.CR
                  + "It will pick up the data in a single field in a single transform of this pipeline",
              150,
              350,
              -1,
              -1);
      pipelineMeta.addNote(note);

      // Save it...
      //
      HopPipelineFileType<PipelineMeta> type = new HopPipelineFileType<>();
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
        pipelineMeta.setFilename(realFilename);
        pipelineMeta.clearChanged();

        // Open it in the Hop GUI
        //
        HopGui.getExplorerPerspective().addPipeline(pipelineMeta);

        // Switch to the perspective
        //
        HopGui.getExplorerPerspective().activate();

        // Save the file
        hopGui.fileDelegate.fileSave();
      }
    } catch (Exception e) {
      new ErrorDialog(parent.getShell(), CONST_ERROR, "Error creating pipeline", e);
    }
  }

  /**
   * Open the specified file
   *
   * @param parent
   */
  private void openPipelineFile(Composite parent) {
    try {
      String filename = manager.getVariables().resolve(wFilename.getText());
      if (StringUtils.isNotEmpty(filename)) {
        hopGui.fileDelegate.fileOpen(filename);
      }
    } catch (Exception e) {
      new ErrorDialog(parent.getShell(), CONST_ERROR, "Error creating pipeline", e);
    }
  }

  private void selectPipelineFilename(Composite parent) {
    HopPipelineFileType<?> type = new HopPipelineFileType<>();
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
    WebService ws = getMetadata();

    wName.setText(Const.NVL(ws.getName(), ""));
    wEnabled.setSelection(ws.isEnabled());
    wFilename.setText(Const.NVL(ws.getFilename(), ""));
    wTransform.setText(Const.NVL(ws.getTransformName(), ""));
    wField.setText(Const.NVL(ws.getFieldName(), ""));
    wStatusCode.setText(Const.NVL(ws.getStatusCode(), ""));
    wContentType.setText(Const.NVL(ws.getContentType(), ""));
    wListStatus.setSelection(ws.isListingStatus());
    wBodyContentVariable.setText(Const.NVL(ws.getBodyContentVariable(), ""));
    wHeaderContentVariable.setText(Const.NVL(ws.getHeaderContentVariable(), ""));
    try {
      wRunConfiguration.fillItems();
      wRunConfiguration.setText(Const.NVL(ws.getRunConfigurationName(), ""));
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting workflow run configurations", e);
    }
  }

  @Override
  public void getWidgetsContent(WebService ws) {
    ws.setName(wName.getText());
    ws.setEnabled(wEnabled.getSelection());
    ws.setFilename(wFilename.getText());
    ws.setTransformName(wTransform.getText());
    ws.setFieldName(wField.getText());
    ws.setStatusCode(wStatusCode.getText());
    ws.setContentType(wContentType.getText());
    ws.setListingStatus(wListStatus.getSelection());
    ws.setBodyContentVariable(wBodyContentVariable.getText());
    ws.setHeaderContentVariable(wHeaderContentVariable.getText());
    ws.setRunConfigurationName(wRunConfiguration.getText());
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite composite) {
    Button wSelect = new Button(composite, SWT.PUSH);
    wSelect.setText(BaseMessages.getString(PKG, "WebServiceEditor.SelectOutput.Button"));
    wSelect.addListener(SWT.Selection, e -> selectOutputField());

    return new Button[] {wSelect};
  }

  private void selectOutputField() {
    IVariables variables = manager.getVariables();
    IHopMetadataProvider metadataProvider = manager.getMetadataProvider();

    String filename = variables.resolve(wFilename.getText());
    try {
      PipelineMeta pipelineMeta = new PipelineMeta(filename, metadataProvider, variables);

      EnterSelectionDialog selectTransformDialog =
          new EnterSelectionDialog(
              hopGui.getActiveShell(),
              pipelineMeta.getTransformNames(),
              "Select output transform",
              "Select the transform output for the web service");
      String transformName = selectTransformDialog.open();
      if (transformName == null) {
        return;
      }
      IRowMeta rowMeta = pipelineMeta.getTransformFields(variables, transformName);
      EnterSelectionDialog selectFieldDialog =
          new EnterSelectionDialog(
              hopGui.getActiveShell(),
              rowMeta.getFieldNames(),
              "Select the output field",
              "Select the field to use as output for this web service");
      String fieldName = selectFieldDialog.open();
      if (fieldName == null) {
        return;
      }
      wTransform.setText(transformName);
      wField.setText(fieldName);
    } catch (Exception e) {
      new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, "Error selecting output field", e);
    }
  }
}
