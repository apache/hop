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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.www.service.WebService;
import org.apache.http.entity.ContentType;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

/**
 * Editor that allows you to change Web Service metadata
 *
 * @see WebService
 */
public class WebServiceEditor extends MetadataEditor<WebService> {
  private static final Class<?> PKG = WebServiceEditor.class; // For Translator

  private Text wName;
  private Button wEnabled;
  private TextVar wFilename;
  private TextVar wTransform;
  private TextVar wField;
  private ComboVar wContentType;
  private Button wListStatus;

  private int middle;
  private int margin;

  public WebServiceEditor(HopGui hopGui, MetadataManager<WebService> manager, WebService metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {

    PropsUi props = PropsUi.getInstance();

    middle = props.getMiddlePct();
    margin = props.getMargin();

    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlIcon = new FormData();
    fdlIcon.top = new FormAttachment(0, 0);
    fdlIcon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlIcon);
    props.setLook(wIcon);

    // What's the name
    Label wlName = new Label(parent, SWT.RIGHT);
    props.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "WebServiceEditor.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(wIcon, margin);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, margin);
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
    props.setLook(wlEnabled);
    wlEnabled.setText(BaseMessages.getString(PKG, "WebServiceEditor.Enabled.Label"));
    FormData fdlEnabled = new FormData();
    fdlEnabled.left = new FormAttachment(0, 0);
    fdlEnabled.right = new FormAttachment(middle, 0);
    fdlEnabled.top = new FormAttachment(lastControl, margin);
    wlEnabled.setLayoutData(fdlEnabled);
    wEnabled = new Button(parent, SWT.CHECK | SWT.LEFT);
    props.setLook(wEnabled);
    FormData fdEnabled = new FormData();
    fdEnabled.left = new FormAttachment(middle, margin);
    fdEnabled.right = new FormAttachment(100, 0);
    fdEnabled.top = new FormAttachment(wlEnabled, 0, SWT.CENTER);
    wEnabled.setLayoutData(fdEnabled);
    lastControl = wlEnabled;

    Label wlFilename = new Label(parent, SWT.RIGHT);
    props.setLook(wlFilename);
    wlFilename.setText(BaseMessages.getString(PKG, "WebServiceEditor.Filename.Label"));
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, 0);
    fdlFilename.top = new FormAttachment(lastControl, 2 * margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbbFilename = new Button(parent, SWT.PUSH);
    props.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbbFilename = new FormData();
    fdbbFilename.right = new FormAttachment(100, 0);
    fdbbFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wbbFilename.setLayoutData(fdbbFilename);
    wbbFilename.addListener(
        SWT.Selection,
        e -> {
          selectPipelineFilename(parent);
        });

    Button wbnFilename = new Button(parent, SWT.PUSH);
    props.setLook(wbnFilename);
    wbnFilename.setText(BaseMessages.getString(PKG, "System.Button.New"));
    FormData fdbnFilename = new FormData();
    fdbnFilename.right = new FormAttachment(wbbFilename, -margin);
    fdbnFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wbnFilename.setLayoutData(fdbnFilename);
    wbnFilename.addListener(
        SWT.Selection,
        e -> {
          createPipelineFile(parent);
        });

    Button wboFilename = new Button(parent, SWT.PUSH);
    props.setLook(wboFilename);
    wboFilename.setText(BaseMessages.getString(PKG, "System.Button.Open"));
    FormData fdboFilename = new FormData();
    fdboFilename.right = new FormAttachment(wbnFilename, -margin);
    fdboFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wboFilename.setLayoutData(fdboFilename);
    wboFilename.addListener(
        SWT.Selection,
        e -> {
          openPipelineFile(parent);
        });

    wFilename = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, margin);
    fdFilename.right = new FormAttachment(wboFilename, -margin);
    fdFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wFilename.setLayoutData(fdFilename);
    lastControl = wlFilename;

    // Transform name
    //
    Label wlTransform = new Label(parent, SWT.RIGHT);
    props.setLook(wlTransform);
    wlTransform.setText(BaseMessages.getString(PKG, "WebServiceEditor.Transform.Label"));
    FormData fdlTransform = new FormData();
    fdlTransform.left = new FormAttachment(0, 0);
    fdlTransform.right = new FormAttachment(middle, 0);
    fdlTransform.top = new FormAttachment(lastControl, 2 * margin);
    wlTransform.setLayoutData(fdlTransform);
    wTransform = new TextVar(manager.getVariables(), parent, SWT.CHECK | SWT.LEFT);
    props.setLook(wTransform);
    FormData fdTransform = new FormData();
    fdTransform.left = new FormAttachment(middle, margin);
    fdTransform.right = new FormAttachment(100, 0);
    fdTransform.top = new FormAttachment(wlTransform, 0, SWT.CENTER);
    wTransform.setLayoutData(fdTransform);
    lastControl = wlTransform;

    // Transform name
    //
    Label wlField = new Label(parent, SWT.RIGHT);
    props.setLook(wlField);
    wlField.setText(BaseMessages.getString(PKG, "WebServiceEditor.Field.Label"));
    FormData fdlField = new FormData();
    fdlField.left = new FormAttachment(0, 0);
    fdlField.right = new FormAttachment(middle, 0);
    fdlField.top = new FormAttachment(lastControl, 2 * margin);
    wlField.setLayoutData(fdlField);
    wField = new TextVar(manager.getVariables(), parent, SWT.CHECK | SWT.LEFT);
    props.setLook(wField);
    FormData fdField = new FormData();
    fdField.left = new FormAttachment(middle, margin);
    fdField.right = new FormAttachment(100, 0);
    fdField.top = new FormAttachment(wlField, 0, SWT.CENTER);
    wField.setLayoutData(fdField);
    lastControl = wlField;

    // Content type
    //
    Label wlContentType = new Label(parent, SWT.RIGHT);
    props.setLook(wlContentType);
    wlContentType.setText(BaseMessages.getString(PKG, "WebServiceEditor.ContentType.Label"));
    FormData fdlContentType = new FormData();
    fdlContentType.left = new FormAttachment(0, 0);
    fdlContentType.right = new FormAttachment(middle, 0);
    fdlContentType.top = new FormAttachment(lastControl, 2 * margin);
    wlContentType.setLayoutData(fdlContentType);
    wContentType = new ComboVar(manager.getVariables(), parent, SWT.LEFT);
    props.setLook(wContentType);
    FormData fdContentType = new FormData();
    fdContentType.left = new FormAttachment(middle, margin);
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
    props.setLook(wlListStatus);
    wlListStatus.setText(BaseMessages.getString(PKG, "WebServiceEditor.ListStatus.Label"));
    FormData fdlListStatus = new FormData();
    fdlListStatus.left = new FormAttachment(0, 0);
    fdlListStatus.right = new FormAttachment(middle, 0);
    fdlListStatus.top = new FormAttachment(lastControl, margin);
    wlListStatus.setLayoutData(fdlListStatus);
    wListStatus = new Button(parent, SWT.CHECK | SWT.LEFT);
    props.setLook(wListStatus);
    FormData fdListStatus = new FormData();
    fdListStatus.left = new FormAttachment(middle, margin);
    fdListStatus.right = new FormAttachment(100, 0);
    fdListStatus.top = new FormAttachment(wlListStatus, 0, SWT.CENTER);
    wListStatus.setLayoutData(fdListStatus);
    lastControl = wlListStatus;
    
    // Add listener to detect change after loading data
    ModifyListener lsMod = e -> setChanged();
    wName.addModifyListener(lsMod);
    wEnabled.addListener(SWT.Selection, e -> setChanged());
    wFilename.addModifyListener(lsMod);
    wTransform.addModifyListener(lsMod);
    wField.addModifyListener(lsMod);
    wContentType.addModifyListener(lsMod);
    wListStatus.addListener(SWT.Selection, e -> setChanged());

    setWidgetsContent();
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

        HopDataOrchestrationPerspective perspective = HopGui.getDataOrchestrationPerspective();

        // Switch to the perspective
        //
        perspective.activate();

        // Open it in the Hop GUI
        //
        HopGui.getDataOrchestrationPerspective().addPipeline(hopGui, pipelineMeta, type);

        // Save the file
        hopGui.fileDelegate.fileSave();
      }
    } catch (Exception e) {
      new ErrorDialog(parent.getShell(), "Error", "Error creating pipeline", e);
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
      new ErrorDialog(parent.getShell(), "Error", "Error creating pipeline", e);
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
    wContentType.setText(Const.NVL(ws.getContentType(), ""));
    wListStatus.setSelection(ws.isListingStatus());

  }

  @Override
  public void getWidgetsContent(WebService ws) {
    ws.setName(wName.getText());
    ws.setEnabled(wEnabled.getSelection());
    ws.setFilename(wFilename.getText());
    ws.setTransformName(wTransform.getText());
    ws.setFieldName(wField.getText());
    ws.setContentType(wContentType.getText());
    ws.setListingStatus(wListStatus.getSelection());
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
      PipelineMeta pipelineMeta = new PipelineMeta(filename, metadataProvider, true, variables);

      EnterSelectionDialog selectTransformDialog = new EnterSelectionDialog( hopGui.getShell(), pipelineMeta.getTransformNames(), "Select output transform", "Select the transform output for the web service");
      String transformName = selectTransformDialog.open();
      if (transformName==null) {
        return;
      }
      IRowMeta rowMeta = pipelineMeta.getTransformFields( variables, transformName );
      EnterSelectionDialog selectFieldDialog = new EnterSelectionDialog( hopGui.getShell(), rowMeta.getFieldNames(), "Select the output field", "Select the field to use as output for this web service" );
      String fieldName = selectFieldDialog.open();
      if (fieldName==null) {
        return;
      }
      wTransform.setText( transformName );
      wField.setText( fieldName );
    } catch (Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error selecting output field", e);
    }
  }
}
