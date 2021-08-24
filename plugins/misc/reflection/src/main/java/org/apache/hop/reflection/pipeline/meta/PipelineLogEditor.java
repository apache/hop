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

package org.apache.hop.reflection.pipeline.meta;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.reflection.pipeline.transform.PipelineLoggingMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.*;

/**
 * Editor that allows you to change Pipeline Log metadata
 *
 * @see PipelineLog
 */
public class PipelineLogEditor extends MetadataEditor<PipelineLog> {
  private static final Class<?> PKG = PipelineLogEditor.class; // For Translator

  private Text wName;
  private Button wEnabled;
  private Button wLoggingParentsOnly;
  private TextVar wFilename;
  private Button wAtStart;
  private Button wAtEnd;
  private Button wPeriodic;
  private Label wlInterval;
  private TextVar wInterval;

  private int middle;
  private int margin;

  public PipelineLogEditor(
      HopGui hopGui, MetadataManager<PipelineLog> manager, PipelineLog metadata) {
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
    wlName.setText(BaseMessages.getString(PKG, "PipelineLoggingEditor.Name.Label"));
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
    wlEnabled.setText(BaseMessages.getString(PKG, "PipelineLoggingEditor.Enabled.Label"));
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

    // LoggingParentsOnly?
    //
    Label wlLoggingParentsOnly = new Label(parent, SWT.RIGHT);
    props.setLook(wlLoggingParentsOnly);
    wlLoggingParentsOnly.setText(
        BaseMessages.getString(PKG, "PipelineLoggingEditor.LoggingParentsOnly.Label"));
    FormData fdlLoggingParentsOnly = new FormData();
    fdlLoggingParentsOnly.left = new FormAttachment(0, 0);
    fdlLoggingParentsOnly.right = new FormAttachment(middle, 0);
    fdlLoggingParentsOnly.top = new FormAttachment(lastControl, 2 * margin);
    wlLoggingParentsOnly.setLayoutData(fdlLoggingParentsOnly);
    wLoggingParentsOnly = new Button(parent, SWT.CHECK | SWT.LEFT);
    props.setLook(wLoggingParentsOnly);
    FormData fdLoggingParentsOnly = new FormData();
    fdLoggingParentsOnly.left = new FormAttachment(middle, margin);
    fdLoggingParentsOnly.right = new FormAttachment(100, 0);
    fdLoggingParentsOnly.top = new FormAttachment(wlLoggingParentsOnly, 0, SWT.CENTER);
    wLoggingParentsOnly.setLayoutData(fdLoggingParentsOnly);
    lastControl = wlLoggingParentsOnly;

    Label wlFilename = new Label(parent, SWT.RIGHT);
    props.setLook(wlFilename);
    wlFilename.setText(BaseMessages.getString(PKG, "PipelineLoggingEditor.Filename.Label"));
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

    // Execute at start
    //
    Label wlAtStart = new Label(parent, SWT.RIGHT);
    props.setLook(wlAtStart);
    wlAtStart.setText(BaseMessages.getString(PKG, "PipelineLoggingEditor.AtStart.Label"));
    FormData fdlAtStart = new FormData();
    fdlAtStart.left = new FormAttachment(0, 0);
    fdlAtStart.right = new FormAttachment(middle, 0);
    fdlAtStart.top = new FormAttachment(lastControl, margin);
    wlAtStart.setLayoutData(fdlAtStart);
    wAtStart = new Button(parent, SWT.CHECK | SWT.LEFT);
    props.setLook(wAtStart);
    FormData fdAtStart = new FormData();
    fdAtStart.left = new FormAttachment(middle, margin);
    fdAtStart.right = new FormAttachment(100, 0);
    fdAtStart.top = new FormAttachment(wlAtStart, 0, SWT.CENTER);
    wAtStart.setLayoutData(fdAtStart);
    lastControl = wlAtStart;

    // Execute at end
    //
    Label wlAtEnd = new Label(parent, SWT.RIGHT);
    props.setLook(wlAtEnd);
    wlAtEnd.setText(BaseMessages.getString(PKG, "PipelineLoggingEditor.AtEnd.Label"));
    FormData fdlAtEnd = new FormData();
    fdlAtEnd.left = new FormAttachment(0, 0);
    fdlAtEnd.right = new FormAttachment(middle, 0);
    fdlAtEnd.top = new FormAttachment(lastControl, 2 * margin);
    wlAtEnd.setLayoutData(fdlAtEnd);
    wAtEnd = new Button(parent, SWT.CHECK | SWT.LEFT);
    props.setLook(wAtEnd);
    FormData fdAtEnd = new FormData();
    fdAtEnd.left = new FormAttachment(middle, margin);
    fdAtEnd.right = new FormAttachment(100, 0);
    fdAtEnd.top = new FormAttachment(wlAtEnd, 0, SWT.CENTER);
    wAtEnd.setLayoutData(fdAtEnd);
    lastControl = wlAtEnd;

    // Execute periodically
    //
    Label wlPeriodic = new Label(parent, SWT.RIGHT);
    props.setLook(wlPeriodic);
    wlPeriodic.setText(BaseMessages.getString(PKG, "PipelineLoggingEditor.Periodic.Label"));
    FormData fdlPeriodic = new FormData();
    fdlPeriodic.left = new FormAttachment(0, 0);
    fdlPeriodic.right = new FormAttachment(middle, 0);
    fdlPeriodic.top = new FormAttachment(lastControl, 2 * margin);
    wlPeriodic.setLayoutData(fdlPeriodic);
    wPeriodic = new Button(parent, SWT.CHECK | SWT.LEFT);
    props.setLook(wPeriodic);
    FormData fdPeriodic = new FormData();
    fdPeriodic.left = new FormAttachment(middle, margin);
    fdPeriodic.right = new FormAttachment(100, 0);
    fdPeriodic.top = new FormAttachment(wlPeriodic, 0, SWT.CENTER);
    wPeriodic.setLayoutData(fdPeriodic);
    lastControl = wlPeriodic;

    // Execute periodically
    //
    wlInterval = new Label(parent, SWT.RIGHT);
    props.setLook(wlInterval);
    wlInterval.setText(BaseMessages.getString(PKG, "PipelineLoggingEditor.Interval.Label"));
    FormData fdlInterval = new FormData();
    fdlInterval.left = new FormAttachment(0, 0);
    fdlInterval.right = new FormAttachment(middle, 0);
    fdlInterval.top = new FormAttachment(lastControl, 2 * margin);
    wlInterval.setLayoutData(fdlInterval);
    wInterval = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    props.setLook(wInterval);
    FormData fdInterval = new FormData();
    fdInterval.left = new FormAttachment(middle, margin);
    fdInterval.right = new FormAttachment(100, 0);
    fdInterval.top = new FormAttachment(wlInterval, 0, SWT.CENTER);
    wInterval.setLayoutData(fdInterval);
    wInterval.addListener(SWT.Selection, this::enableFields);
    lastControl = wlInterval;

    setWidgetsContent();

    // Add listener to detect change after loading data
    Listener modifyListener = e -> setChanged();
    wName.addListener(SWT.Modify, modifyListener);
    wEnabled.addListener(SWT.Selection, modifyListener);
    wLoggingParentsOnly.addListener(SWT.Selection, modifyListener);
    wFilename.addListener(SWT.Modify, modifyListener);
    wAtStart.addListener(SWT.Selection, modifyListener);
    wAtEnd.addListener(SWT.Selection, modifyListener);
    wPeriodic.addListener(SWT.Selection, modifyListener);
    wInterval.addListener(SWT.Modify, modifyListener);
  }

  /**
   * Create a new pipeline file: ask the user for a name. Add a standard transform and a dummy to
   * show how it works.
   *
   * @param parent
   */
  private void createPipelineFile(Composite parent) {
    try {
      PipelineMeta pipelineMeta = new PipelineMeta();

      // Add a Pipeline Logging transform...
      //
      PipelineLoggingMeta pipelineLoggingMeta = new PipelineLoggingMeta();
      pipelineLoggingMeta.setLoggingTransforms(true);
      TransformMeta pipelineLogging = new TransformMeta("Pipeline Logging", pipelineLoggingMeta);
      pipelineLogging.setLocation(200, 150);
      pipelineMeta.addTransform(pipelineLogging);

      // Add a dummy
      //
      DummyMeta dummyMeta = new DummyMeta();
      TransformMeta dummy = new TransformMeta("Save logging here", dummyMeta);
      dummy.setLocation(500, 150);
      pipelineMeta.addTransform(dummy);

      // Add a hop between both transforms...
      //
      pipelineMeta.addPipelineHop(new PipelineHopMeta(pipelineLogging, dummy));

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

  private void enableFields(Event event) {
    boolean intervalEnabled = wPeriodic.getSelection();
    wlInterval.setEnabled(intervalEnabled);
    wInterval.setEnabled(intervalEnabled);
  }

  @Override
  public void setWidgetsContent() {
    PipelineLog pl = getMetadata();

    wName.setText(Const.NVL(pl.getName(), ""));
    wEnabled.setSelection(pl.isEnabled());
    wLoggingParentsOnly.setSelection(pl.isLoggingParentsOnly());
    wFilename.setText(Const.NVL(pl.getPipelineFilename(), ""));
    wAtStart.setSelection(pl.isExecutingAtStart());
    wAtEnd.setSelection(pl.isExecutingAtEnd());
    wPeriodic.setSelection(pl.isExecutingPeriodically());
    wInterval.setText(Const.NVL(pl.getIntervalInSeconds(), ""));
  }

  @Override
  public void getWidgetsContent(PipelineLog pl) {
    pl.setName(wName.getText());
    pl.setEnabled(wEnabled.getSelection());
    pl.setLoggingParentsOnly(wLoggingParentsOnly.getSelection());
    pl.setPipelineFilename(wFilename.getText());
    pl.setExecutingAtStart(wAtStart.getSelection());
    pl.setExecutingAtEnd(wAtEnd.getSelection());
    pl.setExecutingPeriodically(wPeriodic.getSelection());
    pl.setIntervalInSeconds(wInterval.getText());
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }
}
