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

package org.apache.hop.reflection.probe.meta;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.reflection.probe.transform.PipelineDataProbeMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * Editor that allows you to change Pipeline Probe metadata
 *
 * @see PipelineProbe
 */
public class PipelineProbeEditor extends MetadataEditor<PipelineProbe> {
  private static final Class<?> PKG = PipelineProbeEditor.class; // For Translator

  private Text wName;
  private Button wEnabled;
  private TextVar wFilename;
  private TableView wSources;

  public PipelineProbeEditor(
      HopGui hopGui, MetadataManager<PipelineProbe> manager, PipelineProbe metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {

    PropsUi props = PropsUi.getInstance();

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Add listener to detect change after loading data
    ModifyListener lsMod = e -> setChanged();

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
    wlName.setText(BaseMessages.getString(PKG, "PipelineProbeEditor.Name.Label"));
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
    wlEnabled.setText(BaseMessages.getString(PKG, "PipelineProbeEditor.Enabled.Label"));
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
    wlFilename.setText(BaseMessages.getString(PKG, "PipelineProbeEditor.Filename.Label"));
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

    // The locations in a table view:
    //
    Label wlSources = new Label(parent, SWT.RIGHT);
    props.setLook(wlSources);
    wlSources.setText(BaseMessages.getString(PKG, "PipelineProbeEditor.Sources.Label"));
    FormData fdlSources = new FormData();
    fdlSources.left = new FormAttachment(0, 0);
    fdlSources.right = new FormAttachment(middle, 0);
    fdlSources.top = new FormAttachment(lastControl, 2 * margin);
    wlSources.setLayoutData(fdlSources);
    lastControl = wlSources;
    ColumnInfo[] columns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "PipelineProbeEditor.SourcesTable.Column.Pipeline"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "PipelineProbeEditor.SourcesTable.Column.Transform"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
    };
    wSources =
        new TableView(
            manager.getVariables(),
            parent,
            SWT.NONE,
            columns,
            metadata.getDataProbeLocations().size(),
            lsMod,
            props);
    FormData fdSources = new FormData();
    fdSources.left = new FormAttachment(0, 0);
    fdSources.top = new FormAttachment(lastControl, margin);
    fdSources.right = new FormAttachment(100, 0);
    fdSources.bottom = new FormAttachment(100, 0);
    wSources.setLayoutData(fdSources);

    setWidgetsContent();

    wName.addModifyListener(lsMod);
    wEnabled.addListener(SWT.Selection, e -> setChanged());
    wFilename.addModifyListener(lsMod);

    resetChanged();
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

      // Add a Pipeline Data Probe transform...
      //
      PipelineDataProbeMeta pipelineDataProbeMeta = new PipelineDataProbeMeta();
      pipelineDataProbeMeta.setLoggingTransforms(true);
      TransformMeta pipelineLogging =
          new TransformMeta("Pipeline Data Probe", pipelineDataProbeMeta);
      pipelineLogging.setLocation(200, 150);
      pipelineMeta.addTransform(pipelineLogging);

      // Add a dummy
      //
      DummyMeta dummyMeta = new DummyMeta();
      TransformMeta dummy = new TransformMeta("Process values here", dummyMeta);
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

  @Override
  public void setWidgetsContent() {
    PipelineProbe pl = getMetadata();

    wName.setText(Const.NVL(pl.getName(), ""));
    wEnabled.setSelection(pl.isEnabled());
    wFilename.setText(Const.NVL(pl.getPipelineFilename(), ""));

    wSources.removeAll();
    List<DataProbeLocation> locations = pl.getDataProbeLocations();
    for (DataProbeLocation location : locations) {
      TableItem item = new TableItem(wSources.table, SWT.NONE);
      item.setText(1, Const.NVL(location.getSourcePipelineFilename(), ""));
      item.setText(2, Const.NVL(location.getSourceTransformName(), ""));
    }
    wSources.setRowNums();
    wSources.optimizeTableView();
  }

  @Override
  public void getWidgetsContent(PipelineProbe pl) {
    pl.setName(wName.getText());
    pl.setEnabled(wEnabled.getSelection());
    pl.setPipelineFilename(wFilename.getText());
    List<DataProbeLocation> locations = new ArrayList<>();
    List<TableItem> items = wSources.getNonEmptyItems();
    for (TableItem item : items) {
      String filename = item.getText(1);
      String transformName = item.getText(2);
      locations.add(new DataProbeLocation(filename, transformName));
    }
    pl.setDataProbeLocations(locations);
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }
}
