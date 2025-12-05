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

package org.apache.hop.beam.gui;

import java.io.File;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.engines.dataflow.BeamDataFlowPipelineEngine;
import org.apache.hop.beam.pipeline.fatjar.FatJarBuilder;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.perspective.execution.ExecutionPerspective;
import org.apache.hop.ui.hopgui.perspective.execution.IExecutionViewer;
import org.apache.hop.ui.hopgui.perspective.execution.PipelineExecutionViewer;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;

@GuiPlugin
public class HopBeamGuiPlugin {

  public static final Class<?> PKG = HopBeamGuiPlugin.class; // i18n

  public static final String ID_MAIN_MENU_TOOLS_FAT_JAR = "40200-menu-tools-fat-jar";
  public static final String ID_MAIN_MENU_TOOLS_EXPORT_METADATA =
      "40210-menu-tools-export-metadata";
  public static final String TOOLBAR_ID_VISIT_GCP_DATAFLOW =
      "HopGuiPipelineGraph-ToolBar-10450-VisitGcpDataflow";
  public static final String TOOLBAR_ID_PIPELINE_EXECUTION_VIEWER_VISIT_GCP_DATAFLOW =
      "PipelineExecutionViewer-Toolbar-20000-VisitGcpDataflow";

  private static HopBeamGuiPlugin instance;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static HopBeamGuiPlugin getInstance() {
    if (instance == null) {
      instance = new HopBeamGuiPlugin();
    }
    return instance;
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_FAT_JAR,
      label = "i18n::BeamGuiPlugin.Menu.GenerateFatJar.Text",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      image = "beam-logo.svg",
      separator = true)
  public void menuToolsFatJar() {
    HopGui hopGui = HopGui.getInstance();
    final Shell shell = hopGui.getShell();

    MessageBox box = new MessageBox(shell, SWT.OK | SWT.CANCEL | SWT.ICON_INFORMATION);
    box.setText(BaseMessages.getString(PKG, "BeamGuiPlugin.GenerateFatJar.Dialog.Header"));
    box.setMessage(
        BaseMessages.getString(PKG, "BeamGuiPlugin.GenerateFatJar.Dialog.Message1")
            + Const.CR
            + BaseMessages.getString(PKG, "BeamGuiPlugin.GenerateFatJar.Dialog.Message2"));
    int answer = box.open();
    if ((answer & SWT.CANCEL) != 0) {
      return;
    }

    // Ask
    //
    String filename =
        BaseDialog.presentFileDialog(
            true,
            shell,
            new String[] {"*.jar", "*.*"},
            new String[] {
              BaseMessages.getString(PKG, "BeamGuiPlugin.FileTypes.Jars.Label"),
              BaseMessages.getString(PKG, "BeamGuiPlugin.FileTypes.All.Label")
            },
            true);
    if (filename == null) {
      return;
    }

    try {
      List<String> jarFilenames = findInstalledJarFilenames();

      IRunnableWithProgress op =
          monitor -> {
            try {
              monitor.setTaskName(
                  BaseMessages.getString(PKG, "BeamGuiPlugin.GenerateFatJar.Progress.Message"));
              FatJarBuilder fatJarBuilder =
                  new FatJarBuilder(hopGui.getLog(), hopGui.getVariables(), filename, jarFilenames);
              fatJarBuilder.setExtraTransformPluginClasses(null);
              fatJarBuilder.setExtraXpPluginClasses(null);
              fatJarBuilder.buildTargetJar();
              monitor.done();
            } catch (Exception e) {
              throw new InvocationTargetException(e, "Error building fat jar: " + e.getMessage());
            }
          };

      ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);
      pmd.run(false, op);

      GuiResource.getInstance().toClipboard(filename);

      box = new MessageBox(shell, SWT.CLOSE | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "BeamGuiPlugin.FatJarCreated.Dialog.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "BeamGuiPlugin.FatJarCreated.Dialog.Message1", filename)
              + Const.CR
              + BaseMessages.getString(PKG, "BeamGuiPlugin.FatJarCreated.Dialog.Message2"));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error creating fat jar", e);
    }
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_EXPORT_METADATA,
      label = "i18n::BeamGuiPlugin.Menu.ExportMetadata.Text",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      image = "beam-logo.svg",
      separator = true)
  public void menuToolsExportMetadata() {
    HopGui hopGui = HopGui.getInstance();
    final Shell shell = hopGui.getShell();

    MessageBox box = new MessageBox(shell, SWT.OK | SWT.CANCEL | SWT.ICON_INFORMATION);
    box.setText(BaseMessages.getString(PKG, "BeamGuiPlugin.ExportMetadata.Dialog.Header"));
    box.setMessage(BaseMessages.getString(PKG, "BeamGuiPlugin.ExportMetadata.Dialog.Message"));
    int answer = box.open();
    if ((answer & SWT.CANCEL) != 0) {
      return;
    }

    // Ask
    //
    String filename =
        BaseDialog.presentFileDialog(
            true,
            shell,
            new String[] {"*.json", "*.*"},
            new String[] {
              BaseMessages.getString(PKG, "BeamGuiPlugin.FileTypes.Json.Label"),
              BaseMessages.getString(PKG, "BeamGuiPlugin.FileTypes.All.Label")
            },
            true);
    if (filename == null) {
      return;
    }

    try {
      // Save HopGui metadata to JSON...
      //
      SerializableMetadataProvider metadataProvider =
          new SerializableMetadataProvider(hopGui.getMetadataProvider());
      String jsonString = metadataProvider.toJson();
      String realFilename = hopGui.getVariables().resolve(filename);

      try (OutputStream outputStream = HopVfs.getOutputStream(realFilename, false)) {
        outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error saving metadata to JSON file : " + filename, e);
    }
  }

  public static final List<String> findInstalledJarFilenames() {
    Set<File> jarFiles = new HashSet<>();
    jarFiles.addAll(FileUtils.listFiles(new File("lib"), new String[] {"jar"}, true));

    File libSwtFiles = new File("lib/swt/linux/x86_64");
    if (libSwtFiles.exists())
      jarFiles.addAll(FileUtils.listFiles(libSwtFiles, new String[] {"jar"}, true));

    jarFiles.addAll(FileUtils.listFiles(new File("plugins"), new String[] {"jar"}, true));

    // If we are in the hop-web Docker container, we need to import the Hop main JARs too for
    // Dataflow
    // (and for other runners probably too)
    File hopWebJars = new File("webapps/ROOT/WEB-INF/lib");
    if (hopWebJars.exists())
      jarFiles.addAll(FileUtils.listFiles(hopWebJars, new String[] {"jar"}, true));

    List<String> jarFilenames = new ArrayList<>();
    jarFiles.forEach(file -> jarFilenames.add(file.toString()));
    return jarFilenames;
  }

  /**
   * Add a toolbar item just above the pipeline graph to quickly visit the execution of a Dataflow
   * pipeline in the GCP console.
   */
  @GuiToolbarElement(
      root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ID_VISIT_GCP_DATAFLOW,
      toolTip = "i18n::BeamGuiPlugin.VisitDataflow.ToolTip",
      image = "dataflow.svg",
      separator = true)
  public void pipelineGraphVisitGcpDataflow() {
    DataflowPipelineJob dataflowPipelineJob = findDataflowPipelineJob();
    if (dataflowPipelineJob == null) {
      return;
    }

    String jobId = dataflowPipelineJob.getJobId();
    String projectId = dataflowPipelineJob.getProjectId();
    String region = dataflowPipelineJob.getRegion();

    openDataflowJobInConsole(jobId, projectId, region);
  }

  /**
   * Add a toolbar item just above the pipeline graph to quickly visit the execution of a Dataflow
   * pipeline in the GCP console.
   */
  @GuiToolbarElement(
      root = PipelineExecutionViewer.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ID_PIPELINE_EXECUTION_VIEWER_VISIT_GCP_DATAFLOW,
      toolTip = "i18n::BeamGuiPlugin.VisitDataflow.ToolTip",
      image = "dataflow.svg",
      separator = true)
  public void executionViewerVisitGcpDataflow() {
    ExecutionState executionState = findExecutionState();
    if (executionState != null) {
      String jobId =
          executionState.getDetails().get(BeamDataFlowPipelineEngine.DETAIL_DATAFLOW_JOB_ID);
      String projectId =
          executionState.getDetails().get(BeamDataFlowPipelineEngine.DETAIL_DATAFLOW_PROJECT_ID);
      String region =
          executionState.getDetails().get(BeamDataFlowPipelineEngine.DETAIL_DATAFLOW_REGION);

      if (StringUtils.isEmpty(jobId)
          || StringUtils.isEmpty(projectId)
          || StringUtils.isEmpty(region)) {
        return;
      }
      openDataflowJobInConsole(jobId, projectId, region);
    }
  }

  public static ExecutionState findExecutionState() {
    ExecutionPerspective perspective = HopGui.getExecutionPerspective();
    if (perspective == null) {
      return null;
    }
    IExecutionViewer activeViewer = perspective.getActiveViewer();
    if (activeViewer == null) {
      return null;
    }
    if (!(activeViewer instanceof PipelineExecutionViewer)) {
      return null;
    }

    PipelineExecutionViewer viewer = (PipelineExecutionViewer) activeViewer;

    return viewer.getExecutionState();
  }

  public void openDataflowJobInConsole(String jobId, String projectId, String region) {
    // https://console.cloud.google.com/dataflow/jobs/<region>/<id>;graphView=0?project=<projectId>
    //
    // example:
    //
    // https://console.cloud.google.com/dataflow/jobs/us-east1/2022-10-12_02_14_02-12614547583538213213;graphView=0?project=apache-hop
    //
    String url =
        "https://console.cloud.google.com/dataflow/jobs/"
            + region
            + "/"
            + jobId
            + ";graphView=0?project="
            + projectId;

    try {
      EnvironmentUtils.getInstance().openUrl(url);
    } catch (HopException e) {
      HopGui hopGui = HopGui.getInstance();
      final Shell shell = hopGui.getShell();
      MessageBox box = new MessageBox(shell, SWT.CLOSE | SWT.ICON_ERROR);
      box.setText(BaseMessages.getString(PKG, "BeamGuiPlugin.OpenDataflowJob.Dialog.Header"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "BeamGuiPlugin.OpenDataflowJob.Dialog.Message", url, e.getMessage()));
      box.open();
    }
  }

  public static DataflowPipelineJob findDataflowPipelineJob() {
    IHopFileTypeHandler typeHandler = HopGui.getInstance().getActiveFileTypeHandler();
    if (typeHandler == null) {
      return null;
    }
    if (!(typeHandler instanceof HopGuiPipelineGraph)) {
      return null;
    }

    HopGuiPipelineGraph graph = (HopGuiPipelineGraph) typeHandler;

    if (graph.getPipeline() == null) {
      return null;
    }
    if (!(graph.getPipeline() instanceof BeamDataFlowPipelineEngine)) {
      return null;
    }

    BeamDataFlowPipelineEngine pipeline = (BeamDataFlowPipelineEngine) graph.getPipeline();
    if (pipeline.getBeamPipelineResults() == null) {
      return null;
    }
    return (DataflowPipelineJob) pipeline.getBeamPipelineResults();
  }
}
