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
import org.apache.commons.lang3.StringUtils;
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

  /**
   * Collect jar files from the current Hop installation for fat-jar generation.
   *
   * <p>Uses the default Spark client pack ({@code lib/spark-client}), or the versioned pack
   * selected by system property / env {@code HOP_SPARK_CLIENT_VERSION} under {@code
   * lib/spark-clients/<version>}. Versioned packs under {@code lib/spark-clients/} are never all
   * included at once.
   */
  public static final List<String> findInstalledJarFilenames() {
    return findInstalledJarFilenames(resolveSparkClientVersion(null));
  }

  /**
   * @param sparkClientVersion Spark client pack version (e.g. {@code 3.5.8}), or null/blank for the
   *     default pack at {@code lib/spark-client}
   */
  public static final List<String> findInstalledJarFilenames(String sparkClientVersion) {
    Set<File> jarFiles = new HashSet<>();
    boolean versionedPack = StringUtils.isNotBlank(sparkClientVersion);

    // lib/ tree except spark client pack directories (handled separately below)
    collectJarsExcludingSparkClientPacks(new File("lib"), jarFiles, versionedPack);

    File libSwtFiles = new File("lib/swt/linux/x86_64");
    if (libSwtFiles.exists()) {
      jarFiles.addAll(FileUtils.listFiles(libSwtFiles, new String[] {"jar"}, true));
    }

    File pluginsDir = new File("plugins");
    if (pluginsDir.isDirectory()) {
      jarFiles.addAll(FileUtils.listFiles(pluginsDir, new String[] {"jar"}, true));
    }

    // If we are in the hop-web Docker container, we need to import the Hop main JARs too for
    // Dataflow (and for other runners probably too)
    File hopWebJars = new File("webapps/ROOT/WEB-INF/lib");
    if (hopWebJars.exists()) {
      jarFiles.addAll(FileUtils.listFiles(hopWebJars, new String[] {"jar"}, true));
    }

    // Selected Spark client pack only (never all versioned packs at once)
    File sparkClientDir = resolveSparkClientPackDir(sparkClientVersion);
    if (sparkClientDir.isDirectory()) {
      jarFiles.addAll(FileUtils.listFiles(sparkClientDir, new String[] {"jar"}, false));
    }

    // Final guard: when using a versioned pack, drop any spark-* jar outside that pack
    // (avoids Beam fragments under lib/beam mixing serialVersionUID / version-info).
    if (versionedPack) {
      String packPath;
      try {
        packPath = sparkClientDir.getCanonicalPath();
      } catch (Exception e) {
        packPath = sparkClientDir.getAbsolutePath();
      }
      final String packPrefix = packPath;
      jarFiles.removeIf(
          f -> {
            String n = f.getName();
            // Spark distribution jars are named spark-*.jar; Beam's runner is beam-runners-spark-*
            if (!n.startsWith("spark-") || !n.endsWith(".jar")) {
              return false;
            }
            try {
              return !f.getCanonicalPath().startsWith(packPrefix);
            } catch (Exception e) {
              return !f.getAbsolutePath().startsWith(packPrefix);
            }
          });
    }

    List<String> jarFilenames = new ArrayList<>();
    jarFiles.forEach(file -> jarFilenames.add(file.toString()));
    return jarFilenames;
  }

  /**
   * Resolve which Spark client pack version to use. Explicit argument wins, then system property
   * {@code HOP_SPARK_CLIENT_VERSION}, then env of the same name.
   */
  public static String resolveSparkClientVersion(String explicitVersion) {
    if (StringUtils.isNotBlank(explicitVersion)) {
      return explicitVersion.trim();
    }
    String prop = System.getProperty("HOP_SPARK_CLIENT_VERSION");
    if (StringUtils.isNotBlank(prop)) {
      return prop.trim();
    }
    String env = System.getenv("HOP_SPARK_CLIENT_VERSION");
    if (StringUtils.isNotBlank(env)) {
      return env.trim();
    }
    return null;
  }

  /** Directory for the default pack or {@code lib/spark-clients/<version>}. */
  public static File resolveSparkClientPackDir(String sparkClientVersion) {
    if (StringUtils.isBlank(sparkClientVersion)) {
      return new File("lib/spark-client");
    }
    return new File("lib/spark-clients/" + sparkClientVersion.trim());
  }

  /**
   * @param versionedPack when true, also skip {@code spark-*.jar} under {@code lib/beam} so Beam's
   *     fixed Spark fragments cannot mix with an alternate client pack
   */
  private static void collectJarsExcludingSparkClientPacks(
      File dir, Set<File> jarFiles, boolean versionedPack) {
    if (dir == null || !dir.isDirectory()) {
      return;
    }
    String name = dir.getName();
    File parent = dir.getParentFile();
    // Skip lib/spark-client and lib/spark-clients entirely
    if (parent != null
        && "lib".equals(parent.getName())
        && ("spark-client".equals(name) || "spark-clients".equals(name))) {
      return;
    }
    File[] children = dir.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        collectJarsExcludingSparkClientPacks(child, jarFiles, versionedPack);
      } else if (child.isFile() && child.getName().endsWith(".jar")) {
        // Versioned packs own all spark-* jars; drop Beam's spark fragments from lib/beam
        if (versionedPack
            && child.getName().startsWith("spark-")
            && parent != null
            && "beam".equals(parent.getName())) {
          continue;
        }
        jarFiles.add(child);
      }
    }
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
