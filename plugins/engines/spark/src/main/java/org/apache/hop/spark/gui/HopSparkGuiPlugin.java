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

package org.apache.hop.spark.gui;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.pkg.SparkProjectPackage;
import org.apache.hop.spark.util.SparkRunMode;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

/**
 * GUI actions for the Native Spark engine, including project package export for cluster execution
 * and per-transform Spark run-mode overrides.
 */
@GuiPlugin
public class HopSparkGuiPlugin {

  public static final Class<?> PKG = HopSparkGuiPlugin.class;

  public static final String ID_MAIN_MENU_TOOLS_EXPORT_SPARK_PROJECT =
      "40300-menu-tools-export-spark-project-package";

  public static final String ID_MAIN_MENU_TOOLS_SPARK_PACKAGE_CONFIG =
      "40310-menu-tools-spark-package-config";

  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_SPARK_RUN_MODE =
      "pipeline-graph-transform-spark-run-mode";

  private static HopSparkGuiPlugin instance;

  public static HopSparkGuiPlugin getInstance() {
    if (instance == null) {
      instance = new HopSparkGuiPlugin();
    }
    return instance;
  }

  /**
   * Per-transform override of generic mapPartitions run mode (Inherit / Force distributed / Force
   * Driver Only). Stored on {@link TransformMeta} attributes; effective when running on Native
   * Spark.
   */
  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_SPARK_RUN_MODE,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::SparkGuiPlugin.ContextAction.SparkRunMode.Name",
      tooltip = "i18n::SparkGuiPlugin.ContextAction.SparkRunMode.Tooltip",
      image = "spark-run-driver.svg",
      category = "i18n::SparkGuiPlugin.ContextAction.Category",
      categoryOrder = "9")
  public void setSparkRunMode(HopGuiPipelineTransformContext context) {
    HopGui hopGui = HopGui.getInstance();
    try {
      TransformMeta transformMeta = context.getTransformMeta();
      PipelineMeta pipelineMeta = context.getPipelineMeta();

      String currentLabel =
          SparkRunMode.displayLabelForOverride(SparkRunMode.getOverride(transformMeta));
      String[] labels = SparkRunMode.overrideDisplayLabels();
      int preselect = 0;
      for (int i = 0; i < labels.length; i++) {
        if (labels[i].equals(currentLabel)) {
          preselect = i;
          break;
        }
      }
      EnterSelectionDialog dialog =
          new EnterSelectionDialog(
              hopGui.getShell(),
              labels,
              BaseMessages.getString(PKG, "SparkGuiPlugin.ContextAction.SparkRunMode.Dialog.Title"),
              BaseMessages.getString(
                  PKG,
                  "SparkGuiPlugin.ContextAction.SparkRunMode.Dialog.Message",
                  transformMeta.getName(),
                  currentLabel));
      dialog.setAvoidQuickSearch();
      String choice = dialog.open(preselect);
      if (choice == null) {
        return;
      }
      SparkRunMode.setOverride(transformMeta, SparkRunMode.overrideFromDisplayLabel(choice));
      transformMeta.setChanged();
      pipelineMeta.setChanged();
      context.getPipelineGraph().updateGui();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "SparkGuiPlugin.ContextAction.SparkRunMode.Error.Header"),
          BaseMessages.getString(PKG, "SparkGuiPlugin.ContextAction.SparkRunMode.Error.Message"),
          e);
    }
  }

  /** Edit {@code spark-package.json} include/exclude rules for Native Spark project packages. */
  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_SPARK_PACKAGE_CONFIG,
      label = "i18n::SparkGuiPlugin.Menu.SparkPackageConfig.Text",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      image = "spark-file-input.svg",
      separator = true)
  public void menuToolsSparkPackageConfig() {
    HopGui hopGui = HopGui.getInstance();
    Shell shell = hopGui.getShell();
    IVariables variables = hopGui.getVariables();
    String projectHome = variables.getVariable("PROJECT_HOME");
    if (StringUtils.isEmpty(projectHome)) {
      MessageBox box = new MessageBox(shell, SWT.CLOSE | SWT.ICON_WARNING);
      box.setText(
          BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.NoProject.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.NoProject.Message"));
      box.open();
      return;
    }
    projectHome = variables.resolve(projectHome);
    new SparkPackageConfigDialog(shell, projectHome).open();
  }

  /**
   * Export the active project as a zip for <strong>Native Spark</strong> execution (MainSpark
   * {@code --HopProjectPackage}). Not the same as File → Export current project to zip.
   */
  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_EXPORT_SPARK_PROJECT,
      label = "i18n::SparkGuiPlugin.Menu.ExportSparkProjectPackage.Text",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      image = "spark-file-input.svg")
  public void menuToolsExportSparkProjectPackage() {
    HopGui hopGui = HopGui.getInstance();
    Shell shell = hopGui.getShell();
    IVariables variables = hopGui.getVariables();

    MessageBox intro = new MessageBox(shell, SWT.OK | SWT.CANCEL | SWT.ICON_INFORMATION);
    intro.setText(BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.Dialog.Header"));
    intro.setMessage(
        BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.Dialog.Message1")
            + Const.CR
            + Const.CR
            + BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.Dialog.Message2")
            + Const.CR
            + Const.CR
            + BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.Dialog.Message3"));
    if ((intro.open() & SWT.CANCEL) != 0) {
      return;
    }

    String projectHome = variables.getVariable("PROJECT_HOME");
    if (StringUtils.isEmpty(projectHome)) {
      MessageBox box = new MessageBox(shell, SWT.CLOSE | SWT.ICON_WARNING);
      box.setText(
          BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.NoProject.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.NoProject.Message"));
      box.open();
      return;
    }

    String zipFilename =
        BaseDialog.presentFileDialog(
            true,
            shell,
            new String[] {"*.zip", "*.*"},
            new String[] {
              BaseMessages.getString(PKG, "SparkGuiPlugin.FileTypes.Zip.Label"),
              BaseMessages.getString(PKG, "SparkGuiPlugin.FileTypes.All.Label")
            },
            true);
    if (zipFilename == null) {
      return;
    }

    try {
      SparkProjectPackage.exportProject(
          projectHome, zipFilename, hopGui.getMetadataProvider(), variables);

      GuiResource.getInstance().toClipboard(zipFilename);

      MessageBox done = new MessageBox(shell, SWT.CLOSE | SWT.ICON_INFORMATION);
      done.setText(BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.Done.Header"));
      done.setMessage(
          BaseMessages.getString(
                  PKG, "SparkGuiPlugin.ExportSparkProject.Done.Message1", zipFilename)
              + Const.CR
              + Const.CR
              + BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.Done.Message2")
              + Const.CR
              + BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.Done.Message3"));
      done.open();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.Error.Header"),
          BaseMessages.getString(PKG, "SparkGuiPlugin.ExportSparkProject.Error.Message"),
          e);
    }
  }
}
