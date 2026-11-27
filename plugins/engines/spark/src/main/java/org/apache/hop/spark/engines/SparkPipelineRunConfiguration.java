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

package org.apache.hop.spark.engines;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;
import org.apache.hop.spark.engines.check.SparkConfigurationTestDialog;
import org.apache.hop.spark.engines.template.SparkRunConfigTemplate;
import org.apache.hop.spark.util.SparkConst;
import org.apache.hop.spark.util.SparkRunMode;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

@GuiPlugin
@Getter
@Setter
public class SparkPipelineRunConfiguration extends EmptyPipelineRunConfiguration
    implements ISparkPipelineEngineRunConfiguration, Cloneable {

  private static final Class<?> PKG = SparkPipelineRunConfiguration.class;

  public static final String WIDGET_ID_CLUSTER_TYPE = "clusterType";
  public static final String WIDGET_ID_MASTER = "sparkMaster";
  public static final String WIDGET_ID_DEPLOY_MODE = "deployMode";
  public static final String WIDGET_ID_DATABRICKS_CONNECTION = "databricksConnection";
  public static final String WIDGET_ID_DATABRICKS_COMPUTE = "databricksCompute";
  public static final String WIDGET_ID_DATABRICKS_RUNTIME = "databricksRuntime";
  public static final String WIDGET_ID_DATABRICKS_NODE_TYPE = "databricksNodeType";
  public static final String WIDGET_ID_DATABRICKS_WORKERS = "databricksWorkers";

  /**
   * Fill the form from a named deployment template (local, standalone, submit, Databricks, …).
   * Mutates {@code object} (the live editor model); widgets are refreshed by GuiCompositeWidgets.
   */
  @GuiWidgetElement(
      id = "load-spark-config-template",
      order = "10010",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::SparkEngine.LoadTemplate.Label",
      toolTip = "i18n::SparkEngine.LoadTemplate.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.General",
      groupOrder = "10")
  public void loadConfigurationTemplate(Object object) {
    if (!(object instanceof SparkPipelineRunConfiguration config)) {
      return;
    }
    Shell shell = HopGui.getInstance().getShell();
    String[] labels = SparkRunConfigTemplate.displayNames();
    EnterSelectionDialog dialog =
        new EnterSelectionDialog(
            shell,
            labels,
            BaseMessages.getString(PKG, "SparkEngine.LoadTemplate.Dialog.Title"),
            BaseMessages.getString(PKG, "SparkEngine.LoadTemplate.Dialog.Message"));
    dialog.setAvoidQuickSearch();
    String choice = dialog.open();
    if (choice == null) {
      return;
    }
    SparkRunConfigTemplate template = SparkRunConfigTemplate.fromDisplayName(choice);
    if (template == null) {
      return;
    }
    if (SparkRunConfigTemplate.looksCustomized(config)) {
      MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      box.setText(BaseMessages.getString(PKG, "SparkEngine.LoadTemplate.Confirm.Title"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "SparkEngine.LoadTemplate.Confirm.Message", template.getDisplayName()));
      if (box.open() != SWT.YES) {
        return;
      }
    }
    template.applyTo(config);
  }

  /** Runs the checks of this configuration from the Hop machine and lists what they found. */
  @GuiWidgetElement(
      id = "test-spark-configuration",
      order = "10020",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::SparkEngine.TestConfiguration.Label",
      toolTip = "i18n::SparkEngine.TestConfiguration.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.General",
      groupOrder = "10")
  public void testConfiguration(Object object) {
    if (object instanceof SparkPipelineRunConfiguration config) {
      SparkConfigurationTestDialog.open(
          HopGui.getInstance().getShell(),
          config,
          HopGui.getInstance().getVariables(),
          HopGui.getInstance().getMetadataProvider(),
          false);
    }
  }

  /**
   * Runs the checks, and then a small pipeline on the cluster: 1000 rows through a Hop transform on
   * the executors. It uses the cluster; the plain test doesn't.
   */
  @GuiWidgetElement(
      id = "test-spark-configuration-with-job",
      order = "10030",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::SparkEngine.TestConfigurationWithJob.Label",
      toolTip = "i18n::SparkEngine.TestConfigurationWithJob.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.General",
      groupOrder = "10")
  public void testConfigurationWithJob(Object object) {
    if (object instanceof SparkPipelineRunConfiguration config) {
      SparkConfigurationTestDialog.open(
          HopGui.getInstance().getShell(),
          config,
          HopGui.getInstance().getVariables(),
          HopGui.getInstance().getMetadataProvider());
    }
  }

  /**
   * Where the pipeline runs: it decides which options apply and which checks the test runs. Null in
   * a run configuration saved before it existed; {@link #getEffectiveClusterType()} then takes it
   * from the Spark master.
   */
  @GuiWidgetElement(
      id = WIDGET_ID_CLUSTER_TYPE,
      order = "10050",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::SparkEngine.OptionsClusterType.Label",
      toolTip = "i18n::SparkEngine.OptionsClusterType.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.General",
      groupOrder = "10")
  @HopMetadataProperty(key = "cluster_type", storeWithCode = true)
  private SparkClusterType clusterType;

  @GuiWidgetElement(
      order = "10100",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsMaster.Label",
      toolTip = "i18n::SparkEngine.OptionsMaster.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.General",
      groupOrder = "10")
  @HopMetadataProperty
  private String sparkMaster;

  @GuiWidgetElement(
      order = "10300",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsAppName.Label",
      toolTip = "i18n::SparkEngine.OptionsAppName.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.General",
      groupOrder = "10")
  @HopMetadataProperty
  private String sparkAppName;

  @GuiWidgetElement(
      order = "20100",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::SparkEngine.OptionsFatJar.Label",
      toolTip = "i18n::SparkEngine.OptionsFatJar.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Cluster",
      groupOrder = "20")
  @HopMetadataProperty
  private String fatJar;

  @GuiWidgetElement(
      order = "20300",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label = "i18n::SparkEngine.OptionsHadoopConfDir.Label",
      toolTip = "i18n::SparkEngine.OptionsHadoopConfDir.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Cluster",
      groupOrder = "20")
  @HopMetadataProperty
  private String hadoopConfDir;

  /**
   * The Spark settings as {@code key=value} lines: how they are stored, so that Hop versions from
   * before the table keep reading them. The table is a view of these lines.
   */
  @HopMetadataProperty(key = "sparkConfigs")
  private String storedSparkConfigs;

  /**
   * Spark settings: {@code spark.*} keys and their values, as spark-submit --conf takes them. Not
   * stored itself: read from {@link #storedSparkConfigs} when first asked for, null until then.
   */
  @GuiWidgetElement(
      id = "sparkSettings",
      order = "35100",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TABLE,
      label = "i18n::SparkEngine.OptionsSparkSettings.Label",
      toolTip = "i18n::SparkEngine.OptionsSparkSettings.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.SparkSettings",
      groupOrder = "35",
      tableRows = 10)
  private List<SparkSetting> sparkSettings;

  @GuiWidgetElement(
      order = "30100",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsDriverMemory.Label",
      toolTip = "i18n::SparkEngine.OptionsDriverMemory.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Resources",
      groupOrder = "30")
  @HopMetadataProperty
  private String driverMemory;

  @GuiWidgetElement(
      order = "30200",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsExecutorMemory.Label",
      toolTip = "i18n::SparkEngine.OptionsExecutorMemory.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Resources",
      groupOrder = "30")
  @HopMetadataProperty
  private String executorMemory;

  @GuiWidgetElement(
      order = "30300",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsExecutorCores.Label",
      toolTip = "i18n::SparkEngine.OptionsExecutorCores.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Resources",
      groupOrder = "30")
  @HopMetadataProperty
  private String executorCores;

  /**
   * The path scheme map as {@code from=to} lines: how it is stored, so that Hop versions from
   * before the table keep reading it. The table is a view of these lines.
   */
  @HopMetadataProperty(key = "pathSchemeMap")
  private String storedPathSchemeMap;

  /**
   * URI schemes the Spark File and Lake Table transforms rewrite before Spark gets the path. Not
   * stored itself: read from {@link #storedPathSchemeMap} when first asked for, null until then.
   */
  @GuiWidgetElement(
      id = "pathSchemes",
      order = "35200",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TABLE,
      label = "i18n::SparkEngine.OptionsPathSchemes.Label",
      toolTip = "i18n::SparkEngine.OptionsPathSchemeMap.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.SparkSettings",
      groupOrder = "35",
      tableRows = 3)
  private List<PathSchemeMapping> pathSchemes;

  /**
   * Default for generic mapPartitions transforms ({@link SparkRunMode#DISTRIBUTED} or {@link
   * SparkRunMode#DRIVER_ONLY}). Overridable per transform via the "Spark Run Mode" context action.
   */
  @GuiWidgetElement(
      id = "genericTransformRunMode",
      order = "50100",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      label = "i18n::SparkEngine.OptionsGenericRunMode.Label",
      toolTip = "i18n::SparkEngine.OptionsGenericRunMode.ToolTip",
      comboValuesMethod = "getGenericTransformRunModes",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.HopOnSpark",
      groupOrder = "50")
  @HopMetadataProperty(key = "generic_transform_run_mode")
  private String genericTransformRunMode;

  @GuiWidgetElement(
      id = WIDGET_ID_DEPLOY_MODE,
      order = "10200",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::SparkEngine.OptionsDeployMode.Label",
      toolTip = "i18n::SparkEngine.OptionsDeployMode.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.General",
      groupOrder = "10")
  @HopMetadataProperty(key = "deploy_mode")
  private SparkDeployMode deployMode;

  @GuiWidgetElement(
      order = "20500",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label = "i18n::SparkEngine.OptionsStagingFolder.Label",
      toolTip = "i18n::SparkEngine.OptionsStagingFolder.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Cluster",
      groupOrder = "20")
  @HopMetadataProperty(key = "staging_folder")
  private String stagingFolder;

  @GuiWidgetElement(
      order = "20600",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsStagingClusterUrl.Label",
      toolTip = "i18n::SparkEngine.OptionsStagingClusterUrl.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Cluster",
      groupOrder = "20")
  @HopMetadataProperty(key = "staging_cluster_url")
  private String stagingClusterUrl;

  @GuiWidgetElement(
      order = "20400",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsSubmitUrl.Label",
      toolTip = "i18n::SparkEngine.OptionsSubmitUrl.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Cluster",
      groupOrder = "20")
  @HopMetadataProperty(key = "submit_url")
  private String submitUrl;

  @GuiWidgetElement(
      id = WIDGET_ID_DATABRICKS_CONNECTION,
      order = "20010",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.METADATA,
      metadataKey = "databricks-connection",
      label = "i18n::SparkEngine.OptionsDatabricksConnection.Label",
      toolTip = "i18n::SparkEngine.OptionsDatabricksConnection.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Cluster",
      groupOrder = "20")
  @HopMetadataProperty(key = "databricks_connection")
  private String databricksConnection;

  @GuiWidgetElement(
      id = WIDGET_ID_DATABRICKS_COMPUTE,
      order = "20020",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsDatabricksCompute.Label",
      toolTip = "i18n::SparkEngine.OptionsDatabricksCompute.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Cluster",
      groupOrder = "20")
  @HopMetadataProperty(key = "databricks_compute")
  private String databricksCompute;

  @GuiWidgetElement(
      id = WIDGET_ID_DATABRICKS_RUNTIME,
      order = "20030",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsDatabricksRuntime.Label",
      toolTip = "i18n::SparkEngine.OptionsDatabricksRuntime.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Cluster",
      groupOrder = "20")
  @HopMetadataProperty(key = "databricks_runtime")
  private String databricksRuntime;

  @GuiWidgetElement(
      id = WIDGET_ID_DATABRICKS_NODE_TYPE,
      order = "20040",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsDatabricksNodeType.Label",
      toolTip = "i18n::SparkEngine.OptionsDatabricksNodeType.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Cluster",
      groupOrder = "20")
  @HopMetadataProperty(key = "databricks_node_type")
  private String databricksNodeType;

  @GuiWidgetElement(
      id = WIDGET_ID_DATABRICKS_WORKERS,
      order = "20050",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsDatabricksWorkers.Label",
      toolTip = "i18n::SparkEngine.OptionsDatabricksWorkers.ToolTip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::SparkEngine.Group.Cluster",
      groupOrder = "20")
  @HopMetadataProperty(key = "databricks_workers")
  private String databricksWorkers;

  public SparkPipelineRunConfiguration() {
    super();
    this.sparkMaster = "local[*]";
    this.sparkAppName = "Apache Hop";
    this.genericTransformRunMode = SparkRunMode.DISTRIBUTED.name();
    this.deployMode = SparkDeployMode.CLIENT;
    setEnginePluginId(SparkConst.PLUGIN_ID);
    setEnginePluginName(SparkConst.PLUGIN_NAME);
  }

  /**
   * @return the cluster type, or the one the Spark master points to when none is set (a run
   *     configuration saved before the option existed); null when that can't be told either
   */
  public SparkClusterType getEffectiveClusterType() {
    return clusterType != null ? clusterType : SparkClusterType.fromMaster(sparkMaster);
  }

  /** Hides the options that do not apply to the cluster type and deploy mode. */
  @Override
  public String getGuiWidgetsListenerClassName() {
    return SparkRunConfigurationWidgetsListener.class.getName();
  }

  /** The rows of the Spark settings table; the table edits this list in place. */
  public List<SparkSetting> getSparkSettings() {
    if (sparkSettings == null) {
      sparkSettings = SparkSetting.parse(storedSparkConfigs);
    }
    return sparkSettings;
  }

  public void setSparkSettings(List<SparkSetting> sparkSettings) {
    this.sparkSettings = sparkSettings == null ? new ArrayList<>() : sparkSettings;
  }

  /**
   * The Spark settings as they are saved: the lines as they were loaded while the rows say the same
   * (so comment lines stay), else the lines of the rows.
   */
  public String getStoredSparkConfigs() {
    if (sparkSettings == null) {
      return storedSparkConfigs;
    }
    String rows = SparkSetting.toLines(sparkSettings);
    return rows.equals(SparkSetting.toLines(SparkSetting.parse(storedSparkConfigs)))
        ? storedSparkConfigs
        : rows;
  }

  public void setStoredSparkConfigs(String lines) {
    storedSparkConfigs = lines;
    sparkSettings = null;
  }

  /** The Spark settings as {@code key=value} lines, as the engine and spark-submit read them. */
  @Override
  public String getSparkConfigs() {
    return SparkSetting.toLines(getSparkSettings());
  }

  /** Replaces the Spark settings with those of {@code key=value} lines. */
  public void setSparkConfigs(String lines) {
    setStoredSparkConfigs(lines);
  }

  /** The rows of the path scheme table; the table edits this list in place. */
  public List<PathSchemeMapping> getPathSchemes() {
    if (pathSchemes == null) {
      pathSchemes = PathSchemeMapping.parse(storedPathSchemeMap);
    }
    return pathSchemes;
  }

  public void setPathSchemes(List<PathSchemeMapping> pathSchemes) {
    this.pathSchemes = pathSchemes == null ? new ArrayList<>() : pathSchemes;
  }

  /**
   * The path scheme map as it is saved: the lines as they were loaded while the rows say the same,
   * else the lines of the rows.
   */
  public String getStoredPathSchemeMap() {
    if (pathSchemes == null) {
      return storedPathSchemeMap;
    }
    String rows = PathSchemeMapping.toLines(pathSchemes);
    return rows.equals(PathSchemeMapping.toLines(PathSchemeMapping.parse(storedPathSchemeMap)))
        ? storedPathSchemeMap
        : rows;
  }

  public void setStoredPathSchemeMap(String lines) {
    storedPathSchemeMap = lines;
    pathSchemes = null;
  }

  /**
   * The path scheme map as {@code from=to} lines, as the Spark File and Lake Table code reads it.
   */
  @Override
  public String getPathSchemeMap() {
    return PathSchemeMapping.toLines(getPathSchemes());
  }

  /** Replaces the path scheme map with that of {@code from=to} lines. */
  public void setPathSchemeMap(String lines) {
    setStoredPathSchemeMap(lines);
  }

  /**
   * The keys the Spark settings table suggests: those of the cluster type first, then the ones
   * every cluster uses. Any other key can be typed.
   */
  public List<String> getSparkSettingKeys(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return SparkSettingKeys.suggestions(getEffectiveClusterType());
  }

  public SparkPipelineRunConfiguration(SparkPipelineRunConfiguration config) {
    super(config);
    this.sparkMaster = config.sparkMaster;
    this.sparkAppName = config.sparkAppName;
    this.fatJar = config.fatJar;
    this.hadoopConfDir = config.hadoopConfDir;
    // The copy reads its rows from the lines when asked: no rows shared with the original
    this.storedSparkConfigs = config.getStoredSparkConfigs();
    this.driverMemory = config.driverMemory;
    this.executorMemory = config.executorMemory;
    this.executorCores = config.executorCores;
    this.storedPathSchemeMap = config.getStoredPathSchemeMap();
    this.genericTransformRunMode = config.genericTransformRunMode;
    this.deployMode = config.deployMode;
    this.clusterType = config.clusterType;
    this.stagingFolder = config.stagingFolder;
    this.stagingClusterUrl = config.stagingClusterUrl;
    this.submitUrl = config.submitUrl;
    this.databricksConnection = config.databricksConnection;
    this.databricksCompute = config.databricksCompute;
    this.databricksRuntime = config.databricksRuntime;
    this.databricksNodeType = config.databricksNodeType;
    this.databricksWorkers = config.databricksWorkers;
  }

  @Override
  public SparkPipelineRunConfiguration clone() {
    return new SparkPipelineRunConfiguration(this);
  }

  /** Combo values for {@link #genericTransformRunMode}. */
  public List<String> getGenericTransformRunModes(
      ILogChannel log, IHopMetadataProvider metadataProvider) {
    return SparkRunMode.configDisplayValues();
  }
}
