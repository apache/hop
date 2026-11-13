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

package org.apache.hop.spark.engines;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

/**
 * Keeps the native Spark run configuration down to the options that do something for its cluster
 * type and deploy mode, and fills in the cluster type of a configuration saved before it had one.
 */
public class SparkRunConfigurationWidgetsListener extends GuiCompositeWidgetsAdapter {

  static final String FAT_JAR = "fatJar";
  static final String HADOOP_CONF_DIR = "hadoopConfDir";
  static final String SUBMIT_URL = "submitUrl";
  static final String STAGING_FOLDER = "stagingFolder";
  static final String STAGING_CLUSTER_URL = "stagingClusterUrl";
  static final String DRIVER_MEMORY = "driverMemory";
  static final String EXECUTOR_MEMORY = "executorMemory";
  static final String EXECUTOR_CORES = "executorCores";

  /** The options of the Databricks cluster type, hidden for every other type. */
  static final List<String> DATABRICKS_OPTIONS =
      List.of(
          SparkPipelineRunConfiguration.WIDGET_ID_DATABRICKS_CONNECTION,
          SparkPipelineRunConfiguration.WIDGET_ID_DATABRICKS_COMPUTE,
          SparkPipelineRunConfiguration.WIDGET_ID_DATABRICKS_RUNTIME,
          SparkPipelineRunConfiguration.WIDGET_ID_DATABRICKS_NODE_TYPE,
          SparkPipelineRunConfiguration.WIDGET_ID_DATABRICKS_WORKERS);

  private final IPipelineEngineRunConfiguration runConfiguration;

  public SparkRunConfigurationWidgetsListener(IPipelineEngineRunConfiguration runConfiguration) {
    this.runConfiguration = runConfiguration;
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    Combo clusterType =
        combo(compositeWidgets, SparkPipelineRunConfiguration.WIDGET_ID_CLUSTER_TYPE);
    if (clusterType != null && clusterType.getText().isEmpty()) {
      // Saved before the option existed: the Spark master says where it runs
      SparkClusterType fromMaster = typeOfAnOlderConfiguration(masterText(compositeWidgets));
      if (fromMaster != null) {
        clusterType.setText(fromMaster.getDescription());
      }
    }
    hideOptionsThatDoNotApply(compositeWidgets);
  }

  /**
   * The cluster type to show for a configuration saved before it had one. An empty master tells
   * nothing: such a configuration runs locally from Hop and on the cluster under spark-submit, and
   * the type spark-submit would make it refuse to run from Hop once saved. It stays unset.
   *
   * @return the type, or null to leave the type empty
   */
  static SparkClusterType typeOfAnOlderConfiguration(String master) {
    return StringUtils.isBlank(master) ? null : SparkClusterType.fromMaster(master);
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    if (SparkPipelineRunConfiguration.WIDGET_ID_CLUSTER_TYPE.equals(widgetId)) {
      SparkClusterType type = clusterType(compositeWidgets);
      Control master =
          compositeWidgets.getWidgetsMap().get(SparkPipelineRunConfiguration.WIDGET_ID_MASTER);
      if (type != null && master instanceof TextVar text && !type.matchesMaster(text.getText())) {
        text.setText(type.getMasterTemplate());
      }
    }
    if (SparkPipelineRunConfiguration.WIDGET_ID_CLUSTER_TYPE.equals(widgetId)
        || SparkPipelineRunConfiguration.WIDGET_ID_MASTER.equals(widgetId)
        || SparkPipelineRunConfiguration.WIDGET_ID_DEPLOY_MODE.equals(widgetId)) {
      hideOptionsThatDoNotApply(compositeWidgets);
    }
  }

  private void hideOptionsThatDoNotApply(GuiCompositeWidgets compositeWidgets) {
    compositeWidgets.setWidgetsHidden(
        runConfiguration,
        hiddenOptions(clusterType(compositeWidgets), clusterMode(compositeWidgets)));
  }

  /**
   * The options that do nothing for a cluster type and deploy mode. An unknown type (the master is
   * a variable expression) hides nothing.
   */
  static Set<String> hiddenOptions(SparkClusterType type, boolean clusterMode) {
    Set<String> hidden = new HashSet<>();
    if (type == null) {
      return hidden;
    }
    List<String> staging = List.of(STAGING_FOLDER, STAGING_CLUSTER_URL);
    if (type != SparkClusterType.DATABRICKS) {
      hidden.addAll(DATABRICKS_OPTIONS);
    }
    switch (type) {
      case LOCAL -> {
        // Spark runs in this JVM: no driver to submit, no executors, nothing to ship
        hidden.add(SparkPipelineRunConfiguration.WIDGET_ID_DEPLOY_MODE);
        hidden.addAll(
            List.of(
                FAT_JAR,
                HADOOP_CONF_DIR,
                SUBMIT_URL,
                DRIVER_MEMORY,
                EXECUTOR_MEMORY,
                EXECUTOR_CORES));
        hidden.addAll(staging);
      }
        // Staging is optional in client mode: with it the executors download the fat jar from the
        // staging folder instead of from Hop, or use it where it is with a local: URL
      case STANDALONE -> {
        hidden.add(HADOOP_CONF_DIR);
        if (!clusterMode) {
          hidden.add(SUBMIT_URL);
        }
      }
      case YARN -> hidden.add(SUBMIT_URL);
      case KUBERNETES -> {
        hidden.add(HADOOP_CONF_DIR);
        hidden.add(SUBMIT_URL);
      }
      case SUBMITTED -> {
        // spark-submit decides where the driver runs and ships the jar
        hidden.add(SparkPipelineRunConfiguration.WIDGET_ID_MASTER);
        hidden.add(SparkPipelineRunConfiguration.WIDGET_ID_DEPLOY_MODE);
        hidden.add(HADOOP_CONF_DIR);
        hidden.add(SUBMIT_URL);
        hidden.addAll(staging);
      }
      case DATABRICKS -> {
        // The driver always runs on the Databricks cluster, whose settings set the master and
        // the sizes; Hop stages on a volume that the cluster reads under the same path
        hidden.addAll(
            List.of(
                SparkPipelineRunConfiguration.WIDGET_ID_MASTER,
                SparkPipelineRunConfiguration.WIDGET_ID_DEPLOY_MODE,
                HADOOP_CONF_DIR,
                SUBMIT_URL,
                STAGING_CLUSTER_URL,
                DRIVER_MEMORY,
                EXECUTOR_MEMORY,
                EXECUTOR_CORES));
      }
    }
    return hidden;
  }

  private static SparkClusterType clusterType(GuiCompositeWidgets compositeWidgets) {
    Combo combo = combo(compositeWidgets, SparkPipelineRunConfiguration.WIDGET_ID_CLUSTER_TYPE);
    String text = combo == null ? "" : combo.getText();
    for (SparkClusterType type : SparkClusterType.values()) {
      if (type.getDescription().equals(text) || type.getCode().equals(text)) {
        return type;
      }
    }
    // None chosen yet: what the master points to; an empty one hides nothing
    return typeOfAnOlderConfiguration(masterText(compositeWidgets));
  }

  private static boolean clusterMode(GuiCompositeWidgets compositeWidgets) {
    Combo combo = combo(compositeWidgets, SparkPipelineRunConfiguration.WIDGET_ID_DEPLOY_MODE);
    return combo != null && SparkDeployMode.CLUSTER.getDescription().equals(combo.getText());
  }

  private static String masterText(GuiCompositeWidgets compositeWidgets) {
    Control master =
        compositeWidgets.getWidgetsMap().get(SparkPipelineRunConfiguration.WIDGET_ID_MASTER);
    return master instanceof TextVar text ? text.getText() : "";
  }

  private static Combo combo(GuiCompositeWidgets compositeWidgets, String id) {
    Control control = compositeWidgets.getWidgetsMap().get(id);
    return control instanceof Combo combo && !combo.isDisposed() ? combo : null;
  }
}
