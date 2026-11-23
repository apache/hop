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

package org.apache.hop.airflow.engine;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.airflow.bundle.AirflowBundleWriter;
import org.apache.hop.airflow.metadata.AirflowConnection;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.empty.EmptyWorkflowRunConfiguration;

/** Where to publish the project as a DAG bundle and which Airflow runs it. */
@GuiPlugin(description = "Airflow workflow run configuration widgets")
@Getter
@Setter
public class AirflowWorkflowRunConfiguration extends EmptyWorkflowRunConfiguration {

  /** One box: the fields scroll inside it, above the dialog's buttons. */
  private static final String GROUP = "i18n::AirflowWorkflowRunConfiguration.Group.Airflow";

  public static final String DEFAULT_DAG_WAIT_SECONDS = "120";
  public static final String DEFAULT_POLL_SECONDS = "5";

  @GuiWidgetElement(
      id = "connection",
      order = "010",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      type = GuiElementType.METADATA,
      metadata = AirflowConnection.class,
      label = "i18n::AirflowWorkflowRunConfiguration.Connection.Label",
      toolTip = "i18n::AirflowWorkflowRunConfiguration.Connection.Tooltip")
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.AIRFLOW_CONNECTION)
  private String connectionName;

  @GuiWidgetElement(
      id = "bundleFolder",
      order = "020",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      type = GuiElementType.FOLDER,
      label = "i18n::AirflowWorkflowRunConfiguration.BundleFolder.Label",
      toolTip = "i18n::AirflowWorkflowRunConfiguration.BundleFolder.Tooltip")
  @HopMetadataProperty(key = "bundle_folder")
  private String bundleFolder;

  @GuiWidgetElement(
      id = "workerHopHome",
      order = "030",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowWorkflowRunConfiguration.WorkerHopHome.Label",
      toolTip = "i18n::AirflowWorkflowRunConfiguration.WorkerHopHome.Tooltip")
  @HopMetadataProperty(key = "worker_hop_home")
  private String workerHopHome = AirflowBundleWriter.DEFAULT_WORKER_HOP_HOME;

  @GuiWidgetElement(
      id = "workerRunConfiguration",
      order = "040",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowWorkflowRunConfiguration.WorkerRunConfiguration.Label",
      toolTip = "i18n::AirflowWorkflowRunConfiguration.WorkerRunConfiguration.Tooltip")
  @HopMetadataProperty(key = "worker_run_configuration")
  private String workerRunConfiguration;

  @GuiWidgetElement(
      id = "environmentFiles",
      order = "050",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowWorkflowRunConfiguration.EnvironmentFiles.Label",
      toolTip = "i18n::AirflowWorkflowRunConfiguration.EnvironmentFiles.Tooltip")
  @HopMetadataProperty(key = "environment_files")
  private String environmentFiles;

  @GuiWidgetElement(
      id = "dagIdPrefix",
      order = "060",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowWorkflowRunConfiguration.DagIdPrefix.Label",
      toolTip = "i18n::AirflowWorkflowRunConfiguration.DagIdPrefix.Tooltip")
  @HopMetadataProperty(key = "dag_id_prefix")
  private String dagIdPrefix;

  @GuiWidgetElement(
      id = "unpausingDag",
      order = "070",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      type = GuiElementType.CHECKBOX,
      label = "i18n::AirflowWorkflowRunConfiguration.UnpausingDag.Label",
      toolTip = "i18n::AirflowWorkflowRunConfiguration.UnpausingDag.Tooltip")
  @HopMetadataProperty(key = "unpause_dag")
  private boolean unpausingDag = true;

  @GuiWidgetElement(
      id = "dagWaitSeconds",
      order = "080",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowWorkflowRunConfiguration.DagWaitSeconds.Label",
      toolTip = "i18n::AirflowWorkflowRunConfiguration.DagWaitSeconds.Tooltip")
  @HopMetadataProperty(key = "dag_wait_seconds")
  private String dagWaitSeconds = DEFAULT_DAG_WAIT_SECONDS;

  @GuiWidgetElement(
      id = "pollSeconds",
      order = "090",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowWorkflowRunConfiguration.PollSeconds.Label",
      toolTip = "i18n::AirflowWorkflowRunConfiguration.PollSeconds.Tooltip")
  @HopMetadataProperty(key = "poll_seconds")
  private String pollSeconds = DEFAULT_POLL_SECONDS;

  public AirflowWorkflowRunConfiguration() {
    // The engine it configures: needed to save it, also when it isn't created from metadata
    super(AirflowWorkflowEngine.ID, AirflowWorkflowEngine.NAME);
  }

  public AirflowWorkflowRunConfiguration(AirflowWorkflowRunConfiguration config) {
    super(config);
    this.connectionName = config.connectionName;
    this.bundleFolder = config.bundleFolder;
    this.workerHopHome = config.workerHopHome;
    this.workerRunConfiguration = config.workerRunConfiguration;
    this.environmentFiles = config.environmentFiles;
    this.dagIdPrefix = config.dagIdPrefix;
    this.unpausingDag = config.unpausingDag;
    this.dagWaitSeconds = config.dagWaitSeconds;
    this.pollSeconds = config.pollSeconds;
  }

  @Override
  public AirflowWorkflowRunConfiguration clone() {
    return new AirflowWorkflowRunConfiguration(this);
  }
}
