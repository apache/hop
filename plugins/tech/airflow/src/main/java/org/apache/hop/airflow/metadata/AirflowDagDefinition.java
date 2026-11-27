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
package org.apache.hop.airflow.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.airflow.engine.AirflowDagTasks;
import org.apache.hop.airflow.launcher.HopAirflowBundle;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * One Airflow DAG of a workflow: its schedule, limits, what Airflow shows and the parameter values
 * of its runs. {@code hop airflow-bundle} and the Apache Airflow run configuration publish the
 * object with the project; the launcher on the Airflow side reads it from the bundle, without
 * starting Hop, so the values are used as they are: no variables.
 */
@Getter
@Setter
@GuiPlugin
@HopMetadata(
    key = HopAirflowBundle.DAG_METADATA_KEY,
    name = "i18n::AirflowDag.Name",
    description = "i18n::AirflowDag.Description",
    image = "airflow-connection.svg",
    category = HopMetadataCategory.EXECUTION,
    documentationUrl = "/metadata-types/airflow-dag.html")
public class AirflowDagDefinition extends HopMetadataBase implements Serializable, IHopMetadata {

  private static final long serialVersionUID = 1L;

  private static final String PARENT = AirflowDagDefinitionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID;
  private static final String GROUP_DAG = "i18n::AirflowDag.Group.Dag";
  private static final String GROUP_SCHEDULE = "i18n::AirflowDag.Group.Schedule";
  private static final String GROUP_RUNS = "i18n::AirflowDag.Group.Runs";
  private static final String GROUP_PARAMETERS = "i18n::AirflowDag.Group.Parameters";

  @GuiWidgetElement(
      id = "airflow-dag-workflow",
      order = "0100",
      parentId = PARENT,
      type = GuiElementType.FILENAME,
      variables = false,
      typeFilename = AirflowWorkflowFilename.class,
      label = "i18n::AirflowDag.Workflow.Label",
      toolTip = "i18n::AirflowDag.Workflow.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DAG,
      groupOrder = "010")
  @HopMetadataProperty(key = "workflow")
  private String workflow;

  @GuiWidgetElement(
      id = "airflow-dag-dagId",
      order = "0110",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::AirflowDag.DagId.Label",
      toolTip = "i18n::AirflowDag.DagId.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DAG,
      groupOrder = "010")
  @HopMetadataProperty(key = "dag_id")
  private String dagId;

  @GuiWidgetElement(
      id = "airflow-dag-displayName",
      order = "0120",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::AirflowDag.DisplayName.Label",
      toolTip = "i18n::AirflowDag.DisplayName.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DAG,
      groupOrder = "010")
  @HopMetadataProperty(key = "display_name")
  private String displayName;

  @GuiWidgetElement(
      id = "airflow-dag-tags",
      order = "0130",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::AirflowDag.Tags.Label",
      toolTip = "i18n::AirflowDag.Tags.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DAG,
      groupOrder = "010")
  @HopMetadataProperty(key = "tags")
  private String tags;

  @GuiWidgetElement(
      id = "airflow-dag-tasks",
      order = "0140",
      parentId = PARENT,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::AirflowDag.Tasks.Label",
      toolTip = "i18n::AirflowDag.Tasks.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DAG,
      groupOrder = "010")
  @HopMetadataProperty(key = "tasks", storeWithCode = true)
  private AirflowDagTasks tasks = AirflowDagTasks.PER_ACTION;

  @GuiWidgetElement(
      id = "airflow-dag-documentation",
      order = "0150",
      parentId = PARENT,
      type = GuiElementType.MULTI_LINE_TEXT,
      variables = false,
      multiLineTextHeight = 5,
      label = "i18n::AirflowDag.Documentation.Label",
      toolTip = "i18n::AirflowDag.Documentation.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DAG,
      groupOrder = "010")
  @HopMetadataProperty(key = "documentation")
  private String documentation;

  @GuiWidgetElement(
      id = "airflow-dag-schedule",
      order = "0200",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::AirflowDag.Schedule.Label",
      toolTip = "i18n::AirflowDag.Schedule.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEDULE,
      groupOrder = "020")
  @HopMetadataProperty(key = "schedule")
  private String schedule;

  @GuiWidgetElement(
      id = "airflow-dag-startDate",
      order = "0210",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::AirflowDag.StartDate.Label",
      toolTip = "i18n::AirflowDag.StartDate.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEDULE,
      groupOrder = "020")
  @HopMetadataProperty(key = "start_date")
  private String startDate;

  @GuiWidgetElement(
      id = "airflow-dag-endDate",
      order = "0220",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::AirflowDag.EndDate.Label",
      toolTip = "i18n::AirflowDag.EndDate.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEDULE,
      groupOrder = "020")
  @HopMetadataProperty(key = "end_date")
  private String endDate;

  @GuiWidgetElement(
      id = "airflow-dag-catchup",
      order = "0230",
      parentId = PARENT,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::AirflowDag.Catchup.Label",
      toolTip = "i18n::AirflowDag.Catchup.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEDULE,
      groupOrder = "020")
  @HopMetadataProperty(key = "catchup", storeWithCode = true)
  private AirflowDagSwitch catchup = AirflowDagSwitch.AIRFLOW_DEFAULT;

  @GuiWidgetElement(
      id = "airflow-dag-pausedOnCreation",
      order = "0240",
      parentId = PARENT,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::AirflowDag.PausedOnCreation.Label",
      toolTip = "i18n::AirflowDag.PausedOnCreation.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SCHEDULE,
      groupOrder = "020")
  @HopMetadataProperty(key = "paused_on_creation", storeWithCode = true)
  private AirflowDagSwitch pausedOnCreation = AirflowDagSwitch.AIRFLOW_DEFAULT;

  @GuiWidgetElement(
      id = "airflow-dag-maxActiveRuns",
      order = "0300",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::AirflowDag.MaxActiveRuns.Label",
      toolTip = "i18n::AirflowDag.MaxActiveRuns.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_RUNS,
      groupOrder = "030")
  @HopMetadataProperty(key = "max_active_runs")
  private String maxActiveRuns;

  @GuiWidgetElement(
      id = "airflow-dag-maxActiveTasks",
      order = "0310",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::AirflowDag.MaxActiveTasks.Label",
      toolTip = "i18n::AirflowDag.MaxActiveTasks.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_RUNS,
      groupOrder = "030")
  @HopMetadataProperty(key = "max_active_tasks")
  private String maxActiveTasks;

  @GuiWidgetElement(
      id = "airflow-dag-runTimeoutSeconds",
      order = "0320",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::AirflowDag.RunTimeoutSeconds.Label",
      toolTip = "i18n::AirflowDag.RunTimeoutSeconds.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_RUNS,
      groupOrder = "030")
  @HopMetadataProperty(key = "run_timeout_seconds")
  private String runTimeoutSeconds;

  @GuiWidgetElement(
      id = "airflow-dag-maxConsecutiveFailedRuns",
      order = "0330",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::AirflowDag.MaxConsecutiveFailedRuns.Label",
      toolTip = "i18n::AirflowDag.MaxConsecutiveFailedRuns.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_RUNS,
      groupOrder = "030")
  @HopMetadataProperty(key = "max_consecutive_failed_runs")
  private String maxConsecutiveFailedRuns;

  @GuiWidgetElement(
      id = "airflow-dag-failFast",
      order = "0340",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      label = "i18n::AirflowDag.FailFast.Label",
      toolTip = "i18n::AirflowDag.FailFast.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_RUNS,
      groupOrder = "030")
  @HopMetadataProperty(key = "fail_fast")
  private boolean failFast;

  @GuiWidgetElement(
      id = "airflow-dag-parameters",
      order = "0400",
      parentId = PARENT,
      type = GuiElementType.TABLE,
      label = "i18n::AirflowDag.Parameters.Label",
      toolTip = "i18n::AirflowDag.Parameters.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_PARAMETERS,
      groupOrder = "040",
      tableRows = 8)
  @HopMetadataProperty(key = "parameters")
  private List<AirflowDagParameter> parameters = new ArrayList<>();

  public AirflowDagDefinition() {
    super();
  }

  public AirflowDagDefinition(String name) {
    super(name);
  }
}
