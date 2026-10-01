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

package org.apache.hop.replay.engine;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.local.LocalWorkflowRunConfiguration;

@GuiPlugin(description = "Replay workflow run configuration widgets")
@Getter
@Setter
public class ReplayWorkflowRunConfiguration extends LocalWorkflowRunConfiguration {

  @GuiWidgetElement(
      id = "sourceExecutionId",
      order = "110",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Source execution ID",
      toolTip = "The execution ID of the failed workflow run to replay")
  @HopMetadataProperty(key = "source_execution_id")
  protected String sourceExecutionId;

  @GuiWidgetElement(
      id = "startActionName",
      order = "120",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Start action name",
      toolTip =
          "The name of the action to resume execution from (leave blank for first failed action)")
  @HopMetadataProperty(key = "start_action_name")
  protected String startActionName;

  public ReplayWorkflowRunConfiguration() {
    super();
  }

  public ReplayWorkflowRunConfiguration(ReplayWorkflowRunConfiguration config) {
    super(config);
    this.sourceExecutionId = config.sourceExecutionId;
    this.startActionName = config.startActionName;
  }
}
