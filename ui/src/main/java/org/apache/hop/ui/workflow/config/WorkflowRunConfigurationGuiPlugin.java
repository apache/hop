/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.workflow.config;

import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.cluster.IGuiMetaStorePlugin;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;

@GuiPlugin
@GuiMetaStoreElement(
  name = "Workflow Run Configuration",
  description = "Describes how and with which engine a workflow is to be executed",
  iconImage = "ui/images/run.svg"
)
  public class WorkflowRunConfigurationGuiPlugin implements IGuiMetaStorePlugin<WorkflowRunConfiguration> {

  @Override public Class<WorkflowRunConfiguration> getMetaStoreElementClass() {
    return WorkflowRunConfiguration.class;
  }
}
