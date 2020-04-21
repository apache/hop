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

package org.apache.hop.workflow.engines.local;

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.empty.EmptyWorkflowRunConfiguration;

@GuiPlugin
public class LocalWorkflowRunConfiguration extends EmptyWorkflowRunConfiguration implements IWorkflowEngineRunConfiguration {

  @GuiWidgetElement(
    order = "20",
    parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.SafeModeEnabled.Label"
  )
  @MetaStoreAttribute(key="safe_mode")
  protected boolean safeModeEnabled;

  public LocalWorkflowRunConfiguration() {
    super();
    safeModeEnabled = false;
  }

  public LocalWorkflowRunConfiguration( LocalWorkflowRunConfiguration config ) {
    super( config );
    this.safeModeEnabled = config.safeModeEnabled;
  }

  public LocalWorkflowRunConfiguration clone() {
    return new LocalWorkflowRunConfiguration( this );
  }

  /**
   * Gets safeModeEnabled
   *
   * @return value of safeModeEnabled
   */
  public boolean isSafeModeEnabled() {
    return safeModeEnabled;
  }

  /**
   * @param safeModeEnabled The safeModeEnabled to set
   */
  public void setSafeModeEnabled( boolean safeModeEnabled ) {
    this.safeModeEnabled = safeModeEnabled;
  }
}
