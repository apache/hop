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

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePlugin;

@WorkflowEnginePlugin(
  id = "Local",
  name = "Hop local workflow engine",
  description = "Executes your workflow locally"
)
public class LocalWorkflowEngine extends Workflow implements IWorkflowEngine<WorkflowMeta> {

  public LocalWorkflowEngine() {
    super();
    setDefaultRunConfiguration();
  }

  public LocalWorkflowEngine( WorkflowMeta workflowMeta ) {
    super( workflowMeta );
    setDefaultRunConfiguration();
  }

  public LocalWorkflowEngine( WorkflowMeta workflowMeta, ILoggingObject parent ) {
    super( workflowMeta, parent );
    setDefaultRunConfiguration();
  }

  @Override public IWorkflowEngineRunConfiguration createDefaultWorkflowEngineRunConfiguration() {
    return new LocalWorkflowRunConfiguration();
  }

  private void setDefaultRunConfiguration() {
    setWorkflowRunConfiguration( new WorkflowRunConfiguration( "local", "", createDefaultWorkflowEngineRunConfiguration() ) );
  }

}
