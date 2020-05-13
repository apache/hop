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

package org.apache.hop.workflow.engine;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;

public class WorkflowEngineFactory {

  public static final <T extends WorkflowMeta> IWorkflowEngine<T> createWorkflowEngine( String runConfigurationName, IMetaStore metaStore, T workflowMeta ) throws HopException {
    if ( StringUtils.isEmpty(runConfigurationName)) {
      throw new HopException( "You need to specify a workflow run configuration to execute this workflow" );
    }
    WorkflowRunConfiguration runConfiguration;
    try {
      runConfiguration = WorkflowRunConfiguration.createFactory( metaStore ).loadElement( runConfigurationName );
    } catch(Exception e) {
      throw new HopException( "Error loading workflow run configuration '"+runConfigurationName+"'", e );
    }
    if (runConfiguration==null) {
      throw new HopException( "Workflow run configuration '"+runConfigurationName+"' could not be found" );
    }
    IWorkflowEngine<T> workflowEngine = createWorkflowEngine( runConfiguration, workflowMeta );

    // Copy the variables from the metadata
    //
    workflowEngine.initializeVariablesFrom( workflowMeta );

    // Pass the metastores around to make sure
    //
    workflowEngine.setMetaStore( metaStore );
    workflowMeta.setMetaStore( metaStore );

    return workflowEngine;
  }

  private static final <T extends WorkflowMeta> IWorkflowEngine<T> createWorkflowEngine( WorkflowRunConfiguration workflowRunConfiguration, T workflowMeta ) throws HopException {
    IWorkflowEngineRunConfiguration engineRunConfiguration = workflowRunConfiguration.getEngineRunConfiguration();
    if (engineRunConfiguration==null) {
      throw new HopException( "There is no pipeline execution engine specified in run configuration '"+workflowRunConfiguration.getName()+"'" );
    }
    String enginePluginId = engineRunConfiguration.getEnginePluginId();

    // Load this engine from the plugin registry
    //
    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    IPlugin plugin = pluginRegistry.findPluginWithId( WorkflowEnginePluginType.class, enginePluginId );
    if (plugin==null) {
      throw new HopException( "Unable to find pipeline engine plugin type with ID '"+enginePluginId+"'" );
    }

    IWorkflowEngine<T> workflowEngine = pluginRegistry.loadClass( plugin, IWorkflowEngine.class );
    workflowEngine.setWorkflowRunConfiguration( workflowRunConfiguration );

    workflowEngine.setWorkflowMeta( workflowMeta );

    return workflowEngine;
  }
}
