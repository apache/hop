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

package org.apache.hop.www;

import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This is a map between the workflow name and the (running/waiting/finished) workflow.
 *
 * @author Matt
 * @since 26-SEP-2007
 * @since 3.0.0
 */
public class WorkflowMap {
  private final Map<HopServerObjectEntry, IWorkflowEngine<WorkflowMeta>> workflowMap;
  private final Map<HopServerObjectEntry, WorkflowConfiguration> configurationMap;

  private SlaveServerConfig slaveServerConfig;

  public WorkflowMap() {
    workflowMap = new HashMap<>();
    configurationMap = new HashMap<>();
  }

  public synchronized void addWorkflow( String workflowName, String carteObjectId, IWorkflowEngine<WorkflowMeta> workflow, WorkflowConfiguration workflowConfiguration ) {
    HopServerObjectEntry entry = new HopServerObjectEntry( workflowName, carteObjectId );
    workflowMap.put( entry, workflow );
    configurationMap.put( entry, workflowConfiguration );
  }

  public synchronized void registerWorkflow( IWorkflowEngine<WorkflowMeta> workflow, WorkflowConfiguration workflowConfiguration ) {
    workflow.setContainerObjectId( UUID.randomUUID().toString() );
    HopServerObjectEntry entry = new HopServerObjectEntry( workflow.getWorkflowMeta().getName(), workflow.getContainerObjectId() );
    workflowMap.put( entry, workflow );
    configurationMap.put( entry, workflowConfiguration );
  }

  public synchronized void replaceWorkflow( HopServerObjectEntry entry, IWorkflowEngine<WorkflowMeta> workflow, WorkflowConfiguration workflowConfiguration ) {
    workflowMap.put( entry, workflow );
    configurationMap.put( entry, workflowConfiguration );
  }

  /**
   * Find the first workflow in the list that comes to mind!
   *
   * @param workflowName
   * @return the first pipeline with the specified name
   */
  public synchronized IWorkflowEngine<WorkflowMeta> getWorkflow( String workflowName ) {
    for ( HopServerObjectEntry entry : workflowMap.keySet() ) {
      if ( entry.getName().equals( workflowName ) ) {
        return getWorkflow( entry );
      }
    }
    return null;
  }

  /**
   * @param entry The HopServer workflow object
   * @return the workflow with the specified entry
   */
  public synchronized IWorkflowEngine<WorkflowMeta> getWorkflow( HopServerObjectEntry entry ) {
    return workflowMap.get( entry );
  }

  public synchronized WorkflowConfiguration getConfiguration( String workflowName ) {
    for ( HopServerObjectEntry entry : configurationMap.keySet() ) {
      if ( entry.getName().equals( workflowName ) ) {
        return getConfiguration( entry );
      }
    }
    return null;
  }

  /**
   * @param entry The HopServer workflow object
   * @return the workflow configuration with the specified entry
   */
  public synchronized WorkflowConfiguration getConfiguration( HopServerObjectEntry entry ) {
    return configurationMap.get( entry );
  }

  public synchronized void removeJob( HopServerObjectEntry entry ) {
    workflowMap.remove( entry );
    configurationMap.remove( entry );
  }

  public synchronized List<HopServerObjectEntry> getWorkflowObjects() {
    return new ArrayList<>( workflowMap.keySet() );
  }

  public synchronized HopServerObjectEntry getFirstCarteObjectEntry( String workflowName ) {
    for ( HopServerObjectEntry key : workflowMap.keySet() ) {
      if ( key.getName().equals( workflowName ) ) {
        return key;
      }
    }
    return null;
  }

  /**
   * @return the slaveServerConfig
   */
  public SlaveServerConfig getSlaveServerConfig() {
    return slaveServerConfig;
  }

  /**
   * @param slaveServerConfig the slaveServerConfig to set
   */
  public void setSlaveServerConfig( SlaveServerConfig slaveServerConfig ) {
    this.slaveServerConfig = slaveServerConfig;
  }

  /**
   * Find a workflow using the container/carte object ID.
   *
   * @param id the container/carte object ID
   * @return The workflow if it's found, null if the ID couldn't be found in the workflow map.
   */
  public synchronized IWorkflowEngine<WorkflowMeta> findWorkflow( String id ) {
    for ( IWorkflowEngine<WorkflowMeta> workflow : workflowMap.values() ) {
      if ( workflow.getContainerObjectId().equals( id ) ) {
        return workflow;
      }
    }
    return null;
  }

}
