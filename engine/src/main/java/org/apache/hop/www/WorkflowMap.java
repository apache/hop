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

package org.apache.hop.www;

import org.apache.hop.core.util.Utils;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

  private HopServerConfig hopServerConfig;

  public WorkflowMap() {
    workflowMap = new ConcurrentHashMap<>(  );
    configurationMap = new ConcurrentHashMap<>(  );
  }

  public synchronized void addWorkflow( String workflowName, String serverObjectId, IWorkflowEngine<WorkflowMeta> workflow, WorkflowConfiguration workflowConfiguration ) {
    synchronized ( workflowMap ) {
      HopServerObjectEntry entry = new HopServerObjectEntry( workflowName, serverObjectId );
      workflowMap.put( entry, workflow );
      configurationMap.put( entry, workflowConfiguration );
    }
  }

  public synchronized void replaceWorkflow( IWorkflowEngine<WorkflowMeta> oldWorkflow, IWorkflowEngine<WorkflowMeta> workflow, WorkflowConfiguration workflowConfiguration ) {
    synchronized ( workflowMap ) {
      HopServerObjectEntry entry = getEntry( oldWorkflow );
      if ( entry != null ) {
        workflowMap.put( entry, workflow );
        configurationMap.put( entry, workflowConfiguration );
      } else {
        addWorkflow( workflow.getWorkflowName(), workflow.getContainerId(), workflow, workflowConfiguration );
      }
    }
  }

  private HopServerObjectEntry getEntry(IWorkflowEngine<WorkflowMeta> workflow) {
    return new HopServerObjectEntry( workflow.getWorkflowName(), workflow.getContainerId() );
  }

  /**
   * Find the first workflow in the list that comes to mind!
   *
   * @param workflowName
   * @return the first pipeline with the specified name
   */
  public synchronized IWorkflowEngine<WorkflowMeta> getWorkflow( String workflowName ) {
    synchronized ( workflowMap ) {
      for ( HopServerObjectEntry entry : workflowMap.keySet() ) {
        if ( entry.getName().equals( workflowName ) ) {
          return getWorkflow( entry );
        }
      }
    }
    return null;
  }

  /**
   * @param entry The HopServer workflow object
   * @return the workflow with the specified entry
   */
  public synchronized IWorkflowEngine<WorkflowMeta> getWorkflow( HopServerObjectEntry entry ) {
    synchronized ( workflowMap ) {
      return workflowMap.get( entry );
    }
  }

  public synchronized WorkflowConfiguration getConfiguration( String workflowName ) {
    synchronized ( configurationMap ) {
      for ( HopServerObjectEntry entry : configurationMap.keySet() ) {
        if ( entry.getName().equals( workflowName ) ) {
          return getConfiguration( entry );
        }
      }
      return null;
    }
  }

  /**
   * @param entry The HopServer workflow object
   * @return the workflow configuration with the specified entry
   */
  public synchronized WorkflowConfiguration getConfiguration( HopServerObjectEntry entry ) {
    synchronized ( configurationMap ) {
      return configurationMap.get( entry );
    }
  }

  public synchronized void removeWorkflow( HopServerObjectEntry entry ) {
    synchronized ( workflowMap ) {
      workflowMap.remove( entry );
      configurationMap.remove( entry );
    }
  }

  public synchronized List<HopServerObjectEntry> getWorkflowObjects() {
    synchronized ( workflowMap ) {
      return new ArrayList<>( workflowMap.keySet() );
    }
  }

  public synchronized HopServerObjectEntry getFirstHopServerObjectEntry( String workflowName ) {
    synchronized ( workflowMap ) {
      for ( HopServerObjectEntry key : workflowMap.keySet() ) {
        if ( key.getName().equals( workflowName ) ) {
          return key;
        }
      }
      return null;
    }
  }

  /**
   * @return the hopServerConfig
   */
  public HopServerConfig getHopServerConfig() {
    return hopServerConfig;
  }

  /**
   * @param hopServerConfig the hopServerConfig to set
   */
  public void setHopServerConfig( HopServerConfig hopServerConfig ) {
    this.hopServerConfig = hopServerConfig;
  }

  /**
   * Find a workflow using the container/carte object ID.
   *
   * @param id the container/carte object ID
   * @return The workflow if it's found, null if the ID couldn't be found in the workflow map.
   */
  public synchronized IWorkflowEngine<WorkflowMeta> findWorkflow( String id ) {
    synchronized ( workflowMap ) {
      for ( IWorkflowEngine<WorkflowMeta> workflow : workflowMap.values() ) {
        if ( workflow.getContainerId().equals( id ) ) {
          return workflow;
        }
      }
    }
    return null;
  }

  /**
   * Find a workflow with an optional ID
   * @param workflowName The name of the workflow
   * @param id The ID of the workflow or null if it's not provided
   * @return The workflow or null if it couldn't be found.
   */
  public IWorkflowEngine<WorkflowMeta> findWorkflow( String workflowName, String id ) {
    IWorkflowEngine<WorkflowMeta> workflow;
    HopServerObjectEntry entry;
    if ( Utils.isEmpty( id ) ) {
      // get the first workflow that matches...
      //
      entry = getFirstHopServerObjectEntry( workflowName );
      if ( entry == null ) {
        workflow = null;
      } else {
        workflow = getWorkflow( entry );
      }
    } else {
      // Take the ID into account!
      //
      entry = new HopServerObjectEntry( workflowName, id );
      workflow = getWorkflow( entry );
    }
    return workflow;
  }
}
