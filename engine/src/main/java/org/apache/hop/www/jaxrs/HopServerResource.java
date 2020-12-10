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

package org.apache.hop.www.jaxrs;

import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.www.HopServerObjectEntry;
import org.apache.hop.www.HopServerSingleton;
import org.apache.hop.www.HopServerConfig;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Path( "/carte" )
public class HopServerResource {

  public HopServerResource() {
  }

  public static IPipelineEngine<PipelineMeta> getPipeline( String id ) {
    return HopServerSingleton.getInstance().getPipelineMap().getPipeline( getHopServerObjectEntry( id ) );
  }

  public static IWorkflowEngine<WorkflowMeta> getWorkflow( String id ) {
    return HopServerSingleton.getInstance().getWorkflowMap().getWorkflow( getHopServerObjectEntry( id ) );
  }

  public static HopServerObjectEntry getHopServerObjectEntry( String id ) {
    List<HopServerObjectEntry> pipelineList =
      HopServerSingleton.getInstance().getPipelineMap().getPipelineObjects();
    for ( HopServerObjectEntry entry : pipelineList ) {
      if ( entry.getId().equals( id ) ) {
        return entry;
      }
    }
    List<HopServerObjectEntry> workflowList = HopServerSingleton.getInstance().getWorkflowMap().getWorkflowObjects();
    for ( HopServerObjectEntry entry : workflowList ) {
      if ( entry.getId().equals( id ) ) {
        return entry;
      }
    }
    return null;
  }

  @GET
  @Path( "/systemInfo" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public ServerStatus getSystemInfo() {
    ServerStatus serverStatus = new ServerStatus();
    serverStatus.setStatusDescription( "Online" );
    return serverStatus;
  }

  @GET
  @Path( "/configDetails" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public List<NVPair> getConfigDetails() {
    HopServerConfig serverConfig = HopServerSingleton.getInstance().getPipelineMap().getHopServerConfig();
    List<NVPair> list = new ArrayList<>();
    list.add( new NVPair( "maxLogLines", "" + serverConfig.getMaxLogLines() ) );
    list.add( new NVPair( "maxLogLinesAge", "" + serverConfig.getMaxLogTimeoutMinutes() ) );
    list.add( new NVPair( "maxObjectsAge", "" + serverConfig.getObjectTimeoutMinutes() ) );
    list.add( new NVPair( "configFile", "" + serverConfig.getFilename() ) );
    return list;
  }

  @GET
  @Path( "/pipelines" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public List<HopServerObjectEntry> getPipelines() {
    List<HopServerObjectEntry> pipelineEntries = HopServerSingleton.getInstance().getPipelineMap().getPipelineObjects();
    return pipelineEntries;
  }

  @GET
  @Path( "/pipelines/detailed" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public List<PipelineStatus> getPipelineDetails() {
    List<HopServerObjectEntry> pipelineEntries =
      HopServerSingleton.getInstance().getPipelineMap().getPipelineObjects();

    List<PipelineStatus> details = new ArrayList<>();

    PipelineResource pipelineRes = new PipelineResource();
    for ( HopServerObjectEntry entry : pipelineEntries ) {
      details.add( pipelineRes.getPipelineStatus( entry.getId() ) );
    }
    return details;
  }

  @GET
  @Path( "/workflows" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public List<HopServerObjectEntry> getWorkflows() {
    List<HopServerObjectEntry> actions = HopServerSingleton.getInstance().getWorkflowMap().getWorkflowObjects();
    return actions;
  }

  @GET
  @Path( "/workflows/detailed" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public List<WorkflowStatus> getWorkflowsDetails() {
    List<HopServerObjectEntry> actions = HopServerSingleton.getInstance().getWorkflowMap().getWorkflowObjects();

    List<WorkflowStatus> details = new ArrayList<>();

    WorkflowResource jobRes = new WorkflowResource();
    for ( HopServerObjectEntry entry : actions ) {
      details.add( jobRes.getWorkflowStatus( entry.getId() ) );
    }
    return details;
  }

}
