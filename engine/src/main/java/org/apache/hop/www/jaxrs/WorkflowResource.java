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

package org.apache.hop.www.jaxrs;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.apache.hop.www.HopServerObjectEntry;
import org.apache.hop.www.HopServerSingleton;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.UUID;

@Path( "/carte/workflow" )
public class WorkflowResource {

  public WorkflowResource() {
  }

  @GET
  @Path( "/log/{id : .+}" )
  @Produces( { MediaType.TEXT_PLAIN } )
  public String getWorkflowLog( @PathParam( "id" ) String id ) {
    return getWorkflowLog( id, 0 );
  }

  @GET
  @Path( "/log/{id : .+}/{logStart : .+}" )
  @Produces( { MediaType.TEXT_PLAIN } )
  public String getWorkflowLog( @PathParam( "id" ) String id, @PathParam( "logStart" ) int startLineNr ) {
    int lastLineNr = HopLogStore.getLastBufferLineNr();
    IWorkflowEngine<WorkflowMeta> workflow = HopServerResource.getWorkflow( id );
    String logText =
      HopLogStore.getAppender().getBuffer(
        workflow.getLogChannel().getLogChannelId(), false, startLineNr, lastLineNr ).toString();
    return logText;
  }

  @GET
  @Path( "/status/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public WorkflowStatus getWorkflowStatus( @PathParam( "id" ) String id ) {
    WorkflowStatus status = new WorkflowStatus();
    // find workflow
    IWorkflowEngine<WorkflowMeta> workflow = HopServerResource.getWorkflow( id );
    HopServerObjectEntry entry = HopServerResource.getHopServerObjectEntry( id );

    status.setId( entry.getId() );
    status.setName( entry.getName() );
    status.setStatus( workflow.getStatusDescription() );

    return status;
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/start/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public WorkflowStatus startJob( @PathParam( "id" ) String id ) {
    IWorkflowEngine<WorkflowMeta> workflow = HopServerResource.getWorkflow( id );
    HopServerObjectEntry entry = HopServerResource.getHopServerObjectEntry( id );
    if ( workflow.isInitialized() && !workflow.isActive() ) {
      // Re-create the workflow from the workflowMeta
      //

      // Create a new workflow object to start from a sane state. Then replace
      // the new workflow in the workflow map
      //
      synchronized ( this ) {
        WorkflowConfiguration workflowConfiguration = HopServerSingleton.getInstance().getWorkflowMap().getConfiguration( entry );
        IHopMetadataProvider metadataProvider = workflowConfiguration.getMetadataProvider();
        String serverObjectId = UUID.randomUUID().toString();
        SimpleLoggingObject servletLoggingObject = new SimpleLoggingObject( getClass().getName(), LoggingObjectType.HOP_SERVER, null );
        servletLoggingObject.setContainerObjectId( serverObjectId );
        String runConfigurationName = workflowConfiguration.getWorkflowExecutionConfiguration().getRunConfiguration();
        try {
          IWorkflowEngine<WorkflowMeta> newWorkflow = WorkflowEngineFactory.createWorkflowEngine( runConfigurationName, metadataProvider, workflow.getWorkflowMeta(), servletLoggingObject );
          newWorkflow.setLogLevel( workflow.getLogLevel() );

          // Discard old log lines from the old workflow
          //
          HopLogStore.discardLines( workflow.getLogChannelId(), true );

          HopServerSingleton.getInstance().getWorkflowMap().replaceWorkflow( workflow, newWorkflow, workflowConfiguration );
          workflow = newWorkflow;
        } catch(Exception e) {
          throw new RuntimeException( "Unable to instantiate new workflow", e );
        }
      }
    }
    final IWorkflowEngine<WorkflowMeta> finalWorkflow = workflow;

    // Simply start the workflow in the background in a new thread.
    // This will allow us to work asynchronously
    //
    new Thread( () -> finalWorkflow.startExecution() ).start();

    return getWorkflowStatus( id );
  }

  @GET
  @Path( "/stop/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public WorkflowStatus stopJob( @PathParam( "id" ) String id ) {
    IWorkflowEngine<WorkflowMeta> workflow = HopServerResource.getWorkflow( id );
    workflow.stopExecution();
    return getWorkflowStatus( id );
  }

  @GET
  @Path( "/remove/{id : .+}" )
  public Response removeJob( @PathParam( "id" ) String id ) {
    IWorkflowEngine<WorkflowMeta> workflow = HopServerResource.getWorkflow( id );
    HopServerObjectEntry entry = HopServerResource.getHopServerObjectEntry( id );
    HopLogStore.discardLines( workflow.getLogChannelId(), true );
    HopServerSingleton.getInstance().getWorkflowMap().removeWorkflow( entry );
    return Response.ok().build();
  }

  @PUT
  @Path( "/add" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public WorkflowStatus addJob( String xml ) {

    // Parse the XML, create a workflow configuration
    //
    WorkflowConfiguration workflowConfiguration;
    try {
      workflowConfiguration = WorkflowConfiguration.fromXML( xml );
      IHopMetadataProvider metadataProvider = workflowConfiguration.getMetadataProvider();
      WorkflowMeta workflowMeta = workflowConfiguration.getWorkflowMeta();
      WorkflowExecutionConfiguration workflowExecutionConfiguration = workflowConfiguration.getWorkflowExecutionConfiguration();
      workflowMeta.setLogLevel( workflowExecutionConfiguration.getLogLevel() );
      workflowMeta.injectVariables( workflowExecutionConfiguration.getVariablesMap() );

      String serverObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject = new SimpleLoggingObject( getClass().getName(), LoggingObjectType.HOP_SERVER, null );
      servletLoggingObject.setContainerObjectId( serverObjectId );
      servletLoggingObject.setLogLevel( workflowExecutionConfiguration.getLogLevel() );

      // Create the workflow and store in the list...
      //
      String runConfigurationName = workflowConfiguration.getWorkflowExecutionConfiguration().getRunConfiguration();
      final IWorkflowEngine<WorkflowMeta> workflow = WorkflowEngineFactory.createWorkflowEngine( runConfigurationName, metadataProvider, workflowMeta, servletLoggingObject );

      // Setting variables
      //
      workflow.initializeVariablesFrom( null );
      workflow.getWorkflowMeta().setInternalHopVariables( workflow );
      workflow.injectVariables( workflowConfiguration.getWorkflowExecutionConfiguration().getVariablesMap() );

      // Also copy the parameters over...
      //
      workflow.copyParametersFrom( workflowMeta );
      workflow.clearParameters();
      String[] parameterNames = workflow.listParameters();
      for ( int idx = 0; idx < parameterNames.length; idx++ ) {
        // Grab the parameter value set in the action
        //
        String thisValue = workflowExecutionConfiguration.getParametersMap().get( parameterNames[ idx ] );
        if ( !Utils.isEmpty( thisValue ) ) {
          // Set the value as specified by the user in the action
          //
          workflowMeta.setParameterValue( parameterNames[ idx ], thisValue );
        }
      }
      workflowMeta.activateParameters();

      HopServerSingleton.getInstance().getWorkflowMap().addWorkflow( workflow.getWorkflowName(), serverObjectId, workflow, workflowConfiguration );

      return getWorkflowStatus( serverObjectId );
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    return null;
  }
}
