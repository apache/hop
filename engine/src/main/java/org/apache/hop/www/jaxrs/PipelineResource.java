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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.transform.TransformStatus;
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

@Path( "/carte/pipeline" )
public class PipelineResource {

  public PipelineResource() {
  }

  @GET
  @Path( "/log/{id : .+}" )
  @Produces( { MediaType.TEXT_PLAIN } )
  public String getPipelineLog( @PathParam( "id" ) String id ) {
    return getPipelineLog( id, 0 );
  }

  @GET
  @Path( "/log/{id : .+}/{logStart : .+}" )
  @Produces( { MediaType.TEXT_PLAIN } )
  public String getPipelineLog( @PathParam( "id" ) String id, @PathParam( "logStart" ) int startLineNr ) {
    int lastLineNr = HopLogStore.getLastBufferLineNr();
    IPipelineEngine<PipelineMeta> pipeline = HopServerResource.getPipeline( id );
    String logText =
      HopLogStore.getAppender().getBuffer(
        pipeline.getLogChannel().getLogChannelId(), false, startLineNr, lastLineNr ).toString();
    return logText;
  }

  @GET
  @Path( "/status/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public PipelineStatus getPipelineStatus( @PathParam( "id" ) String id ) {
    PipelineStatus status = new PipelineStatus();
    // find pipeline
    IPipelineEngine<PipelineMeta> pipeline = HopServerResource.getPipeline( id );
    HopServerObjectEntry entry = HopServerResource.getHopServerObjectEntry( id );

    status.setId( entry.getId() );
    status.setName( entry.getName() );
    status.setStatus( pipeline.getStatusDescription() );

    for ( IEngineComponent component : pipeline.getComponents() ) {
      if ( ( component.isRunning() ) || ( component.getStatus() != ComponentExecutionStatus.STATUS_EMPTY ) ) {
        TransformStatus transformStatus = new TransformStatus( component );
        status.addTransformStatus( transformStatus );
      }
    }
    return status;
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/start/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public PipelineStatus startPipeline( @PathParam( "id" ) String id ) {
    IPipelineEngine<PipelineMeta> pipeline = HopServerResource.getPipeline( id );
    try {
      // Discard old log lines from old pipeline runs
      //
      HopLogStore.discardLines( pipeline.getLogChannelId(), true );

      String serverObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject( getClass().getName(), LoggingObjectType.HOP_SERVER, null );
      servletLoggingObject.setContainerObjectId( serverObjectId );
      servletLoggingObject.setLogLevel( pipeline.getLogLevel() );
      pipeline.setParent( servletLoggingObject );
      pipeline.execute();
    } catch ( HopException e ) {
      e.printStackTrace();
    }
    return getPipelineStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/prepare/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public PipelineStatus preparePipeline( @PathParam( "id" ) String id ) {
    IPipelineEngine<PipelineMeta> pipeline = HopServerResource.getPipeline( id );
    try {

      HopServerObjectEntry entry = HopServerResource.getHopServerObjectEntry( id );
      PipelineConfiguration pipelineConfiguration = HopServerSingleton.getInstance().getPipelineMap().getConfiguration( entry );
      PipelineExecutionConfiguration executionConfiguration = pipelineConfiguration.getPipelineExecutionConfiguration();
      // Set the appropriate logging, variables, arguments, replay date, ...
      // etc.
      pipeline.setVariables( executionConfiguration.getVariablesMap() );
      pipeline.setPreviousResult( executionConfiguration.getPreviousResult() );

      pipeline.prepareExecution();
    } catch ( HopException e ) {
      e.printStackTrace();
    }
    return getPipelineStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/pause/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public PipelineStatus pausePipeline( @PathParam( "id" ) String id ) {
    HopServerResource.getPipeline( id ).pauseExecution();
    return getPipelineStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/resume/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public PipelineStatus resumePipeline( @PathParam( "id" ) String id ) {
    HopServerResource.getPipeline( id ).resumeExecution();
    return getPipelineStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/stop/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public PipelineStatus stopPipeline( @PathParam( "id" ) String id ) {
    HopServerResource.getPipeline( id ).stopAll();
    return getPipelineStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/remove/{id : .+}" )
  public Response removePipeline( @PathParam( "id" ) String id ) {
    IPipelineEngine<PipelineMeta> pipeline = HopServerResource.getPipeline( id );
    HopServerObjectEntry entry = HopServerResource.getHopServerObjectEntry( id );
    HopLogStore.discardLines( pipeline.getLogChannelId(), true );
    HopServerSingleton.getInstance().getPipelineMap().removePipeline( entry );
    return Response.ok().build();
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/cleanup/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public PipelineStatus cleanupPipeline( @PathParam( "id" ) String id ) {
    HopServerResource.getPipeline( id ).cleanup();
    return getPipelineStatus( id );
  }

  @PUT
  @Path( "/add" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public PipelineStatus addPipeline( String xml ) {
    PipelineConfiguration pipelineConfiguration;
    try {
      pipelineConfiguration = PipelineConfiguration.fromXml( xml.toString() );
      IHopMetadataProvider metadataProvider = pipelineConfiguration.getMetadataProvider();
      PipelineMeta pipelineMeta = pipelineConfiguration.getPipelineMeta();
      PipelineExecutionConfiguration pipelineExecutionConfiguration = pipelineConfiguration.getPipelineExecutionConfiguration();
      pipelineMeta.setLogLevel( pipelineExecutionConfiguration.getLogLevel() );
      ILogChannel log = HopServerSingleton.getInstance().getLog();
      if ( log.isDetailed() ) {
        log.logDetailed( "Logging level set to " + log.getLogLevel().getDescription() );
      }

      PipelineExecutionConfiguration executionConfiguration = pipelineConfiguration.getPipelineExecutionConfiguration();

      String serverObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject( getClass().getName(), LoggingObjectType.HOP_SERVER, null );
      servletLoggingObject.setContainerObjectId( serverObjectId );
      servletLoggingObject.setLogLevel( executionConfiguration.getLogLevel() );

      // Create the pipeline and store in the list...
      //
      String runConfigurationName = executionConfiguration.getRunConfiguration();
      IVariables variables = Variables.getADefaultVariableSpace(); // TODO: configure
      final IPipelineEngine pipeline = PipelineEngineFactory.createPipelineEngine( variables, runConfigurationName, metadataProvider, pipelineMeta);
      pipeline.setParent( servletLoggingObject );

      HopServerSingleton.getInstance().getPipelineMap().addPipeline(pipelineMeta.getName(), serverObjectId, pipeline, pipelineConfiguration );
      pipeline.setContainerId( serverObjectId );

      return getPipelineStatus( serverObjectId );
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    return null;
  }

}
