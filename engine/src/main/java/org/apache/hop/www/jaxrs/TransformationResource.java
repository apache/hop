/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import java.util.Map;
import java.util.UUID;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransAdapter;
import org.apache.hop.trans.TransConfiguration;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepData.StepExecutionStatus;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepStatus;
import org.apache.hop.www.HopServerObjectEntry;
import org.apache.hop.www.HopServerSingleton;

@Path( "/carte/trans" )
public class TransformationResource {

  public TransformationResource() {
  }

  @GET
  @Path( "/log/{id : .+}" )
  @Produces( { MediaType.TEXT_PLAIN } )
  public String getTransformationLog( @PathParam( "id" ) String id ) {
    return getTransformationLog( id, 0 );
  }

  @GET
  @Path( "/log/{id : .+}/{logStart : .+}" )
  @Produces( { MediaType.TEXT_PLAIN } )
  public String getTransformationLog( @PathParam( "id" ) String id, @PathParam( "logStart" ) int startLineNr ) {
    int lastLineNr = HopLogStore.getLastBufferLineNr();
    Trans trans = HopServerResource.getTransformation( id );
    String logText =
      HopLogStore.getAppender().getBuffer(
        trans.getLogChannel().getLogChannelId(), false, startLineNr, lastLineNr ).toString();
    return logText;
  }

  @GET
  @Path( "/status/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public TransformationStatus getTransformationStatus( @PathParam( "id" ) String id ) {
    TransformationStatus status = new TransformationStatus();
    // find trans
    Trans trans = HopServerResource.getTransformation( id );
    HopServerObjectEntry entry = HopServerResource.getCarteObjectEntry( id );

    status.setId( entry.getId() );
    status.setName( entry.getName() );
    status.setStatus( trans.getStatus() );

    for ( int i = 0; i < trans.nrSteps(); i++ ) {
      StepInterface step = trans.getRunThread( i );
      if ( ( step.isRunning() ) || step.getStatus() != StepExecutionStatus.STATUS_EMPTY ) {
        StepStatus stepStatus = new StepStatus( step );
        status.addStepStatus( stepStatus );
      }
    }
    return status;
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/start/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public TransformationStatus startTransformation( @PathParam( "id" ) String id ) {
    Trans trans = HopServerResource.getTransformation( id );
    try {
      // Discard old log lines from old transformation runs
      //
      HopLogStore.discardLines( trans.getLogChannelId(), true );

      String carteObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject( getClass().getName(), LoggingObjectType.CARTE, null );
      servletLoggingObject.setContainerObjectId( carteObjectId );
      servletLoggingObject.setLogLevel( trans.getLogLevel() );
      trans.setParent( servletLoggingObject );
      trans.execute( null );
    } catch ( HopException e ) {
      e.printStackTrace();
    }
    return getTransformationStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/prepare/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public TransformationStatus prepareTransformation( @PathParam( "id" ) String id ) {
    Trans trans = HopServerResource.getTransformation( id );
    try {

      HopServerObjectEntry entry = HopServerResource.getCarteObjectEntry( id );
      TransConfiguration transConfiguration =
        HopServerSingleton.getInstance().getTransformationMap().getConfiguration( entry );
      TransExecutionConfiguration executionConfiguration = transConfiguration.getTransExecutionConfiguration();
      // Set the appropriate logging, variables, arguments, replay date, ...
      // etc.
      trans.setArguments( executionConfiguration.getArgumentStrings() );
      trans.setReplayDate( executionConfiguration.getReplayDate() );
      trans.setSafeModeEnabled( executionConfiguration.isSafeModeEnabled() );
      trans.setGatheringMetrics( executionConfiguration.isGatheringMetrics() );
      trans.injectVariables( executionConfiguration.getVariables() );
      trans.setPreviousResult( executionConfiguration.getPreviousResult() );

      trans.prepareExecution( null );
    } catch ( HopException e ) {
      e.printStackTrace();
    }
    return getTransformationStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/pause/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public TransformationStatus pauseTransformation( @PathParam( "id" ) String id ) {
    HopServerResource.getTransformation( id ).pauseRunning();
    return getTransformationStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/resume/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public TransformationStatus resumeTransformation( @PathParam( "id" ) String id ) {
    HopServerResource.getTransformation( id ).resumeRunning();
    return getTransformationStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/stop/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public TransformationStatus stopTransformation( @PathParam( "id" ) String id ) {
    HopServerResource.getTransformation( id ).stopAll();
    return getTransformationStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/safeStop/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public TransformationStatus safeStopTransformation( @PathParam( "id" ) String id ) {
    HopServerResource.getTransformation( id ).safeStop();
    return getTransformationStatus( id );
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/remove/{id : .+}" )
  public Response removeTransformation( @PathParam( "id" ) String id ) {
    Trans trans = HopServerResource.getTransformation( id );
    HopServerObjectEntry entry = HopServerResource.getCarteObjectEntry( id );
    HopLogStore.discardLines( trans.getLogChannelId(), true );
    HopServerSingleton.getInstance().getTransformationMap().removeTransformation( entry );
    return Response.ok().build();
  }

  // change from GET to UPDATE/POST for proper REST method
  @GET
  @Path( "/cleanup/{id : .+}" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public TransformationStatus cleanupTransformation( @PathParam( "id" ) String id ) {
    HopServerResource.getTransformation( id ).cleanup();
    return getTransformationStatus( id );
  }

  @PUT
  @Path( "/add" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public TransformationStatus addTransformation( String xml ) {
    TransConfiguration transConfiguration;
    try {
      transConfiguration = TransConfiguration.fromXML( xml.toString() );
      TransMeta transMeta = transConfiguration.getTransMeta();
      TransExecutionConfiguration transExecutionConfiguration =
        transConfiguration.getTransExecutionConfiguration();
      transMeta.setLogLevel( transExecutionConfiguration.getLogLevel() );
      LogChannelInterface log = HopServerSingleton.getInstance().getLog();
      if ( log.isDetailed() ) {
        log.logDetailed( "Logging level set to " + log.getLogLevel().getDescription() );
      }
      transMeta.injectVariables( transExecutionConfiguration.getVariables() );

      // Also copy the parameters over...
      //
      Map<String, String> params = transExecutionConfiguration.getParams();
      for ( String param : params.keySet() ) {
        String value = params.get( param );
        transMeta.setParameterValue( param, value );
      }

      // If there was a repository, we know about it at this point in time.
      //
      TransExecutionConfiguration executionConfiguration = transConfiguration.getTransExecutionConfiguration();
      final Repository repository = transConfiguration.getTransExecutionConfiguration().getRepository();

      String carteObjectId = UUID.randomUUID().toString();
      SimpleLoggingObject servletLoggingObject =
        new SimpleLoggingObject( getClass().getName(), LoggingObjectType.CARTE, null );
      servletLoggingObject.setContainerObjectId( carteObjectId );
      servletLoggingObject.setLogLevel( executionConfiguration.getLogLevel() );

      // Create the transformation and store in the list...
      //
      final Trans trans = new Trans( transMeta, servletLoggingObject );

      trans.setRepository( repository );
      trans.setSocketRepository( HopServerSingleton.getInstance().getSocketRepository() );

      HopServerSingleton.getInstance().getTransformationMap().addTransformation(
        transMeta.getName(), carteObjectId, trans, transConfiguration );
      trans.setContainerObjectId( carteObjectId );

      if ( repository != null ) {
        // The repository connection is open: make sure we disconnect from the repository once we
        // are done with this transformation.
        //
        trans.addTransListener( new TransAdapter() {
          @Override public void transFinished( Trans trans ) {
            repository.disconnect();
          }
        } );
      }

      return getTransformationStatus( carteObjectId );
    } catch ( HopException e ) {
      e.printStackTrace();
    }
    return null;
  }

}
