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

import org.apache.hop.server.HopServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class HopServerSingleton {

  private static final Class<?> PKG = org.apache.hop.www.HopServer.class; // For Translator

  private static HopServerConfig hopServerConfig;
  private static HopServerSingleton hopServerSingleton;
  private static org.apache.hop.www.HopServer hopServer;

  private ILogChannel log;

  private PipelineMap pipelineMap;
  private WorkflowMap workflowMap;

  private HopServerSingleton( HopServerConfig config ) throws HopException {
    HopEnvironment.init();
    HopLogStore.init();

    this.log = new LogChannel( "HopServer" );
    pipelineMap = new PipelineMap();
    pipelineMap.setHopServerConfig( config );
    workflowMap = new WorkflowMap();
    workflowMap.setHopServerConfig( config );

    installPurgeTimer( config, log, pipelineMap, workflowMap );

    org.apache.hop.server.HopServer hopServer = config.getHopServer();
    if ( hopServer != null ) {
      int port = WebServer.PORT;
      if ( !Utils.isEmpty( hopServer.getPort() ) ) {
        try {
          port = Integer.parseInt( hopServer.getPort() );
        } catch ( Exception e ) {
          log.logError( BaseMessages.getString( PKG, "HopServer.Error.CanNotPartPort", hopServer.getHostname(), ""
            + port ), e );
        }
      }
    }
  }

  public static void installPurgeTimer( final HopServerConfig config, final ILogChannel log,
                                        final PipelineMap pipelineMap, final WorkflowMap workflowMap ) {

    final int objectTimeout;
    String systemTimeout = EnvUtil.getSystemProperty( Const.HOP_SERVER_OBJECT_TIMEOUT_MINUTES, null );

    // The value specified in XML takes precedence over the environment variable!
    //
    if ( config.getObjectTimeoutMinutes() > 0 ) {
      objectTimeout = config.getObjectTimeoutMinutes();
    } else if ( !Utils.isEmpty( systemTimeout ) ) {
      objectTimeout = Const.toInt( systemTimeout, 1440 );
    } else {
      objectTimeout = 24 * 60; // 1440 : default is a one day time-out
    }

    // If we need to time out finished or idle objects, we should create a timer
    // in the background to clean
    //
    if ( objectTimeout > 0 ) {

      log.logBasic( "Installing timer to purge stale objects after " + objectTimeout + " minutes." );

      Timer timer = new Timer( true );

      final AtomicBoolean busy = new AtomicBoolean( false );
      TimerTask timerTask = new TimerTask() {
        public void run() {
          if ( !busy.get() ) {
            busy.set( true );

            try {
              // Check all pipelines...
              //
              for ( HopServerObjectEntry entry : pipelineMap.getPipelineObjects() ) {
                IPipelineEngine<PipelineMeta> pipeline = pipelineMap.getPipeline( entry );

                // See if the pipeline is finished or stopped.
                //
                if ( pipeline != null && ( pipeline.isFinished() || pipeline.isStopped() ) && pipeline.getExecutionStartDate() != null ) {
                  // check the last log time
                  //
                  int diffInMinutes =
                    (int) Math.floor( ( System.currentTimeMillis() - pipeline.getExecutionStartDate().getTime() ) / 60000 );
                  if ( diffInMinutes >= objectTimeout ) {
                    // Let's remove this from the pipeline map...
                    //
                    pipelineMap.removePipeline( entry );

                    // Remove the logging information from the log registry & central log store
                    //
                    LoggingRegistry.getInstance().removeIncludingChildren( pipeline.getLogChannelId() );
                    HopLogStore.discardLines( pipeline.getLogChannelId(), false );

                    // pipelineMap.deallocateServerSocketPorts(entry);

                    log.logBasic( "Cleaned up pipeline "
                      + entry.getName() + " with id " + entry.getId() + " from " + pipeline.getExecutionStartDate()
                      + ", diff=" + diffInMinutes );
                  }
                }
              }

              // And the workflows...
              //
              for ( HopServerObjectEntry entry : workflowMap.getWorkflowObjects() ) {
                IWorkflowEngine<WorkflowMeta> workflow = workflowMap.getWorkflow( entry );

                // See if the workflow is finished or stopped.
                //
                if ( workflow != null && ( workflow.isFinished() || workflow.isStopped() ) && workflow.getExecutionStartDate() != null ) {
                  // check the last log time
                  //
                  int diffInMinutes =
                    (int) Math.floor( ( System.currentTimeMillis() - workflow.getExecutionStartDate().getTime() ) / 60000 );
                  if ( diffInMinutes >= objectTimeout ) {
                    // Let's remove this from the workflow map...
                    //
                    String id = workflowMap.getWorkflow( entry ).getLogChannelId();
                    LoggingRegistry.getInstance().removeLogChannelFileWriterBuffer( id );

                    workflowMap.removeWorkflow( entry );

                    log.logBasic( "Cleaned up workflow "
                      + entry.getName() + " with id " + entry.getId() + " from " + workflow.getExecutionStartDate() );
                  }
                }
              }

            } finally {
              busy.set( false );
            }
          }
        }
      };

      // Search for stale objects every 20 seconds:
      //
      timer.schedule( timerTask, 20000, 20000 );
    }
  }

  public static HopServerSingleton getInstance() {
    try {
      if ( hopServerSingleton == null ) {
        if ( hopServerConfig == null ) {
          hopServerConfig = new HopServerConfig();
          org.apache.hop.server.HopServer hopServer = new HopServer();
          hopServerConfig.setHopServer( hopServer );
        }

        hopServerSingleton = new HopServerSingleton( hopServerConfig );

        String serverObjectId = UUID.randomUUID().toString();
        SimpleLoggingObject servletLoggingObject =
          new SimpleLoggingObject( "HopServerSingleton", LoggingObjectType.HOP_SERVER, null );
        servletLoggingObject.setContainerObjectId( serverObjectId );
        servletLoggingObject.setLogLevel( LogLevel.BASIC );

        return hopServerSingleton;
      } else {
        return hopServerSingleton;
      }
    } catch ( HopException ke ) {
      throw new RuntimeException( ke );
    }
  }

  public PipelineMap getPipelineMap() {
    return pipelineMap;
  }

  public void setPipelineMap( PipelineMap pipelineMap ) {
    this.pipelineMap = pipelineMap;
  }

  public WorkflowMap getWorkflowMap() {
    return workflowMap;
  }

  public void setWorkflowMap( WorkflowMap workflowMap ) {
    this.workflowMap = workflowMap;
  }

   public static HopServerConfig getHopServerConfig() {
    return hopServerConfig;
  }

  public static void setHopServerConfig( HopServerConfig hopServerConfig ) {
    HopServerSingleton.hopServerConfig = hopServerConfig;
  }

  public static void setHopServer( org.apache.hop.www.HopServer hopServer ) {
    HopServerSingleton.hopServer = hopServer;
  }

  public static org.apache.hop.www.HopServer getHopServer() {
    return HopServerSingleton.hopServer;
  }

  public ILogChannel getLog() {
    return log;
  }
}
