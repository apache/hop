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

import org.apache.hop.cluster.SlaveServer;
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
import org.apache.hop.workflow.Workflow;
import org.apache.hop.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class HopServerSingleton {

  private static Class<?> PKG = HopServer.class; // for i18n purposes, needed by Translator!!

  private static SlaveServerConfig slaveServerConfig;
  private static HopServerSingleton hopServerSingleton;
  private static HopServer hopServer;

  private ILogChannel log;

  private PipelineMap pipelineMap;
  private WorkflowMap workflowMap;
  private List<SlaveServerDetection> detections;
  private SocketRepository socketRepository;

  private HopServerSingleton( SlaveServerConfig config ) throws HopException {
    HopEnvironment.init();
    HopLogStore.init( config.getMaxLogLines(), config.getMaxLogTimeoutMinutes() );

    this.log = new LogChannel( "HopServer" );
    pipelineMap = new PipelineMap();
    pipelineMap.setSlaveServerConfig( config );
    workflowMap = new WorkflowMap();
    workflowMap.setSlaveServerConfig( config );
    detections = new ArrayList<SlaveServerDetection>();
    socketRepository = new SocketRepository( log );

    installPurgeTimer( config, log, pipelineMap, workflowMap );

    SlaveServer slaveServer = config.getSlaveServer();
    if ( slaveServer != null ) {
      int port = WebServer.PORT;
      if ( !Utils.isEmpty( slaveServer.getPort() ) ) {
        try {
          port = Integer.parseInt( slaveServer.getPort() );
        } catch ( Exception e ) {
          log.logError( BaseMessages.getString( PKG, "HopServer.Error.CanNotPartPort", slaveServer.getHostname(), ""
            + port ), e );
        }
      }

      // TODO: see if we need to keep doing this on a periodic basis.
      // The master might be dead or not alive yet at the time we send this
      // message.
      // Repeating the registration over and over every few minutes might
      // harden this sort of problems.
      //
      if ( config.isReportingToMasters() ) {
        String hostname = slaveServer.getHostname();
        final SlaveServer client =
          new SlaveServer( "Dynamic slave [" + hostname + ":" + port + "]", hostname, "" + port, slaveServer
            .getUsername(), slaveServer.getPassword() );
        for ( final SlaveServer master : config.getMasters() ) {
          // Here we use the username/password specified in the slave
          // server section of the configuration.
          // This doesn't have to be the same pair as the one used on the
          // master!
          //
          try {
            SlaveServerDetection slaveServerDetection = new SlaveServerDetection( client );
            master.sendXML( slaveServerDetection.getXml(), RegisterSlaveServlet.CONTEXT_PATH + "/" );
            log.logBasic( "Registered this slave server to master slave server ["
              + master.toString() + "] on address [" + master.getServerAndPort() + "]" );
          } catch ( Exception e ) {
            log.logError( "Unable to register to master slave server ["
              + master.toString() + "] on address [" + master.getServerAndPort() + "]" );
          }
        }
      }
    }
  }

  public static void installPurgeTimer( final SlaveServerConfig config, final ILogChannel log,
                                        final PipelineMap pipelineMap, final WorkflowMap workflowMap ) {

    final int objectTimeout;
    String systemTimeout = EnvUtil.getSystemProperty( Const.HOP_CARTE_OBJECT_TIMEOUT_MINUTES, null );

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
                Pipeline pipeline = pipelineMap.getPipeline( entry );

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

                    log.logMinimal( "Cleaned up pipeline "
                      + entry.getName() + " with id " + entry.getId() + " from " + pipeline.getExecutionStartDate()
                      + ", diff=" + diffInMinutes );
                  }
                }
              }

              // And the workflows...
              //
              for ( HopServerObjectEntry entry : workflowMap.getWorkflowObjects() ) {
                Workflow workflow = workflowMap.getWorkflow( entry );

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

                    workflowMap.removeJob( entry );

                    log.logMinimal( "Cleaned up workflow "
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
        if ( slaveServerConfig == null ) {
          slaveServerConfig = new SlaveServerConfig();
          SlaveServer slaveServer = new SlaveServer();
          slaveServerConfig.setSlaveServer( slaveServer );
        }

        hopServerSingleton = new HopServerSingleton( slaveServerConfig );

        String carteObjectId = UUID.randomUUID().toString();
        SimpleLoggingObject servletLoggingObject =
          new SimpleLoggingObject( "HopServerSingleton", LoggingObjectType.CARTE, null );
        servletLoggingObject.setContainerObjectId( carteObjectId );
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

  public List<SlaveServerDetection> getDetections() {
    return detections;
  }

  public void setDetections( List<SlaveServerDetection> detections ) {
    this.detections = detections;
  }

  public SocketRepository getSocketRepository() {
    return socketRepository;
  }

  public void setSocketRepository( SocketRepository socketRepository ) {
    this.socketRepository = socketRepository;
  }

  public static SlaveServerConfig getSlaveServerConfig() {
    return slaveServerConfig;
  }

  public static void setSlaveServerConfig( SlaveServerConfig slaveServerConfig ) {
    HopServerSingleton.slaveServerConfig = slaveServerConfig;
  }

  public static void setHopServer( HopServer hopServer ) {
    HopServerSingleton.hopServer = hopServer;
  }

  public static HopServer getHopServer() {
    return HopServerSingleton.hopServer;
  }

  public ILogChannel getLog() {
    return log;
  }
}
