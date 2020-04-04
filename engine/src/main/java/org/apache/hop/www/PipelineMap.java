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


import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This is a map between the pipeline name and the (running/waiting/finished) pipeline.
 *
 * @author Matt
 */
public class PipelineMap {
  private final Map<HopServerObjectEntry, PipelineData> pipelineMap;

  private final Map<String, List<SocketPortAllocation>> hostServerSocketPortsMap;

  private SlaveServerConfig slaveServerConfig;

  public PipelineMap() {
    pipelineMap = new ConcurrentHashMap<>();
    hostServerSocketPortsMap = new ConcurrentHashMap<>();
  }

  /**
   * Add a pipeline to the map
   *
   * @param pipelineName The name of the pipeline to add
   * @param containerObjectId  the unique ID of the pipeline in this container.
   * @param pipeline              The pipeline to add
   * @param pipelineConfiguration the pipeline configuration to add
   */
  public void addPipeline( String pipelineName, String containerObjectId, Pipeline pipeline,
                           PipelineConfiguration pipelineConfiguration ) {
    HopServerObjectEntry entry = new HopServerObjectEntry( pipelineName, containerObjectId );
    pipelineMap.put( entry, new PipelineData( pipeline, pipelineConfiguration ) );
  }

  public void registerPipeline( Pipeline pipeline, PipelineConfiguration pipelineConfiguration ) {
    pipeline.setContainerObjectId( UUID.randomUUID().toString() );
    HopServerObjectEntry entry = new HopServerObjectEntry( pipeline.getPipelineMeta().getName(), pipeline.getContainerObjectId() );
    pipelineMap.put( entry, new PipelineData( pipeline, pipelineConfiguration ) );
  }

  /**
   * Find the first pipeline in the list that comes to mind!
   *
   * @param pipelineName
   * @return the first pipeline with the specified name
   */
  public Pipeline getPipeline( String pipelineName ) {
    for ( HopServerObjectEntry entry : pipelineMap.keySet() ) {
      if ( entry.getName().equals( pipelineName ) ) {
        return pipelineMap.get( entry ).getPipeline();
      }
    }
    return null;
  }

  /**
   * @param entry The HopServer pipeline object
   * @return the pipeline with the specified entry
   */
  public Pipeline getPipeline( HopServerObjectEntry entry ) {
    return pipelineMap.get( entry ).getPipeline();
  }

  /**
   * @param pipelineName
   * @return The first pipeline configuration with the specified name
   */
  public PipelineConfiguration getConfiguration( String pipelineName ) {
    for ( HopServerObjectEntry entry : pipelineMap.keySet() ) {
      if ( entry.getName().equals( pipelineName ) ) {
        return pipelineMap.get( entry ).getConfiguration();
      }
    }
    return null;
  }

  /**
   * @param entry The HopServer pipeline object
   * @return the pipeline configuration with the specified entry
   */
  public PipelineConfiguration getConfiguration( HopServerObjectEntry entry ) {
    return pipelineMap.get( entry ).getConfiguration();
  }

  /**
   * @param entry the HopServer object entry
   */
  public void removePipeline( HopServerObjectEntry entry ) {
    pipelineMap.remove( entry );
  }

  public List<HopServerObjectEntry> getPipelineObjects() {
    return new ArrayList<>( pipelineMap.keySet() );
  }


  /**
   * This is the meat of the whole problem. We'll allocate a port for a given slave, pipeline and transform copy,
   * always on the same host. Algorithm: 1) Search for the right map in the hostPortMap
   *
   * @param portRangeStart     the start of the port range as described in the used cluster schema
   * @param hostname           the host name to allocate this address for
   * @param clusteredRunId     A unique id, created for each new clustered run during pipeline split.
   * @param pipelineName
   * @param sourceTransformName
   * @param sourceTransformCopy
   * @return
   */
  public SocketPortAllocation allocateServerSocketPort( int portRangeStart, String hostname,
                                                        String clusteredRunId, String pipelineName, String sourceSlaveName, String sourceTransformName,
                                                        String sourceTransformCopy, String targetSlaveName, String targetTransformName, String targetTransformCopy ) {

    // Do some validations first...
    //
    if ( Utils.isEmpty( clusteredRunId ) ) {
      throw new RuntimeException(
        "A server socket allocation always has to accompanied by a cluster run ID but it was empty" );
    }
    if ( portRangeStart <= 0 ) {
      throw new RuntimeException(
        "A server socket allocation always has to accompanied by port range start > 0 but it was "
          + portRangeStart );
    }
    if ( Utils.isEmpty( hostname ) ) {
      throw new RuntimeException(
        "A server socket allocation always has to accompanied by a hostname but it was empty" );
    }
    if ( Utils.isEmpty( pipelineName ) ) {
      throw new RuntimeException(
        "A server socket allocation always has to accompanied by a pipeline name but it was empty" );
    }
    if ( Utils.isEmpty( sourceSlaveName ) ) {
      throw new RuntimeException(
        "A server socket allocation always has to accompanied by a source slave server name but it was empty" );
    }
    if ( Utils.isEmpty( targetSlaveName ) ) {
      throw new RuntimeException(
        "A server socket allocation always has to accompanied by a target slave server name but it was empty" );
    }
    if ( Utils.isEmpty( sourceTransformName ) ) {
      throw new RuntimeException(
        "A server socket allocation always has to accompanied by a source transform name but it was empty" );
    }
    if ( Utils.isEmpty( targetTransformName ) ) {
      throw new RuntimeException(
        "A server socket allocation always has to accompanied by a target transform name but it was empty" );
    }
    if ( Utils.isEmpty( sourceTransformCopy ) ) {
      throw new RuntimeException(
        "A server socket allocation always has to accompanied by a source transform copy but it was empty" );
    }
    if ( Utils.isEmpty( targetTransformCopy ) ) {
      throw new RuntimeException(
        "A server socket allocation always has to accompanied by a target transform copy but it was empty" );
    }

    // Look up the sockets list for the given host
    //
    List<SocketPortAllocation> serverSocketPorts;
    serverSocketPorts = hostServerSocketPortsMap.computeIfAbsent( hostname, k -> new CopyOnWriteArrayList<>() );
    serverSocketPorts = serverSocketPorts != null ? serverSocketPorts : hostServerSocketPortsMap.get( hostname );

    // Find the socket port allocation in the list...
    //
    SocketPortAllocation socketPortAllocation = null;
    int maxPort = portRangeStart - 1;
    for ( int index = 0; index < serverSocketPorts.size(); index++ ) {
      SocketPortAllocation spa = serverSocketPorts.get( index );
      if ( spa.getPort() > maxPort ) {
        maxPort = spa.getPort();
      }
      synchronized ( spa ) {

        if ( spa.getClusterRunId().equalsIgnoreCase( clusteredRunId )
          && spa.getSourceSlaveName().equalsIgnoreCase( sourceSlaveName )
          && spa.getTargetSlaveName().equalsIgnoreCase( targetSlaveName )
          && spa.getPipelineName().equalsIgnoreCase( pipelineName )
          && spa.getSourceTransformName().equalsIgnoreCase( sourceTransformName )
          && spa.getSourceTransformCopy().equalsIgnoreCase( sourceTransformCopy )
          && spa.getTargetTransformName().equalsIgnoreCase( targetTransformName )
          && spa.getTargetTransformCopy().equalsIgnoreCase( targetTransformCopy ) ) {
          // This is the port we want, return it. Make sure it's allocated.
          //
          spa.setAllocated( true );
          socketPortAllocation = spa;
          break;
        } else {
          // If we find an available spot, take it.
          //
          if ( !spa.isAllocated() ) {
            // This is not an allocated port.
            // So we can basically use this port slot to put our own allocation
            // in it.
            //
            // However, that is ONLY possible if the port belongs to the same
            // slave server couple.
            // Otherwise, we keep on searching.
            //
            if ( spa.getSourceSlaveName().equalsIgnoreCase( sourceSlaveName )
              && spa.getTargetSlaveName().equalsIgnoreCase( targetSlaveName ) ) {
              socketPortAllocation =
                new SocketPortAllocation(
                  spa.getPort(), new Date(), clusteredRunId, pipelineName, sourceSlaveName,
                  sourceTransformName, sourceTransformCopy, targetSlaveName, targetTransformName, targetTransformCopy );
              serverSocketPorts.set( index, socketPortAllocation );
              break;
            }
          }
        }
      }
    }
    if ( socketPortAllocation == null ) {
      // Allocate a new port and add it to the back of the list
      // Normally this list should stay sorted on port number this way
      //
      socketPortAllocation =
        new SocketPortAllocation(
          maxPort + 1, new Date(), clusteredRunId, pipelineName, sourceSlaveName, sourceTransformName,
          sourceTransformCopy, targetSlaveName, targetTransformName, targetTransformCopy );
      serverSocketPorts.add( socketPortAllocation );
    }

    // DEBUG : Do a verification on the content of the list.
    // If we find a port twice in the list, complain!
    //
    /*
     * for (int i = 0; i < serverSocketPortsMap.size(); i++) { for (int j = 0; j < serverSocketPortsMap.size(); j++)
     * { if (i != j) { SocketPortAllocation one = serverSocketPortsMap.get(i); SocketPortAllocation two =
     * serverSocketPortsMap.get(j); if (one.getPort() == two.getPort()) { throw new
     * RuntimeException("Error detected !! Identical ports discovered in the ports list."); } } } }
     */

    // give back the good news too...
    //
    return socketPortAllocation;
  }

  /**
   * Deallocate all the ports for the given pipeline name, across all hosts.
   *
   * @param pipelineName     the pipeline name to release
   * @param carteObjectId the carte object ID to reference
   */
  public void deallocateServerSocketPorts( String pipelineName, String carteObjectId ) {
    for ( String hostname : hostServerSocketPortsMap.keySet() ) {
      List<SocketPortAllocation> spas = hostServerSocketPortsMap.get( hostname );
      for ( SocketPortAllocation spa : spas ) {
        synchronized ( spa ) {
          if ( spa.getPipelineName().equalsIgnoreCase( pipelineName )
            && ( Utils.isEmpty( carteObjectId ) || spa.getClusterRunId().equals( carteObjectId ) ) ) {
            spa.setAllocated( false );
          }
        }
      }
    }
  }

  /**
   * Deallocate all the ports for the given pipeline entry, across all hosts.
   *
   * @param entry the pipeline object entry name to release the sockets for
   */
  public void deallocateServerSocketPorts( HopServerObjectEntry entry ) {
    for ( String hostname : hostServerSocketPortsMap.keySet() ) {
      List<SocketPortAllocation> serverSocketPorts = hostServerSocketPortsMap.get( hostname );
      for ( SocketPortAllocation spa : hostServerSocketPortsMap.get( hostname ) ) {
        synchronized ( spa ) {
          if ( spa.getPipelineName().equalsIgnoreCase( entry.getName() ) ) {
            spa.setAllocated( false );
          }
        }
      }
    }
  }

  public void deallocateServerSocketPort( int port, String hostname ) {
    // Look up the sockets list for the given host
    //
    List<SocketPortAllocation> serverSocketPorts = hostServerSocketPortsMap.get( hostname );

    if ( serverSocketPorts == null ) {
      return; // nothing to deallocate
    }
    // Find the socket port allocation in the list...
    //
    for ( SocketPortAllocation spa : serverSocketPorts ) {
      synchronized ( spa ) {
        if ( spa.getPort() == port ) {
          spa.setAllocated( false );
          return;
        }
      }
    }
  }

  public HopServerObjectEntry getFirstCarteObjectEntry( String pipelineName ) {
    for ( HopServerObjectEntry key : pipelineMap.keySet() ) {
      if ( key.getName().equals( pipelineName ) ) {
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
   * @return the hostServerSocketPortsMap
   */
  public List<SocketPortAllocation> getHostServerSocketPorts( String hostname ) {
    List<SocketPortAllocation> ports = hostServerSocketPortsMap.get( hostname );
    return ports == null ? Collections.emptyList() : Collections.unmodifiableList( ports );
  }

  public SlaveSequence getSlaveSequence( String name ) {
    return SlaveSequence.findSlaveSequence( name, slaveServerConfig.getSlaveSequences() );
  }

  public boolean isAutomaticSlaveSequenceCreationAllowed() {
    return slaveServerConfig.isAutomaticCreationAllowed();
  }

  public SlaveSequence createSlaveSequence( String name ) throws HopException {
    SlaveSequence auto = slaveServerConfig.getAutoSequence();
    if ( auto == null ) {
      throw new HopException( "No auto-sequence information found in the slave server config.  "
        + "Slave sequence could not be created automatically." );
    }

    SlaveSequence slaveSequence =
      new SlaveSequence( name, auto.getStartValue(), auto.getDatabaseMeta(), auto.getSchemaName(), auto
        .getTableName(), auto.getSequenceNameField(), auto.getValueField() );

    slaveServerConfig.getSlaveSequences().add( slaveSequence );

    return slaveSequence;
  }

  private static class PipelineData {

    private Pipeline pipeline;

    private PipelineConfiguration configuration;

    PipelineData( Pipeline pipeline, PipelineConfiguration configuration ) {
      this.pipeline = pipeline;
      this.configuration = configuration;
    }

    public Pipeline getPipeline() {
      return pipeline;
    }

    public void setPipeline( Pipeline pipeline ) {
      this.pipeline = pipeline;
    }

    public PipelineConfiguration getConfiguration() {
      return configuration;
    }

    public void setConfiguration( PipelineConfiguration configuration ) {
      this.configuration = configuration;
    }
  }
}
