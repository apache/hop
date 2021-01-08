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


import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is a map between the pipeline name and the (running/waiting/finished) pipeline.
 *
 * @author Matt
 */
public class PipelineMap {
  private final Map<HopServerObjectEntry, PipelineData> pipelineMap;

  private HopServerConfig hopServerConfig;

  public PipelineMap() {
    pipelineMap = new ConcurrentHashMap<>();
  }

  /**
   * Add a pipeline to the map
   *
   * @param pipelineName The name of the pipeline to add
   * @param containerObjectId  the unique ID of the pipeline in this container.
   * @param pipeline              The pipeline to add
   * @param pipelineConfiguration the pipeline configuration to add
   */
  public void addPipeline( String pipelineName, String containerObjectId, IPipelineEngine<PipelineMeta> pipeline,
                           PipelineConfiguration pipelineConfiguration ) {
    HopServerObjectEntry entry = new HopServerObjectEntry( pipelineName, containerObjectId );
    pipelineMap.put( entry, new PipelineData( pipeline, pipelineConfiguration ) );
  }

  public void registerPipeline( Pipeline pipeline, PipelineConfiguration pipelineConfiguration ) {
    pipeline.setContainerId( UUID.randomUUID().toString() );
    HopServerObjectEntry entry = new HopServerObjectEntry( pipeline.getPipelineMeta().getName(), pipeline.getContainerId() );
    pipelineMap.put( entry, new PipelineData( pipeline, pipelineConfiguration ) );
  }

  /**
   * Find the first pipeline in the list that comes to mind!
   *
   * @param pipelineName
   * @return the first pipeline with the specified name
   */
  public IPipelineEngine<PipelineMeta> getPipeline( String pipelineName ) {
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
  public IPipelineEngine<PipelineMeta> getPipeline( HopServerObjectEntry entry ) {
    PipelineData pipelineData = pipelineMap.get( entry );
    if (pipelineData!=null) {
      return pipelineData.getPipeline();
    }
    return null;
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


  public HopServerObjectEntry getFirstServerObjectEntry( String pipelineName ) {
    for ( HopServerObjectEntry key : pipelineMap.keySet() ) {
      if ( key.getName().equals( pipelineName ) ) {
        return key;
      }
    }
    return null;
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

  public HopServerSequence getServerSequence( String name ) {
    return HopServerSequence.findServerSequence( name, hopServerConfig.getHopServerSequences() );
  }

  public boolean isAutomaticServerSequenceCreationAllowed() {
    return hopServerConfig.isAutomaticCreationAllowed();
  }

  public HopServerSequence createServerSequence( String name ) throws HopException {
    HopServerSequence auto = hopServerConfig.getAutoSequence();
    if ( auto == null ) {
      throw new HopException( "No auto-sequence information found in the hop server config.  "
        + "Server sequence could not be created automatically." );
    }

    HopServerSequence hopServerSequence =
      new HopServerSequence( name, auto.getStartValue(), auto.getDatabaseMeta(), auto.getSchemaName(), auto
        .getTableName(), auto.getSequenceNameField(), auto.getValueField() );

    hopServerConfig.getHopServerSequences().add( hopServerSequence );

    return hopServerSequence;
  }

  private static class PipelineData {

    private IPipelineEngine<PipelineMeta> pipeline;

    private PipelineConfiguration configuration;

    PipelineData( IPipelineEngine<PipelineMeta> pipeline, PipelineConfiguration configuration ) {
      this.pipeline = pipeline;
      this.configuration = configuration;
    }

    public IPipelineEngine<PipelineMeta> getPipeline() {
      return pipeline;
    }

    public void setPipeline( IPipelineEngine<PipelineMeta> pipeline ) {
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
