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

package org.apache.hop.pipeline.config;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;

public class PipelineRunConfigurationMetadataObjectFactory implements IHopMetadataObjectFactory {

  @Override public Object createObject( String id, Object parentObject ) throws HopException {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.findPluginWithId( PipelineEnginePluginType.class, id );
    if ( plugin == null ) {
      throw new HopException( "Unable to find the plugin in the context of a pipeline engine plugin for id: " + id );
    }

    try {
      // We don't return the engine but the corresponding engine configuration
      //
      IPipelineEngine engine = registry.loadClass( plugin, IPipelineEngine.class );

      IPipelineEngineRunConfiguration engineRunConfiguration = engine.createDefaultPipelineEngineRunConfiguration();
      engineRunConfiguration.setEnginePluginId( plugin.getIds()[0] );
      engineRunConfiguration.setEnginePluginName( plugin.getName() );

      if (parentObject!=null && (parentObject instanceof IVariables )) {
        engineRunConfiguration.initializeFrom( (IVariables) parentObject );
      }

      return engineRunConfiguration;
    } catch ( HopPluginException e ) {
      throw new HopException( "Unable to load the pipeline engine plugin class with plugin id: " + id, e );
    }
  }

  @Override public String getObjectId( Object object ) throws HopException {
    if (!(object instanceof IPipelineEngineRunConfiguration)) {
      throw new HopException("Object provided needs to be of class "+IPipelineEngineRunConfiguration.class.getName());
    }
    return ( (IPipelineEngineRunConfiguration) object ).getEnginePluginId();
  }
}
