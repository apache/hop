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

package org.apache.hop.core.graph;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;

/** Creates the {@link IGraphDatabase} of a graph database connection from its plugin ID. */
public class GraphDatabaseObjectFactory implements IHopMetadataObjectFactory {

  @Override
  public Object createObject(String id, Object parentObject) throws HopException {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.findPluginWithId(GraphDatabasePluginType.class, id);
    if (plugin == null) {
      throw new HopException("Unable to find graph database type plugin with ID '" + id + "'");
    }
    IGraphDatabase graphDatabase = (IGraphDatabase) registry.loadClass(plugin);
    graphDatabase.setPluginId(plugin.getIds()[0]);
    graphDatabase.setPluginName(plugin.getName());
    return graphDatabase;
  }

  @Override
  public String getObjectId(Object object) throws HopException {
    if (!(object instanceof IGraphDatabase graphDatabase)) {
      throw new HopException(
          "Object is not of class IGraphDatabase but of '" + object.getClass().getName() + "'");
    }
    String pluginId = graphDatabase.getPluginId();
    if (pluginId == null) {
      IPlugin plugin =
          PluginRegistry.getInstance().getPlugin(GraphDatabasePluginType.class, object);
      if (plugin != null) {
        pluginId = plugin.getIds()[0];
        graphDatabase.setPluginId(pluginId);
      }
    }
    return pluginId;
  }
}
