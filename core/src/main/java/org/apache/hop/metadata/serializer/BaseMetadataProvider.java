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

package org.apache.hop.metadata.serializer;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.plugin.MetadataPluginType;

import java.util.ArrayList;
import java.util.List;

public class BaseMetadataProvider {

  // The variables which get inherited by all loaded objects which implement IVariables
  //
  protected IVariables variables;
  protected String description;

  public BaseMetadataProvider(IVariables variables, String description) {
    this.variables = variables;
    this.description = description;
  }

  public <T extends IHopMetadata> List<Class<T>> getMetadataClasses() {
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      List<Class<T>> classes = new ArrayList<>();
      for (IPlugin plugin : registry.getPlugins(MetadataPluginType.class)) {
        String className = plugin.getClassMap().get(plugin.getMainType());
        Class<?> pluginClass = registry.getClassLoader(plugin).loadClass(className);
        classes.add((Class<T>) pluginClass);
      }
      return classes;
    } catch (Exception e) {
      throw new RuntimeException("Error listing metadata plugin classes (setup issue?)", e);
    }
  }

  public <T extends IHopMetadata> Class<T> getMetadataClassForKey(String key) throws HopException {
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin = registry.findPluginWithId(MetadataPluginType.class, key);
      if (plugin == null) {
        throw new HopException(
            "The metadata plugin for key " + key + " could not be found in the plugin registry");
      }
      String className = plugin.getClassMap().get(plugin.getMainType());
      Class<?> pluginClass = registry.getClassLoader(plugin).loadClass(className);

      return (Class<T>) pluginClass;
    } catch (Exception e) {
      throw new HopException("Error find metadata class for key " + key, e);
    }
  }

  /**
   * Get the variables
   *
   * @return The variables which get inherited by all loaded objects which implement IVariables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * @param variables The variables which get inherited by all loaded objects which implement
   *     IVariables
   */
  public void setVariables(IVariables variables) {
    this.variables = variables;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }
}
