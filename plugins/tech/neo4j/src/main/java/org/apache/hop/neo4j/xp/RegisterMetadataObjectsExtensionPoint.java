/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.neo4j.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.shared.NeoConnection;

import java.net.URL;
import java.util.List;

@ExtensionPoint(
    id = "RegisterMetadataObjectsExtensionPoint",
    extensionPointId = "HopEnvironmentAfterInit",
    description = "Register the Neo4j metadata plugin classes")
public class RegisterMetadataObjectsExtensionPoint implements IExtensionPoint<PluginRegistry> {
  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, PluginRegistry pluginRegistry) throws HopException {

    // Find the plugin for this class.
    //
    IPlugin thisPlugin =
        pluginRegistry.findPluginWithId(
            ExtensionPointPluginType.class, "RegisterMetadataObjectsExtensionPoint");
    ClassLoader classLoader = this.getClass().getClassLoader();
    List<String> libraries = thisPlugin.getLibraries();
    URL pluginUrl = thisPlugin.getPluginDirectory();

    pluginRegistry.registerPluginClass(
        classLoader,
        libraries,
        pluginUrl,
        NeoConnection.class.getName(),
        MetadataPluginType.class,
        HopMetadata.class,
        false);
    pluginRegistry.registerPluginClass(
        classLoader,
        libraries,
        pluginUrl,
        GraphModel.class.getName(),
        MetadataPluginType.class,
        HopMetadata.class,
        false);
  }
}
