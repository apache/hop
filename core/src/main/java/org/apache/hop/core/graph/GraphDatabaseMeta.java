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

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * A connection to a graph database. The database type is a {@link GraphDatabasePluginType} plugin
 * which holds the type specific settings.
 */
@HopMetadata(
    key = "graph-database-connection",
    name = "i18n::GraphDatabaseMeta.name",
    description = "i18n::GraphDatabaseMeta.description",
    image = "ui/images/graph-database.svg",
    category = HopMetadataCategory.CONNECTIONS,
    documentationUrl = "/metadata-types/graphs/graph-database-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.GRAPH_CONNECTION)
@Getter
@Setter
public class GraphDatabaseMeta extends HopMetadataBase implements IHopMetadata {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID =
      "GraphDatabaseMeta-PluginSpecific-Options";

  @HopMetadataProperty private IGraphDatabase graphDatabase;

  public GraphDatabaseMeta() {}

  public GraphDatabaseMeta(String name, IGraphDatabase graphDatabase) {
    this.name = name;
    this.graphDatabase = graphDatabase;
  }

  public GraphDatabaseMeta(GraphDatabaseMeta source) {
    this.name = source.name;
    this.graphDatabase = source.graphDatabase == null ? null : source.graphDatabase.clone();
  }

  /**
   * Load a graph database connection by name.
   *
   * @return The connection or null if there is no connection with that name
   */
  public static GraphDatabaseMeta load(IHopMetadataProvider metadataProvider, String name)
      throws HopException {
    if (metadataProvider == null || StringUtils.isEmpty(name)) {
      return null;
    }
    return metadataProvider.getSerializer(GraphDatabaseMeta.class).load(name);
  }

  /**
   * Create an empty graph database type with the given plugin ID.
   *
   * @throws HopException if there is no graph database type plugin with that ID
   */
  public static IGraphDatabase createGraphDatabase(String pluginId) throws HopException {
    return (IGraphDatabase) new GraphDatabaseObjectFactory().createObject(pluginId, null);
  }

  /** The plugin ID of the graph database type, or null if no type is set. */
  public String getPluginId() {
    return graphDatabase == null ? null : graphDatabase.getPluginId();
  }

  /** Open a connection. The caller closes it. */
  public IGraphConnection connect(ILogChannel log, IVariables variables) throws HopException {
    return getValidGraphDatabase().connect(log, variables, name);
  }

  /** Test the connection, returns a description of what was tested. */
  public String test(IVariables variables) throws HopException {
    return getValidGraphDatabase().test(variables, name);
  }

  private IGraphDatabase getValidGraphDatabase() throws HopException {
    if (graphDatabase == null) {
      throw new HopException("No graph database type set in graph database connection " + name);
    }
    return graphDatabase;
  }

  @Override
  public String toString() {
    return name == null ? super.toString() : name;
  }
}
