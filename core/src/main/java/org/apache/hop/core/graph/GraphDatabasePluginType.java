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

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

/** The plugin type of the graph database types a graph database connection can use. */
@PluginMainClassType(IGraphDatabase.class)
@PluginAnnotationType(GraphDatabasePlugin.class)
public class GraphDatabasePluginType extends BasePluginType<GraphDatabasePlugin> {
  private static GraphDatabasePluginType pluginType;

  private GraphDatabasePluginType() {
    super(GraphDatabasePlugin.class, "GRAPH_DATABASE", "Graph Database");
  }

  public static GraphDatabasePluginType getInstance() {
    if (pluginType == null) {
      pluginType = new GraphDatabasePluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractID(GraphDatabasePlugin annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(GraphDatabasePlugin annotation) {
    return annotation.name();
  }

  @Override
  protected String extractDesc(GraphDatabasePlugin annotation) {
    return annotation.description();
  }

  @Override
  protected String extractDocumentationUrl(GraphDatabasePlugin annotation) {
    return annotation.documentationUrl();
  }

  @Override
  protected String extractClassLoaderGroup(GraphDatabasePlugin annotation) {
    return annotation.classLoaderGroup();
  }
}
