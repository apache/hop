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

package org.apache.hop.config;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphConnectionLookup;
import org.apache.hop.core.graph.GraphDatabaseCapabilities;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.IGraphDatabase;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import picocli.CommandLine;

/**
 * hop-conf options for graph database connections of any type: test a connection and list what the
 * graph database types support. The connections come from the metadata of hop-conf: the project
 * enabled with {@code hop -j <project> conf} or {@code hop -e <environment> conf}, or the folder in
 * HOP_METADATA_FOLDER. The same is available in Hop GUI in the graph database connection editor:
 * the Test button and the capabilities of the selected type.
 */
@Getter
@Setter
@ConfigPlugin(
    id = "GraphConnectionConfigOptionPlugin",
    description = "Test graph database connections and list the graph database types")
public class GraphConnectionConfigOptionPlugin implements IConfigOptions {

  @CommandLine.Option(
      names = {"--graph-connection-test"},
      description =
          "Test the graph database connection or Neo4j connection with the given name."
              + " Fails if the database can't be reached")
  private String testConnectionName;

  @CommandLine.Option(
      names = {"--graph-connection-info"},
      description =
          "Print the graph database type and its capabilities of the graph database connection or"
              + " Neo4j connection with the given name as JSON")
  private String infoConnectionName;

  @CommandLine.Option(
      names = {"--graph-database-types"},
      description = "Print the installed graph database types and their capabilities as JSON")
  private boolean listGraphDatabaseTypes;

  /** Where the results go, standard output unless a test sets another stream. */
  private PrintStream out = System.out;

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    boolean actionTaken = false;
    if (listGraphDatabaseTypes) {
      listGraphDatabaseTypes();
      actionTaken = true;
    }
    if (StringUtils.isNotEmpty(infoConnectionName)) {
      printConnectionInfo(getMetadataProvider(hasHopMetadataProvider), infoConnectionName);
      actionTaken = true;
    }
    if (StringUtils.isNotEmpty(testConnectionName)) {
      testConnection(
          log, getMetadataProvider(hasHopMetadataProvider), variables, testConnectionName);
      actionTaken = true;
    }
    return actionTaken;
  }

  private static IHopMetadataProvider getMetadataProvider(
      IHasHopMetadataProvider hasHopMetadataProvider) throws HopException {
    if (hasHopMetadataProvider == null || hasHopMetadataProvider.getMetadataProvider() == null) {
      throw new HopException("No metadata available to find graph database connections in");
    }
    return hasHopMetadataProvider.getMetadataProvider();
  }

  void listGraphDatabaseTypes() throws HopException {
    List<Map<String, Object>> types = new ArrayList<>();
    for (GraphDatabaseCapabilities capabilities : GraphDatabaseCapabilities.getAll()) {
      types.add(capabilities.toMap());
    }
    out.println(GraphDatabaseCapabilities.toJson(types));
  }

  /** The type and capabilities of a connection, never its settings: no password ends up here. */
  void printConnectionInfo(IHopMetadataProvider metadataProvider, String name) throws HopException {
    GraphConnectionLookup.Found found = GraphConnectionLookup.get(metadataProvider, name);
    IGraphDatabase graphDatabase = found.graphDatabaseMeta().getGraphDatabase();
    Map<String, Object> info = new LinkedHashMap<>();
    info.put("name", found.graphDatabaseMeta().getName());
    info.put("metadataType", found.metadataKey());
    info.put(
        "type", graphDatabase == null ? null : GraphDatabaseCapabilities.of(graphDatabase).toMap());
    out.println(GraphDatabaseCapabilities.toJson(info));
  }

  void testConnection(
      ILogChannel log, IHopMetadataProvider metadataProvider, IVariables variables, String name)
      throws HopException {
    GraphDatabaseMeta graphDatabaseMeta =
        GraphConnectionLookup.get(metadataProvider, name).graphDatabaseMeta();
    IVariables testVariables = variables == null ? Variables.getADefaultVariableSpace() : variables;
    String tested;
    try {
      tested = graphDatabaseMeta.test(testVariables);
    } catch (Exception e) {
      throw new HopException("Graph database connection '" + name + "' failed", e);
    }
    out.println("Graph database connection '" + name + "' is OK: " + tested);
    if (log != null) {
      log.logDetailed("Tested graph database connection '" + name + "': " + tested);
    }
  }
}
