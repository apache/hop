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
 */

package org.apache.hop.neo4j.shared;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.neo4j.bolt.BoltGraphDatabase;
import org.apache.hop.neo4j.bolt.Neo4jGraphDatabase;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.ResultSummary;

public class NeoConnectionUtils {
  private static final Class<?> PKG =
      NeoConnectionUtils.class; // for i18n purposes, needed by Translator2!!

  private static final String NEO4J_PLUGIN_ID = "NEO4J";

  /**
   * Log the notifications of a statement. Notifications are advice about a statement which
   * succeeded, never errors: errors are thrown as exceptions. Warnings are logged at the basic
   * level, informational notifications at the detailed level.
   */
  public static void logNotifications(ILogChannel log, ResultSummary summary) {
    logNotifications(log, summary, null);
  }

  /**
   * Log the notifications of a result. With a set of the notifications logged already, a
   * notification code with a severity is logged once at the normal level and after that only at
   * debug level, so that a statement executed for every row doesn't flood the log.
   *
   * @param log The log channel
   * @param summary The summary of the result
   * @param logged The codes and severities logged already, updated by this method, or null to log
   *     every notification
   */
  public static void logNotifications(ILogChannel log, ResultSummary summary, Set<String> logged) {
    for (Notification notification : summary.notifications()) {
      String message =
          notification.rawSeverityLevel().orElse("")
              + " : "
              + notification.title()
              + " : "
              + notification.code()
              + " : "
              + notification.description()
              + ", position "
              + notification.position();
      String key = notification.code() + "/" + notification.rawSeverityLevel().orElse("");
      if (logged != null && !logged.add(key)) {
        if (log.isDebug()) {
          log.logDebug(message);
        }
      } else if (isInformational(notification)) {
        log.logDetailed(message);
      } else {
        log.logBasic(message);
      }
    }
  }

  /**
   * Informational notifications, like Neo4j's INFORMATION or Memgraph's INFO planner hints, are not
   * errors.
   */
  public static boolean isInformational(Notification notification) {
    String severity = notification.rawSeverityLevel().orElse("");
    return "INFORMATION".equalsIgnoreCase(severity) || "INFO".equalsIgnoreCase(severity);
  }

  /**
   * Find a connection by name. A Neo4j connection comes first, so existing projects behave exactly
   * as before. Otherwise a graph database connection of a Bolt type is used, in the form of a Neo4j
   * connection with the same settings.
   *
   * @return The connection or null if there is no Neo4j or Bolt connection with that name
   */
  public static NeoConnection loadConnection(IHopMetadataProvider metadataProvider, String name)
      throws HopException {
    if (metadataProvider == null || StringUtils.isEmpty(name)) {
      return null;
    }
    NeoConnection connection = metadataProvider.getSerializer(NeoConnection.class).load(name);
    if (connection != null) {
      return connection;
    }
    GraphDatabaseMeta graphDatabaseMeta = GraphDatabaseMeta.load(metadataProvider, name);
    if (graphDatabaseMeta != null
        && graphDatabaseMeta.getGraphDatabase() instanceof BoltGraphDatabase bolt) {
      return bolt.toNeoConnection(graphDatabaseMeta.getName());
    }
    return null;
  }

  /**
   * Convert a Neo4j connection into a graph database connection of type Neo4j with the same name
   * and settings. The graph database connection is saved first, then the Neo4j connection is
   * deleted, so the connection can be found by name at any time.
   *
   * <p>A Neo4j connection which exists with the same name in several metadata providers, a child
   * project overriding the one of its parent project for example, is not converted: after deleting
   * one, the other Neo4j connection would be found by name before the new graph database
   * connection, silently pointing pipelines and workflows at another server.
   *
   * @return The new graph database connection
   * @throws HopException if there is no such Neo4j connection, it exists in several metadata
   *     providers, or a graph database connection with that name already exists
   */
  public static GraphDatabaseMeta convertToGraphConnection(
      IHopMetadataProvider metadataProvider, String name) throws HopException {
    IHopMetadataSerializer<NeoConnection> neoSerializer =
        metadataProvider.getSerializer(NeoConnection.class);
    IHopMetadataSerializer<GraphDatabaseMeta> graphSerializer =
        metadataProvider.getSerializer(GraphDatabaseMeta.class);
    NeoConnection neo = neoSerializer.load(name);
    if (neo == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "NeoConnectionUtils.Convert.DoesNotExist", name));
    }
    List<String> locations = getNeoConnectionLocations(metadataProvider, name);
    if (locations.size() > 1) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "NeoConnectionUtils.Convert.Duplicate", name, String.join(", ", locations)));
    }
    if (graphSerializer.exists(name)) {
      throw new HopException(
          BaseMessages.getString(PKG, "NeoConnectionUtils.Convert.GraphConnectionExists", name));
    }
    Neo4jGraphDatabase neo4j =
        (Neo4jGraphDatabase) GraphDatabaseMeta.createGraphDatabase(NEO4J_PLUGIN_ID);
    neo4j.copyFrom(neo);
    GraphDatabaseMeta graphDatabaseMeta = new GraphDatabaseMeta(neo.getName(), neo4j);
    graphDatabaseMeta.setVirtualPath(neo.getVirtualPath());
    // Save it next to the Neo4j connection: in a project with a parent project, for example
    //
    graphDatabaseMeta.setMetadataProviderName(neo.getMetadataProviderName());
    graphSerializer.save(graphDatabaseMeta);
    neoSerializer.delete(name);
    return graphDatabaseMeta;
  }

  /**
   * The descriptions of the metadata providers holding a Neo4j connection with the given name, the
   * providers of a multi-provider unwrapped: the parent project first, the active project last.
   */
  public static List<String> getNeoConnectionLocations(
      IHopMetadataProvider metadataProvider, String name) throws HopException {
    List<String> locations = new ArrayList<>();
    for (IHopMetadataProvider provider : HopMetadataUtil.getProviders(metadataProvider)) {
      if (provider.getSerializer(NeoConnection.class).exists(name)) {
        locations.add(provider.getDescription());
      }
    }
    return locations;
  }

  /**
   * The metadata provider of the active project: the last provider of a multi-provider, where new
   * objects are saved, or the provider itself.
   */
  public static IHopMetadataProvider getActiveProvider(IHopMetadataProvider metadataProvider) {
    List<IHopMetadataProvider> providers = HopMetadataUtil.getProviders(metadataProvider);
    return providers.isEmpty() ? metadataProvider : providers.get(providers.size() - 1);
  }

  /**
   * The sorted names of the Neo4j connections stored in the active project, the ones {@link
   * #convertAllToGraphConnections(IHopMetadataProvider)} converts.
   */
  public static List<String> getConvertibleConnectionNames(IHopMetadataProvider metadataProvider)
      throws HopException {
    return new ArrayList<>(
        new TreeSet<>(
            getActiveProvider(metadataProvider)
                .getSerializer(NeoConnection.class)
                .listObjectNames()));
  }

  /**
   * Convert the Neo4j connections of the active project into graph database connections of type
   * Neo4j. Neo4j connections stored in another metadata provider, a parent project for example, are
   * skipped: convert those in their own project. A Neo4j connection is also skipped when a graph
   * database connection with the same name exists already or when it exists in several metadata
   * providers.
   *
   * @return The names of the skipped connections, mapped to the reason. Empty if all were
   *     converted.
   */
  public static Map<String, String> convertAllToGraphConnections(
      IHopMetadataProvider metadataProvider) throws HopException {
    Map<String, String> skipped = new TreeMap<>();
    List<String> convertible = getConvertibleConnectionNames(metadataProvider);
    for (String name : metadataProvider.getSerializer(NeoConnection.class).listObjectNames()) {
      if (!convertible.contains(name)) {
        skipped.put(
            name,
            BaseMessages.getString(
                PKG,
                "NeoConnectionUtils.ConvertAll.OtherProject",
                String.join(", ", getNeoConnectionLocations(metadataProvider, name))));
        continue;
      }
      try {
        convertToGraphConnection(metadataProvider, name);
      } catch (HopException e) {
        skipped.put(name, Const.trim(e.getMessage()));
      }
    }
    return skipped;
  }

  /**
   * Find a connection by name: a Neo4j connection first, so existing projects behave exactly as
   * before, otherwise a graph database connection of any type.
   *
   * @return The connection or null if there is no connection with that name
   */
  public static NamedGraphConnection findGraphConnection(
      IHopMetadataProvider metadataProvider, String name) throws HopException {
    if (metadataProvider == null || StringUtils.isEmpty(name)) {
      return null;
    }
    NeoConnection connection = metadataProvider.getSerializer(NeoConnection.class).load(name);
    if (connection != null) {
      return new NamedGraphConnection(name, connection, null);
    }
    GraphDatabaseMeta graphDatabaseMeta = GraphDatabaseMeta.load(metadataProvider, name);
    if (graphDatabaseMeta != null) {
      return new NamedGraphConnection(name, null, graphDatabaseMeta);
    }
    return null;
  }

  /** True for Neo4j connections and graph database connections of a Bolt type. */
  public static boolean isBolt(NamedGraphConnection graphConnection) {
    return graphConnection.neoConnection() != null
        || graphConnection.graphDatabaseMeta().getGraphDatabase() instanceof BoltGraphDatabase;
  }

  /**
   * Find a connection by name or fail.
   *
   * @throws HopException when there is no Neo4j or graph database connection with that name
   */
  public static NamedGraphConnection getGraphConnection(
      IHopMetadataProvider metadataProvider, String name) throws HopException {
    if (StringUtils.isEmpty(name)) {
      throw new HopException("Please specify a graph database or Neo4j connection");
    }
    NamedGraphConnection connection = findGraphConnection(metadataProvider, name);
    if (connection == null) {
      throw new HopException("Unable to find graph database or Neo4j connection '" + name + "'");
    }
    return connection;
  }

  /**
   * The sorted names of all Neo4j connections and graph database connections of any type, for the
   * transforms and actions which work through {@link NamedGraphConnection}.
   */
  public static List<String> getAllConnectionNames(IHopMetadataProvider metadataProvider)
      throws HopException {
    Set<String> names =
        new TreeSet<>(metadataProvider.getSerializer(NeoConnection.class).listObjectNames());
    names.addAll(metadataProvider.getSerializer(GraphDatabaseMeta.class).listObjectNames());
    return new ArrayList<>(names);
  }

  /**
   * The sorted names of all Neo4j connections and graph database connections of a database which
   * speaks Cypher, for the features built on Cypher statements.
   */
  public static List<String> getCypherConnectionNames(IHopMetadataProvider metadataProvider)
      throws HopException {
    Set<String> names =
        new TreeSet<>(metadataProvider.getSerializer(NeoConnection.class).listObjectNames());
    for (GraphDatabaseMeta graphDatabaseMeta :
        metadataProvider.getSerializer(GraphDatabaseMeta.class).loadAll()) {
      if (graphDatabaseMeta.getGraphDatabase() == null
          || graphDatabaseMeta.getGraphDatabase().getGraphDialect().isCypher()) {
        names.add(graphDatabaseMeta.getName());
      }
    }
    return new ArrayList<>(names);
  }

  /**
   * Create a unique constraint (one key property) or an index (several key properties) on the first
   * label, through a graph connection in its dialect.
   */
  public static void createNodeIndex(
      ILogChannel log, IGraphConnection connection, List<String> labels, List<String> keyProperties)
      throws HopException {
    IGraphDialect dialect = connection.getGraphDialect();
    String cypher = getCreateNodeIndexCypher(labels, keyProperties, dialect);
    if (cypher == null) {
      if (!labels.isEmpty() && !keyProperties.isEmpty()) {
        log.logBasic(
            dialect.getId()
                + " doesn't create indexes in Cypher: not creating an index on "
                + labels.get(0));
      }
      return;
    }
    log.logDetailed("Creating index or constraint : " + cypher);
    try {
      connection.execute(cypher, Map.of());
    } catch (HopException e) {
      if (!dialect.isExistingOrMissingIndexError(e)) {
        throw e;
      }
    }
  }

  /**
   * Run an index or constraint statement: in a write transaction, or on its own where the database
   * doesn't allow schema changes in a transaction.
   *
   * @param description What the statement does, for the log, for example "Creating index"
   */
  public static void runSchemaStatement(
      NamedGraphConnection graphConnection,
      ILogChannel log,
      org.apache.hop.core.variables.IVariables variables,
      String cypher,
      String description)
      throws HopException {
    IGraphDialect dialect = graphConnection.getDialect();
    // Connecting is outside the try which tolerates existing or missing indexes: a connection
    // error always fails, whatever its message says.
    //
    IGraphConnection connection;
    try {
      connection = graphConnection.connect(log, variables);
    } catch (HopException e) {
      throw new HopException(
          description + " failed: unable to connect to '" + graphConnection.name() + "'", e);
    }
    try (connection) {
      log.logDetailed(description + " with cypher: " + cypher);
      try {
        if (!dialect.isSupportingSchemaChangesInTransactions()
            || !connection.isSupportingTransactions()) {
          connection.execute(cypher, Map.of());
        } else {
          connection.executeWrite(
              transaction -> {
                transaction.execute(cypher, Map.of());
                return true;
              });
        }
      } catch (HopException e) {
        if (dialect.isExistingOrMissingIndexError(e)) {
          log.logDetailed(description + ": nothing to do, " + Const.getSimpleStackTrace(e));
          return;
        }
        throw new HopException(description + " failed with cypher [" + cypher + "]", e);
      }
    }
  }

  /** The statement creating the index of {@link #createNodeIndex}, null if there is none. */
  public static String getCreateNodeIndexCypher(
      List<String> labels, List<String> keyProperties, IGraphDialect dialect) {
    if (keyProperties.isEmpty() || labels.isEmpty()) {
      return null;
    }
    return dialect.getCreateNodeKeyIndexStatement(labels.get(0), keyProperties);
  }

  /** The sorted names of all Neo4j connections and graph database connections of a Bolt type. */
  public static List<String> getConnectionNames(IHopMetadataProvider metadataProvider)
      throws HopException {
    Set<String> names =
        new TreeSet<>(metadataProvider.getSerializer(NeoConnection.class).listObjectNames());
    for (GraphDatabaseMeta graphDatabaseMeta :
        metadataProvider.getSerializer(GraphDatabaseMeta.class).loadAll()) {
      if (graphDatabaseMeta.getGraphDatabase() instanceof BoltGraphDatabase) {
        names.add(graphDatabaseMeta.getName());
      }
    }
    return new ArrayList<>(names);
  }
}
