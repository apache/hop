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

package org.apache.hop.age;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.BaseGraphDatabase;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphDatabasePlugin;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Apache AGE: Cypher in PostgreSQL, one graph per connection. */
@GraphDatabasePlugin(
    id = "AGE",
    name = "i18n::AgeGraphDatabase.name",
    description = "i18n::AgeGraphDatabase.description",
    documentationUrl = "/metadata-types/graphs/graph-database-connection.html")
@GuiPlugin
@Getter
@Setter
public class AgeGraphDatabase extends BaseGraphDatabase {
  /** One lock per JDBC URL and graph name: the graph existence check and creation in one go. */
  private static final Map<String, Object> GRAPH_CREATION_LOCKS = new ConcurrentHashMap<>();

  private static final String GROUP = "i18n::AgeGraphDatabase.Group.Connection";

  @GuiWidgetElement(
      id = "hostname",
      order = "0010",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AgeGraphDatabase.hostname.Label",
      toolTip = "i18n::AgeGraphDatabase.hostname.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String hostname;

  @GuiWidgetElement(
      id = "port",
      order = "0020",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AgeGraphDatabase.port.Label",
      toolTip = "i18n::AgeGraphDatabase.port.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String port;

  @GuiWidgetElement(
      id = "databaseName",
      order = "0030",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AgeGraphDatabase.databaseName.Label",
      toolTip = "i18n::AgeGraphDatabase.databaseName.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String databaseName;

  @GuiWidgetElement(
      id = "username",
      order = "0040",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AgeGraphDatabase.username.Label",
      toolTip = "i18n::AgeGraphDatabase.username.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String username;

  @GuiWidgetElement(
      id = "password",
      order = "0050",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::AgeGraphDatabase.password.Label",
      toolTip = "i18n::AgeGraphDatabase.password.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty(password = true)
  private String password;

  @GuiWidgetElement(
      id = "graphName",
      order = "0060",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AgeGraphDatabase.graphName.Label",
      toolTip = "i18n::AgeGraphDatabase.graphName.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String graphName;

  @GuiWidgetElement(
      id = "creatingGraph",
      order = "0070",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::AgeGraphDatabase.creatingGraph.Label",
      toolTip = "i18n::AgeGraphDatabase.creatingGraph.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private boolean creatingGraph;

  public AgeGraphDatabase() {
    hostname = "localhost";
    port = "5432";
    databaseName = "postgres";
    graphName = "hop_graph";
    creatingGraph = true;
  }

  @Override
  public IGraphDialect getGraphDialect() {
    return new AgeGraphDialect(graphName);
  }

  @Override
  public IGraphDialect getGraphDialect(IVariables variables) {
    return new AgeGraphDialect(variables.resolve(graphName));
  }

  /** The JDBC URL of the database. */
  public String getUrl(IVariables variables) {
    return "jdbc:postgresql://"
        + variables.resolve(hostname)
        + ":"
        + variables.resolve(port)
        + "/"
        + variables.resolve(databaseName);
  }

  @Override
  public IGraphConnection connect(ILogChannel log, IVariables variables, String connectionName)
      throws HopException {
    String realGraphName = variables.resolve(graphName);
    if (StringUtils.isEmpty(realGraphName)) {
      throw new HopException(
          "Please specify the name of the graph in connection " + connectionName);
    }
    Properties properties = new Properties();
    properties.setProperty("user", Const.NVL(variables.resolve(username), ""));
    String realPassword = variables.resolve(password);
    if (StringUtils.isNotEmpty(realPassword)) {
      properties.setProperty("password", Encr.decryptPasswordOptionallyEncrypted(realPassword));
    }
    Connection connection;
    try {
      connection = DriverManager.getConnection(getUrl(variables), properties);
    } catch (SQLException e) {
      throw new HopException(
          "Unable to connect to " + getUrl(variables) + " for " + connectionName, e);
    }
    try {
      prepareSession(log, connection, realGraphName, getUrl(variables) + "|" + realGraphName);
      return new AgeGraphConnection(connection, realGraphName, log);
    } catch (SQLException | RuntimeException e) {
      try {
        connection.close();
      } catch (SQLException ce) {
        // Report the first error
      }
      throw new HopException("Unable to use Apache AGE graph " + realGraphName, e);
    }
  }

  /** Load AGE, put its catalog on the search path and create the graph if needed. */
  private void prepareSession(
      ILogChannel log, Connection connection, String realGraphName, String lockKey)
      throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("LOAD 'age'");
      } catch (SQLException e) {
        // AGE is loaded with shared_preload_libraries already, or LOAD isn't allowed
        if (log != null) {
          log.logDetailed("LOAD 'age' failed, continuing: " + e.getMessage());
        }
      }
      statement.execute("SET search_path = ag_catalog, \"$user\", public");
    }
    if (creatingGraph) {
      // Transforms that start in parallel connect at the same time: create the graph once
      Object lock = GRAPH_CREATION_LOCKS.computeIfAbsent(lockKey, k -> new Object());
      synchronized (lock) {
        createGraphIfMissing(log, connection, realGraphName);
      }
    }
  }

  /**
   * Create the graph unless it exists. Another process can create the same graph between the check
   * and the creation. In that case PostgreSQL reports a duplicate (schema or catalog entry): the
   * graph is checked again and used if it exists.
   */
  static void createGraphIfMissing(ILogChannel log, Connection connection, String realGraphName)
      throws SQLException {
    if (graphExists(connection, realGraphName)) {
      return;
    }
    boolean autoCommit = connection.getAutoCommit();
    Savepoint savepoint = autoCommit ? null : connection.setSavepoint();
    try (PreparedStatement ps = connection.prepareStatement("SELECT ag_catalog.create_graph(?)")) {
      ps.setString(1, realGraphName);
      ps.execute();
    } catch (SQLException e) {
      if (!isAlreadyExists(e)) {
        throw e;
      }
      if (savepoint != null) {
        connection.rollback(savepoint);
      }
      if (!graphExists(connection, realGraphName)) {
        throw e;
      }
      if (log != null) {
        log.logDetailed(
            "Apache AGE graph " + realGraphName + " was created by another session, using it");
      }
      return;
    }
    if (savepoint != null) {
      connection.releaseSavepoint(savepoint);
    }
  }

  private static boolean graphExists(Connection connection, String realGraphName)
      throws SQLException {
    try (PreparedStatement ps =
        connection.prepareStatement("SELECT count(*) FROM ag_catalog.ag_graph WHERE name = ?")) {
      ps.setString(1, realGraphName);
      try (ResultSet resultSet = ps.executeQuery()) {
        return resultSet.next() && resultSet.getLong(1) > 0;
      }
    }
  }

  /** A unique violation (23505), a duplicate schema (42P06) or AGE's "already exists" message. */
  static boolean isAlreadyExists(SQLException e) {
    String state = e.getSQLState();
    if ("23505".equals(state) || "42P06".equals(state)) {
      return true;
    }
    String message = e.getMessage();
    return message != null && message.contains("already exists");
  }

  @Override
  public String test(IVariables variables, String connectionName) throws HopException {
    try (IGraphConnection connection = connect(null, variables, connectionName)) {
      connection.execute("RETURN 0", Map.of());
    }
    return getUrl(variables) + ", graph " + variables.resolve(graphName);
  }
}
