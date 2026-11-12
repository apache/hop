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

package org.apache.hop.neo4j.actions.propertygraph;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/**
 * Creates a SQL/PGQ property graph in a relational database from a graph model: the vertex and edge
 * tables when they don't exist yet, and the CREATE PROPERTY GRAPH statement on top of them.
 */
@Action(
    id = "CREATE_PROPERTY_GRAPH",
    name = "i18n::ActionCreatePropertyGraph.Name",
    description = "i18n::ActionCreatePropertyGraph.Description",
    image = "property_graph.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionCreatePropertyGraph.Keywords",
    documentationUrl = "/workflow/actions/create-property-graph.html")
@GuiPlugin
@Getter
@Setter
public class ActionCreatePropertyGraph extends ActionBase implements IAction {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "ActionCreatePropertyGraph-Options";
  private static final String GROUP_GRAPH = "i18n::ActionCreatePropertyGraph.Group.Graph";
  private static final String GROUP_NODES = "i18n::ActionCreatePropertyGraph.Group.NodeTables";
  private static final String GROUP_EDGES = "i18n::ActionCreatePropertyGraph.Group.EdgeTables";

  @GuiWidgetElement(
      id = "connection",
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DatabaseMeta.class,
      label = "i18n::ActionCreatePropertyGraph.Connection.Label",
      toolTip = "i18n::ActionCreatePropertyGraph.Connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GRAPH,
      groupOrder = "10")
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @GuiWidgetElement(
      id = "schemaName",
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::ActionCreatePropertyGraph.Schema.Label",
      toolTip = "i18n::ActionCreatePropertyGraph.Schema.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GRAPH,
      groupOrder = "10")
  @HopMetadataProperty(key = "schema")
  private String schemaName;

  @GuiWidgetElement(
      id = "graphName",
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::ActionCreatePropertyGraph.GraphName.Label",
      toolTip = "i18n::ActionCreatePropertyGraph.GraphName.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GRAPH,
      groupOrder = "10")
  @HopMetadataProperty(key = "graph_name")
  private String graphName;

  @GuiWidgetElement(
      id = "graphModel",
      order = "0400",
      type = GuiElementType.METADATA,
      metadata = GraphModel.class,
      label = "i18n::ActionCreatePropertyGraph.GraphModel.Label",
      toolTip = "i18n::ActionCreatePropertyGraph.GraphModel.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GRAPH,
      groupOrder = "10")
  @HopMetadataProperty(key = "graph_model")
  private String graphModel;

  @GuiWidgetElement(
      id = "creatingTables",
      order = "0500",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ActionCreatePropertyGraph.CreatingTables.Label",
      toolTip = "i18n::ActionCreatePropertyGraph.CreatingTables.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GRAPH,
      groupOrder = "10")
  @HopMetadataProperty(key = "create_tables")
  private boolean creatingTables;

  @GuiWidgetElement(
      id = "replacingGraph",
      order = "0600",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ActionCreatePropertyGraph.ReplacingGraph.Label",
      toolTip = "i18n::ActionCreatePropertyGraph.ReplacingGraph.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_GRAPH,
      groupOrder = "10")
  @HopMetadataProperty(key = "replace_graph")
  private boolean replacingGraph;

  @GuiWidgetElement(
      id = "nodeTables",
      order = "0700",
      type = GuiElementType.TABLE,
      toolTip = "i18n::ActionCreatePropertyGraph.NodeTables.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_NODES,
      groupOrder = "20",
      tableRows = 8)
  @HopMetadataProperty(groupKey = "node_tables", key = "node_table")
  private List<NodeTableMapping> nodeTables = new ArrayList<>();

  @GuiWidgetElement(
      id = "edgeTables",
      order = "0800",
      type = GuiElementType.TABLE,
      toolTip = "i18n::ActionCreatePropertyGraph.EdgeTables.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_EDGES,
      groupOrder = "30",
      tableRows = 8)
  @HopMetadataProperty(groupKey = "edge_tables", key = "edge_table")
  private List<EdgeTableMapping> edgeTables = new ArrayList<>();

  public ActionCreatePropertyGraph() {
    this("");
  }

  public ActionCreatePropertyGraph(String name) {
    super(name, "");
    creatingTables = true;
  }

  public ActionCreatePropertyGraph(ActionCreatePropertyGraph other) {
    super(other);
    this.connection = other.connection;
    this.schemaName = other.schemaName;
    this.graphName = other.graphName;
    this.graphModel = other.graphModel;
    this.creatingTables = other.creatingTables;
    this.replacingGraph = other.replacingGraph;
    for (NodeTableMapping m : other.nodeTables) {
      nodeTables.add(new NodeTableMapping(m.getNodeName(), m.getTableName(), m.getKeyColumns()));
    }
    for (EdgeTableMapping m : other.edgeTables) {
      edgeTables.add(
          new EdgeTableMapping(
              m.getRelationshipName(),
              m.getTableName(),
              m.getKeyColumns(),
              m.getSourceKeyColumns(),
              m.getTargetKeyColumns()));
    }
  }

  @Override
  public ActionCreatePropertyGraph clone() {
    return new ActionCreatePropertyGraph(this);
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    result.setResult(false);
    IHopMetadataProvider metadataProvider = getMetadataProvider();
    try {
      DatabaseMeta databaseMeta = loadDatabaseMeta(metadataProvider);
      try (Database database = new Database(this, this, databaseMeta)) {
        database.connect();
        executeStatements(
            database, getPlannedStatements(database, loadGraphModel(metadataProvider)));
      }
    } catch (Exception e) {
      result.increaseErrors(1L);
      logError("Error creating property graph '" + resolve(graphName) + "'", e);
      return result;
    }
    result.setResult(true);
    return result;
  }

  /**
   * Run the statements one by one. When one fails, drop the tables this run created, newest first,
   * so a failing CREATE PROPERTY GRAPH doesn't leave them behind. Each statement commits on its
   * own, so a rollback can't undo them.
   */
  void executeStatements(Database database, List<PlannedStatement> statements) throws HopException {
    List<String> createdTables = new ArrayList<>();
    for (PlannedStatement statement : statements) {
      if (isDetailed()) {
        logDetailed("Executing: " + statement.sql());
      }
      try {
        database.execStatement(statement.sql());
      } catch (HopException e) {
        String dropError = dropCreatedTables(database, createdTables);
        if (dropError != null) {
          throw new HopException(
              "The property graph can't be created and the tables it created can't be dropped: "
                  + dropError,
              e);
        }
        throw e;
      }
      if (statement.createdTable() != null) {
        createdTables.add(statement.createdTable());
      }
    }
  }

  /**
   * Drop the tables in reverse order.
   *
   * @return null when all of them are dropped, or a message naming the tables that are left
   */
  private String dropCreatedTables(Database database, List<String> createdTables) {
    List<String> left = new ArrayList<>();
    String reason = null;
    for (int i = createdTables.size() - 1; i >= 0; i--) {
      String table = createdTables.get(i);
      try {
        database.execStatement("DROP TABLE " + table);
        logBasic("Dropped table " + table + ", created by this action");
      } catch (HopException e) {
        left.add(0, table);
        if (reason == null) {
          reason = e.getMessage();
        }
      }
    }
    if (left.isEmpty()) {
      return null;
    }
    return String.join(", ", left) + " stay in the database (" + Const.trim(reason) + ")";
  }

  public DatabaseMeta loadDatabaseMeta(IHopMetadataProvider metadataProvider) throws HopException {
    String connectionName = resolve(connection);
    if (StringUtils.isEmpty(connectionName)) {
      throw new HopException("Please specify a relational database connection");
    }
    DatabaseMeta databaseMeta =
        metadataProvider.getSerializer(DatabaseMeta.class).load(connectionName);
    if (databaseMeta == null) {
      throw new HopException(
          "Unable to find relational database connection '" + connectionName + "'");
    }
    return databaseMeta;
  }

  public GraphModel loadGraphModel(IHopMetadataProvider metadataProvider) throws HopException {
    String modelName = resolve(graphModel);
    GraphModel model = metadataProvider.getSerializer(GraphModel.class).load(modelName);
    if (model == null) {
      throw new HopException("Unable to find graph model '" + modelName + "'");
    }
    return model;
  }

  /**
   * The statements to execute: CREATE TABLE and ALTER TABLE ... ADD PRIMARY KEY for the missing
   * tables when creating tables, and the CREATE PROPERTY GRAPH statement.
   *
   * @param database A connected database, used to check which tables exist
   * @param model The graph model
   */
  public List<String> getStatements(Database database, GraphModel model) throws HopException {
    List<String> statements = new ArrayList<>();
    for (PlannedStatement statement : getPlannedStatements(database, model)) {
      statements.add(statement.sql());
    }
    return statements;
  }

  /**
   * A statement to execute.
   *
   * @param sql The statement
   * @param createdTable The quoted name of the table a CREATE TABLE statement creates, otherwise
   *     null
   */
  record PlannedStatement(String sql, String createdTable) {}

  List<PlannedStatement> getPlannedStatements(Database database, GraphModel model)
      throws HopException {
    if (StringUtils.isEmpty(resolve(graphName))) {
      throw new HopException("Please specify the name of the property graph");
    }
    DatabaseMeta databaseMeta = database.getDatabaseMeta();
    String schema = resolve(schemaName);
    PropertyGraphGenerator generator =
        new PropertyGraphGenerator(model, resolveNodeTables(), resolveEdgeTables());
    // Refuse names which can't be quoted safely before generating any SQL with them
    //
    generator.validateIdentifiers(
        resolve(graphName),
        StringUtils.isEmpty(schema) ? resolve(databaseMeta.getPreferredSchemaName()) : schema,
        databaseMeta.getStartQuote(),
        databaseMeta.getEndQuote());
    generator.validateTables();
    List<PlannedStatement> statements = new ArrayList<>();
    if (creatingTables) {
      for (PropertyGraphGenerator.NodeTable table : generator.getNodeTables()) {
        addCreateTable(database, schema, table.table(), table.columns(), table.keys(), statements);
      }
      for (PropertyGraphGenerator.EdgeTable table : generator.getEdgeTables()) {
        addCreateTable(database, schema, table.table(), table.columns(), table.keys(), statements);
      }
    }
    statements.add(
        new PlannedStatement(
            generator.getCreatePropertyGraphStatement(
                resolve(graphName), schema, replacingGraph, databaseMeta::quoteField),
            null));
    return statements;
  }

  private void addCreateTable(
      Database database,
      String schema,
      String table,
      List<PropertyGraphGenerator.Column> columns,
      List<String> keys,
      List<PlannedStatement> statements)
      throws HopException {
    if (database.checkTableExists(schema, table)) {
      return;
    }
    DatabaseMeta databaseMeta = database.getDatabaseMeta();
    String qualified = databaseMeta.getQuotedSchemaTableCombination(this, schema, table);
    statements.add(
        new PlannedStatement(
            database
                .getCreateTableStatement(
                    qualified, PropertyGraphGenerator.getRowMeta(columns), null, false, null, false)
                .trim(),
            qualified));
    List<String> quotedKeys = new ArrayList<>();
    for (String key : keys) {
      quotedKeys.add(databaseMeta.quoteField(key));
    }
    statements.add(
        new PlannedStatement(
            "ALTER TABLE " + qualified + " ADD PRIMARY KEY (" + String.join(", ", quotedKeys) + ")",
            null));
  }

  private List<NodeTableMapping> resolveNodeTables() {
    List<NodeTableMapping> resolved = new ArrayList<>();
    for (NodeTableMapping m : nodeTables) {
      resolved.add(
          new NodeTableMapping(
              m.getNodeName(), resolve(m.getTableName()), resolve(m.getKeyColumns())));
    }
    return resolved;
  }

  private List<EdgeTableMapping> resolveEdgeTables() {
    List<EdgeTableMapping> resolved = new ArrayList<>();
    for (EdgeTableMapping m : edgeTables) {
      resolved.add(
          new EdgeTableMapping(
              m.getRelationshipName(),
              resolve(m.getTableName()),
              resolve(m.getKeyColumns()),
              resolve(m.getSourceKeyColumns()),
              resolve(m.getTargetKeyColumns())));
    }
    return resolved;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }
}
