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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.model.GraphNode;
import org.apache.hop.neo4j.model.GraphProperty;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.model.GraphRelationship;

/**
 * Generates the SQL/PGQ (SQL:2023) statements for a graph model: the vertex and edge tables and the
 * {@code CREATE PROPERTY GRAPH} statement on top of them. Missing mapping values take defaults:
 *
 * <ul>
 *   <li>a node table is named after the node, keyed on the primary properties of the node
 *   <li>an edge table is named after the relationship, with source_&lt;key&gt; and
 *       target_&lt;key&gt; columns referring to the keys of the source and target node tables. It
 *       is keyed on the primary properties of the relationship or else on those columns.
 * </ul>
 */
public class PropertyGraphGenerator {
  private static final Class<?> PKG = PropertyGraphGenerator.class;
  private static final int KEY_STRING_LENGTH = 255;
  private static final int STRING_LENGTH = 2000;

  /** A column of a vertex or edge table. */
  public record Column(String name, GraphPropertyType type, boolean key) {}

  /** A vertex table: the table of a graph model node. */
  public record NodeTable(
      GraphNode node, String table, List<String> keys, List<String> labels, List<Column> columns) {}

  /** An edge table: the table of a graph model relationship. */
  public record EdgeTable(
      GraphRelationship relationship,
      String table,
      List<String> keys,
      NodeTable source,
      List<String> sourceKeys,
      NodeTable target,
      List<String> targetKeys,
      List<Column> columns) {}

  private final List<NodeTable> nodeTables = new ArrayList<>();
  private final List<EdgeTable> edgeTables = new ArrayList<>();

  public PropertyGraphGenerator(
      GraphModel model, List<NodeTableMapping> nodeMappings, List<EdgeTableMapping> edgeMappings)
      throws HopException {
    Map<String, NodeTable> nodeTablesByName = new LinkedHashMap<>();
    for (GraphNode node : model.getNodes()) {
      NodeTable nodeTable = createNodeTable(node, findNodeMapping(nodeMappings, node.getName()));
      nodeTables.add(nodeTable);
      nodeTablesByName.put(node.getName(), nodeTable);
    }
    for (GraphRelationship relationship : model.getRelationships()) {
      edgeTables.add(
          createEdgeTable(
              relationship,
              findEdgeMapping(edgeMappings, relationship.getName()),
              nodeTablesByName));
    }
  }

  /** The mappings with all the defaults filled in, to show and edit in a dialog. */
  public static List<NodeTableMapping> getDefaultNodeMappings(GraphModel model)
      throws HopException {
    List<NodeTableMapping> mappings = new ArrayList<>();
    for (NodeTable nodeTable : new PropertyGraphGenerator(model, null, null).nodeTables) {
      mappings.add(
          new NodeTableMapping(
              nodeTable.node().getName(), nodeTable.table(), String.join(", ", nodeTable.keys())));
    }
    return mappings;
  }

  /** The mappings with all the defaults filled in, to show and edit in a dialog. */
  public static List<EdgeTableMapping> getDefaultEdgeMappings(GraphModel model)
      throws HopException {
    List<EdgeTableMapping> mappings = new ArrayList<>();
    for (EdgeTable edgeTable : new PropertyGraphGenerator(model, null, null).edgeTables) {
      mappings.add(
          new EdgeTableMapping(
              edgeTable.relationship().getName(),
              edgeTable.table(),
              String.join(", ", edgeTable.keys()),
              String.join(", ", edgeTable.sourceKeys()),
              String.join(", ", edgeTable.targetKeys())));
    }
    return mappings;
  }

  private static NodeTableMapping findNodeMapping(List<NodeTableMapping> mappings, String name) {
    if (mappings != null) {
      for (NodeTableMapping mapping : mappings) {
        if (name.equals(mapping.getNodeName())) {
          return mapping;
        }
      }
    }
    return new NodeTableMapping();
  }

  private static EdgeTableMapping findEdgeMapping(List<EdgeTableMapping> mappings, String name) {
    if (mappings != null) {
      for (EdgeTableMapping mapping : mappings) {
        if (name.equals(mapping.getRelationshipName())) {
          return mapping;
        }
      }
    }
    return new EdgeTableMapping();
  }

  private static List<String> split(String columns) {
    List<String> list = new ArrayList<>();
    if (StringUtils.isNotBlank(columns)) {
      for (String column : columns.split(",")) {
        if (StringUtils.isNotBlank(column)) {
          list.add(column.trim());
        }
      }
    }
    return list;
  }

  private static NodeTable createNodeTable(GraphNode node, NodeTableMapping mapping)
      throws HopException {
    String table = Const.NVL(StringUtils.trimToNull(mapping.getTableName()), node.getName());
    List<String> keys = split(mapping.getKeyColumns());
    if (keys.isEmpty()) {
      for (GraphProperty property : node.getProperties()) {
        if (property.isPrimary()) {
          keys.add(property.getName());
        }
      }
    }
    if (keys.isEmpty()) {
      throw new HopException(
          "Node '"
              + node.getName()
              + "' has no primary property: mark one in the graph model or specify the key columns");
    }
    List<Column> columns = new ArrayList<>();
    for (GraphProperty property : node.getProperties()) {
      columns.add(
          new Column(property.getName(), property.getType(), keys.contains(property.getName())));
    }
    for (String key : keys) {
      if (columns.stream().noneMatch(c -> c.name().equals(key))) {
        throw new HopException(
            "Key column '" + key + "' of node '" + node.getName() + "' is not a node property");
      }
    }
    List<String> labels = new ArrayList<>();
    if (node.getLabels() != null) {
      for (String label : node.getLabels()) {
        if (StringUtils.isNotBlank(label)) {
          labels.add(label);
        }
      }
    }
    if (labels.isEmpty()) {
      labels.add(node.getName());
    }
    return new NodeTable(node, table, keys, labels, columns);
  }

  private static EdgeTable createEdgeTable(
      GraphRelationship relationship, EdgeTableMapping mapping, Map<String, NodeTable> nodeTables)
      throws HopException {
    NodeTable source = nodeTables.get(relationship.getNodeSource());
    NodeTable target = nodeTables.get(relationship.getNodeTarget());
    if (source == null || target == null) {
      throw new HopException(
          "Relationship '"
              + relationship.getName()
              + "' refers to a node which is not in the model");
    }
    String table =
        Const.NVL(StringUtils.trimToNull(mapping.getTableName()), relationship.getName());
    List<String> sourceKeys = split(mapping.getSourceKeyColumns());
    if (sourceKeys.isEmpty()) {
      source.keys().forEach(key -> sourceKeys.add("source_" + key));
    }
    List<String> targetKeys = split(mapping.getTargetKeyColumns());
    if (targetKeys.isEmpty()) {
      target.keys().forEach(key -> targetKeys.add("target_" + key));
    }
    if (sourceKeys.size() != source.keys().size() || targetKeys.size() != target.keys().size()) {
      throw new HopException(
          "Relationship '"
              + relationship.getName()
              + "' needs as many source and target key columns as its nodes have key columns");
    }

    List<Column> columns = new ArrayList<>();
    for (int i = 0; i < sourceKeys.size(); i++) {
      columns.add(new Column(sourceKeys.get(i), keyType(source, source.keys().get(i)), true));
    }
    for (int i = 0; i < targetKeys.size(); i++) {
      columns.add(new Column(targetKeys.get(i), keyType(target, target.keys().get(i)), true));
    }
    List<String> primary = new ArrayList<>();
    for (GraphProperty property : relationship.getProperties()) {
      if (columns.stream().noneMatch(c -> c.name().equals(property.getName()))) {
        columns.add(new Column(property.getName(), property.getType(), property.isPrimary()));
      }
      if (property.isPrimary()) {
        primary.add(property.getName());
      }
    }
    List<String> keys = split(mapping.getKeyColumns());
    if (keys.isEmpty()) {
      keys.addAll(primary);
    }
    if (keys.isEmpty()) {
      keys.addAll(sourceKeys);
      keys.addAll(targetKeys);
    }
    return new EdgeTable(
        relationship, table, keys, source, sourceKeys, target, targetKeys, columns);
  }

  private static GraphPropertyType keyType(NodeTable nodeTable, String key) {
    for (Column column : nodeTable.columns()) {
      if (column.name().equals(key)) {
        return column.type();
      }
    }
    return GraphPropertyType.String;
  }

  public List<NodeTable> getNodeTables() {
    return nodeTables;
  }

  public List<EdgeTable> getEdgeTables() {
    return edgeTables;
  }

  /**
   * Refuse identifiers which can't be quoted safely. Identifiers are quoted with {@link
   * org.apache.hop.core.database.DatabaseMeta#quoteField(String)}, which leaves a name containing a
   * quote character as it is and doesn't quote every other character, so such a name would break
   * the generated statements or change what they do.
   *
   * <p>Checks the graph name, the schema, and the table, key, column, label and property names of
   * every node and edge table, after variable resolution.
   *
   * @param graphName The name of the property graph
   * @param schema The schema of the graph and the tables, may be empty
   * @param quotes The quote characters of the database, refused as well
   * @throws HopException naming the first identifier which isn't allowed
   */
  public void validateIdentifiers(String graphName, String schema, String... quotes)
      throws HopException {
    validateIdentifier(
        graphName, BaseMessages.getString(PKG, "PropertyGraphGenerator.GraphName"), quotes);
    validateIdentifier(
        schema, BaseMessages.getString(PKG, "PropertyGraphGenerator.Schema"), quotes);
    for (NodeTable nodeTable : nodeTables) {
      String context =
          BaseMessages.getString(PKG, "PropertyGraphGenerator.Node", nodeTable.node().getName());
      validateIdentifier(nodeTable.table(), context, quotes);
      validateIdentifiers(nodeTable.keys(), context, quotes);
      validateIdentifiers(nodeTable.labels(), context, quotes);
      for (Column column : nodeTable.columns()) {
        validateIdentifier(column.name(), context, quotes);
      }
    }
    for (EdgeTable edgeTable : edgeTables) {
      GraphRelationship relationship = edgeTable.relationship();
      String context =
          BaseMessages.getString(
              PKG, "PropertyGraphGenerator.Relationship", relationship.getName());
      validateIdentifier(edgeTable.table(), context, quotes);
      validateIdentifiers(edgeTable.keys(), context, quotes);
      validateIdentifiers(edgeTable.sourceKeys(), context, quotes);
      validateIdentifiers(edgeTable.targetKeys(), context, quotes);
      for (Column column : edgeTable.columns()) {
        validateIdentifier(column.name(), context, quotes);
      }
      validateIdentifier(
          Const.NVL(StringUtils.trimToNull(relationship.getLabel()), relationship.getName()),
          context,
          quotes);
      for (GraphProperty property : relationship.getProperties()) {
        validateIdentifier(property.getName(), context, quotes);
      }
    }
  }

  private static void validateIdentifiers(List<String> names, String context, String... quotes)
      throws HopException {
    for (String name : names) {
      validateIdentifier(name, context, quotes);
    }
  }

  /**
   * Refuse a name with a quote character of the database, a quote or backtick, a backslash, a
   * semicolon ending the statement or a control character such as a line break.
   */
  static void validateIdentifier(String name, String context, String... quotes)
      throws HopException {
    if (StringUtils.isEmpty(name)) {
      return;
    }
    List<String> forbidden = new ArrayList<>(List.of("\"", "'", "`", ";", "\\"));
    if (quotes != null) {
      for (String quote : quotes) {
        if (StringUtils.isNotEmpty(quote)) {
          forbidden.add(quote);
        }
      }
    }
    String found = null;
    for (String text : forbidden) {
      if (name.contains(text)) {
        found = text;
        break;
      }
    }
    if (found == null) {
      for (int i = 0; i < name.length(); i++) {
        if (Character.isISOControl(name.charAt(i))) {
          found = String.format("\\u%04x", (int) name.charAt(i));
          break;
        }
      }
    }
    if (found != null) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "PropertyGraphGenerator.InvalidIdentifier", name, context, found));
    }
  }

  /**
   * The CREATE PROPERTY GRAPH statement.
   *
   * @param graphName The name of the property graph
   * @param schema The schema of the tables, may be empty
   * @param replace True to replace an existing graph (CREATE OR REPLACE)
   * @param quote Quotes an identifier where needed
   */
  public String getCreatePropertyGraphStatement(
      String graphName, String schema, boolean replace, UnaryOperator<String> quote) {
    StringBuilder sql = new StringBuilder("CREATE ");
    if (replace) {
      sql.append("OR REPLACE ");
    }
    sql.append("PROPERTY GRAPH ").append(qualify(schema, graphName, quote)).append(Const.CR);
    sql.append("  VERTEX TABLES (").append(Const.CR);
    for (int i = 0; i < nodeTables.size(); i++) {
      NodeTable nodeTable = nodeTables.get(i);
      sql.append("    ").append(tableReference(schema, nodeTable.table(), quote));
      sql.append(" KEY (").append(list(nodeTable.keys(), quote)).append(")");
      List<String> properties = nodeTable.columns().stream().map(Column::name).toList();
      for (String label : nodeTable.labels()) {
        sql.append(" LABEL ").append(quote.apply(label)).append(properties(properties, quote));
      }
      sql.append(i < nodeTables.size() - 1 ? "," : "").append(Const.CR);
    }
    sql.append("  )");
    if (!edgeTables.isEmpty()) {
      sql.append(Const.CR).append("  EDGE TABLES (").append(Const.CR);
      for (int i = 0; i < edgeTables.size(); i++) {
        EdgeTable edgeTable = edgeTables.get(i);
        sql.append("    ").append(tableReference(schema, edgeTable.table(), quote));
        sql.append(" KEY (").append(list(edgeTable.keys(), quote)).append(")");
        sql.append(" SOURCE KEY (").append(list(edgeTable.sourceKeys(), quote)).append(")");
        sql.append(" REFERENCES ").append(quote.apply(edgeTable.source().table()));
        sql.append(" (").append(list(edgeTable.source().keys(), quote)).append(")");
        sql.append(" DESTINATION KEY (").append(list(edgeTable.targetKeys(), quote)).append(")");
        sql.append(" REFERENCES ").append(quote.apply(edgeTable.target().table()));
        sql.append(" (").append(list(edgeTable.target().keys(), quote)).append(")");
        String label =
            Const.NVL(
                StringUtils.trimToNull(edgeTable.relationship().getLabel()),
                edgeTable.relationship().getName());
        List<String> properties = new ArrayList<>();
        for (GraphProperty property : edgeTable.relationship().getProperties()) {
          properties.add(property.getName());
        }
        sql.append(" LABEL ").append(quote.apply(label)).append(properties(properties, quote));
        sql.append(i < edgeTables.size() - 1 ? "," : "").append(Const.CR);
      }
      sql.append("  )");
    }
    return sql.toString();
  }

  /** Name the graph element after the table, also when the table is in a schema. */
  private static String tableReference(String schema, String table, UnaryOperator<String> quote) {
    if (StringUtils.isEmpty(schema)) {
      return quote.apply(table);
    }
    return qualify(schema, table, quote) + " AS " + quote.apply(table);
  }

  private static String qualify(String schema, String name, UnaryOperator<String> quote) {
    if (StringUtils.isEmpty(schema)) {
      return quote.apply(name);
    }
    return quote.apply(schema) + "." + quote.apply(name);
  }

  private static String list(List<String> names, UnaryOperator<String> quote) {
    return String.join(", ", names.stream().map(quote).toList());
  }

  private static String properties(List<String> properties, UnaryOperator<String> quote) {
    if (properties.isEmpty()) {
      return " NO PROPERTIES";
    }
    return " PROPERTIES (" + list(properties, quote) + ")";
  }

  /** The columns of a table as row metadata, to generate its CREATE TABLE statement from. */
  public static IRowMeta getRowMeta(List<Column> columns) {
    IRowMeta rowMeta = new RowMeta();
    for (Column column : columns) {
      rowMeta.addValueMeta(createValueMeta(column));
    }
    return rowMeta;
  }

  private static IValueMeta createValueMeta(Column column) {
    GraphPropertyType type = column.type() == null ? GraphPropertyType.String : column.type();
    return switch (type) {
      case Integer -> new ValueMetaInteger(column.name(), 18, 0);
      case Float -> new ValueMetaNumber(column.name(), 38, 10);
      case Boolean -> new ValueMetaBoolean(column.name());
      case Date -> new ValueMetaDate(column.name());
      case LocalDateTime, DateTime -> new ValueMetaTimestamp(column.name());
      case ByteArray -> new ValueMetaBinary(column.name());
      default ->
          new ValueMetaString(column.name(), column.key() ? KEY_STRING_LENGTH : STRING_LENGTH, 0);
    };
  }
}
