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

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphIndexDefinition;
import org.apache.hop.core.graph.IGraphDialect;

/**
 * Apache AGE speaks Cypher, but its indexes and constraints are PostgreSQL ones: the Cypher index
 * and constraint statements are not supported. The indexes on node properties are created in SQL,
 * run by {@link AgeGraphConnection#executeSchemaStatement}. There are no vector indexes, no indexes
 * on relationship properties and no constraints.
 */
public class AgeGraphDialect implements IGraphDialect {

  private final String graphName;

  /**
   * @param graphName The name of the graph, the schema of the tables of its labels
   */
  public AgeGraphDialect(String graphName) {
    this.graphName = graphName;
  }

  @Override
  public String getId() {
    return "AGE";
  }

  @Override
  public String toString() {
    return getId();
  }

  @Override
  public boolean isSupportingNodeIndexes() {
    return true;
  }

  /**
   * Create the label if it doesn't exist yet, then the index on its table: the index can be created
   * before the nodes are loaded.
   */
  @Override
  public String getCreateIndexStatement(GraphIndexDefinition index) throws HopException {
    validateIndex(index);
    if (index.properties().isEmpty()) {
      throw new HopException("Please specify the properties to index on " + index.objectName());
    }
    return getCreateLabelStatement(index.objectName())
        + ";\n"
        + getCreateNodeIndexStatement(index.name(), index.objectName(), index.properties());
  }

  @Override
  public String getDropIndexStatement(GraphIndexDefinition index) throws HopException {
    validateIndex(index);
    return "DROP INDEX IF EXISTS "
        + quoteIdentifier(graphName)
        + '.'
        + quoteIdentifier(index.name());
  }

  /** Only named indexes on nodes: PostgreSQL needs the name to create the index if not exists. */
  private void validateIndex(GraphIndexDefinition index) throws HopException {
    if (index.isRelationship()) {
      throw IGraphDialect.indexesNotSupported(this, index.objectType(), index.objectName());
    }
    if (StringUtils.isEmpty(index.objectName())) {
      throw new HopException("Please specify the label of the index " + index.name());
    }
    if (StringUtils.isEmpty(index.name())) {
      throw new HopException(
          "Apache AGE indexes need a name. Label: "
              + index.objectName()
              + ", properties: "
              + String.join(",", index.properties()));
    }
  }

  /**
   * A block creating the node label in the graph unless it exists: AGE creates the table of a label
   * with the first node.
   */
  String getCreateLabelStatement(String label) {
    String graphLiteral = quoteLiteral(graphName);
    String labelLiteral = quoteLiteral(label);
    String body =
        "BEGIN IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_label l"
            + " JOIN ag_catalog.ag_graph g ON g.graphid = l.graph WHERE g.name = "
            + graphLiteral
            + " AND l.kind = 'v' AND l.name = "
            + labelLiteral
            + ") THEN PERFORM ag_catalog.create_vlabel("
            + graphLiteral
            + ", "
            + labelLiteral
            + "); END IF; END";
    String tag = "$hop$";
    for (int i = 1; body.contains(tag); i++) {
      tag = "$hop" + i + "$";
    }
    return "DO " + tag + " " + body + " " + tag;
  }

  /** A PostgreSQL string literal: between single quotes, with the single quotes in it doubled. */
  static String quoteLiteral(String value) {
    return '\'' + value.replace("'", "''") + '\'';
  }

  /**
   * A PostgreSQL index on the label's table: AGE stores the nodes of each label in a table of the
   * graph's schema, their properties in an agtype column. The label needs to exist: it does once a
   * node with it was created.
   */
  @Override
  public String getCreateNodeIndexStatement(
      String indexName, String label, List<String> properties) {
    StringBuilder sql = new StringBuilder("CREATE INDEX IF NOT EXISTS ");
    sql.append(quoteIdentifier(indexName))
        .append(" ON ")
        .append(quoteIdentifier(graphName))
        .append('.')
        .append(quoteIdentifier(label))
        .append(" (");
    for (int i = 0; i < properties.size(); i++) {
      sql.append(i > 0 ? ", " : "")
          .append("ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, '\"")
          .append(properties.get(i).replace("'", "''").replace("\"", "\\\""))
          .append("\"'::ag_catalog.agtype])");
    }
    return sql.append(')').toString();
  }

  /** A PostgreSQL identifier between double quotes, with the double quotes in it doubled. */
  static String quoteIdentifier(String identifier) {
    return '"' + identifier.replace("\"", "\"\"") + '"';
  }
}
