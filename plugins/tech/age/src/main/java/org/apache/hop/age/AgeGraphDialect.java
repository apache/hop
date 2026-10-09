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
import org.apache.hop.core.graph.IGraphDialect;

/**
 * Apache AGE speaks Cypher, but its indexes and constraints are PostgreSQL ones: the Cypher index
 * and constraint statements are not supported. The indexes on node properties are created in SQL.
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
