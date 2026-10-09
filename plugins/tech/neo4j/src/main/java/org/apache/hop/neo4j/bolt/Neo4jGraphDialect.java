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

package org.apache.hop.neo4j.bolt;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphIndex;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.IGraphConnection;

/** Neo4j 5: named indexes and constraints, IF [NOT] EXISTS. */
public class Neo4jGraphDialect extends BoltGraphDialect {

  public static final Neo4jGraphDialect INSTANCE = new Neo4jGraphDialect();

  public Neo4jGraphDialect() {
    super("NEO4J");
  }

  @Override
  public List<GraphIndex> getIndexes(IGraphConnection connection) throws HopException {
    try {
      return BoltIndexes.fromNeo4j(connection.execute("SHOW INDEXES", Map.of()));
    } catch (HopException e) {
      // Neo4j before 4.2 doesn't know SHOW INDEXES. If the old procedure fails as well, the
      // error of SHOW INDEXES is the one which tells what went wrong.
      try {
        return BoltIndexes.fromNeo4j(connection.execute("CALL db.indexes()", Map.of()));
      } catch (HopException fallbackException) {
        e.addSuppressed(fallbackException);
        throw new HopException("Unable to list the indexes of the database", e);
      }
    }
  }

  /**
   * The schema from the schema procedures, which read all nodes and relationships: the sample size
   * doesn't apply. The labels at the ends of the relationships come from db.schema.visualization(),
   * left out if it fails. On versions or with privileges without the procedures, a sample.
   */
  @Override
  public GraphSchema getSchema(IGraphConnection connection, int sampleSize) throws HopException {
    List<Map<String, Object>> nodes;
    List<Map<String, Object>> relationships;
    try {
      nodes = connection.execute("CALL db.schema.nodeTypeProperties()", Map.of());
      relationships = connection.execute("CALL db.schema.relTypeProperties()", Map.of());
    } catch (HopException e) {
      return super.getSchema(connection, sampleSize);
    }
    List<Map<String, Object>> visualization;
    try {
      visualization = connection.execute("CALL db.schema.visualization()", Map.of());
    } catch (HopException e) {
      visualization = null;
    }
    return BoltSchemas.fromNeo4j(nodes, relationships, visualization);
  }
}
