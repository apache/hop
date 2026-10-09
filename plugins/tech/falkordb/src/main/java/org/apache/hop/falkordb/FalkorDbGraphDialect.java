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

package org.apache.hop.falkordb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.CypherGraphDialect;
import org.apache.hop.core.graph.GraphConstraintType;
import org.apache.hop.core.graph.GraphIndexDefinition;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaSampler;
import org.apache.hop.core.graph.GraphStatement;
import org.apache.hop.core.graph.GraphVectorIndexDefinition;
import org.apache.hop.core.graph.GraphVectorSearchDefinition;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphConnection;

/**
 * FalkorDB: unnamed indexes in Cypher, without IF [NOT] EXISTS, constraints only through its own
 * commands. Vectors are stored with vecf32(), the only vectors it indexes.
 */
public class FalkorDbGraphDialect extends CypherGraphDialect {

  public static final FalkorDbGraphDialect INSTANCE = new FalkorDbGraphDialect();

  public FalkorDbGraphDialect() {
    super("FALKORDB");
  }

  @Override
  public Set<GraphConstraintType> getNodeConstraintTypes() {
    return Set.of();
  }

  @Override
  public Set<GraphConstraintType> getRelationshipConstraintTypes() {
    return Set.of();
  }

  @Override
  public boolean isSupportingShortestPath() {
    return false;
  }

  /**
   * FalkorDB's parser doesn't take backticks in quoted names, not even doubled: names with a
   * backtick can't be written.
   */
  @Override
  protected String quoteIdentifier(String identifier) {
    String quoted = quote(identifier);
    if (quoted != null && quoted.substring(1, quoted.length() - 1).indexOf('`') >= 0) {
      throw new IllegalArgumentException(
          "FalkorDB doesn't support backticks in labels, types and properties: " + identifier);
    }
    return quoted;
  }

  /** FalkorDB only indexes vectors created with vecf32(). */
  @Override
  public String vectorValue(String parameterExpression) {
    return "vecf32(" + parameterExpression + ")";
  }

  /**
   * FalkorDB has no IF [NOT] EXISTS: an index which exists already, or doesn't exist when dropping,
   * is not an error, the same as with Neo4j. Only the message of the root cause, the error the
   * server sent, is checked: the messages of the exceptions wrapping it contain the statement.
   */
  @Override
  public boolean isExistingOrMissingIndexError(Throwable error) {
    Throwable root = error;
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    while (root != null && root.getCause() != null && seen.add(root)) {
      root = root.getCause();
    }
    String message = root == null ? null : root.getMessage();
    return message != null
        && (message.contains("already indexed") || message.contains("no such index"));
  }

  /**
   * The labels and relationship types from db.labels() and db.relationshipTypes(), their properties
   * from a sample of every label and type: FalkorDB lists property keys, not per label.
   */
  @Override
  public GraphSchema getSchema(IGraphConnection connection, int sampleSize) throws HopException {
    List<String> labels = new ArrayList<>();
    for (Map<String, Object> row : connection.execute("CALL db.labels()", Map.of())) {
      labels.add(String.valueOf(row.get("label")));
    }
    List<String> types = new ArrayList<>();
    for (Map<String, Object> row : connection.execute("CALL db.relationshipTypes()", Map.of())) {
      types.add(String.valueOf(row.get("relationshipType")));
    }
    return GraphSchemaSampler.sample(connection, labels, types, this::quoteIdentifier, sampleSize);
  }

  /**
   * FalkorDB's vector index procedures: the index by label and property for nodes, by relationship
   * type and property for relationships. Its score is a distance: 1 - cosine similarity for cosine
   * indexes, returned as the cosine similarity between -1 and 1, and the euclidean distance for
   * euclidean indexes, returned as 1 / (1 + squared distance), like Neo4j.
   */
  @Override
  public GraphStatement getVectorSearchStatement(GraphVectorSearchDefinition search)
      throws HopException {
    validateVectorSearch(search, false);
    String similarity =
        search.similarity() == GraphVectorSimilarity.EUCLIDEAN
            ? "1.0 / (1.0 + score * score)"
            : "1.0 - score";
    String variable = search.isRelationship() ? "relationship" : "node";
    String objectParameter = search.isRelationship() ? "type" : "label";
    return new GraphStatement(
        "CALL db.idx.vector."
            + (search.isRelationship() ? "queryRelationships" : "queryNodes")
            + "($"
            + objectParameter
            + ", $attribute, $k, vecf32($"
            + GraphVectorSearchDefinition.PARAMETER_VECTOR
            + ")) YIELD "
            + variable
            + ", score WITH "
            + variable
            + ", "
            + similarity
            + " AS similarity RETURN similarity AS "
            + GraphVectorSearchDefinition.COLUMN_SCORE
            + getVectorSearchReturnColumns(search, variable)
            + " ORDER BY "
            + GraphVectorSearchDefinition.COLUMN_SCORE
            + " DESC",
        Map.of(
            objectParameter,
            search.label(),
            "attribute",
            search.property(),
            "k",
            (long) search.k()));
  }

  @Override
  public String getCreateIndexStatement(GraphIndexDefinition index) throws HopException {
    return "CREATE " + getIndexClause(index);
  }

  @Override
  public String getDropIndexStatement(GraphIndexDefinition index) throws HopException {
    return "DROP " + getIndexClause(index);
  }

  /** FalkorDB indexes have no name: INDEX FOR (n:Label) ON (n.property, ...) */
  private String getIndexClause(GraphIndexDefinition index) throws HopException {
    if (StringUtils.isEmpty(index.objectName()) || index.properties().isEmpty()) {
      throw new HopException(
          "FalkorDB indexes are identified by label and properties, please specify both. Index: "
              + index.name());
    }
    return "INDEX FOR "
        + pattern(index.objectType(), index.objectName(), "n")
        + " ON ("
        + propertyList("n", index.properties())
        + ")";
  }

  @Override
  public String getCreateVectorIndexStatement(GraphVectorIndexDefinition index)
      throws HopException {
    String property = getVectorProperty(index);
    return "CREATE VECTOR INDEX FOR "
        + pattern(index.objectType(), index.objectName(), "n")
        + " ON (n."
        + quoteIdentifier(property)
        + ") OPTIONS {dimension: "
        + getVectorDimensions(index)
        + ", similarityFunction: '"
        + index.similarity().name().toLowerCase()
        + "'}";
  }

  /** Vector indexes have no name either, they are dropped by label and property. */
  @Override
  public String getDropVectorIndexStatement(GraphVectorIndexDefinition index) throws HopException {
    return "DROP VECTOR INDEX FOR "
        + pattern(index.objectType(), index.objectName(), "n")
        + " ON (n."
        + quoteIdentifier(getVectorProperty(index))
        + ")";
  }

  @Override
  public String getCreateNodeKeyIndexStatement(String label, List<String> keyProperties) {
    if (keyProperties.isEmpty()) {
      return null;
    }
    return getCreateNodeIndexStatement(null, label, keyProperties);
  }

  @Override
  public String getCreateNodeIndexStatement(
      String indexName, String label, List<String> properties) {
    return "CREATE INDEX FOR "
        + pattern(GraphObjectType.NODE, label, "n")
        + " ON ("
        + propertyList("n", properties)
        + ")";
  }
}
