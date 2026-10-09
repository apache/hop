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

import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphConstraintDefinition;
import org.apache.hop.core.graph.GraphConstraintType;
import org.apache.hop.core.graph.GraphIndex;
import org.apache.hop.core.graph.GraphIndexDefinition;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphStatement;
import org.apache.hop.core.graph.GraphVectorIndexDefinition;
import org.apache.hop.core.graph.GraphVectorSearchDefinition;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphConnection;

/**
 * Memgraph: unnamed label-property and edge-type-property indexes, node constraints only, named
 * vector indexes on nodes and relationships. Index and constraint changes run in auto-commit
 * transactions.
 */
public class MemgraphGraphDialect extends BoltGraphDialect {

  public static final MemgraphGraphDialect INSTANCE = new MemgraphGraphDialect();

  /** The default number of vectors a Memgraph vector index reserves room for. */
  public static final int DEFAULT_VECTOR_CAPACITY = 1000;

  /**
   * The statements Memgraph doesn't run in an explicit transaction: information queries like SHOW
   * INDEX INFO, index and constraint changes, and a few administrative statements.
   */
  private static final Pattern AUTO_COMMIT_STATEMENT =
      Pattern.compile(
          "^\\s*(SHOW\\b|DROP\\s+ALL\\b|ANALYZE\\s+GRAPH\\b|FREE\\s+MEMORY\\b|STORAGE\\s+MODE\\b"
              + "|(CREATE|DROP)\\s+(\\w+\\s+)*(INDEX|CONSTRAINT)\\b)",
          Pattern.CASE_INSENSITIVE);

  public MemgraphGraphDialect() {
    super("MEMGRAPH");
  }

  @Override
  public Set<GraphConstraintType> getNodeConstraintTypes() {
    return EnumSet.of(GraphConstraintType.UNIQUE, GraphConstraintType.NOT_NULL);
  }

  @Override
  public Set<GraphConstraintType> getRelationshipConstraintTypes() {
    return Set.of();
  }

  @Override
  public boolean isSupportingSchemaChangesInTransactions() {
    return false;
  }

  @Override
  public boolean isSupportingShortestPath() {
    return false;
  }

  @Override
  public boolean isRequiringAutoCommit(String statement) {
    return statement != null
        && AUTO_COMMIT_STATEMENT.matcher(stripLeadingComments(statement)).find();
  }

  /** The statement without the whitespace and comments it starts with. */
  static String stripLeadingComments(String statement) {
    String rest = statement.stripLeading();
    while (true) {
      if (rest.startsWith("//")) {
        int newline = rest.indexOf('\n');
        rest = newline < 0 ? "" : rest.substring(newline + 1).stripLeading();
      } else if (rest.startsWith("/*")) {
        int end = rest.indexOf("*/");
        rest = end < 0 ? "" : rest.substring(end + 2).stripLeading();
      } else {
        return rest;
      }
    }
  }

  /**
   * Memgraph has no IF [NOT] EXISTS. Creating a vector index or constraint which exists already, or
   * dropping one which doesn't exist, fails with an error saying so, for example "Given vector
   * index already exists.". Only the message of the root cause, the error the server sent, is
   * checked: the messages of the exceptions wrapping it can contain anything.
   */
  @Override
  public boolean isExistingOrMissingIndexError(Throwable error) {
    String message = getRootCauseMessage(error).toLowerCase(Locale.ROOT);
    return message.contains("already exists")
        || message.contains("does not exist")
        || message.contains("doesn't exist");
  }

  /** The message of the innermost cause of an error, an empty string if it has none. */
  public static String getRootCauseMessage(Throwable error) {
    Throwable root = error;
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    while (root != null && root.getCause() != null && seen.add(root)) {
      root = root.getCause();
    }
    return root == null || root.getMessage() == null ? "" : root.getMessage();
  }

  @Override
  public List<GraphIndex> getIndexes(IGraphConnection connection) throws HopException {
    return BoltIndexes.fromMemgraph(
        connection.execute("SHOW INDEX INFO", Map.of()),
        connection.execute("SHOW CONSTRAINT INFO", Map.of()));
  }

  /**
   * The schema from SHOW SCHEMA INFO, which Memgraph keeps up to date when started with
   * --schema-info-enabled. Without, a sample of the nodes and relationships.
   */
  @Override
  public GraphSchema getSchema(IGraphConnection connection, int sampleSize) throws HopException {
    List<Map<String, Object>> rows;
    try {
      rows = connection.execute("SHOW SCHEMA INFO", Map.of());
    } catch (HopException e) {
      return super.getSchema(connection, sampleSize);
    }
    if (rows.isEmpty() || rows.get(0).get("schema") == null) {
      return super.getSchema(connection, sampleSize);
    }
    return BoltSchemas.fromMemgraph(String.valueOf(rows.get(0).get("schema")));
  }

  /**
   * Memgraph's vector search procedures: the index by name, vector_search.search for an index on
   * nodes, vector_search.search_edges for one on relationships. Its distance is 1 - cosine
   * similarity for cos indexes, returned as the cosine similarity between -1 and 1, and the squared
   * euclidean distance for l2sq indexes, returned as 1 / (1 + squared distance). Its own similarity
   * column is not used: it is the absolute value of 1 - distance, wrong for opposite vectors.
   */
  @Override
  public GraphStatement getVectorSearchStatement(GraphVectorSearchDefinition search)
      throws HopException {
    validateVectorSearch(search, true);
    String variable = search.isRelationship() ? "edge" : "node";
    return new GraphStatement(
        "CALL vector_search."
            + (search.isRelationship() ? "search_edges" : "search")
            + "($index, $k, $"
            + GraphVectorSearchDefinition.PARAMETER_VECTOR
            + ") YIELD "
            + variable
            + ", distance RETURN "
            + (search.similarity() == GraphVectorSimilarity.EUCLIDEAN
                ? "1.0 / (1.0 + distance)"
                : "1.0 - distance")
            + " AS "
            + GraphVectorSearchDefinition.COLUMN_SCORE
            + getVectorSearchReturnColumns(search, variable)
            + " ORDER BY "
            + GraphVectorSearchDefinition.COLUMN_SCORE
            + " DESC",
        Map.of("index", search.indexName(), "k", (long) search.k()));
  }

  @Override
  public String getCreateIndexStatement(GraphIndexDefinition index) throws HopException {
    return "CREATE " + getIndexClause(index);
  }

  @Override
  public String getDropIndexStatement(GraphIndexDefinition index) throws HopException {
    return "DROP " + getIndexClause(index);
  }

  /** Memgraph indexes have no name: [EDGE] INDEX ON :Label(property, ...) */
  private String getIndexClause(GraphIndexDefinition index) throws HopException {
    if (StringUtils.isEmpty(index.objectName()) || index.properties().isEmpty()) {
      throw new HopException(
          "Memgraph indexes are identified by label and properties, please specify both. Index: "
              + index.name());
    }
    if (index.isRelationship() && index.properties().size() > 1) {
      throw new HopException(
          "Memgraph edge indexes are on a single property, not on "
              + String.join(",", index.properties()));
    }
    return (index.isRelationship() ? "EDGE " : "")
        + "INDEX ON :"
        + quoteIdentifier(index.objectName())
        + "("
        + quotedList(index.properties())
        + ")";
  }

  @Override
  public String getCreateVectorIndexStatement(GraphVectorIndexDefinition index)
      throws HopException {
    String property = getVectorProperty(index);
    int capacity = index.capacity() == null ? DEFAULT_VECTOR_CAPACITY : index.capacity();
    return "CREATE VECTOR "
        + (index.isRelationship() ? "EDGE " : "")
        + "INDEX "
        + getVectorIndexName(index)
        + " ON :"
        + quoteIdentifier(index.objectName())
        + "("
        + quoteIdentifier(property)
        + ") WITH CONFIG {\"dimension\": "
        + getVectorDimensions(index)
        + ", \"capacity\": "
        + capacity
        + ", \"metric\": \""
        + (index.similarity() == GraphVectorSimilarity.COSINE ? "cos" : "l2sq")
        + "\"}";
  }

  @Override
  public String getDropVectorIndexStatement(GraphVectorIndexDefinition index) throws HopException {
    return "DROP VECTOR INDEX " + getVectorIndexName(index);
  }

  /**
   * Memgraph vector indexes have a name, which you also drop them by: the same statement drops a
   * vector index on nodes and one on relationships.
   */
  private String getVectorIndexName(GraphVectorIndexDefinition index) throws HopException {
    if (StringUtils.isEmpty(index.name())) {
      throw new HopException("Memgraph vector indexes need a name. Object: " + index.objectName());
    }
    return quoteIdentifier(index.name());
  }

  @Override
  public String getCreateConstraintStatement(GraphConstraintDefinition constraint)
      throws HopException {
    validateConstraintSupport(constraint);
    return "CREATE " + getConstraintClause(constraint);
  }

  @Override
  public String getDropConstraintStatement(GraphConstraintDefinition constraint)
      throws HopException {
    validateConstraintSupport(constraint);
    return "DROP " + getConstraintClause(constraint);
  }

  /** Memgraph constraints have no name: CONSTRAINT ON (n:Label) ASSERT ... */
  private String getConstraintClause(GraphConstraintDefinition constraint) throws HopException {
    List<String> properties = constraint.properties();
    if (StringUtils.isEmpty(constraint.objectName()) || properties.isEmpty()) {
      throw new HopException(
          "Memgraph constraints are identified by label and properties, please specify both."
              + " Constraint: "
              + constraint.name());
    }
    String clause = "CONSTRAINT ON (n:" + quoteIdentifier(constraint.objectName()) + ") ASSERT ";
    if (constraint.constraintType() == GraphConstraintType.NOT_NULL) {
      if (properties.size() > 1) {
        throw new HopException(
            "Memgraph existence constraints are on a single property, not on "
                + String.join(",", properties));
      }
      return clause + "EXISTS (n." + quoteIdentifier(properties.get(0)) + ")";
    }
    return clause + propertyList("n", properties) + " IS UNIQUE";
  }

  /** A unique constraint for one key property, an index for several. */
  @Override
  public String getCreateNodeKeyIndexStatement(String label, List<String> keyProperties) {
    if (keyProperties.isEmpty()) {
      return null;
    }
    if (keyProperties.size() == 1) {
      return "CREATE CONSTRAINT ON (n:"
          + quoteIdentifier(label)
          + ") ASSERT n."
          + quoteIdentifier(keyProperties.get(0))
          + " IS UNIQUE";
    }
    return getCreateNodeIndexStatement(null, label, keyProperties);
  }

  @Override
  public String getCreateNodeIndexStatement(
      String indexName, String label, List<String> properties) {
    return "CREATE INDEX ON :" + quoteIdentifier(label) + "(" + quotedList(properties) + ")";
  }
}
