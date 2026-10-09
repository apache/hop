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

package org.apache.hop.core.graph;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;

/**
 * A graph database dialect speaking Cypher with the index and constraint syntax of Neo4j 5: named
 * indexes and constraints with IF [NOT] EXISTS. Extend it for a database which speaks Cypher and
 * override the statements which differ. Labels, relationship types, properties and names are quoted
 * with backticks.
 */
public class CypherGraphDialect implements IGraphDialect {

  /**
   * The dialect of graph databases which don't tell theirs: Cypher with the syntax of Neo4j 5,
   * which is what the transforms and actions generate for them.
   */
  public static final CypherGraphDialect DEFAULT = new CypherGraphDialect("CYPHER");

  private final String id;

  public CypherGraphDialect(String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return id;
  }

  @Override
  public boolean isSupportingNodeIndexes() {
    return true;
  }

  @Override
  public boolean isSupportingRelationshipIndexes() {
    return true;
  }

  @Override
  public boolean isSupportingVectorIndexes() {
    return true;
  }

  @Override
  public Set<GraphConstraintType> getNodeConstraintTypes() {
    return EnumSet.allOf(GraphConstraintType.class);
  }

  @Override
  public Set<GraphConstraintType> getRelationshipConstraintTypes() {
    return EnumSet.of(GraphConstraintType.UNIQUE, GraphConstraintType.NOT_NULL);
  }

  @Override
  public boolean isSupportingShortestPath() {
    return true;
  }

  /**
   * Quote a label, relationship type, property or name with backticks, doubling the backticks in
   * it. A name which is quoted already is left as it is.
   */
  public static String quote(String identifier) {
    if (identifier == null) {
      return null;
    }
    if (isQuoted(identifier)) {
      return identifier;
    }
    return '`' + identifier.replace("`", "``") + '`';
  }

  /** True if the whole identifier is between backticks, with the backticks in it doubled. */
  static boolean isQuoted(String identifier) {
    if (identifier.length() < 2 || !identifier.startsWith("`") || !identifier.endsWith("`")) {
      return false;
    }
    String inside = identifier.substring(1, identifier.length() - 1);
    return inside.replace("``", "").indexOf('`') < 0;
  }

  /**
   * Quote a label, relationship type, property or name in this dialect. By default {@link
   * #quote(String)}.
   */
  protected String quoteIdentifier(String identifier) {
    return quote(identifier);
  }

  /** (n:`Label`) or ()-[n:`TYPE`]-() */
  protected String pattern(GraphObjectType objectType, String objectName, String variable) {
    return objectType == GraphObjectType.RELATIONSHIP
        ? "()-[" + variable + ":" + quoteIdentifier(objectName) + "]-()"
        : "(" + variable + ":" + quoteIdentifier(objectName) + ")";
  }

  /** n.`a`, n.`b`, ... */
  protected String propertyList(String variable, List<String> properties) {
    List<String> list = new ArrayList<>();
    for (String property : properties) {
      list.add(variable + "." + quoteIdentifier(property));
    }
    return String.join(", ", list);
  }

  /** `a`, `b`, ... */
  protected String quotedList(List<String> properties) {
    List<String> list = new ArrayList<>();
    for (String property : properties) {
      list.add(quoteIdentifier(property));
    }
    return String.join(", ", list);
  }

  /** Fail unless the dialect supports indexes on this kind of object. */
  protected void validateIndexSupport(GraphObjectType objectType, String objectName)
      throws HopException {
    boolean supported =
        objectType == GraphObjectType.RELATIONSHIP
            ? isSupportingRelationshipIndexes()
            : isSupportingNodeIndexes();
    if (!supported) {
      throw IGraphDialect.indexesNotSupported(this, objectType, objectName);
    }
  }

  /** Fail unless the dialect supports this type of constraint on this kind of object. */
  protected void validateConstraintSupport(GraphConstraintDefinition constraint)
      throws HopException {
    Set<GraphConstraintType> supported =
        constraint.isRelationship() ? getRelationshipConstraintTypes() : getNodeConstraintTypes();
    if (!supported.contains(constraint.constraintType())) {
      throw IGraphDialect.constraintNotSupported(this, constraint);
    }
  }

  /** The one property of a vector index, after checking the vector index can be created. */
  protected String getVectorProperty(GraphVectorIndexDefinition index) throws HopException {
    if (!isSupportingVectorIndexes()) {
      throw IGraphDialect.vectorIndexesNotSupported(this);
    }
    if (StringUtils.isEmpty(index.objectName()) || index.properties().isEmpty()) {
      throw new HopException(
          "A vector index needs a label or relationship type and a property. Index: "
              + index.name());
    }
    if (index.properties().size() != 1) {
      throw new HopException(
          "A vector index is on a single property, not on " + String.join(",", index.properties()));
    }
    return index.properties().get(0);
  }

  /** The dimensions of a vector index to create. */
  protected static int getVectorDimensions(GraphVectorIndexDefinition index) throws HopException {
    if (index.dimensions() == null || index.dimensions() <= 0) {
      throw new HopException(
          "Please specify a positive number of dimensions for vector index " + index.name());
    }
    return index.dimensions();
  }

  @Override
  public String getCreateIndexStatement(GraphIndexDefinition index) throws HopException {
    validateIndexSupport(index.objectType(), index.objectName());
    if (index.properties().isEmpty()) {
      throw new HopException("Please specify the properties to index on " + index.objectName());
    }
    return "CREATE INDEX "
        + (StringUtils.isEmpty(index.name()) ? "" : quoteIdentifier(index.name()))
        + " IF NOT EXISTS FOR "
        + pattern(index.objectType(), index.objectName(), "n")
        + " ON ("
        + propertyList("n", index.properties())
        + ")";
  }

  @Override
  public String getDropIndexStatement(GraphIndexDefinition index) throws HopException {
    validateIndexSupport(index.objectType(), index.objectName());
    return "DROP INDEX "
        + getIndexNameToDrop(index.name(), index.objectName(), index.properties())
        + " IF EXISTS";
  }

  private String getIndexNameToDrop(String name, String objectName, List<String> properties)
      throws HopException {
    if (StringUtils.isEmpty(name)) {
      throw new HopException(
          "Please drop indexes with the name of the index. Object: "
              + objectName
              + ", properties: "
              + String.join(",", properties));
    }
    return quoteIdentifier(name);
  }

  @Override
  public String getCreateVectorIndexStatement(GraphVectorIndexDefinition index)
      throws HopException {
    String property = getVectorProperty(index);
    if (StringUtils.isEmpty(index.name())) {
      // Without a name the index couldn't be dropped again
      throw new HopException(
          "Vector indexes need a name, to drop them by. Object: " + index.objectName());
    }
    return "CREATE VECTOR INDEX "
        + quoteIdentifier(index.name())
        + " IF NOT EXISTS FOR "
        + pattern(index.objectType(), index.objectName(), "n")
        + " ON (n."
        + quoteIdentifier(property)
        + ") OPTIONS {indexConfig: {`vector.dimensions`: "
        + getVectorDimensions(index)
        + ", `vector.similarity_function`: '"
        + index.similarity().name().toLowerCase()
        + "'}}";
  }

  /** A vector index is dropped by name like any other index. */
  @Override
  public String getDropVectorIndexStatement(GraphVectorIndexDefinition index) throws HopException {
    if (!isSupportingVectorIndexes()) {
      throw IGraphDialect.vectorIndexesNotSupported(this);
    }
    return "DROP INDEX "
        + getIndexNameToDrop(index.name(), index.objectName(), index.properties())
        + " IF EXISTS";
  }

  /** Vector search goes through the vector indexes. */
  @Override
  public boolean isSupportingVectorSearch() {
    return isSupportingVectorIndexes();
  }

  /** Neo4j searches vector indexes on relationships since 5.18. */
  @Override
  public boolean isSupportingRelationshipVectorSearch() {
    return isSupportingVectorSearch();
  }

  /**
   * Neo4j's vector index procedures: the index by name, queryRelationships for an index on
   * relationships, queryNodes for one on nodes. Neo4j's score for a cosine index is (1 + cosine
   * similarity) / 2, returned as the cosine similarity between -1 and 1. Its score for a euclidean
   * index is 1 / (1 + squared distance), returned as is.
   */
  @Override
  public GraphStatement getVectorSearchStatement(GraphVectorSearchDefinition search)
      throws HopException {
    validateVectorSearch(search, true);
    String variable = search.isRelationship() ? "relationship" : "node";
    return new GraphStatement(
        "CALL db.index.vector."
            + (search.isRelationship() ? "queryRelationships" : "queryNodes")
            + "($index, $k, $"
            + GraphVectorSearchDefinition.PARAMETER_VECTOR
            + ") YIELD "
            + variable
            + ", score RETURN "
            + (search.similarity() == GraphVectorSimilarity.EUCLIDEAN ? "score" : "2 * score - 1")
            + " AS "
            + GraphVectorSearchDefinition.COLUMN_SCORE
            + getVectorSearchReturnColumns(search, variable)
            + " ORDER BY "
            + GraphVectorSearchDefinition.COLUMN_SCORE
            + " DESC",
        Map.of("index", search.indexName(), "k", (long) search.k()));
  }

  /**
   * Fail unless the dialect searches vectors and the search has what it needs.
   *
   * @param byIndexName True if the database searches an index by name, false if by label and
   *     property
   */
  protected void validateVectorSearch(GraphVectorSearchDefinition search, boolean byIndexName)
      throws HopException {
    if (!isSupportingVectorSearch()) {
      throw IGraphDialect.vectorSearchNotSupported(this);
    }
    if (search.isRelationship() && !isSupportingRelationshipVectorSearch()) {
      throw IGraphDialect.relationshipVectorSearchNotSupported(this);
    }
    if (search.k() <= 0) {
      throw new HopException(
          "The number of "
              + (search.isRelationship() ? "relationships" : "nodes")
              + " to find"
              + " needs to be at least 1");
    }
    if (byIndexName && StringUtils.isEmpty(search.indexName())) {
      throw new HopException(
          getId() + " searches a vector index by name: please specify the name of the index");
    }
    if (!byIndexName
        && (StringUtils.isEmpty(search.label()) || StringUtils.isEmpty(search.property()))) {
      throw new HopException(
          getId()
              + " searches a vector index by "
              + (search.isRelationship() ? "relationship type" : "label")
              + " and property: please specify both, not the name of the index");
    }
  }

  /** , node.`a` AS p0, node.`b` AS p1, ... with the variable of the node or relationship */
  protected String getVectorSearchReturnColumns(
      GraphVectorSearchDefinition search, String variable) {
    StringBuilder columns = new StringBuilder();
    List<String> properties = search.returnProperties();
    for (int i = 0; i < properties.size(); i++) {
      columns
          .append(", ")
          .append(variable)
          .append('.')
          .append(quoteIdentifier(properties.get(i)))
          .append(" AS ")
          .append(GraphVectorSearchDefinition.getPropertyColumn(i));
    }
    return columns.toString();
  }

  @Override
  public String getCreateConstraintStatement(GraphConstraintDefinition constraint)
      throws HopException {
    validateConstraintSupport(constraint);
    if (StringUtils.isEmpty(constraint.name())) {
      throw new HopException(
          "Please create constraints with a name for the constraint. This was for label: "
              + constraint.objectName()
              + ", properties: "
              + String.join(",", constraint.properties()));
    }
    List<String> properties = constraint.properties();
    if (properties.isEmpty()) {
      throw new HopException(
          constraint.constraintType()
              + " constraint requires at least one property, for "
              + constraint.objectName());
    }
    String variable = constraint.isRelationship() ? "r" : "n";
    String statement =
        "CREATE CONSTRAINT "
            + quoteIdentifier(constraint.name())
            + " IF NOT EXISTS FOR "
            + pattern(constraint.objectType(), constraint.objectName(), variable)
            + " REQUIRE ";
    String list = propertyList(variable, properties);
    return switch (constraint.constraintType()) {
      case UNIQUE ->
          statement + " " + (properties.size() > 1 ? "(" + list + ")" : list) + " IS UNIQUE ";
      case NOT_NULL -> {
        if (properties.size() > 1) {
          throw new HopException(
              "A NOT_NULL constraint is on a single property, not on "
                  + String.join(",", properties));
        }
        yield statement + " " + list + " IS NOT NULL ";
      }
      case NODE_KEY -> statement + "(" + list + ") IS NODE KEY ";
    };
  }

  @Override
  public String getDropConstraintStatement(GraphConstraintDefinition constraint)
      throws HopException {
    validateConstraintSupport(constraint);
    if (StringUtils.isEmpty(constraint.name())) {
      throw new HopException(
          "Please drop constraints with the name of the constraint. This was for label: "
              + constraint.objectName()
              + ", properties: "
              + String.join(",", constraint.properties()));
    }
    return "DROP CONSTRAINT " + quoteIdentifier(constraint.name()) + " IF EXISTS ";
  }

  /** A unique constraint for one key property, an index for several. */
  @Override
  public String getCreateNodeKeyIndexStatement(String label, List<String> keyProperties) {
    if (keyProperties.isEmpty() || !isSupportingNodeIndexes()) {
      return null;
    }
    if (keyProperties.size() == 1) {
      return "CREATE CONSTRAINT IF NOT EXISTS FOR "
          + pattern(GraphObjectType.NODE, label, "n")
          + " REQUIRE "
          + propertyList("n", keyProperties)
          + " IS UNIQUE;";
    }
    return "CREATE INDEX IF NOT EXISTS FOR "
        + pattern(GraphObjectType.NODE, label, "n")
        + " ON ("
        + propertyList("n", keyProperties)
        + ")";
  }

  @Override
  public String getCreateNodeIndexStatement(
      String indexName, String label, List<String> properties) {
    return "CREATE INDEX "
        + quoteIdentifier(indexName)
        + " IF NOT EXISTS FOR "
        + pattern(GraphObjectType.NODE, label, "n")
        + " ON ("
        + propertyList("n", properties)
        + ")";
  }
}
