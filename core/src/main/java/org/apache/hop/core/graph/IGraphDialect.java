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

import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;

/**
 * The dialect of a graph database: what it supports and the syntax of its index and constraint
 * statements. A graph database plugin publishes its dialect through {@link
 * IGraphDatabase#getGraphDialect()} and {@link IGraphConnection#getGraphDialect()}, so the
 * transforms and actions which generate statements don't need to know the database.
 *
 * <p>By default a dialect supports nothing but statements: no indexes, constraints or vector
 * indexes. {@link CypherGraphDialect} is a complete dialect with the syntax of Neo4j 5 Cypher, to
 * extend for databases which speak Cypher.
 */
public interface IGraphDialect {

  /**
   * @return The identifier of the dialect, for example NEO4J or MEMGRAPH, used in messages
   */
  String getId();

  /**
   * @return False for databases which don't speak Cypher, like Gremlin servers
   */
  default boolean isCypher() {
    return true;
  }

  default boolean isSupportingNodeIndexes() {
    return false;
  }

  default boolean isSupportingRelationshipIndexes() {
    return false;
  }

  default boolean isSupportingIndexes() {
    return isSupportingNodeIndexes() || isSupportingRelationshipIndexes();
  }

  /**
   * @return True for the databases with native vector indexes
   */
  default boolean isSupportingVectorIndexes() {
    return false;
  }

  /**
   * @return The constraint types this database supports on nodes
   */
  default Set<GraphConstraintType> getNodeConstraintTypes() {
    return Set.of();
  }

  /**
   * @return The constraint types this database supports on relationships
   */
  default Set<GraphConstraintType> getRelationshipConstraintTypes() {
    return Set.of();
  }

  default boolean isSupportingConstraints() {
    return !getNodeConstraintTypes().isEmpty() || !getRelationshipConstraintTypes().isEmpty();
  }

  /**
   * @return False if index and constraint changes have to run in auto-commit transactions
   */
  default boolean isSupportingSchemaChangesInTransactions() {
    return true;
  }

  /**
   * @return True if the database has Neo4j's shortestPath() function
   */
  default boolean isSupportingShortestPath() {
    return false;
  }

  /**
   * @return True if the database refuses to run this statement in an explicit transaction, so it
   *     has to run on its own in an auto-commit transaction
   */
  default boolean isRequiringAutoCommit(String statement) {
    return false;
  }

  /**
   * The expression that stores a vector parameter in a property.
   *
   * @param parameterExpression The parameter holding the vector, like {@code $param1}
   */
  default String vectorValue(String parameterExpression) {
    return parameterExpression;
  }

  /**
   * @return True if {@link #getSchema(IGraphConnection, int)} tells the labels, relationship types
   *     and properties of the database. By default for the databases which speak Cypher.
   */
  default boolean isSupportingSchemaIntrospection() {
    return isCypher();
  }

  /**
   * Read the labels, relationship types and their properties. By default from a sample of the nodes
   * and relationships, with Cypher.
   *
   * @param connection The connection to the database
   * @param sampleSize The maximum number of nodes and relationships to sample, per label or type
   *     where the database lists those. Databases with a catalog of their schema may ignore it.
   * @return The schema. Its indexes are null when the dialect leaves them to {@link
   *     IGraphConnection#getSchema(int)}.
   */
  default GraphSchema getSchema(IGraphConnection connection, int sampleSize) throws HopException {
    if (!isSupportingSchemaIntrospection() || !isCypher()) {
      throw new HopException("Reading the schema is not supported by " + getId());
    }
    return GraphSchemaSampler.sample(connection, null, null, CypherGraphDialect::quote, sampleSize);
  }

  /**
   * @return True if {@link #getVectorSearchStatement(GraphVectorSearchDefinition)} searches a
   *     vector index for the nearest nodes
   */
  default boolean isSupportingVectorSearch() {
    return false;
  }

  /**
   * @return True if {@link #getVectorSearchStatement(GraphVectorSearchDefinition)} also searches a
   *     vector index on relationships for the nearest relationships
   */
  default boolean isSupportingRelationshipVectorSearch() {
    return false;
  }

  /**
   * The statement searching a vector index for the nodes, or relationships, nearest to a query
   * vector. The caller sets the query vector in parameter {@link
   * GraphVectorSearchDefinition#PARAMETER_VECTOR}, as a list of numbers.
   *
   * @param search What to search
   * @return The statement and its other parameters, which return the columns described in {@link
   *     GraphVectorSearchDefinition}
   */
  default GraphStatement getVectorSearchStatement(GraphVectorSearchDefinition search)
      throws HopException {
    throw vectorSearchNotSupported(this);
  }

  /**
   * True if the error says that the index or constraint to create exists already, or that the one
   * to drop doesn't exist: for databases without IF [NOT] EXISTS, so that creating and dropping
   * indexes behaves the same everywhere.
   */
  default boolean isExistingOrMissingIndexError(Throwable error) {
    return false;
  }

  /** The statement creating an index. */
  default String getCreateIndexStatement(GraphIndexDefinition index) throws HopException {
    throw indexesNotSupported(this, index.objectType(), index.objectName());
  }

  /** The statement dropping an index. */
  default String getDropIndexStatement(GraphIndexDefinition index) throws HopException {
    throw indexesNotSupported(this, index.objectType(), index.objectName());
  }

  /** The statement creating a vector index. */
  default String getCreateVectorIndexStatement(GraphVectorIndexDefinition index)
      throws HopException {
    throw vectorIndexesNotSupported(this);
  }

  /** The statement dropping a vector index. */
  default String getDropVectorIndexStatement(GraphVectorIndexDefinition index) throws HopException {
    throw vectorIndexesNotSupported(this);
  }

  /** The statement creating a constraint. */
  default String getCreateConstraintStatement(GraphConstraintDefinition constraint)
      throws HopException {
    throw constraintNotSupported(this, constraint);
  }

  /** The statement dropping a constraint. */
  default String getDropConstraintStatement(GraphConstraintDefinition constraint)
      throws HopException {
    throw constraintNotSupported(this, constraint);
  }

  /**
   * The statement which makes looking up nodes by their key fast, for the transforms which write
   * nodes: a unique constraint or an index, whatever the database has.
   *
   * @param label The node label
   * @param keyProperties The key properties
   * @return The statement, or null if the database doesn't create indexes this way
   */
  default String getCreateNodeKeyIndexStatement(String label, List<String> keyProperties) {
    return null;
  }

  /**
   * The statement creating an index on properties of the nodes with a label.
   *
   * @param indexName The name of the index, for the databases with named indexes
   * @param label The node label
   * @param properties The indexed properties
   * @return The statement, or null if the database has no such indexes
   */
  default String getCreateNodeIndexStatement(
      String indexName, String label, List<String> properties) {
    return null;
  }

  static HopException indexesNotSupported(
      IGraphDialect dialect, GraphObjectType objectType, String objectName) {
    return new HopException(
        "Index updates on "
            + objectType
            + " are not supported by "
            + dialect.getId()
            + " for index on "
            + objectName);
  }

  static HopException vectorIndexesNotSupported(IGraphDialect dialect) {
    return new HopException(
        "Vector indexes are not supported by "
            + dialect.getId()
            + ": use a graph database with vector indexes, like Neo4j, Memgraph or FalkorDB");
  }

  static HopException vectorSearchNotSupported(IGraphDialect dialect) {
    return new HopException(
        "Vector search is not supported by "
            + dialect.getId()
            + ": use a graph database with vector indexes, like Neo4j, Memgraph or FalkorDB");
  }

  static HopException relationshipVectorSearchNotSupported(IGraphDialect dialect) {
    return new HopException(
        "Vector search over relationships is not supported by "
            + dialect.getId()
            + ": use a graph database with vector indexes on relationships, like Neo4j, Memgraph"
            + " or FalkorDB, or search nodes");
  }

  static HopException constraintNotSupported(
      IGraphDialect dialect, GraphConstraintDefinition constraint) {
    return new HopException(
        constraint.constraintType()
            + " constraints on "
            + constraint.objectType()
            + " are not supported by "
            + dialect.getId()
            + ", for "
            + constraint.objectName());
  }
}
