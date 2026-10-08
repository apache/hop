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
import java.util.Map;
import org.apache.hop.core.exception.HopException;

/** An open connection to a graph database. Not thread-safe: use one per thread. */
public interface IGraphConnection extends AutoCloseable {

  /**
   * Execute a statement.
   *
   * @param statement The statement in the query language of the database
   * @param parameters The statement parameters, may be empty
   * @return The result rows, column name to value. Values are plain Java values: strings, numbers,
   *     booleans, java.time values, lists and maps, with nodes, relationships and paths as {@link
   *     GraphNodeValue}, {@link GraphRelationshipValue} and {@link GraphPathValue}.
   * @throws HopException In case the statement failed
   */
  List<Map<String, Object>> execute(String statement, Map<String, Object> parameters)
      throws HopException;

  /**
   * Begin an explicit transaction. Databases without multi-statement transactions execute each
   * statement on its own and ignore commit and rollback.
   */
  IGraphTransaction beginTransaction() throws HopException;

  /**
   * Run work in a write transaction, committed when the work returns. Where the database supports
   * it, the work is retried on transient errors.
   */
  <T> T executeWrite(IGraphTransactionWork<T> work) throws HopException;

  /**
   * Run work which only reads, in a read transaction where the database has those: on a cluster
   * they can go to a replica. By default the same as {@link #executeWrite}.
   */
  default <T> T executeRead(IGraphTransactionWork<T> work) throws HopException {
    return executeWrite(work);
  }

  /**
   * @return The dialect of the database: what it supports and the syntax of its index and
   *     constraint statements. By default Cypher with the syntax of Neo4j 5.
   */
  default IGraphDialect getGraphDialect() {
    return CypherGraphDialect.DEFAULT;
  }

  /**
   * @return True if a transaction groups several statements atomically. False if every statement is
   *     executed and committed on its own, whatever transaction it runs in.
   */
  default boolean isSupportingTransactions() {
    return true;
  }

  /**
   * @return True if this connection writes nodes and relationships with {@link #upsert} instead of
   *     with statements, as for databases which don't speak Cypher.
   */
  default boolean isSupportingUpserts() {
    return false;
  }

  /**
   * Create or update nodes and then relationships, in this order.
   *
   * @param nodes The nodes to upsert
   * @param relationships The relationships to upsert between nodes, which are upserted first
   */
  default void upsert(List<GraphUpsertNode> nodes, List<GraphUpsertRelationship> relationships)
      throws HopException {
    throw new HopException("This graph database connection doesn't support upserts");
  }

  /**
   * List the indexes and unique constraints, for example to validate a graph model against the
   * database.
   *
   * @return The indexes, or null if this database can't tell which indexes it has
   */
  default List<GraphIndex> getIndexes() throws HopException {
    return null;
  }

  /**
   * Read the labels, relationship types and their properties, with the indexes. By default through
   * {@link IGraphDialect#getSchema(IGraphConnection, int)} and {@link #getIndexes()}.
   *
   * @param sampleSize The maximum number of nodes and relationships to sample, for the databases
   *     which read the schema from a sample. {@link GraphSchema#DEFAULT_SAMPLE_SIZE} for zero or
   *     less.
   * @return The schema
   */
  default GraphSchema getSchema(int sampleSize) throws HopException {
    GraphSchema schema = getGraphDialect().getSchema(this, sampleSize);
    if (schema.indexes() == null) {
      schema = schema.withIndexes(getIndexes());
    }
    return schema;
  }

  @Override
  void close() throws HopException;
}
