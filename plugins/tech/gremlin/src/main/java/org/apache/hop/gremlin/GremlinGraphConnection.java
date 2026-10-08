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

package org.apache.hop.gremlin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaBuilder;
import org.apache.hop.core.graph.GraphSchemaSampler;
import org.apache.hop.core.graph.GraphUpsertNode;
import org.apache.hop.core.graph.GraphUpsertRelationship;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.graph.IGraphTransaction;
import org.apache.hop.core.graph.IGraphTransactionWork;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * A connection to a Gremlin server. Statements are Gremlin scripts, with their parameters as
 * bindings. Nodes and relationships are upserted with traversals: no scripts, so this also works on
 * servers which don't run scripts with bindings. Every request is a transaction of its own.
 */
public class GremlinGraphConnection implements IGraphConnection {
  /** The key of the label in an element map. */
  private static final Object LABEL = org.apache.tinkerpop.gremlin.structure.T.label;

  /** How many nodes or relationships go into one upsert traversal. */
  private static final int UPSERT_CHUNK_SIZE = 100;

  private final Cluster cluster;
  private final Client client;
  private final GraphTraversalSource g;
  private final ILogChannel log;

  public GremlinGraphConnection(
      Cluster cluster, Client client, GraphTraversalSource g, ILogChannel log) {
    this.cluster = cluster;
    this.client = client;
    this.g = g;
    this.log = log;
  }

  @Override
  public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters)
      throws HopException {
    try {
      List<Result> results =
          client.submit(statement, parameters == null ? Map.of() : parameters).all().get();
      List<Map<String, Object>> rows = new ArrayList<>();
      for (Result result : results) {
        rows.add(GremlinValues.toRow(result.getObject()));
      }
      return rows;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException("Interrupted executing Gremlin script: " + statement, e);
    } catch (Exception e) {
      throw new HopException("Error executing Gremlin script: " + statement, e);
    }
  }

  @Override
  public boolean isSupportingUpserts() {
    return true;
  }

  @Override
  public void upsert(List<GraphUpsertNode> nodes, List<GraphUpsertRelationship> relationships)
      throws HopException {
    for (GraphUpsertNode node : nodes) {
      checkKeys(node);
    }
    for (GraphUpsertRelationship relationship : relationships) {
      checkKeys(relationship.source());
      checkKeys(relationship.target());
    }
    try {
      for (int start = 0; start < nodes.size(); start += UPSERT_CHUNK_SIZE) {
        upsertNodes(g, nodes.subList(start, Math.min(nodes.size(), start + UPSERT_CHUNK_SIZE)))
            .iterate();
      }
      for (int start = 0; start < relationships.size(); start += UPSERT_CHUNK_SIZE) {
        upsertRelationships(
                g,
                relationships.subList(
                    start, Math.min(relationships.size(), start + UPSERT_CHUNK_SIZE)))
            .iterate();
      }
    } catch (Exception e) {
      throw new HopException("Error writing nodes and relationships to the Gremlin server", e);
    }
  }

  /**
   * A node without keys can't be looked up: it would match every vertex with its label. A null key
   * value never matches and isn't stored, so every run would add another vertex without the key.
   *
   * @throws HopException if the node has no keys or a key without a value
   */
  private static void checkKeys(GraphUpsertNode node) throws HopException {
    if (node.keys() == null || node.keys().isEmpty()) {
      throw new HopException(
          "Node with label '"
              + node.label()
              + "' has no key properties: Gremlin upserts need at least one key to find the node");
    }
    for (Map.Entry<String, Object> key : node.keys().entrySet()) {
      if (key.getValue() == null) {
        throw new HopException(
            "Node with label '"
                + node.label()
                + "' has a null value for key property '"
                + key.getKey()
                + "': Gremlin upserts need a value for every key to find the node");
      }
    }
  }

  /**
   * One traversal for the nodes, one round trip for the chunk. Every node is a side effect of a
   * single injected traverser: V().has(keys).limit(1).fold().coalesce(unfold(),
   * addV(label).property(keys)) followed by the other properties. A side effect passes on the
   * traverser it gets, so every node is upserted exactly once, also when its keys match more than
   * one vertex. Vertex properties are set with single cardinality: servers with set or list as the
   * default, like Amazon Neptune, would add a value on every update instead of replacing it.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static GraphTraversal upsertNodes(GraphTraversalSource g, List<GraphUpsertNode> nodes) {
    if (nodes.isEmpty()) {
      return g.inject();
    }
    GraphTraversal traversal = g.inject(0L);
    for (GraphUpsertNode node : nodes) {
      GraphTraversal create = __.addV(node.label());
      for (Map.Entry<String, Object> key : node.keys().entrySet()) {
        create =
            create.property(
                VertexProperty.Cardinality.single,
                key.getKey(),
                GremlinValues.toPropertyValue(key.getValue()));
      }
      GraphTraversal upsert = hasKeys(__.V(), node).limit(1).fold().coalesce(__.unfold(), create);
      traversal = traversal.sideEffect(setProperties(upsert, node.properties(), true));
    }
    return traversal;
  }

  /**
   * One traversal for the relationships, one round trip for the chunk. Every relationship is a side
   * effect of a single injected traverser: the first existing edge between the two nodes, or a new
   * one, followed by its properties. The nodes are looked up by their keys. Edge properties have no
   * cardinality.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static GraphTraversal upsertRelationships(
      GraphTraversalSource g, List<GraphUpsertRelationship> relationships) {
    if (relationships.isEmpty()) {
      return g.inject();
    }
    GraphTraversal traversal = g.inject(0L);
    for (GraphUpsertRelationship relationship : relationships) {
      GraphTraversal existing =
          hasKeys(__.V(), relationship.source())
              .outE(relationship.label())
              .where(hasKeys(__.inV(), relationship.target()))
              .limit(1);
      GraphTraversal create =
          __.addE(relationship.label())
              .from(hasKeys(__.V(), relationship.source()).limit(1))
              .to(hasKeys(__.V(), relationship.target()).limit(1));
      traversal =
          traversal.sideEffect(
              setProperties(__.coalesce(existing, create), relationship.properties(), false));
    }
    return traversal;
  }

  @SuppressWarnings("rawtypes")
  private static GraphTraversal hasKeys(GraphTraversal traversal, GraphUpsertNode node) {
    if (node.keys() == null || node.keys().isEmpty()) {
      throw new IllegalStateException(
          "Node with label '" + node.label() + "' has no key properties to look it up by");
    }
    GraphTraversal result = traversal.hasLabel(node.label());
    for (Map.Entry<String, Object> key : node.keys().entrySet()) {
      result = result.has(key.getKey(), GremlinValues.toPropertyValue(key.getValue()));
    }
    return result;
  }

  /**
   * Set the properties. Null values are skipped: Gremlin properties can't be null.
   *
   * @param vertex True for vertex properties, set with single cardinality. Edges reject a
   *     cardinality.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static GraphTraversal setProperties(
      GraphTraversal traversal, Map<String, Object> properties, boolean vertex) {
    GraphTraversal result = traversal;
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      if (property.getValue() != null) {
        Object value = GremlinValues.toPropertyValue(property.getValue());
        result =
            vertex
                ? result.property(VertexProperty.Cardinality.single, property.getKey(), value)
                : result.property(property.getKey(), value);
      }
    }
    return result;
  }

  /**
   * The vertex and edge labels with their properties, from the element maps of the first vertices
   * and edges: Gremlin servers have no catalog of their schema. Edge maps tell the labels of the
   * vertices at both ends. No indexes.
   */
  @Override
  public GraphSchema getSchema(int sampleSize) throws HopException {
    int limit = GraphSchemaSampler.getSampleSize(sampleSize);
    try {
      return fromElementMaps(
          g.V().limit(limit).elementMap().toList(), g.E().limit(limit).elementMap().toList());
    } catch (Exception e) {
      throw new HopException("Error reading the schema from the Gremlin server", e);
    }
  }

  /**
   * The schema from element maps of vertices and edges: the label under {@link
   * org.apache.tinkerpop.gremlin.structure.T#label}, the properties under their names, and for
   * edges the vertices at the ends under {@link Direction#OUT} and {@link Direction#IN}.
   */
  static GraphSchema fromElementMaps(
      List<? extends Map<Object, Object>> vertices, List<? extends Map<Object, Object>> edges) {
    GraphSchemaBuilder builder = new GraphSchemaBuilder();
    for (Map<Object, Object> vertex : vertices) {
      builder.addSampledNode(String.valueOf(vertex.get(LABEL)), properties(vertex));
    }
    for (Map<Object, Object> edge : edges) {
      builder.addSampledRelationship(
          String.valueOf(edge.get(LABEL)),
          endLabel(edge.get(Direction.OUT)),
          endLabel(edge.get(Direction.IN)),
          properties(edge));
    }
    return builder.build(List.of(), true);
  }

  private static Map<String, Object> properties(Map<Object, Object> element) {
    Map<String, Object> properties = new LinkedHashMap<>();
    for (Map.Entry<Object, Object> entry : element.entrySet()) {
      if (entry.getKey() instanceof String key) {
        properties.put(key, entry.getValue());
      }
    }
    return properties;
  }

  private static List<String> endLabel(Object end) {
    if (end instanceof Map<?, ?> map && map.get(LABEL) != null) {
      return List.of(String.valueOf(map.get(LABEL)));
    }
    return List.of();
  }

  @Override
  public IGraphTransaction beginTransaction() {
    return new IGraphTransaction() {
      @Override
      public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters)
          throws HopException {
        return GremlinGraphConnection.this.execute(statement, parameters);
      }

      @Override
      public void commit() {
        // Every request is committed on its own
      }

      @Override
      public void rollback() {
        // Every request is committed on its own
      }

      @Override
      public void close() {
        // Nothing to close
      }
    };
  }

  @Override
  public <T> T executeWrite(IGraphTransactionWork<T> work) throws HopException {
    return work.execute(beginTransaction());
  }

  @Override
  public IGraphDialect getGraphDialect() {
    return GremlinGraphDialect.INSTANCE;
  }

  @Override
  public boolean isSupportingTransactions() {
    return false;
  }

  @Override
  public void close() throws HopException {
    try {
      g.close();
    } catch (Exception e) {
      if (log != null) {
        log.logDetailed("Error closing the Gremlin traversal source: " + e.getMessage());
      }
    } finally {
      client.close();
      cluster.close();
    }
  }
}
