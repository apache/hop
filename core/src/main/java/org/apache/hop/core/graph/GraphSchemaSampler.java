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
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.hop.core.exception.HopException;

/**
 * Reads the schema of a Cypher database from a sample of its nodes and relationships, with
 * statements every Cypher database runs: labels(), type() and properties().
 *
 * <p>When the labels and relationship types are known, every label and every type is sampled on its
 * own, so that rare labels are not missed. Otherwise the first nodes and relationships are sampled,
 * whatever their label.
 */
public final class GraphSchemaSampler {

  private GraphSchemaSampler() {}

  /** The sample size to use: the default for zero or less. */
  public static int getSampleSize(int sampleSize) {
    return sampleSize > 0 ? sampleSize : GraphSchema.DEFAULT_SAMPLE_SIZE;
  }

  /**
   * The statement sampling nodes.
   *
   * @param quotedLabel The quoted label to sample, null to sample nodes with any label
   * @param sampleSize The maximum number of nodes
   * @return A statement returning properties, and labels when sampling any label
   */
  public static String getNodeSampleStatement(String quotedLabel, int sampleSize) {
    int limit = getSampleSize(sampleSize);
    if (quotedLabel == null) {
      return "MATCH (n) WITH n LIMIT "
          + limit
          + " RETURN labels(n) AS labels, properties(n) AS properties";
    }
    return "MATCH (n:"
        + quotedLabel
        + ") WITH n LIMIT "
        + limit
        + " RETURN properties(n) AS properties";
  }

  /**
   * The statement sampling relationships.
   *
   * @param quotedType The quoted relationship type to sample, null to sample any type
   * @param sampleSize The maximum number of relationships
   * @return A statement returning the labels of the start and end nodes and the properties, and the
   *     type when sampling any type
   */
  public static String getRelationshipSampleStatement(String quotedType, int sampleSize) {
    int limit = getSampleSize(sampleSize);
    String ends = "labels(a) AS startLabels, labels(b) AS endLabels, properties(r) AS properties";
    if (quotedType == null) {
      return "MATCH (a)-[r]->(b) WITH a, r, b LIMIT " + limit + " RETURN type(r) AS type, " + ends;
    }
    return "MATCH (a)-[r:" + quotedType + "]->(b) WITH a, r, b LIMIT " + limit + " RETURN " + ends;
  }

  /**
   * Sample the nodes and relationships.
   *
   * @param connection The connection to sample with
   * @param labels The labels to sample one by one, null to sample nodes with any label
   * @param types The relationship types to sample one by one, null to sample any type
   * @param quote Quotes a label or relationship type in the dialect of the database
   * @param sampleSize The maximum number of nodes per label and relationships per type
   * @return The schema, without indexes
   */
  public static GraphSchema sample(
      IGraphConnection connection,
      List<String> labels,
      List<String> types,
      UnaryOperator<String> quote,
      int sampleSize)
      throws HopException {
    GraphSchemaBuilder builder = new GraphSchemaBuilder();
    if (labels == null) {
      for (Map<String, Object> row :
          connection.execute(getNodeSampleStatement(null, sampleSize), Map.of())) {
        for (String label : toStrings(row.get("labels"))) {
          builder.addSampledNode(label, toMap(row.get("properties")));
        }
      }
    } else {
      for (String label : labels) {
        builder.addElement(GraphObjectType.NODE, label);
        for (Map<String, Object> row :
            connection.execute(getNodeSampleStatement(quote.apply(label), sampleSize), Map.of())) {
          builder.addSampledNode(label, toMap(row.get("properties")));
        }
      }
    }
    if (types == null) {
      for (Map<String, Object> row :
          connection.execute(getRelationshipSampleStatement(null, sampleSize), Map.of())) {
        addSampledRelationship(builder, String.valueOf(row.get("type")), row);
      }
    } else {
      for (String type : types) {
        builder.addElement(GraphObjectType.RELATIONSHIP, type);
        for (Map<String, Object> row :
            connection.execute(
                getRelationshipSampleStatement(quote.apply(type), sampleSize), Map.of())) {
          addSampledRelationship(builder, type, row);
        }
      }
    }
    return builder.build(null, true);
  }

  private static void addSampledRelationship(
      GraphSchemaBuilder builder, String type, Map<String, Object> row) {
    builder.addSampledRelationship(
        type,
        toStrings(row.get("startLabels")),
        toStrings(row.get("endLabels")),
        toMap(row.get("properties")));
  }

  /** A returned list of names as strings, a single value as a list of one. */
  public static List<String> toStrings(Object value) {
    List<String> strings = new ArrayList<>();
    if (value instanceof Iterable<?> iterable) {
      iterable.forEach(element -> strings.add(String.valueOf(element)));
    } else if (value != null) {
      strings.add(value.toString());
    }
    return strings;
  }

  /** A returned map of properties. */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> toMap(Object value) {
    return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
  }
}
