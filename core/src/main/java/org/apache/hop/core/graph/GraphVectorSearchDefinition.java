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

/**
 * A search for the nodes or relationships with the vectors nearest to a query vector, through a
 * vector index.
 *
 * <p>The statement of a dialect for it takes the query vector in the parameter {@link
 * #PARAMETER_VECTOR}, set by the caller for every search, and returns a column {@link
 * #COLUMN_SCORE} with the similarity, higher for nearer nodes or relationships, followed by a
 * column per returned property, named with {@link #getPropertyColumn(int)}. The hits come nearest
 * first.
 *
 * @param indexName The name of the vector index, for the databases which search an index by name
 * @param label The label of the nodes, or the type of the relationships, for the databases which
 *     search by label or type and property
 * @param property The property with the vectors, for the databases which search by label or type
 *     and property
 * @param k The maximum number of nodes or relationships to return
 * @param returnProperties The properties of the nodes or relationships to return
 * @param similarity How the index compares vectors, for the databases which return a distance that
 *     needs converting into a similarity. Cosine when null.
 * @param elementType Whether to search nodes or relationships. Nodes when null.
 */
public record GraphVectorSearchDefinition(
    String indexName,
    String label,
    String property,
    int k,
    List<String> returnProperties,
    GraphVectorSimilarity similarity,
    GraphObjectType elementType) {

  /** The parameter with the query vector, a list of numbers. */
  public static final String PARAMETER_VECTOR = "vector";

  /** The column with the similarity score. */
  public static final String COLUMN_SCORE = "score";

  public GraphVectorSearchDefinition {
    returnProperties = returnProperties == null ? List.of() : List.copyOf(returnProperties);
    similarity = similarity == null ? GraphVectorSimilarity.COSINE : similarity;
    elementType = elementType == null ? GraphObjectType.NODE : elementType;
  }

  /** A search for nodes. */
  public GraphVectorSearchDefinition(
      String indexName,
      String label,
      String property,
      int k,
      List<String> returnProperties,
      GraphVectorSimilarity similarity) {
    this(indexName, label, property, k, returnProperties, similarity, GraphObjectType.NODE);
  }

  /** True for a search for relationships. */
  public boolean isRelationship() {
    return elementType == GraphObjectType.RELATIONSHIP;
  }

  /** The name of the column with the returned property at this index. */
  public static String getPropertyColumn(int index) {
    return "p" + index;
  }
}
