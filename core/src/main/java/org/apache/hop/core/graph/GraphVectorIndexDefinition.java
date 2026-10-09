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
 * A vector index on one property of nodes with a label or relationships with a type.
 *
 * @param name The name of the index, may be empty for databases with unnamed indexes
 * @param objectType Whether the index is on nodes or relationships
 * @param objectName The node label or relationship type
 * @param properties The indexed properties: a vector index is on exactly one
 * @param dimensions The number of dimensions of the vectors, null when dropping the index
 * @param similarity How vectors are compared, cosine when null
 * @param capacity The number of vectors to reserve room for, null for the database default
 */
public record GraphVectorIndexDefinition(
    String name,
    GraphObjectType objectType,
    String objectName,
    List<String> properties,
    Integer dimensions,
    GraphVectorSimilarity similarity,
    Integer capacity) {

  public GraphVectorIndexDefinition {
    objectType = objectType == null ? GraphObjectType.NODE : objectType;
    properties = properties == null ? List.of() : List.copyOf(properties);
    similarity = similarity == null ? GraphVectorSimilarity.COSINE : similarity;
  }

  public boolean isRelationship() {
    return objectType == GraphObjectType.RELATIONSHIP;
  }
}
