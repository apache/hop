/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.neo4j.actions.index;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class IndexUpdate {
  @HopMetadataProperty(key = "object_type")
  private ObjectType objectType;

  @HopMetadataProperty(key = "index_name")
  private String indexName;

  @HopMetadataProperty(key = "object_name")
  private String objectName;

  @HopMetadataProperty(key = "object_properties")
  private String objectProperties;

  @HopMetadataProperty(key = "update_type")
  private UpdateType type;

  /** RANGE (the default, also for actions saved before vector indexes) or VECTOR. */
  @HopMetadataProperty(key = "index_type")
  private IndexType indexType;

  /** VECTOR indexes: the number of dimensions of the vectors. */
  @HopMetadataProperty(key = "vector_dimensions")
  private String vectorDimensions;

  /** VECTOR indexes: how vectors are compared, COSINE when not set. */
  @HopMetadataProperty(key = "vector_similarity")
  private GraphVectorSimilarity vectorSimilarity;

  /** VECTOR indexes on Memgraph: the number of vectors to reserve room for. */
  @HopMetadataProperty(key = "vector_capacity")
  private String vectorCapacity;

  public IndexUpdate() {}

  public IndexUpdate(
      UpdateType type,
      ObjectType objectType,
      String indexName,
      String objectName,
      String objectProperties) {
    this.type = type;
    this.objectType = objectType;
    this.indexName = indexName;
    this.objectName = objectName;
    this.objectProperties = objectProperties;
  }

  public IndexUpdate(IndexUpdate i) {
    this.objectType = i.objectType;
    this.indexName = i.indexName;
    this.objectName = i.objectName;
    this.objectProperties = i.objectProperties;
    this.type = i.type;
    this.indexType = i.indexType;
    this.vectorDimensions = i.vectorDimensions;
    this.vectorSimilarity = i.vectorSimilarity;
    this.vectorCapacity = i.vectorCapacity;
  }

  /** A vector index update. */
  public static IndexUpdate vector(
      UpdateType type,
      ObjectType objectType,
      String indexName,
      String objectName,
      String property,
      String dimensions,
      GraphVectorSimilarity similarity) {
    IndexUpdate update = new IndexUpdate(type, objectType, indexName, objectName, property);
    update.setIndexType(IndexType.VECTOR);
    update.setVectorDimensions(dimensions);
    update.setVectorSimilarity(similarity);
    return update;
  }

  /** True for a vector index. */
  public boolean isVector() {
    return indexType == IndexType.VECTOR;
  }
}
