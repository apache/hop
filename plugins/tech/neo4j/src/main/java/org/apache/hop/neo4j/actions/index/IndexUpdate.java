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

import org.apache.hop.metadata.api.HopMetadataProperty;

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
  }

  /**
   * Gets objectType
   *
   * @return value of objectType
   */
  public ObjectType getObjectType() {
    return objectType;
  }

  /** @param objectType The objectType to set */
  public void setObjectType(ObjectType objectType) {
    this.objectType = objectType;
  }

  /**
   * Gets indexName
   *
   * @return value of indexName
   */
  public String getIndexName() {
    return indexName;
  }

  /** @param indexName The indexName to set */
  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  /**
   * Gets nodeName
   *
   * @return value of nodeName
   */
  public String getObjectName() {
    return objectName;
  }

  /** @param objectName The nodeName to set */
  public void setObjectName(String objectName) {
    this.objectName = objectName;
  }

  /**
   * Gets properties
   *
   * @return value of properties
   */
  public String getObjectProperties() {
    return objectProperties;
  }

  /** @param objectProperties The properties to set */
  public void setObjectProperties(String objectProperties) {
    this.objectProperties = objectProperties;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public UpdateType getType() {
    return type;
  }

  /** @param type The type to set */
  public void setType(UpdateType type) {
    this.type = type;
  }
}
