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

package org.apache.hop.neo4j.actions.constraint;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class ConstraintUpdate {
  @HopMetadataProperty(key = "update_type")
  private UpdateType updateType;

  @HopMetadataProperty(key = "object_type")
  private ObjectType objectType;

  @HopMetadataProperty(key = "index_name")
  private String constraintName;

  @HopMetadataProperty(key = "object_name")
  private String objectName;

  @HopMetadataProperty(key = "object_properties")
  private String objectProperties;

  @HopMetadataProperty(key = "constraint_type")
  private ConstraintType constraintType;

  public ConstraintUpdate() {
    constraintType = ConstraintType.UNIQUE;
    updateType = UpdateType.CREATE;
  }

  public ConstraintUpdate(ConstraintUpdate u) {
    this.updateType = u.updateType;
    this.objectType = u.objectType;
    this.constraintName = u.constraintName;
    this.objectName = u.objectName;
    this.objectProperties = u.objectProperties;
    this.constraintType = u.constraintType;
  }

  public ConstraintUpdate(
      UpdateType updateType,
      ObjectType objectType,
      ConstraintType constraintType,
      String constraintName,
      String objectName,
      String objectProperties) {
    this.updateType = updateType;
    this.objectType = objectType;
    this.constraintName = constraintName;
    this.objectName = objectName;
    this.objectProperties = objectProperties;
    this.constraintType = constraintType;
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
   * Gets constraintName
   *
   * @return value of constraintName
   */
  public String getConstraintName() {
    return constraintName;
  }

  /** @param constraintName The constraintName to set */
  public void setConstraintName(String constraintName) {
    this.constraintName = constraintName;
  }

  /**
   * Gets objectName
   *
   * @return value of objectName
   */
  public String getObjectName() {
    return objectName;
  }

  /** @param objectName The objectName to set */
  public void setObjectName(String objectName) {
    this.objectName = objectName;
  }

  /**
   * Gets objectProperties
   *
   * @return value of objectProperties
   */
  public String getObjectProperties() {
    return objectProperties;
  }

  /** @param objectProperties The objectProperties to set */
  public void setObjectProperties(String objectProperties) {
    this.objectProperties = objectProperties;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public UpdateType getUpdateType() {
    return updateType;
  }

  /** @param updateType The type to set */
  public void setUpdateType(UpdateType updateType) {
    this.updateType = updateType;
  }

  /**
   * Gets constraintType
   *
   * @return value of constraintType
   */
  public ConstraintType getConstraintType() {
    return constraintType;
  }

  /** @param constraintType The constraintType to set */
  public void setConstraintType(ConstraintType constraintType) {
    this.constraintType = constraintType;
  }
}
