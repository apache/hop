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
 *
 */

package org.apache.hop.neo4j.transforms.graph;

import org.apache.hop.core.injection.Injection;

public class FieldModelMapping {

  /** The Hop input field where the data is coming from */
  @Injection(name = "MAPPING_SOURCE_FIELD", group = "MAPPINGS")
  private String field;

  /** Write to a node or a relationship */
  @Injection(name = "MAPPING_TARGET_TYPE", group = "MAPPINGS")
  private ModelTargetType targetType;

  /** Name of the node or relationship to write to */
  @Injection(name = "MAPPING_TARGET_NAME", group = "MAPPINGS")
  private String targetName;

  /** Name of the property to write to */
  @Injection(name = "MAPPING_TARGET_PROPERTY", group = "MAPPINGS")
  private String targetProperty;

  public FieldModelMapping() {
    targetType = ModelTargetType.Node;
  }

  public FieldModelMapping(
      String field, ModelTargetType targetType, String targetName, String targetProperty) {
    this.field = field;
    this.targetType = targetType;
    this.targetName = targetName;
    this.targetProperty = targetProperty;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public ModelTargetType getTargetType() {
    return targetType;
  }

  public void setTargetType(ModelTargetType targetType) {
    this.targetType = targetType;
  }

  public String getTargetName() {
    return targetName;
  }

  public void setTargetName(String targetName) {
    this.targetName = targetName;
  }

  public String getTargetProperty() {
    return targetProperty;
  }

  public void setTargetProperty(String targetProperty) {
    this.targetProperty = targetProperty;
  }
}
