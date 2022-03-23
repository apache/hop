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

package org.apache.hop.neo4j.transforms.graph;

import org.apache.hop.metadata.api.IEnumHasCode;

public enum NodeMappingType implements IEnumHasCode {
  All("all", "Select all defined labels (default)"),
  UsingValue("using-value", "Use a field value to select a specific label"),
  AddLabel("add-label", "Add the specified label if a field contains a specific value"),
  First("first", "Select the first label"),
  ;

  private String code;
  private String description;

  NodeMappingType(String code, String description) {
    this.code = code;
    this.description = description;
  }

  public static String[] getDescriptions() {
    String[] descriptions = new String[values().length];
    for (int i = 0; i < descriptions.length; i++) {
      descriptions[i] = values()[i].getDescription();
    }
    return descriptions;
  }

  public static NodeMappingType getTypeFromDescription(String description) {
    for (NodeMappingType type : values()) {
      if (type.getDescription().equals(description)) {
        return type;
      }
    }
    return All;
  }

  /**
   * Gets code
   *
   * @return value of code
   */
  @Override
  public String getCode() {
    return code;
  }

  /** @param code The code to set */
  public void setCode(String code) {
    this.code = code;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /** @param description The description to set */
  public void setDescription(String description) {
    this.description = description;
  }
}
