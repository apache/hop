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
 *
 */

package org.apache.hop.neo4j.transforms.cypherbuilder.operation;

public enum OperationType {
  MATCH("MATCH", "Match node"),
  MERGE("MERGE", "Merge node"),
  CREATE("CREATE", "Create node"),
  DELETE("DELETE", "Delete node"),
  SET("SET", "Set values"),
  RETURN("RETURN", "Return values"),
  ORDER_BY("ORDER BY", "Order by"),
  EDGE_MATCH("MATCH", "Match relationship"),
  EDGE_MERGE("MERGE", "Merge relationship"),
  EDGE_CREATE("CREATE", "Create relationship"),
  ;

  private final String keyWord;
  private final String description;

  OperationType(String keyWord, String description) {
    this.keyWord = keyWord;
    this.description = description;
  }

  public static String[] getOperationDescriptions() {
    String[] descriptions = new String[OperationType.values().length];
    for (int i = 0; i < values().length; i++) {
      descriptions[i] = values()[i].getDescription();
    }
    return descriptions;
  }

  public static OperationType getOperationByDescription(String description) {
    for (OperationType operationType : values()) {
      if (operationType.description.equalsIgnoreCase(description)) {
        return operationType;
      }
    }
    return null;
  }

  /**
   * Gets keyWord
   *
   * @return value of keyWord
   */
  public String keyWord() {
    return keyWord;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }
}
