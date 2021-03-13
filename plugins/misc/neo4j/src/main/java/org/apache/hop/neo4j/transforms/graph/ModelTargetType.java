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

public enum ModelTargetType {
  Unmapped,
  Node,
  Relationship,
  ;

  /**
   * Get the code for a type, handles the null case
   *
   * @param type
   * @return
   */
  public static String getCode(ModelTargetType type) {
    if (type == null) {
      return null;
    }
    return type.name();
  }

  /**
   * Default to Unmapped in case we can't recognize the code or is null
   *
   * @param code
   * @return
   */
  public static ModelTargetType parseCode(String code) {
    if (code == null) {
      return Unmapped;
    }
    try {
      return ModelTargetType.valueOf(code);
    } catch (IllegalArgumentException e) {
      return Unmapped;
    }
  }

  public static String[] getNames() {
    String[] names = new String[values().length];
    for (int i = 0; i < names.length; i++) {
      names[i] = values()[i].name();
    }
    return names;
  }
}
