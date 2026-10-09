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

/** The types of constraints a graph database can have. */
public enum GraphConstraintType {
  UNIQUE,
  NOT_NULL,
  NODE_KEY,
  ;

  public static String[] getNames() {
    String[] names = new String[values().length];
    for (int i = 0; i < names.length; i++) {
      names[i] = values()[i].name();
    }
    return names;
  }

  /** The type with the given name, null when it is unknown. */
  public static GraphConstraintType getType(String code) {
    for (GraphConstraintType type : values()) {
      if (type.name().equalsIgnoreCase(code)) {
        return type;
      }
    }
    return null;
  }
}
