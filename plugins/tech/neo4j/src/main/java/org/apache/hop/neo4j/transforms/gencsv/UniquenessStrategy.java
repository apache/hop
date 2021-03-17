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

package org.apache.hop.neo4j.transforms.gencsv;

public enum UniquenessStrategy {
  None, // Don't calculate unique nore or relationship values
  First, // Take the first version of the node or relationship
  Last, // Take the last version of the node or relationship
// UpdateProperties, // Not supported yet.  Update all the available properties
;

  public static final UniquenessStrategy getStrategyFromName(String name) {
    for (UniquenessStrategy strategy : values()) {
      if (strategy.name().equalsIgnoreCase(name)) {
        return strategy;
      }
    }
    return None;
  }

  public static final String[] getNames() {
    String[] names = new String[values().length];
    for (int i = 0; i < names.length; i++) {
      names[i] = values()[i].name();
    }
    return names;
  }
}
