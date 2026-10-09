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
 * An index or unique constraint of a graph database, as far as matters to know which properties of
 * which labels are indexed.
 *
 * @param name The name of the index, may be empty
 * @param relationship True for an index on relationships, false for one on nodes
 * @param labelsOrTypes The node labels or relationship types the index is on
 * @param properties The indexed properties, or {@link #ALL_PROPERTIES} for an index on all of them
 * @param unique True if the index or constraint guarantees unique values
 */
public record GraphIndex(
    String name,
    boolean relationship,
    List<String> labelsOrTypes,
    List<String> properties,
    boolean unique) {

  /** The properties of an index on all properties, for example a GIN index on Apache AGE. */
  public static final List<String> ALL_PROPERTIES = List.of("*");

  /** True if this index covers the given property of the given label or relationship type. */
  public boolean covers(String labelOrType, String property) {
    return labelsOrTypes.contains(labelOrType)
        && (properties.contains(property) || properties.equals(ALL_PROPERTIES));
  }
}
