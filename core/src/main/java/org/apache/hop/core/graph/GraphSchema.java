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
 * The labels, relationship types and their properties of a graph database, read from the database
 * catalog where it has one, or from a sample of the nodes and relationships.
 *
 * @param entries One entry per property of a label or relationship type, one without property for a
 *     label or type without properties
 * @param indexes The indexes and unique constraints, null if the database can't tell
 * @param sampled True if the entries come from a sample, so that properties of the nodes and
 *     relationships outside the sample are missing
 */
public record GraphSchema(
    List<GraphSchemaEntry> entries, List<GraphIndex> indexes, boolean sampled) {

  /** How many nodes and how many relationships are sampled by default. */
  public static final int DEFAULT_SAMPLE_SIZE = 1000;

  public GraphSchema {
    entries = entries == null ? List.of() : List.copyOf(entries);
    indexes = indexes == null ? null : List.copyOf(indexes);
  }

  /** The same schema with these indexes. */
  public GraphSchema withIndexes(List<GraphIndex> newIndexes) {
    return new GraphSchema(entries, newIndexes, sampled);
  }

  /**
   * True if an index covers the property of this entry.
   *
   * @return null if the indexes are unknown
   */
  public Boolean isIndexed(GraphSchemaEntry entry) {
    return hasIndex(entry, false);
  }

  /**
   * True if a unique index or constraint covers the property of this entry.
   *
   * @return null if the indexes are unknown
   */
  public Boolean isUnique(GraphSchemaEntry entry) {
    return hasIndex(entry, true);
  }

  private Boolean hasIndex(GraphSchemaEntry entry, boolean unique) {
    if (indexes == null) {
      return null;
    }
    if (entry.property() == null) {
      return false;
    }
    for (GraphIndex index : indexes) {
      if (index.relationship() == entry.isRelationship()
          && (!unique || index.unique())
          && index.covers(entry.name(), entry.property())) {
        return true;
      }
    }
    return false;
  }
}
