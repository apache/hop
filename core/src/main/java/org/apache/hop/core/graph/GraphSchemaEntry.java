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
 * One property of the nodes with a label or of the relationships with a type, as far as the
 * database tells or a sample shows.
 *
 * @param elementType Nodes or relationships
 * @param name The node label or relationship type
 * @param property The property, null for a label or type without properties
 * @param propertyTypes The types of the values seen, for example String or Integer. Empty if
 *     unknown.
 * @param mandatory True if every node or relationship has the property, null if unknown
 * @param startLabels For relationships: the labels of the nodes they start from, empty if unknown
 * @param endLabels For relationships: the labels of the nodes they end at, empty if unknown
 */
public record GraphSchemaEntry(
    GraphObjectType elementType,
    String name,
    String property,
    List<String> propertyTypes,
    Boolean mandatory,
    List<String> startLabels,
    List<String> endLabels) {

  public GraphSchemaEntry {
    elementType = elementType == null ? GraphObjectType.NODE : elementType;
    propertyTypes = propertyTypes == null ? List.of() : List.copyOf(propertyTypes);
    startLabels = startLabels == null ? List.of() : List.copyOf(startLabels);
    endLabels = endLabels == null ? List.of() : List.copyOf(endLabels);
  }

  public boolean isRelationship() {
    return elementType == GraphObjectType.RELATIONSHIP;
  }
}
