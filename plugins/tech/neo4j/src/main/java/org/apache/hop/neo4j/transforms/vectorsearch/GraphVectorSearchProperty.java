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

package org.apache.hop.neo4j.transforms.vectorsearch;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** A property of the nodes found, returned in an output field. */
@Getter
@Setter
public class GraphVectorSearchProperty {

  @GuiTableColumn(
      order = "10",
      type = GuiTableColumnType.TEXT,
      label = "i18n::GraphVectorSearch.Property.Property.Label",
      toolTip = "i18n::GraphVectorSearch.Property.Property.Tooltip")
  @HopMetadataProperty(
      key = "property",
      injectionKey = "PROPERTY",
      injectionKeyDescription = "GraphVectorSearch.Injection.PROPERTY")
  private String property;

  @GuiTableColumn(
      order = "20",
      type = GuiTableColumnType.TEXT,
      variables = false,
      label = "i18n::GraphVectorSearch.Property.FieldName.Label",
      toolTip = "i18n::GraphVectorSearch.Property.FieldName.Tooltip")
  @HopMetadataProperty(
      key = "field_name",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "GraphVectorSearch.Injection.FIELD_NAME")
  private String fieldName;

  @GuiTableColumn(
      order = "30",
      type = GuiTableColumnType.COMBO,
      variables = false,
      label = "i18n::GraphVectorSearch.Property.Type.Label",
      toolTip = "i18n::GraphVectorSearch.Property.Type.Tooltip",
      comboValuesMethod = "getValueTypeNames")
  @HopMetadataProperty(
      key = "type",
      injectionKey = "TYPE",
      injectionKeyDescription = "GraphVectorSearch.Injection.TYPE")
  private String type;

  public GraphVectorSearchProperty() {}

  public GraphVectorSearchProperty(String property, String fieldName, String type) {
    this.property = property;
    this.fieldName = fieldName;
    this.type = type;
  }

  public GraphVectorSearchProperty(GraphVectorSearchProperty other) {
    this(other.property, other.fieldName, other.type);
  }
}
