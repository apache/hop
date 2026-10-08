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

package org.apache.hop.neo4j.actions.propertygraph;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Which table holds the edges of a graph model relationship. Empty values take the defaults. */
@Getter
@Setter
public class EdgeTableMapping {
  @HopMetadataProperty
  @GuiTableColumn(
      order = "10",
      type = GuiTableColumnType.TEXT,
      label = "i18n::ActionCreatePropertyGraph.EdgeTables.Relationship.Label",
      variables = false)
  private String relationshipName;

  @HopMetadataProperty
  @GuiTableColumn(
      order = "20",
      type = GuiTableColumnType.TEXT,
      label = "i18n::ActionCreatePropertyGraph.EdgeTables.Table.Label")
  private String tableName;

  @HopMetadataProperty
  @GuiTableColumn(
      order = "30",
      type = GuiTableColumnType.TEXT,
      label = "i18n::ActionCreatePropertyGraph.EdgeTables.KeyColumns.Label")
  private String keyColumns;

  @HopMetadataProperty
  @GuiTableColumn(
      order = "40",
      type = GuiTableColumnType.TEXT,
      label = "i18n::ActionCreatePropertyGraph.EdgeTables.SourceKeyColumns.Label")
  private String sourceKeyColumns;

  @HopMetadataProperty
  @GuiTableColumn(
      order = "50",
      type = GuiTableColumnType.TEXT,
      label = "i18n::ActionCreatePropertyGraph.EdgeTables.TargetKeyColumns.Label")
  private String targetKeyColumns;

  public EdgeTableMapping() {}

  public EdgeTableMapping(
      String relationshipName,
      String tableName,
      String keyColumns,
      String sourceKeyColumns,
      String targetKeyColumns) {
    this.relationshipName = relationshipName;
    this.tableName = tableName;
    this.keyColumns = keyColumns;
    this.sourceKeyColumns = sourceKeyColumns;
    this.targetKeyColumns = targetKeyColumns;
  }
}
