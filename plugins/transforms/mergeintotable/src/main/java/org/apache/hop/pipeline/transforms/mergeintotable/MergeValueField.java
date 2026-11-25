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

package org.apache.hop.pipeline.transforms.mergeintotable;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

/**
 * A column the MERGE writes from a stream field: always on insert, and on update when {@link
 * #update} is set.
 */
@Getter
@Setter
public class MergeValueField {

  @HopMetadataProperty(
      key = "stream_name",
      injectionKey = "VALUE_STREAM_FIELD",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.ValueStreamField",
      hopMetadataPropertyType = HopMetadataPropertyType.STREAM_FIELD)
  @GuiTableColumn(
      id = "valueStreamField",
      order = "10",
      type = GuiTableColumnType.COMBO,
      label = "i18n::MergeIntoTableMeta.Column.StreamField.Label",
      toolTip = "i18n::MergeIntoTableMeta.Column.ValueStreamField.Tooltip",
      variables = false,
      width = 220)
  private String streamField = "";

  @HopMetadataProperty(
      key = "column_name",
      injectionKey = "VALUE_TABLE_COLUMN",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.ValueTableColumn",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
  @GuiTableColumn(
      id = "valueTableColumn",
      order = "20",
      type = GuiTableColumnType.COMBO,
      label = "i18n::MergeIntoTableMeta.Column.TableColumn.Label",
      toolTip = "i18n::MergeIntoTableMeta.Column.ValueTableColumn.Tooltip",
      variables = false,
      width = 220)
  private String tableColumn = "";

  @HopMetadataProperty(
      key = "update",
      injectionKey = "VALUE_UPDATE",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.ValueUpdate")
  @GuiTableColumn(
      id = "valueUpdate",
      order = "30",
      type = GuiTableColumnType.CHECKBOX,
      label = "i18n::MergeIntoTableMeta.Column.Update.Label",
      toolTip = "i18n::MergeIntoTableMeta.Column.Update.Tooltip",
      width = 90)
  private boolean update = true;

  public MergeValueField() {}

  public MergeValueField(String streamField, String tableColumn, boolean update) {
    this.streamField = streamField;
    this.tableColumn = tableColumn;
    this.update = update;
  }

  public MergeValueField(MergeValueField other) {
    this(other.streamField, other.tableColumn, other.update);
  }
}
