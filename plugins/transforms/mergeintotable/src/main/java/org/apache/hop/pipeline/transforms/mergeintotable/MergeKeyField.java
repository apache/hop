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

/** A key of the MERGE: a table column that must equal a stream field for a row to match. */
@Getter
@Setter
public class MergeKeyField {

  @HopMetadataProperty(
      key = "stream_name",
      injectionKey = "KEY_STREAM_FIELD",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.KeyStreamField",
      hopMetadataPropertyType = HopMetadataPropertyType.STREAM_FIELD)
  @GuiTableColumn(
      id = "keyStreamField",
      order = "10",
      type = GuiTableColumnType.COMBO,
      label = "i18n::MergeIntoTableMeta.Column.StreamField.Label",
      toolTip = "i18n::MergeIntoTableMeta.Column.KeyStreamField.Tooltip",
      variables = false,
      width = 220)
  private String streamField = "";

  @HopMetadataProperty(
      key = "column_name",
      injectionKey = "KEY_TABLE_COLUMN",
      injectionKeyDescription = "MergeIntoTableMeta.Injection.KeyTableColumn",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
  @GuiTableColumn(
      id = "keyTableColumn",
      order = "20",
      type = GuiTableColumnType.COMBO,
      label = "i18n::MergeIntoTableMeta.Column.TableColumn.Label",
      toolTip = "i18n::MergeIntoTableMeta.Column.KeyTableColumn.Tooltip",
      variables = false,
      width = 220)
  private String tableColumn = "";

  public MergeKeyField() {}

  public MergeKeyField(String streamField, String tableColumn) {
    this.streamField = streamField;
    this.tableColumn = tableColumn;
  }

  public MergeKeyField(MergeKeyField other) {
    this(other.streamField, other.tableColumn);
  }
}
