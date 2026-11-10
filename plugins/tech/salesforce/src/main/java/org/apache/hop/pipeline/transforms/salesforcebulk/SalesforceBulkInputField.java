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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** One column read from a Bulk API 2.0 query result. */
@Getter
@Setter
public class SalesforceBulkInputField {
  @HopMetadataProperty(key = "name")
  @GuiTableColumn(
      id = "name",
      order = "10",
      type = GuiTableColumnType.TEXT,
      label = "i18n::SalesforceBulkInputField.Name.Label",
      width = 220)
  private String name = "";

  @HopMetadataProperty(key = "type")
  @GuiTableColumn(
      id = "type",
      order = "20",
      type = GuiTableColumnType.COMBO,
      comboValuesMethod = "hopTypeNames",
      label = "i18n::SalesforceBulkInputField.Type.Label",
      width = 140)
  private String type = "String";

  public SalesforceBulkInputField() {}

  public SalesforceBulkInputField(String name, String type) {
    this.name = name;
    this.type = type;
  }
}
