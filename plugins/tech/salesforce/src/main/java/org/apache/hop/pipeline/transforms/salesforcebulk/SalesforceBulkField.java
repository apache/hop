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

/** One stream field written as a Salesforce field in a bulk load. */
@Getter
@Setter
public class SalesforceBulkField {
  @HopMetadataProperty(key = "stream")
  @GuiTableColumn(
      id = "stream",
      order = "10",
      type = GuiTableColumnType.TEXT,
      label = "i18n::SalesforceBulkField.Stream.Label",
      width = 220)
  private String streamField = "";

  @HopMetadataProperty(key = "salesforce")
  @GuiTableColumn(
      id = "salesforce",
      order = "20",
      type = GuiTableColumnType.TEXT,
      label = "i18n::SalesforceBulkField.Salesforce.Label",
      width = 220)
  private String salesforceField = "";

  public SalesforceBulkField() {}

  public SalesforceBulkField(String streamField, String salesforceField) {
    this.streamField = streamField;
    this.salesforceField = salesforceField;
  }
}
