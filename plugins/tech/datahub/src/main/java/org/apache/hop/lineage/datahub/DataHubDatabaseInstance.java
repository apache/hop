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

package org.apache.hop.lineage.datahub;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * The DataHub platform instance of the tables of one relational connection: the {@code
 * platform_instance} of the DataHub ingestion source that reads that database server.
 */
@Getter
@Setter
public class DataHubDatabaseInstance {

  @HopMetadataProperty
  @GuiTableColumn(
      id = "connection",
      order = "10",
      type = GuiTableColumnType.COMBO,
      comboValuesMethod = "connectionNames",
      label = "i18n::DataHubDatabaseInstance.Connection.Label",
      toolTip = "i18n::DataHubDatabaseInstance.Connection.Tooltip",
      width = 240)
  private String connection = "";

  @HopMetadataProperty
  @GuiTableColumn(
      id = "platformInstance",
      order = "20",
      type = GuiTableColumnType.TEXT,
      label = "i18n::DataHubDatabaseInstance.PlatformInstance.Label",
      toolTip = "i18n::DataHubDatabaseInstance.PlatformInstance.Tooltip",
      width = 240)
  private String platformInstance = "";

  public DataHubDatabaseInstance() {}

  public DataHubDatabaseInstance(String connection, String platformInstance) {
    this.connection = connection;
    this.platformInstance = platformInstance;
  }
}
