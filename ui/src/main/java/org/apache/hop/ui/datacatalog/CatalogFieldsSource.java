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

package org.apache.hop.ui.datacatalog;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.datacatalog.CatalogDataset;
import org.apache.hop.datacatalog.CatalogDatasetType;
import org.apache.hop.datacatalog.DataCatalog;

/** The data catalog and dataset to take the fields of a transform from. */
@GuiPlugin(description = "Get fields from a data catalog")
@Getter
@Setter
public class CatalogFieldsSource {

  public static final String PARENT_ID = "CatalogFieldsDialog.Parent";

  private static final String GROUP = "i18n::CatalogFieldsDialog.Group.Dataset";

  @GuiWidgetElement(
      id = "catalogName",
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DataCatalog.class,
      parentId = PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      groupOrder = "10",
      label = "i18n::CatalogFieldsDialog.Catalog.Label",
      toolTip = "i18n::CatalogFieldsDialog.Catalog.Tooltip")
  private String catalogName;

  @GuiWidgetElement(
      id = "datasetType",
      order = "0200",
      type = GuiElementType.COMBO,
      variables = false,
      parentId = PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      groupOrder = "10",
      label = "i18n::CatalogFieldsDialog.DatasetType.Label",
      toolTip = "i18n::CatalogFieldsDialog.DatasetType.Tooltip")
  private CatalogDatasetType datasetType = CatalogDatasetType.FILE;

  @GuiWidgetElement(
      id = "connection",
      order = "0300",
      type = GuiElementType.METADATA,
      metadata = DatabaseMeta.class,
      parentId = PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      groupOrder = "10",
      label = "i18n::CatalogFieldsDialog.Connection.Label",
      toolTip = "i18n::CatalogFieldsDialog.Connection.Tooltip")
  private String connection;

  @GuiWidgetElement(
      id = "schema",
      order = "0400",
      type = GuiElementType.TEXT,
      parentId = PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      groupOrder = "10",
      label = "i18n::CatalogFieldsDialog.Schema.Label",
      toolTip = "i18n::CatalogFieldsDialog.Schema.Tooltip")
  private String schema;

  @GuiWidgetElement(
      id = "name",
      order = "0500",
      type = GuiElementType.TEXT,
      parentId = PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP,
      groupOrder = "10",
      label = "i18n::CatalogFieldsDialog.Name.Label",
      toolTip = "i18n::CatalogFieldsDialog.Name.Tooltip")
  private String name;

  public CatalogDataset dataset() {
    return new CatalogDataset(datasetType, connection, schema, name);
  }
}
