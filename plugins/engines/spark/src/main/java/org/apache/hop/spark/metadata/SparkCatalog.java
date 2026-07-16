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

package org.apache.hop.spark.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.spark.table.SparkLakeFormats;

/**
 * Reusable Spark SQL catalog configuration for lakehouse TABLE mode (Iceberg Hadoop / REST, etc.).
 * Applied as {@code spark.sql.catalog.<catalogName>.*} when building a native Spark session.
 */
@Getter
@Setter
@GuiPlugin
@HopMetadata(
    key = "SparkCatalog",
    name = "i18n::SparkCatalog.Name",
    description = "i18n::SparkCatalog.Description",
    image = "spark-catalog.svg",
    category = HopMetadataCategory.CONNECTIONS,
    documentationUrl = "/metadata-types/spark-catalog.html")
public class SparkCatalog extends HopMetadataBase implements Serializable, IHopMetadata {

  public static final String TYPE_HADOOP = "hadoop";
  public static final String TYPE_REST = "rest";
  public static final String TYPE_CUSTOM = "custom";

  /** Advanced — confExtra only; not packaged by default. */
  public static final String TYPE_HIVE = "hive";

  /** Advanced — confExtra only; not packaged by default. */
  public static final String TYPE_GLUE = "glue";

  private static final String PARENT = SparkCatalogEditor.GUI_WIDGETS_PARENT_ID;

  @GuiWidgetElement(
      id = "10000-catalog-name",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::SparkCatalog.CatalogName.Label",
      toolTip = "i18n::SparkCatalog.CatalogName.Tooltip")
  @HopMetadataProperty
  private String catalogName;

  @GuiWidgetElement(
      id = "10010-catalog-type",
      parentId = PARENT,
      type = GuiElementType.COMBO,
      label = "i18n::SparkCatalog.CatalogType.Label",
      toolTip = "i18n::SparkCatalog.CatalogType.Tooltip",
      comboValuesMethod = "getCatalogTypeValues")
  @HopMetadataProperty
  private String catalogType = TYPE_HADOOP;

  @GuiWidgetElement(
      id = "10020-implementation",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::SparkCatalog.Implementation.Label",
      toolTip = "i18n::SparkCatalog.Implementation.Tooltip")
  @HopMetadataProperty
  private String implementation;

  @GuiWidgetElement(
      id = "10030-warehouse",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::SparkCatalog.Warehouse.Label",
      toolTip = "i18n::SparkCatalog.Warehouse.Tooltip")
  @HopMetadataProperty
  private String warehouse;

  @GuiWidgetElement(
      id = "10040-uri",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::SparkCatalog.Uri.Label",
      toolTip = "i18n::SparkCatalog.Uri.Tooltip")
  @HopMetadataProperty
  private String uri;

  @GuiWidgetElement(
      id = "10050-credential",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::SparkCatalog.Credential.Label",
      toolTip = "i18n::SparkCatalog.Credential.Tooltip")
  @HopMetadataProperty(password = true)
  private String credential;

  @GuiWidgetElement(
      id = "10060-conf-extra",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::SparkCatalog.ConfExtra.Label",
      toolTip = "i18n::SparkCatalog.ConfExtra.Tooltip")
  @HopMetadataProperty
  private String confExtra;

  public SparkCatalog() {
    this.catalogType = TYPE_HADOOP;
    this.implementation = SparkLakeFormats.ICEBERG_CATALOG;
  }

  /** Combo values for catalog type widget (GuiCompositeWidgets signature). */
  public java.util.List<String> getCatalogTypeValues(
      org.apache.hop.core.logging.ILogChannel log,
      org.apache.hop.metadata.api.IHopMetadataProvider metadataProvider) {
    return java.util.List.of(TYPE_HADOOP, TYPE_REST, TYPE_CUSTOM, TYPE_HIVE, TYPE_GLUE);
  }
}
