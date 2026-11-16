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

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.lineage.openlineage.OpenLineageDeliveryConfiguration;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * DataHub as a {@link LineageSink}. Where DataHub is, and the token and environment to write with,
 * come from a DataHub {@link DataCatalog}: Hop then reads from and writes to the same DataHub, and
 * the datasets it writes are the ones it reads. The rest are the options the DataHub sink otherwise
 * reads from {@code HOP_LINEAGE_DATAHUB_*} variables.
 */
@GuiPlugin(description = "DataHub lineage sink")
@Getter
@Setter
public class DataHubSinkConfiguration extends OpenLineageDeliveryConfiguration {

  public static final String GROUP_DATAHUB = "i18n::DataHubSinkConfiguration.Group.DataHub";

  /** The prefix of the DataHub sink's variables. */
  static final String DATAHUB_PREFIX = "HOP_LINEAGE_DATAHUB_";

  @GuiWidgetElement(
      id = "dataCatalog",
      order = "010",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.METADATA,
      metadata = DataCatalog.class,
      label = "i18n::DataHubSinkConfiguration.DataCatalog.Label",
      toolTip = "i18n::DataHubSinkConfiguration.DataCatalog.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DATAHUB,
      groupOrder = "010")
  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.DATA_CATALOG)
  private String dataCatalog;

  @GuiWidgetElement(
      id = "platformInstance",
      order = "020",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::DataHubSinkConfiguration.PlatformInstance.Label",
      toolTip = "i18n::DataHubSinkConfiguration.PlatformInstance.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DATAHUB,
      groupOrder = "010")
  @HopMetadataProperty
  private String platformInstance;

  @GuiWidgetElement(
      id = "datasetSchema",
      order = "030",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::DataHubSinkConfiguration.DatasetSchema.Label",
      toolTip = "i18n::DataHubSinkConfiguration.DatasetSchema.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DATAHUB,
      groupOrder = "010")
  @HopMetadataProperty
  private DataHubSinkConfig.SchemaPolicy datasetSchema = DataHubSinkConfig.SchemaPolicy.MISSING;

  @GuiWidgetElement(
      id = "lifecycle",
      order = "040",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::DataHubSinkConfiguration.Lifecycle.Label",
      toolTip = "i18n::DataHubSinkConfiguration.Lifecycle.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DATAHUB,
      groupOrder = "010")
  @HopMetadataProperty
  private boolean lifecycle;

  @GuiWidgetElement(
      id = "actor",
      order = "050",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::DataHubSinkConfiguration.Actor.Label",
      toolTip = "i18n::DataHubSinkConfiguration.Actor.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DATAHUB,
      groupOrder = "010")
  @HopMetadataProperty
  private String actor;

  @GuiWidgetElement(
      id = "sqlParse",
      order = "060",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::DataHubSinkConfiguration.SqlParse.Label",
      toolTip = "i18n::DataHubSinkConfiguration.SqlParse.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DATAHUB,
      groupOrder = "010")
  @HopMetadataProperty
  private boolean sqlParse = true;

  @Override
  public Map<String, String> toSettings(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    DataHubDataCatalog catalog = dataHubCatalog(variables, metadataProvider);
    Map<String, String> settings = new LinkedHashMap<>();
    put(settings, DataHubSinkConfig.HOP_LINEAGE_DATAHUB_URL, catalog.getUrl(), variables);
    if (!settings.containsKey(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_URL)) {
      throw new HopException(
          "Data catalog '" + variables.resolve(dataCatalog) + "' has no DataHub URL");
    }
    put(settings, DataHubSinkConfig.HOP_LINEAGE_DATAHUB_TOKEN, catalog.getToken(), variables);
    put(settings, DataHubSinkConfig.HOP_LINEAGE_DATAHUB_ENV, catalog.getEnvironment(), variables);
    // The catalog's trust store, unless the sink has one of its own (added below).
    put(settings, DATAHUB_PREFIX + "TRUSTSTORE_PATH", catalog.getTrustStorePath(), variables);
    put(
        settings,
        DATAHUB_PREFIX + "TRUSTSTORE_PASSWORD",
        catalog.getTrustStorePassword(),
        variables);
    put(settings, DATAHUB_PREFIX + "KEYSTORE_TYPE", catalog.getTrustStoreType(), variables);
    Map<String, String> databaseInstances =
        DataHubDatabaseInstances.resolve(
            catalog.getDatabaseInstances(), variables, metadataProvider);
    if (!databaseInstances.isEmpty()) {
      settings.put(
          DataHubSinkConfig.HOP_LINEAGE_DATAHUB_DATABASE_INSTANCES,
          DataHubDatabaseInstances.format(databaseInstances));
    }
    put(
        settings,
        DataHubSinkConfig.HOP_LINEAGE_DATAHUB_PLATFORM_INSTANCE,
        platformInstance,
        variables);
    if (datasetSchema != null) {
      settings.put(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_DATASET_SCHEMA, datasetSchema.getCode());
    }
    put(settings, DataHubSinkConfig.HOP_LINEAGE_DATAHUB_LIFECYCLE, lifecycle);
    put(settings, DataHubSinkConfig.HOP_LINEAGE_DATAHUB_ACTOR, actor, variables);
    put(settings, DATAHUB_PREFIX + "SQL_PARSE", sqlParse);
    addDeliverySettings(settings, DATAHUB_PREFIX, variables);
    return settings;
  }

  private DataHubDataCatalog dataHubCatalog(
      IVariables variables, IHopMetadataProvider metadataProvider) throws HopException {
    String name = variables.resolve(dataCatalog);
    if (Utils.isEmpty(name)) {
      throw new HopException("The DataHub lineage sink needs a DataHub data catalog");
    }
    DataCatalog catalog = metadataProvider.getSerializer(DataCatalog.class).load(name);
    if (catalog == null) {
      throw new HopException("Data catalog '" + name + "' does not exist");
    }
    if (!(catalog.getCatalog() instanceof DataHubDataCatalog dataHub)) {
      throw new HopException("Data catalog '" + name + "' is not a DataHub catalog");
    }
    return dataHub;
  }

  @Override
  public DataHubSinkConfiguration clone() {
    return (DataHubSinkConfiguration) super.clone();
  }
}
