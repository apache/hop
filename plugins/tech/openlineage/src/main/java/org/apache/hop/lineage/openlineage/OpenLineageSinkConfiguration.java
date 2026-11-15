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

package org.apache.hop.lineage.openlineage;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * An OpenLineage collector as a {@link LineageSink}: the options the OpenLineage sink otherwise
 * reads from {@code HOP_LINEAGE_OPENLINEAGE_*} variables.
 */
@GuiPlugin(description = "OpenLineage lineage sink")
@Getter
@Setter
public class OpenLineageSinkConfiguration extends OpenLineageDeliveryConfiguration {

  public static final String GROUP_COLLECTOR = "i18n::OpenLineageSinkConfiguration.Group.Collector";
  public static final String GROUP_LINEAGE = "i18n::OpenLineageSinkConfiguration.Group.Lineage";

  /** The prefix of the OpenLineage sink's variables. */
  static final String OPENLINEAGE_PREFIX = "HOP_LINEAGE_OPENLINEAGE_";

  @GuiWidgetElement(
      id = "url",
      order = "010",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.Url.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.Url.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_COLLECTOR,
      groupOrder = "010")
  @HopMetadataProperty
  private String url;

  @GuiWidgetElement(
      id = "apiKey",
      order = "020",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::OpenLineageSinkConfiguration.ApiKey.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.ApiKey.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_COLLECTOR,
      groupOrder = "010")
  @HopMetadataProperty(password = true)
  private String apiKey;

  @GuiWidgetElement(
      id = "namespace",
      order = "030",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.Namespace.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.Namespace.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_COLLECTOR,
      groupOrder = "010")
  @HopMetadataProperty
  private String namespace;

  @GuiWidgetElement(
      id = "producer",
      order = "040",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.Producer.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.Producer.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_COLLECTOR,
      groupOrder = "010")
  @HopMetadataProperty
  private String producer;

  @GuiWidgetElement(
      id = "transformJobs",
      order = "110",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::OpenLineageSinkConfiguration.TransformJobs.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.TransformJobs.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_LINEAGE,
      groupOrder = "020")
  @HopMetadataProperty
  private boolean transformJobs;

  @GuiWidgetElement(
      id = "parentRuns",
      order = "120",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::OpenLineageSinkConfiguration.ParentRuns.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.ParentRuns.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_LINEAGE,
      groupOrder = "020")
  @HopMetadataProperty
  private boolean parentRuns = true;

  @GuiWidgetElement(
      id = "fileSpecNaming",
      order = "130",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::OpenLineageSinkConfiguration.FileSpecNaming.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.FileSpecNaming.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_LINEAGE,
      groupOrder = "020")
  @HopMetadataProperty
  private boolean fileSpecNaming = true;

  @GuiWidgetElement(
      id = "sqlParse",
      order = "140",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::OpenLineageSinkConfiguration.SqlParse.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.SqlParse.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_LINEAGE,
      groupOrder = "020")
  @HopMetadataProperty
  private boolean sqlParse = true;

  @Override
  public Map<String, String> toSettings(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    Map<String, String> settings = new LinkedHashMap<>();
    put(settings, OpenLineageSinkConfig.VAR_URL, url, variables);
    if (!settings.containsKey(OpenLineageSinkConfig.VAR_URL)) {
      throw new HopException("The OpenLineage lineage sink needs the URL of a collector");
    }
    put(settings, OpenLineageSinkConfig.VAR_API_KEY, apiKey, variables);
    put(settings, OpenLineageSinkConfig.VAR_NAMESPACE, namespace, variables);
    put(settings, OpenLineageSinkConfig.VAR_PRODUCER, producer, variables);
    put(settings, OpenLineageSinkConfig.VAR_TRANSFORM_JOBS, transformJobs);
    put(settings, OpenLineageSinkConfig.VAR_PARENT_RUNS, parentRuns);
    put(settings, OpenLineageSinkConfig.VAR_FILE_SPEC_NAMING, fileSpecNaming);
    put(settings, OpenLineageSinkConfig.VAR_SQL_PARSE, sqlParse);
    addDeliverySettings(settings, OPENLINEAGE_PREFIX, variables);
    return settings;
  }

  @Override
  public OpenLineageSinkConfiguration clone() {
    return (OpenLineageSinkConfiguration) super.clone();
  }
}
