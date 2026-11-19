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

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.metadata.ILineageSinkConfiguration;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * The delivery options of a lineage sink built on the OpenLineage sink: how events wait, are
 * retried and spooled, and the HTTP client's timeouts and trust store. Each option is the setting
 * of the same name the sink reads from variables, under the sink's own prefix. An option left empty
 * keeps the sink's default.
 */
@Getter
@Setter
public abstract class OpenLineageDeliveryConfiguration implements ILineageSinkConfiguration {

  public static final String GROUP_DELIVERY = "i18n::OpenLineageSinkConfiguration.Group.Delivery";
  private static final String GROUP_DELIVERY_ORDER = "900";

  /** What happens to an event when the queue of events waiting for delivery is full. */
  public enum Overflow implements IEnumHasCodeAndDescription {
    DROP("OpenLineageSinkConfiguration.Overflow.Drop"),
    BLOCK("OpenLineageSinkConfiguration.Overflow.Block");

    private final String descriptionKey;

    Overflow(String descriptionKey) {
      this.descriptionKey = descriptionKey;
    }

    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getDescription() {
      return BaseMessages.getString(OpenLineageDeliveryConfiguration.class, descriptionKey);
    }
  }

  private String pluginId;
  private String pluginName;

  @GuiWidgetElement(
      id = "spoolDirectory",
      order = "900",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label = "i18n::OpenLineageSinkConfiguration.SpoolDirectory.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.SpoolDirectory.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String spoolDirectory;

  @GuiWidgetElement(
      id = "spoolMaxBytes",
      order = "910",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.SpoolMaxBytes.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.SpoolMaxBytes.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String spoolMaxBytes;

  @GuiWidgetElement(
      id = "bufferSize",
      order = "920",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.BufferSize.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.BufferSize.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String bufferSize;

  @GuiWidgetElement(
      id = "overflowPolicy",
      order = "930",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::OpenLineageSinkConfiguration.Overflow.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.Overflow.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private Overflow overflowPolicy = Overflow.DROP;

  @GuiWidgetElement(
      id = "enqueueTimeoutMs",
      order = "940",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.EnqueueTimeout.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.EnqueueTimeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String enqueueTimeoutMs;

  @GuiWidgetElement(
      id = "maxRetries",
      order = "950",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.MaxRetries.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.MaxRetries.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String maxRetries;

  @GuiWidgetElement(
      id = "retryBackoffMs",
      order = "960",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.RetryBackoff.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.RetryBackoff.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String retryBackoffMs;

  @GuiWidgetElement(
      id = "connectTimeoutMs",
      order = "970",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.ConnectTimeout.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.ConnectTimeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String connectTimeoutMs;

  @GuiWidgetElement(
      id = "readTimeoutMs",
      order = "980",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.ReadTimeout.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.ReadTimeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String readTimeoutMs;

  @GuiWidgetElement(
      id = "shutdownDrainMs",
      order = "990",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.ShutdownDrain.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.ShutdownDrain.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String shutdownDrainMs;

  @GuiWidgetElement(
      id = "metricsIntervalMs",
      order = "991",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.MetricsInterval.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.MetricsInterval.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String metricsIntervalMs;

  @GuiWidgetElement(
      id = "trustStorePath",
      order = "992",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::OpenLineageSinkConfiguration.TrustStorePath.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.TrustStorePath.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String trustStorePath;

  @GuiWidgetElement(
      id = "trustStorePassword",
      order = "993",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::OpenLineageSinkConfiguration.TrustStorePassword.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.TrustStorePassword.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty(password = true)
  private String trustStorePassword;

  @GuiWidgetElement(
      id = "keyStoreType",
      order = "994",
      parentId = LineageSink.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OpenLineageSinkConfiguration.KeyStoreType.Label",
      toolTip = "i18n::OpenLineageSinkConfiguration.KeyStoreType.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DELIVERY,
      groupOrder = GROUP_DELIVERY_ORDER)
  @HopMetadataProperty
  private String keyStoreType;

  /**
   * Adds the delivery options, under {@code prefix} ({@code HOP_LINEAGE_OPENLINEAGE_}, say),
   * resolved against {@code variables}.
   */
  protected void addDeliverySettings(
      Map<String, String> settings, String prefix, IVariables variables) {
    put(settings, prefix + "SPOOL_DIR", spoolDirectory, variables);
    put(settings, prefix + "SPOOL_MAX_BYTES", spoolMaxBytes, variables);
    put(settings, prefix + "BUFFER_SIZE", bufferSize, variables);
    if (overflowPolicy != null) {
      settings.put(prefix + "OVERFLOW_POLICY", overflowPolicy.getCode());
    }
    put(settings, prefix + "ENQUEUE_TIMEOUT_MS", enqueueTimeoutMs, variables);
    put(settings, prefix + "MAX_RETRIES", maxRetries, variables);
    put(settings, prefix + "RETRY_BACKOFF_MS", retryBackoffMs, variables);
    put(settings, prefix + "CONNECT_TIMEOUT_MS", connectTimeoutMs, variables);
    put(settings, prefix + "READ_TIMEOUT_MS", readTimeoutMs, variables);
    put(settings, prefix + "SHUTDOWN_DRAIN_MS", shutdownDrainMs, variables);
    put(settings, prefix + "METRICS_INTERVAL_MS", metricsIntervalMs, variables);
    put(settings, prefix + "TRUSTSTORE_PATH", trustStorePath, variables);
    put(settings, prefix + "TRUSTSTORE_PASSWORD", trustStorePassword, variables);
    put(settings, prefix + "KEYSTORE_TYPE", keyStoreType, variables);
  }

  /** Adds a setting, resolved; a value that is empty once resolved is left out. */
  protected static void put(
      Map<String, String> settings, String name, String value, IVariables variables) {
    String resolved = variables.resolve(value);
    if (StringUtils.isNotBlank(resolved)) {
      settings.put(name, resolved.trim());
    }
  }

  /** Adds a Y/N setting. */
  protected static void put(Map<String, String> settings, String name, boolean value) {
    settings.put(name, value ? "Y" : "N");
  }

  @Override
  public OpenLineageDeliveryConfiguration clone() {
    try {
      // Only strings, booleans and enums: a shallow copy is a full one.
      return (OpenLineageDeliveryConfiguration) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }
}
