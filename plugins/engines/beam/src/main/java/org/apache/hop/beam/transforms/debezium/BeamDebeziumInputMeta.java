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

package org.apache.hop.beam.transforms.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.io.debezium.DebeziumIO;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamDebeziumInputTransform;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metadata.RunnerType;
import org.apache.hop.beam.pipeline.BeamSourceTopology;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.kafka.connect.source.SourceConnector;

@Getter
@Setter
@GuiPlugin
@Transform(
    id = "BeamDebeziumInput",
    name = "i18n::BeamDebeziumInput.Name",
    description = "i18n::BeamDebeziumInput.Description",
    image = "beam-debezium-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamDebeziumInput.Keywords",
    documentationUrl = "/pipeline/transforms/beamdebeziuminput.html",
    supportedEngines = {
      "BeamDirectPipelineEngine",
      "BeamFlinkPipelineEngine",
      "BeamDataFlowPipelineEngine"
    })
public class BeamDebeziumInputMeta
    extends BaseTransformMeta<BeamDebeziumInput, BeamDebeziumInputData>
    implements IBeamPipelineTransformHandler {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "BEAM_DEBEZIUM_INPUT_OPTIONS";

  @HopMetadataProperty(key = "connector")
  @GuiWidgetElement(
      id = "connector",
      order = "0100",
      type = GuiElementType.COMBO,
      comboValuesMethod = "getConnectorNames",
      label = "i18n::BeamDebeziumInput.Connector.Label",
      toolTip = "i18n::BeamDebeziumInput.Connector.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Connection.Group",
      groupOrder = "0100")
  private String connector = "PostgreSQL";

  @HopMetadataProperty(key = "connector_class")
  @GuiWidgetElement(
      id = "connectorClass",
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::BeamDebeziumInput.ConnectorClass.Label",
      toolTip = "i18n::BeamDebeziumInput.ConnectorClass.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Connection.Group",
      groupOrder = "0100")
  private String connectorClass = "";

  @HopMetadataProperty(key = "hostname")
  @GuiWidgetElement(
      id = "hostname",
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::BeamDebeziumInput.Hostname.Label",
      toolTip = "i18n::BeamDebeziumInput.Hostname.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Connection.Group",
      groupOrder = "0100")
  private String hostname = "localhost";

  @HopMetadataProperty(key = "port")
  @GuiWidgetElement(
      id = "port",
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::BeamDebeziumInput.Port.Label",
      toolTip = "i18n::BeamDebeziumInput.Port.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Connection.Group",
      groupOrder = "0100")
  private String port = "5432";

  @HopMetadataProperty(key = "username")
  @GuiWidgetElement(
      id = "username",
      order = "0500",
      type = GuiElementType.TEXT,
      label = "i18n::BeamDebeziumInput.Username.Label",
      toolTip = "i18n::BeamDebeziumInput.Username.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Connection.Group",
      groupOrder = "0100")
  private String username = "";

  @HopMetadataProperty(key = "password", password = true)
  @GuiWidgetElement(
      id = "password",
      order = "0600",
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::BeamDebeziumInput.Password.Label",
      toolTip = "i18n::BeamDebeziumInput.Password.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Connection.Group",
      groupOrder = "0100")
  private String password = "";

  @HopMetadataProperty(key = "connector_properties")
  @GuiWidgetElement(
      id = "connectorProperties",
      order = "0100",
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 10,
      label = "i18n::BeamDebeziumInput.Properties.Label",
      toolTip = "i18n::BeamDebeziumInput.Properties.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Properties.Group",
      groupOrder = "0200")
  private String connectorProperties =
      "{\"database.dbname\":\"inventory\",\"plugin.name\":\"pgoutput\",\"topic.prefix\":\"hop-cdc\"}";

  @HopMetadataProperty(key = "max_records")
  @GuiWidgetElement(
      id = "maxRecords",
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::BeamDebeziumInput.MaxRecords.Label",
      toolTip = "i18n::BeamDebeziumInput.MaxRecords.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Output.Group",
      groupOrder = "0300")
  private String maxRecords = "";

  @HopMetadataProperty(key = "max_time_ms")
  @GuiWidgetElement(
      id = "maxTimeMs",
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::BeamDebeziumInput.MaxTime.Label",
      toolTip = "i18n::BeamDebeziumInput.MaxTime.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Output.Group",
      groupOrder = "0300")
  private String maxTimeMs = "";

  @HopMetadataProperty(key = "polling_timeout_ms")
  @GuiWidgetElement(
      id = "pollingTimeoutMs",
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::BeamDebeziumInput.PollingTimeout.Label",
      toolTip = "i18n::BeamDebeziumInput.PollingTimeout.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Output.Group",
      groupOrder = "0300")
  private String pollingTimeoutMs = "1000";

  @HopMetadataProperty(key = "json_field")
  @GuiWidgetElement(
      id = "jsonField",
      order = "0100",
      type = GuiElementType.TEXT,
      label = "i18n::BeamDebeziumInput.JsonField.Label",
      toolTip = "i18n::BeamDebeziumInput.JsonField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamDebeziumInput.Output.Group",
      groupOrder = "0300")
  private String jsonField = "event";

  @Override
  public void setDefault() {
    jsonField = "event";
  }

  @Override
  public String getDialogClassName() {
    return BeamDebeziumInputDialog.class.getName();
  }

  public List<String> getConnectorNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return List.of("PostgreSQL", "Custom");
  }

  public DebeziumIO.ConnectorConfiguration buildConnectorConfiguration(IVariables variables)
      throws HopException {
    require(variables, hostname, "Hostname");
    require(variables, username, "Username");
    require(variables, jsonField, "JSON field");
    long resolvedPort = positive(variables, port, "Port", false, 65535);
    positive(variables, maxRecords, "Maximum records", true, Integer.MAX_VALUE);
    positive(variables, maxTimeMs, "Maximum time", true, Long.MAX_VALUE);
    positive(variables, pollingTimeoutMs, "Polling timeout", true, Long.MAX_VALUE);
    Map<String, String> properties = resolveProperties(variables);
    String selected = require(variables, connector, "Connector");
    if (!"PostgreSQL".equals(selected) && !"Custom".equals(selected)) {
      throw new HopException("Select PostgreSQL or Custom as the Debezium connector");
    }
    if ("PostgreSQL".equals(selected) && !properties.containsKey("database.dbname")) {
      throw new HopException("PostgreSQL connector properties require database.dbname");
    }
    String className =
        "PostgreSQL".equals(selected)
            ? "io.debezium.connector.postgresql.PostgresConnector"
            : require(variables, connectorClass, "Custom connector class");
    try {
      Class<?> type =
          Class.forName(className, true, Thread.currentThread().getContextClassLoader());
      type.asSubclass(SourceConnector.class);
      return DebeziumIO.ConnectorConfiguration.create()
          .withConnectorClass(type)
          .withHostName(variables.resolve(hostname))
          .withPort(Long.toString(resolvedPort))
          .withUsername(variables.resolve(username))
          .withPassword(
              Encr.decryptPasswordOptionallyEncrypted(
                  variables.resolve(password == null ? "" : password)))
          .withConnectionProperties(properties);
    } catch (ReflectiveOperationException | LinkageError | ClassCastException e) {
      throw new HopException(
          "Unable to load Debezium connector "
              + className
              + "; add the compatible connector and driver jars to Hop and the Beam worker classpath",
          e);
    }
  }

  private Map<String, String> resolveProperties(IVariables variables) throws HopException {
    JsonNode properties;
    try {
      properties =
          new ObjectMapper()
              .readTree(
                  variables.resolve(connectorProperties == null ? "{}" : connectorProperties));
    } catch (Exception e) {
      // Do not put the JSON or parser's source excerpt (which may contain secrets) into the log.
      throw new HopException("Connector properties must be a JSON object with string values");
    }
    if (properties == null || !properties.isObject()) {
      throw new HopException("Connector properties must be a JSON object with string values");
    }
    Map<String, String> config = new LinkedHashMap<>();
    var entries = properties.fields();
    while (entries.hasNext()) {
      var entry = entries.next();
      if (entry.getKey().isBlank() || !entry.getValue().isTextual()) {
        throw new HopException("Connector properties must have non-empty keys and string values");
      }
      if (Set.of(
              "connector.class",
              "database.hostname",
              "database.port",
              "database.user",
              "database.password")
          .contains(entry.getKey())) {
        throw new HopException(
            "Use the Connection tab instead of connector property " + entry.getKey());
      }
      config.put(entry.getKey(), entry.getValue().textValue());
    }
    return config;
  }

  private static String require(IVariables variables, String value, String label)
      throws HopException {
    String resolved = variables.resolve(value);
    if (resolved == null || resolved.isBlank()) {
      throw new HopException(label + " is required");
    }
    return resolved;
  }

  private static long positive(
      IVariables variables, String value, String label, boolean optional, long maximum)
      throws HopException {
    String resolved = variables.resolve(value);
    if (optional && (resolved == null || resolved.isBlank())) {
      return 0;
    }
    try {
      long number = Long.parseLong(resolved);
      if (number > 0 && number <= maximum) {
        return number;
      }
    } catch (NumberFormatException e) {
      // Report the setting name, not the potentially secret resolved value.
    }
    throw new HopException(label + " must be a positive integer no greater than " + maximum);
  }

  @Override
  public boolean consumesMainInput() {
    return false;
  }

  @Override
  public boolean canStartWithoutInput() {
    return true;
  }

  @Override
  public boolean isInput() {
    return true;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta next,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    row.clear();
    ValueMetaString value = new ValueMetaString(variables.resolve(jsonField));
    value.setOrigin(name);
    row.addValueMeta(value);
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    try {
      buildConnectorConfiguration(variables);
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(BeamDebeziumInputMeta.class, "BeamDebeziumInput.Check.Valid"),
              transformMeta));
    } catch (HopException e) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
    }
    if (input != null && input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  BeamDebeziumInputMeta.class, "BeamDebeziumInput.Check.NoIncoming"),
              transformMeta));
    }
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      String dataSamplersJson,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      org.apache.beam.sdk.Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input,
      String parentLogChannelId)
      throws HopException {
    BeamSourceTopology.rejectIncomingRows(
        pipelineMeta,
        transformMeta,
        previousTransforms,
        input,
        "Beam Debezium input is a source and does not accept incoming rows");
    Class<?> runner = pipeline.getOptions().getRunner();
    if ((runConfiguration != null && runConfiguration.getRunnerType() == RunnerType.Spark)
        || (runner != null && runner.getName().startsWith("org.apache.beam.runners.spark."))) {
      throw new HopException(
          "Beam Spark does not support the unbounded Splittable DoFn used by DebeziumIO; use Direct, Flink or Dataflow");
    }
    pipeline.getOptions().as(StreamingOptions.class).setStreaming(true);
    DebeziumIO.Read<String> read =
        DebeziumIO.readAsJson().withConnectorConfiguration(buildConnectorConfiguration(variables));
    String records = variables.resolve(maxRecords);
    if (records != null && !records.isBlank()) {
      read = read.withMaxNumberOfRecords(Integer.valueOf(records));
    }
    String time = variables.resolve(maxTimeMs);
    if (time != null && !time.isBlank()) {
      read = read.withMaxTimeToRun(Long.valueOf(time));
    }
    String polling = variables.resolve(pollingTimeoutMs);
    if (polling != null && !polling.isBlank()) {
      read = read.withPollingTimeout(Long.valueOf(polling));
    }
    PCollection<HopRow> output =
        pipeline.apply(new BeamDebeziumInputTransform(transformMeta.getName(), read));
    transformCollectionMap.put(transformMeta.getName(), output);
  }
}
