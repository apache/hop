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

package org.apache.hop.beam.transforms.mqtt;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamMqttConnection;
import org.apache.hop.beam.core.transform.BeamMqttInputTransform;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.BeamSourceTopology;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@GuiPlugin
@Transform(
    id = "BeamMqttInput",
    name = "i18n::BeamMqttInputDialog.Title",
    description = "i18n::BeamMqttInputMeta.Description",
    image = "beam-mqtt-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamMqttMeta.keyword",
    documentationUrl = "/pipeline/transforms/beammqttinput.html",
    supportedEngines = {"Beam*"})
public class BeamMqttInputMeta extends BaseTransformMeta<BeamMqttInput, BeamMqttInputData>
    implements IBeamPipelineTransformHandler {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "BEAMMQTTINPUT_OPTIONS";

  public static final String WIDGET_SERVER_URI = "serverUri";
  public static final String WIDGET_TOPIC = "topic";
  public static final String WIDGET_CLIENT_ID = "clientId";
  public static final String WIDGET_USERNAME = "username";
  public static final String WIDGET_PASSWORD = "password";
  public static final String WIDGET_PAYLOAD_FIELD = "payloadField";
  public static final String WIDGET_PAYLOAD_TYPE = "payloadType";
  public static final String WIDGET_MAX_NUM_RECORDS = "maxNumRecords";
  public static final String WIDGET_MAX_READ_TIME = "maxReadTime";

  @HopMetadataProperty(key = "server_uri")
  @GuiWidgetElement(
      id = WIDGET_SERVER_URI,
      order = "0100",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamMqttMeta.serverUri.Label",
      toolTip = "i18n::BeamMqttMeta.serverUri.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamMqttMeta.Connection",
      groupOrder = "0100")
  private String serverUri;

  @HopMetadataProperty(key = "topic")
  @GuiWidgetElement(
      id = WIDGET_TOPIC,
      order = "0200",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamMqttMeta.topic.Label",
      toolTip = "i18n::BeamMqttMeta.topic.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamMqttMeta.Connection",
      groupOrder = "0100")
  private String topic;

  @HopMetadataProperty(key = "client_id")
  @GuiWidgetElement(
      id = WIDGET_CLIENT_ID,
      order = "0300",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamMqttMeta.clientId.Label",
      toolTip = "i18n::BeamMqttMeta.clientId.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamMqttMeta.Connection",
      groupOrder = "0100")
  private String clientId;

  @HopMetadataProperty(key = "username")
  @GuiWidgetElement(
      id = WIDGET_USERNAME,
      order = "0400",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamMqttMeta.username.Label",
      toolTip = "i18n::BeamMqttMeta.username.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamMqttMeta.Connection",
      groupOrder = "0100")
  private String username;

  @HopMetadataProperty(key = "password", password = true)
  @GuiWidgetElement(
      id = WIDGET_PASSWORD,
      order = "0500",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamMqttMeta.password.Label",
      toolTip = "i18n::BeamMqttMeta.password.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamMqttMeta.Connection",
      groupOrder = "0100",
      password = true)
  private String password;

  @HopMetadataProperty(key = "payload_field")
  @GuiWidgetElement(
      id = WIDGET_PAYLOAD_FIELD,
      order = "0600",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamMqttMeta.payloadField.Label",
      toolTip = "i18n::BeamMqttMeta.payloadField.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamMqttMeta.Payload",
      groupOrder = "0200")
  private String payloadField = "message";

  @HopMetadataProperty(key = "payload_type")
  @GuiWidgetElement(
      id = WIDGET_PAYLOAD_TYPE,
      order = "0700",
      type = GuiElementType.COMBO,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamMqttMeta.payloadType.Label",
      toolTip = "i18n::BeamMqttMeta.payloadType.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamMqttMeta.Payload",
      groupOrder = "0200",
      comboValuesMethod = "getPayloadTypes")
  private String payloadType = "String";

  @HopMetadataProperty(key = "max_num_records")
  @GuiWidgetElement(
      id = WIDGET_MAX_NUM_RECORDS,
      order = "0800",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamMqttMeta.maxNumRecords.Label",
      toolTip = "i18n::BeamMqttMeta.maxNumRecords.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamMqttMeta.Limits",
      groupOrder = "0300")
  private String maxNumRecords = "0";

  @HopMetadataProperty(key = "max_read_time")
  @GuiWidgetElement(
      id = WIDGET_MAX_READ_TIME,
      order = "0900",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamMqttMeta.maxReadTime.Label",
      toolTip = "i18n::BeamMqttMeta.maxReadTime.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamMqttMeta.Limits",
      groupOrder = "0300")
  private String maxReadTime = "0";

  public BeamMqttInputTransform buildInputTransform(IVariables variables, String name)
      throws HopException {
    var connection =
        BeamMqttConnection.resolve(variables, serverUri, topic, clientId, username, password, true);
    BeamMqttConnection.required(variables, payloadField, "payload field");
    String type = BeamMqttConnection.payloadType(variables, payloadType);
    long records = BeamMqttConnection.limit(variables, maxNumRecords, "maximum records");
    long seconds = BeamMqttConnection.limit(variables, maxReadTime, "maximum read time");
    if (seconds > Long.MAX_VALUE / 1000)
      throw new HopException("MQTT maximum read time is too large");
    return new BeamMqttInputTransform(name, connection, type, records, seconds);
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
        "Beam MQTT input does not accept incoming rows");
    transformCollectionMap.put(
        transformMeta.getName(),
        pipeline.apply(
            transformMeta.getName(), buildInputTransform(variables, transformMeta.getName())));
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
      if (input != null && input.length > 0)
        throw new HopException("Beam MQTT input does not accept incoming rows");
      buildInputTransform(variables, transformMeta.getName());
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(BeamMqttInputMeta.class, "BeamMqttMeta.ConfigurationValid"),
              transformMeta));
    } catch (HopException e) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
    }
  }

  @Override
  public String getDialogClassName() {
    return BeamMqttInputDialog.class.getName();
  }

  public List<String> getPayloadTypes(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return List.of("String", "Binary");
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
  public boolean consumesMainInput() {
    return false;
  }

  @Override
  public boolean canStartWithoutInput() {
    return true;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta next,
      IVariables variables,
      IHopMetadataProvider provider)
      throws HopTransformException {
    try {
      String field = BeamMqttConnection.required(variables, payloadField, "payload field");
      String type = BeamMqttConnection.payloadType(variables, payloadType);
      IValueMeta value =
          "Binary".equalsIgnoreCase(type) ? new ValueMetaBinary(field) : new ValueMetaString(field);
      value.setOrigin(name);
      row.clear();
      row.addValueMeta(value);
    } catch (HopException e) {
      throw new HopTransformException(e.getMessage(), e);
    }
  }
}
