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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamMqttConnection;
import org.apache.hop.beam.core.transform.BeamMqttOutputTransform;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
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
    id = "BeamMqttOutput",
    name = "i18n::BeamMqttOutputDialog.Title",
    description = "i18n::BeamMqttOutputMeta.Description",
    image = "beam-mqtt-output.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamMqttMeta.keyword",
    documentationUrl = "/pipeline/transforms/beammqttoutput.html",
    supportedEngines = {"Beam*"})
public class BeamMqttOutputMeta extends BaseTransformMeta<BeamMqttOutput, BeamMqttOutputData>
    implements IBeamPipelineTransformHandler {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "BEAMMQTTOUTPUT_OPTIONS";

  public static final String WIDGET_SERVER_URI = "serverUri";
  public static final String WIDGET_TOPIC = "topic";
  public static final String WIDGET_CLIENT_ID = "clientId";
  public static final String WIDGET_USERNAME = "username";
  public static final String WIDGET_PASSWORD = "password";
  public static final String WIDGET_PAYLOAD_FIELD = "payloadField";
  public static final String WIDGET_PAYLOAD_TYPE = "payloadType";
  public static final String WIDGET_RETAINED = "retained";

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

  @HopMetadataProperty(key = "retained")
  @GuiWidgetElement(
      id = WIDGET_RETAINED,
      order = "0800",
      type = GuiElementType.CHECKBOX,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamMqttMeta.retained.Label",
      toolTip = "i18n::BeamMqttMeta.retained.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamMqttMeta.Payload",
      groupOrder = "0200")
  private boolean retained = false;

  public BeamMqttOutputTransform buildOutputTransform(
      IVariables variables, String name, IRowMeta rowMeta) throws HopException {
    var connection =
        BeamMqttConnection.resolve(
            variables, serverUri, topic, clientId, username, password, false);
    String field = BeamMqttConnection.required(variables, payloadField, "payload field");
    String type = BeamMqttConnection.payloadType(variables, payloadType);
    int index = rowMeta.indexOfValue(field);
    if (index < 0) throw new HopException("MQTT payload field not found: " + field);
    if ("Binary".equalsIgnoreCase(type)
        && rowMeta.getValueMeta(index).getType() != IValueMeta.TYPE_BINARY)
      throw new HopException("MQTT binary payload requires a Binary field: " + field);
    String rowMetaXml;
    try {
      rowMetaXml = rowMeta.getMetaXml();
    } catch (IOException e) {
      throw new HopException("Unable to serialize MQTT row metadata", e);
    }
    return new BeamMqttOutputTransform(name, connection, field, type, rowMetaXml, retained);
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
    if (input == null || previousTransforms.size() != 1)
      throw new HopException("Beam MQTT output requires exactly one incoming transform");
    input.apply(
        transformMeta.getName(), buildOutputTransform(variables, transformMeta.getName(), rowMeta));
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
      if (input == null || input.length != 1 || prev == null)
        throw new HopException("Beam MQTT output requires exactly one incoming transform");
      buildOutputTransform(variables, transformMeta.getName(), prev);
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(BeamMqttOutputMeta.class, "BeamMqttMeta.ConfigurationValid"),
              transformMeta));
    } catch (HopException e) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
    }
  }

  @Override
  public String getDialogClassName() {
    return BeamMqttOutputDialog.class.getName();
  }

  public List<String> getPayloadTypes(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return List.of("String", "Binary");
  }

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
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
    row.clear();
  }
}
