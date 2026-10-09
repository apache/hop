/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.beam.transforms.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamElasticsearchOutputTransform;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "BeamElasticsearchOutput",
    name = "i18n::BeamElasticsearchOutput.Name",
    description = "i18n::BeamElasticsearchOutput.Description",
    image = "beam-elasticsearch-output.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamElasticsearch.keyword",
    documentationUrl = "/pipeline/transforms/beamelasticsearchoutput.html",
    supportedEngines = {"Beam*"})
@GuiPlugin
@Getter
@Setter
public class BeamElasticsearchOutputMeta
    extends BaseTransformMeta<BeamElasticsearchOutput, BeamElasticsearchOutputData>
    implements IBeamPipelineTransformHandler {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "BeamElasticsearchOutput";
  public static final String WIDGET_MAX_BATCH_SIZE = "maxBatchSize";

  @GuiWidgetElement(
      id = WIDGET_MAX_BATCH_SIZE,
      order = "0900",
      type = GuiElementType.TEXT,
      label = "i18n::BeamElasticsearch.maxBatchSize.Label",
      toolTip = "i18n::BeamElasticsearch.maxBatchSize.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamElasticsearch.Documents.Group",
      groupOrder = "0200")
  @HopMetadataProperty(key = "max_batch_size")
  private String maxBatchSize = "1000";

  public static final String WIDGET_MAX_BATCH_BYTES = "maxBatchBytes";

  @GuiWidgetElement(
      id = WIDGET_MAX_BATCH_BYTES,
      order = "1000",
      type = GuiElementType.TEXT,
      label = "i18n::BeamElasticsearch.maxBatchBytes.Label",
      toolTip = "i18n::BeamElasticsearch.maxBatchBytes.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamElasticsearch.Documents.Group",
      groupOrder = "0200")
  @HopMetadataProperty(key = "max_batch_bytes")
  private String maxBatchBytes = "5242880";

  public static final String WIDGET_HOSTS = "hosts";
  public static final String WIDGET_INDEX = "index";
  public static final String WIDGET_DOCUMENT_TYPE = "documentType";
  public static final String WIDGET_USERNAME = "username";
  public static final String WIDGET_PASSWORD = "password";
  public static final String WIDGET_JSON_FIELD = "jsonField";

  @GuiWidgetElement(
      id = WIDGET_HOSTS,
      order = "0000",
      type = GuiElementType.TEXT,
      label = "i18n::BeamElasticsearch.hosts.Label",
      toolTip = "i18n::BeamElasticsearch.hosts.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamElasticsearch.Connection.Group",
      groupOrder = "0100")
  @HopMetadataProperty
  private String hosts;

  @GuiWidgetElement(
      id = WIDGET_INDEX,
      order = "0001",
      type = GuiElementType.TEXT,
      label = "i18n::BeamElasticsearch.index.Label",
      toolTip = "i18n::BeamElasticsearch.index.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamElasticsearch.Connection.Group",
      groupOrder = "0100")
  @HopMetadataProperty
  private String index;

  @GuiWidgetElement(
      id = WIDGET_DOCUMENT_TYPE,
      order = "0002",
      type = GuiElementType.TEXT,
      label = "i18n::BeamElasticsearch.documentType.Label",
      toolTip = "i18n::BeamElasticsearch.documentType.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamElasticsearch.Connection.Group",
      groupOrder = "0100")
  @HopMetadataProperty(key = "document_type")
  private String documentType;

  @GuiWidgetElement(
      id = WIDGET_USERNAME,
      order = "0003",
      type = GuiElementType.TEXT,
      label = "i18n::BeamElasticsearch.username.Label",
      toolTip = "i18n::BeamElasticsearch.username.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamElasticsearch.Connection.Group",
      groupOrder = "0100")
  @HopMetadataProperty
  private String username;

  @GuiWidgetElement(
      id = WIDGET_PASSWORD,
      order = "0004",
      type = GuiElementType.TEXT,
      label = "i18n::BeamElasticsearch.password.Label",
      toolTip = "i18n::BeamElasticsearch.password.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamElasticsearch.Connection.Group",
      groupOrder = "0100",
      password = true)
  @HopMetadataProperty(password = true)
  private String password;

  @GuiWidgetElement(
      id = WIDGET_JSON_FIELD,
      order = "0005",
      type = GuiElementType.TEXT,
      label = "i18n::BeamElasticsearch.jsonField.Label",
      toolTip = "i18n::BeamElasticsearch.jsonField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamElasticsearch.Documents.Group",
      groupOrder = "0200")
  @HopMetadataProperty(key = "json_field")
  private String jsonField = "json";

  public ElasticsearchIO.ConnectionConfiguration connectionConfiguration(IVariables variables)
      throws HopException {
    return BeamElasticsearchConfig.connection(
        variables, hosts, index, documentType, username, password);
  }

  public ElasticsearchIO.Write createWrite(IVariables variables) throws HopException {
    return ElasticsearchIO.write()
        .withConnectionConfiguration(connectionConfiguration(variables))
        .withMaxBatchSize(
            BeamElasticsearchConfig.positiveLong(variables, maxBatchSize, "Maximum batch size"))
        .withMaxBatchSizeBytes(
            BeamElasticsearchConfig.positiveLong(variables, maxBatchBytes, "Maximum batch bytes"));
  }

  @Override
  public String getDialogClassName() {
    return "org.apache.hop.beam.transforms.elasticsearch.BeamElasticsearchOutputDialog";
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
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta next,
      IVariables variables,
      IHopMetadataProvider provider) {
    rowMeta.clear();
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
    if (input == null) {
      throw new HopException("Elasticsearch output requires incoming rows");
    }
    String resolvedField = BeamElasticsearchConfig.required(variables, jsonField, "JSON field");
    int fieldIndex = rowMeta.indexOfValue(resolvedField);
    if (fieldIndex < 0 || !rowMeta.getValueMeta(fieldIndex).isString()) {
      throw new HopException(
          "JSON field must exist in incoming rows and have type String: " + resolvedField);
    }
    ElasticsearchIO.Write writeIo = createWrite(variables);
    String rowMetaXml;
    try {
      rowMetaXml = rowMeta.getMetaXml();
    } catch (IOException e) {
      throw new HopException("Unable to serialize Elasticsearch input fields", e);
    }
    input.apply(
        transformMeta.getName(),
        new BeamElasticsearchOutputTransform(
            transformMeta.getName(), writeIo, resolvedField, rowMetaXml));
  }
}
