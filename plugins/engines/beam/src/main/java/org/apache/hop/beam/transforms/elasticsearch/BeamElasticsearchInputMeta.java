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

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamElasticsearchInputTransform;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.BeamSourceTopology;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "BeamElasticsearchInput",
    name = "i18n::BeamElasticsearchInput.Name",
    description = "i18n::BeamElasticsearchInput.Description",
    image = "beam-elasticsearch-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamElasticsearch.keyword",
    documentationUrl = "/pipeline/transforms/beamelasticsearchinput.html",
    supportedEngines = {"Beam*"})
@GuiPlugin
@Getter
@Setter
public class BeamElasticsearchInputMeta
    extends BaseTransformMeta<BeamElasticsearchInput, BeamElasticsearchInputData>
    implements IBeamPipelineTransformHandler {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "BeamElasticsearchInput";
  public static final String WIDGET_SCROLL_KEEPALIVE = "scrollKeepalive";

  @GuiWidgetElement(
      id = WIDGET_SCROLL_KEEPALIVE,
      order = "0900",
      type = GuiElementType.TEXT,
      label = "i18n::BeamElasticsearch.scrollKeepalive.Label",
      toolTip = "i18n::BeamElasticsearch.scrollKeepalive.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamElasticsearch.Documents.Group",
      groupOrder = "0200")
  @HopMetadataProperty(key = "scroll_keepalive")
  private String scrollKeepalive = "5m";

  public static final String WIDGET_HOSTS = "hosts";
  public static final String WIDGET_INDEX = "index";
  public static final String WIDGET_DOCUMENT_TYPE = "documentType";
  public static final String WIDGET_USERNAME = "username";
  public static final String WIDGET_PASSWORD = "password";
  public static final String WIDGET_JSON_FIELD = "jsonField";
  public static final String WIDGET_QUERY = "query";

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
      id = WIDGET_QUERY,
      order = "0006",
      type = GuiElementType.MULTI_LINE_TEXT,
      label = "i18n::BeamElasticsearch.query.Label",
      toolTip = "i18n::BeamElasticsearch.query.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamElasticsearch.Documents.Group",
      groupOrder = "0200",
      multiLineTextHeight = 5)
  @HopMetadataProperty
  private String query;

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

  @Override
  public boolean consumesMainInput() {
    return false;
  }

  @Override
  public boolean canStartWithoutInput() {
    return true;
  }

  public ElasticsearchIO.ConnectionConfiguration connectionConfiguration(IVariables variables)
      throws HopException {
    return BeamElasticsearchConfig.connection(
        variables, hosts, index, documentType, username, password);
  }

  public ElasticsearchIO.Read createRead(IVariables variables) throws HopException {
    String resolvedQuery = BeamElasticsearchConfig.optional(variables, query, "Query");
    if (!resolvedQuery.isBlank()) {
      BeamElasticsearchConfig.jsonObject(resolvedQuery, "Query");
    }
    String keepalive =
        BeamElasticsearchConfig.required(variables, scrollKeepalive, "Scroll keepalive");
    if (!keepalive.matches("[1-9][0-9]*(ms|s|m|h|d)")) {
      throw new HopException("Scroll keepalive must be a positive duration (ms, s, m, h or d)");
    }
    ElasticsearchIO.Read read =
        ElasticsearchIO.read()
            .withConnectionConfiguration(connectionConfiguration(variables))
            .withScrollKeepalive(keepalive);
    return resolvedQuery.isBlank() ? read : read.withQuery(resolvedQuery);
  }

  @Override
  public String getDialogClassName() {
    return "org.apache.hop.beam.transforms.elasticsearch.BeamElasticsearchInputDialog";
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
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    String resolvedField;
    try {
      resolvedField = BeamElasticsearchConfig.required(variables, jsonField, "JSON field");
    } catch (HopException e) {
      throw new HopTransformException(e.getMessage(), e);
    }
    rowMeta.clear();
    ValueMetaString field = new ValueMetaString(resolvedField);
    field.setOrigin(name);
    rowMeta.addValueMeta(field);
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
        "Elasticsearch input is a source and does not accept incoming rows");
    ElasticsearchIO.Read readIo = createRead(variables);
    RowMeta outputMeta = new RowMeta();
    getFields(outputMeta, transformMeta.getName(), null, null, variables, metadataProvider);
    BeamElasticsearchInputTransform read =
        new BeamElasticsearchInputTransform(
            transformMeta.getName(), readIo, JsonRowMeta.toJson(outputMeta));
    transformCollectionMap.put(
        transformMeta.getName(), pipeline.apply(transformMeta.getName(), read));
  }
}
