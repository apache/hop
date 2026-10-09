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

package org.apache.hop.beam.transforms.snowflake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.core.fn.SnowflakeCsvToHop;
import org.apache.hop.beam.core.fn.SnowflakeValues;
import org.apache.hop.beam.core.transform.BeamSnowflakeInputTransform;
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
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
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
    id = "BeamSnowflakeInput",
    name = "i18n::BeamSnowflakeInputDialog.Title",
    description = "i18n::BeamSnowflakeInputMeta.Description",
    image = "beam-snowflake-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamSnowflakeInput.keyword",
    documentationUrl = "/pipeline/transforms/beamsnowflakeinput.html",
    supportedEngines = {"Beam*"})
public class BeamSnowflakeInputMeta
    extends BaseTransformMeta<BeamSnowflakeInput, BeamSnowflakeInputData>
    implements IBeamPipelineTransformHandler {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "BEAMSNOWFLAKEINPUT_OPTIONS";
  private static final String CONNECTION = "i18n::BeamSnowflake.Connection.Group";
  private static final String STAGING = "i18n::BeamSnowflake.Staging.Group";
  private static final String READ = "i18n::BeamSnowflakeInput.Read.Group";

  @HopMetadataProperty(key = "server_name")
  @GuiWidgetElement(
      id = "serverName",
      order = "0100",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.serverName.Label",
      toolTip = "i18n::BeamSnowflake.serverName.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = CONNECTION,
      groupOrder = "0100")
  private String serverName;

  @HopMetadataProperty(key = "username")
  @GuiWidgetElement(
      id = "username",
      order = "0200",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.username.Label",
      toolTip = "i18n::BeamSnowflake.username.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = CONNECTION,
      groupOrder = "0100")
  private String username;

  @HopMetadataProperty(key = "password", password = true)
  @GuiWidgetElement(
      id = "password",
      order = "0300",
      type = GuiElementType.TEXT,
      password = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.password.Label",
      toolTip = "i18n::BeamSnowflake.password.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = CONNECTION,
      groupOrder = "0100")
  private String password;

  @HopMetadataProperty(key = "private_key", password = true)
  @GuiWidgetElement(
      id = "privateKey",
      order = "0400",
      type = GuiElementType.TEXT,
      password = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.privateKey.Label",
      toolTip = "i18n::BeamSnowflake.privateKey.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = CONNECTION,
      groupOrder = "0100")
  private String privateKey;

  @HopMetadataProperty(key = "private_key_passphrase", password = true)
  @GuiWidgetElement(
      id = "privateKeyPassphrase",
      order = "0500",
      type = GuiElementType.TEXT,
      password = true,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.privateKeyPassphrase.Label",
      toolTip = "i18n::BeamSnowflake.privateKeyPassphrase.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = CONNECTION,
      groupOrder = "0100")
  private String privateKeyPassphrase;

  @HopMetadataProperty(key = "database")
  @GuiWidgetElement(
      id = "database",
      order = "0600",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.database.Label",
      toolTip = "i18n::BeamSnowflake.database.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = CONNECTION,
      groupOrder = "0100")
  private String database;

  @HopMetadataProperty(key = "warehouse")
  @GuiWidgetElement(
      id = "warehouse",
      order = "0700",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.warehouse.Label",
      toolTip = "i18n::BeamSnowflake.warehouse.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = CONNECTION,
      groupOrder = "0100")
  private String warehouse;

  @HopMetadataProperty(key = "schema")
  @GuiWidgetElement(
      id = "schema",
      order = "0800",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.schema.Label",
      toolTip = "i18n::BeamSnowflake.schema.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = CONNECTION,
      groupOrder = "0100")
  private String schema;

  @HopMetadataProperty(key = "role")
  @GuiWidgetElement(
      id = "role",
      order = "0900",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.role.Label",
      toolTip = "i18n::BeamSnowflake.role.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = CONNECTION,
      groupOrder = "0100")
  private String role;

  @HopMetadataProperty(key = "port")
  @GuiWidgetElement(
      id = "port",
      order = "0950",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.port.Label",
      toolTip = "i18n::BeamSnowflake.port.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = CONNECTION,
      groupOrder = "0100")
  private String port;

  @HopMetadataProperty(key = "staging_bucket")
  @GuiWidgetElement(
      id = "stagingBucket",
      order = "1000",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.stagingBucket.Label",
      toolTip = "i18n::BeamSnowflake.stagingBucket.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = STAGING,
      groupOrder = "0200")
  private String stagingBucket;

  @HopMetadataProperty(key = "storage_integration")
  @GuiWidgetElement(
      id = "storageIntegration",
      order = "1100",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.storageIntegration.Label",
      toolTip = "i18n::BeamSnowflake.storageIntegration.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = STAGING,
      groupOrder = "0200")
  private String storageIntegration;

  @HopMetadataProperty(key = "quotation_mark")
  @GuiWidgetElement(
      id = "quotationMark",
      order = "1200",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflake.quotationMark.Label",
      toolTip = "i18n::BeamSnowflake.quotationMark.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = STAGING,
      groupOrder = "0200")
  private String quotationMark;

  @HopMetadataProperty(key = "table_name")
  @GuiWidgetElement(
      id = "tableName",
      order = "1300",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflakeInput.tableName.Label",
      toolTip = "i18n::BeamSnowflakeInput.tableName.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = READ,
      groupOrder = "0300")
  private String tableName;

  @HopMetadataProperty(key = "query")
  @GuiWidgetElement(
      id = "query",
      order = "1400",
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 4,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflakeInput.query.Label",
      toolTip = "i18n::BeamSnowflakeInput.query.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = READ,
      groupOrder = "0300")
  private String query;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<SnowflakeField> fields = new ArrayList<>();

  public SnowflakeSpec spec(IVariables variables) throws HopException {
    return SnowflakeSpec.build(
        variables,
        serverName,
        username,
        password,
        privateKey,
        privateKeyPassphrase,
        database,
        warehouse,
        schema,
        role,
        port,
        stagingBucket,
        storageIntegration,
        quotationMark,
        tableName,
        query,
        true);
  }

  public RowMeta outputRowMeta(IVariables variables, String origin) throws HopException {
    RowMeta rowMeta = new RowMeta();
    if (fields == null || fields.isEmpty())
      throw new HopException("Snowflake input requires at least one field");
    for (SnowflakeField field : fields) {
      if (field == null || StringUtils.isBlank(field.getName()))
        throw new HopException("Snowflake field name must be an identifier");
      String name = SnowflakeSpec.columnName(variables.resolve(field.getName()).trim());
      if (rowMeta.indexOfValue(name) >= 0)
        throw new HopException("Snowflake field name is duplicated");
      int type = SnowflakeValues.hopType(field.getType());
      IValueMeta value = ValueMetaFactory.createValueMeta(name, type);
      value.setOrigin(origin);
      rowMeta.addValueMeta(value);
    }
    return rowMeta;
  }

  public BeamSnowflakeInputTransform buildRead(IVariables variables, String name)
      throws HopException {
    SnowflakeSpec spec = spec(variables);
    RowMeta rowMeta = outputRowMeta(variables, name);
    String rowMetaXml;
    try {
      rowMetaXml = rowMeta.getMetaXml();
    } catch (IOException e) {
      throw new HopException("Unable to serialize Snowflake input fields", e);
    }
    Coder<HopRow> coder = new HopRowCoder();
    SnowflakeIO.Read<HopRow> read =
        SnowflakeIO.<HopRow>read()
            .withDataSourceConfiguration(spec.getDataSource())
            .withStagingBucketName(spec.getStagingBucket())
            .withStorageIntegrationName(spec.getStorageIntegration())
            .withCsvMapper(new SnowflakeCsvToHop(name, rowMetaXml))
            .withCoder(coder);
    read =
        spec.getQuery() != null ? read.fromQuery(spec.getQuery()) : read.fromTable(spec.getTable());
    if (spec.getQuotationMark() != null) read = read.withQuotationMark(spec.getQuotationMark());
    return new BeamSnowflakeInputTransform(read);
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
        "Beam Snowflake input is a source and does not accept incoming rows");
    transformCollectionMap.put(
        transformMeta.getName(),
        pipeline.apply(transformMeta.getName(), buildRead(variables, transformMeta.getName())));
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
        throw new HopException(
            "Beam Snowflake input is a source and does not accept incoming rows");
      buildRead(variables, transformMeta.getName());
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  BeamSnowflakeInputMeta.class, "BeamSnowflakeInput.ConfigurationValid"),
              transformMeta));
    } catch (HopException e) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
    }
  }

  @Override
  public String getDialogClassName() {
    return BeamSnowflakeInputDialog.class.getName();
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
      RowMeta output = outputRowMeta(variables, name);
      row.clear();
      for (int i = 0; i < output.size(); i++) row.addValueMeta(output.getValueMeta(i));
    } catch (HopException e) {
      throw new HopTransformException(e.getMessage(), e);
    }
  }
}
