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
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.HopToSnowflakeRow;
import org.apache.hop.beam.core.fn.SnowflakeValues;
import org.apache.hop.beam.core.transform.BeamSnowflakeOutputTransform;
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
    id = "BeamSnowflakeOutput",
    name = "i18n::BeamSnowflakeOutputDialog.Title",
    description = "i18n::BeamSnowflakeOutputMeta.Description",
    image = "beam-snowflake-output.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamSnowflakeOutput.keyword",
    documentationUrl = "/pipeline/transforms/beamsnowflakeoutput.html",
    supportedEngines = {"Beam*"})
public class BeamSnowflakeOutputMeta
    extends BaseTransformMeta<BeamSnowflakeOutput, BeamSnowflakeOutputData>
    implements IBeamPipelineTransformHandler {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "BEAMSNOWFLAKEOUTPUT_OPTIONS";
  private static final String CONNECTION = "i18n::BeamSnowflake.Connection.Group";
  private static final String STAGING = "i18n::BeamSnowflake.Staging.Group";
  private static final String WRITE = "i18n::BeamSnowflakeOutput.Write.Group";

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
      label = "i18n::BeamSnowflakeOutput.tableName.Label",
      toolTip = "i18n::BeamSnowflakeOutput.tableName.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = WRITE,
      groupOrder = "0300")
  private String tableName;

  @HopMetadataProperty(key = "write_disposition")
  @GuiWidgetElement(
      id = "writeDisposition",
      order = "1400",
      type = GuiElementType.COMBO,
      comboValuesMethod = "getWriteDispositions",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflakeOutput.writeDisposition.Label",
      toolTip = "i18n::BeamSnowflakeOutput.writeDisposition.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = WRITE,
      groupOrder = "0300")
  private String writeDisposition = "APPEND";

  @HopMetadataProperty(key = "create_disposition")
  @GuiWidgetElement(
      id = "createDisposition",
      order = "1500",
      type = GuiElementType.COMBO,
      comboValuesMethod = "getCreateDispositions",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSnowflakeOutput.createDisposition.Label",
      toolTip = "i18n::BeamSnowflakeOutput.createDisposition.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = WRITE,
      groupOrder = "0300")
  private String createDisposition = "CREATE_NEVER";

  public List<String> getWriteDispositions(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return List.of("APPEND", "TRUNCATE", "EMPTY");
  }

  public List<String> getCreateDispositions(
      ILogChannel log, IHopMetadataProvider metadataProvider) {
    return List.of("CREATE_NEVER", "CREATE_IF_NEEDED");
  }

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
        null,
        false);
  }

  public BeamSnowflakeOutputTransform buildWrite(
      IVariables variables, String name, IRowMeta rowMeta) throws HopException {
    if (rowMeta == null || rowMeta.isEmpty())
      throw new HopException("Snowflake output requires an incoming row with at least one field");
    SnowflakeSpec spec = spec(variables);
    SnowflakeColumn[] columns = new SnowflakeColumn[rowMeta.size()];
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta value = rowMeta.getValueMeta(i);
      columns[i] =
          SnowflakeColumn.of(
              SnowflakeSpec.columnName(value.getName()),
              SnowflakeValues.snowflakeType(value),
              true);
    }
    String rowMetaXml;
    try {
      rowMetaXml = rowMeta.getMetaXml();
    } catch (IOException e) {
      throw new HopException("Unable to serialize Snowflake output fields", e);
    }
    CreateDisposition create = createDisposition(variables);
    SnowflakeIO.Write<HopRow> write =
        SnowflakeIO.<HopRow>write()
            .withDataSourceConfiguration(spec.getDataSource())
            .withStagingBucketName(spec.getStagingBucket())
            .withStorageIntegrationName(spec.getStorageIntegration())
            .withUserDataMapper(new HopToSnowflakeRow(name, rowMetaXml))
            .to(spec.getTable())
            .withWriteDisposition(writeDisposition(variables))
            .withCreateDisposition(create);
    if (create == CreateDisposition.CREATE_IF_NEEDED)
      write = write.withTableSchema(SnowflakeTableSchema.of(columns));
    if (spec.getQuotationMark() != null) write = write.withQuotationMark(spec.getQuotationMark());
    return new BeamSnowflakeOutputTransform(write);
  }

  private WriteDisposition writeDisposition(IVariables variables) throws HopException {
    String value = blank(variables, writeDisposition, "APPEND");
    try {
      return WriteDisposition.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new HopException("Snowflake write disposition must be APPEND, TRUNCATE or EMPTY");
    }
  }

  private CreateDisposition createDisposition(IVariables variables) throws HopException {
    String value = blank(variables, createDisposition, "CREATE_NEVER");
    try {
      return CreateDisposition.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new HopException(
          "Snowflake create disposition must be CREATE_NEVER or CREATE_IF_NEEDED");
    }
  }

  private static String blank(IVariables variables, String value, String fallback) {
    if (value == null) return fallback;
    String resolved = variables.resolve(value).trim();
    return resolved.isEmpty() ? fallback : resolved;
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
    if (input == null || previousTransforms == null || previousTransforms.size() != 1)
      throw new HopException("Beam Snowflake output requires exactly one incoming transform");
    SnowflakeSpec.rejectUnbounded(input.isBounded());
    input.apply(transformMeta.getName(), buildWrite(variables, transformMeta.getName(), rowMeta));
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
        throw new HopException("Beam Snowflake output requires exactly one incoming transform");
      buildWrite(variables, transformMeta.getName(), prev);
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  BeamSnowflakeOutputMeta.class, "BeamSnowflakeOutput.ConfigurationValid"),
              transformMeta));
    } catch (HopException e) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
    }
  }

  @Override
  public String getDialogClassName() {
    return BeamSnowflakeOutputDialog.class.getName();
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
