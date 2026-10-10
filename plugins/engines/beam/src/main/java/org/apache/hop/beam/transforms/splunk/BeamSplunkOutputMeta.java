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

package org.apache.hop.beam.transforms.splunk;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamSplunkOutputTransform;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
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
    id = "BeamSplunkOutput",
    name = "i18n::BeamSplunkOutputDialog.Title",
    description = "i18n::BeamSplunkOutputMeta.Description",
    image = "beam-splunk-output.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamSplunkOutput.keyword",
    documentationUrl = "/pipeline/transforms/beamsplunkoutput.html",
    supportedEngines = {"Beam*"})
public class BeamSplunkOutputMeta extends BaseTransformMeta<BeamSplunkOutput, BeamSplunkOutputData>
    implements IBeamPipelineTransformHandler {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "BEAMSPLINKOUTPUT_OPTIONS";
  private static final Pattern HEC_URL = Pattern.compile("^http(s?)://([^:]+)(:[0-9]+)?$");

  @HopMetadataProperty(key = "hec_url")
  @GuiWidgetElement(
      id = "hecUrl",
      order = "0100",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.hecUrl.Label",
      toolTip = "i18n::BeamSplunkOutput.hecUrl.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Connection.Group",
      groupOrder = "0100")
  private String hecUrl;

  @HopMetadataProperty(key = "token", password = true)
  @GuiWidgetElement(
      id = "token",
      order = "0200",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.token.Label",
      toolTip = "i18n::BeamSplunkOutput.token.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Connection.Group",
      groupOrder = "0100",
      password = true)
  private String token;

  @HopMetadataProperty(key = "batch_count")
  @GuiWidgetElement(
      id = "batchCount",
      order = "0300",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.batchCount.Label",
      toolTip = "i18n::BeamSplunkOutput.batchCount.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Connection.Group",
      groupOrder = "0100")
  private String batchCount;

  @HopMetadataProperty(key = "root_ca_certificate_path")
  @GuiWidgetElement(
      id = "rootCaCertificatePath",
      order = "0400",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.rootCa.Label",
      toolTip = "i18n::BeamSplunkOutput.rootCa.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Connection.Group",
      groupOrder = "0100")
  private String rootCaCertificatePath;

  @HopMetadataProperty(key = "disable_certificate_validation")
  @GuiWidgetElement(
      id = "disableCertificateValidation",
      order = "0500",
      type = GuiElementType.CHECKBOX,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.disableCertificateValidation.Label",
      toolTip = "i18n::BeamSplunkOutput.disableCertificateValidation.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Connection.Group",
      groupOrder = "0100")
  private boolean disableCertificateValidation;

  @HopMetadataProperty(key = "enable_gzip")
  @GuiWidgetElement(
      id = "enableGzip",
      order = "0600",
      type = GuiElementType.CHECKBOX,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.enableGzip.Label",
      toolTip = "i18n::BeamSplunkOutput.enableGzip.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Connection.Group",
      groupOrder = "0100")
  private boolean enableGzip = true;

  @HopMetadataProperty(key = "event_field")
  @GuiWidgetElement(
      id = "eventField",
      order = "0700",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.eventField.Label",
      toolTip = "i18n::BeamSplunkOutput.eventField.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Event.Group",
      groupOrder = "0200")
  private String eventField = "event";

  @HopMetadataProperty(key = "splunk_index")
  @GuiWidgetElement(
      id = "index",
      order = "0800",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.index.Label",
      toolTip = "i18n::BeamSplunkOutput.index.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Event.Group",
      groupOrder = "0200")
  private String index;

  @HopMetadataProperty(key = "source")
  @GuiWidgetElement(
      id = "source",
      order = "0900",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.source.Label",
      toolTip = "i18n::BeamSplunkOutput.source.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Event.Group",
      groupOrder = "0200")
  private String source;

  @HopMetadataProperty(key = "source_type")
  @GuiWidgetElement(
      id = "sourceType",
      order = "1000",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.sourceType.Label",
      toolTip = "i18n::BeamSplunkOutput.sourceType.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Event.Group",
      groupOrder = "0200")
  private String sourceType;

  @HopMetadataProperty(key = "host")
  @GuiWidgetElement(
      id = "host",
      order = "1100",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::BeamSplunkOutput.host.Label",
      toolTip = "i18n::BeamSplunkOutput.host.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BeamSplunkOutput.Event.Group",
      groupOrder = "0200")
  private String host;

  public BeamSplunkOutputTransform buildOutputTransform(
      IVariables variables, String name, IRowMeta rowMeta) throws HopException {
    String url = resolved(variables, hecUrl);
    if (!HEC_URL.matcher(url).matches())
      throw new HopException(
          "Splunk HEC URL must look like http://host:8088 or https://host:8088, with no path, user or query");
    String secret = resolveToken(variables, token);
    String field = required(variables, eventField, "event field");
    if (rowMeta.indexOfValue(field) < 0)
      throw new HopException("Splunk event field not found: " + field);
    String rowMetaXml;
    try {
      rowMetaXml = rowMeta.getMetaXml();
    } catch (IOException e) {
      throw new HopException("Unable to serialize Splunk row metadata", e);
    }
    return new BeamSplunkOutputTransform(
        name,
        url,
        secret,
        batchCount(variables),
        disableCertificateValidation,
        enableGzip,
        blankToNull(variables, rootCaCertificatePath),
        field,
        rowMetaXml,
        blankToNull(variables, host),
        blankToNull(variables, source),
        blankToNull(variables, sourceType),
        blankToNull(variables, index));
  }

  static String resolveToken(IVariables variables, String token) throws HopException {
    String secret =
        Encr.decryptPasswordOptionallyEncrypted(
            variables.resolve(Encr.decryptPasswordOptionallyEncrypted(token)));
    if (StringUtils.isBlank(secret)) throw new HopException("Splunk HEC token is required");
    return secret;
  }

  private Integer batchCount(IVariables variables) throws HopException {
    String value = variables.resolve(batchCount);
    if (StringUtils.isBlank(value)) return null;
    try {
      int parsed = Integer.parseInt(value.trim());
      if (parsed < 1) throw new NumberFormatException();
      return parsed;
    } catch (NumberFormatException e) {
      throw new HopException("Splunk batch count must be a positive integer");
    }
  }

  private static String required(IVariables variables, String value, String label)
      throws HopException {
    String resolved = resolved(variables, value);
    if (StringUtils.isBlank(resolved)) throw new HopException("Splunk " + label + " is required");
    return resolved;
  }

  private static String resolved(IVariables variables, String value) {
    return value == null ? "" : variables.resolve(value).trim();
  }

  private static String blankToNull(IVariables variables, String value) {
    String resolved = resolved(variables, value);
    return StringUtils.isBlank(resolved) ? null : resolved;
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
      throw new HopException("Beam Splunk output requires exactly one incoming transform");
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
        throw new HopException("Beam Splunk output requires exactly one incoming transform");
      buildOutputTransform(variables, transformMeta.getName(), prev);
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  BeamSplunkOutputMeta.class, "BeamSplunkOutput.ConfigurationValid"),
              transformMeta));
    } catch (HopException e) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
    }
  }

  @Override
  public String getDialogClassName() {
    return BeamSplunkOutputDialog.class.getName();
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
