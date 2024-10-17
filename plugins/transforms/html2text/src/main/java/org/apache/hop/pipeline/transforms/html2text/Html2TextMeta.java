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

package org.apache.hop.pipeline.transforms.html2text;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_OK;
import static org.apache.hop.core.util.Utils.isEmpty;
import static org.apache.hop.core.xml.XmlHandler.addTagValue;
import static org.apache.hop.core.xml.XmlHandler.getTagValue;
import static org.apache.hop.i18n.BaseMessages.getString;
import static org.apache.hop.pipeline.transforms.html2text.Html2TextMeta.SafelistType.basic;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "Html2Text",
    image = "html2text.svg",
    name = "i18n::BaseTransform.TypeLongDesc.Html2Text",
    description = "i18n::BaseTransform.TypeTooltipDesc.Html2Text",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/html2text.html")
public class Html2TextMeta extends BaseTransformMeta<Html2Text, Html2TextData> {
  private static final Class<?> PKG = Html2TextMeta.class; // For Translator

  private String htmlField;
  private String outputField = "html2text_output";
  private String safelistType = basic.getCode();
  private boolean parallelism = false;
  private boolean normalisedText = false;
  private boolean cleanOnly = false;

  public Html2TextMeta() {
    super();
  }

  @Override
  public void setDefault() {
    normalisedText = false;
    parallelism = false;
    cleanOnly = false;
    outputField = "html2text_output";
    safelistType = basic.getCode();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      htmlField = getTagValue(transformNode, "htmlField");
      outputField = getTagValue(transformNode, "outputField");
      safelistType = getTagValue(transformNode, "safelistType");
      cleanOnly = equalsIgnoreCase("Y", getTagValue(transformNode, "cleanOnly"));
      parallelism = equalsIgnoreCase("Y", getTagValue(transformNode, "parallelism"));
      normalisedText = equalsIgnoreCase("Y", getTagValue(transformNode, "normalisedText"));

    } catch (Exception e) {
      throw new HopXmlException(
          getString(PKG, "Html2TextMeta.Exception.UnableToReadTransformMeta"), e);
    }
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    valueMetaString(r, name, outputField);
  }

  private void valueMetaString(IRowMeta r, String name, String metaName) {
    IValueMeta sText = new ValueMetaString(metaName);
    sText.setOrigin(name);
    r.addValueMeta(sText);
  }

  private void valueMetaBoolean(IRowMeta r, String name, String metaName) {
    IValueMeta sText = new ValueMetaBoolean(metaName);
    sText.setOrigin(name);
    r.addValueMeta(sText);
  }

  private void valueMetaInteger(IRowMeta r, String name, String metaName) {
    IValueMeta sText = new ValueMetaInteger(metaName);
    sText.setOrigin(name);
    r.addValueMeta(sText);
  }

  @Override
  public String getXml() {
    return "    "
        + addTagValue("htmlField", htmlField)
        + "    "
        + addTagValue("outputField", outputField)
        + "    "
        + addTagValue("safelistType", safelistType)
        + "    "
        + addTagValue("cleanOnly", cleanOnly)
        + "    "
        + addTagValue("normalisedText", normalisedText)
        + "    "
        + addTagValue("parallelism", parallelism);
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
    CheckResult cr;

    if (isEmpty(htmlField)) {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              getString(PKG, "Html2TextMeta.CheckResult.HtmlFieldMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              getString(PKG, "Html2TextMeta.CheckResult.HtmlFieldOK"),
              transformMeta);
    }
    remarks.add(cr);

    if (isEmpty(outputField)) {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              getString(PKG, "Html2TextMeta.CheckResult.OutputFieldMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              getString(PKG, "Html2TextMeta.CheckResult.OutputFieldOK"),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              getString(PKG, "Html2TextMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              getString(PKG, "Html2TextMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public String getHtmlField() {
    return htmlField;
  }

  public void setHtmlField(String htmlField) {
    this.htmlField = htmlField;
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField(String outputField) {
    this.outputField = outputField;
  }

  public boolean isParallelism() {
    return parallelism;
  }

  public void setParallelism(boolean parallelism) {
    this.parallelism = parallelism;
  }

  public String getSafelistType() {
    return safelistType;
  }

  public void setSafelistType(String safelistType) {
    this.safelistType = safelistType;
  }

  public boolean isCleanOnly() {
    return cleanOnly;
  }

  public void setCleanOnly(boolean cleanOnly) {
    this.cleanOnly = cleanOnly;
  }

  public boolean isNormalisedText() {
    return normalisedText;
  }

  public void setNormalisedText(boolean normalisedText) {
    this.normalisedText = normalisedText;
  }

  public enum SafelistType {
    none("none", getString(PKG, "Html2TextDialog.SafelistType.none")),
    relaxed("relaxed", getString(PKG, "Html2TextDialog.SafelistType.relaxed")),
    basic("basic", getString(PKG, "Html2TextDialog.SafelistType.basic")),
    simpleText("simpleText", getString(PKG, "Html2TextDialog.SafelistType.simpleText")),
    basicWithImages(
        "basicWithImages", getString(PKG, "Html2TextDialog.SafelistType.basicWithImages"));

    private final String code;
    private final String description;

    SafelistType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static SafelistType getTypeFromDescription(String description) {
      for (SafelistType type : values()) {
        if (equalsIgnoreCase(type.description, description)) {
          return type;
        }
      }
      return basic;
    }

    public static String[] getDescriptions() {
      SafelistType[] types = SafelistType.values();
      String[] descriptions = new String[types.length];
      for (int i = 0; i < types.length; i++) {
        descriptions[i] = types[i].description;
      }
      return descriptions;
    }

    public String getCode() {
      return code;
    }

    public String getDescription() {
      return description;
    }
  }
}
