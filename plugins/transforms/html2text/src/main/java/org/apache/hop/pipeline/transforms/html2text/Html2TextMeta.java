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

import static org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_OK;
import static org.apache.hop.core.util.Utils.isEmpty;
import static org.apache.hop.i18n.BaseMessages.getString;
import static org.apache.hop.pipeline.transforms.html2text.Html2TextMeta.SafelistType.BASIC;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "Html2Text",
    image = "html2text.svg",
    name = "i18n::BaseTransform.TypeLongDesc.Html2Text",
    description = "i18n::BaseTransform.TypeTooltipDesc.Html2Text",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/html2text.html")
@Getter
@Setter
public class Html2TextMeta extends BaseTransformMeta<Html2Text, Html2TextData> {
  private static final Class<?> PKG = Html2TextMeta.class; // For Translator

  @HopMetadataProperty(key = "htmlField")
  private String htmlField;

  @HopMetadataProperty(key = "outputField")
  private String outputField;

  @HopMetadataProperty(key = "safelistType", storeWithCode = true)
  private SafelistType safelistType;

  @HopMetadataProperty(key = "cleanOnly")
  private boolean cleanOnly;

  @HopMetadataProperty(key = "parallelism")
  private boolean parallelism;

  @HopMetadataProperty(key = "normalisedText")
  private boolean normalisedText;

  public Html2TextMeta() {
    super();
    this.outputField = "html2text_output";
    this.safelistType = BASIC;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    // We simply add the output field to contain the generated text.
    //
    IValueMeta outputFieldMeta = new ValueMetaString(outputField);
    outputFieldMeta.setOrigin(name);
    inputRowMeta.addValueMeta(outputFieldMeta);
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

  @Getter
  public enum SafelistType implements IEnumHasCodeAndDescription {
    NONE("none", getString(PKG, "Html2TextDialog.SafelistType.none")),
    RELAXED("relaxed", getString(PKG, "Html2TextDialog.SafelistType.relaxed")),
    BASIC("basic", getString(PKG, "Html2TextDialog.SafelistType.basic")),
    SIMPLE_TEXT("simpleText", getString(PKG, "Html2TextDialog.SafelistType.simpleText")),
    BASIC_WITH_IMAGES(
        "basicWithImages", getString(PKG, "Html2TextDialog.SafelistType.basicWithImages"));

    private final String code;
    private final String description;

    SafelistType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static SafelistType getTypeFromDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(SafelistType.class, description, BASIC);
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(SafelistType.class);
    }
  }
}
