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

package org.apache.hop.pipeline.transforms.language;

import static org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_OK;
import static org.apache.hop.core.util.Utils.isEmpty;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "DetectLanguage",
    image = "detectlanguage.svg",
    name = "i18n::DetectLanguage.Name",
    description = "i18n::DetectLanguage.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::DetectLanguage.Keyword",
    documentationUrl = "/pipeline/transforms/detectlanguage.html")
public class DetectLanguageMeta extends BaseTransformMeta<DetectLanguage, DetectLanguageData> {
  private static final Class<?> PKG = DetectLanguageMeta.class;

  @HopMetadataProperty(
      key = "corpusField",
      injectionKey = "FIELD",
      injectionKeyDescription = "DetectLanguage.Injection.CorpusField")
  private String corpusField;

  @HopMetadataProperty(
      key = "parallelism",
      injectionKey = "PARALLISM",
      injectionKeyDescription = "DetectLanguage.Injection.Parallelism")
  private boolean parallelism = false;

  public DetectLanguageMeta() {
    super();
  }

  @Override
  public void setDefault() {
    parallelism = false;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    // name = getParentTransformMeta().getName()
    valueMetaString(r, name, "detected_language");
    valueMetaNumber(r, name, "detected_language_confidence");
  }

  private void valueMetaString(IRowMeta r, String name, String metaName) {
    IValueMeta sText = new ValueMetaString(metaName);
    sText.setOrigin(name);
    r.addValueMeta(sText);
  }

  private void valueMetaNumber(IRowMeta r, String name, String metaName) {
    IValueMeta sText = new ValueMetaNumber(metaName);
    sText.setOrigin(name);
    r.addValueMeta(sText);
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

    if (isEmpty(corpusField)) {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DetectLanguageMeta.CheckResult.CorpusFieldMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DetectLanguageMeta.CheckResult.CorpusFieldOK"),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "DetectLanguageMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DetectLanguageMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public String getCorpusField() {
    return corpusField;
  }

  public void setCorpusField(String corpusField) {
    this.corpusField = corpusField;
  }

  public boolean isParallelism() {
    return parallelism;
  }

  public void setParallelism(boolean parallelism) {
    this.parallelism = parallelism;
  }
}
