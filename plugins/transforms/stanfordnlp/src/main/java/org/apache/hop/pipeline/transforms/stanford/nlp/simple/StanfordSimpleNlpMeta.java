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

package org.apache.hop.pipeline.transforms.stanford.nlp.simple;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_OK;
import static org.apache.hop.core.util.Utils.isEmpty;
import static org.apache.hop.core.xml.XmlHandler.addTagValue;
import static org.apache.hop.core.xml.XmlHandler.getTagValue;

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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "StanfordSimpleNlp",
    image = "stanfordnlp.svg",
    name = "i18n::BaseTransform.TypeLongDesc.StanfordSimpleNlp",
    description = "i18n::BaseTransform.TypeTooltipDesc.StanfordSimpleNlp",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/stanfordnlp.html")
public class StanfordSimpleNlpMeta
    extends BaseTransformMeta<StanfordSimpleNlp, StanfordSimpleNlpData> {
  private static final Class<?> PKG = StanfordSimpleNlpMeta.class; // For Translator

  private String corpusField;
  private boolean includePartOfSpeech = false;
  private boolean parallelism = false;
  private String outputFieldNamePrefix = "sentence_";

  public StanfordSimpleNlpMeta() {
    super();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      corpusField = getTagValue(transformNode, "corpusField");
      includePartOfSpeech =
          equalsIgnoreCase("Y", getTagValue(transformNode, "includePartOfSpeech"));
      parallelism = equalsIgnoreCase("Y", getTagValue(transformNode, "parallelism"));

      outputFieldNamePrefix = getTagValue(transformNode, "outputFieldNamePrefix");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "StanfordSimpleNlpMeta.Exception.UnableToReadTransformMeta"),
          e);
    }
  }

  @Override
  public void setDefault() {
    includePartOfSpeech = false;
    parallelism = false;
    outputFieldNamePrefix = "sentence_";
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    valueMetaString(r, name, outputFieldNamePrefix + "text");
    valueMetaInteger(r, name, outputFieldNamePrefix + "index");
    valueMetaInteger(r, name, outputFieldNamePrefix + "index_start");
    valueMetaInteger(r, name, outputFieldNamePrefix + "index_end");
    valueMetaInteger(r, name, outputFieldNamePrefix + "character_count");
    valueMetaInteger(r, name, outputFieldNamePrefix + "word_count");

    if (includePartOfSpeech) {
      valueMetaString(r, name, outputFieldNamePrefix + "pos_tagged");
      valueMetaString(r, name, outputFieldNamePrefix + "pos_tags");
      for (PennTreebankPartOfSpeech e : PennTreebankPartOfSpeech.values()) {
        valueMetaInteger(r, name, outputFieldNamePrefix + "penn_treebank_pos_" + e.name());
      }
    }
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
        + addTagValue("corpusField", corpusField)
        + "    "
        + addTagValue("includePartOfSpeech", includePartOfSpeech)
        + "    "
        + addTagValue("parallelism", parallelism)
        + "    "
        + addTagValue("outputFieldNamePrefix", outputFieldNamePrefix);
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
              BaseMessages.getString(PKG, "StanfordSimpleNlpMeta.CheckResult.CorpusFieldMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "StanfordSimpleNlpMeta.CheckResult.CorpusFieldOK"),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "StanfordSimpleNlpMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "StanfordSimpleNlpMeta.CheckResult.NoInpuReceived"),
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

  public boolean isIncludePartOfSpeech() {
    return includePartOfSpeech;
  }

  public void setIncludePartOfSpeech(boolean includePartOfSpeech) {
    this.includePartOfSpeech = includePartOfSpeech;
  }

  public boolean isParallelism() {
    return parallelism;
  }

  public void setParallelism(boolean parallelism) {
    this.parallelism = parallelism;
  }

  public String getOutputFieldNamePrefix() {
    return outputFieldNamePrefix;
  }

  public void setOutputFieldNamePrefix(String outputFieldNamePrefix) {
    this.outputFieldNamePrefix = outputFieldNamePrefix;
  }
}
