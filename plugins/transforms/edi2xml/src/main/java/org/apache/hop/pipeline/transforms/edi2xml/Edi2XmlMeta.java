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

package org.apache.hop.pipeline.transforms.edi2xml;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "TypeExitEdi2XmlTransform",
    image = "edi2xml.svg",
    name = "i18n::Edi2Xml.Name",
    description = "i18n::Edi2Xml.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::Edi2XmlMeta.keyword",
    documentationUrl = "/pipeline/transforms/edi2xml.html")
public class Edi2XmlMeta extends BaseTransformMeta<Edi2Xml, Edi2XmlData> {
  @HopMetadataProperty(key = "outputfield")
  private String outputField;

  @HopMetadataProperty(key = "inputfield")
  private String inputField;

  public Edi2XmlMeta() {
    super();
  }

  @Override
  public void setDefault() {
    outputField = "edi_xml";
    inputField = "";
  }

  public Edi2XmlMeta(Edi2XmlMeta m) {
    this();
    this.outputField = m.outputField;
    this.inputField = m.inputField;
  }

  @Override
  public Edi2XmlMeta clone() {
    return new Edi2XmlMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta r,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    IValueMeta extra = null;

    if (!Utils.isEmpty(getOutputField())) {
      extra = new ValueMetaString(variables.resolve(getOutputField()));
      extra.setOrigin(origin);
      r.addValueMeta(extra);
    } else {
      if (!Utils.isEmpty(getInputField())) {
        extra = r.searchValueMeta(variables.resolve(getInputField()));
      }
    }

    if (extra != null) {
      extra.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    }
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

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Transform is receiving input from other transforms.",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "No input received from other transforms!",
              transformMeta);
      remarks.add(cr);
    }

    // is the input field there?
    String realInputField = variables.resolve(getInputField());
    if (prev.searchValueMeta(realInputField) != null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Transform is seeing input field: " + realInputField,
              transformMeta);
      remarks.add(cr);

      if (prev.searchValueMeta(realInputField).isString()) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                "Field " + realInputField + " is a string type",
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                "Field " + realInputField + " is not a string type!",
                transformMeta);
        remarks.add(cr);
      }

    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "Transform is not seeing input field: " + realInputField + "!",
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public String getInputField() {
    return inputField;
  }

  public void setInputField(String inputField) {
    this.inputField = inputField;
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField(String outputField) {
    this.outputField = outputField;
  }
}
