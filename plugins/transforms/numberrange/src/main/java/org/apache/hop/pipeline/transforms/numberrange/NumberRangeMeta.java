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

package org.apache.hop.pipeline.transforms.numberrange;

import java.util.LinkedList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "NumberRange",
    image = "numberrange.svg",
    name = "i18n::NumberRange.Name",
    description = "i18n::NumberRange.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::NumberRange.Keyword",
    documentationUrl = "/pipeline/transforms/numberrange.html")
public class NumberRangeMeta extends BaseTransformMeta<NumberRange, NumberRangeData> {

  private static final Class<?> PKG = NumberRangeMeta.class;

  @HopMetadataProperty(
      key = "inputField",
      injectionKey = "INPUT_FIELD",
      injectionKeyDescription = "NumberRangeMeta.Injection.INPUT_FIELD")
  private String inputField;

  @HopMetadataProperty(
      key = "outputField",
      injectionKey = "OUTPUT_FIELD",
      injectionKeyDescription = "NumberRangeMeta.Injection.OUTPUT_FIELD")
  private String outputField;

  @HopMetadataProperty(
      key = "fallBackValue",
      injectionKey = "FALL_BACK_VALUE",
      injectionKeyDescription = "NumberRangeMeta.Injection.FALL_BACK_VALUE")
  private String fallBackValue;

  @HopMetadataProperty(
      groupKey = "rules",
      key = "rule",
      injectionGroupKey = "RULES",
      injectionGroupDescription = "NumberRangeMeta.Injection.RULES")
  private List<NumberRangeRule> rules;

  public NumberRangeMeta() {
    super();
    rules = new LinkedList<>();
  }

  public void emptyRules() {
    rules = new LinkedList<>();
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    IValueMeta mcValue = new ValueMetaString(outputField);
    mcValue.setOrigin(name);
    mcValue.setLength(255);
    row.addValueMeta(mcValue);
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
    emptyRules();
    setFallBackValue("unknown");
    addRule("", "5", "Less than 5");
    addRule("5", "10", "5-10");
    addRule("10", "", "More than 10");
    inputField = "";
    outputField = "range";
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transforminfo,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    if (prev == null || prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "NumberRangeMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform"),
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "NumberRangeMeta.CheckResult.TransformReceivingFieldsOK", prev.size() + ""),
              transforminfo);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "NumberRangeMeta.CheckResult.TransformReceivingInfoOK"),
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "NumberRangeMeta.CheckResult.NoInputReceivedError"),
              transforminfo);
      remarks.add(cr);
    }

    // Check that the lower and upper bounds are numerics
    for (NumberRangeRule rule : this.rules) {
      try {
        if (!Utils.isEmpty(rule.getLowerBound())) {
          Double.valueOf(rule.getLowerBound());
        }
        if (!Utils.isEmpty(rule.getUpperBound())) {
          Double.valueOf(rule.getUpperBound());
        }
      } catch (NumberFormatException e) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "NumberRangeMeta.CheckResult.NotNumericRule",
                    rule.getLowerBound(),
                    rule.getUpperBound(),
                    rule.getValue()),
                transforminfo);
        remarks.add(cr);
      }
    }
  }

  public String getInputField() {
    return inputField;
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField(String outputField) {
    this.outputField = outputField;
  }

  public List<NumberRangeRule> getRules() {
    return rules;
  }

  public String getFallBackValue() {
    return fallBackValue;
  }

  public void setInputField(String inputField) {
    this.inputField = inputField;
  }

  public void setFallBackValue(String fallBackValue) {
    this.fallBackValue = fallBackValue;
  }

  public NumberRangeRule addRule(String lowerBound, String upperBound, String value) {
    NumberRangeRule rule = new NumberRangeRule(lowerBound, upperBound, value);
    rules.add(rule);
    return rule;
  }

  public void setRules(List<NumberRangeRule> rules) {
    this.rules = rules;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
