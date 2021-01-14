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

package org.apache.hop.pipeline.transforms.calculator;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

/**
 * Contains the meta-data for the Calculator transform: calculates predefined formula's
 *
 * @since 08 september 2005
 */
@Transform(
    id = "Calculator",
    image = "calculator.svg",
    name = "i18n::BaseTransform.TypeLongDesc.Calculator",
    description = "i18n::BaseTransform.TypeTooltipDesc.Calculator",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/calculator.html")
public class CalculatorMeta extends BaseTransformMeta
    implements ITransformMeta<Calculator, CalculatorData> {

  private static final Class<?> PKG = CalculatorMeta.class; // For Translator

  /** The calculations to be performed */
  private CalculatorMetaFunction[] calculation;

  /** Raise an error if file does not exist */
  private boolean failIfNoFile;

  public CalculatorMetaFunction[] getCalculation() {
    return calculation;
  }

  public void setCalculation(CalculatorMetaFunction[] calcTypes) {
    this.calculation = calcTypes;
  }

  public boolean isFailIfNoFile() {
    return failIfNoFile;
  }

  public void setFailIfNoFile(boolean failIfNoFile) {
    this.failIfNoFile = failIfNoFile;
  }

  public void allocate(int nrCalcs) {
    calculation = new CalculatorMetaFunction[nrCalcs];
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    failIfNoFile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "failIfNoFile"));

    int nrCalcs = XmlHandler.countNodes(transformNode, CalculatorMetaFunction.XML_TAG);
    allocate(nrCalcs);
    for (int i = 0; i < nrCalcs; i++) {
      Node calcnode = XmlHandler.getSubNodeByNr(transformNode, CalculatorMetaFunction.XML_TAG, i);
      calculation[i] = new CalculatorMetaFunction(calcnode);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    ").append(XmlHandler.addTagValue("failIfNoFile", failIfNoFile));

    if (calculation != null) {
      for (CalculatorMetaFunction aCalculation : calculation) {
        retval.append(aCalculation.getXml());
      }
    }

    return retval.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && (obj.getClass().equals(this.getClass()))) {
      CalculatorMeta m = (CalculatorMeta) obj;
      return (getXml().equals(m.getXml()));
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(calculation);
  }

  @Override
  public Object clone() {
    CalculatorMeta retval = (CalculatorMeta) super.clone();
    retval.setFailIfNoFile(isFailIfNoFile());
    if (calculation != null) {
      retval.allocate(calculation.length);
      for (int i = 0; i < calculation.length; i++) {
        (retval.getCalculation())[i] = (CalculatorMetaFunction) calculation[i].clone();
      }
    } else {
      retval.allocate(0);
    }
    return retval;
  }

  @Override
  public void setDefault() {
    failIfNoFile = true;
    calculation = new CalculatorMetaFunction[0];
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (CalculatorMetaFunction fn : calculation) {
      if (!fn.isRemovedFromResult()) {
        if (!Utils.isEmpty(fn.getFieldName())) { // It's a new field!
          IValueMeta v = getValueMeta(fn, origin);
          row.addValueMeta(v);
        }
      }
    }
  }

  private IValueMeta getValueMeta(CalculatorMetaFunction fn, String origin) {
    IValueMeta v;
    // What if the user didn't specify a data type?
    // In that case we look for the default data type
    //
    int defaultResultType = fn.getValueType();
    if (defaultResultType == IValueMeta.TYPE_NONE) {
      defaultResultType = CalculatorMetaFunction.getCalcFunctionDefaultResultType(fn.getCalcType());
    }
    try {
      v = ValueMetaFactory.createValueMeta(fn.getFieldName(), defaultResultType);
    } catch (Exception ex) {
      return null;
    }
    v.setLength(fn.getValueLength());
    v.setPrecision(fn.getValuePrecision());
    v.setOrigin(origin);
    v.setComments(fn.getCalcTypeDesc());
    v.setConversionMask(fn.getConversionMask());
    v.setDecimalSymbol(fn.getDecimalSymbol());
    v.setGroupingSymbol(fn.getGroupingSymbol());
    v.setCurrencySymbol(fn.getCurrencySymbol());

    return v;
  }

  public IRowMeta getAllFields(IRowMeta inputRowMeta) {
    IRowMeta rowMeta = inputRowMeta.clone();

    for (CalculatorMetaFunction fn : getCalculation()) {
      if (!Utils.isEmpty(fn.getFieldName())) { // It's a new field!
        IValueMeta v = getValueMeta(fn, null);
        rowMeta.addValueMeta(v);
      }
    }
    return rowMeta;
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
              BaseMessages.getString(PKG, "CalculatorMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);

      if (prev == null || prev.size() == 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_WARNING,
                BaseMessages.getString(PKG, "CalculatorMeta.CheckResult.ExpectedInputError"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "CalculatorMeta.CheckResult.FieldsReceived", "" + prev.size()),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "CalculatorMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public Calculator createTransform(
      TransformMeta transformMeta,
      CalculatorData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new Calculator(transformMeta, this, data, cnr, tr, pipeline);
  }

  @Override
  public CalculatorData getTransformData() {
    return new CalculatorData();
  }
}
