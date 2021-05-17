/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.filterrows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.sql.SqlCondition;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;

import java.util.List;
import java.util.Optional;

/*
 * Created on 02-jun-2003
 *
 */
@Transform(
    id = "Filter",
    image = "filter.svg",
    name = "i18n::BaseTransform.TypeLongDesc.Filter",
    description = "i18n::BaseTransform.TypeTooltipDesc.Filter",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/filter.html")
public class FilterMeta extends BaseTransformMeta implements ITransformMeta<Filter, FilterData> {
  private static final Class<?> PKG = FilterMeta.class; // For Translator

  @HopMetadataProperty(
      key = "send_true_to",
      injectionKey = "SEND_TRUE_TRANSFORM",
      injectionKeyDescription = "FilterMeta.Injection.SEND_TRUE_TRANSFORM")
  private String trueTransformName;

  @HopMetadataProperty(
      key = "send_false_to",
      injectionKey = "SEND_FALSE_TRANSFORM",
      injectionKeyDescription = "FilterMeta.Injection.SEND_FALSE_TRANSFORM")
  private String falseTransformName;

  @HopMetadataProperty(
      injectionKey = "CONDITION",
      injectionKeyDescription = "FilterMeta.Injection.CONDITION")
  private String condition;

  public FilterMeta() {}

  public FilterMeta clone() {
    FilterMeta meta = new FilterMeta();
    meta.falseTransformName = falseTransformName;
    meta.trueTransformName = trueTransformName;
    meta.condition = condition;
    return meta;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();
    targetStreams
        .get(0)
        .setTransformMeta(TransformMeta.findTransform(transforms, trueTransformName));
    targetStreams
        .get(1)
        .setTransformMeta(TransformMeta.findTransform(transforms, falseTransformName));
  }

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
    String errorMessage = "";

    checkTarget(transformMeta, "true", trueTransformName, output).ifPresent(remarks::add);
    checkTarget(transformMeta, "false", falseTransformName, output).ifPresent(remarks::add);

    if (condition.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "FilterMeta.CheckResult.NoConditionSpecified"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "FilterMeta.CheckResult.ConditionSpecified"),
              transformMeta);
    }
    remarks.add(cr);

    // Look up fields in the input stream <prev>
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FilterMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      // See if the condition is valid...
      //
      try {
        new SqlCondition("", condition, prev);
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "FilterMeta.CheckResult.ConditionParsedCorrectly"),
                transformMeta));
      } catch (Exception e) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "FilterMeta.CheckResult.ConditionParsedWithErrors",
                    condition,
                    Const.getStackTracker(e)),
                transformMeta));
      }
    } else {
      errorMessage =
          BaseMessages.getString(
                  PKG, "FilterMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform")
              + Const.CR;
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FilterMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FilterMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  private Optional<CheckResult> checkTarget(
      TransformMeta transformMeta, String target, String targetTransformName, String[] output) {
    if (targetTransformName != null) {
      int trueTargetIdx = Const.indexOfString(targetTransformName, output);
      if (trueTargetIdx < 0) {
        return Optional.of(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "FilterMeta.CheckResult.TargetTransformInvalid",
                    target,
                    targetTransformName),
                transformMeta));
      }
    }
    return Optional.empty();
  }

  public Filter createTransform(
      TransformMeta transformMeta, FilterData data, int cnr, PipelineMeta tr, Pipeline pipeline) {
    return new Filter(transformMeta, this, data, cnr, tr, pipeline);
  }

  public FilterData getTransformData() {
    return new FilterData();
  }

  /** Returns the Input/Output metadata for this transform. */
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, true, false, false, false, false);

      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              null,
              BaseMessages.getString(PKG, "FilterMeta.InfoStream.True.Description"),
              StreamIcon.TRUE,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              null,
              BaseMessages.getString(PKG, "FilterMeta.InfoStream.False.Description"),
              StreamIcon.FALSE,
              null));
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {}

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata
   * implications of that.
   *
   * @param stream The optional stream to handle.
   */
  public void handleStreamSelection(IStream stream) {
    // This transform targets another transform.
    // Make sure that we don't specify the same transform for true and false...
    // If the user requests false, we blank out true and vice versa
    //
    List<IStream> targets = getTransformIOMeta().getTargetStreams();
    int index = targets.indexOf(stream);
    switch (index) {
      case 0:
        {
          // True
          //
          TransformMeta falseTransform = targets.get(1).getTransformMeta();
          if (falseTransform != null && falseTransform.equals(stream.getTransformMeta())) {
            targets.get(1).setTransformMeta(null);
          }
        }
        break;
      case 1:
        {
          // False
          //
          TransformMeta trueTransform = targets.get(0).getTransformMeta();
          if (trueTransform != null && trueTransform.equals(stream.getTransformMeta())) {
            targets.get(0).setTransformMeta(null);
          }
        }
        break;
    }
  }

  @Override
  public void convertIOMetaToTransformNames() {
    List<IStream> targets = getTransformIOMeta().getTargetStreams();
    trueTransformName = targets.get(0).getTransformName();
    falseTransformName = targets.get(1).getTransformName();
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  private String getTargetTransformName(int streamIndex) {
    switch (streamIndex) {
      case 0:
        return trueTransformName;
      case 1:
        return falseTransformName;
    }
    throw new RuntimeException("Illegal index given for target transform: " + streamIndex);
  }

  /**
   * Gets trueTransformName
   *
   * @return value of trueTransformName
   */
  public String getTrueTransformName() {
    return trueTransformName;
  }

  /** @param trueTransformName The trueTransformName to set */
  public void setTrueTransformName(String trueTransformName) {
    this.trueTransformName = trueTransformName;
  }

  /**
   * Gets falseTransformName
   *
   * @return value of falseTransformName
   */
  public String getFalseTransformName() {
    return falseTransformName;
  }

  /** @param falseTransformName The falseTransformName to set */
  public void setFalseTransformName(String falseTransformName) {
    this.falseTransformName = falseTransformName;
  }

  /**
   * Gets condition
   *
   * @return value of condition
   */
  public String getCondition() {
    return condition;
  }

  /** @param condition The condition to set */
  public void setCondition(String condition) {
    this.condition = condition;
  }
}
