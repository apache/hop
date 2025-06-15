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
package org.apache.hop.pipeline.transforms.javafilter;

import java.util.List;
import java.util.Objects;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

/** Contains the meta-data for the java filter transform: calculates conditions using Janino */
@Transform(
    id = "JavaFilter",
    image = "javafilter.svg",
    name = "i18n::JavaFilter.Name",
    description = "i18n::JavaFilter.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    keywords = "i18n::JavaFilterMeta.keyword",
    documentationUrl = "/pipeline/transforms/javafilter.html")
public class JavaFilterMeta extends BaseTransformMeta<JavaFilter, JavaFilterData> {
  private static final Class<?> PKG = JavaFilterMeta.class;

  /** The formula calculations to be performed */
  @HopMetadataProperty(
      key = "condition",
      injectionKeyDescription = "JavaFilterMeta.Injection.Condition")
  private String condition;

  @HopMetadataProperty(
      key = "send_true_to",
      injectionKeyDescription = "JavaFilterMeta.Injection.TrueTransform")
  private String trueTransform;

  @HopMetadataProperty(
      key = "send_false_to",
      injectionKeyDescription = "JavaFilterMeta.Injection.FalseTransform")
  private String falseTransform;

  public String getTrueTransform() {
    return trueTransform;
  }

  public void setTrueTransform(String trueTransform) {
    this.trueTransform = trueTransform;
  }

  public String getFalseTransform() {
    return falseTransform;
  }

  public void setFalseTransform(String falseTransform) {
    this.falseTransform = falseTransform;
  }

  public JavaFilterMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getCondition() {
    return condition;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTransformIOMeta().getTargetStreams(), condition);
  }

  @Override
  public Object clone() {
    JavaFilterMeta retval = (JavaFilterMeta) super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
    condition = "true";
  }

  @Override
  public void convertIOMetaToTransformNames() {
    List<IStream> streams = getTransformIOMeta().getTargetStreams();
    trueTransform = Const.NVL(streams.get(0).getTransformName(), "");
    falseTransform = Const.NVL(streams.get(1).getTransformName(), "");
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> streams = getTransformIOMeta().getTargetStreams();
    streams.get(0).setTransformMeta(TransformMeta.findTransform(transforms, trueTransform));
    streams.get(1).setTransformMeta(TransformMeta.findTransform(transforms, falseTransform));
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
    String errorMessage = "";

    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();

    if (targetStreams.get(0).getTransformName() != null
        && targetStreams.get(1).getTransformName() != null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "JavaFilterMeta.CheckResult.BothTrueAndFalseTransformSpecified"),
              transformMeta);
      remarks.add(cr);
    } else if (targetStreams.get(0).getTransformName() == null
        && targetStreams.get(1).getTransformName() == null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "JavaFilterMeta.CheckResult.NeitherTrueAndFalseTransformSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "JavaFilterMeta.CheckResult.PlsSpecifyBothTrueAndFalseTransform"),
              transformMeta);
      remarks.add(cr);
    }

    if (targetStreams.get(0).getTransformName() != null) {
      int trueTargetIdx = Const.indexOfString(targetStreams.get(0).getTransformName(), output);
      if (trueTargetIdx < 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "JavaFilterMeta.CheckResult.TargetTransformInvalid",
                    "true",
                    targetStreams.get(0).getTransformName()),
                transformMeta);
        remarks.add(cr);
      }
    }

    if (targetStreams.get(1).getTransformName() != null) {
      int falseTargetIdx = Const.indexOfString(targetStreams.get(1).getTransformName(), output);
      if (falseTargetIdx < 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "JavaFilterMeta.CheckResult.TargetTransformInvalid",
                    "false",
                    targetStreams.get(1).getTransformName()),
                transformMeta);
        remarks.add(cr);
      }
    }

    if (Utils.isEmpty(condition)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JavaFilterMeta.CheckResult.NoConditionSpecified"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "JavaFilterMeta.CheckResult.ConditionSpecified"),
              transformMeta);
    }
    remarks.add(cr);

    // Look up fields in the input stream <prev>
    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "JavaFilterMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      // What fields are used in the condition?
      // TODO: verify condition, parse it
      //
    } else {
      errorMessage =
          BaseMessages.getString(
                  PKG, "JavaFilterMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform")
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
                  PKG, "JavaFilterMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "JavaFilterMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /** Returns the Input/Output metadata for this transform. */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, true, false, false, false, false);

      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              null,
              BaseMessages.getString(PKG, "JavaFilterMeta.InfoStream.True.Description"),
              StreamIcon.TRUE,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              null,
              BaseMessages.getString(PKG, "JavaFilterMeta.InfoStream.False.Description"),
              StreamIcon.FALSE,
              null));
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
    // ignore reset
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }
}
