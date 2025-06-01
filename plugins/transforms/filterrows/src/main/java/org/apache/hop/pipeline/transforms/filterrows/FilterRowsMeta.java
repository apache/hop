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

package org.apache.hop.pipeline.transforms.filterrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IStringObjectConverter;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

@Transform(
    id = "FilterRows",
    image = "filterrows.svg",
    name = "i18n::FilterRows.Name",
    description = "i18n::FilterRows.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    keywords = "i18n::FilterRowsMeta.keyword",
    documentationUrl = "/pipeline/transforms/filterrows.html")
public class FilterRowsMeta extends BaseTransformMeta<FilterRows, FilterRowsData> {
  private static final Class<?> PKG = FilterRowsMeta.class;

  /** This is the main condition for the complete filter. */
  @HopMetadataProperty(
      key = "compare",
      injectionKey = "CONDITION",
      injectionKeyDescription = "FilterRowsMeta.Injection.CONDITION",
      injectionStringObjectConverter = ConditionXmlConverter.class)
  private FRCompare compare;

  @HopMetadataProperty(
      key = "send_true_to",
      injectionKey = "SEND_TRUE_TRANSFORM",
      injectionKeyDescription = "FilterRowsMeta.Injection.SEND_TRUE_TRANSFORM")
  private String trueTransformName;

  @HopMetadataProperty(
      key = "send_false_to",
      injectionKey = "SEND_FALSE_TRANSFORM",
      injectionKeyDescription = "FilterRowsMeta.Injection.SEND_FALSE_TRANSFORM")
  private String falseTransformName;

  public FilterRowsMeta() {
    super();
    compare = new FRCompare();
  }

  public FilterRowsMeta(FilterRowsMeta m) {
    this.compare = m.compare == null ? new FRCompare() : new FRCompare(m.compare);
    this.setTrueTransformName(m.getTrueTransformName());
    this.setFalseTransformName(m.getFalseTransformName());
  }

  @Override
  public FilterRowsMeta clone() {
    return new FilterRowsMeta(this);
  }

  @Override
  public void setDefault() {
    compare = new FRCompare();
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();
    for (IStream stream : targetStreams) {
      stream.setTransformMeta(TransformMeta.findTransform(transforms, stream.getSubject()));
    }
  }

  @Override
  public void resetTransformIoMeta() {
    // Do nothing, keep dialog information around
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Clear the sortedDescending flag on fields used within the condition - otherwise the
    // comparisons will be
    // inverted!!
    String[] conditionField = getCondition().getUsedFields();
    for (String s : conditionField) {
      int idx = rowMeta.indexOfValue(s);
      if (idx >= 0) {
        IValueMeta valueMeta = rowMeta.getValueMeta(idx);
        valueMeta.setSortedDescending(false);
      }
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
    StringBuilder errorMessage;

    checkTarget(transformMeta, "true", getTrueTransformName(), output).ifPresent(remarks::add);
    checkTarget(transformMeta, "false", getFalseTransformName(), output).ifPresent(remarks::add);

    if (getCondition().isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "FilterRowsMeta.CheckResult.NoConditionSpecified"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "FilterRowsMeta.CheckResult.ConditionSpecified"),
              transformMeta);
    }
    remarks.add(cr);

    // Look up fields in the input stream <prev>
    if (!Utils.isEmpty(prev)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FilterRowsMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      List<String> orphanFields = getOrphanFields(getCondition(), prev);
      if (!orphanFields.isEmpty()) {
        errorMessage =
            new StringBuilder(
                BaseMessages.getString(
                        PKG, "FilterRowsMeta.CheckResult.FieldsNotFoundFromPreviousTransform")
                    + Const.CR);
        for (String field : orphanFields) {
          errorMessage.append("\t\t").append(field).append(Const.CR);
        }
        cr =
            new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "FilterRowsMeta.CheckResult.AllFieldsFoundInInputStream"),
                transformMeta);
      }
      remarks.add(cr);
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FilterRowsMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform"),
              transformMeta));
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FilterRowsMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FilterRowsMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta));
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
                    "FilterRowsMeta.CheckResult.TargetTransformInvalid",
                    target,
                    targetTransformName),
                transformMeta));
      }
    }
    return Optional.empty();
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
              BaseMessages.getString(PKG, "FilterRowsMeta.InfoStream.True.Description"),
              StreamIcon.TRUE,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              null,
              BaseMessages.getString(PKG, "FilterRowsMeta.InfoStream.False.Description"),
              StreamIcon.FALSE,
              null));
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata
   * implications of that.
   *
   * @param stream The optional stream to handle.
   */
  @Override
  public void handleStreamSelection(IStream stream) {
    // This transform targets another transform.
    // Make sure that we don't specify the same transform for true and false...
    // If the user requests false, we blank out true and vice versa
    //
    List<IStream> targets = getTransformIOMeta().getTargetStreams();
    int index = targets.indexOf(stream);
    if (index == 0) {
      // True
      //
      TransformMeta falseTransform = targets.get(1).getTransformMeta();
      if (falseTransform != null && falseTransform.equals(stream.getTransformMeta())) {
        targets.get(1).setTransformMeta(null);
      }
    }
    if (index == 1) {
      // False
      //
      TransformMeta trueTransform = targets.get(0).getTransformMeta();
      if (trueTransform != null && trueTransform.equals(stream.getTransformMeta())) {
        targets.get(0).setTransformMeta(null);
      }
    }
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  /**
   * Get non-existing referenced input fields
   *
   * @param condition The condition to examine
   * @param prev The list of fields coming from previous transforms.
   * @return The list of orphaned fields
   */
  public List<String> getOrphanFields(Condition condition, IRowMeta prev) {
    List<String> orphans = new ArrayList<>();
    if (condition == null || prev == null) {
      return orphans;
    }
    String[] key = condition.getUsedFields();
    for (String s : key) {
      if (Utils.isEmpty(s)) {
        continue;
      }
      IValueMeta v = prev.searchValueMeta(s);
      if (v == null) {
        orphans.add(s);
      }
    }
    return orphans;
  }

  public String getTrueTransformName() {
    return getTargetTransformName(0);
  }

  public void setTrueTransformName(String trueTransformName) {
    getTransformIOMeta().getTargetStreams().get(0).setSubject(trueTransformName);
  }

  public String getFalseTransformName() {
    return getTargetTransformName(1);
  }

  public void setFalseTransformName(String falseTransformName) {
    getTransformIOMeta().getTargetStreams().get(1).setSubject(falseTransformName);
  }

  private String getTargetTransformName(int streamIndex) {
    IStream stream = getTransformIOMeta().getTargetStreams().get(streamIndex);
    return java.util.stream.Stream.of(stream.getTransformName(), stream.getSubject())
        .filter(Objects::nonNull)
        .findFirst()
        .map(Object::toString)
        .orElse(null);
  }

  public String getConditionXml() {
    String conditionXML = null;
    try {
      conditionXML = getCondition().getXml();
    } catch (HopValueException e) {
      log.logError(e.getMessage());
    }
    return conditionXML;
  }

  /**
   * @return Returns the condition.
   */
  public Condition getCondition() {
    return compare.condition;
  }

  /**
   * @param condition The condition to set.
   */
  public void setCondition(Condition condition) {
    this.compare.condition = condition;
  }

  /**
   * Gets compare
   *
   * @return value of compare
   */
  public FRCompare getCompare() {
    return compare;
  }

  /**
   * Sets compare
   *
   * @param compare value of compare
   */
  public void setCompare(FRCompare compare) {
    this.compare = compare;
  }

  public static final class FRCompare {
    @HopMetadataProperty(key = "condition")
    private Condition condition;

    public FRCompare() {
      condition = new Condition();
    }

    public FRCompare(FRCompare c) {
      this.condition = new Condition(c.condition);
    }

    public FRCompare(Condition condition) {
      this.condition = condition;
    }

    /**
     * Gets condition
     *
     * @return value of condition
     */
    public Condition getCondition() {
      return condition;
    }

    /**
     * Sets condition
     *
     * @param condition value of condition
     */
    public void setCondition(Condition condition) {
      this.condition = condition;
    }
  }

  public static final class ConditionXmlConverter implements IStringObjectConverter {
    @Override
    public String getString(Object object) throws HopException {
      if (!(object instanceof FRCompare)) {
        throw new HopException("We only support XML serialization of Condition objects here");
      }
      try {
        return ((FRCompare) object).getCondition().getXml();
      } catch (Exception e) {
        throw new HopException("Error serializing Condition to XML", e);
      }
    }

    @Override
    public Object getObject(String xml) throws HopException {
      try {
        return new FRCompare(new Condition(xml));
      } catch (Exception e) {
        throw new HopException("Error serializing Condition from XML", e);
      }
    }
  }
}
