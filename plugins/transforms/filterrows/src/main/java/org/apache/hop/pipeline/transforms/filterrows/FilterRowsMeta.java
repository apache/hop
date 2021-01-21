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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
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
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/*
 * Created on 02-jun-2003
 *
 */
@InjectionSupported(localizationPrefix = "FilterRowsMeta.Injection.")
@Transform(
    id = "FilterRows",
    image = "filterrows.svg",
    name = "i18n::BaseTransform.TypeLongDesc.FilterRows",
    description = "i18n::BaseTransform.TypeTooltipDesc.FilterRows",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/filterrows.html")
public class FilterRowsMeta extends BaseTransformMeta
    implements ITransformMeta<FilterRows, FilterRowsData> {
  private static final Class<?> PKG = FilterRowsMeta.class; // For Translator

  /**
   * This is the main condition for the complete filter.
   *
   * @since version 2.1
   */
  private Condition condition;

  public FilterRowsMeta() {
    super(); // allocate BaseTransformMeta
    condition = new Condition();
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  /** @return Returns the condition. */
  public Condition getCondition() {
    return condition;
  }

  /** @param condition The condition to set. */
  public void setCondition(Condition condition) {
    this.condition = condition;
  }

  public void allocate() {
    condition = new Condition();
  }

  public Object clone() {
    FilterRowsMeta retval = (FilterRowsMeta) super.clone();

    retval.setTrueTransformName(getTrueTransformName());
    retval.setFalseTransformName(getFalseTransformName());

    if (condition != null) {
      retval.condition = (Condition) condition.clone();
    } else {
      retval.condition = null;
    }

    return retval;
  }

  public String getXml() throws HopException {
    StringBuilder retval = new StringBuilder(200);

    retval.append(XmlHandler.addTagValue("send_true_to", getTrueTransformName()));
    retval.append(XmlHandler.addTagValue("send_false_to", getFalseTransformName()));
    retval.append("    <compare>").append(Const.CR);

    if (condition != null) {
      retval.append(condition.getXml());
    }

    retval.append("    </compare>").append(Const.CR);

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      setTrueTransformName(XmlHandler.getTagValue(transformNode, "send_true_to"));
      setFalseTransformName(XmlHandler.getTagValue(transformNode, "send_false_to"));

      Node compare = XmlHandler.getSubNode(transformNode, "compare");
      Node condnode = XmlHandler.getSubNode(compare, "condition");

      // The new situation...
      if (condnode != null) {
        condition = new Condition(condnode);
      } else {
        // Old style condition: Line1 OR Line2 OR Line3: @deprecated!
        condition = new Condition();

        int nrkeys = XmlHandler.countNodes(compare, "key");
        if (nrkeys == 1) {
          Node knode = XmlHandler.getSubNodeByNr(compare, "key", 0);

          String key = XmlHandler.getTagValue(knode, "name");
          String value = XmlHandler.getTagValue(knode, "value");
          String field = XmlHandler.getTagValue(knode, "field");
          String comparator = XmlHandler.getTagValue(knode, "condition");

          condition.setOperator(Condition.OPERATOR_NONE);
          condition.setLeftValuename(key);
          condition.setFunction(Condition.getFunction(comparator));
          condition.setRightValuename(field);
          condition.setRightExact(new ValueMetaAndData("value", value));
        } else {
          for (int i = 0; i < nrkeys; i++) {
            Node knode = XmlHandler.getSubNodeByNr(compare, "key", i);

            String key = XmlHandler.getTagValue(knode, "name");
            String value = XmlHandler.getTagValue(knode, "value");
            String field = XmlHandler.getTagValue(knode, "field");
            String comparator = XmlHandler.getTagValue(knode, "condition");

            Condition subc = new Condition();
            if (i > 0) {
              subc.setOperator(Condition.OPERATOR_OR);
            } else {
              subc.setOperator(Condition.OPERATOR_NONE);
            }
            subc.setLeftValuename(key);
            subc.setFunction(Condition.getFunction(comparator));
            subc.setRightValuename(field);
            subc.setRightExact(new ValueMetaAndData("value", value));

            condition.addCondition(subc);
          }
        }
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "FilterRowsMeta.Exception..UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    allocate();
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();
    for (IStream stream : targetStreams) {
      stream.setTransformMeta(
          TransformMeta.findTransform(transforms, (String) stream.getSubject()));
    }
  }

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
    String[] conditionField = condition.getUsedFields();
    for (int i = 0; i < conditionField.length; i++) {
      int idx = rowMeta.indexOfValue(conditionField[i]);
      if (idx >= 0) {
        IValueMeta valueMeta = rowMeta.getValueMeta(idx);
        valueMeta.setSortedDescending(false);
      }
    }
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

    checkTarget(transformMeta, "true", getTrueTransformName(), output).ifPresent(remarks::add);
    checkTarget(transformMeta, "false", getFalseTransformName(), output).ifPresent(remarks::add);

    if (condition.isEmpty()) {
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
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FilterRowsMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      List<String> orphanFields = getOrphanFields(condition, prev);
      if (orphanFields.size() > 0) {
        errorMessage =
            BaseMessages.getString(
                    PKG, "FilterRowsMeta.CheckResult.FieldsNotFoundFromPreviousTransform")
                + Const.CR;
        for (String field : orphanFields) {
          errorMessage += "\t\t" + field + Const.CR;
        }
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
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
      errorMessage =
          BaseMessages.getString(
                  PKG, "FilterRowsMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform")
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
                  PKG, "FilterRowsMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FilterRowsMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
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
                    "FilterRowsMeta.CheckResult.TargetTransformInvalid",
                    target,
                    targetTransformName),
                transformMeta));
      }
    }
    return Optional.empty();
  }

  public FilterRows createTransform(
      TransformMeta transformMeta,
      FilterRowsData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new FilterRows(transformMeta, this, data, cnr, tr, pipeline);
  }

  public FilterRowsData getTransformData() {
    return new FilterRowsData();
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
   * @param condition
   * @param prev
   * @return
   */
  public List<String> getOrphanFields(Condition condition, IRowMeta prev) {
    List<String> orphans = new ArrayList<>();
    if (condition == null || prev == null) {
      return orphans;
    }
    String[] key = condition.getUsedFields();
    for (int i = 0; i < key.length; i++) {
      if (Utils.isEmpty(key[i])) {
        continue;
      }
      IValueMeta v = prev.searchValueMeta(key[i]);
      if (v == null) {
        orphans.add(key[i]);
      }
    }
    return orphans;
  }

  public String getTrueTransformName() {
    return getTargetTransformName(0);
  }

  @Injection(name = "SEND_TRUE_TRANSFORM")
  public void setTrueTransformName(String trueTransformName) {
    getTransformIOMeta().getTargetStreams().get(0).setSubject(trueTransformName);
  }

  public String getFalseTransformName() {
    return getTargetTransformName(1);
  }

  @Injection(name = "SEND_FALSE_TRANSFORM")
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
      conditionXML = condition.getXml();
    } catch (HopValueException e) {
      log.logError(e.getMessage());
    }
    return conditionXML;
  }

  @Injection(name = "CONDITION")
  public void setConditionXml(String conditionXml) {
    try {
      this.condition = new Condition(conditionXml);
    } catch (HopXmlException e) {
      log.logError(e.getMessage());
    }
  }
}
