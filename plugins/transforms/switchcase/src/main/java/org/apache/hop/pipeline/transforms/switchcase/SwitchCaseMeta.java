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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "SwitchCase",
    image = "switchcase.svg",
    name = "i18n::BaseTransform.TypeLongDesc.SwitchCase",
    description = "i18n::BaseTransform.TypeTooltipDesc.SwitchCase",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    keywords = "i18n::SwitchCaseMeta.keyword",
    documentationUrl = "/pipeline/transforms/switchcase.html")
public class SwitchCaseMeta extends BaseTransformMeta
    implements ITransformMeta<SwitchCase, SwitchCaseData> {
  private static final Class<?> PKG = SwitchCaseMeta.class; // For Translator

  /** The field to switch over */
  @HopMetadataProperty(
      key = "fieldname",
      injectionKey = "FIELD_NAME",
      injectionKeyDescription = "SwitchCaseMeta.Injection.FIELD_NAME")
  private String fieldName;

  /** The case value type to help parse numeric and date-time data */
  @HopMetadataProperty(
      key = "case_value_type",
      injectionKey = "VALUE_TYPE",
      injectionKeyDescription = "SwitchCaseMeta.Injection.VALUE_TYPE")
  private String caseValueType;

  /** The case value format to help parse numeric and date-time data */
  @HopMetadataProperty(
      key = "case_value_format",
      injectionKey = "VALUE_FORMAT",
      injectionKeyDescription = "SwitchCaseMeta.Injection.VALUE_FORMAT")
  private String caseValueFormat;

  /** The decimal symbol to help parse numeric data */
  @HopMetadataProperty(
      key = "case_value_decimal",
      injectionKey = "VALUE_DECIMAL",
      injectionKeyDescription = "SwitchCaseMeta.Injection.VALUE_DECIMAL")
  private String caseValueDecimal;

  /** The grouping symbol to help parse numeric data */
  @HopMetadataProperty(
      key = "case_value_group",
      injectionKey = "VALUE_GROUP",
      injectionKeyDescription = "SwitchCaseMeta.Injection.VALUE_GROUP")
  private String caseValueGroup;

  /** The targets to switch over */
  @HopMetadataProperty(groupKey = "cases", key = "case")
  private List<SwitchCaseTarget> caseTargets;

  /** The default target transform name (only used during serialization) */
  @HopMetadataProperty(
      key = "default_target_transform",
      injectionKey = "DEFAULT_TARGET_TRANSFORM_NAME",
      injectionKeyDescription = "SwitchCaseMeta.Injection.DEFAULT_TARGET_TRANSFORM_NAME")
  private String defaultTargetTransformName;

  /** True if the comparison is a String.contains instead of equals */
  @HopMetadataProperty(
      key = "use_contains",
      injectionKey = "CONTAINS",
      injectionKeyDescription = "SwitchCaseMeta.Injection.CONTAINS")
  private boolean usingContains;

  public SwitchCaseMeta() {
    caseTargets = new ArrayList<>();
  }

  public SwitchCaseMeta(SwitchCaseMeta m) {
    this();
    this.fieldName = m.fieldName;
    this.caseValueType = m.caseValueType;
    this.caseValueFormat = m.caseValueFormat;
    this.caseValueDecimal = m.caseValueDecimal;
    this.caseValueGroup = m.caseValueGroup;
    this.defaultTargetTransformName = m.defaultTargetTransformName;
    this.usingContains = m.usingContains;
    for (SwitchCaseTarget target : this.caseTargets) {
      this.caseTargets.add(new SwitchCaseTarget(target));
    }
  }

  @Override
  public SwitchCaseMeta clone() {
    return new SwitchCaseMeta(this);
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
    // Default: nothing changes to rowMeta
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

    for (SwitchCaseTarget target : caseTargets) {
      TransformMeta check = pipelineMeta.findTransform(target.getCaseTargetTransformName());
      if (check == null) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "SwitchCaseMeta.CheckResult.TargetTransformInvalid",
                    "false",
                    target.getCaseTargetTransformName()),
                transformMeta);
        remarks.add(cr);
      }
    }

    if (Utils.isEmpty(fieldName)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SwitchCaseMeta.CheckResult.NoFieldSpecified"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SwitchCaseMeta.CheckResult.FieldSpecified"),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SwitchCaseMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "SwitchCaseMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public SwitchCaseData getTransformData() {
    return new SwitchCaseData();
  }

  /** @return the fieldname */
  public String getFieldName() {
    return fieldName;
  }

  /** @param fieldName the fieldname to set */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /** @return the caseValueFormat */
  public String getCaseValueFormat() {
    return caseValueFormat;
  }

  /** @param caseValueFormat the caseValueFormat to set */
  public void setCaseValueFormat(String caseValueFormat) {
    this.caseValueFormat = caseValueFormat;
  }

  /** @return the caseValueDecimal */
  public String getCaseValueDecimal() {
    return caseValueDecimal;
  }

  /** @param caseValueDecimal the caseValueDecimal to set */
  public void setCaseValueDecimal(String caseValueDecimal) {
    this.caseValueDecimal = caseValueDecimal;
  }

  /** @return the caseValueGroup */
  public String getCaseValueGroup() {
    return caseValueGroup;
  }

  /** @param caseValueGroup the caseValueGroup to set */
  public void setCaseValueGroup(String caseValueGroup) {
    this.caseValueGroup = caseValueGroup;
  }

  /** @return the caseValueType */
  public String getCaseValueType() {
    return caseValueType;
  }

  /** @param caseValueType the caseValueType to set */
  public void setCaseValueType(String caseValueType) {
    this.caseValueType = caseValueType;
  }

  /** @return the defaultTargetTransformName */
  public String getDefaultTargetTransformName() {
    return defaultTargetTransformName;
  }

  /** @param defaultTargetTransformName the defaultTargetTransformName to set */
  public void setDefaultTargetTransformName(String defaultTargetTransformName) {
    this.defaultTargetTransformName = defaultTargetTransformName;
  }

  public boolean isUsingContains() {
    return usingContains;
  }

  public void setUsingContains(boolean isContains) {
    this.usingContains = isContains;
  }

  /** Returns the Input/Output metadata for this transform. */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, false, false, false, false, true);

      // Add the targets...
      //
      for (SwitchCaseTarget target : caseTargets) {
        IStream stream =
            new Stream(
                StreamType.TARGET,
                null,
                BaseMessages.getString(
                    PKG,
                    "SwitchCaseMeta.TargetStream.CaseTarget.Description",
                    Const.NVL(target.getCaseValue(), "")),
                StreamIcon.TARGET,
                target.getCaseTargetTransformName());
        ioMeta.addStream(stream);
      }

      // Add the default target transform as a stream
      //
      if (StringUtils.isNotEmpty(defaultTargetTransformName)) {
        ioMeta.addStream(
            new Stream(
                StreamType.TARGET,
                null,
                BaseMessages.getString(PKG, "SwitchCaseMeta.TargetStream.Default.Description"),
                StreamIcon.TARGET,
                defaultTargetTransformName));
      }
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();
    int index = 0;
    for (SwitchCaseTarget target : caseTargets) {
      IStream stream = targetStreams.get(index++);

      TransformMeta transformMeta =
          TransformMeta.findTransform(transforms, target.getCaseTargetTransformName());
      stream.setTransformMeta(transformMeta);
    }
    // Extra one is the default target (if any)...
    //
    if (StringUtils.isNotEmpty(defaultTargetTransformName)) {
      IStream stream = targetStreams.get(index);
      TransformMeta transformMeta =
          TransformMeta.findTransform(transforms, defaultTargetTransformName);
      stream.setTransformMeta(transformMeta);
    }
  }

  @Override
  public void convertIOMetaToTransformNames() {
    // TODO
  }

  private static IStream newDefaultStream =
      new Stream(
          StreamType.TARGET,
          null,
          BaseMessages.getString(PKG, "SwitchCaseMeta.TargetStream.Default.Description"),
          StreamIcon.TARGET,
          null);
  private static IStream newCaseTargetStream =
      new Stream(
          StreamType.TARGET,
          null,
          BaseMessages.getString(PKG, "SwitchCaseMeta.TargetStream.NewCaseTarget.Description"),
          StreamIcon.TARGET,
          null);

  @Override
  public List<IStream> getOptionalStreams() {
    List<IStream> list = new ArrayList<>();

    if (StringUtils.isEmpty(defaultTargetTransformName)) {
      list.add(newDefaultStream);
    }
    list.add(newCaseTargetStream);

    return list;
  }

  @Override
  public void handleStreamSelection(IStream stream) {
    if (stream == newDefaultStream) {
      defaultTargetTransformName = stream.getTransformMeta().getName();

      IStream newStream =
          new Stream(
              StreamType.TARGET,
              stream.getTransformMeta(),
              BaseMessages.getString(PKG, "SwitchCaseMeta.TargetStream.Default.Description"),
              StreamIcon.TARGET,
              stream.getTransformMeta().getName());
      getTransformIOMeta().addStream(newStream);
    } else if (stream == newCaseTargetStream) {
      // Add the target..
      //
      SwitchCaseTarget target = new SwitchCaseTarget();
      target.setCaseTargetTransformName(stream.getTransformMeta().getName());
      target.setCaseValue(stream.getTransformMeta().getName());
      caseTargets.add(target);
      IStream newStream =
          new Stream(
              StreamType.TARGET,
              stream.getTransformMeta(),
              BaseMessages.getString(
                  PKG,
                  "SwitchCaseMeta.TargetStream.CaseTarget.Description",
                  Const.NVL(target.getCaseValue(), "")),
              StreamIcon.TARGET,
              stream.getTransformMeta().getName());
      getTransformIOMeta().addStream(newStream);
    } else {
      // A target was selected...
      //
      List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();
      for (int i = 0; i < targetStreams.size(); i++) {
        if (stream == targetStreams.get(i)) {
          if (i < caseTargets.size()) {
            caseTargets.get(i).setCaseTargetTransformName(stream.getTransformMeta().getName());
          } else {
            defaultTargetTransformName = stream.getTransformMeta().getName();
          }
        }
      }
    }
  }

  /** @return the caseTargets */
  public List<SwitchCaseTarget> getCaseTargets() {
    return caseTargets;
  }

  /** @param caseTargets the caseTargets to set */
  public void setCaseTargets(List<SwitchCaseTarget> caseTargets) {
    this.caseTargets = caseTargets;
  }

  /** This method is added to exclude certain transforms from copy/distribute checking. */
  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  @Override
  public SwitchCase createTransform(
      TransformMeta transformMeta,
      SwitchCaseData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SwitchCase(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }
}
