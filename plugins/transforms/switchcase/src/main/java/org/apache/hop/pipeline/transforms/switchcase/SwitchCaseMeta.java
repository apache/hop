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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.DataTypeConverter;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
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

/*
 * Created on 14-may-2008
 *
 */
@InjectionSupported(
    groups = {},
    localizationPrefix = "SwitchCaseMeta.Injection.")
@Transform(
    id = "SwitchCase",
    image = "switchcase.svg",
    name = "i18n::BaseTransform.TypeLongDesc.SwitchCase",
    description = "i18n::BaseTransform.TypeTooltipDesc.SwitchCase",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/switchcase.html")
public class SwitchCaseMeta extends BaseTransformMeta
    implements ITransformMeta<SwitchCase, SwitchCaseData> {
  private static final Class<?> PKG = SwitchCaseMeta.class; // For Translator

  private static final String XML_TAG_CASE_VALUES = "cases";
  private static final String XML_TAG_CASE_VALUE = "case";

  /** The field to switch over */
  @Injection(name = "FIELD_NAME")
  private String fieldname;

  /** The case value type to help parse numeric and date-time data */
  @Injection(name = "VALUE_TYPE", converter = DataTypeConverter.class)
  private int caseValueType;
  /** The case value format to help parse numeric and date-time data */
  @Injection(name = "VALUE_FORMAT")
  private String caseValueFormat;
  /** The decimal symbol to help parse numeric data */
  @Injection(name = "VALUE_DECIMAL")
  private String caseValueDecimal;
  /** The grouping symbol to help parse numeric data */
  @Injection(name = "VALUE_GROUP")
  private String caseValueGroup;

  /** The targets to switch over */
  @InjectionDeep(prefix = "SWITCH_CASE_TARGET")
  private List<SwitchCaseTarget> caseTargets;

  /** The default target transform name (only used during serialization) */
  @Injection(name = "DEFAULT_TARGET_TRANSFORM_NAME")
  private String defaultTargetTransformName;

  /** The default target transform */
  private TransformMeta defaultTargetTransform;

  /** True if the comparison is a String.contains instead of equals */
  @Injection(name = "CONTAINS")
  private boolean isContains;

  public SwitchCaseMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate() {
    caseTargets = new ArrayList<>();
  }

  public Object clone() {
    SwitchCaseMeta retval = (SwitchCaseMeta) super.clone();
    retval.allocate();
    try {
      for (int i = 0; i < caseTargets.size(); i++) {
        retval.caseTargets.add((SwitchCaseTarget) caseTargets.get(i).clone());
      }
      return retval;
    } catch (CloneNotSupportedException ex) {
      // I hate this design pattern, but most of the other implementations of
      // clone catch the exception and return null. So, I'm sticking with what is known
      // MB - PDI-15057
      return null;
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(200);

    retval.append(XmlHandler.addTagValue("fieldname", fieldname));
    retval.append(XmlHandler.addTagValue("use_contains", isContains));
    retval.append(
        XmlHandler.addTagValue("case_value_type", ValueMetaBase.getTypeDesc(caseValueType)));
    retval.append(XmlHandler.addTagValue("case_value_format", caseValueFormat));
    retval.append(XmlHandler.addTagValue("case_value_decimal", caseValueDecimal));
    retval.append(XmlHandler.addTagValue("case_value_group", caseValueGroup));
    retval.append(
        XmlHandler.addTagValue(
            "default_target_transform",
            defaultTargetTransform != null
                ? defaultTargetTransform.getName()
                : defaultTargetTransformName));

    retval.append(XmlHandler.openTag(XML_TAG_CASE_VALUES));
    for (SwitchCaseTarget target : caseTargets) {
      retval.append(XmlHandler.openTag(XML_TAG_CASE_VALUE));
      retval.append(
          XmlHandler.addTagValue("value", target.caseValue != null ? target.caseValue : ""));
      retval.append(
          XmlHandler.addTagValue(
              "target_transform",
              target.caseTargetTransform != null
                  ? target.caseTargetTransform.getName()
                  : target.caseTargetTransformName));
      retval.append(XmlHandler.closeTag(XML_TAG_CASE_VALUE));
    }
    retval.append(XmlHandler.closeTag(XML_TAG_CASE_VALUES));

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      fieldname = XmlHandler.getTagValue(transformNode, "fieldname");
      isContains = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "use_contains"));
      caseValueType =
          ValueMetaBase.getType(XmlHandler.getTagValue(transformNode, "case_value_type"));
      caseValueFormat = XmlHandler.getTagValue(transformNode, "case_value_format");
      caseValueDecimal = XmlHandler.getTagValue(transformNode, "case_value_decimal");
      caseValueGroup = XmlHandler.getTagValue(transformNode, "case_value_group");

      defaultTargetTransformName =
          XmlHandler.getTagValue(transformNode, "default_target_transform");

      Node casesNode = XmlHandler.getSubNode(transformNode, XML_TAG_CASE_VALUES);
      int nrCases = XmlHandler.countNodes(casesNode, XML_TAG_CASE_VALUE);
      allocate();
      for (int i = 0; i < nrCases; i++) {
        Node caseNode = XmlHandler.getSubNodeByNr(casesNode, XML_TAG_CASE_VALUE, i);
        SwitchCaseTarget target = new SwitchCaseTarget();
        target.caseValue = XmlHandler.getTagValue(caseNode, "value");
        target.caseTargetTransformName = XmlHandler.getTagValue(caseNode, "target_transform");
        caseTargets.add(target);
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "SwitchCaseMeta.Exception..UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    allocate();
  }

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

    ITransformIOMeta ioMeta = getTransformIOMeta();
    List<IStream> targetStreams = ioMeta.getTargetStreams();
    for (IStream stream : targetStreams) {
      SwitchCaseTarget target = (SwitchCaseTarget) stream.getSubject();

      if (target != null && target.caseTargetTransform == null) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "SwitchCaseMeta.CheckResult.TargetTransformInvalid",
                    "false",
                    target.caseTargetTransformName),
                transformMeta);
        remarks.add(cr);
      }
    }

    if (Utils.isEmpty(fieldname)) {
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

  public SwitchCaseData getTransformData() {
    return new SwitchCaseData();
  }

  /** @return the fieldname */
  public String getFieldname() {
    return fieldname;
  }

  /** @param fieldname the fieldname to set */
  public void setFieldname(String fieldname) {
    this.fieldname = fieldname;
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
  public int getCaseValueType() {
    return caseValueType;
  }

  /** @param caseValueType the caseValueType to set */
  public void setCaseValueType(int caseValueType) {
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

  /** @return the defaultTargetTransform */
  public TransformMeta getDefaultTargetTransform() {
    return defaultTargetTransform;
  }

  /** @param defaultTargetTransform the defaultTargetTransform to set */
  public void setDefaultTargetTransform(TransformMeta defaultTargetTransform) {
    this.defaultTargetTransform = defaultTargetTransform;
  }

  public boolean isContains() {
    return isContains;
  }

  public void setContains(boolean isContains) {
    this.isContains = isContains;
  }

  /** Returns the Input/Output metadata for this transform. */
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
                target.caseTargetTransform,
                BaseMessages.getString(
                    PKG,
                    "SwitchCaseMeta.TargetStream.CaseTarget.Description",
                    Const.NVL(target.caseValue, "")),
                StreamIcon.TARGET,
                target);
        ioMeta.addStream(stream);
      }

      // Add the default target transform as a stream
      //
      if (getDefaultTargetTransform() != null) {
        ioMeta.addStream(
            new Stream(
                StreamType.TARGET,
                getDefaultTargetTransform(),
                BaseMessages.getString(PKG, "SwitchCaseMeta.TargetStream.Default.Description"),
                StreamIcon.TARGET,
                null));
      }
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();
    for (IStream stream : targetStreams) {
      SwitchCaseTarget target = (SwitchCaseTarget) stream.getSubject();
      if (target != null) {
        TransformMeta transformMeta =
            TransformMeta.findTransform(transforms, target.caseTargetTransformName);
        target.caseTargetTransform = transformMeta;
      }
    }
    defaultTargetTransform = TransformMeta.findTransform(transforms, defaultTargetTransformName);
    resetTransformIoMeta();
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

  public List<IStream> getOptionalStreams() {
    List<IStream> list = new ArrayList<>();

    if (getDefaultTargetTransform() == null) {
      list.add(newDefaultStream);
    }
    list.add(newCaseTargetStream);

    return list;
  }

  public void handleStreamSelection(IStream stream) {
    if (stream == newDefaultStream) {
      setDefaultTargetTransform(stream.getTransformMeta());
    }

    if (stream == newCaseTargetStream) {
      // Add the target..
      //
      SwitchCaseTarget target = new SwitchCaseTarget();
      target.caseTargetTransform = stream.getTransformMeta();
      target.caseValue = stream.getTransformMeta().getName();
      caseTargets.add(target);
    }

    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();
    for (int i = 0; i < targetStreams.size(); i++) {
      if (stream == targetStreams.get(i)) {
        SwitchCaseTarget target = (SwitchCaseTarget) stream.getSubject();
        if (target == null) { // Default!
          setDefaultTargetTransform(stream.getTransformMeta());
        } else {
          target.caseTargetTransform = stream.getTransformMeta();
        }
      }
    }

    resetTransformIoMeta(); // force transformIo to be recreated when it is next needed.
  }

  /** @return the caseTargets */
  public List<SwitchCaseTarget> getCaseTargets() {
    return caseTargets;
  }

  /** @param caseTargets the caseTargets to set */
  public void setCaseTargets(List<SwitchCaseTarget> caseTargets) {
    this.caseTargets = caseTargets;
  }

  /**
   * This method is added to exclude certain transforms from copy/distribute checking.
   *
   * @since 4.0.0
   */
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
