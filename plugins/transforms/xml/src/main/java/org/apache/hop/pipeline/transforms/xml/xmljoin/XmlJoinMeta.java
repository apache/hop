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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

/**
 * This class knows how to handle the MetaData for the XML join transform
 *
 * @since 30-04-2008
 */
@Transform(
    id = "XMLJoin",
    image = "XJN.svg",
    name = "i18n::XmlJoin.name",
    description = "i18n::XmlJoin.description",
    categoryDescription = "i18n::XmlJoin.category",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/xmljoin.html")
@InjectionSupported(localizationPrefix = "XmlJoin.Injection.")
public class XmlJoinMeta extends BaseTransformMeta implements ITransformMeta<XmlJoin, XmlJoinData> {
  private static final Class<?> PKG = XmlJoinMeta.class; // For Translator

  /** The base name of the output file */

  /** Flag: execute complex join */
  @Injection(name = "COMPLEX_JOIN")
  private boolean complexJoin;

  /** What transform holds the xml string to join into */
  @Injection(name = "TARGET_XML_TRANSFORM")
  private String targetXmlTransform;

  /** What field holds the xml string to join into */
  @Injection(name = "TARGET_XML_FIELD")
  private String targetXmlField;

  /** What field holds the XML tags to join */
  @Injection(name = "SOURCE_XML_FIELD")
  private String sourceXmlField;

  /** The name value containing the resulting XML fragment */
  @Injection(name = "VALUE_XML_FIELD")
  private String valueXmlField;

  /** The name of the repeating row XML element */
  @Injection(name = "TARGET_XPATH")
  private String targetXPath;

  /** What transform holds the xml strings to join */
  @Injection(name = "SOURCE_XML_TRANSFORM")
  private String sourceXmlTransform;

  /** What field holds the join compare value */
  @Injection(name = "JOIN_COMPARE_FIELD")
  private String joinCompareField;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @Injection(name = "ENCODING")
  private String encoding;

  /** Flag: execute complex join */
  @Injection(name = "OMIT_XML_HEADER")
  private boolean omitXmlHeader;

  /** Flag: omit null values from result xml */
  @Injection(name = "OMIT_NULL_VALUES")
  private boolean omitNullValues;

  public XmlJoinMeta() {
    super(); // allocate BaseTransformMeta
  }

  public Object clone() {
    XmlJoinMeta retval = (XmlJoinMeta) super.clone();
    return retval;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {

      List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
      infoStreams.get(0).setSubject(XmlHandler.getTagValue(transformNode, "targetXmlTransform"));
      infoStreams.get(1).setSubject(XmlHandler.getTagValue(transformNode, "sourceXmlTransform"));

      valueXmlField = XmlHandler.getTagValue(transformNode, "valueXmlField");
      targetXmlField = XmlHandler.getTagValue(transformNode, "targetXmlField");
      sourceXmlField = XmlHandler.getTagValue(transformNode, "sourceXmlField");
      targetXPath = XmlHandler.getTagValue(transformNode, "targetXPath");
      joinCompareField = XmlHandler.getTagValue(transformNode, "joinCompareField");
      encoding = XmlHandler.getTagValue(transformNode, "encoding");
      complexJoin = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "complexJoin"));
      omitXmlHeader = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "omitXMLHeader"));
      omitNullValues =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "omitNullValues"));

    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public void setDefault() {
    // complexJoin = false;
    encoding = Const.XML_ENCODING;
  }

  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    IValueMeta v = new ValueMetaString(this.getValueXmlField());
    v.setOrigin(name);

    try {
      // Row should only include fields from the target and not the source. During the preview table
      // generation
      // the fields from all previous transforms (source and target) are included in the row so lets
      // remove the
      // source fields.
      List<String> targetFieldNames = null;
      IRowMeta targetRowMeta = info[0];
      if (targetRowMeta != null) {
        targetFieldNames = Arrays.asList(targetRowMeta.getFieldNames());
      }
      IRowMeta sourceRowMeta = info[1];
      if (sourceRowMeta != null) {
        for (String fieldName : sourceRowMeta.getFieldNames()) {
          if (targetFieldNames == null || !targetFieldNames.contains(fieldName)) {
            row.removeValueMeta(fieldName);
          }
        }
      }
    } catch (HopValueException e) {
      // Pass
    }

    row.addValueMeta(v);
  }

  public String getXml() {
    StringBuffer xml = new StringBuffer(500);

    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    xml.append(XmlHandler.addTagValue("targetXmlTransform", infoStreams.get(0).getTransformName()));
    xml.append(XmlHandler.addTagValue("sourceXmlTransform", infoStreams.get(1).getTransformName()));

    xml.append("    ").append(XmlHandler.addTagValue("valueXmlField", valueXmlField));
    xml.append("    ").append(XmlHandler.addTagValue("targetXmlField", targetXmlField));
    xml.append("    ").append(XmlHandler.addTagValue("sourceXmlField", sourceXmlField));
    xml.append("    ").append(XmlHandler.addTagValue("complexJoin", complexJoin));
    xml.append("    ").append(XmlHandler.addTagValue("joinCompareField", joinCompareField));
    xml.append("    ").append(XmlHandler.addTagValue("targetXPath", targetXPath));
    xml.append("    ").append(XmlHandler.addTagValue("encoding", encoding));
    xml.append("    ").append(XmlHandler.addTagValue("omitXMLHeader", omitXmlHeader));
    xml.append("    ").append(XmlHandler.addTagValue("omitNullValues", omitNullValues));

    return xml.toString();
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
    // checks for empty field which are required
    if (this.targetXmlTransform == null || this.targetXmlTransform.length() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXMLTransformNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXMLTransformSpecified"),
              transformMeta);
      remarks.add(cr);
    }
    if (this.targetXmlField == null || this.targetXmlField.length() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXMLFieldNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXMLFieldSpecified"),
              transformMeta);
      remarks.add(cr);
    }
    if (this.sourceXmlTransform == null || this.sourceXmlTransform.length() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.SourceXMLTransformNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.SourceXMLTransformSpecified"),
              transformMeta);
      remarks.add(cr);
    }
    if (this.sourceXmlField == null || this.sourceXmlField.length() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.SourceXMLFieldNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.SourceXMLFieldSpecified"),
              transformMeta);
      remarks.add(cr);
    }
    if (this.valueXmlField == null || this.valueXmlField.length() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.ResultFieldNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.ResultFieldSpecified"),
              transformMeta);
      remarks.add(cr);
    }
    if (this.targetXPath == null || this.targetXPath.length() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXPathNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXPathSpecified"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have the right input streams leading to this transform!
    if (input.length > 0) {
      boolean targetTransformFound = false;
      boolean sourceTransformFound = false;
      for (int i = 0; i < input.length; i++) {
        if (this.targetXmlTransform != null && this.targetXmlTransform.equals(input[i])) {
          targetTransformFound = true;
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "XmlJoin.CheckResult.TargetXMLTransformFound", this.targetXmlTransform),
                  transformMeta);
          remarks.add(cr);
        }
        if (this.sourceXmlTransform != null && this.sourceXmlTransform.equals(input[i])) {
          sourceTransformFound = true;
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "XmlJoin.CheckResult.SourceXMLTransformFound", this.sourceXmlTransform),
                  transformMeta);
          remarks.add(cr);
        }
      }

      if (!targetTransformFound) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "XmlJoin.CheckResult.TargetXMLTransformNotFound", this.targetXmlTransform),
                transformMeta);
        remarks.add(cr);
      }
      if (!sourceTransformFound) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "XmlJoin.CheckResult.SourceXMLTransformNotFound", this.sourceXmlTransform),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces
   * output, does not accept input!
   */
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, true, false, false, false, false);

      ioMeta.addStream(
          new Stream(
              IStream.StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "XmlJoinMeta.InfoStream.TargetInputStream.Description"),
              StreamIcon.INFO,
              null));
      ioMeta.addStream(
          new Stream(
              IStream.StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "XmlJoinMeta.InfoStream.SourceInputStream.Description"),
              StreamIcon.INFO,
              null));
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    for (IStream stream : infoStreams) {
      stream.setTransformMeta(
          TransformMeta.findTransform(transforms, (String) stream.getSubject()));
    }
  }

  public void resetTransformIoMeta() {
    // Don't reset!
  }

  public boolean isComplexJoin() {
    return complexJoin;
  }

  public void setComplexJoin(boolean complexJoin) {
    this.complexJoin = complexJoin;
  }

  /**
   * Gets targetXmlTransform
   *
   * @return value of targetXmlTransform
   */
  public String getTargetXmlTransform() {
    return targetXmlTransform;
  }

  /** @param targetXmlTransform The targetXmlTransform to set */
  public void setTargetXmlTransform(String targetXmlTransform) {
    this.targetXmlTransform = targetXmlTransform;
  }

  public String getTargetXmlField() {
    return targetXmlField;
  }

  public void setTargetXmlField(String targetXMLfield) {
    this.targetXmlField = targetXMLfield;
  }

  /**
   * Gets sourceXmlTransform
   *
   * @return value of sourceXmlTransform
   */
  public String getSourceXmlTransform() {
    return sourceXmlTransform;
  }

  /** @param sourceXmlTransform The sourceXmlTransform to set */
  public void setSourceXmlTransform(String sourceXmlTransform) {
    this.sourceXmlTransform = sourceXmlTransform;
  }

  public String getSourceXmlField() {
    return sourceXmlField;
  }

  public void setSourceXmlField(String sourceXMLfield) {
    this.sourceXmlField = sourceXMLfield;
  }

  public String getValueXmlField() {
    return valueXmlField;
  }

  public void setValueXmlField(String valueXMLfield) {
    this.valueXmlField = valueXMLfield;
  }

  public String getTargetXPath() {
    return targetXPath;
  }

  public void setTargetXPath(String targetXPath) {
    this.targetXPath = targetXPath;
  }

  public String getJoinCompareField() {
    return joinCompareField;
  }

  public void setJoinCompareField(String joinCompareField) {
    this.joinCompareField = joinCompareField;
  }

  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      XmlJoinData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new XmlJoin(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public XmlJoinData getTransformData() {
    return new XmlJoinData();
  }

  /**
   * Gets omitXmlHeader
   *
   * @return value of omitXmlHeader
   */
  public boolean isOmitXmlHeader() {
    return omitXmlHeader;
  }

  /** @param omitXmlHeader The omitXmlHeader to set */
  public void setOmitXmlHeader(boolean omitXmlHeader) {
    this.omitXmlHeader = omitXmlHeader;
  }

  public void setOmitNullValues(boolean omitNullValues) {

    this.omitNullValues = omitNullValues;
  }

  public boolean isOmitNullValues() {
    return omitNullValues;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }
}
