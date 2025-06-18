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

import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
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
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

/** This class knows how to handle the MetaData for the XML join transform */
@Getter
@Setter
@Transform(
    id = "XMLJoin",
    image = "XJN.svg",
    name = "i18n::XmlJoin.name",
    description = "i18n::XmlJoin.description",
    categoryDescription = "i18n::XmlJoin.category",
    keywords = "i18n::XmlJoinMeta.keyword",
    documentationUrl = "/pipeline/transforms/xmljoin.html")
public class XmlJoinMeta extends BaseTransformMeta<XmlJoin, XmlJoinData> {
  private static final Class<?> PKG = XmlJoinMeta.class;

  /** The base name of the output file */

  /** Flag: execute complex join */
  @HopMetadataProperty(injectionKey = "COMPLEX_JOIN")
  private boolean complexJoin;

  /** What transform holds the xml string to join into */
  @HopMetadataProperty(injectionKey = "TARGET_XML_TRANSFORM")
  private String targetXmlTransform;

  /** What field holds the xml string to join into */
  @HopMetadataProperty(injectionKey = "TARGET_XML_FIELD")
  private String targetXmlField;

  /** What field holds the XML tags to join */
  @HopMetadataProperty(injectionKey = "SOURCE_XML_FIELD")
  private String sourceXmlField;

  /** The name value containing the resulting XML fragment */
  @HopMetadataProperty(injectionKey = "VALUE_XML_FIELD")
  private String valueXmlField;

  /** The name of the repeating row XML element */
  @HopMetadataProperty(injectionKey = "TARGET_XPATH")
  private String targetXPath;

  /** What transform holds the xml strings to join */
  @HopMetadataProperty(injectionKey = "SOURCE_XML_TRANSFORM")
  private String sourceXmlTransform;

  /** What field holds the join compare value */
  @HopMetadataProperty(injectionKey = "JOIN_COMPARE_FIELD")
  private String joinCompareField;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(injectionKey = "ENCODING")
  private String encoding;

  /** Flag: execute complex join */
  @HopMetadataProperty(injectionKey = "OMIT_XML_HEADER")
  private boolean omitXmlHeader;

  /** Flag: omit null values from result xml */
  @HopMetadataProperty(injectionKey = "OMIT_NULL_VALUES")
  private boolean omitNullValues;

  public XmlJoinMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public Object clone() {
    XmlJoinMeta retval = (XmlJoinMeta) super.clone();
    return retval;
  }


  @Override
  public void setDefault() {
    encoding = Const.XML_ENCODING;
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

    IValueMeta v = new ValueMetaString(variables.resolve(this.getValueXmlField()));
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
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "XmlJoin.CheckResult.TargetXMLTransformFound", this.targetXmlTransform),
                  transformMeta);
          remarks.add(cr);
        }
        if (this.sourceXmlTransform != null && this.sourceXmlTransform.equals(input[i])) {
          sourceTransformFound = true;
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
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
  @Override
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

  @Override
  public void resetTransformIoMeta() {
    // Don't reset!
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }
}
