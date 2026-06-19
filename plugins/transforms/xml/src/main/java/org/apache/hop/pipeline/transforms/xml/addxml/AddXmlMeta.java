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

package org.apache.hop.pipeline.transforms.xml.addxml;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** This class knows how to handle the MetaData for the XML output transform */
@Transform(
    id = "AddXML",
    image = "add_xml.svg",
    name = "i18n::AddXML.name",
    description = "i18n::AddXML.description",
    categoryDescription = "i18n::AddXML.category",
    keywords = "i18n::AddXmlMeta.keyword",
    documentationUrl = "/pipeline/transforms/addxml.html")
@Getter
@Setter
public class AddXmlMeta extends BaseTransformMeta<AddXml, AddXmlData> {
  private static final Class<?> PKG = AddXmlMeta.class;
  public static final String CONST_FIELD = "field";
  public static final String CONST_SPACES = "        ";

  /** The base name of the output file */
  @Getter
  @Setter
  public static class OmitDetails {
    /** Flag: omit the XML Header */
    @HopMetadataProperty(
        key = "omitXMLheader",
        injectionKey = "OMIT_XML_HEADER",
        injectionKeyDescription = "AddXMLMeta.Injection.OMIT_XML_HEADER")
    private boolean omittingXmlHeader;

    /** Flag: omit null elements from the xml result */
    @HopMetadataProperty(
        key = "omitNullValues",
        injectionKey = "OMIT_NULL_VALUES",
        injectionKeyDescription = "AddXMLMeta.Injection.OMIT_NULL_VALUES")
    private boolean omittingNullValues;

    public OmitDetails() {
      omittingXmlHeader = true;
    }

    public OmitDetails(OmitDetails o) {
      this();
      this.omittingNullValues = o.omittingNullValues;
      this.omittingXmlHeader = o.omittingXmlHeader;
    }
  }

  @HopMetadataProperty(key = "file")
  private OmitDetails omitDetails;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "ENCODING",
      injectionKeyDescription = "AddXMLMeta.Injection.ENCODING")
  private String encoding;

  /** The name value containing the resulting XML fragment */
  @HopMetadataProperty(
      key = "valueName",
      injectionKey = "VALUE_NAME",
      injectionKeyDescription = "AddXMLMeta.Injection.VALUE_NAME")
  private String valueName;

  /** The name of the repeating row XML element */
  @HopMetadataProperty(
      key = "xml_repeat_element",
      injectionKey = "ROOT_NODE",
      injectionKeyDescription = "AddXMLMeta.Injection.ROOT_NODE")
  private String rootNode;

  /** The output fields */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "OUTPUT_FIELDS",
      injectionGroupDescription = "AddXMLMeta.Injection.OUTPUT_FIELDS")
  private List<XmlField> outputFields;

  public AddXmlMeta() {
    super();
    encoding = Const.UTF_8;
    valueName = "xmlvaluename";
    rootNode = "Row";
    omitDetails = new OmitDetails();
    outputFields = new ArrayList<>();
  }

  public AddXmlMeta(AddXmlMeta m) {
    this();
    this.encoding = m.encoding;
    this.omitDetails = new OmitDetails(m.omitDetails);
    this.rootNode = m.rootNode;
    this.valueName = m.valueName;
    m.outputFields.forEach(f -> outputFields.add(new XmlField(f)));
  }

  @Override
  public Object clone() {
    return new AddXmlMeta(this);
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

    IValueMeta v = new ValueMetaString(this.getValueName());
    v.setOrigin(name);
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

    // Check output fields
    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "AddXMLMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (XmlField outputField : outputFields) {
        int idx = prev.indexOfValue(outputField.getFieldName());
        if (idx < 0) {
          errorMessage += "\t\t" + outputField.getFieldName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "AddXMLMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "AddXMLMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "AddXMLMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AddXMLMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "AddXMLMeta.CheckResult.FilesNotChecked"),
            transformMeta);
    remarks.add(cr);
  }
}
