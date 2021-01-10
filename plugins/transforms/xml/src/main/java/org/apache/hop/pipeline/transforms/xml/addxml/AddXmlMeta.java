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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
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
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This class knows how to handle the MetaData for the XML output transform
 *
 * @since 14-jan-2006
 */
@Transform(
    id = "AddXML",
    image = "add_xml.svg",
    name = "i18n::AddXML.name",
    description = "i18n::AddXML.description",
    categoryDescription = "i18n::AddXML.category",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/addxml.html")
@InjectionSupported(
    localizationPrefix = "AddXMLMeta.Injection.",
    groups = {"OUTPUT_FIELDS"})
public class AddXmlMeta extends BaseTransformMeta implements ITransformMeta<AddXml, AddXmlData> {
  private static final Class<?> PKG = AddXmlMeta.class; // For Translator

  /** The base name of the output file */

  /** Flag: ommit the XML Header */
  @Injection(name = "OMIT_XML_HEADER")
  private boolean omitXMLheader;

  /** Flag: omit null elements from the xml result */
  @Injection(name = "OMIT_NULL_VALUES")
  private boolean omitNullValues;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @Injection(name = "ENCODING")
  private String encoding;

  /** The name value containing the resulting XML fragment */
  @Injection(name = "VALUE_NAME")
  private String valueName;

  /** The name of the repeating row XML element */
  @Injection(name = "ROOT_NODE")
  private String rootNode;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  @InjectionDeep private XmlField[] outputFields;

  public AddXmlMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the zipped. */
  public boolean isOmitXMLheader() {
    return omitXMLheader;
  }

  /** @param omitXMLheader The omit XML header flag to set. */
  public void setOmitXMLheader(boolean omitXMLheader) {
    this.omitXMLheader = omitXMLheader;
  }

  public void setOmitNullValues(boolean omitNullValues) {

    this.omitNullValues = omitNullValues;
  }

  public boolean isOmitNullValues() {

    return omitNullValues;
  }

  /** @return Returns the outputFields. */
  public XmlField[] getOutputFields() {
    return outputFields;
  }

  /** @param outputFields The outputFields to set. */
  public void setOutputFields(XmlField[] outputFields) {
    this.outputFields = outputFields;
  }

  public void allocate(int nrFields) {
    outputFields = new XmlField[nrFields];
  }

  public Object clone() {
    AddXmlMeta retval = (AddXmlMeta) super.clone();
    int nrFields = outputFields.length;

    retval.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      retval.outputFields[i] = (XmlField) outputFields[i].clone();
    }

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      AddXmlData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new AddXml(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public AddXmlData getTransformData() {
    return new AddXmlData();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      encoding = XmlHandler.getTagValue(transformNode, "encoding");
      valueName = XmlHandler.getTagValue(transformNode, "valueName");
      rootNode = XmlHandler.getTagValue(transformNode, "xml_repeat_element");

      omitXMLheader =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "omitXMLheader"));
      omitNullValues =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "omitNullValues"));

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        outputFields[i] = new XmlField();
        outputFields[i].setFieldName(XmlHandler.getTagValue(fnode, "name"));
        outputFields[i].setElementName(XmlHandler.getTagValue(fnode, "element"));
        outputFields[i].setType(XmlHandler.getTagValue(fnode, "type"));
        outputFields[i].setFormat(XmlHandler.getTagValue(fnode, "format"));
        outputFields[i].setCurrencySymbol(XmlHandler.getTagValue(fnode, "currency"));
        outputFields[i].setDecimalSymbol(XmlHandler.getTagValue(fnode, "decimal"));
        outputFields[i].setGroupingSymbol(XmlHandler.getTagValue(fnode, "group"));
        outputFields[i].setNullString(XmlHandler.getTagValue(fnode, "nullif"));
        outputFields[i].setLength(Const.toInt(XmlHandler.getTagValue(fnode, "length"), -1));
        outputFields[i].setPrecision(Const.toInt(XmlHandler.getTagValue(fnode, "precision"), -1));
        outputFields[i].setAttribute(
            "Y".equalsIgnoreCase(XmlHandler.getTagValue(fnode, "attribute")));
        outputFields[i].setAttributeParentName(
            XmlHandler.getTagValue(fnode, "attributeParentName"));
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public void setDefault() {
    omitXMLheader = true;
    omitNullValues = false;
    encoding = Const.XML_ENCODING;

    valueName = "xmlvaluename";
    rootNode = "Row";

    int nrFields = 0;

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      outputFields[i] = new XmlField();

      outputFields[i].setFieldName("field" + i);
      outputFields[i].setElementName("field" + i);
      outputFields[i].setType("Number");
      outputFields[i].setFormat(" 0,000,000.00;-0,000,000.00");
      outputFields[i].setCurrencySymbol("");
      outputFields[i].setDecimalSymbol(",");
      outputFields[i].setGroupingSymbol(".");
      outputFields[i].setNullString("");
      outputFields[i].setLength(-1);
      outputFields[i].setPrecision(-1);
      outputFields[i].setAttribute(false);
      outputFields[i].setElementName("field" + i);
    }
  }

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
  public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer(500);

    xml.append("    ").append(XmlHandler.addTagValue("encoding", encoding));
    xml.append("    ").append(XmlHandler.addTagValue("valueName", valueName));
    xml.append("    ").append(XmlHandler.addTagValue("xml_repeat_element", rootNode));

    xml.append("    <file>").append(Const.CR);
    xml.append("      ").append(XmlHandler.addTagValue("omitXMLheader", omitXMLheader));
    xml.append("      ").append(XmlHandler.addTagValue("omitNullValues", omitNullValues));
    xml.append("    </file>").append(Const.CR);
    xml.append("    <fields>").append(Const.CR);
    for (int i = 0; i < outputFields.length; i++) {
      XmlField field = outputFields[i];

      if (field.getFieldName() != null && field.getFieldName().length() != 0) {
        xml.append("      <field>").append(Const.CR);
        xml.append("        ").append(XmlHandler.addTagValue("name", field.getFieldName()));
        xml.append("        ").append(XmlHandler.addTagValue("element", field.getElementName()));
        xml.append("        ").append(XmlHandler.addTagValue("type", field.getTypeDesc()));
        xml.append("        ").append(XmlHandler.addTagValue("format", field.getFormat()));
        xml.append("        ")
            .append(XmlHandler.addTagValue("currency", field.getCurrencySymbol()));
        xml.append("        ").append(XmlHandler.addTagValue("decimal", field.getDecimalSymbol()));
        xml.append("        ").append(XmlHandler.addTagValue("group", field.getGroupingSymbol()));
        xml.append("        ").append(XmlHandler.addTagValue("nullif", field.getNullString()));
        xml.append("        ").append(XmlHandler.addTagValue("length", field.getLength()));
        xml.append("        ").append(XmlHandler.addTagValue("precision", field.getPrecision()));
        xml.append("        ").append(XmlHandler.addTagValue("attribute", field.isAttribute()));
        xml.append("        ")
            .append(XmlHandler.addTagValue("attributeParentName", field.getAttributeParentName()));
        xml.append("        </field>").append(Const.CR);
      }
    }
    xml.append("    </fields>" + Const.CR);

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
    // TODO - add checks for empty fieldnames

    // Check output fields
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "AddXMLMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (int i = 0; i < outputFields.length; i++) {
        int idx = prev.indexOfValue(outputFields[i].getFieldName());
        if (idx < 0) {
          errorMessage += "\t\t" + outputFields[i].getFieldName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "AddXMLMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "AddXMLMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "AddXMLMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AddXMLMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            CheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "AddXMLMeta.CheckResult.FilesNotChecked"),
            transformMeta);
    remarks.add(cr);
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /** @return Returns the rootNode. */
  public String getRootNode() {
    return rootNode;
  }

  /** @param rootNode The root node to set. */
  public void setRootNode(String rootNode) {
    this.rootNode = rootNode;
  }

  public String getValueName() {
    return valueName;
  }

  public void setValueName(String valueName) {
    this.valueName = valueName;
  }
}
