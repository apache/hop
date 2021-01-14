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

package org.apache.hop.pipeline.transforms.xml.xmlinputstream;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "XMLInputStream",
    image = "xml_input_stream.svg",
    name = "i18n::XMLInputStream.name",
    description = "i18n::XMLInputStream.description",
    categoryDescription = "i18n::XMLInputStream.category",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/xmlinputstream.html")
public class XmlInputStreamMeta extends BaseTransformMeta
    implements ITransformMeta<XmlInputStream, XmlInputStreamData> {
  private static final int DEFAULT_STRING_LEN_FILENAME = 256; // default length for XML path
  private static final int DEFAULT_STRING_LEN_PATH = 1024; // default length for XML path
  public static final String DEFAULT_STRING_LEN = "1024"; // used by defaultStringLen
  public static final String DEFAULT_ENCODING = "UTF-8"; // used by encoding

  private String filename;
  private boolean addResultFile;

  /** The number of rows to ignore before sending rows to the next transform */
  private String
      nrRowsToSkip; // String for variable usage, enables chunk loading defined in an outer loop

  /** The maximum number of lines to read */
  private String
      rowLimit; // String for variable usage, enables chunk loading defined in an outer loop

  /** This is the default String length for name/value elements & attributes */
  private String defaultStringLen; // default set to DEFAULT_STRING_LEN

  /** Encoding to be used */
  private String encoding; // default set to DEFAULT_ENCODING

  /** Enable Namespaces in the output? (will be slower) */
  private boolean enableNamespaces;

  /** Trim all name/value elements & attributes? */
  private boolean
      enableTrim; // trim is also eliminating white spaces, tab, cr, lf at the beginning and end of
                  // the
  // string

  // The fields in the output stream
  private boolean includeFilenameField;
  private String filenameField;

  private boolean includeRowNumberField;
  private String rowNumberField;

  private boolean includeXmlDataTypeNumericField;
  private String xmlDataTypeNumericField;

  private boolean includeXmlDataTypeDescriptionField;
  private String xmlDataTypeDescriptionField;

  private boolean includeXmlLocationLineField;
  private String xmlLocationLineField;

  private boolean includeXmlLocationColumnField;
  private String xmlLocationColumnField;

  private boolean includeXmlElementIDField;
  private String xmlElementIDField;

  private boolean includeXmlParentElementIDField;
  private String xmlParentElementIDField;

  private boolean includeXmlElementLevelField;
  private String xmlElementLevelField;

  private boolean includeXmlPathField;
  private String xmlPathField;

  private boolean includeXmlParentPathField;
  private String xmlParentPathField;

  private boolean includeXmlDataNameField;
  private String xmlDataNameField;

  private boolean includeXmlDataValueField;
  private String xmlDataValueField;

  /** Are we accepting filenames in input rows? */
  public boolean sourceFromInput;

  /** The field in which the filename is placed */
  public String sourceFieldName;

  public XmlInputStreamMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    int defaultStringLenNameValueElements =
        Const.toInt(variables.resolve(defaultStringLen), new Integer(DEFAULT_STRING_LEN));

    if (includeFilenameField) {
      IValueMeta v = new ValueMetaString(variables.resolve(filenameField));
      v.setLength(DEFAULT_STRING_LEN_FILENAME);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (includeRowNumberField) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (includeXmlDataTypeNumericField) {
      IValueMeta vdtn = new ValueMetaInteger(variables.resolve(xmlDataTypeNumericField));
      vdtn.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      vdtn.setOrigin(name);
      r.addValueMeta(vdtn);
    }

    if (includeXmlDataTypeDescriptionField) {
      IValueMeta vdtd = new ValueMetaString(variables.resolve(xmlDataTypeDescriptionField));
      vdtd.setLength(25);
      vdtd.setOrigin(name);
      r.addValueMeta(vdtd);
    }

    if (includeXmlLocationLineField) {
      IValueMeta vline = new ValueMetaInteger(variables.resolve(xmlLocationLineField));
      vline.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      vline.setOrigin(name);
      r.addValueMeta(vline);
    }

    if (includeXmlLocationColumnField) {
      IValueMeta vcol = new ValueMetaInteger(variables.resolve(xmlLocationColumnField));
      vcol.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      vcol.setOrigin(name);
      r.addValueMeta(vcol);
    }

    if (includeXmlElementIDField) {
      IValueMeta vdid = new ValueMetaInteger("xml_element_id");
      vdid.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      vdid.setOrigin(name);
      r.addValueMeta(vdid);
    }

    if (includeXmlParentElementIDField) {
      IValueMeta vdparentid = new ValueMetaInteger("xml_parent_element_id");
      vdparentid.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      vdparentid.setOrigin(name);
      r.addValueMeta(vdparentid);
    }

    if (includeXmlElementLevelField) {
      IValueMeta vdlevel = new ValueMetaInteger("xml_element_level");
      vdlevel.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      vdlevel.setOrigin(name);
      r.addValueMeta(vdlevel);
    }

    if (includeXmlPathField) {
      IValueMeta vdparentxp = new ValueMetaString("xml_path");
      vdparentxp.setLength(DEFAULT_STRING_LEN_PATH);
      vdparentxp.setOrigin(name);
      r.addValueMeta(vdparentxp);
    }

    if (includeXmlParentPathField) {
      IValueMeta vdparentpxp = new ValueMetaString("xml_parent_path");
      vdparentpxp.setLength(DEFAULT_STRING_LEN_PATH);
      vdparentpxp.setOrigin(name);
      r.addValueMeta(vdparentpxp);
    }

    if (includeXmlDataNameField) {
      IValueMeta vdname = new ValueMetaString("xml_data_name");
      vdname.setLength(defaultStringLenNameValueElements);
      vdname.setOrigin(name);
      r.addValueMeta(vdname);
    }

    if (includeXmlDataValueField) {
      IValueMeta vdval = new ValueMetaString("xml_data_value");
      vdval.setLength(defaultStringLenNameValueElements);
      vdval.setOrigin(name);
      r.addValueMeta(vdval);
    }
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      sourceFromInput =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "sourceFromInput"));
      sourceFieldName = Const.NVL(XmlHandler.getTagValue(transformNode, "sourceFieldName"), "");

      filename = Const.NVL(XmlHandler.getTagValue(transformNode, "filename"), "");
      addResultFile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addResultFile"));

      nrRowsToSkip = Const.NVL(XmlHandler.getTagValue(transformNode, "nrRowsToSkip"), "0");
      rowLimit = Const.NVL(XmlHandler.getTagValue(transformNode, "rowLimit"), "0");
      defaultStringLen =
          Const.NVL(XmlHandler.getTagValue(transformNode, "defaultStringLen"), DEFAULT_STRING_LEN);
      encoding = Const.NVL(XmlHandler.getTagValue(transformNode, "encoding"), DEFAULT_ENCODING);
      enableNamespaces =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "enableNamespaces"));
      enableTrim = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "enableTrim"));

      // The fields in the output stream
      // When they are undefined (checked with NVL) the original default value will be taken
      includeFilenameField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeFilenameField"));
      filenameField =
          Const.NVL(XmlHandler.getTagValue(transformNode, "filenameField"), filenameField);

      includeRowNumberField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeRowNumberField"));
      rowNumberField =
          Const.NVL(XmlHandler.getTagValue(transformNode, "rowNumberField"), rowNumberField);

      includeXmlDataTypeNumericField =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "includeDataTypeNumericField"));
      xmlDataTypeNumericField =
          Const.NVL(
              XmlHandler.getTagValue(transformNode, "dataTypeNumericField"),
              xmlDataTypeNumericField);

      includeXmlDataTypeDescriptionField =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "includeDataTypeDescriptionField"));
      xmlDataTypeDescriptionField =
          Const.NVL(
              XmlHandler.getTagValue(transformNode, "dataTypeDescriptionField"),
              xmlDataTypeDescriptionField);

      includeXmlLocationLineField =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "includeXmlLocationLineField"));
      xmlLocationLineField =
          Const.NVL(
              XmlHandler.getTagValue(transformNode, "xmlLocationLineField"), xmlLocationLineField);

      includeXmlLocationColumnField =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "includeXmlLocationColumnField"));
      xmlLocationColumnField =
          Const.NVL(
              XmlHandler.getTagValue(transformNode, "xmlLocationColumnField"),
              xmlLocationColumnField);

      includeXmlElementIDField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeXmlElementIDField"));
      xmlElementIDField =
          Const.NVL(XmlHandler.getTagValue(transformNode, "xmlElementIDField"), xmlElementIDField);

      includeXmlParentElementIDField =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "includeXmlParentElementIDField"));
      xmlParentElementIDField =
          Const.NVL(
              XmlHandler.getTagValue(transformNode, "xmlParentElementIDField"),
              xmlParentElementIDField);

      includeXmlElementLevelField =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "includeXmlElementLevelField"));
      xmlElementLevelField =
          Const.NVL(
              XmlHandler.getTagValue(transformNode, "xmlElementLevelField"), xmlElementLevelField);

      includeXmlPathField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeXmlPathField"));
      xmlPathField = Const.NVL(XmlHandler.getTagValue(transformNode, "xmlPathField"), xmlPathField);

      includeXmlParentPathField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeXmlParentPathField"));
      xmlParentPathField =
          Const.NVL(
              XmlHandler.getTagValue(transformNode, "xmlParentPathField"), xmlParentPathField);

      includeXmlDataNameField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeXmlDataNameField"));
      xmlDataNameField =
          Const.NVL(XmlHandler.getTagValue(transformNode, "xmlDataNameField"), xmlDataNameField);

      includeXmlDataValueField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeXmlDataValueField"));
      xmlDataValueField =
          Const.NVL(XmlHandler.getTagValue(transformNode, "xmlDataValueField"), xmlDataValueField);

    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  @Override
  public Object clone() {
    XmlInputStreamMeta retval = (XmlInputStreamMeta) super.clone();
    // TODO check

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      XmlInputStreamData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new XmlInputStream(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public XmlInputStreamData getTransformData() {
    return new XmlInputStreamData();
  }

  @Override
  public String getXml() {
    StringBuffer retval = new StringBuffer();
    retval.append("    " + XmlHandler.addTagValue("sourceFromInput", sourceFromInput));
    retval.append("    " + XmlHandler.addTagValue("sourceFieldName", sourceFieldName));
    retval.append("    " + XmlHandler.addTagValue("filename", filename));
    retval.append("    " + XmlHandler.addTagValue("addResultFile", addResultFile));

    retval.append("    " + XmlHandler.addTagValue("nrRowsToSkip", nrRowsToSkip));
    retval.append("    " + XmlHandler.addTagValue("rowLimit", rowLimit));
    retval.append("    " + XmlHandler.addTagValue("defaultStringLen", defaultStringLen));
    retval.append("    " + XmlHandler.addTagValue("encoding", encoding));
    retval.append("    " + XmlHandler.addTagValue("enableNamespaces", enableNamespaces));
    retval.append("    " + XmlHandler.addTagValue("enableTrim", enableTrim));

    // The fields in the output stream
    retval.append("    " + XmlHandler.addTagValue("includeFilenameField", includeFilenameField));
    retval.append("    " + XmlHandler.addTagValue("filenameField", filenameField));

    retval.append("    " + XmlHandler.addTagValue("includeRowNumberField", includeRowNumberField));
    retval.append("    " + XmlHandler.addTagValue("rowNumberField", rowNumberField));

    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "includeDataTypeNumericField", includeXmlDataTypeNumericField));
    retval.append("    " + XmlHandler.addTagValue("dataTypeNumericField", xmlDataTypeNumericField));

    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "includeDataTypeDescriptionField", includeXmlDataTypeDescriptionField));
    retval.append(
        "    " + XmlHandler.addTagValue("dataTypeDescriptionField", xmlDataTypeDescriptionField));

    retval.append(
        "    "
            + XmlHandler.addTagValue("includeXmlLocationLineField", includeXmlLocationLineField));
    retval.append("    " + XmlHandler.addTagValue("xmlLocationLineField", xmlLocationLineField));

    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "includeXmlLocationColumnField", includeXmlLocationColumnField));
    retval.append(
        "    " + XmlHandler.addTagValue("xmlLocationColumnField", xmlLocationColumnField));

    retval.append(
        "    " + XmlHandler.addTagValue("includeXmlElementIDField", includeXmlElementIDField));
    retval.append("    " + XmlHandler.addTagValue("xmlElementIDField", xmlElementIDField));

    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "includeXmlParentElementIDField", includeXmlParentElementIDField));
    retval.append(
        "    " + XmlHandler.addTagValue("xmlParentElementIDField", xmlParentElementIDField));

    retval.append(
        "    "
            + XmlHandler.addTagValue("includeXmlElementLevelField", includeXmlElementLevelField));
    retval.append("    " + XmlHandler.addTagValue("xmlElementLevelField", xmlElementLevelField));

    retval.append("    " + XmlHandler.addTagValue("includeXmlPathField", includeXmlPathField));
    retval.append("    " + XmlHandler.addTagValue("xmlPathField", xmlPathField));

    retval.append(
        "    " + XmlHandler.addTagValue("includeXmlParentPathField", includeXmlParentPathField));
    retval.append("    " + XmlHandler.addTagValue("xmlParentPathField", xmlParentPathField));

    retval.append(
        "    " + XmlHandler.addTagValue("includeXmlDataNameField", includeXmlDataNameField));
    retval.append("    " + XmlHandler.addTagValue("xmlDataNameField", xmlDataNameField));

    retval.append(
        "    " + XmlHandler.addTagValue("includeXmlDataValueField", includeXmlDataValueField));
    retval.append("    " + XmlHandler.addTagValue("xmlDataValueField", xmlDataValueField));

    return retval.toString();
  }

  @Override
  public void setDefault() {
    filename = "";
    addResultFile = false;

    nrRowsToSkip = "0";
    rowLimit = "0";
    defaultStringLen = DEFAULT_STRING_LEN;
    encoding = DEFAULT_ENCODING;
    enableNamespaces = false;
    enableTrim = true;

    // The fields in the output stream
    includeFilenameField = false;
    filenameField = "xml_filename";

    includeRowNumberField = false;
    rowNumberField = "xml_row_number";

    includeXmlDataTypeNumericField = false;
    xmlDataTypeNumericField = "xml_data_type_numeric";

    includeXmlDataTypeDescriptionField = true;
    xmlDataTypeDescriptionField = "xml_data_type_description";

    includeXmlLocationLineField = false;
    xmlLocationLineField = "xml_location_line";

    includeXmlLocationColumnField = false;
    xmlLocationColumnField = "xml_location_column";

    includeXmlElementIDField = true;
    xmlElementIDField = "xml_element_id";

    includeXmlParentElementIDField = true;
    xmlParentElementIDField = "xml_parent_element_id";

    includeXmlElementLevelField = true;
    xmlElementLevelField = "xml_element_level";

    includeXmlPathField = true;
    xmlPathField = "xml_path";

    includeXmlParentPathField = true;
    xmlParentPathField = "xml_parent_path";

    includeXmlDataNameField = true;
    xmlDataNameField = "xml_data_name";

    includeXmlDataValueField = true;
    xmlDataValueField = "xml_data_value";
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
    // TODO externalize messages
    CheckResult cr;
    if (Utils.isEmpty(filename)) {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, "Filename is not given", transformMeta);
    } else {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, "Filename is given", transformMeta);
    }
    remarks.add(cr);

    if (pipelineMeta.findNrPrevTransforms(transformMeta) > 0) {
      IRowMeta previousFields;
      try {
        previousFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
        if (null == previousFields.searchValueMeta(filename)) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  "Field name is not in previous transform",
                  transformMeta);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK, "Field name is in previous transform", transformMeta);
        }
      } catch (HopTransformException e) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR, "Could not find previous transform", transformMeta);
      }
      remarks.add(cr);
    }

    if (includeXmlDataTypeNumericField || includeXmlDataTypeDescriptionField) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_COMMENT,
              "At least one Data Type field (numeric or description) is in the data stream",
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              "Data Type field (numeric or description) is missing in the data stream",
              transformMeta);
    }
    remarks.add(cr);

    if (includeXmlDataValueField && includeXmlDataNameField) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_COMMENT,
              "Data Name and Data Value fields are in the data stream",
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              "Both Data Name and Data Value fields should be in the data stream",
              transformMeta);
    }
    remarks.add(cr);
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public boolean isAddResultFile() {
    return addResultFile;
  }

  public void setAddResultFile(boolean addResultFile) {
    this.addResultFile = addResultFile;
  }

  public String getNrRowsToSkip() {
    return nrRowsToSkip;
  }

  public void setNrRowsToSkip(String nrRowsToSkip) {
    this.nrRowsToSkip = nrRowsToSkip;
  }

  public String getRowLimit() {
    return rowLimit;
  }

  public void setRowLimit(String rowLimit) {
    this.rowLimit = rowLimit;
  }

  public String getDefaultStringLen() {
    return defaultStringLen;
  }

  public void setDefaultStringLen(String defaultStringLen) {
    this.defaultStringLen = defaultStringLen;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  public boolean isEnableNamespaces() {
    return enableNamespaces;
  }

  public void setEnableNamespaces(boolean enableNamespaces) {
    this.enableNamespaces = enableNamespaces;
  }

  public boolean isEnableTrim() {
    return enableTrim;
  }

  public void setEnableTrim(boolean enableTrim) {
    this.enableTrim = enableTrim;
  }

  public boolean isIncludeFilenameField() {
    return includeFilenameField;
  }

  public void setIncludeFilenameField(boolean includeFilenameField) {
    this.includeFilenameField = includeFilenameField;
  }

  public String getFilenameField() {
    return filenameField;
  }

  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  public boolean isIncludeRowNumberField() {
    return includeRowNumberField;
  }

  public void setIncludeRowNumberField(boolean includeRowNumberField) {
    this.includeRowNumberField = includeRowNumberField;
  }

  public String getRowNumberField() {
    return rowNumberField;
  }

  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  public boolean isIncludeXmlDataTypeNumericField() {
    return includeXmlDataTypeNumericField;
  }

  public void setIncludeXmlDataTypeNumericField(boolean includeXmlDataTypeNumericField) {
    this.includeXmlDataTypeNumericField = includeXmlDataTypeNumericField;
  }

  public String getXmlDataTypeNumericField() {
    return xmlDataTypeNumericField;
  }

  public void setXmlDataTypeNumericField(String xmlDataTypeNumericField) {
    this.xmlDataTypeNumericField = xmlDataTypeNumericField;
  }

  public boolean isIncludeXmlDataTypeDescriptionField() {
    return includeXmlDataTypeDescriptionField;
  }

  public void setIncludeXmlDataTypeDescriptionField(boolean includeXmlDataTypeDescriptionField) {
    this.includeXmlDataTypeDescriptionField = includeXmlDataTypeDescriptionField;
  }

  public String getXmlDataTypeDescriptionField() {
    return xmlDataTypeDescriptionField;
  }

  public void setXmlDataTypeDescriptionField(String xmlDataTypeDescriptionField) {
    this.xmlDataTypeDescriptionField = xmlDataTypeDescriptionField;
  }

  public boolean isIncludeXmlLocationLineField() {
    return includeXmlLocationLineField;
  }

  public void setIncludeXmlLocationLineField(boolean includeXmlLocationLineField) {
    this.includeXmlLocationLineField = includeXmlLocationLineField;
  }

  public String getXmlLocationLineField() {
    return xmlLocationLineField;
  }

  public void setXmlLocationLineField(String xmlLocationLineField) {
    this.xmlLocationLineField = xmlLocationLineField;
  }

  public boolean isIncludeXmlLocationColumnField() {
    return includeXmlLocationColumnField;
  }

  public void setIncludeXmlLocationColumnField(boolean includeXmlLocationColumnField) {
    this.includeXmlLocationColumnField = includeXmlLocationColumnField;
  }

  public String getXmlLocationColumnField() {
    return xmlLocationColumnField;
  }

  public void setXmlLocationColumnField(String xmlLocationColumnField) {
    this.xmlLocationColumnField = xmlLocationColumnField;
  }

  public boolean isIncludeXmlElementIDField() {
    return includeXmlElementIDField;
  }

  public void setIncludeXmlElementIDField(boolean includeXmlElementIDField) {
    this.includeXmlElementIDField = includeXmlElementIDField;
  }

  public String getXmlElementIDField() {
    return xmlElementIDField;
  }

  public void setXmlElementIDField(String xmlElementIDField) {
    this.xmlElementIDField = xmlElementIDField;
  }

  public boolean isIncludeXmlParentElementIDField() {
    return includeXmlParentElementIDField;
  }

  public void setIncludeXmlParentElementIDField(boolean includeXmlParentElementIDField) {
    this.includeXmlParentElementIDField = includeXmlParentElementIDField;
  }

  public String getXmlParentElementIDField() {
    return xmlParentElementIDField;
  }

  public void setXmlParentElementIDField(String xmlParentElementIDField) {
    this.xmlParentElementIDField = xmlParentElementIDField;
  }

  public boolean isIncludeXmlElementLevelField() {
    return includeXmlElementLevelField;
  }

  public void setIncludeXmlElementLevelField(boolean includeXmlElementLevelField) {
    this.includeXmlElementLevelField = includeXmlElementLevelField;
  }

  public String getXmlElementLevelField() {
    return xmlElementLevelField;
  }

  public void setXmlElementLevelField(String xmlElementLevelField) {
    this.xmlElementLevelField = xmlElementLevelField;
  }

  public boolean isIncludeXmlPathField() {
    return includeXmlPathField;
  }

  public void setIncludeXmlPathField(boolean includeXmlPathField) {
    this.includeXmlPathField = includeXmlPathField;
  }

  public String getXmlPathField() {
    return xmlPathField;
  }

  public void setXmlPathField(String xmlPathField) {
    this.xmlPathField = xmlPathField;
  }

  public boolean isIncludeXmlParentPathField() {
    return includeXmlParentPathField;
  }

  public void setIncludeXmlParentPathField(boolean includeXmlParentPathField) {
    this.includeXmlParentPathField = includeXmlParentPathField;
  }

  public String getXmlParentPathField() {
    return xmlParentPathField;
  }

  public void setXmlParentPathField(String xmlParentPathField) {
    this.xmlParentPathField = xmlParentPathField;
  }

  public boolean isIncludeXmlDataNameField() {
    return includeXmlDataNameField;
  }

  public void setIncludeXmlDataNameField(boolean includeXmlDataNameField) {
    this.includeXmlDataNameField = includeXmlDataNameField;
  }

  public String getXmlDataNameField() {
    return xmlDataNameField;
  }

  public void setXmlDataNameField(String xmlDataNameField) {
    this.xmlDataNameField = xmlDataNameField;
  }

  public boolean isIncludeXmlDataValueField() {
    return includeXmlDataValueField;
  }

  public void setIncludeXmlDataValueField(boolean includeXmlDataValueField) {
    this.includeXmlDataValueField = includeXmlDataValueField;
  }

  public String getXmlDataValueField() {
    return xmlDataValueField;
  }

  public void setXmlDataValueField(String xmlDataValueField) {
    this.xmlDataValueField = xmlDataValueField;
  }
}
