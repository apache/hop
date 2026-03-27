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
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "XMLInputStream",
    image = "xml_input_stream.svg",
    name = "i18n::XMLInputStream.name",
    description = "i18n::XMLInputStream.description",
    categoryDescription = "i18n::XMLInputStream.category",
    keywords = "i18n::XmlInputStreamMeta.keyword",
    documentationUrl = "/pipeline/transforms/xmlinputstream.html")
@Getter
@Setter
public class XmlInputStreamMeta extends BaseTransformMeta<XmlInputStream, XmlInputStreamData> {
  private static final int DEFAULT_STRING_LEN_FILENAME = 256; // default length for XML path
  private static final int DEFAULT_STRING_LEN_PATH = 1024; // default length for XML path
  public static final String DEFAULT_STRING_LEN = "1024"; // used by defaultStringLen
  public static final String DEFAULT_ENCODING = "UTF-8"; // used by encoding

  @HopMetadataProperty(key = "filename")
  private String filename;

  @HopMetadataProperty(key = "addResultFile")
  private boolean addResultFile;

  /**
   * The number of rows to ignore before sending rows to the next transform . String for variable
   * usage, enables chunk loading defined in an outer loop.
   */
  @HopMetadataProperty(key = "nrRowsToSkip")
  private String nrRowsToSkip;

  /** The maximum number of lines to read */
  @HopMetadataProperty(key = "rowLimit")
  private String rowLimit;

  /**
   * This is the default String length for name/value elements & attributes. // default set to
   * DEFAULT_STRING_LEN
   */
  @HopMetadataProperty(key = "defaultStringLen")
  private String defaultStringLen;

  /** Encoding to be used */
  @HopMetadataProperty(key = "encoding")
  private String encoding;

  /** Enable Namespaces in the output? (will be slower) */
  @HopMetadataProperty(key = "enableNamespaces")
  private boolean enableNamespaces;

  /**
   * Trim all name/value elements & attributes? Trim is also eliminating white spaces, tab, cr, lf
   * at the beginning and end of.
   */
  @HopMetadataProperty(key = "enableTrim")
  private boolean enableTrim;

  // The fields in the output stream
  @HopMetadataProperty(key = "includeFilenameField")
  private boolean includeFilenameField;

  @HopMetadataProperty(key = "filenameField")
  private String filenameField;

  @HopMetadataProperty(key = "includeRowNumberField")
  private boolean includeRowNumberField;

  @HopMetadataProperty(key = "rowNumberField")
  private String rowNumberField;

  @HopMetadataProperty(key = "includeDataTypeNumericField")
  private boolean includeXmlDataTypeNumericField;

  @HopMetadataProperty(key = "dataTypeNumericField")
  private String xmlDataTypeNumericField;

  @HopMetadataProperty(key = "includeDataTypeDescriptionField")
  private boolean includeXmlDataTypeDescriptionField;

  @HopMetadataProperty(key = "dataTypeDescriptionField")
  private String xmlDataTypeDescriptionField;

  @HopMetadataProperty(key = "includeXmlLocationLineField")
  private boolean includeXmlLocationLineField;

  @HopMetadataProperty(key = "xmlLocationLineField")
  private String xmlLocationLineField;

  @HopMetadataProperty(key = "includeXmlLocationColumnField")
  private boolean includeXmlLocationColumnField;

  @HopMetadataProperty(key = "xmlLocationColumnField")
  private String xmlLocationColumnField;

  @HopMetadataProperty(key = "includeXmlElementIDField")
  private boolean includeXmlElementIDField;

  @HopMetadataProperty(key = "xmlElementIDField")
  private String xmlElementIDField;

  @HopMetadataProperty(key = "includeXmlParentElementIDField")
  private boolean includeXmlParentElementIDField;

  @HopMetadataProperty(key = "xmlParentElementIDField")
  private String xmlParentElementIDField;

  @HopMetadataProperty(key = "includeXmlElementLevelField")
  private boolean includeXmlElementLevelField;

  @HopMetadataProperty(key = "xmlElementLevelField")
  private String xmlElementLevelField;

  @HopMetadataProperty(key = "includeXmlPathField")
  private boolean includeXmlPathField;

  @HopMetadataProperty(key = "xmlPathField")
  private String xmlPathField;

  @HopMetadataProperty(key = "includeXmlParentPathField")
  private boolean includeXmlParentPathField;

  @HopMetadataProperty(key = "xmlParentPathField")
  private String xmlParentPathField;

  @HopMetadataProperty(key = "includeXmlDataNameField")
  private boolean includeXmlDataNameField;

  @HopMetadataProperty(key = "xmlDataNameField")
  private String xmlDataNameField;

  @HopMetadataProperty(key = "includeXmlDataValueField")
  private boolean includeXmlDataValueField;

  @HopMetadataProperty(key = "xmlDataValueField")
  private String xmlDataValueField;

  /** Are we accepting filenames in input rows? */
  @HopMetadataProperty(key = "sourceFromInput")
  public boolean sourceFromInput;

  /** The field in which the filename is placed */
  @HopMetadataProperty(key = "sourceFieldName")
  public String sourceFieldName;

  public XmlInputStreamMeta() {
    super();
    filename = "";
    nrRowsToSkip = "0";
    rowLimit = "0";
    defaultStringLen = DEFAULT_STRING_LEN;
    encoding = DEFAULT_ENCODING;
    enableTrim = true;
    filenameField = "xml_filename";
    rowNumberField = "xml_row_number";
    xmlDataTypeNumericField = "xml_data_type_numeric";
    includeXmlDataTypeDescriptionField = true;
    xmlDataTypeDescriptionField = "xml_data_type_description";
    xmlLocationLineField = "xml_location_line";
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

  public XmlInputStreamMeta(XmlInputStreamMeta m) {
    this();
    this.addResultFile = m.addResultFile;
    this.defaultStringLen = m.defaultStringLen;
    this.enableNamespaces = m.enableNamespaces;
    this.enableTrim = m.enableTrim;
    this.encoding = m.encoding;
    this.filename = m.filename;
    this.filenameField = m.filenameField;
    this.includeFilenameField = m.includeFilenameField;
    this.includeRowNumberField = m.includeRowNumberField;
    this.includeXmlDataNameField = m.includeXmlDataNameField;
    this.includeXmlDataTypeDescriptionField = m.includeXmlDataTypeDescriptionField;
    this.includeXmlDataTypeNumericField = m.includeXmlDataTypeNumericField;
    this.includeXmlDataValueField = m.includeXmlDataValueField;
    this.includeXmlElementIDField = m.includeXmlElementIDField;
    this.includeXmlElementLevelField = m.includeXmlElementLevelField;
    this.includeXmlLocationColumnField = m.includeXmlLocationColumnField;
    this.includeXmlLocationLineField = m.includeXmlLocationLineField;
    this.includeXmlParentElementIDField = m.includeXmlParentElementIDField;
    this.includeXmlParentPathField = m.includeXmlParentPathField;
    this.includeXmlPathField = m.includeXmlPathField;
    this.nrRowsToSkip = m.nrRowsToSkip;
    this.rowLimit = m.rowLimit;
    this.rowNumberField = m.rowNumberField;
    this.sourceFieldName = m.sourceFieldName;
    this.sourceFromInput = m.sourceFromInput;
    this.xmlDataNameField = m.xmlDataNameField;
    this.xmlDataTypeDescriptionField = m.xmlDataTypeDescriptionField;
    this.xmlDataTypeNumericField = m.xmlDataTypeNumericField;
    this.xmlDataValueField = m.xmlDataValueField;
    this.xmlElementIDField = m.xmlElementIDField;
    this.xmlElementLevelField = m.xmlElementLevelField;
    this.xmlLocationColumnField = m.xmlLocationColumnField;
    this.xmlLocationLineField = m.xmlLocationLineField;
    this.xmlParentElementIDField = m.xmlParentElementIDField;
    this.xmlParentPathField = m.xmlParentPathField;
    this.xmlPathField = m.xmlPathField;
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
        Const.toInt(variables.resolve(defaultStringLen), Integer.parseInt(DEFAULT_STRING_LEN));

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
      IValueMeta v = new ValueMetaInteger(variables.resolve(xmlDataTypeNumericField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeXmlDataTypeDescriptionField) {
      IValueMeta v = new ValueMetaString(variables.resolve(xmlDataTypeDescriptionField));
      v.setLength(25);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeXmlLocationLineField) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(xmlLocationLineField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeXmlLocationColumnField) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(xmlLocationColumnField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeXmlElementIDField) {
      IValueMeta v = new ValueMetaInteger("xml_element_id");
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeXmlParentElementIDField) {
      IValueMeta v = new ValueMetaInteger("xml_parent_element_id");
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeXmlElementLevelField) {
      IValueMeta v = new ValueMetaInteger("xml_element_level");
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeXmlPathField) {
      IValueMeta v = new ValueMetaString("xml_path");
      v.setLength(DEFAULT_STRING_LEN_PATH);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeXmlParentPathField) {
      IValueMeta v = new ValueMetaString("xml_parent_path");
      v.setLength(DEFAULT_STRING_LEN_PATH);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeXmlDataNameField) {
      IValueMeta v = new ValueMetaString("xml_data_name");
      v.setLength(defaultStringLenNameValueElements);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeXmlDataValueField) {
      IValueMeta v = new ValueMetaString("xml_data_value");
      v.setLength(defaultStringLenNameValueElements);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
  }

  @Override
  public Object clone() {
    return new XmlInputStreamMeta(this);
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
    if (Utils.isEmpty(filename)) {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, "Filename is not given", transformMeta);
    } else {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, "Filename is given", transformMeta);
    }
    remarks.add(cr);

    if (!pipelineMeta.findPreviousTransforms(transformMeta).isEmpty()) {
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
                  ICheckResult.TYPE_RESULT_OK,
                  "Field name is in previous transform",
                  transformMeta);
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
}
