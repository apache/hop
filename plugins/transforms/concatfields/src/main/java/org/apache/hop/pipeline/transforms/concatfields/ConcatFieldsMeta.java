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

package org.apache.hop.pipeline.transforms.concatfields;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;
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
import org.apache.hop.pipeline.transforms.textfileoutput.TextFileField;
import org.apache.hop.pipeline.transforms.textfileoutput.TextFileOutputMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * ConcatFieldsMeta
 *
 */
@Transform(
    id = "ConcatFields",
    image = "concatfields.svg",
    name = "i18n::BaseTransform.TypeLongDesc.ConcatFields",
    description = "i18n::BaseTransform.TypeTooltipDesc.ConcatFields",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/concatfields.html")
public class ConcatFieldsMeta extends BaseTransformMeta
    implements ITransformMeta<ConcatFields, ConcatFieldsData> {

  private static final Class<?> PKG =
      ConcatFieldsMeta.class; // For Translator

  /** The separator to choose for the CSV file */
  @Injection(name = "SEPARATOR")
  private String separator;

  /** The enclosure to use in case the separator is part of a field's value */
  @Injection(name = "ENCLOSURE")
  private String enclosure;

  /**
   * Setting to allow the enclosure to be always surrounding a String value, even when there is no
   * separator inside
   */
  @Injection(name = "FORCE_ENCLOSURE")
  private boolean enclosureForced;

  /**
   * Setting to allow for backwards compatibility where the enclosure did not show up at all if
   * Force Enclosure was not checked
   */
  @Injection(name = "DISABLE_ENCLOSURE_FIX")
  private boolean disableEnclosureFix;

  /**
   * The file format: DOS or Unix It could be injected using the key "FORMAT" see the setter {@link
   * TextFileOutputMeta#setFileFormat(String)}.
   */
  private String fileFormat;

  /** Flag : Do not open new file when pipeline start */
  @Injection(name = "SPECIFY_DATE_FORMAT")
  private boolean specifyingFormat;

  /** The date format appended to the file name */
  @Injection(name = "DATE_FORMAT")
  private String dateTimeFormat;

  /** Flag: pad fields to their specified length */
  @Injection(name = "RIGHT_PAD_FIELDS")
  private boolean padded;

  /** The output fields */
  @InjectionDeep private TextFileField[] outputFields;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @Injection(name = "ENCODING")
  private String encoding;

  // have a different namespace in repository in contrast to the TextFileOutput
  private static final String ConcatFieldsNodeNameSpace = "ConcatFields";

  private String targetFieldName; // the target field name
  private int targetFieldLength; // the length of the string field
  private boolean removeSelectedFields; // remove the selected fields in the output stream

  public String getTargetFieldName() {
    return targetFieldName;
  }

  public void setTargetFieldName(String targetField) {
    this.targetFieldName = targetField;
  }

  public int getTargetFieldLength() {
    return targetFieldLength;
  }

  public void setTargetFieldLength(int targetFieldLength) {
    this.targetFieldLength = targetFieldLength;
  }

  public boolean isRemoveSelectedFields() {
    return removeSelectedFields;
  }

  public void setRemoveSelectedFields(boolean removeSelectedFields) {
    this.removeSelectedFields = removeSelectedFields;
  }

  public ConcatFieldsMeta() {
    super();
    // allocate TextFileOutputMeta
  }

  @Override
  public void setDefault() {
    //    super.setDefault();
    //    createparentfolder = true; // Default createparentfolder to true
    separator = ";";
    enclosure = "\"";
    setSpecifyingFormat(false);
    setDateTimeFormat(null);
    enclosureForced = false;
    disableEnclosureFix = false;
    padded = false;

    int i, nrFields = 0;

    allocate(nrFields);

    for (i = 0; i < nrFields; i++) {
      outputFields[i] = new TextFileField();

      outputFields[i].setName("field" + i);
      outputFields[i].setType("Number");
      outputFields[i].setFormat(" 0,000,000.00;-0,000,000.00");
      outputFields[i].setCurrencySymbol("");
      outputFields[i].setDecimalSymbol(",");
      outputFields[i].setGroupingSymbol(".");
      outputFields[i].setNullString("");
      outputFields[i].setLength(-1);
      outputFields[i].setPrecision(-1);
    }
    // set default for new properties specific to the concat fields
    targetFieldName = "";
    targetFieldLength = 0;
    removeSelectedFields = false;
  }

  @Deprecated
  public void getFieldsModifyInput(
      IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform, IVariables variables)
      throws HopTransformException {
    getFieldsModifyInput(row, name, info, nextTransform, variables, null);
  }

  public void getFieldsModifyInput(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // the field precisions and lengths are altered! see TextFileOutputMeta.getFields().
    super.getFields(row, name, info, nextTransform, variables, metadataProvider);
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
    // do not call the super class from TextFileOutputMeta since it modifies the source meta data
    // see getFieldsModifyInput() instead

    // remove selected fields from the stream when true
    if (removeSelectedFields) {
      if (getOutputFields().length > 0) {
        for (int i = 0; i < getOutputFields().length; i++) {
          TextFileField field = getOutputFields()[i];
          try {
            row.removeValueMeta(field.getName());
          } catch (HopValueException e) {
            // just ignore exceptions since missing fields are handled in the ConcatFields class
          }
        }
      } else { // no output fields selected, take them all, remove them all
        row.clear();
      }
    }

    // Check Target Field Name
    if (StringUtil.isEmpty(targetFieldName)) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "ConcatFieldsMeta.CheckResult.TargetFieldNameMissing"));
    }
    // add targetFieldName
    IValueMeta vValue = new ValueMetaString(targetFieldName, targetFieldLength, 0);
    vValue.setOrigin(name);
    if (!StringUtil.isEmpty(getEncoding())) {
      vValue.setStringEncoding(getEncoding());
    }
    row.addValueMeta(vValue);
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    super.loadXml(transformNode, metadataProvider);
    separator = XmlHandler.getTagValue(transformNode, "separator");
    if (separator == null) {
      separator = "";
    }

    enclosure = XmlHandler.getTagValue(transformNode, "enclosure");
    if (enclosure == null) {
      enclosure = "";
    }

    enclosureForced =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "enclosure_forced"));

    targetFieldName =
        XmlHandler.getTagValue(transformNode, ConcatFieldsNodeNameSpace, "targetFieldName");
    targetFieldLength =
        Const.toInt(
            XmlHandler.getTagValue(transformNode, ConcatFieldsNodeNameSpace, "targetFieldLength"),
            0);
    removeSelectedFields =
        "Y"
            .equalsIgnoreCase(
                XmlHandler.getTagValue(
                    transformNode, ConcatFieldsNodeNameSpace, "removeSelectedFields"));

    Node fields = XmlHandler.getSubNode(transformNode, "fields");
    int nrFields = XmlHandler.countNodes(fields, "field");

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

      outputFields[i] = new TextFileField();
      outputFields[i].setName(XmlHandler.getTagValue(fnode, "name"));
      outputFields[i].setType(XmlHandler.getTagValue(fnode, "type"));
      outputFields[i].setFormat(XmlHandler.getTagValue(fnode, "format"));
      outputFields[i].setCurrencySymbol(XmlHandler.getTagValue(fnode, "currency"));
      outputFields[i].setDecimalSymbol(XmlHandler.getTagValue(fnode, "decimal"));
      outputFields[i].setGroupingSymbol(XmlHandler.getTagValue(fnode, "group"));
      outputFields[i].setTrimType(
          ValueMetaBase.getTrimTypeByCode(XmlHandler.getTagValue(fnode, "trim_type")));
      outputFields[i].setNullString(XmlHandler.getTagValue(fnode, "nullif"));
      outputFields[i].setLength(Const.toInt(XmlHandler.getTagValue(fnode, "length"), -1));
      outputFields[i].setPrecision(Const.toInt(XmlHandler.getTagValue(fnode, "precision"), -1));
    }
  }

  @Override
  public String getXml() {
    //    String retval = super.getXml();
    StringBuilder retval = new StringBuilder(800);
    retval.append("    ").append(XmlHandler.addTagValue("separator", separator));
    retval.append("    ").append(XmlHandler.addTagValue("enclosure", enclosure));
    retval.append("    ").append(XmlHandler.addTagValue("enclosure_forced", enclosureForced));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("enclosure_fix_disabled", disableEnclosureFix));
    retval.append("    ").append(XmlHandler.addTagValue("format", fileFormat));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));
    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < outputFields.length; i++) {
      TextFileField field = outputFields[i];

      if (field.getName() != null && field.getName().length() != 0) {
        retval.append("      <field>").append(Const.CR);
        retval.append("        ").append(XmlHandler.addTagValue("name", field.getName()));
        retval.append("        ").append(XmlHandler.addTagValue("type", field.getTypeDesc()));
        retval.append("        ").append(XmlHandler.addTagValue("format", field.getFormat()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("currency", field.getCurrencySymbol()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("decimal", field.getDecimalSymbol()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("group", field.getGroupingSymbol()));
        retval.append("        ").append(XmlHandler.addTagValue("nullif", field.getNullString()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("trim_type", field.getTrimTypeCode()));
        retval.append("        ").append(XmlHandler.addTagValue("length", field.getLength()));
        retval.append("        ").append(XmlHandler.addTagValue("precision", field.getPrecision()));
        retval.append("      </field>").append(Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);
    retval.append("    <" + ConcatFieldsNodeNameSpace + ">" + Const.CR);
    retval.append(XmlHandler.addTagValue("targetFieldName", targetFieldName));
    retval.append(XmlHandler.addTagValue("targetFieldLength", targetFieldLength));
    retval.append(XmlHandler.addTagValue("removeSelectedFields", removeSelectedFields));
    retval.append("    </" + ConcatFieldsNodeNameSpace + ">" + Const.CR);
    return retval.toString();
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

    // Check Target Field Name
    if (StringUtil.isEmpty(targetFieldName)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ConcatFieldsMeta.CheckResult.TargetFieldNameMissing"),
              transformMeta);
      remarks.add(cr);
    }

    // Check Target Field Length when Fast Data Dump
    if (targetFieldLength <= 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "ConcatFieldsMeta.CheckResult.TargetFieldLengthMissingFastDataDump"),
              transformMeta);
      remarks.add(cr);
    }

    // Check output fields
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ConcatFieldsMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (int i = 0; i < getOutputFields().length; i++) {
        int idx = prev.indexOfValue(getOutputFields()[i].getName());
        if (idx < 0) {
          errorMessage += "\t\t" + getOutputFields()[i].getName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "ConcatFieldsMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ConcatFieldsMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  /**
   * @return The desired encoding of output file, null or empty if the default system encoding needs
   *     to be used.
   */
  public String getEncoding() {
    return encoding;
  }

  /** @return Returns the outputFields. */
  public TextFileField[] getOutputFields() {
    return outputFields;
  }

  public void allocate(int nrFields) {
    outputFields = new TextFileField[nrFields];
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      ConcatFieldsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ConcatFields(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public ConcatFieldsData getTransformData() {
    return new ConcatFieldsData();
  }

  public void setSpecifyingFormat(boolean specifyingFormat) {
    this.specifyingFormat = specifyingFormat;
  }

  public void setDateTimeFormat(String dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  /** @return Returns the separator. */
  public String getSeparator() {
    return separator;
  }

  public void setSeparator(String separator) {
    this.separator = separator;
  }

  public String getEnclosure() {
    return enclosure;
  }

  public void setEnclosure(String enclosure) {
    this.enclosure = enclosure;
  }
}
