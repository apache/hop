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

package org.apache.hop.pipeline.transforms.fieldsplitter;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.DataTypeConverter;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 31-okt-2003
 *
 */

/**
 * <CODE>
 * Example1:<p>
 * -------------<p>
 * DATUM;VALUES<p>
 * 20031031;500,300,200,100<p>
 * <p>
 * ||<t>        delimiter     = ,<p>
 * \||/<t>       field[]       = SALES1, SALES2, SALES3, SALES4<p>
 * \/<t>        id[]          = <empty><p>
 * <t>        idrem[]       = no, no, no, no<p>
 * <t>       type[]        = Number, Number, Number, Number<p>
 * <t>      format[]      = ###.##, ###.##, ###.##, ###.##<p>
 * <t>      group[]       = <empty><p>
 * <t>      decimal[]     = .<p>
 * <t>      currency[]    = <empty><p>
 * <t>      length[]      = 3, 3, 3, 3<p>
 * <t>      precision[]   = 0, 0, 0, 0<p>
 * <p>
 * DATUM;SALES1;SALES2;SALES3;SALES4<p>
 * 20031031;500;300;200;100<p>
 * <p>
 * Example2:<p>
 * -----------<p>
 * <p>
 * 20031031;Sales2=310.50, Sales4=150.23<p>
 * <p>
 * ||        delimiter     = ,<p>
 * \||/       field[]       = SALES1, SALES2, SALES3, SALES4<p>
 * \/        id[]          = Sales1, Sales2, Sales3, Sales4<p>
 * idrem[]       = yes, yes, yes, yes (remove ID's from split field)<p>
 * type[]        = Number, Number, Number, Number<p>
 * format[]      = ###.##, ###.##, ###.##, ###.##<p>
 * group[]       = <empty><p>
 * decimal[]     = .<p>
 * currency[]    = <empty><p>
 * length[]      = 3, 3, 3, 3<p>
 * precision[]   = 0, 0, 0, 0<p>
 * <p>
 * DATUM;SALES1;SALES2;SALES3;SALES4<p>
 * 20031031;310,50;150,23<p>
 * <p>
 *
 * </CODE>
 */
@InjectionSupported(
    localizationPrefix = "FieldSplitter.Injection.",
    groups = {"FIELDS"})
@Transform(
    id = "FieldSplitter",
    image = "fieldsplitter.svg",
    name = "i18n::SplitFields.Name",
    description = "i18n::SplitFields.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/fieldsplitter.html")
public class FieldSplitterMeta extends BaseTransformMeta
    implements ITransformMeta<FieldSplitter, FieldSplitterData> {
  private static final Class<?> PKG = FieldSplitterMeta.class; // For Translator

  /** Field to split */
  @Injection(name = "FIELD_TO_SPLIT")
  private String splitField;

  /** Split fields based upon this delimiter. */
  @Injection(name = "DELIMITER")
  private String delimiter;

  /** Ignore delimiter inside pairs of the enclosure string */
  @Injection(name = "ENCLOSURE")
  private String enclosure;

  /** Ignore delimiter when preceded by an escape string */
  @Injection(name = "ESCAPE_STRING")
  private String escapeString;

  /** new field names */
  @Injection(name = "NAME", group = "FIELDS")
  private String[] fieldName;

  /** Field ID's to scan for */
  @Injection(name = "ID", group = "FIELDS")
  private String[] fieldID;

  /** flag: remove ID */
  @Injection(name = "REMOVE_ID", group = "FIELDS")
  private boolean[] fieldRemoveID;

  /** type of new field */
  @Injection(name = "DATA_TYPE", group = "FIELDS", converter = DataTypeConverter.class)
  private int[] fieldType;

  /** formatting mask to convert value */
  @Injection(name = "FORMAT", group = "FIELDS")
  private String[] fieldFormat;

  /** Grouping symbol */
  @Injection(name = "GROUPING", group = "FIELDS")
  private String[] fieldGroup;

  /** Decimal point . or , */
  @Injection(name = "DECIMAL", group = "FIELDS")
  private String[] fieldDecimal;

  /** Currency symbol */
  @Injection(name = "CURRENCY", group = "FIELDS")
  private String[] fieldCurrency;

  /** Length of field */
  @Injection(name = "LENGTH", group = "FIELDS")
  private int[] fieldLength;

  /** Precision of field */
  @Injection(name = "PRECISION", group = "FIELDS")
  private int[] fieldPrecision;

  /** Replace this value with a null */
  @Injection(name = "NULL_IF", group = "FIELDS")
  private String[] fieldNullIf;

  /** Default value in case no value was found (ID option) */
  @Injection(name = "DEFAULT", group = "FIELDS")
  private String[] fieldIfNull;

  /** Perform trimming of this type on the fieldName during lookup and storage */
  @Injection(name = "TRIM_TYPE", group = "FIELDS", converter = TrimTypeConverter.class)
  private int[] fieldTrimType;

  public FieldSplitterMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getSplitField() {
    return splitField;
  }

  public void setSplitField(final String splitField) {
    this.splitField = splitField;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(final String delimiter) {
    this.delimiter = delimiter;
  }

  public String getEnclosure() {
    return enclosure;
  }

  public void setEnclosure(final String enclosure) {
    this.enclosure = enclosure;
  }

  /**
   * Gets escapeString
   *
   * @return value of escapeString
   */
  public String getEscapeString() {
    return escapeString;
  }

  /** @param escapeString The escapeString to set */
  public void setEscapeString(String escapeString) {
    this.escapeString = escapeString;
  }

  public String[] getFieldName() {
    return fieldName;
  }

  public void setFieldName(final String[] fieldName) {
    this.fieldName = fieldName;
  }

  public String[] getFieldID() {
    return fieldID;
  }

  public void setFieldID(final String[] fieldID) {
    this.fieldID = fieldID;
  }

  public boolean[] getFieldRemoveID() {
    return fieldRemoveID;
  }

  public void setFieldRemoveID(final boolean[] fieldRemoveID) {
    this.fieldRemoveID = fieldRemoveID;
  }

  public int[] getFieldType() {
    return fieldType;
  }

  public void setFieldType(final int[] fieldType) {
    this.fieldType = fieldType;
  }

  public String[] getFieldFormat() {
    return fieldFormat;
  }

  public void setFieldFormat(final String[] fieldFormat) {
    this.fieldFormat = fieldFormat;
  }

  public String[] getFieldGroup() {
    return fieldGroup;
  }

  public void setFieldGroup(final String[] fieldGroup) {
    this.fieldGroup = fieldGroup;
  }

  public String[] getFieldDecimal() {
    return fieldDecimal;
  }

  public void setFieldDecimal(final String[] fieldDecimal) {
    this.fieldDecimal = fieldDecimal;
  }

  public String[] getFieldCurrency() {
    return fieldCurrency;
  }

  public void setFieldCurrency(final String[] fieldCurrency) {
    this.fieldCurrency = fieldCurrency;
  }

  public int[] getFieldLength() {
    return fieldLength;
  }

  public void setFieldLength(final int[] fieldLength) {
    this.fieldLength = fieldLength;
  }

  public int[] getFieldPrecision() {
    return fieldPrecision;
  }

  public void setFieldPrecision(final int[] fieldPrecision) {
    this.fieldPrecision = fieldPrecision;
  }

  public String[] getFieldNullIf() {
    return fieldNullIf;
  }

  public void setFieldNullIf(final String[] fieldNullIf) {
    this.fieldNullIf = fieldNullIf;
  }

  public String[] getFieldIfNull() {
    return fieldIfNull;
  }

  public void setFieldIfNull(final String[] fieldIfNull) {
    this.fieldIfNull = fieldIfNull;
  }

  public int[] getFieldTrimType() {
    return fieldTrimType;
  }

  public void setFieldTrimType(final int[] fieldTrimType) {
    this.fieldTrimType = fieldTrimType;
  }

  public void allocate(int nrFields) {
    fieldName = new String[nrFields];
    fieldID = new String[nrFields];
    fieldRemoveID = new boolean[nrFields];
    fieldType = new int[nrFields];
    fieldFormat = new String[nrFields];
    fieldGroup = new String[nrFields];
    fieldDecimal = new String[nrFields];
    fieldCurrency = new String[nrFields];
    fieldLength = new int[nrFields];
    fieldPrecision = new int[nrFields];
    fieldNullIf = new String[nrFields];
    fieldIfNull = new String[nrFields];
    fieldTrimType = new int[nrFields];
  }

  public Object clone() {
    FieldSplitterMeta retval = (FieldSplitterMeta) super.clone();

    final int nrFields = fieldName.length;

    retval.allocate(nrFields);

    System.arraycopy(fieldName, 0, retval.fieldName, 0, nrFields);
    System.arraycopy(fieldID, 0, retval.fieldID, 0, nrFields);
    System.arraycopy(fieldRemoveID, 0, retval.fieldRemoveID, 0, nrFields);
    System.arraycopy(fieldType, 0, retval.fieldType, 0, nrFields);
    System.arraycopy(fieldLength, 0, retval.fieldLength, 0, nrFields);
    System.arraycopy(fieldPrecision, 0, retval.fieldPrecision, 0, nrFields);
    System.arraycopy(fieldFormat, 0, retval.fieldFormat, 0, nrFields);
    System.arraycopy(fieldGroup, 0, retval.fieldGroup, 0, nrFields);
    System.arraycopy(fieldDecimal, 0, retval.fieldDecimal, 0, nrFields);
    System.arraycopy(fieldCurrency, 0, retval.fieldCurrency, 0, nrFields);
    System.arraycopy(fieldNullIf, 0, retval.fieldNullIf, 0, nrFields);
    System.arraycopy(fieldIfNull, 0, retval.fieldIfNull, 0, nrFields);
    System.arraycopy(fieldTrimType, 0, retval.fieldTrimType, 0, nrFields);

    return retval;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      splitField = XmlHandler.getTagValue(transformNode, "splitfield");
      delimiter = XmlHandler.getTagValue(transformNode, "delimiter");
      enclosure = XmlHandler.getTagValue(transformNode, "enclosure");
      escapeString = XmlHandler.getTagValue(transformNode, "escape_string");

      final Node fields = XmlHandler.getSubNode(transformNode, "fields");
      final int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        final Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldName[i] = XmlHandler.getTagValue(fnode, "name");
        fieldID[i] = XmlHandler.getTagValue(fnode, "id");
        final String sidrem = XmlHandler.getTagValue(fnode, "idrem");
        final String stype = XmlHandler.getTagValue(fnode, "type");
        fieldFormat[i] = XmlHandler.getTagValue(fnode, "format");
        fieldGroup[i] = XmlHandler.getTagValue(fnode, "group");
        fieldDecimal[i] = XmlHandler.getTagValue(fnode, "decimal");
        fieldCurrency[i] = XmlHandler.getTagValue(fnode, "currency");
        final String slen = XmlHandler.getTagValue(fnode, "length");
        final String sprc = XmlHandler.getTagValue(fnode, "precision");
        fieldNullIf[i] = XmlHandler.getTagValue(fnode, "nullif");
        fieldIfNull[i] = XmlHandler.getTagValue(fnode, "ifnull");
        final String trim = XmlHandler.getTagValue(fnode, "trimtype");

        fieldRemoveID[i] = "Y".equalsIgnoreCase(sidrem);
        fieldType[i] = ValueMetaFactory.getIdForValueMeta(stype);
        fieldLength[i] = Const.toInt(slen, -1);
        fieldPrecision[i] = Const.toInt(sprc, -1);
        fieldTrimType[i] = ValueMetaString.getTrimTypeByCode(trim);
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "FieldSplitterMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    splitField = "";
    delimiter = ",";
    enclosure = null;
    allocate(0);
  }

  public int getFieldsCount() {
    int count = Math.min(getFieldName().length, getFieldType().length);
    count = Math.min(count, getFieldLength().length);
    count = Math.min(count, getFieldPrecision().length);
    count = Math.min(count, getFieldFormat().length);
    count = Math.min(count, getFieldDecimal().length);
    count = Math.min(count, getFieldGroup().length);
    count = Math.min(count, getFieldCurrency().length);
    count = Math.min(count, getFieldTrimType().length);
    return count;
  }

  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Remove the field to split
    int idx = r.indexOfValue(getSplitField());
    if (idx < 0) { // not found
      throw new RuntimeException(
          BaseMessages.getString(
              PKG, "FieldSplitter.Log.CouldNotFindFieldToSplit", getSplitField()));
    }

    // Add the new fields at the place of the index --> replace!
    int count = getFieldsCount();
    for (int i = 0; i < count; i++) {
      try {
        final IValueMeta v = ValueMetaFactory.createValueMeta(getFieldName()[i], getFieldType()[i]);
        v.setLength(getFieldLength()[i], getFieldPrecision()[i]);
        v.setOrigin(name);
        v.setConversionMask(getFieldFormat()[i]);
        v.setDecimalSymbol(getFieldDecimal()[i]);
        v.setGroupingSymbol(getFieldGroup()[i]);
        v.setCurrencySymbol(getFieldCurrency()[i]);
        v.setTrimType(getFieldTrimType()[i]);
        // TODO when implemented in UI
        // v.setDateFormatLenient(dateFormatLenient);
        // TODO when implemented in UI
        // v.setDateFormatLocale(dateFormatLocale);
        if (i == 0 && idx >= 0) {
          // the first valueMeta (splitField) will be replaced
          r.setValueMeta(idx, v);
        } else {
          // other valueMeta will be added
          if (idx >= r.size()) {
            r.addValueMeta(v);
          }
          r.addValueMeta(idx + i, v);
        }
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }
  }

  public String getXml() {
    final StringBuilder retval = new StringBuilder(500);

    retval
        .append("   ")
        .append(XmlHandler.addTagValue("splitfield", splitField))
        .append("   ")
        .append(XmlHandler.addTagValue("delimiter", delimiter))
        .append("   ")
        .append(XmlHandler.addTagValue("enclosure", enclosure))
        .append("   ")
        .append(XmlHandler.addTagValue("escape_string", escapeString));

    retval.append("   ").append("<fields>");
    for (int i = 0; i < fieldName.length; i++) {
      retval
          .append("      ")
          .append("<field>")
          .append("        ")
          .append(XmlHandler.addTagValue("name", fieldName[i]))
          .append("        ")
          .append(XmlHandler.addTagValue("id", ArrayUtils.isEmpty(fieldID) ? null : fieldID[i]))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "idrem", ArrayUtils.isEmpty(fieldRemoveID) ? false : fieldRemoveID[i]))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "type",
                  ValueMetaFactory.getValueMetaName(
                      ArrayUtils.isEmpty(fieldType) ? 0 : fieldType[i])))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "format", ArrayUtils.isEmpty(fieldFormat) ? null : fieldFormat[i]))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "group", ArrayUtils.isEmpty(fieldGroup) ? null : fieldGroup[i]))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "decimal", ArrayUtils.isEmpty(fieldDecimal) ? null : fieldDecimal[i]))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "currency", ArrayUtils.isEmpty(fieldCurrency) ? null : fieldCurrency[i]))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "length", ArrayUtils.isEmpty(fieldLength) ? -1 : fieldLength[i]))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "precision", ArrayUtils.isEmpty(fieldPrecision) ? -1 : fieldPrecision[i]))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "nullif", ArrayUtils.isEmpty(fieldNullIf) ? null : fieldNullIf[i]))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "ifnull", ArrayUtils.isEmpty(fieldIfNull) ? null : fieldIfNull[i]))
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "trimtype",
                  ValueMetaString.getTrimTypeCode(
                      ArrayUtils.isEmpty(fieldTrimType) ? 0 : fieldTrimType[i])))
          .append("      ")
          .append("</field>");
    }
    retval.append("    ").append("</fields>");

    return retval.toString();
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
    String errorMessage = "";
    CheckResult cr;

    // Look up fields in the input stream <prev>
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FieldSplitterMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      errorMessage = "";

      int i = prev.indexOfValue(splitField);
      if (i < 0) {
        errorMessage =
            BaseMessages.getString(
                PKG,
                "FieldSplitterMeta.CheckResult.SplitedFieldNotPresentInInputStream",
                splitField);
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "FieldSplitterMeta.CheckResult.SplitedFieldFoundInInputStream",
                    splitField),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      errorMessage =
          BaseMessages.getString(
                  PKG, "FieldSplitterMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform")
              + Const.CR;
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FieldSplitterMeta.CheckResult.TransformReceivingInfoFromOtherTransform"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FieldSplitterMeta.CheckResult.NoInputReceivedFromOtherTransform"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public FieldSplitter createTransform(
      TransformMeta transformMeta,
      FieldSplitterData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new FieldSplitter(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public FieldSplitterData getTransformData() {
    return new FieldSplitterData();
  }

  /**
   * If we use injection we can have different arrays lengths. We need synchronize them for
   * consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = (fieldName == null ? -1 : fieldName.length);
    if (nrFields <= 0) {
      return;
    }

    String[][] normalizedStringArrays =
        Utils.normalizeArrays(
            nrFields,
            fieldID,
            fieldFormat,
            fieldGroup,
            fieldDecimal,
            fieldCurrency,
            fieldNullIf,
            fieldIfNull);
    fieldID = normalizedStringArrays[0];
    fieldFormat = normalizedStringArrays[1];
    fieldGroup = normalizedStringArrays[2];
    fieldDecimal = normalizedStringArrays[3];
    fieldCurrency = normalizedStringArrays[4];
    fieldNullIf = normalizedStringArrays[5];
    fieldIfNull = normalizedStringArrays[6];

    boolean[][] normalizedBooleanArrays = Utils.normalizeArrays(nrFields, fieldRemoveID);
    fieldRemoveID = normalizedBooleanArrays[0];

    int[][] normalizedIntArrays =
        Utils.normalizeArrays(nrFields, fieldType, fieldLength, fieldPrecision, fieldTrimType);
    fieldType = normalizedIntArrays[0];
    fieldLength = normalizedIntArrays[1];
    fieldPrecision = normalizedIntArrays[2];
    fieldTrimType = normalizedIntArrays[3];
  }
}
