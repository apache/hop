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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "FieldSplitter",
    image = "fieldsplitter.svg",
    name = "i18n::SplitFields.Name",
    description = "i18n::SplitFields.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::FieldSplitterMeta.keyword",
    documentationUrl = "/pipeline/transforms/splitfields.html")
public class FieldSplitterMeta extends BaseTransformMeta<FieldSplitter, FieldSplitterData> {
  private static final Class<?> PKG = FieldSplitterMeta.class;

  /** Field to split */
  @HopMetadataProperty(
      key = "splitfield",
      injectionKey = "FIELD_TO_SPLIT",
      injectionKeyDescription = "FieldSplitter.Injection.FIELD_TO_SPLIT")
  private String splitField;

  /** Split fields based upon this delimiter. */
  @HopMetadataProperty(
      key = "delimiter",
      injectionKey = "DELIMITER",
      injectionKeyDescription = "FieldSplitter.Injection.DELIMITER")
  private String delimiter;

  /** Ignore delimiter inside pairs of the enclosure string */
  @HopMetadataProperty(
      key = "enclosure",
      injectionKey = "ENCLOSURE",
      injectionKeyDescription = "FieldSplitterDialog.Enclosure.Label")
  private String enclosure;

  /** Ignore delimiter when preceded by an escape string */
  @HopMetadataProperty(
      key = "escape_string",
      injectionKey = "ESCAPE_STRING",
      injectionKeyDescription = "FieldSplitterDialog.EscapeString.Label")
  private String escapeString;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionKey = "FIELD",
      injectionGroupKey = "FIELDS",
      injectionKeyDescription = "FieldSplitter.Injection.FIELDS")
  private List<FSField> fields;

  public FieldSplitterMeta() {
    super();
    fields = new ArrayList<>();
  }

  public FieldSplitterMeta(FieldSplitterMeta m) {
    this();
    this.splitField = m.splitField;
    this.delimiter = m.delimiter;
    this.enclosure = m.enclosure;
    this.escapeString = m.escapeString;
    m.fields.forEach(f -> this.fields.add(new FSField(f)));
  }

  @Override
  public FieldSplitterMeta clone() {
    return new FieldSplitterMeta(this);
  }

  @Override
  public void setDefault() {
    splitField = "";
    delimiter = ",";
    enclosure = null;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // Get the index of the field to split
    //
    int idx = r.indexOfValue(getSplitField());
    if (idx < 0) { // not found
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "FieldSplitter.Log.CouldNotFindFieldToSplit", getSplitField()));
    }

    // Add the new fields at the place of the index --> replace!
    //
    for (int i = 0; i < fields.size(); i++) {
      FSField field = fields.get(i);
      try {
        final IValueMeta v = field.createValueMeta();
        v.setOrigin(name);
        if (i == 0) {
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

    // Look up fields in the input stream <prev>
    if (prev != null && !prev.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FieldSplitterMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta));

      int i = prev.indexOfValue(splitField);
      if (i < 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "FieldSplitterMeta.CheckResult.SplitedFieldNotPresentInInputStream",
                    splitField),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "FieldSplitterMeta.CheckResult.SplitedFieldFoundInInputStream",
                    splitField),
                transformMeta));
      }
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FieldSplitterMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform"),
              transformMeta));
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FieldSplitterMeta.CheckResult.TransformReceivingInfoFromOtherTransform"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FieldSplitterMeta.CheckResult.NoInputReceivedFromOtherTransform"),
              transformMeta));
    }
  }

  public static class FSField {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "NAME",
        injectionKeyDescription = "FieldSplitter.Injection.NAME")
    private String name;

    @HopMetadataProperty(
        key = "id",
        injectionKey = "ID",
        injectionKeyDescription = "FieldSplitter.Injection.ID")
    private String id;

    @HopMetadataProperty(
        key = "idrem",
        injectionKey = "REMOVE_ID",
        injectionKeyDescription = "FieldSplitter.Injection.REMOVE_ID")
    private boolean idRemoved;

    @HopMetadataProperty(
        key = "type",
        injectionKey = "DATA_TYPE",
        injectionKeyDescription = "FieldSplitter.Injection.DATA_TYPE")
    private String type;

    @HopMetadataProperty(
        key = "format",
        injectionKey = "FORMAT",
        injectionKeyDescription = "FieldSplitter.Injection.FORMAT")
    private String format;

    @HopMetadataProperty(
        key = "group",
        injectionKey = "GROUPING",
        injectionKeyDescription = "FieldSplitter.Injection.GROUPING")
    private String group;

    @HopMetadataProperty(
        key = "decimal",
        injectionKey = "DECIMAL",
        injectionKeyDescription = "FieldSplitter.Injection.DECIMAL")
    private String decimal;

    @HopMetadataProperty(
        key = "currency",
        injectionKey = "CURRENCY",
        injectionKeyDescription = "FieldSplitter.Injection.CURRENCY")
    private String currency;

    @HopMetadataProperty(
        key = "length",
        injectionKey = "LENGTH",
        injectionKeyDescription = "FieldSplitter.Injection.LENGTH")
    private int length;

    @HopMetadataProperty(
        key = "precision",
        injectionKey = "PRECISION",
        injectionKeyDescription = "FieldSplitter.Injection.PRECISION")
    private int precision;

    @HopMetadataProperty(
        key = "nullif",
        injectionKey = "NULL_IF",
        injectionKeyDescription = "FieldSplitter.Injection.NULL_IF")
    private String nullIf;

    @HopMetadataProperty(
        key = "ifnull",
        injectionKey = "DEFAULT",
        injectionKeyDescription = "FieldSplitter.Injection.DEFAULT")
    private String ifNull;

    @HopMetadataProperty(
        key = "trimtype",
        storeWithCode = true,
        injectionKey = "TRIM_TYPE",
        injectionKeyDescription = "FieldSplitter.Injection.TRIM_TYPE")
    private IValueMeta.TrimType trimType;

    public FSField() {
      trimType = IValueMeta.TrimType.NONE;
    }

    public FSField(FSField f) {
      this.name = f.name;
      this.id = f.id;
      this.idRemoved = f.idRemoved;
      this.type = f.type;
      this.format = f.format;
      this.group = f.group;
      this.decimal = f.decimal;
      this.currency = f.currency;
      this.length = f.length;
      this.precision = f.precision;
      this.nullIf = f.nullIf;
      this.ifNull = f.ifNull;
      this.trimType = f.trimType;
    }

    public int getHopType() {
      return ValueMetaFactory.getIdForValueMeta(type);
    }

    public IValueMeta createValueMeta() throws HopPluginException {
      int hopType = getHopType();
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, hopType);
      valueMeta.setLength(length, precision);
      valueMeta.setConversionMask(format);
      valueMeta.setDecimalSymbol(decimal);
      valueMeta.setGroupingSymbol(group);
      valueMeta.setCurrencySymbol(currency);
      valueMeta.setTrimType(trimType.getType());
      return valueMeta;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets id
     *
     * @return value of id
     */
    public String getId() {
      return id;
    }

    /**
     * Sets id
     *
     * @param id value of id
     */
    public void setId(String id) {
      this.id = id;
    }

    /**
     * Gets idRemoved
     *
     * @return value of idRemoved
     */
    public boolean isIdRemoved() {
      return idRemoved;
    }

    /**
     * Sets idRemoved
     *
     * @param idRemoved value of idRemoved
     */
    public void setIdRemoved(boolean idRemoved) {
      this.idRemoved = idRemoved;
    }

    /**
     * Gets type
     *
     * @return value of type
     */
    public String getType() {
      return type;
    }

    /**
     * Sets type
     *
     * @param type value of type
     */
    public void setType(String type) {
      this.type = type;
    }

    /**
     * Gets format
     *
     * @return value of format
     */
    public String getFormat() {
      return format;
    }

    /**
     * Sets format
     *
     * @param format value of format
     */
    public void setFormat(String format) {
      this.format = format;
    }

    /**
     * Gets group
     *
     * @return value of group
     */
    public String getGroup() {
      return group;
    }

    /**
     * Sets group
     *
     * @param group value of group
     */
    public void setGroup(String group) {
      this.group = group;
    }

    /**
     * Gets decimal
     *
     * @return value of decimal
     */
    public String getDecimal() {
      return decimal;
    }

    /**
     * Sets decimal
     *
     * @param decimal value of decimal
     */
    public void setDecimal(String decimal) {
      this.decimal = decimal;
    }

    /**
     * Gets currency
     *
     * @return value of currency
     */
    public String getCurrency() {
      return currency;
    }

    /**
     * Sets currency
     *
     * @param currency value of currency
     */
    public void setCurrency(String currency) {
      this.currency = currency;
    }

    /**
     * Gets length
     *
     * @return value of length
     */
    public int getLength() {
      return length;
    }

    /**
     * Sets length
     *
     * @param length value of length
     */
    public void setLength(int length) {
      this.length = length;
    }

    /**
     * Gets precision
     *
     * @return value of precision
     */
    public int getPrecision() {
      return precision;
    }

    /**
     * Sets precision
     *
     * @param precision value of precision
     */
    public void setPrecision(int precision) {
      this.precision = precision;
    }

    /**
     * Gets nullIf
     *
     * @return value of nullIf
     */
    public String getNullIf() {
      return nullIf;
    }

    /**
     * Sets nullIf
     *
     * @param nullIf value of nullIf
     */
    public void setNullIf(String nullIf) {
      this.nullIf = nullIf;
    }

    /**
     * Gets ifNull
     *
     * @return value of ifNull
     */
    public String getIfNull() {
      return ifNull;
    }

    /**
     * Sets ifNull
     *
     * @param ifNull value of ifNull
     */
    public void setIfNull(String ifNull) {
      this.ifNull = ifNull;
    }

    /**
     * Gets trimType
     *
     * @return value of trimType
     */
    public IValueMeta.TrimType getTrimType() {
      return trimType;
    }

    /**
     * Sets trimType
     *
     * @param trimType value of trimType
     */
    public void setTrimType(IValueMeta.TrimType trimType) {
      this.trimType = trimType;
    }
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

  /**
   * @param escapeString The escapeString to set
   */
  public void setEscapeString(String escapeString) {
    this.escapeString = escapeString;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<FSField> getFields() {
    return fields;
  }

  /**
   * Sets fields
   *
   * @param fields value of fields
   */
  public void setFields(List<FSField> fields) {
    this.fields = fields;
  }
}
