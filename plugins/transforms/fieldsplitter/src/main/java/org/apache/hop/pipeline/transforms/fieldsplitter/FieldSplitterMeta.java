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
import lombok.Getter;
import lombok.Setter;
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

@Getter
@Setter
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

  /**
   * When true, keep the field being split and append the new fields at the end of the field list.
   */
  @HopMetadataProperty(
      key = "keep_split_field",
      injectionKey = "KEEP_SPLIT_FIELD",
      injectionKeyDescription = "FieldSplitter.Injection.KEEP_SPLIT_FIELD")
  private boolean keepSplitField;

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

  @Override
  public void setDefault() {
    splitField = "";
    delimiter = ",";
    enclosure = null;
    keepSplitField = false;
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

    if (keepSplitField) {
      // Keep the original field and append the new fields at the end
      //
      for (FSField field : fields) {
        try {
          final IValueMeta v = field.createValueMeta();
          v.setOrigin(name);
          r.addValueMeta(v);
        } catch (Exception e) {
          throw new HopTransformException(e);
        }
      }
      return;
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

  @Getter
  @Setter
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
  }
}
