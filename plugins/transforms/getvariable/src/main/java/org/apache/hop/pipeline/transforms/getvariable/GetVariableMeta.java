/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.getvariable;

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
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IIntCodeConverter;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "GetVariable",
    image = "getvariable.svg",
    name = "i18n::GetVariable.Name",
    description = "i18n::GetVariable.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Workflow",
    keywords = "i18n::GetVariableMeta.keyword",
    documentationUrl = "/pipeline/transforms/getvariable.html")
public class GetVariableMeta extends BaseTransformMeta<GetVariable, GetVariableData> {
  private static final Class<?> PKG = GetVariableMeta.class;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "GetVariable.Injection.FIELDS")
  private List<FieldDefinition> fieldDefinitions;

  public GetVariableMeta() {
    super();
    this.fieldDefinitions = new ArrayList<>();
  }

  public GetVariableMeta(GetVariableMeta m) {
    this();
    m.fieldDefinitions.forEach(f -> this.fieldDefinitions.add(new FieldDefinition(f)));
  }

  @Override
  public GetVariableMeta clone() {
    return new GetVariableMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      for (FieldDefinition fieldDefinition : fieldDefinitions) {
        IValueMeta valueMeta = fieldDefinition.createValueMeta();
        valueMeta.setOrigin(name);
        rowMeta.addValueMeta(valueMeta);
      }
    } catch (HopPluginException e) {
      throw new HopTransformException(e);
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
    // See if we have input streams leading to this transform!
    int nrRemarks = remarks.size();
    for (FieldDefinition fieldDefinition : fieldDefinitions) {
      if (Utils.isEmpty(fieldDefinition.getVariableString())) {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "GetVariableMeta.CheckResult.VariableNotSpecified",
                    fieldDefinition.getFieldName()),
                transformMeta);
        remarks.add(cr);
      }
    }
    if (remarks.size() == nrRemarks) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "GetVariableMeta.CheckResult.AllVariablesSpecified"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public static final class IntTypeConverter implements IIntCodeConverter {
    public IntTypeConverter() {
      // Do nothing
    }

    @Override
    public String getCode(int type) {
      return ValueMetaFactory.getValueMetaName(type);
    }

    @Override
    public int getType(String code) {
      return ValueMetaFactory.getIdForValueMeta(code);
    }
  }

  public static final class FieldDefinition {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "FIELDNAME",
        injectionKeyDescription = "GetVariable.Injection.FIELDNAME")
    private String fieldName;

    @HopMetadataProperty(
        key = "variable",
        injectionKey = "VARIABLE",
        injectionKeyDescription = "GetVariable.Injection.VARIABLE")
    private String variableString;

    @HopMetadataProperty(
        key = "type",
        injectionKey = "FIELDTYPE",
        injectionKeyDescription = "GetVariable.Injection.FIELDTYPE",
        intCodeConverter = IntTypeConverter.class)
    private String fieldType;

    @HopMetadataProperty(
        key = "format",
        injectionKey = "FIELDFORMAT",
        injectionKeyDescription = "GetVariable.Injection.FIELDFORMAT")
    private String fieldFormat;

    @HopMetadataProperty(
        key = "length",
        injectionKey = "FIELDLENGTH",
        injectionKeyDescription = "GetVariable.Injection.FIELDLENGTH")
    private int fieldLength;

    @HopMetadataProperty(
        key = "precision",
        injectionKey = "FIELDPRECISION",
        injectionKeyDescription = "GetVariable.Injection.FIELDPRECISION")
    private int fieldPrecision;

    @HopMetadataProperty(
        key = "currency",
        injectionKey = "CURRENCY",
        injectionKeyDescription = "GetVariable.Injection.CURRENCY")
    private String currency;

    @HopMetadataProperty(
        key = "decimal",
        injectionKey = "DECIMAL",
        injectionKeyDescription = "GetVariable.Injection.DECIMAL")
    private String decimal;

    @HopMetadataProperty(
        key = "group",
        injectionKey = "GROUP",
        injectionKeyDescription = "GetVariable.Injection.GROUP")
    private String group;

    @HopMetadataProperty(
        key = "trim_type",
        storeWithCode = true,
        injectionKey = "TRIMTYPE",
        injectionKeyDescription = "GetVariable.Injection.TRIMTYPE")
    private IValueMeta.TrimType trimType;

    public FieldDefinition() {
      trimType = IValueMeta.TrimType.NONE;
    }

    public FieldDefinition(FieldDefinition d) {
      this();
      this.fieldName = d.fieldName;
      this.variableString = d.variableString;
      this.fieldType = d.fieldType;
      this.fieldFormat = d.fieldFormat;
      this.fieldLength = d.fieldLength;
      this.fieldPrecision = d.fieldPrecision;
      this.currency = d.currency;
      this.decimal = d.decimal;
      this.group = d.group;
      this.trimType = d.trimType;
    }

    public int getHopType() {
      return ValueMetaFactory.getIdForValueMeta(fieldType);
    }

    public IValueMeta createValueMeta() throws HopPluginException {
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta(fieldName, getHopType());
      valueMeta.setLength(fieldLength, fieldPrecision);
      valueMeta.setConversionMask(fieldFormat);
      valueMeta.setDecimalSymbol(decimal);
      valueMeta.setGroupingSymbol(group);
      valueMeta.setCurrencySymbol(currency);
      valueMeta.setTrimType(trimType.getType());
      return valueMeta;
    }

    /**
     * Gets fieldName
     *
     * @return value of fieldName
     */
    public String getFieldName() {
      return fieldName;
    }

    /**
     * Sets fieldName
     *
     * @param fieldName value of fieldName
     */
    public void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    /**
     * Gets variableString
     *
     * @return value of variableString
     */
    public String getVariableString() {
      return variableString;
    }

    /**
     * Sets variableString
     *
     * @param variableString value of variableString
     */
    public void setVariableString(String variableString) {
      this.variableString = variableString;
    }

    /**
     * Gets fieldType
     *
     * @return value of fieldType
     */
    public String getFieldType() {
      return fieldType;
    }

    /**
     * Sets fieldType
     *
     * @param fieldType value of fieldType
     */
    public void setFieldType(String fieldType) {
      this.fieldType = fieldType;
    }

    /**
     * Gets fieldFormat
     *
     * @return value of fieldFormat
     */
    public String getFieldFormat() {
      return fieldFormat;
    }

    /**
     * Sets fieldFormat
     *
     * @param fieldFormat value of fieldFormat
     */
    public void setFieldFormat(String fieldFormat) {
      this.fieldFormat = fieldFormat;
    }

    /**
     * Gets fieldLength
     *
     * @return value of fieldLength
     */
    public int getFieldLength() {
      return fieldLength;
    }

    /**
     * Sets fieldLength
     *
     * @param fieldLength value of fieldLength
     */
    public void setFieldLength(int fieldLength) {
      this.fieldLength = fieldLength;
    }

    /**
     * Gets fieldPrecision
     *
     * @return value of fieldPrecision
     */
    public int getFieldPrecision() {
      return fieldPrecision;
    }

    /**
     * Sets fieldPrecision
     *
     * @param fieldPrecision value of fieldPrecision
     */
    public void setFieldPrecision(int fieldPrecision) {
      this.fieldPrecision = fieldPrecision;
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

  /**
   * Gets fieldDefinitions
   *
   * @return value of fieldDefinitions
   */
  public List<FieldDefinition> getFieldDefinitions() {
    return fieldDefinitions;
  }

  /**
   * Sets fieldDefinitions
   *
   * @param fieldDefinitions value of fieldDefinitions
   */
  public void setFieldDefinitions(List<FieldDefinition> fieldDefinitions) {
    this.fieldDefinitions = fieldDefinitions;
  }
}
