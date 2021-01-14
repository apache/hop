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

package org.apache.hop.pipeline.transforms.constant;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
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

import java.text.DecimalFormat;
import java.util.List;

/*
 * Created on 4-apr-2003
 *
 */
@Transform(
    id = "Constant",
    image = "constant.svg",
    name = "i18n::BaseTransform.TypeLongDesc.AddConstants",
    description = "i18n::BaseTransform.TypeTooltipDesc.AddConstants",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/constant.html")
public class ConstantMeta extends BaseTransformMeta
    implements ITransformMeta<Constant, ConstantData> {

  private static final Class<?> PKG = ConstantMeta.class; // For Translator

  private String[] currency;
  private String[] decimal;
  private String[] group;
  private String[] value; // Null-if

  private String[] fieldName;
  private String[] fieldType;
  private String[] fieldFormat;

  private int[] fieldLength;
  private int[] fieldPrecision;
  /** Flag : set empty string */
  private boolean[] setEmptyString;

  public ConstantMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the currency. */
  public String[] getCurrency() {
    return currency;
  }

  /** @param currency The currency to set. */
  public void setCurrency(String[] currency) {
    this.currency = currency;
  }

  /** @return Returns the decimal. */
  public String[] getDecimal() {
    return decimal;
  }

  /** @param decimal The decimal to set. */
  public void setDecimal(String[] decimal) {
    this.decimal = decimal;
  }

  /** @return Returns the fieldFormat. */
  public String[] getFieldFormat() {
    return fieldFormat;
  }

  /** @param fieldFormat The fieldFormat to set. */
  public void setFieldFormat(String[] fieldFormat) {
    this.fieldFormat = fieldFormat;
  }

  /** @return Returns the fieldLength. */
  public int[] getFieldLength() {
    return fieldLength;
  }

  /** @param fieldLength The fieldLength to set. */
  public void setFieldLength(int[] fieldLength) {
    this.fieldLength = fieldLength;
  }

  /** @return Returns the fieldName. */
  public String[] getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set. */
  public void setFieldName(String[] fieldName) {
    this.fieldName = fieldName;
  }

  /** @return Returns the fieldPrecision. */
  public int[] getFieldPrecision() {
    return fieldPrecision;
  }

  /** @param fieldPrecision The fieldPrecision to set. */
  public void setFieldPrecision(int[] fieldPrecision) {
    this.fieldPrecision = fieldPrecision;
  }

  /** @return Returns the fieldType. */
  public String[] getFieldType() {
    return fieldType;
  }

  /** @param fieldType The fieldType to set. */
  public void setFieldType(String[] fieldType) {
    this.fieldType = fieldType;
  }

  /**
   * @return the setEmptyString
   * @deprecated use {@link #isEmptyString()} instead
   */
  @Deprecated
  public boolean[] isSetEmptyString() {
    return setEmptyString;
  }

  public boolean[] isEmptyString() {
    return setEmptyString;
  }

  /** @param setEmptyString the setEmptyString to set */
  public void setEmptyString(boolean[] setEmptyString) {
    this.setEmptyString = setEmptyString;
  }

  /** @return Returns the group. */
  public String[] getGroup() {
    return group;
  }

  /** @param group The group to set. */
  public void setGroup(String[] group) {
    this.group = group;
  }

  /** @return Returns the value. */
  public String[] getValue() {
    return value;
  }

  /** @param value The value to set. */
  public void setValue(String[] value) {
    this.value = value;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrFields) {
    fieldName = new String[nrFields];
    fieldType = new String[nrFields];
    fieldFormat = new String[nrFields];
    fieldLength = new int[nrFields];
    fieldPrecision = new int[nrFields];
    currency = new String[nrFields];
    decimal = new String[nrFields];
    group = new String[nrFields];
    value = new String[nrFields];
    setEmptyString = new boolean[nrFields];
  }

  public Object clone() {
    ConstantMeta retval = (ConstantMeta) super.clone();

    int nrFields = fieldName.length;

    retval.allocate(nrFields);
    System.arraycopy(fieldName, 0, retval.fieldName, 0, nrFields);
    System.arraycopy(fieldType, 0, retval.fieldType, 0, nrFields);
    System.arraycopy(fieldFormat, 0, retval.fieldFormat, 0, nrFields);
    System.arraycopy(currency, 0, retval.currency, 0, nrFields);
    System.arraycopy(decimal, 0, retval.decimal, 0, nrFields);
    System.arraycopy(group, 0, retval.group, 0, nrFields);
    System.arraycopy(value, 0, retval.value, 0, nrFields);
    System.arraycopy(fieldLength, 0, retval.fieldLength, 0, nrFields);
    System.arraycopy(fieldPrecision, 0, retval.fieldPrecision, 0, nrFields);
    System.arraycopy(setEmptyString, 0, retval.setEmptyString, 0, nrFields);

    return retval;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      String slength, sprecision;

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldName[i] = XmlHandler.getTagValue(fnode, "name");
        fieldType[i] = XmlHandler.getTagValue(fnode, "type");
        fieldFormat[i] = XmlHandler.getTagValue(fnode, "format");
        currency[i] = XmlHandler.getTagValue(fnode, "currency");
        decimal[i] = XmlHandler.getTagValue(fnode, "decimal");
        group[i] = XmlHandler.getTagValue(fnode, "group");
        value[i] = XmlHandler.getTagValue(fnode, "nullif");
        slength = XmlHandler.getTagValue(fnode, "length");
        sprecision = XmlHandler.getTagValue(fnode, "precision");

        fieldLength[i] = Const.toInt(slength, -1);
        fieldPrecision[i] = Const.toInt(sprecision, -1);
        String emptyString = XmlHandler.getTagValue(fnode, "set_empty_string");
        setEmptyString[i] = !Utils.isEmpty(emptyString) && "Y".equalsIgnoreCase(emptyString);
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public void setDefault() {
    int i, nrFields = 0;

    allocate(nrFields);

    DecimalFormat decimalFormat = new DecimalFormat();

    for (i = 0; i < nrFields; i++) {
      fieldName[i] = "field" + i;
      fieldType[i] = "Number";
      fieldFormat[i] = "\u00A40,000,000.00;\u00A4-0,000,000.00";
      fieldLength[i] = 9;
      fieldPrecision[i] = 2;
      currency[i] = decimalFormat.getDecimalFormatSymbols().getCurrencySymbol();
      decimal[i] =
          new String(new char[] {decimalFormat.getDecimalFormatSymbols().getDecimalSeparator()});
      group[i] =
          new String(new char[] {decimalFormat.getDecimalFormatSymbols().getGroupingSeparator()});
      value[i] = "-";
      setEmptyString[i] = false;
    }
  }

  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (int i = 0; i < fieldName.length; i++) {
      if (fieldName[i] != null && fieldName[i].length() != 0) {
        int type = ValueMetaFactory.getIdForValueMeta(fieldType[i]);
        if (type == IValueMeta.TYPE_NONE) {
          type = IValueMeta.TYPE_STRING;
        }
        try {
          IValueMeta v = ValueMetaFactory.createValueMeta(fieldName[i], type);
          v.setLength(fieldLength[i]);
          v.setPrecision(fieldPrecision[i]);
          v.setOrigin(name);
          v.setConversionMask(fieldFormat[i]);
          rowMeta.addValueMeta(v);
        } catch (Exception e) {
          throw new HopTransformException(e);
        }
      }
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < fieldName.length; i++) {
      if (fieldName[i] != null && fieldName[i].length() != 0) {
        retval.append("      <field>").append(Const.CR);
        retval.append("        ").append(XmlHandler.addTagValue("name", fieldName[i]));
        retval.append("        ").append(XmlHandler.addTagValue("type", fieldType[i]));
        retval.append("        ").append(XmlHandler.addTagValue("format", fieldFormat[i]));
        retval.append("        ").append(XmlHandler.addTagValue("currency", currency[i]));
        retval.append("        ").append(XmlHandler.addTagValue("decimal", decimal[i]));
        retval.append("        ").append(XmlHandler.addTagValue("group", group[i]));
        retval.append("        ").append(XmlHandler.addTagValue("nullif", value[i]));
        retval.append("        ").append(XmlHandler.addTagValue("length", fieldLength[i]));
        retval.append("        ").append(XmlHandler.addTagValue("precision", fieldPrecision[i]));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("set_empty_string", setEmptyString[i]));
        retval.append("      </field>").append(Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);

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
    CheckResult cr;
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ConstantMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ConstantMeta.CheckResult.NoFields"),
              transformMeta);
      remarks.add(cr);
    }

    // Check the constants...
    ConstantData data = new ConstantData();
    ConstantMeta meta = (ConstantMeta) transformMeta.getTransform();
    Constant.buildRow(meta, data, remarks);
  }

  public Constant createTransform(
      TransformMeta transformMeta,
      ConstantData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Constant(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public ConstantData getTransformData() {
    return new ConstantData();
  }
}
