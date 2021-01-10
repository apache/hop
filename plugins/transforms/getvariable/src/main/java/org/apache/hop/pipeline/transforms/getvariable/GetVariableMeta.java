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

package org.apache.hop.pipeline.transforms.getvariable;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
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
 * Created on 05-aug-2003
 */
@InjectionSupported(
    localizationPrefix = "GetVariable.Injection.",
    groups = {"FIELDS"})
@Transform(
    id = "GetVariable",
    image = "getvariable.svg",
    name = "i18n::BaseTransform.TypeLongDesc.GetVariable",
    description = "i18n::BaseTransform.TypeTooltipDesc.GetVariable",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Workflow",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/getvariable.html")
public class GetVariableMeta extends BaseTransformMeta
    implements ITransformMeta<GetVariable, GetVariableData> {
  private static final Class<?> PKG = GetVariableMeta.class; // For Translator

  @InjectionDeep private FieldDefinition[] fieldDefinitions;

  public GetVariableMeta() {
    super(); // allocate BaseTransformMeta
  }

  public FieldDefinition[] getFieldDefinitions() {
    return fieldDefinitions;
  }

  public void setFieldDefinitions(FieldDefinition[] fieldDefinitions) {
    this.fieldDefinitions = fieldDefinitions;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int count) {
    fieldDefinitions = new FieldDefinition[count];
    for (int i = 0; i < fieldDefinitions.length; i++) {
      fieldDefinitions[i] = new FieldDefinition();
    }
  }

  @Override
  public Object clone() {
    GetVariableMeta retval = (GetVariableMeta) super.clone();

    int count = fieldDefinitions.length;

    retval.allocate(count);
    for (int i = 0; i < count; i++) {
      retval.getFieldDefinitions()[i] = fieldDefinitions[i].clone();
    }
    return retval;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int count = XmlHandler.countNodes(fields, "field");

      allocate(count);

      for (int i = 0; i < count; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldDefinitions[i].setFieldName(XmlHandler.getTagValue(fnode, "name"));
        fieldDefinitions[i].setVariableString(XmlHandler.getTagValue(fnode, "variable"));
        fieldDefinitions[i].setFieldType(
            ValueMetaFactory.getIdForValueMeta(XmlHandler.getTagValue(fnode, "type")));
        fieldDefinitions[i].setFieldFormat(XmlHandler.getTagValue(fnode, "format"));
        fieldDefinitions[i].setCurrency(XmlHandler.getTagValue(fnode, "currency"));
        fieldDefinitions[i].setDecimal(XmlHandler.getTagValue(fnode, "decimal"));
        fieldDefinitions[i].setGroup(XmlHandler.getTagValue(fnode, "group"));
        fieldDefinitions[i].setFieldLength(
            Const.toInt(XmlHandler.getTagValue(fnode, "length"), -1));
        fieldDefinitions[i].setFieldPrecision(
            Const.toInt(XmlHandler.getTagValue(fnode, "precision"), -1));
        fieldDefinitions[i].setTrimType(
            ValueMetaString.getTrimTypeByCode(XmlHandler.getTagValue(fnode, "trim_type")));

        // Backward compatibility
        //
        if (fieldDefinitions[i].getFieldType() == IValueMeta.TYPE_NONE) {
          fieldDefinitions[i].setFieldType(IValueMeta.TYPE_STRING);
        }
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to read transform information from XML", e);
    }
  }

  @Override
  public void setDefault() {
    int count = 0;

    allocate(count);

    for (int i = 0; i < count; i++) {
      fieldDefinitions[i].setFieldName("field" + i);
      fieldDefinitions[i].setVariableString("");
    }
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Determine the maximum length...
    //
    int length = -1;
    for (int i = 0; i < fieldDefinitions.length; i++) {
      String variableString = fieldDefinitions[i].getVariableString();
      if (variableString != null) {
        String string = variables.resolve(variableString);
        if (string.length() > length) {
          length = string.length();
        }
      }
    }

    IRowMeta row = new RowMeta();
    for (int i = 0; i < fieldDefinitions.length; i++) {
      IValueMeta v;
      try {
        v =
            ValueMetaFactory.createValueMeta(
                fieldDefinitions[i].getFieldName(), fieldDefinitions[i].getFieldType());
      } catch (HopPluginException e) {
        throw new HopTransformException(e);
      }
      int fieldLength = fieldDefinitions[i].getFieldLength();
      if (fieldLength < 0) {
        v.setLength(length);
      } else {
        v.setLength(fieldLength);
      }
      int fieldPrecision = fieldDefinitions[i].getFieldPrecision();
      if (fieldPrecision >= 0) {
        v.setPrecision(fieldPrecision);
      }
      v.setConversionMask(fieldDefinitions[i].getFieldFormat());
      v.setGroupingSymbol(fieldDefinitions[i].getGroup());
      v.setDecimalSymbol(fieldDefinitions[i].getDecimal());
      v.setCurrencySymbol(fieldDefinitions[i].getCurrency());
      v.setTrimType(fieldDefinitions[i].getTrimType());
      v.setOrigin(name);

      row.addValueMeta(v);
    }

    inputRowMeta.mergeRowMeta(row, name);
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < fieldDefinitions.length; i++) {
      String fieldName = fieldDefinitions[i].getFieldName();
      if (fieldName != null && fieldName.length() != 0) {
        retval.append("      <field>").append(Const.CR);
        retval.append("        ").append(XmlHandler.addTagValue("name", fieldName));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("variable", fieldDefinitions[i].getVariableString()));
        retval
            .append("        ")
            .append(
                XmlHandler.addTagValue(
                    "type", ValueMetaFactory.getValueMetaName(fieldDefinitions[i].getFieldType())));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("format", fieldDefinitions[i].getFieldFormat()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("currency", fieldDefinitions[i].getCurrency()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("decimal", fieldDefinitions[i].getDecimal()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("group", fieldDefinitions[i].getGroup()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("length", fieldDefinitions[i].getFieldLength()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("precision", fieldDefinitions[i].getFieldPrecision()));
        retval
            .append("        ")
            .append(
                XmlHandler.addTagValue(
                    "trim_type",
                    ValueMetaString.getTrimTypeCode(fieldDefinitions[i].getTrimType())));

        retval.append("      </field>").append(Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);

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
    // See if we have input streams leading to this transform!
    int nrRemarks = remarks.size();
    for (int i = 0; i < fieldDefinitions.length; i++) {
      if (Utils.isEmpty(fieldDefinitions[i].getVariableString())) {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "GetVariableMeta.CheckResult.VariableNotSpecified",
                    fieldDefinitions[i].getFieldName()),
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

  @Override
  public GetVariable createTransform(
      TransformMeta transformMeta,
      GetVariableData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new GetVariable(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public GetVariableData getTransformData() {
    return new GetVariableData();
  }

  public static class FieldDefinition implements Cloneable {

    @Injection(name = "FIELDNAME", group = "FIELDS")
    private String fieldName;

    @Injection(name = "VARIABLE", group = "FIELDS")
    private String variableString;

    @Injection(name = "FIELDTYPE", group = "FIELDS")
    private int fieldType;

    @Injection(name = "FIELDFORMAT", group = "FIELDS")
    private String fieldFormat;

    @Injection(name = "FIELDLENGTH", group = "FIELDS")
    private int fieldLength;

    @Injection(name = "FIELDPRECISION", group = "FIELDS")
    private int fieldPrecision;

    @Injection(name = "CURRENCY", group = "FIELDS")
    private String currency;

    @Injection(name = "DECIMAL", group = "FIELDS")
    private String decimal;

    @Injection(name = "GROUP", group = "FIELDS")
    private String group;

    @Injection(name = "TRIMTYPE", group = "FIELDS")
    private int trimType;

    /** @return Returns the fieldName. */
    public String getFieldName() {
      return fieldName;
    }

    /** @param fieldName The fieldName to set. */
    public void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    /** @return Returns the strings containing variables. */
    public String getVariableString() {
      return variableString;
    }

    /** @param variableString The variable strings to set. */
    public void setVariableString(String variableString) {
      this.variableString = variableString;
    }

    /** @return the field type (IValueMeta.TYPE_*) */
    public int getFieldType() {
      return fieldType;
    }

    /** @param fieldType the field type to set (IValueMeta.TYPE_*) */
    public void setFieldType(int fieldType) {
      this.fieldType = fieldType;
    }

    /** @return the fieldFormat */
    public String getFieldFormat() {
      return fieldFormat;
    }

    /** @param fieldFormat the fieldFormat to set */
    public void setFieldFormat(String fieldFormat) {
      this.fieldFormat = fieldFormat;
    }

    /** @return the fieldLength */
    public int getFieldLength() {
      return fieldLength;
    }

    /** @param fieldLength the fieldLength to set */
    public void setFieldLength(int fieldLength) {
      this.fieldLength = fieldLength;
    }

    /** @return the fieldPrecision */
    public int getFieldPrecision() {
      return fieldPrecision;
    }

    /** @param fieldPrecision the fieldPrecision to set */
    public void setFieldPrecision(int fieldPrecision) {
      this.fieldPrecision = fieldPrecision;
    }

    /** @return the currency */
    public String getCurrency() {
      return currency;
    }

    /** @param currency the currency to set */
    public void setCurrency(String currency) {
      this.currency = currency;
    }

    /** @return the decimal */
    public String getDecimal() {
      return decimal;
    }

    /** @param decimal the decimal to set */
    public void setDecimal(String decimal) {
      this.decimal = decimal;
    }

    /** @return the group */
    public String getGroup() {
      return group;
    }

    /** @param group the group to set */
    public void setGroup(String group) {
      this.group = group;
    }

    /** @return the trimType */
    public int getTrimType() {
      return trimType;
    }

    /** @param trimType the trimType to set */
    public void setTrimType(int trimType) {
      this.trimType = trimType;
    }

    @Override
    public FieldDefinition clone() {
      try {
        return (FieldDefinition) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
