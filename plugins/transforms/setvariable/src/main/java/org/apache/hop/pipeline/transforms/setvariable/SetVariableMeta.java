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

package org.apache.hop.pipeline.transforms.setvariable;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
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

/**
 * Sets environment variables based on content in certain fields of a single input row.
 *
 * <p>Created on 27-apr-2006
 */
@Transform(
    id = "SetVariable",
    image = "setvariable.svg",
    name = "i18n::BaseTransform.TypeLongDesc.SetVariable",
    description = "i18n::BaseTransform.TypeTooltipDesc.SetVariable",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Workflow",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/setvariable.html")
public class SetVariableMeta extends BaseTransformMeta
    implements ITransformMeta<SetVariable, SetVariableData> {
  private static final Class<?> PKG = SetVariableMeta.class; // For Translator

  public static final int VARIABLE_TYPE_JVM = 0;
  public static final int VARIABLE_TYPE_PARENT_WORKFLOW = 1;
  public static final int VARIABLE_TYPE_GRAND_PARENT_WORKFLOW = 2;
  public static final int VARIABLE_TYPE_ROOT_WORKFLOW = 3;

  private static final String[] variableTypeCode = {
    "JVM", "PARENT_WORKFLOW", "GP_WORKFLOW", "ROOT_WORKFLOW",
  };

  private static final String[] variableTypeDesc = {
    "Valid in the Java Virtual Machine",
    "Valid in the parent workflow",
    "Valid in the grand-parent workflow",
    "Valid in the root workflow",
  };

  private String[] fieldName;
  private String[] variableName;
  private int[] variableType;
  private String[] defaultValue;

  private boolean usingFormatting;

  public SetVariableMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the fieldName. */
  public String[] getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set. */
  public void setFieldName(String[] fieldName) {
    this.fieldName = fieldName;
  }

  /** @param fieldValue The fieldValue to set. */
  public void setVariableName(String[] fieldValue) {
    this.variableName = fieldValue;
  }

  /** @return Returns the fieldValue. */
  public String[] getVariableName() {
    return variableName;
  }

  /**
   * @return Returns the local variable flag: true if this variable is only valid in the parents
   *     workflow.
   */
  public int[] getVariableType() {
    return variableType;
  }

  /** @return Returns the defaultValue. */
  public String[] getDefaultValue() {
    return defaultValue;
  }

  /** @param defaultValue The defaultValue to set. */
  public void setDefaultValue(String[] defaultValue) {
    this.defaultValue = defaultValue;
  }

  /**
   * @param variableType The variable type, see also VARIABLE_TYPE_...
   * @return the variable type code for this variable type
   */
  public static final String getVariableTypeCode(int variableType) {
    return variableTypeCode[variableType];
  }

  /**
   * @param variableType The variable type, see also VARIABLE_TYPE_...
   * @return the variable type description for this variable type
   */
  public static final String getVariableTypeDescription(int variableType) {
    return variableTypeDesc[variableType];
  }

  /**
   * @param variableType The code or description of the variable type
   * @return The variable type
   */
  public static final int getVariableType(String variableType) {
    for (int i = 0; i < variableTypeCode.length; i++) {
      if (variableTypeCode[i].equalsIgnoreCase(variableType)) {
        return i;
      }
    }
    for (int i = 0; i < variableTypeDesc.length; i++) {
      if (variableTypeDesc[i].equalsIgnoreCase(variableType)) {
        return i;
      }
    }
    return VARIABLE_TYPE_JVM;
  }

  /** @param localVariable The localVariable to set. */
  public void setVariableType(int[] localVariable) {
    this.variableType = localVariable;
  }

  public static final String[] getVariableTypeDescriptions() {
    return variableTypeDesc;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int count) {
    fieldName = new String[count];
    variableName = new String[count];
    variableType = new int[count];
    defaultValue = new String[count];
  }

  public Object clone() {
    SetVariableMeta retval = (SetVariableMeta) super.clone();

    int count = fieldName.length;

    retval.allocate(count);
    System.arraycopy(fieldName, 0, retval.fieldName, 0, count);
    System.arraycopy(variableName, 0, retval.variableName, 0, count);
    System.arraycopy(variableType, 0, retval.variableType, 0, count);
    System.arraycopy(defaultValue, 0, retval.defaultValue, 0, count);

    return retval;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int count = XmlHandler.countNodes(fields, "field");

      allocate(count);

      for (int i = 0; i < count; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldName[i] = XmlHandler.getTagValue(fnode, "field_name");
        variableName[i] = XmlHandler.getTagValue(fnode, "variable_name");
        variableType[i] = getVariableType(XmlHandler.getTagValue(fnode, "variable_type"));
        defaultValue[i] = XmlHandler.getTagValue(fnode, "default_value");
      }

      // Default to "N" for backward compatibility
      //
      usingFormatting =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "use_formatting"));
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "SetVariableMeta.RuntimeError.UnableToReadXML.SETVARIABLE0004"),
          e);
    }
  }

  public void setDefault() {
    int count = 0;

    allocate(count);

    for (int i = 0; i < count; i++) {
      fieldName[i] = "field" + i;
      variableName[i] = "";
      variableType[i] = VARIABLE_TYPE_JVM;
      defaultValue[i] = "";
    }

    usingFormatting = true;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(150);

    retval.append("    <fields>").append(Const.CR);

    for (int i = 0; i < fieldName.length; i++) {
      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("field_name", fieldName[i]));
      retval.append("        ").append(XmlHandler.addTagValue("variable_name", variableName[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("variable_type", getVariableTypeCode(variableType[i])));
      retval.append("        ").append(XmlHandler.addTagValue("default_value", defaultValue[i]));
      retval.append("        </field>").append(Const.CR);
    }
    retval.append("      </fields>").append(Const.CR);

    retval.append("    ").append(XmlHandler.addTagValue("use_formatting", usingFormatting));

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
    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "SetVariableMeta.CheckResult.NotReceivingFieldsFromPreviousTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "SetVariableMeta.CheckResult.ReceivingFieldsFromPreviousTransforms",
                  "" + prev.size()),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SetVariableMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "SetVariableMeta.CheckResult.NotReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public SetVariable createTransform(
      TransformMeta transformMeta,
      SetVariableData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SetVariable(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public SetVariableData getTransformData() {
    return new SetVariableData();
  }

  /** @return the usingFormatting */
  public boolean isUsingFormatting() {
    return usingFormatting;
  }

  /** @param usingFormatting the usingFormatting to set */
  public void setUsingFormatting(boolean usingFormatting) {
    this.usingFormatting = usingFormatting;
  }
}
