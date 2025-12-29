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

package org.apache.hop.pipeline.transforms.setvariable;

import java.util.ArrayList;
import java.util.Objects;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class VariableItem {

  private static final Class<?> PKG = VariableItem.class;

  public static final String VARIABLE_TYPE_ROOT_WORKFLOW = "ROOT_WORKFLOW";
  public static final String VARIABLE_TYPE_GRAND_PARENT_WORKFLOW = "GP_WORKFLOW";
  public static final String VARIABLE_TYPE_PARENT_WORKFLOW = "PARENT_WORKFLOW";
  public static final String VARIABLE_TYPE_JVM = "JVM";

  private static final String[] variableTypeCodes = {
    VARIABLE_TYPE_JVM,
    VARIABLE_TYPE_PARENT_WORKFLOW,
    VARIABLE_TYPE_GRAND_PARENT_WORKFLOW,
    VARIABLE_TYPE_ROOT_WORKFLOW,
  };

  private static final String[] variableTypeDescriptions = {
    "SetVariable.Validity.Jvm",
    "SetVariable.Validity.Parent",
    "SetVariable.Validity.GParent",
    "SetVariable.Validity.RootWf",
  };

  @HopMetadataProperty(key = "field_name")
  private String fieldName;

  @HopMetadataProperty(key = "variable_name")
  private String variableName;

  @HopMetadataProperty(key = "variable_type")
  private String variableType;

  @HopMetadataProperty(key = "default_value")
  private String defaultValue;

  public VariableItem() {}

  public VariableItem(
      String fieldName, String variableName, String variableType, String defaultValue) {
    this.fieldName = fieldName;
    this.variableName = variableName;
    this.variableType = variableType;
    this.defaultValue = defaultValue;
  }

  public static final String[] getVariableTypeDescriptionsList() {
    ArrayList<String> variableTypesList = new ArrayList<>(variableTypeDescriptions.length);

    for (String variableTypeDescription : variableTypeDescriptions) {
      variableTypesList.add(BaseMessages.getString(PKG, variableTypeDescription));
    }
    return variableTypesList.toArray(new String[0]);
  }

  /**
   * @param variableType The variable type, see also VARIABLE_TYPE_...
   * @return the variable type description for this variable type
   */
  public static final String getVariableTypeDescription(String variableType) {
    String vt = null;
    for (int i = 0; i < variableTypeCodes.length; i++) {
      if (variableTypeCodes[i].equalsIgnoreCase(variableType)) {
        vt = BaseMessages.getString(PKG, variableTypeDescriptions[i]);
      }
    }
    return vt;
  }

  /**
   * @param variableTypeDesc The code or description of the variable type
   * @return The variable type
   */
  public static final String getVariableTypeFromDesc(String variableTypeDesc) {

    String vt = variableTypeCodes[0];
    for (int i = 0; i < variableTypeDescriptions.length; i++) {
      if (BaseMessages.getString(PKG, variableTypeDescriptions[i])
          .equalsIgnoreCase(variableTypeDesc)) {
        vt = variableTypeCodes[i];
      }
    }
    return vt;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getVariableName() {
    return variableName;
  }

  public void setVariableName(String variableName) {
    this.variableName = variableName;
  }

  public String getVariableType() {
    return variableType;
  }

  public void setVariableType(String variableType) {
    this.variableType = variableType;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VariableItem that = (VariableItem) o;
    return variableType == that.variableType
        && fieldName.equals(that.fieldName)
        && variableName.equals(that.variableName)
        && defaultValue.equals(that.defaultValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, variableName, variableType, defaultValue);
  }
}
