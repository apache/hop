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

package org.apache.hop.workflow.actions.simpleeval;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This defines a 'simple evaluation' action.
 *
 * @author Samatar Hassan
 * @since 01-01-2009
 */
@Action(
    id = "SIMPLE_EVAL",
    name = "i18n::ActionSimpleEval.Name",
    description = "i18n::ActionSimpleEval.Description",
    image = "SimpleEval.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/simpleeval.html")
public class ActionSimpleEval extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSimpleEval.class; // For Translator

  public static final String[] valueTypeDesc =
      new String[] {
        BaseMessages.getString(PKG, "JobSimpleEval.EvalPreviousField.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.EvalVariable.Label"),
      };
  public static final String[] valueTypeCode = new String[] {"field", "variable"};
  public static final int VALUE_TYPE_FIELD = 0;
  public static final int VALUE_TYPE_VARIABLE = 1;
  public int valuetype;

  public static final String[] successConditionDesc =
      new String[] {
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenEqual.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenDifferent.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenContains.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenNotContains.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenStartWith.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenNotStartWith.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenEndWith.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenNotEndWith.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenRegExp.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenInList.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenNotInList.Label")
      };
  public static final String[] successConditionCode =
      new String[] {
        "equal",
        "different",
        "contains",
        "notcontains",
        "startswith",
        "notstatwith",
        "endswith",
        "notendwith",
        "regexp",
        "inlist",
        "notinlist"
      };

  public static final int SUCCESS_CONDITION_EQUAL = 0;
  public static final int SUCCESS_CONDITION_DIFFERENT = 1;
  public static final int SUCCESS_CONDITION_CONTAINS = 2;
  public static final int SUCCESS_CONDITION_NOT_CONTAINS = 3;
  public static final int SUCCESS_CONDITION_START_WITH = 4;
  public static final int SUCCESS_CONDITION_NOT_START_WITH = 5;
  public static final int SUCCESS_CONDITION_END_WITH = 6;
  public static final int SUCCESS_CONDITION_NOT_END_WITH = 7;
  public static final int SUCCESS_CONDITION_REGEX = 8;
  public static final int SUCCESS_CONDITION_IN_LIST = 9;
  public static final int SUCCESS_CONDITION_NOT_IN_LIST = 10;

  public int successcondition;

  public static final String[] fieldTypeDesc =
      new String[] {
        BaseMessages.getString(PKG, "JobSimpleEval.FieldTypeString.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.FieldTypeNumber.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.FieldTypeDateTime.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.FieldTypeBoolean.Label"),
      };
  public static final String[] fieldTypeCode =
      new String[] {"string", "number", "datetime", "boolean"};
  public static final int FIELD_TYPE_STRING = 0;
  public static final int FIELD_TYPE_NUMBER = 1;
  public static final int FIELD_TYPE_DATE_TIME = 2;
  public static final int FIELD_TYPE_BOOLEAN = 3;

  public int fieldtype;

  public static final String[] successNumberConditionDesc =
      new String[] {
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenEqual.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenDifferent.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenSmallThan.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenSmallOrEqualThan.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenGreaterThan.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenGreaterOrEqualThan.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessBetween.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenInList.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenNotInList.Label"),
      };
  public static final String[] successNumberConditionCode =
      new String[] {
        "equal",
        "different",
        "smaller",
        "smallequal",
        "greater",
        "greaterequal",
        "between",
        "inlist",
        "notinlist"
      };
  public static final int SUCCESS_NUMBER_CONDITION_EQUAL = 0;
  public static final int SUCCESS_NUMBER_CONDITION_DIFFERENT = 1;
  public static final int SUCCESS_NUMBER_CONDITION_SMALLER = 2;
  public static final int SUCCESS_NUMBER_CONDITION_SMALLER_EQUAL = 3;
  public static final int SUCCESS_NUMBER_CONDITION_GREATER = 4;
  public static final int SUCCESS_NUMBER_CONDITION_GREATER_EQUAL = 5;
  public static final int SUCCESS_NUMBER_CONDITION_BETWEEN = 6;
  public static final int SUCCESS_NUMBER_CONDITION_IN_LIST = 7;
  public static final int SUCCESS_NUMBER_CONDITION_NOT_IN_LIST = 8;

  public int successnumbercondition;

  public static final String[] successBooleanConditionDesc =
      new String[] {
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenTrue.Label"),
        BaseMessages.getString(PKG, "JobSimpleEval.SuccessWhenFalse.Label")
      };
  public static final String[] successBooleanConditionCode = new String[] {"true", "false"};
  public static final int SUCCESS_BOOLEAN_CONDITION_TRUE = 0;
  public static final int SUCCESS_BOOLEAN_CONDITION_FALSE = 1;

  public int successbooleancondition;

  private String fieldname;
  private String variablename;
  private String mask;
  private String comparevalue;
  private String minvalue;
  private String maxvalue;

  private boolean successwhenvarset;

  public ActionSimpleEval(String n) {
    super(n, "");
    valuetype = VALUE_TYPE_FIELD;
    successcondition = SUCCESS_CONDITION_EQUAL;
    successnumbercondition = SUCCESS_NUMBER_CONDITION_EQUAL;
    successbooleancondition = SUCCESS_BOOLEAN_CONDITION_FALSE;
    minvalue = null;
    maxvalue = null;
    comparevalue = null;
    fieldname = null;
    variablename = null;
    fieldtype = FIELD_TYPE_STRING;
    mask = null;
    successwhenvarset = false;
  }

  public ActionSimpleEval() {
    this("");
  }

  @Override
  public Object clone() {
    ActionSimpleEval je = (ActionSimpleEval) super.clone();
    return je;
  }

  private static String getValueTypeCode(int i) {
    if (i < 0 || i >= valueTypeCode.length) {
      return valueTypeCode[0];
    }
    return valueTypeCode[i];
  }

  private static String getFieldTypeCode(int i) {
    if (i < 0 || i >= fieldTypeCode.length) {
      return fieldTypeCode[0];
    }
    return fieldTypeCode[i];
  }

  private static String getSuccessConditionCode(int i) {
    if (i < 0 || i >= successConditionCode.length) {
      return successConditionCode[0];
    }
    return successConditionCode[i];
  }

  public static String getSuccessNumberConditionCode(int i) {
    if (i < 0 || i >= successNumberConditionCode.length) {
      return successNumberConditionCode[0];
    }
    return successNumberConditionCode[i];
  }

  private static String getSuccessBooleanConditionCode(int i) {
    if (i < 0 || i >= successBooleanConditionCode.length) {
      return successBooleanConditionCode[0];
    }
    return successBooleanConditionCode[i];
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder(300);

    xml.append(super.getXml());
    xml.append("      ").append(XmlHandler.addTagValue("valuetype", getValueTypeCode(valuetype)));
    xml.append("      ").append(XmlHandler.addTagValue("fieldname", fieldname));
    xml.append("      ").append(XmlHandler.addTagValue("variablename", variablename));
    xml.append("      ").append(XmlHandler.addTagValue("fieldtype", getFieldTypeCode(fieldtype)));
    xml.append("      ").append(XmlHandler.addTagValue("mask", mask));
    xml.append("      ").append(XmlHandler.addTagValue("comparevalue", comparevalue));
    xml.append("      ").append(XmlHandler.addTagValue("minvalue", minvalue));
    xml.append("      ").append(XmlHandler.addTagValue("maxvalue", maxvalue));
    xml.append("      ")
        .append(
            XmlHandler.addTagValue("successcondition", getSuccessConditionCode(successcondition)));
    xml.append("      ")
        .append(
            XmlHandler.addTagValue(
                "successnumbercondition", getSuccessNumberConditionCode(successnumbercondition)));
    xml.append("      ")
        .append(
            XmlHandler.addTagValue(
                "successbooleancondition",
                getSuccessBooleanConditionCode(successbooleancondition)));
    xml.append("      ").append(XmlHandler.addTagValue("successwhenvarset", successwhenvarset));
    return xml.toString();
  }

  private static int getValueTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < valueTypeCode.length; i++) {
      if (valueTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  private static int getSuccessNumberByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < successNumberConditionCode.length; i++) {
      if (successNumberConditionCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  private static int getSuccessBooleanByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < successBooleanConditionCode.length; i++) {
      if (successBooleanConditionCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  private static int getFieldTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < fieldTypeCode.length; i++) {
      if (fieldTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  private static int getSuccessConditionByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < successConditionCode.length; i++) {
      if (successConditionCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public void setSuccessWhenVarSet(boolean successwhenvarset) {
    this.successwhenvarset = successwhenvarset;
  }

  public boolean isSuccessWhenVarSet() {
    return this.successwhenvarset;
  }

  public static int getSuccessNumberConditionByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < successNumberConditionCode.length; i++) {
      if (successNumberConditionCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  private static int getSuccessBooleanConditionByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < successBooleanConditionCode.length; i++) {
      if (successBooleanConditionCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);

      valuetype = getValueTypeByCode(Const.NVL(XmlHandler.getTagValue(entrynode, "valuetype"), ""));
      fieldname = XmlHandler.getTagValue(entrynode, "fieldname");
      fieldtype = getFieldTypeByCode(Const.NVL(XmlHandler.getTagValue(entrynode, "fieldtype"), ""));
      variablename = XmlHandler.getTagValue(entrynode, "variablename");
      mask = XmlHandler.getTagValue(entrynode, "mask");
      comparevalue = XmlHandler.getTagValue(entrynode, "comparevalue");
      minvalue = XmlHandler.getTagValue(entrynode, "minvalue");
      maxvalue = XmlHandler.getTagValue(entrynode, "maxvalue");
      successcondition =
          getSuccessConditionByCode(
              Const.NVL(XmlHandler.getTagValue(entrynode, "successcondition"), ""));
      successnumbercondition =
          getSuccessNumberConditionByCode(
              Const.NVL(XmlHandler.getTagValue(entrynode, "successnumbercondition"), ""));
      successbooleancondition =
          getSuccessBooleanConditionByCode(
              Const.NVL(XmlHandler.getTagValue(entrynode, "successbooleancondition"), ""));
      successwhenvarset =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "successwhenvarset"));
    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionSimple.Error.Exception.UnableLoadXML"), xe);
    }
  }

  @Override
  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;

    result.setNrErrors(1);
    result.setResult(false);

    String sourcevalue = null;
    switch (valuetype) {
      case VALUE_TYPE_FIELD:
        List<RowMetaAndData> rows = result.getRows();
        RowMetaAndData resultRow = null;
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionSimpleEval.Log.ArgFromPrevious.Found",
                  (rows != null ? rows.size() : 0) + ""));
        }

        if (rows.size() == 0) {
          rows = null;
          logError(BaseMessages.getString(PKG, "ActionSimpleEval.Error.NoRows"));
          return result;
        }
        // get first row
        resultRow = rows.get(0);
        String realfieldname = resolve(fieldname);
        int indexOfField = -1;
        indexOfField = resultRow.getRowMeta().indexOfValue(realfieldname);
        if (indexOfField == -1) {
          logError(
              BaseMessages.getString(PKG, "ActionSimpleEval.Error.FieldNotExist", realfieldname));
          resultRow = null;
          rows = null;
          return result;
        }
        sourcevalue = resultRow.getString(indexOfField, null);
        if (sourcevalue == null) {
          sourcevalue = "";
        }
        resultRow = null;
        rows = null;
        break;
      case VALUE_TYPE_VARIABLE:
        if (Utils.isEmpty(variablename)) {
          logError(BaseMessages.getString(PKG, "ActionSimpleEval.Error.VariableMissing"));
          return result;
        }
        if (isSuccessWhenVarSet()) {
          // return variable name
          // remove specifications if needed
          String variableName = StringUtil.getVariableName(Const.NVL(getVariableName(), ""));
          // Get value, if the variable is not set, Null will be returned
          String value = getVariable(variableName);

          if (value != null) {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionSimpleEval.VariableSet", variableName));
            }
            result.setResult(true);
            result.setNrErrors(0);
            return result;
          } else {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionSimpleEval.VariableNotSet", variableName));
            }
            // PDI-6943: this action does not set errors upon evaluation, independently of the
            // outcome of the check
            result.setNrErrors(0);
            return result;
          }
        }
        sourcevalue = resolve(getVariableWithSpec());
        break;
      default:
        break;
    }

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "JobSimpleEval.Log.ValueToevaluate", sourcevalue));
    }

    boolean success = false;
    String realCompareValue = resolve(comparevalue);
    if (realCompareValue == null) {
      realCompareValue = "";
    }
    String realMinValue = resolve(minvalue);
    String realMaxValue = resolve(maxvalue);

    switch (fieldtype) {
      case FIELD_TYPE_STRING:
        switch (successcondition) {
          case SUCCESS_CONDITION_EQUAL: // equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            success = (sourcevalue.equals(realCompareValue));
            if (valuetype == VALUE_TYPE_VARIABLE && !success) {
              // make the empty value evaluate to true when compared to a not set variable
              if (Utils.isEmpty(realCompareValue)) {
                String variableName = StringUtil.getVariableName(variablename);
                if (System.getProperty(variableName) == null) {
                  success = true;
                }
              }
            }
            break;
          case SUCCESS_CONDITION_DIFFERENT: // different
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            success = (!sourcevalue.equals(realCompareValue));
            break;
          case SUCCESS_CONDITION_CONTAINS: // contains
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            success = (sourcevalue.contains(realCompareValue));
            break;
          case SUCCESS_CONDITION_NOT_CONTAINS: // not contains
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            success = (!sourcevalue.contains(realCompareValue));
            break;
          case SUCCESS_CONDITION_START_WITH: // starts with
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            success = (sourcevalue.startsWith(realCompareValue));
            break;
          case SUCCESS_CONDITION_NOT_START_WITH: // not start with
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            success = (!sourcevalue.startsWith(realCompareValue));
            break;
          case SUCCESS_CONDITION_END_WITH: // ends with
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            success = (sourcevalue.endsWith(realCompareValue));
            break;
          case SUCCESS_CONDITION_NOT_END_WITH: // not ends with
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            success = (!sourcevalue.endsWith(realCompareValue));
            break;
          case SUCCESS_CONDITION_REGEX: // regexp
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            success = (Pattern.compile(realCompareValue).matcher(sourcevalue).matches());
            break;
          case SUCCESS_CONDITION_IN_LIST: // in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            realCompareValue = Const.NVL(realCompareValue, "");
            String[] parts = realCompareValue.split(",");
            for (int i = 0; i < parts.length && !success; i++) {
              success = (sourcevalue.equals(parts[i].trim()));
            }
            break;
          case SUCCESS_CONDITION_NOT_IN_LIST: // not in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            realCompareValue = Const.NVL(realCompareValue, "");
            parts = realCompareValue.split(",");
            success = true;
            for (int i = 0; i < parts.length && success; i++) {
              success = !(sourcevalue.equals(parts[i].trim()));
            }
            break;
          default:
            break;
        }
        break;
      case FIELD_TYPE_NUMBER:
        double valuenumber;
        try {
          valuenumber = Double.parseDouble(sourcevalue);
        } catch (Exception e) {
          logError(
              BaseMessages.getString(
                  PKG, "ActionSimpleEval.Error.UnparsableNumber", sourcevalue, e.getMessage()));
          return result;
        }

        double valuecompare;
        switch (successnumbercondition) {
          case SUCCESS_NUMBER_CONDITION_EQUAL: // equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionSimpleEval.Error.UnparsableNumber",
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber == valuecompare);
            break;
          case SUCCESS_NUMBER_CONDITION_DIFFERENT: // different
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionSimpleEval.Error.UnparsableNumber",
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber != valuecompare);
            break;
          case SUCCESS_NUMBER_CONDITION_SMALLER: // smaller
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionSimpleEval.Error.UnparsableNumber",
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber < valuecompare);
            break;
          case SUCCESS_NUMBER_CONDITION_SMALLER_EQUAL: // smaller or equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionSimpleEval.Error.UnparsableNumber",
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber <= valuecompare);
            break;
          case SUCCESS_NUMBER_CONDITION_GREATER: // greater
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionSimpleEval.Error.UnparsableNumber",
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber > valuecompare);
            break;
          case SUCCESS_NUMBER_CONDITION_GREATER_EQUAL: // greater or equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionSimpleEval.Error.UnparsableNumber",
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber >= valuecompare);
            break;
          case SUCCESS_NUMBER_CONDITION_BETWEEN: // between min and max
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValues", realMinValue, realMaxValue));
            }
            double valuemin;
            try {
              valuemin = Double.parseDouble(realMinValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionSimpleEval.Error.UnparsableNumber",
                      realMinValue,
                      e.getMessage()));
              return result;
            }
            double valuemax;
            try {
              valuemax = Double.parseDouble(realMaxValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionSimpleEval.Error.UnparsableNumber",
                      realMaxValue,
                      e.getMessage()));
              return result;
            }

            if (valuemin >= valuemax) {
              logError(
                  BaseMessages.getString(
                      PKG, "ActionSimpleEval.Error.IncorrectNumbers", realMinValue, realMaxValue));
              return result;
            }
            success = (valuenumber >= valuemin && valuenumber <= valuemax);
            break;
          case SUCCESS_NUMBER_CONDITION_IN_LIST: // in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            String[] parts = realCompareValue.split(",");

            for (int i = 0; i < parts.length && !success; i++) {
              try {
                valuecompare = Double.parseDouble(parts[i]);
              } catch (Exception e) {
                logError(
                    toString(),
                    BaseMessages.getString(
                        PKG, "ActionSimpleEval.Error.UnparsableNumber", parts[i], e.getMessage()));
                return result;
              }
              success = (valuenumber == valuecompare);
            }
            break;
          case SUCCESS_NUMBER_CONDITION_NOT_IN_LIST: // not in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            realCompareValue = Const.NVL(realCompareValue, "");
            parts = realCompareValue.split(",");
            success = true;
            for (int i = 0; i < parts.length && success; i++) {
              try {
                valuecompare = Double.parseDouble(parts[i]);
              } catch (Exception e) {
                logError(
                    toString(),
                    BaseMessages.getString(
                        PKG, "ActionSimpleEval.Error.UnparsableNumber", parts[i], e.getMessage()));
                return result;
              }

              success = (valuenumber != valuecompare);
            }
            break;
          default:
            break;
        }
        break;
      case FIELD_TYPE_DATE_TIME:
        String realMask = resolve(mask);
        SimpleDateFormat df = new SimpleDateFormat();
        if (!Utils.isEmpty(realMask)) {
          df.applyPattern(realMask);
        }

        Date datevalue = null;
        try {
          datevalue = convertToDate(sourcevalue, realMask, df);
        } catch (Exception e) {
          logError(e.getMessage());
          return result;
        }

        Date datecompare;
        switch (successnumbercondition) {
          case SUCCESS_NUMBER_CONDITION_EQUAL: // equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (datevalue.equals(datecompare));
            break;
          case SUCCESS_NUMBER_CONDITION_DIFFERENT: // different
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (!datevalue.equals(datecompare));
            break;
          case SUCCESS_NUMBER_CONDITION_SMALLER: // smaller
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (datevalue.before(datecompare));
            break;
          case SUCCESS_NUMBER_CONDITION_SMALLER_EQUAL: // smaller or equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (datevalue.before(datecompare) || datevalue.equals(datecompare));
            break;
          case SUCCESS_NUMBER_CONDITION_GREATER: // greater
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (datevalue.after(datecompare));
            break;
          case SUCCESS_NUMBER_CONDITION_GREATER_EQUAL: // greater or equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (datevalue.after(datecompare) || datevalue.equals(datecompare));
            break;
          case SUCCESS_NUMBER_CONDITION_BETWEEN: // between min and max
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValues", realMinValue, realMaxValue));
            }
            Date datemin;
            try {
              datemin = convertToDate(realMinValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }

            Date datemax;
            try {
              datemax = convertToDate(realMaxValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }

            if (datemin.after(datemax) || datemin.equals(datemax)) {
              logError(
                  BaseMessages.getString(
                      PKG, "ActionSimpleEval.Error.IncorrectDates", realMinValue, realMaxValue));
              return result;
            }

            success =
                ((datevalue.after(datemin) || datevalue.equals(datemin))
                    && (datevalue.before(datemax) || datevalue.equals(datemax)));
            break;
          case SUCCESS_NUMBER_CONDITION_IN_LIST: // in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            String[] parts = realCompareValue.split(",");

            for (int i = 0; i < parts.length && !success; i++) {
              try {
                datecompare = convertToDate(realCompareValue, realMask, df);
              } catch (Exception e) {
                logError(toString(), e.getMessage());
                return result;
              }
              success = (datevalue.equals(datecompare));
            }
            break;
          case SUCCESS_NUMBER_CONDITION_NOT_IN_LIST: // not in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "JobSimpleEval.Log.CompareWithValue", sourcevalue, realCompareValue));
            }
            realCompareValue = Const.NVL(realCompareValue, "");
            parts = realCompareValue.split(",");
            success = true;
            for (int i = 0; i < parts.length && success; i++) {
              try {
                datecompare = convertToDate(realCompareValue, realMask, df);
              } catch (Exception e) {
                logError(toString(), e.getMessage());
                return result;
              }
              success = (!datevalue.equals(datecompare));
            }
            break;
          default:
            break;
        }
        df = null;
        break;
      case FIELD_TYPE_BOOLEAN:
        boolean valuebool;
        try {
          valuebool = ValueMetaString.convertStringToBoolean(sourcevalue);
        } catch (Exception e) {
          logError(
              BaseMessages.getString(
                  PKG, "ActionSimpleEval.Error.UnparsableBoolean", sourcevalue, e.getMessage()));
          return result;
        }

        switch (successbooleancondition) {
          case SUCCESS_BOOLEAN_CONDITION_FALSE: // false
            success = (!valuebool);
            break;
          case SUCCESS_BOOLEAN_CONDITION_TRUE: // true
            success = (valuebool);
            break;
          default:
            break;
        }
        break;
      default:
        break;
    }

    result.setResult(success);
    // This action does not set errors upon evaluation, independently of the outcome of the check
    result.setNrErrors(0);
    return result;
  }

  /*
   * Returns variable with specifications
   */
  private String getVariableWithSpec() {
    String variable = getVariableName();
    if ((!variable.contains(StringUtil.UNIX_OPEN)
            && !variable.contains(StringUtil.WINDOWS_OPEN)
            && !variable.contains(StringUtil.HEX_OPEN))
        && ((!variable.contains(StringUtil.UNIX_CLOSE)
            && !variable.contains(StringUtil.WINDOWS_CLOSE)
            && !variable.contains(StringUtil.HEX_CLOSE)))) {
      // Add specifications to variable
      variable = StringUtil.UNIX_OPEN + variable + StringUtil.UNIX_CLOSE;
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionSimpleEval.CheckingVariable", variable));
      }
    }
    return variable;
  }

  private Date convertToDate(String valueString, String mask, SimpleDateFormat df)
      throws HopException {
    Date datevalue = null;
    try {
      datevalue = df.parse(valueString);
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionSimpleEval.Error.UnparsableDate", valueString));
    }
    return datevalue;
  }

  public static String getValueTypeDesc(int i) {
    if (i < 0 || i >= valueTypeDesc.length) {
      return valueTypeDesc[0];
    }
    return valueTypeDesc[i];
  }

  public static String getFieldTypeDesc(int i) {
    if (i < 0 || i >= fieldTypeDesc.length) {
      return fieldTypeDesc[0];
    }
    return fieldTypeDesc[i];
  }

  public static String getSuccessConditionDesc(int i) {
    if (i < 0 || i >= successConditionDesc.length) {
      return successConditionDesc[0];
    }
    return successConditionDesc[i];
  }

  public static String getSuccessNumberConditionDesc(int i) {
    if (i < 0 || i >= successNumberConditionDesc.length) {
      return successNumberConditionDesc[0];
    }
    return successNumberConditionDesc[i];
  }

  public static String getSuccessBooleanConditionDesc(int i) {
    if (i < 0 || i >= successBooleanConditionDesc.length) {
      return successBooleanConditionDesc[0];
    }
    return successBooleanConditionDesc[i];
  }

  public static int getValueTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < valueTypeDesc.length; i++) {
      if (valueTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getValueTypeByCode(tt);
  }

  public static int getFieldTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < fieldTypeDesc.length; i++) {
      if (fieldTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getFieldTypeByCode(tt);
  }

  public static int getSuccessConditionByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < successConditionDesc.length; i++) {
      if (successConditionDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getSuccessConditionByCode(tt);
  }

  public static int getSuccessNumberConditionByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < successNumberConditionDesc.length; i++) {
      if (successNumberConditionDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getSuccessNumberByCode(tt);
  }

  public static int getSuccessBooleanConditionByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < successBooleanConditionDesc.length; i++) {
      if (successBooleanConditionDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getSuccessBooleanByCode(tt);
  }

  public void setMinValue(String minvalue) {
    this.minvalue = minvalue;
  }

  public String getMinValue() {
    return minvalue;
  }

  public void setCompareValue(String comparevalue) {
    this.comparevalue = comparevalue;
  }

  public String getMask() {
    return mask;
  }

  public void setMask(String mask) {
    this.mask = mask;
  }

  public String getFieldName() {
    return fieldname;
  }

  public void setFieldName(String fieldname) {
    this.fieldname = fieldname;
  }

  public String getVariableName() {
    return variablename;
  }

  public void setVariableName(String variablename) {
    this.variablename = variablename;
  }

  public String getCompareValue() {
    return comparevalue;
  }

  public void setMaxValue(String maxvalue) {
    this.maxvalue = maxvalue;
  }

  public String getMaxValue() {
    return maxvalue;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }
}
