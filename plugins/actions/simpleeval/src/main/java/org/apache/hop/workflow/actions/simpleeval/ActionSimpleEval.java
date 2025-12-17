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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/** This defines a 'simple evaluation' action. */
@Action(
    id = "SIMPLE_EVAL",
    name = "i18n::ActionSimpleEval.Name",
    description = "i18n::ActionSimpleEval.Description",
    image = "SimpleEval.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionSimpleEval.keyword",
    documentationUrl = "/workflow/actions/simpleeval.html")
public class ActionSimpleEval extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSimpleEval.class;
  public static final String CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE =
      "ActionSimpleEval.Log.CompareWithValue";
  public static final String CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER =
      "ActionSimpleEval.Error.UnparsableNumber";

  public enum ValueType implements IEnumHasCodeAndDescription {
    FIELD("field", BaseMessages.getString(PKG, "ActionSimpleEval.EvalPreviousField.Label")),
    VARIABLE("variable", BaseMessages.getString(PKG, "ActionSimpleEval.EvalVariable.Label"));

    private final String code;
    private final String description;

    ValueType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(ValueType.class);
    }

    public static ValueType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(ValueType.class, description, FIELD);
    }

    public static ValueType lookupCode(String code) {
      return IEnumHasCode.lookupCode(ValueType.class, code, FIELD);
    }

    @Override
    public String getCode() {
      return code;
    }

    @Override
    public String getDescription() {
      return description;
    }
  }

  public enum SuccessStringCondition implements IEnumHasCodeAndDescription {
    EQUAL("equal", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenEqual.Label")),
    DIFFERENT(
        "different", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenDifferent.Label")),
    CONTAINS("contains", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenContains.Label")),
    NOT_CONTAINS(
        "notcontains",
        BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenNotContains.Label")),
    START_WITH(
        "startswith", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenStartWith.Label")),
    NOT_START_WITH(
        "notstatwith",
        BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenNotStartWith.Label")),
    END_WITH("endswith", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenEndWith.Label")),
    NOT_END_WITH(
        "notendwith", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenNotEndWith.Label")),
    REGEX("regexp", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenRegExp.Label")),
    IN_LIST("inlist", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenInList.Label")),
    NOT_IN_LIST(
        "notinlist", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenNotInList.Label"));

    private final String code;
    private final String description;

    SuccessStringCondition(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(SuccessStringCondition.class);
    }

    public static SuccessStringCondition lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          SuccessStringCondition.class, description, EQUAL);
    }

    public static SuccessStringCondition lookupCode(String code) {
      return IEnumHasCode.lookupCode(SuccessStringCondition.class, code, EQUAL);
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    @Override
    public String getDescription() {
      return description;
    }
  }

  public enum FieldType implements IEnumHasCodeAndDescription {
    STRING("string", BaseMessages.getString(PKG, "ActionSimpleEval.FieldTypeString.Label")),
    NUMBER("number", BaseMessages.getString(PKG, "ActionSimpleEval.FieldTypeNumber.Label")),
    DATE_TIME("datetime", BaseMessages.getString(PKG, "ActionSimpleEval.FieldTypeDateTime.Label")),
    BOOLEAN("boolean", BaseMessages.getString(PKG, "ActionSimpleEval.FieldTypeBoolean.Label"));

    private final String code;
    private final String description;

    FieldType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(FieldType.class);
    }

    public static FieldType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(FieldType.class, description, STRING);
    }

    public static FieldType lookupCode(String code) {
      return IEnumHasCode.lookupCode(FieldType.class, code, STRING);
    }

    @Override
    public String getCode() {
      return code;
    }

    @Override
    public String getDescription() {
      return description;
    }
  }

  public enum SuccessNumberCondition implements IEnumHasCodeAndDescription {
    EQUAL("equal", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenEqual.Label")),
    DIFFERENT(
        "different", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenDifferent.Label")),
    SMALLER("smaller", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenSmallThan.Label")),
    SMALLER_EQUAL(
        "smallequal",
        BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenSmallOrEqualThan.Label")),
    GREATER(
        "greater", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenGreaterThan.Label")),
    GREATER_EQUAL(
        "greaterequal",
        BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenGreaterOrEqualThan.Label")),
    BETWEEN("between", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessBetween.Label")),
    IN_LIST("inlist", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenInList.Label")),
    NOT_IN_LIST(
        "notinlist", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenNotInList.Label"));

    private final String code;
    private final String description;

    SuccessNumberCondition(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(SuccessNumberCondition.class);
    }

    public static SuccessNumberCondition lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          SuccessNumberCondition.class, description, EQUAL);
    }

    public static SuccessNumberCondition lookupCode(String code) {
      return IEnumHasCode.lookupCode(SuccessNumberCondition.class, code, EQUAL);
    }

    @Override
    public String getCode() {
      return code;
    }

    @Override
    public String getDescription() {
      return description;
    }
  }

  public enum SuccessBooleanCondition implements IEnumHasCodeAndDescription {
    TRUE("true", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenTrue.Label")),
    FALSE("false", BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenFalse.Label"));

    private final String code;
    private final String description;

    SuccessBooleanCondition(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(SuccessBooleanCondition.class);
    }

    public static SuccessBooleanCondition lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          SuccessBooleanCondition.class, description, TRUE);
    }

    public static SuccessBooleanCondition lookupCode(String code) {
      return IEnumHasCode.lookupCode(SuccessBooleanCondition.class, code, TRUE);
    }

    @Override
    public String getCode() {
      return code;
    }

    @Override
    public String getDescription() {
      return description;
    }
  }

  @HopMetadataProperty(key = "valuetype", storeWithCode = true)
  private ValueType valueType;

  @HopMetadataProperty(key = "fieldtype", storeWithCode = true)
  private FieldType fieldType;

  @HopMetadataProperty(key = "fieldname")
  private String fieldName;

  @HopMetadataProperty(key = "variablename")
  private String variableName;

  @HopMetadataProperty(key = "mask")
  private String mask;

  @HopMetadataProperty(key = "comparevalue")
  private String compareValue;

  @HopMetadataProperty(key = "minvalue")
  private String minValue;

  @HopMetadataProperty(key = "maxvalue")
  private String maxValue;

  @HopMetadataProperty(key = "successcondition", storeWithCode = true)
  private SuccessStringCondition successStringCondition;

  @HopMetadataProperty(key = "successwhenvarset")
  private boolean successWhenVarSet;

  @HopMetadataProperty(key = "successbooleancondition", storeWithCode = true)
  private SuccessBooleanCondition successBooleanCondition;

  @HopMetadataProperty(key = "successnumbercondition", storeWithCode = true)
  private SuccessNumberCondition successNumberCondition;

  public ActionSimpleEval(String n) {
    super(n, "");
    valueType = ValueType.FIELD;
    fieldType = FieldType.STRING;
    successStringCondition = SuccessStringCondition.EQUAL;
    successNumberCondition = SuccessNumberCondition.EQUAL;
    successBooleanCondition = SuccessBooleanCondition.TRUE;
    minValue = null;
    maxValue = null;
    compareValue = null;
    fieldName = null;
    variableName = null;
    mask = null;
    successWhenVarSet = false;
  }

  public ActionSimpleEval() {
    this("");
  }

  @Override
  public Object clone() {
    ActionSimpleEval je = (ActionSimpleEval) super.clone();
    return je;
  }

  public void setSuccessWhenVarSet(boolean successwhenvarset) {
    this.successWhenVarSet = successwhenvarset;
  }

  public boolean isSuccessWhenVarSet() {
    return this.successWhenVarSet;
  }

  @Override
  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;

    result.setNrErrors(1);
    result.setResult(false);

    String sourcevalue = null;
    switch (valueType) {
      case FIELD:
        List<RowMetaAndData> rows = result.getRows();
        RowMetaAndData resultRow = null;
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionSimpleEval.Log.ArgFromPrevious.Found",
                  (rows != null ? rows.size() : 0) + ""));
        }

        if (rows.isEmpty()) {
          rows = null;
          logError(BaseMessages.getString(PKG, "ActionSimpleEval.Error.NoRows"));
          return result;
        }
        // get first row
        resultRow = rows.get(0);
        String realfieldname = resolve(fieldName);
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
      case VARIABLE:
        if (Utils.isEmpty(variableName)) {
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
            // this action does not set errors upon evaluation, independently of the
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
      logDetailed(BaseMessages.getString(PKG, "ActionSimpleEval.Log.ValueToevaluate", sourcevalue));
    }

    boolean success = false;
    String realCompareValue = resolve(compareValue);
    if (realCompareValue == null) {
      realCompareValue = "";
    }
    String realMinValue = resolve(minValue);
    String realMaxValue = resolve(maxValue);

    switch (fieldType) {
      case STRING:
        switch (successStringCondition) {
          case EQUAL: // equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            success = (sourcevalue.equals(realCompareValue));
            if (valueType == ValueType.VARIABLE && !success && Utils.isEmpty(realCompareValue)) {
              // make the empty value evaluate to true when compared to a not set variable
              if (resolve(variableName) == null) {
                success = true;
              }
            }
            break;
          case DIFFERENT: // different
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            success = (!sourcevalue.equals(realCompareValue));
            break;
          case CONTAINS: // contains
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            success = (sourcevalue.contains(realCompareValue));
            break;
          case NOT_CONTAINS: // not contains
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            success = (!sourcevalue.contains(realCompareValue));
            break;
          case START_WITH: // starts with
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            success = (sourcevalue.startsWith(realCompareValue));
            break;
          case NOT_START_WITH: // not start with
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            success = (!sourcevalue.startsWith(realCompareValue));
            break;
          case END_WITH: // ends with
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            success = (sourcevalue.endsWith(realCompareValue));
            break;
          case NOT_END_WITH: // not ends with
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            success = (!sourcevalue.endsWith(realCompareValue));
            break;
          case REGEX: // regexp
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            success = (Pattern.compile(realCompareValue).matcher(sourcevalue).matches());
            break;
          case IN_LIST: // in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            realCompareValue = Const.NVL(realCompareValue, "");
            String[] parts = realCompareValue.split(",");
            for (int i = 0; i < parts.length && !success; i++) {
              success = (sourcevalue.equals(parts[i].trim()));
            }
            break;
          case NOT_IN_LIST: // not in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
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
      case NUMBER:
        double valuenumber;
        try {
          valuenumber = Double.parseDouble(sourcevalue);
        } catch (Exception e) {
          logError(
              BaseMessages.getString(
                  PKG,
                  CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
                  sourcevalue,
                  e.getMessage()));
          return result;
        }

        double valuecompare;
        switch (successNumberCondition) {
          case EQUAL: // equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber == valuecompare);
            break;
          case DIFFERENT: // different
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber != valuecompare);
            break;
          case SMALLER: // smaller
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber < valuecompare);
            break;
          case SMALLER_EQUAL: // smaller or equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber <= valuecompare);
            break;
          case GREATER: // greater
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber > valuecompare);
            break;
          case GREATER_EQUAL: // greater or equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              valuecompare = Double.parseDouble(realCompareValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
                      realCompareValue,
                      e.getMessage()));
              return result;
            }
            success = (valuenumber >= valuecompare);
            break;
          case BETWEEN: // between min and max
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "ActionSimpleEval.Log.CompareWithValues", realMinValue, realMaxValue));
            }
            double valuemin;
            try {
              valuemin = Double.parseDouble(realMinValue);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
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
                      CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
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
          case IN_LIST: // in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            String[] parts = realCompareValue.split(",");

            for (int i = 0; i < parts.length && !success; i++) {
              try {
                valuecompare = Double.parseDouble(parts[i]);
              } catch (Exception e) {
                logError(
                    toString(),
                    BaseMessages.getString(
                        PKG,
                        CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
                        parts[i],
                        e.getMessage()));
                return result;
              }
              success = (valuenumber == valuecompare);
            }
            break;
          case NOT_IN_LIST: // not in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
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
                        PKG,
                        CONST_ACTION_SIMPLE_EVAL_ERROR_UNPARSABLE_NUMBER,
                        parts[i],
                        e.getMessage()));
                return result;
              }

              success = (valuenumber != valuecompare);
            }
            break;
          default:
            break;
        }
        break;
      case DATE_TIME:
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
        switch (successNumberCondition) {
          case EQUAL: // equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (datevalue.equals(datecompare));
            break;
          case DIFFERENT: // different
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (!datevalue.equals(datecompare));
            break;
          case SMALLER: // smaller
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (datevalue.before(datecompare));
            break;
          case SMALLER_EQUAL: // smaller or equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (datevalue.before(datecompare) || datevalue.equals(datecompare));
            break;
          case GREATER: // greater
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (datevalue.after(datecompare));
            break;
          case GREATER_EQUAL: // greater or equal
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
            }
            try {
              datecompare = convertToDate(realCompareValue, realMask, df);
            } catch (Exception e) {
              logError(e.getMessage());
              return result;
            }
            success = (datevalue.after(datecompare) || datevalue.equals(datecompare));
            break;
          case BETWEEN: // between min and max
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "ActionSimpleEval.Log.CompareWithValues", realMinValue, realMaxValue));
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
          case IN_LIST: // in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
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
          case NOT_IN_LIST: // not in list
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_SIMPLE_EVAL_LOG_COMPARE_WITH_VALUE,
                      sourcevalue,
                      realCompareValue));
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
      case BOOLEAN:
        boolean valuebool;
        try {
          valuebool = ValueMetaBase.convertStringToBoolean(sourcevalue);
        } catch (Exception e) {
          logError(
              BaseMessages.getString(
                  PKG, "ActionSimpleEval.Error.UnparsableBoolean", sourcevalue, e.getMessage()));
          return result;
        }

        switch (successBooleanCondition) {
          case FALSE: // false
            success = (!valuebool);
            break;
          case TRUE: // true
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
        && (!variable.contains(StringUtil.UNIX_CLOSE)
            && !variable.contains(StringUtil.WINDOWS_CLOSE)
            && !variable.contains(StringUtil.HEX_CLOSE))) {
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

  public void setMinValue(String minvalue) {
    this.minValue = minvalue;
  }

  public String getMinValue() {
    return minValue;
  }

  public void setCompareValue(String comparevalue) {
    this.compareValue = comparevalue;
  }

  public String getMask() {
    return mask;
  }

  public void setMask(String mask) {
    this.mask = mask;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldname) {
    this.fieldName = fieldname;
  }

  public String getVariableName() {
    return variableName;
  }

  public void setVariableName(String variablename) {
    this.variableName = variablename;
  }

  public String getCompareValue() {
    return compareValue;
  }

  public void setMaxValue(String maxvalue) {
    this.maxValue = maxvalue;
  }

  public String getMaxValue() {
    return maxValue;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public FieldType getFieldType() {
    return fieldType;
  }

  public SuccessStringCondition getSuccessStringCondition() {
    return successStringCondition;
  }

  public void setValueType(ValueType valuetype) {
    this.valueType = valuetype;
  }

  public void setFieldType(FieldType fieldtype) {
    this.fieldType = fieldtype;
  }

  public void setSuccessStringCondition(SuccessStringCondition successcondition) {
    this.successStringCondition = successcondition;
  }

  public SuccessBooleanCondition getSuccessBooleanCondition() {
    return successBooleanCondition;
  }

  public SuccessNumberCondition getSuccessNumberCondition() {
    return successNumberCondition;
  }

  public void setSuccessBooleanCondition(SuccessBooleanCondition successBooleanCondition) {
    this.successBooleanCondition = successBooleanCondition;
  }

  public void setSuccessNumberCondition(SuccessNumberCondition successNumberCondition) {
    this.successNumberCondition = successNumberCondition;
  }
}
