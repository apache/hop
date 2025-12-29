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

package org.apache.hop.core;

import static org.apache.hop.core.Condition.Function.EQUAL;
import static org.apache.hop.core.Condition.Function.NOT_NULL;
import static org.apache.hop.core.Condition.Function.NULL;
import static org.apache.hop.core.Condition.Function.TRUE;
import static org.apache.hop.core.Condition.Operator.AND;
import static org.apache.hop.core.Condition.Operator.NONE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.w3c.dom.Node;

/**
 * This class describes a condition in a general meaning.
 *
 * <p>A condition can either be
 *
 * <p>
 *
 * <p>1) Atomic (a=10, B='aa')
 *
 * <p>2) Composite ( NOT Condition1 AND Condition2 OR Condition3 )
 *
 * <p>
 *
 * <p>If the nr of atomic conditions is 0, the condition is atomic, otherwise it's Composit.
 *
 * <p>Precedence doesn't exist. Conditions are evaluated in the order in which they are found.
 *
 * <p>A condition can be negated or not.
 *
 * <p>
 *
 * <p>
 */
@Getter
@Setter
public class Condition implements Cloneable {
  public static final String XML_TAG = "condition";

  private static final String CONST_OR_NOT = "OR NOT";
  private static final String CONST_AND_NOT = "AND NOT";
  public static final String[] operators =
      new String[] {"-", "OR", "AND", "NOT", CONST_OR_NOT, CONST_AND_NOT, "XOR"};
  public static final int OPERATOR_NONE = 0;
  public static final int OPERATOR_OR = 1;
  public static final int OPERATOR_AND = 2;
  public static final int OPERATOR_NOT = 3;
  public static final int OPERATOR_OR_NOT = 4;
  public static final int OPERATOR_AND_NOT = 5;
  public static final int OPERATOR_XOR = 6;

  public static final String[] functions =
      new String[] {
        "=",
        "<>",
        "<",
        "<=",
        ">",
        ">=",
        "REGEXP",
        "IS NULL",
        "IS NOT NULL",
        "IN LIST",
        "CONTAINS",
        "STARTS WITH",
        "ENDS WITH",
        "LIKE",
        "TRUE"
      };

  public static final int FUNC_EQUAL = 0;
  public static final int FUNC_NOT_EQUAL = 1;
  public static final int FUNC_SMALLER = 2;
  public static final int FUNC_SMALLER_EQUAL = 3;
  public static final int FUNC_LARGER = 4;
  public static final int FUNC_LARGER_EQUAL = 5;
  public static final int FUNC_REGEXP = 6;
  public static final int FUNC_NULL = 7;
  public static final int FUNC_NOT_NULL = 8;
  public static final int FUNC_IN_LIST = 9;
  public static final int FUNC_CONTAINS = 10;
  public static final int FUNC_STARTS_WITH = 11;
  public static final int FUNC_ENDS_WITH = 12;
  public static final int FUNC_LIKE = 13;
  public static final int FUNC_TRUE = 14;

  //
  // These parameters allow for:
  // value = othervalue
  // value = 'A'
  // NOT value = othervalue
  //

  @HopMetadataProperty(key = "negated", isExcludedFromInjection = true)
  private boolean negated;

  @HopMetadataProperty(
      key = "operator",
      storeWithCode = true,
      enumNameWhenNotFound = "NONE",
      isExcludedFromInjection = true)
  private Operator operator;

  @HopMetadataProperty(key = "leftvalue", isExcludedFromInjection = true)
  private String leftValueName;

  @HopMetadataProperty(key = "function", storeWithCode = true, isExcludedFromInjection = true)
  private Function function;

  @HopMetadataProperty(key = "rightvalue", isExcludedFromInjection = true)
  private String rightValueName;

  @HopMetadataProperty(key = "value", isExcludedFromInjection = true)
  private CValue rightValue;

  @HopMetadataProperty(groupKey = "conditions", key = "condition", isExcludedFromInjection = true)
  private List<Condition> children;

  // Cached values for performance:
  private int leftFieldIndex;
  private int rightFieldIndex;
  private String rightString;

  // Cache for constant right values (only used when rightFieldIndex == -2)
  private IValueMeta cachedFieldMeta2;
  private Object cachedField2;
  private boolean rightValueCached;

  /**
   * Temporary variable, no need to persist this one. Contains the sorted array of strings in an IN
   * LIST condition
   */
  private String[] inList;

  public Condition() {
    this.children = new ArrayList<>();
    this.operator = NONE;
    this.negated = false;
    this.rightValue = null;
    this.function = EQUAL;

    leftFieldIndex = -2;
    rightFieldIndex = -2;
    rightValueCached = false;
  }

  public Condition(String valueName, Function function, String valueName2, ValueMetaAndData exact)
      throws HopValueException {
    this();
    this.leftValueName = valueName;
    this.function = function;
    this.rightValueName = valueName2;
    this.rightValue = exact == null ? null : new CValue(exact);

    clearFieldPositions();
  }

  public Condition(
      Operator operator,
      String valueName,
      Function function,
      String valueName2,
      ValueMetaAndData exact)
      throws HopValueException {
    this();
    this.operator = operator;
    this.leftValueName = valueName;
    this.function = function;
    this.rightValueName = valueName2;
    this.rightValue = exact == null ? null : new CValue(exact);

    clearFieldPositions();
  }

  public Condition(
      boolean negated,
      String valueName,
      Function function,
      String valueName2,
      ValueMetaAndData exact)
      throws HopValueException {
    this(valueName, function, valueName2, exact);
    this.negated = negated;
  }

  public Condition(Condition c) {
    this();
    this.negated = c.negated;
    this.operator = c.operator;
    this.function = c.function;
    this.leftValueName = c.leftValueName;
    this.rightValueName = c.rightValueName;
    this.rightValue = c.rightValue == null ? null : new CValue(c.rightValue);
    c.children.forEach(child -> this.children.add(new Condition(child)));
  }

  @Override
  public Condition clone() {
    return new Condition(this);
  }

  public String getOperatorDesc() {
    return Const.rightPad(operator.getCode(), 7);
  }

  public static int getOperator(String description) {
    if (description == null) {
      return OPERATOR_NONE;
    }

    for (int i = 1; i < operators.length; i++) {
      if (operators[i].equalsIgnoreCase(Const.trim(description))) {
        return i;
      }
    }
    return OPERATOR_NONE;
  }

  public static String[] getOperators() {
    String[] operatorCodes = new String[Condition.operators.length - 1];
    System.arraycopy(Condition.operators, 1, operatorCodes, 0, Condition.operators.length - 1);
    return operatorCodes;
  }

  public static final String[] getRealOperators() {
    return new String[] {"OR", "AND", CONST_OR_NOT, CONST_AND_NOT, "XOR"};
  }

  public String getFunctionDesc() {
    return function == null ? EQUAL.getCode() : function.getCode();
  }

  public static int getFunction(String description) {
    for (int i = 1; i < functions.length; i++) {
      if (functions[i].equalsIgnoreCase(Const.trim(description))) {
        return i;
      }
    }
    return FUNC_EQUAL;
  }

  public String getRightValueString() {
    if (rightValue == null) {
      return null;
    }
    return rightValue.getText();
  }

  public boolean isAtomic() {
    return children.isEmpty();
  }

  public boolean isComposite() {
    return !children.isEmpty();
  }

  public void negate() {
    setNegated(!isNegated());
  }

  /** A condition is empty when the condition is atomic and no left field is specified. */
  public boolean isEmpty() {
    return (isAtomic() && leftValueName == null);
  }

  /**
   * We cache the position of a value in a row. If ever we want to change the rowtype, we need to
   * clear these cached field positions...
   */
  public void clearFieldPositions() {
    leftFieldIndex = -2;
    rightFieldIndex = -2;
    rightValueCached = false;
    cachedFieldMeta2 = null;
    cachedField2 = null;
  }

  /**
   * Evaluate the condition...
   *
   * @param rowMeta the row metadata
   * @param r the row data
   * @return true if the condition evaluates to true.
   */
  public boolean evaluate(IRowMeta rowMeta, Object[] r) {
    boolean evaluation = false;

    // If we have 0 items in the list, evaluate the current condition
    // Otherwise, evaluate all sub-conditions
    //
    try {
      if (isAtomic()) {

        if (function == TRUE) {
          return !negated;
        }

        // Get field index: left value name
        //
        // Check out the field index if we don't have them...
        if (StringUtils.isNotEmpty(leftValueName)) {
          leftFieldIndex = rowMeta.indexOfValue(leftValueName);
        }

        // Get field index: right value name
        //
        if (StringUtils.isNotEmpty(rightValueName)) {
          rightFieldIndex = rowMeta.indexOfValue(rightValueName);

          // We can't have a right value in this case
          rightValue = null;
        }

        // Get field index: left field
        //
        IValueMeta fieldMeta;
        Object field;
        if (leftFieldIndex >= 0) {
          fieldMeta = rowMeta.getValueMeta(leftFieldIndex);
          field = r[leftFieldIndex];
        } else {
          return false; // no fields to evaluate
        }

        // Get field index: right value
        //
        IValueMeta fieldMeta2;
        Object field2;

        if (rightFieldIndex >= 0) {
          // Right value is a field from the row - get it dynamically
          fieldMeta2 = rowMeta.getValueMeta(rightFieldIndex);
          field2 = r[rightFieldIndex];
        } else if (rightValue != null) {
          // Right value is a constant - cache it for performance
          if (!rightValueCached) {
            cachedFieldMeta2 = rightValue.createValueMeta();
            cachedField2 = rightValue.createValueData();
            rightValueCached = true;
          }
          fieldMeta2 = cachedFieldMeta2;
          field2 = cachedField2;
        } else {
          fieldMeta2 = null;
          field2 = null;
        }

        // Evaluate
        switch (function) {
          case EQUAL:
            evaluation = (fieldMeta.compare(field, fieldMeta2, field2) == 0);
            break;
          case NOT_EQUAL:
            evaluation = (fieldMeta.compare(field, fieldMeta2, field2) != 0);
            break;
          case SMALLER:
            if (fieldMeta.isNull(field)) {
              evaluation = false;
            } else {
              evaluation = (fieldMeta.compare(field, fieldMeta2, field2) < 0);
            }
            break;
          case SMALLER_EQUAL:
            if (fieldMeta.isNull(field)) {
              evaluation = false;
            } else {
              evaluation = (fieldMeta.compare(field, fieldMeta2, field2) <= 0);
            }
            break;
          case LARGER:
            evaluation = (fieldMeta.compare(field, fieldMeta2, field2) > 0);
            break;
          case LARGER_EQUAL:
            evaluation = (fieldMeta.compare(field, fieldMeta2, field2) >= 0);
            break;
          case REGEXP:
            if (fieldMeta.isNull(field) || field2 == null) {
              evaluation = false;
            } else {
              evaluation =
                  Pattern.matches(
                      fieldMeta2.getCompatibleString(field2), fieldMeta.getCompatibleString(field));
            }
            break;
          case NULL:
            evaluation = (fieldMeta.isNull(field));
            break;
          case NOT_NULL:
            evaluation = (!fieldMeta.isNull(field));
            break;
          case IN_LIST:
            // performance reason: create the array first or again when it is against a field and
            // not a constant
            //
            if (inList == null || rightFieldIndex >= 0) {
              inList = Const.splitString(fieldMeta2.getString(field2), ';', true);
              for (int i = 0; i < inList.length; i++) {
                inList[i] = inList[i] == null ? null : inList[i].replace("\\", "");
              }
              Arrays.sort(inList);
            }
            String searchString = fieldMeta.getCompatibleString(field);
            int inIndex = -1;
            if (searchString != null) {
              inIndex = Arrays.binarySearch(inList, searchString);
            }
            evaluation = inIndex >= 0;
            break;
          case CONTAINS:
            evaluation =
                fieldMeta.getCompatibleString(field) != null
                    && fieldMeta
                        .getCompatibleString(field)
                        .contains(fieldMeta2.getCompatibleString(field2));
            break;
          case STARTS_WITH:
            evaluation =
                fieldMeta.getCompatibleString(field) != null
                    && fieldMeta
                        .getCompatibleString(field)
                        .startsWith(fieldMeta2.getCompatibleString(field2));
            break;
          case ENDS_WITH:
            String string = fieldMeta.getCompatibleString(field);
            if (!Utils.isEmpty(string)) {
              if (rightString == null && field2 != null) {
                rightString = fieldMeta2.getCompatibleString(field2);
              }
              if (rightString != null) {
                evaluation = string.endsWith(fieldMeta2.getCompatibleString(field2));
              } else {
                evaluation = false;
              }
            } else {
              evaluation = false;
            }
            break;
          case LIKE:
            // Converts to a regular expression
            //
            if (fieldMeta.isNull(field) || field2 == null) {
              evaluation = false;
            } else {
              String regex = fieldMeta2.getCompatibleString(field2);
              regex = regex.replace("%", ".*");
              regex = regex.replace("?", ".");
              evaluation = Pattern.matches(regex, fieldMeta.getCompatibleString(field));
            }
            break;
          default:
            break;
        }

        // Only NOT makes sense, the rest doesn't, so ignore!!!!
        // Optionally negate
        //
        if (isNegated()) {
          evaluation = !evaluation;
        }
      } else {
        // Composite : get first
        Condition cb0 = children.get(0);
        evaluation = cb0.evaluate(rowMeta, r);

        // Loop over the conditions listed below.
        //
        for (int i = 1; i < children.size(); i++) {
          // Composite : #i
          // Get right hand condition
          Condition cb = children.get(i);

          // Evaluate the right hand side of the condition cb.evaluate() within
          // the switch statement
          // because the condition may be short-circuited due to the left hand
          // side (evaluation)
          switch (cb.getOperator()) {
            case OR:
              evaluation = evaluation || cb.evaluate(rowMeta, r);
              break;
            case AND:
              evaluation = evaluation && cb.evaluate(rowMeta, r);
              break;
            case OR_NOT:
              evaluation = evaluation || (!cb.evaluate(rowMeta, r));
              break;
            case AND_NOT:
              evaluation = evaluation && (!cb.evaluate(rowMeta, r));
              break;
            case XOR:
              evaluation = evaluation ^ cb.evaluate(rowMeta, r);
              break;
            default:
              break;
          }
        }

        // Composite: optionally negate
        if (isNegated()) {
          evaluation = !evaluation;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error evaluation condition [" + this + "]", e);
    }

    return evaluation;
  }

  public void addCondition(Condition cb) {
    if (isAtomic() && getLeftValueName() != null) {
      /*
       * Copy current atomic setup...
       */
      Condition current = new Condition(this);
      current.setNegated(isNegated());
      setNegated(false);
      children.add(current);
    } else {
      // Set default operator if not on first position...
      if (isComposite() && (cb.getOperator() == NONE)) {
        cb.setOperator(AND);
      }
    }
    children.add(cb);
  }

  public void addCondition(int idx, Condition cb) {
    if (isAtomic() && getLeftValueName() != null) {
      /*
       * Copy current atomic setup...
       */
      Condition current = new Condition(this);
      current.setNegated(isNegated());
      setNegated(false);
      children.add(current);
    } else {
      // Set default operator if not on first position...
      if (isComposite() && idx > 0 && cb.getOperator() == NONE) {
        cb.setOperator(AND);
      }
    }
    children.add(idx, cb);
  }

  public void removeCondition(int nr) {
    if (isComposite()) {
      Condition c = children.get(nr);
      children.remove(nr);

      // Nothing left or only one condition left: move it to the parent: make it atomic.

      boolean moveUp = isAtomic() || nrConditions() == 1;
      if (nrConditions() == 1) {
        c = getCondition(0);
      }

      if (moveUp) {
        setLeftValueName(c.getLeftValueName());
        setFunction(c.getFunction());
        setRightValueName(c.getRightValueName());
        setRightValue(c.getRightValue());
        setNegated(isNegated() ^ c.isNegated());
      }
    }
  }

  /**
   * This method moves up atomic conditions if there is only one sub-condition.
   *
   * @return true if there was a simplification.
   */
  public boolean simplify() {

    if (nrConditions() == 1) {
      Condition condition = getCondition(0);
      if (condition.isAtomic()) {
        return simplify(condition, this);
      }
    }

    boolean changed = false;
    for (int i = 0; i < nrConditions(); i++) {
      Condition condition = getCondition(i);
      changed |= condition.simplify();
      if (i == 0) {
        condition.setOperator(NONE);
      }
    }
    return changed;
  }

  private boolean simplify(Condition condition, Condition parent) {
    // If condition is atomic
    // AND
    // if parent only contain a single child: simplify
    //
    if (condition.isAtomic() && parent.nrConditions() == 1) {
      parent.setLeftValueName(condition.getLeftValueName());
      parent.setFunction(condition.getFunction());
      parent.setRightValueName(condition.getRightValueName());
      parent.setRightValue(condition.getRightValue());
      parent.setNegated(condition.isNegated() ^ parent.isNegated());
      parent.children.clear();
      return true;
    }
    return false;
  }

  public int nrConditions() {
    return children.size();
  }

  public Condition getCondition(int i) {
    return children.get(i);
  }

  public void setCondition(int i, Condition subCondition) {
    children.set(i, subCondition);
  }

  @Override
  public String toString() {
    return toString(0, true, true);
  }

  public String toString(int level, boolean showNegate, boolean showOperator) {
    StringBuilder retval = new StringBuilder();

    if (isAtomic()) {
      for (int i = 0; i < level; i++) {
        retval.append("  ");
      }

      if (showOperator && getOperator() != NONE) {
        retval.append(getOperatorDesc());
        retval.append(" ");
      } else {
        retval.append("        ");
      }

      // Atomic is negated?
      if (isNegated() && (showNegate || level > 0)) {
        retval.append("NOT ( ");
      } else {
        retval.append("      ");
      }

      if (function == TRUE) {
        retval.append(" TRUE");
      } else {
        retval.append(leftValueName + " " + getFunctionDesc());
        if (function != NULL && function != NOT_NULL) {
          if (rightValueName != null) {
            retval.append(" ");
            retval.append(rightValueName);
          } else {
            retval.append(
                " [" + (getRightValueString() == null ? "" : getRightValueString()) + "]");
          }
        }
      }

      if (isNegated() && (showNegate || level > 0)) {
        retval.append(" )");
      }

      retval.append(Const.CR);
    } else {

      // Group is negated?
      if (isNegated() && (showNegate || level > 0)) {
        for (int i = 0; i < level; i++) {
          retval.append("  ");
        }
        retval.append("NOT");
        retval.append(Const.CR);
      }
      // Group is preceded by an operator:
      if (getOperator() != NONE && (showOperator || level > 0)) {
        for (int i = 0; i < level; i++) {
          retval.append("  ");
        }
        retval.append(getOperatorDesc());
        retval.append(Const.CR);
      }
      for (int i = 0; i < level; i++) {
        retval.append("  ");
      }
      retval.append("(" + Const.CR);
      for (int i = 0; i < children.size(); i++) {
        Condition cb = children.get(i);
        retval.append(cb.toString(level + 1, true, i > 0));
      }
      for (int i = 0; i < level; i++) {
        retval.append("  ");
      }
      retval.append(")");
      retval.append(Const.CR);
    }

    return retval.toString();
  }

  public String getXml() throws HopValueException {
    try {
      return XmlHandler.openTag(XML_TAG)
          + XmlMetadataUtil.serializeObjectToXml(this)
          + XmlHandler.closeTag(XML_TAG);
    } catch (Exception e) {
      throw new HopValueException("Error serializing Condition to XML", e);
    }
  }

  public Condition(String xml) throws HopXmlException {
    this(XmlHandler.loadXmlString(xml, Condition.XML_TAG));
  }

  /**
   * Build a new condition using an XML Document Node
   *
   * @param conditionNode
   * @throws HopXmlException
   */
  public Condition(Node conditionNode) throws HopXmlException {
    this();
    XmlMetadataUtil.deSerializeFromXml(conditionNode, Condition.class, this, null);
  }

  public String[] getUsedFields() {
    Map<String, String> fields = new HashMap<>();
    getUsedFields(fields);
    return fields.keySet().toArray(new String[0]);
  }

  public void getUsedFields(Map<String, String> fields) {
    if (isAtomic()) {
      if (getLeftValueName() != null) {
        fields.put(getLeftValueName(), "-");
      }
      if (getRightValueName() != null) {
        fields.put(getRightValueName(), "-");
      }
    } else {
      for (int i = 0; i < nrConditions(); i++) {
        Condition subc = getCondition(i);
        subc.getUsedFields(fields);
      }
    }
  }

  @Getter
  @Setter
  public static final class CValue {
    @HopMetadataProperty(key = "name", isExcludedFromInjection = true)
    private String name;

    @HopMetadataProperty(key = "type", isExcludedFromInjection = true)
    private String type;

    @HopMetadataProperty(key = "text", isExcludedFromInjection = true)
    private String text;

    @HopMetadataProperty(key = "length", isExcludedFromInjection = true)
    private int length;

    @HopMetadataProperty(key = "precision", isExcludedFromInjection = true)
    private int precision;

    @HopMetadataProperty(key = "isnull", isExcludedFromInjection = true)
    private boolean nullValue;

    @HopMetadataProperty(key = "mask", isExcludedFromInjection = true)
    private String mask;

    public CValue() {}

    public CValue(CValue c) {
      this.name = c.name;
      this.type = c.type;
      this.text = c.text;
      this.length = c.length;
      this.precision = c.precision;
      this.nullValue = c.nullValue;
      this.mask = c.mask;
    }

    public CValue(ValueMetaAndData v) throws HopValueException {
      if (v == null) {
        return;
      }
      IValueMeta valueMeta = v.getValueMeta();
      Object valueData = v.getValueData();
      nullValue = valueMeta.isNull(valueData);
      text = valueMeta.getString(valueData);
      name = valueMeta.getName();
      type = valueMeta.getTypeDesc();
      length = valueMeta.getLength();
      precision = valueMeta.getPrecision();
      mask = valueMeta.getConversionMask();
    }

    public int getHopType() {
      return ValueMetaFactory.getIdForValueMeta(type);
    }

    /**
     * Create a new IValueMeta object to describe the right value of the condition
     *
     * @return A new value metadata object
     * @throws HopPluginException
     */
    public IValueMeta createValueMeta() throws HopPluginException {
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, getHopType());
      valueMeta.setLength(length, precision);
      valueMeta.setConversionMask(mask);
      valueMeta.setDecimalSymbol(String.valueOf(Const.DEFAULT_DECIMAL_SEPARATOR));
      valueMeta.setGroupingSymbol(null);
      valueMeta.setCurrencySymbol(null);
      return valueMeta;
    }

    /**
     * Convert the text stored to the desired data type in a compatible way
     *
     * @return
     */
    public Object createValueData() throws HopException {
      if (isNullValue()) {
        return null;
      }
      IValueMeta valueMeta = createValueMeta();

      ValueMetaAndData val = new ValueMetaAndData(valueMeta.getName(), text);
      val.setValueMeta(valueMeta);

      IValueMeta stringValueMeta = new ValueMetaString(valueMeta.getName());
      stringValueMeta.setConversionMetadata(valueMeta);

      return stringValueMeta.convertDataUsingConversionMetaData(val.getValueData());
    }
  }

  public enum Operator implements IEnumHasCode {
    NONE("-", OPERATOR_NONE),
    OR("OR", OPERATOR_OR),
    AND("AND", OPERATOR_AND),
    NOT("NOT", OPERATOR_NOT),
    OR_NOT(Condition.CONST_OR_NOT, OPERATOR_OR_NOT),
    AND_NOT(Condition.CONST_AND_NOT, OPERATOR_AND_NOT),
    XOR("XOR", OPERATOR_XOR);
    private final String code;
    private final int type;

    Operator(String code, int type) {
      this.code = code;
      this.type = type;
    }

    public static Operator lookupType(int type) {
      for (Operator value : values()) {
        if (value.getType() == type) {
          return value;
        }
      }
      return null;
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
     * Gets type
     *
     * @return value of type
     */
    public int getType() {
      return type;
    }
  }

  public enum Function implements IEnumHasCodeAndDescription {
    EQUAL("=", FUNC_EQUAL),
    NOT_EQUAL("<>", FUNC_NOT_EQUAL),
    SMALLER("<", FUNC_SMALLER),
    SMALLER_EQUAL("<=", FUNC_SMALLER_EQUAL),
    LARGER(">", FUNC_LARGER),
    LARGER_EQUAL(">=", FUNC_LARGER_EQUAL),
    REGEXP("REGEXP", FUNC_REGEXP),
    NULL("IS NULL", FUNC_NULL),
    NOT_NULL("IS NOT NULL", FUNC_NOT_NULL),
    IN_LIST("IN LIST", FUNC_IN_LIST),
    CONTAINS("CONTAINS", FUNC_CONTAINS),
    STARTS_WITH("STARTS WITH", FUNC_STARTS_WITH),
    ENDS_WITH("ENDS WITH", FUNC_ENDS_WITH),
    LIKE("LIKE", FUNC_LIKE),
    TRUE("TRUE", FUNC_TRUE),
    ;
    private final String code;
    private final String description;
    private final int type;

    Function(String code, int type) {
      this.code = code;
      this.description = code;
      this.type = type;
    }

    public static Function lookupType(int type) {
      for (Function value : values()) {
        if (value.getType() == type) {
          return value;
        }
      }
      return null;
    }

    public static Function lookupCode(String code) {
      for (Function value : values()) {
        if (value.getCode().equalsIgnoreCase(code)) {
          return value;
        }
      }
      return null;
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

    /**
     * Gets type
     *
     * @return value of type
     */
    public int getType() {
      return type;
    }
  }
}
