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

package org.apache.hop.core.sql;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopSqlException;
import org.apache.hop.core.jdbc.ThinUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlCondition {

  private String tableAlias;
  IRowMeta serviceFields;
  private Condition condition;
  private String conditionClause;
  private SqlFields selectFields;
  private Map<String, DateToStrFunction> dateToStrFunctions = Collections.emptyMap();

  private static final Pattern PARAMETER_REGEX_PATTERN =
      Pattern.compile("(?i)^PARAMETER\\s*\\(\\s*'(.*)'\\s*\\)\\s*=\\s*'?([^']*)'?$");

  private static final Pattern DATE_TO_STR_REGEX_PATTERN =
      Pattern.compile("(?i)^DATE_TO_STR\\s*\\(\\s*([^,]*)\\s*,?\\s*'?(([^']|'')*)'?\\s*\\)$");

  public SqlCondition(String tableAlias, String conditionSql, IRowMeta serviceFields)
      throws HopException {
    this(tableAlias, conditionSql, serviceFields, null);
  }

  public SqlCondition(
      String tableAlias, String conditionSql, IRowMeta serviceFields, SqlFields selectFields)
      throws HopException {
    this.tableAlias = tableAlias;
    this.conditionClause = conditionSql;
    this.serviceFields = serviceFields;
    this.selectFields = selectFields;

    parse();
  }

  /**
   * Support for conditions is very simple for now:
   *
   * <p><Field> <operator> <value> <Field> <operator> <other field>
   *
   * <p>TODO: figure out a simple algorithm to split up on brackets, AND, OR
   *
   * @throws HopSqlException
   */
  private void parse() throws HopException {

    // Split the condition clause on brackets and operators (AND, OR)
    // Since the Kettle condition simply evaluates without precedence, we'll just
    // break the clause down into pieces and then define one or more conditions
    // depending on the number of pieces we found.
    //
    condition = splitConditionByOperator(conditionClause, null, Condition.OPERATOR_NONE);
    for (int i = 0; i < 20; i++) {
      // Simplify
      if (!condition.simplify()) {
        break;
      }
    }
  }

  /**
   * Searches for the given word in a clause and returns the start index if found, -1 if not found.
   * This method skips brackets and single quotes. Case is ignored
   *
   * @param clause the clause
   * @param word the word to search
   * @param startIndex the index to start searching
   * @return the index if the word is found, -1 if not found
   * @throws HopSqlException
   */
  int searchForWord(String clause, String word, int startIndex) throws HopSqlException {
    int index = startIndex;
    while (index < clause.length()) {
      index = ThinUtil.skipChars(clause, index, '\'', '(');
      if (index + word.length() > clause.length()) {
        return -1; // done.
      }
      if (clause.substring(index).toUpperCase().startsWith(word.toUpperCase())) {
        if (index > 0 && String.valueOf(clause.charAt(index - 1)).matches("\\S")) {
          // symbol before is not a whitespace character
          index++;
          continue;
        }
        if (index + word.length() < clause.length()
            && String.valueOf(clause.charAt(index + word.length())).matches("\\S")) {
          // symbol after is not a whitespace character
          index++;
          continue;
        }
        return index;
      }
      index++;
    }
    return -1;
  }

  private Condition splitConditionByOperator(
      String clause, Condition parentCondition, int parentOperator)
      throws HopSqlException, HopPluginException {
    if (parentCondition == null) {
      parentCondition = new Condition();
    } else {
      // add a new condition to the list...
      //
      Condition c = new Condition();
      c.setOperator(parentOperator);
      parentCondition.addCondition(c);
      parentCondition = c;
    }

    // First we find bracket pairs, then OR, then AND
    //
    // C='foo' AND ( A=5 OR B=6 )
    // ( A=4 OR B=3 ) AND ( A=5 OR B=6 )
    // ( clause1 ) AND ( clause2) AND ( clause3 )
    //

    // First try to split by OR, leaving grouped AND blocks.
    // e.g. A OR B AND C OR D --> A OR ( B AND C ) OR D --> A, B AND C, D
    //
    int lastIndex = splitByOperator(clause, parentCondition, "OR", Condition.OPERATOR_OR);
    if (lastIndex == 0) {

      // No OR operator(s) was found, now we can look for AND operators in the clause...
      // Try to split by AND
      //
      lastIndex = splitByOperator(clause, parentCondition, "AND", Condition.OPERATOR_AND);
      if (lastIndex == 0) {

        // No AND operator(s) was found
        //
        String cleaned = Const.trim(clause);
        boolean negation = false;

        // See if it's a PARAMETER
        //
        Matcher paramMatcher = PARAMETER_REGEX_PATTERN.matcher(cleaned);
        if (paramMatcher.matches()) {
          String parameterName = paramMatcher.group(1);
          String parameterValue = paramMatcher.group(2);

          validateParam(clause, parameterName, parameterValue);

          parentCondition.addCondition(
              createParameterCondition(Condition.OPERATOR_OR, parameterName, parameterValue));
        } else {

          // See if this elementary block is a NOT ( ) construct
          //
          if (Pattern.matches("^NOT\\s*\\(.*\\)$", cleaned.toUpperCase())) {
            negation = true;
            cleaned = Const.trim(cleaned.substring(3));
          }

          // No AND or OR operators found,
          // First remove possible brackets though
          //
          if (cleaned.startsWith("(") && cleaned.endsWith(")")) {
            // Brackets are skipped above so we add a new condition to the list, and remove the
            // brackets
            //
            cleaned = cleaned.substring(1, cleaned.length() - 1);
            Condition c =
                splitConditionByOperator(cleaned, parentCondition, Condition.OPERATOR_NONE);
            c.setNegated(negation);

          } else {

            // Atomic condition
            //
            Condition subCondition = parseAtomicCondition(cleaned);
            subCondition.setOperator(Condition.OPERATOR_OR);
            parentCondition.addCondition(subCondition);
          }
        }
      }
    }

    return parentCondition;
  }

  /** Creates a Condition object which will act as a container for a Parameter key/value. */
  private Condition createParameterCondition(
      int orConditionOperator, String parameterName, String parameterValue) {
    Condition subCondition =
        new Condition(
            parameterName,
            Condition.FUNC_TRUE,
            parameterName,
            new ValueMetaAndData(new ValueMetaString("string"), Const.NVL(parameterValue, "")));
    subCondition.setOperator(orConditionOperator);
    return subCondition;
  }

  private void validateParam(String clause, String parameterName, String parameterValue)
      throws HopSqlException {
    if (StringUtils.isEmpty(parameterName)) {
      throw new HopSqlException("A parameter name cannot be empty in : " + clause);
    }
    if (Utils.isEmpty(parameterValue) || parameterValue.equals("''")) {
      throw new HopSqlException("A parameter value cannot be empty in : " + clause);
    }
  }

  private int splitByOperator(
      String clause, Condition parentCondition, String operatorString, int conditionOperator)
      throws HopSqlException, HopPluginException {
    int lastIndex = 0;
    int index = 0;
    while (index < clause.length() && (index = searchForWord(clause, operatorString, index)) >= 0) {
      // Split on the index --> ( clause1 ), ( clause2), (clause 3)
      //
      String left = clause.substring(lastIndex, index);
      splitConditionByOperator(left, parentCondition, conditionOperator);
      index += operatorString.length();
      lastIndex = index;
    }

    // let's not forget to split the last right part or the OR(s)
    //
    if (lastIndex > 0) {
      String right = clause.substring(lastIndex);
      splitConditionByOperator(right, parentCondition, conditionOperator);
    }

    return lastIndex;
  }

  private Condition parseAtomicCondition(String clause) throws HopSqlException, HopPluginException {
    List<String> clauseElements = splitConditionClause(clause);
    if (clauseElements.size() > 3) {
      throw new HopSqlException(
          "Unfortunately support for conditions is still very rudimentary, only 1 simple condition is supported ["
              + clause
              + "]");
    }
    String left = getCleansedName(getAlias(clauseElements.get(0)).orElse(clauseElements.get(0)));
    left = ThinUtil.unQuote(left.replaceAll("\"\"", "\""));
    int opFunction = Condition.getFunction(clauseElements.get(1));
    String right = getCleansedName(clauseElements.get(2));

    // Process instances of DATE_TO_STR
    left = processDateToStr(left);
    right = processDateToStr(right);

    if (isEqualityComparisonOfLiteralValues(clauseElements)) {
      // comparison of literals, e.g. ( 1 = 0 ) or ( 1 = 1 )
      // not currently supporting general literal comparisons.
      return new Condition(!left.equals(right), left, Condition.FUNC_TRUE, right, null);
    }

    boolean negation = Pattern.matches("^NULL$", right.trim().toUpperCase());

    ValueMetaAndData value;

    int function = negation ? Condition.FUNC_TRUE : opFunction;
    if (function == Condition.FUNC_IN_LIST) {
      // lose the brackets
      //
      String trimmed = Const.trim(right);
      String partClause = trimmed.substring(1, trimmed.length() - 1);
      List<String> parts = ThinUtil.splitClause(partClause, ',', '\'');
      StringBuilder valueString = new StringBuilder();
      for (String part : parts) {
        if (valueString.length() > 0) {
          valueString.append(";");
        }

        part = Const.trim(part);

        ValueMetaAndData extractedConstraint = ThinUtil.extractConstant(part);
        if (extractedConstraint == null) {
          throw new HopSqlException("Condition parsing error: [" + part + "]");
        } else if (!extractedConstraint.getValueMeta().isNumber()
            && !extractedConstraint.getValueMeta().isBigNumber()) {
          part = extractedConstraint.toString();
        }

        // Escape semi-colons
        //
        part = part.replace(";", "\\;");

        valueString.append(part);
      }
      value = new ValueMetaAndData(new ValueMetaString("constant-in-list"), valueString.toString());
    } else {

      // Mondrian, analyzer CONTAINS hack:
      // '%' || 'string' || '%' --> '%string%'
      //
      String prefix = "'%'";
      String suffix = "'%'";
      if (right.startsWith(prefix) && right.endsWith(suffix)) {
        int leftOrIndex = right.indexOf("||");
        if (leftOrIndex > 0) {
          int rightOrIndex = right.indexOf("||", leftOrIndex + 2);
          if (rightOrIndex > 0) {
            String raw = Const.trim(right.substring(leftOrIndex + 2, rightOrIndex));
            if (raw.startsWith("'") && raw.endsWith("'")) {
              right = "'%" + raw.substring(1, raw.length() - 1) + "%'";
            }
          }
        }
      }

      value = ThinUtil.extractConstant(right);
    }

    if (value != null) {
      return new Condition(negation, left, function, null, value);
    } else {
      return new Condition(negation, left, function, right, null);
    }
  }

  /**
   * Replace DATE_TO_STR() call with internal field name and store call to name mapping.
   *
   * @param element Atomic element to match.
   * @return Internal field name for call result if it is a DATE_TO_STR() call, otherwise returns
   *     the original value.
   */
  private String processDateToStr(String element) throws HopSqlException {
    Matcher dateToStrMatcher = DATE_TO_STR_REGEX_PATTERN.matcher(element);
    if (dateToStrMatcher.matches()) {
      String dateToStrFieldName = dateToStrMatcher.group(1);
      String dateToStrMatcherValue = resolveEscapedSingleQuotes(dateToStrMatcher.group(2));

      // get clean field name
      dateToStrFieldName = getCleansedName(getAlias(dateToStrFieldName).orElse(dateToStrFieldName));

      validateDateToStrField(element, dateToStrFieldName);

      // placeholder field name & lookup key
      String resultFieldName =
          "__date_to_str_" + dateToStrFieldName.toLowerCase() + "_" + dateToStrMatcherValue;
      if (dateToStrFunctions.containsKey(resultFieldName)) {
        return dateToStrFunctions.get(resultFieldName).getResultName();
      }

      // create new mapping
      if (dateToStrFunctions.isEmpty()) {
        // on first use, replace with a mutable instance
        dateToStrFunctions = new HashMap<>();
      }
      dateToStrFunctions.put(
          resultFieldName,
          new DateToStrFunction(dateToStrFieldName, dateToStrMatcherValue, resultFieldName));
      return resultFieldName;
    } else {
      return element;
    }
  }

  private String resolveEscapedSingleQuotes(String str) {
    return str.replaceAll("''", "'");
  }

  private void validateDateToStrField(String element, String dateToStrFieldName)
      throws HopSqlException {
    // field name must exist in serviceFields and be either a date or timestamp
    int dateToStrFieldIndex = this.serviceFields.indexOfValue(dateToStrFieldName);
    if (dateToStrFieldIndex < 0) {
      throw new HopSqlException("Unknown field '" + dateToStrFieldName + "' in : " + element);
    } else {
      int dateToStrFieldType = this.serviceFields.getValueMeta(dateToStrFieldIndex).getType();
      if (!(dateToStrFieldType == IValueMeta.TYPE_DATE
          || dateToStrFieldType == IValueMeta.TYPE_TIMESTAMP)) {
        throw new HopSqlException(
            "Invalid field type in : " + element + " : type must be DATE or TIMESTAMP");
      }
    }
  }

  private boolean isEqualityComparisonOfLiteralValues(List<String> clauseElements)
      throws HopSqlException {
    String left = clauseElements.get(0);
    int opFunction = Condition.getFunction(clauseElements.get(1));
    String right = clauseElements.get(2);
    return isLiteral(left) && isLiteral(right) && opFunction == Condition.FUNC_EQUAL;
  }

  private boolean isAggregateField(String left) throws HopSqlException {
    // match scheme is consistent with the current impl of SqlField.
    // refactoring SqlField to extract agg expression determination is
    // too risky to mess with.
    return Arrays.stream(SqlAggregation.values())
        .filter(agg -> left.trim().toUpperCase().startsWith(agg + "("))
        .findFirst()
        .isPresent();
  }

  private boolean isDateToStrTemporaryField(String element) {
    Matcher dateToStrMatcher = DATE_TO_STR_REGEX_PATTERN.matcher(element);
    return dateToStrMatcher.matches();
  }

  /**
   * If the element is not determined to have an alias, and doesn't map to a service field, then
   * we'll assume it's a literal value.
   */
  private boolean isLiteral(String element) throws HopSqlException {
    return !getAlias(element).isPresent()
        && !ThinUtil.getIValueMeta(getCleansedName(element), getServiceFields()).isPresent()
        && !isAggregateField(element)
        && !isDateToStrTemporaryField(element);
  }

  /**
   * Removes table alias and quotes, if present. If the field maps to a service field, retrieves the
   * service field's name.
   */
  private String getCleansedName(String field) {
    return ThinUtil.getIValueMeta(field, getServiceFields())
        .map(IValueMeta::getName)
        .orElse(ThinUtil.stripQuotes(ThinUtil.stripQuoteTableAlias(field, tableAlias), '"'));
  }

  /**
   * Returns the alias associated with expression, if one is present.
   *
   * <p>Currently only HAVING clauses will have a selectFields set. Swapping in the alias with
   * HAVING clauses are necessary with queries like this SELECT country, count(distinct id) as
   * customerCount FROM service GROUP BY country HAVING count(distinct id) > 10 Since the
   * "count(distinct id)" needs to be associated with the alias name used in the generated trans.
   *
   * @param expression
   */
  private Optional<String> getAlias(String expression) {
    if (selectFields != null) {
      for (SqlField field : selectFields.getFields()) {
        if (field.getExpression().equalsIgnoreCase(expression)) {
          if (StringUtils.isNotEmpty(field.getAlias())) {
            return Optional.of(field.getAlias());
          }
        }
      }
    }
    return Optional.empty();
  }

  /**
   * We need to split conditions on a single operator (for now)
   *
   * @param clause
   * @return 3 string list (left, operator, right)
   * @throws HopSqlException
   */
  private List<String> splitConditionClause(String clause) throws HopSqlException {
    List<String> strings = new ArrayList<>();

    String[] operators =
        new String[] {
          "<>",
          ">=",
          "=>",
          "<=",
          "=<",
          "<",
          ">",
          "=",
          " REGEX ",
          " IN ",
          " IS NOT NULL",
          " IS NULL",
          " LIKE",
          "CONTAINS "
        };
    int[] functions =
        new int[] {
          Condition.FUNC_NOT_EQUAL,
          Condition.FUNC_LARGER_EQUAL,
          Condition.FUNC_LARGER_EQUAL,
          Condition.FUNC_SMALLER_EQUAL,
          Condition.FUNC_SMALLER_EQUAL,
          Condition.FUNC_SMALLER,
          Condition.FUNC_LARGER,
          Condition.FUNC_EQUAL,
          Condition.FUNC_REGEXP,
          Condition.FUNC_IN_LIST,
          Condition.FUNC_NOT_NULL,
          Condition.FUNC_NULL,
          Condition.FUNC_LIKE,
          Condition.FUNC_CONTAINS
        };
    int index = 0;

    while (index < clause.length()) {
      index = ThinUtil.skipChars(clause, index, '\'', '"');
      for (String operator : operators) {
        if (index <= clause.length() - operator.length()) {
          if (clause.substring(index).toUpperCase().startsWith(operator)) {
            int functionIndex = Const.indexOfString(operator, operators);

            // OK, we found an operator.
            // The part before is the first string
            //
            String left = Const.trim(clause.substring(0, index));
            String op = Condition.functions[functions[functionIndex]];
            String right = Const.trim(clause.substring(index + operator.length()));

            return Arrays.asList(left, op, right);
          }
        }
      }

      index++;
    }

    return strings;
  }

  /** @return the serviceFields */
  public IRowMeta getServiceFields() {
    return serviceFields;
  }

  /** @param serviceFields the serviceFields to set */
  public void setServiceFields(IRowMeta serviceFields) {
    this.serviceFields = serviceFields;
  }

  /** @return the condition */
  public Condition getCondition() {
    return condition;
  }

  /** @param condition the condition to set */
  public void setCondition(Condition condition) {
    this.condition = condition;
  }

  /** @return the conditionClause */
  public String getConditionClause() {
    return conditionClause;
  }

  /** @param conditionClause the conditionClause to set */
  public void setConditionClause(String conditionClause) {
    this.conditionClause = conditionClause;
  }

  public boolean isEmpty() {
    return condition.isEmpty();
  }

  /** @return the selectFields */
  public SqlFields getSelectFields() {
    return selectFields;
  }

  /** @return the tableAlias */
  public String getTableAlias() {
    return tableAlias;
  }

  /**
   * Extract the list of having fields from this having condition
   *
   * @param aggFields
   * @param rowMeta
   * @return
   * @throws HopSqlException
   */
  public List<SqlField> extractHavingFields(
      List<SqlField> selectFields, List<SqlField> aggFields, IRowMeta rowMeta) throws HopException {
    List<SqlField> list = new ArrayList<>();

    // Get a list of all the lowest level field names and see if we can parse them as aggregation
    // fields
    //
    List<String> expressions = new ArrayList<>();
    addExpressions(condition, expressions);

    for (String expression : expressions) {
      // See if we already specified the aggregation in the Select clause, let's aggregate twice.
      //
      SqlField aggField = SqlField.searchSQLFieldByFieldOrAlias(aggFields, expression);
      if (aggField == null) {

        SqlField field = new SqlField(tableAlias, expression, serviceFields);
        if (field.getAggregation() != null) {
          field.setField(expression);
          list.add(field);
        }
      }
    }

    return list;
  }

  private void addExpressions(Condition condition, List<String> expressions) {
    if (condition.isAtomic()) {
      if (!expressions.contains(condition.getLeftValuename())) {
        expressions.add(condition.getLeftValuename());
      }
    } else {
      for (Condition child : condition.getChildren()) {
        addExpressions(child, expressions);
      }
    }
  }

  /** @return all unique calls to DATE_TO_STR present in this condition */
  public Collection<DateToStrFunction> getDateToStrFunctions() {
    return dateToStrFunctions.values();
  }
}
