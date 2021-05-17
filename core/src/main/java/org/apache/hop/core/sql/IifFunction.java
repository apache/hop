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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopSqlException;
import org.apache.hop.core.jdbc.ThinUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.ValueMetaAndData;

public class IifFunction {
  private String tableAlias;
  private String conditionClause;
  private SqlCondition sqlCondition;
  private IRowMeta serviceFields;

  private String trueValueString;
  private ValueMetaAndData trueValue;
  private boolean trueField;
  private String falseValueString;
  private ValueMetaAndData falseValue;
  private boolean falseField;

  public IifFunction(
      String tableAlias,
      String conditionClause,
      String trueValueString,
      String falseValueString,
      IRowMeta serviceFields)
      throws HopException {
    this.tableAlias = tableAlias;
    this.conditionClause = conditionClause;
    this.trueValueString = trueValueString;
    this.falseValueString = falseValueString;
    this.serviceFields = serviceFields;

    // Parse the Sql
    this.sqlCondition = new SqlCondition(tableAlias, conditionClause, serviceFields);

    // rudimentary string, date, number, integer determination
    //
    trueValue = extractValue(trueValueString, true);
    falseValue = extractValue(falseValueString, false);
  }

  private ValueMetaAndData extractValue(String string, boolean trueIndicator) throws HopException {
    if (StringUtils.isEmpty(string)) {
      return null;
    }

    ValueMetaAndData value = ThinUtil.attemptDateValueExtraction(string);
    if (value != null) {
      return value;
    }

    value = ThinUtil.attemptStringValueExtraction(string);
    if (value != null) {
      return value;
    }

    // See if it's a field...
    //
    int index = serviceFields.indexOfValue(string);
    if (index >= 0) {
      if (trueIndicator) {
        trueField = true;
      } else {
        falseField = true;
      }
      return new ValueMetaAndData(serviceFields.getValueMeta(index), null);
    }

    value = ThinUtil.attemptBooleanValueExtraction(string);
    if (value != null) {
      return value;
    }

    value = ThinUtil.attemptIntegerValueExtraction(string);
    if (value != null) {
      return value;
    }

    value = ThinUtil.attemptNumberValueExtraction(string);
    if (value != null) {
      return value;
    }

    value = ThinUtil.attemptBigNumberValueExtraction(string);
    if (value != null) {
      return value;
    }

    throw new HopSqlException("Unable to determine value data type for string: [" + string + "]");
  }

  /** @return the conditionClause */
  public String getConditionClause() {
    return conditionClause;
  }

  /** @return the sqlCondition */
  public SqlCondition getSqlCondition() {
    return sqlCondition;
  }

  /** @return the serviceFields */
  public IRowMeta getServiceFields() {
    return serviceFields;
  }

  /** @return the trueValueString */
  public String getTrueValueString() {
    return trueValueString;
  }

  /** @return the trueValue */
  public ValueMetaAndData getTrueValue() {
    return trueValue;
  }

  /** @return the falseValueString */
  public String getFalseValueString() {
    return falseValueString;
  }

  /** @return the falseValue */
  public ValueMetaAndData getFalseValue() {
    return falseValue;
  }

  /** @return the falseField */
  public boolean isFalseField() {
    return falseField;
  }

  /** @return the trueField */
  public boolean isTrueField() {
    return trueField;
  }

  /** @return the tableAlias */
  public String getTableAlias() {
    return tableAlias;
  }
}
