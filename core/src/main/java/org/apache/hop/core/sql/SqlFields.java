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

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.jdbc.ThinUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;

import java.util.ArrayList;
import java.util.List;

public class SqlFields {
  private static final String DISTINCT_PREFIX = "DISTINCT ";
  private String tableAlias;
  private IRowMeta serviceFields;
  private String fieldsClause;
  private List<SqlField> fields;
  private SqlFields selectFields;

  private boolean distinct;

  public SqlFields(String tableAlias, IRowMeta serviceFields, String fieldsClause)
      throws HopException {
    this(tableAlias, serviceFields, fieldsClause, false);
  }

  public SqlFields(
      String tableAlias, IRowMeta serviceFields, String fieldsClause, boolean orderClause)
      throws HopException {
    this(tableAlias, serviceFields, fieldsClause, orderClause, null);
  }

  public SqlFields(
      String tableAlias,
      IRowMeta serviceFields,
      String fieldsClause,
      boolean orderClause,
      SqlFields selectFields)
      throws HopException {
    this.tableAlias = tableAlias;
    this.serviceFields = serviceFields;
    this.fieldsClause = fieldsClause;
    this.selectFields = selectFields;
    fields = Lists.newArrayList();

    distinct = false;

    parse(orderClause);
  }

  private void parse(boolean orderClause) throws HopException {
    if (StringUtils.isEmpty(fieldsClause)) {
      return;
    }

    if (fieldsClause.regionMatches(true, 0, DISTINCT_PREFIX, 0, DISTINCT_PREFIX.length())) {
      distinct = true;
      fieldsClause = fieldsClause.substring(DISTINCT_PREFIX.length());
    }

    List<String> strings = new ArrayList<String>();
    int startIndex = 0;
    for (int index = 0; index < fieldsClause.length(); index++) {
      index = ThinUtil.skipChars(fieldsClause, index, '"', '\'', '(');
      if (index >= fieldsClause.length()) {
        strings.add(fieldsClause.substring(startIndex));
        startIndex = -1;
        break;
      }
      if (fieldsClause.charAt(index) == ',') {
        strings.add(fieldsClause.substring(startIndex, index));
        startIndex = index + 1;
      }
    }
    if (startIndex >= 0) {
      strings.add(fieldsClause.substring(startIndex));
    }

    // Now grab all the fields and determine their composition...
    //
    fields.clear();
    for (String string : strings) {
      String fieldString = ThinUtil.stripTableAlias(Const.trim(string), tableAlias);
      if ("*".equals(fieldString)) {
        // Add all service fields
        //
        for (IValueMeta valueMeta : serviceFields.getValueMetaList()) {
          fields.add(
              new SqlField(
                  tableAlias,
                  "\"" + valueMeta.getName() + "\"",
                  serviceFields,
                  orderClause,
                  selectFields));
        }
      } else {
        fields.add(new SqlField(tableAlias, fieldString, serviceFields, orderClause, selectFields));
      }
    }

    indexFields();
  }

  public List<SqlField> getFields() {
    return fields;
  }

  public List<SqlField> getNonAggregateFields() {
    List<SqlField> list = new ArrayList<SqlField>();
    for (SqlField field : fields) {
      if (field.getAggregation() == null) {
        list.add(field);
      }
    }
    return list;
  }

  public List<SqlField> getAggregateFields() {
    List<SqlField> list = new ArrayList<SqlField>();
    for (SqlField field : fields) {
      if (field.getAggregation() != null) {
        list.add(field);
      }
    }
    return list;
  }

  public boolean isEmpty() {
    return fields.isEmpty();
  }

  /**
   * Find a field by it's field name (not alias)
   *
   * @param fieldName the name of the field
   * @return the field or null if nothing was found.
   */
  public SqlField findByName(String fieldName) {
    for (SqlField field : fields) {
      if (field.getField().equalsIgnoreCase(fieldName)) {
        return field;
      }
    }
    return null;
  }

  /** @return the serviceFields */
  public IRowMeta getServiceFields() {
    return serviceFields;
  }

  /** @param serviceFields the serviceFields to set */
  public void setServiceFields(IRowMeta serviceFields) {
    this.serviceFields = serviceFields;
  }

  /** @return the fieldsClause */
  public String getFieldsClause() {
    return fieldsClause;
  }

  /** @param fieldsClause the fieldsClause to set */
  public void setFieldsClause(String fieldsClause) {
    this.fieldsClause = fieldsClause;
  }

  /** @return the selectFields */
  public SqlFields getSelectFields() {
    return selectFields;
  }

  /** @param selectFields the selectFields to set */
  public void setSelectFields(SqlFields selectFields) {
    this.selectFields = selectFields;
  }

  /** @param fields the fields to set */
  public void setFields(List<SqlField> fields) {
    this.fields = fields;
    indexFields();
  }

  private void indexFields() {
    for (int i = 0; i < fields.size(); i++) {
      fields.get(i).setFieldIndex(i);
    }
  }

  /** @return true if one or more fields is an aggregation. */
  public boolean hasAggregates() {
    for (SqlField field : fields) {
      if (field.getAggregation() != null) {
        return true;
      }
    }
    return false;
  }

  public List<SqlField> getIifFunctionFields() {
    List<SqlField> list = new ArrayList<SqlField>();

    for (SqlField field : fields) {
      if (field.getIif() != null) {
        list.add(field);
      }
    }

    return list;
  }

  public List<SqlField> getRegularFields() {
    List<SqlField> list = new ArrayList<SqlField>();

    for (SqlField field : fields) {
      if (field.getIif() == null
          && field.getAggregation() == null
          && field.getValueData() == null) {
        list.add(field);
      }
    }

    return list;
  }

  public List<SqlField> getConstantFields() {
    List<SqlField> list = new ArrayList<SqlField>();

    for (SqlField field : fields) {
      if (field.getValueMeta() != null && field.getValueData() != null) {
        list.add(field);
      }
    }

    return list;
  }

  /** @return the distinct */
  public boolean isDistinct() {
    return distinct;
  }

  /** @return the tableAlias */
  public String getTableAlias() {
    return tableAlias;
  }
}
