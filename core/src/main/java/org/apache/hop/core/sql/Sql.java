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
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.jdbc.ThinUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;

import java.util.List;

public class Sql {
  private String sqlString;

  private IRowMeta rowMeta;

  private String serviceClause;
  private String namespace;
  private String serviceName;
  private String serviceAlias;

  private String selectClause;
  private SqlFields selectFields;

  private String whereClause;
  private SqlCondition whereCondition;

  private String groupClause;
  private SqlFields groupFields;

  private String havingClause;
  private SqlCondition havingCondition;

  private String orderClause;
  private SqlFields orderFields;

  private String limitClause;
  private SqlLimit limitValues;

  /**
   * Create a new Sql object by parsing the supplied Sql string. This is a simple implementation
   * with only one table allows
   *
   * @param sqlString the Sql string to parse
   */
  public Sql(String sqlString) throws HopException {
    this.sqlString = sqlString;

    splitSql(sqlString);
  }

  private void splitSql(String sql) throws HopException {
    // First get the major blocks...
    /*
     * SELECT A, B, C FROM Step
     *
     * SELECT A, B, C FROM Step WHERE D > 6 AND E = 'abcd'
     *
     * SELECT A, B, C FROM Step ORDER BY B, A, C
     *
     * SELECT A, B, sum(C) FROM Step WHERE D > 6 AND E = 'abcd' GROUP BY A, B HAVING sum(C) > 100 ORDER BY sum(C) DESC
     */
    //
    FoundClause foundClause = ThinUtil.findClauseWithRest(sql, "SELECT", "FROM");
    selectClause = foundClause.getClause();
    if (foundClause.getRest() == null) {
      return;
    }
    foundClause =
        ThinUtil.findClauseWithRest(
            foundClause.getRest(), "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT");
    serviceClause = foundClause.getClause();
    parseServiceClause();
    if (foundClause.getRest() == null) {
      return;
    }
    foundClause =
        ThinUtil.findClauseWithRest(
            foundClause.getRest(), "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT");
    whereClause = foundClause.getClause();
    if (foundClause.getRest() == null) {
      return;
    }
    foundClause =
        ThinUtil.findClauseWithRest(
            foundClause.getRest(), "GROUP BY", "HAVING", "ORDER BY", "LIMIT");
    groupClause = foundClause.getClause();
    if (foundClause.getRest() == null) {
      return;
    }
    foundClause = ThinUtil.findClauseWithRest(foundClause.getRest(), "HAVING", "ORDER BY", "LIMIT");
    havingClause = foundClause.getClause();
    if (foundClause.getRest() == null) {
      return;
    }
    foundClause = ThinUtil.findClauseWithRest(foundClause.getRest(), "ORDER BY", "LIMIT");
    orderClause = foundClause.getClause();
    if (foundClause.getRest() == null) {
      return;
    }
    foundClause = ThinUtil.findClauseWithRest(foundClause.getRest(), "LIMIT");
    limitClause = foundClause.getClause();
  }

  public boolean hasServiceClause() {
    return StringUtils.isNotEmpty(serviceClause);
  }

  private void parseServiceClause() throws HopException {
    if (StringUtils.isEmpty(serviceClause)) {
      serviceName = "dual";
      return;
    }

    List<String> parts = ThinUtil.splitClause(serviceClause, ' ', '"');
    if (parts.size() >= 1) {
      // The service name is in the first part/
      // However, it can be in format Namespace.Service (Schema.Table)
      //
      List<String> list = ThinUtil.splitClause(parts.get(0), '.', '"');
      if (list.size() == 1) {
        namespace = null;
        serviceName = ThinUtil.stripQuotes(list.get(0), '"');
      }
      if (list.size() == 2) {
        namespace = ThinUtil.stripQuotes(list.get(0), '"');
        serviceName = ThinUtil.stripQuotes(list.get(1), '"');
      }
      if (list.size() > 2) {
        throw new HopException(
            "Too many parts detected in table name specification [" + serviceClause + "]");
      }
    }

    if (parts.size() == 2) {
      serviceAlias = ThinUtil.stripQuotes(parts.get(1), '"');
    }
    if (parts.size() == 3) {

      if (parts.get(1).equalsIgnoreCase("AS")) {
        serviceAlias = ThinUtil.stripQuotes(parts.get(2), '"');
      } else {
        throw new HopException("AS expected in from clause: " + serviceClause);
      }
    }
    if (parts.size() > 3) {
      StringBuilder builder = new StringBuilder();
      builder.append("Found ");
      builder.append(parts.size());
      builder.append(
          " parts for the FROM clause when only a table name and optionally an alias is supported: ");
      builder.append(serviceClause);
      throw new HopException(builder.toString());
    }

    serviceAlias = Const.NVL(serviceAlias, serviceName);
  }

  public void parse(IRowMeta rowMeta) throws HopException {

    // Now do the actual parsing and interpreting of the Sql, map it to the service row metadata

    this.rowMeta = rowMeta;

    selectFields = new SqlFields(serviceAlias, rowMeta, selectClause);
    if (StringUtils.isNotEmpty(whereClause)) {
      whereCondition = new SqlCondition(serviceAlias, whereClause, rowMeta);
    }
    if (StringUtils.isNotEmpty(groupClause)) {
      groupFields = new SqlFields(serviceAlias, rowMeta, groupClause);
    } else {
      groupFields = new SqlFields(serviceAlias, new RowMeta(), null);
    }
    if (StringUtils.isNotEmpty(havingClause)) {
      havingCondition = new SqlCondition(serviceAlias, havingClause, rowMeta, selectFields);
    }
    if (StringUtils.isNotEmpty(orderClause)) {
      orderFields = new SqlFields(serviceAlias, rowMeta, orderClause, true, selectFields);
    }
    if (StringUtils.isNotEmpty(limitClause)) {
      limitValues = new SqlLimit(limitClause);
    }
  }

  public String getSqlString() {
    return sqlString;
  }

  public IRowMeta getRowMeta() {
    return rowMeta;
  }

  public String getServiceName() {
    return serviceName;
  }

  /** @return the selectClause */
  public String getSelectClause() {
    return selectClause;
  }

  /** @param selectClause the selectClause to set */
  public void setSelectClause(String selectClause) {
    this.selectClause = selectClause;
  }

  /** @return the whereClause */
  public String getWhereClause() {
    return whereClause;
  }

  /** @param whereClause the whereClause to set */
  public void setWhereClause(String whereClause) {
    this.whereClause = whereClause;
  }

  /** @return the groupClause */
  public String getGroupClause() {
    return groupClause;
  }

  /** @param groupClause the groupClause to set */
  public void setGroupClause(String groupClause) {
    this.groupClause = groupClause;
  }

  /** @return the havingClause */
  public String getHavingClause() {
    return havingClause;
  }

  /** @param havingClause the havingClause to set */
  public void setHavingClause(String havingClause) {
    this.havingClause = havingClause;
  }

  /** @return the orderClause */
  public String getOrderClause() {
    return orderClause;
  }

  /** @param orderClause the orderClause to set */
  public void setOrderClause(String orderClause) {
    this.orderClause = orderClause;
  }

  /** @return the limitClause */
  public String getLimitClause() {
    return limitClause;
  }

  /** @param limitClause the orderClause to set */
  public void setLimitClause(String limitClause) {
    this.limitClause = limitClause;
  }

  /** @return the limitValues */
  public SqlLimit getLimitValues() {
    return limitValues;
  }

  /** @param limitValues the limitValues to set */
  public void setLimitValues(SqlLimit limitValues) {
    this.limitValues = limitValues;
  }

  /** @param sqlString the sql string to set */
  public void setSqlString(String sqlString) {
    this.sqlString = sqlString;
  }

  /** @return the selectFields */
  public SqlFields getSelectFields() {
    return selectFields;
  }

  /** @param selectFields the selectFields to set */
  public void setSelectFields(SqlFields selectFields) {
    this.selectFields = selectFields;
  }

  /** @return the groupFields */
  public SqlFields getGroupFields() {
    return groupFields;
  }

  /** @param groupFields the groupFields to set */
  public void setGroupFields(SqlFields groupFields) {
    this.groupFields = groupFields;
  }

  /** @return the orderFields */
  public SqlFields getOrderFields() {
    return orderFields;
  }

  /** @param orderFields the orderFields to set */
  public void setOrderFields(SqlFields orderFields) {
    this.orderFields = orderFields;
  }

  /** @return the whereCondition */
  public SqlCondition getWhereCondition() {
    return whereCondition;
  }

  /** @param whereCondition the whereCondition to set */
  public void setWhereCondition(SqlCondition whereCondition) {
    this.whereCondition = whereCondition;
  }

  /** @param rowMeta the rowMeta to set */
  public void setRowMeta(IRowMeta rowMeta) {
    this.rowMeta = rowMeta;
  }

  /** @param serviceName the serviceName to set */
  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  /** @return the havingCondition */
  public SqlCondition getHavingCondition() {
    return havingCondition;
  }

  /** @param havingCondition the havingCondition to set */
  public void setHavingCondition(SqlCondition havingCondition) {
    this.havingCondition = havingCondition;
  }

  /** @return the namespace */
  public String getNamespace() {
    return namespace;
  }
}
