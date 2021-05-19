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

import org.apache.hop.core.Condition;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.jdbc.ThinUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlTest {

  @Before
  public void before() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  public void testSql01() throws Exception {
    IRowMeta rowMeta = generateTest3RowMeta();

    String sqlString = "SELECT A, B, C\nFROM Service\nWHERE B > 5\nORDER BY B DESC";

    Sql sql = new Sql(sqlString);

    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertEquals("A, B, C", sql.getSelectClause());
    List<SqlField> selectFields = sql.getSelectFields().getFields();
    assertEquals(3, selectFields.size());

    assertEquals("B > 5", sql.getWhereClause());
    SqlCondition whereCondition = sql.getWhereCondition();
    assertNotNull(whereCondition.getCondition());

    assertNull(sql.getGroupClause());
    assertNull(sql.getHavingClause());
    assertEquals("B DESC", sql.getOrderClause());
    List<SqlField> orderFields = sql.getOrderFields().getFields();
    assertEquals(1, orderFields.size());
    SqlField orderField = orderFields.get(0);
    assertTrue(orderField.isOrderField());
    assertFalse(orderField.isAscending());
    assertNull(orderField.getAlias());
    assertEquals("B", orderField.getValueMeta().getName().toUpperCase());
  }

  @Test
  public void testSql02() throws Exception {
    IRowMeta rowMeta = generateTest3RowMeta();

    String sqlString =
        "SELECT A as \"FROM\", B as \"TO\", C\nFROM Service\nWHERE B > 5\nORDER BY B DESC";

    Sql sql = new Sql(sqlString);

    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertEquals("A as \"FROM\", B as \"TO\", C", sql.getSelectClause());
    assertEquals("B > 5", sql.getWhereClause());
    assertNull(sql.getGroupClause());
    assertNull(sql.getHavingClause());
    assertEquals("B DESC", sql.getOrderClause());
  }

  @Test
  public void testSql03() throws Exception {
    IRowMeta rowMeta = generateTest3RowMeta();

    String sqlString =
        "SELECT A as \"FROM\", B as \"TO\", C, COUNT(*)\nFROM Service\nWHERE B > 5\nGROUP BY A,B,C\nHAVING COUNT(*) > "
            + "100\nORDER BY A,B,C";

    Sql sql = new Sql(sqlString);

    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertEquals("A as \"FROM\", B as \"TO\", C, COUNT(*)", sql.getSelectClause());
    assertEquals("B > 5", sql.getWhereClause());
    assertEquals("A,B,C", sql.getGroupClause());
    assertEquals("COUNT(*) > 100", sql.getHavingClause());
    assertEquals("A,B,C", sql.getOrderClause());
  }

  @Test
  public void testSql04() throws Exception {
    IRowMeta rowMeta = generateTest3RowMeta();

    String sqlString = "SELECT *\nFROM Service\nWHERE B > 5\nORDER BY B DESC";

    Sql sql = new Sql(sqlString);

    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertEquals("*", sql.getSelectClause());
    List<SqlField> selectFields = sql.getSelectFields().getFields();
    assertEquals(3, selectFields.size());

    assertEquals("B > 5", sql.getWhereClause());
    SqlCondition whereCondition = sql.getWhereCondition();
    assertNotNull(whereCondition.getCondition());

    assertNull(sql.getGroupClause());
    assertNull(sql.getHavingClause());
    assertEquals("B DESC", sql.getOrderClause());
    List<SqlField> orderFields = sql.getOrderFields().getFields();
    assertEquals(1, orderFields.size());
    SqlField orderField = orderFields.get(0);
    assertTrue(orderField.isOrderField());
    assertFalse(orderField.isAscending());
    assertNull(orderField.getAlias());
    assertEquals("B", orderField.getValueMeta().getName().toUpperCase());
  }

  @Test
  public void testSql05() throws Exception {
    IRowMeta rowMeta = generateTest3RowMeta();

    String sqlString = "SELECT count(*) as NrOfRows FROM Service";

    Sql sql = new Sql(sqlString);

    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertEquals("count(*) as NrOfRows", sql.getSelectClause());
    List<SqlField> selectFields = sql.getSelectFields().getFields();
    assertEquals(1, selectFields.size());
    SqlField countField = selectFields.get(0);
    assertTrue(countField.isCountStar());
    assertEquals("*", countField.getField());
    assertEquals("NrOfRows", countField.getAlias());

    assertNull(sql.getGroupClause());
    assertNotNull(sql.getGroupFields());
    assertNull(sql.getHavingClause());
    assertNull(sql.getOrderClause());
  }

  /**
   * Query generated by interactive reporting.
   *
   * @throws Exception
   */
  @Test
  public void testSql06() throws Exception {
    IRowMeta rowMeta = generateServiceRowMeta();

    String sqlString =
        "SELECT DISTINCT\n          BT_SERVICE_SERVICE.Category AS COL0\n         ,BT_SERVICE_SERVICE.Country AS COL1\n"
            + "         ,BT_SERVICE_SERVICE.products_sold AS COL2\n         ,BT_SERVICE_SERVICE.sales_amount AS COL3\n"
            + "FROM \n          Service BT_SERVICE_SERVICE\n"
            + "ORDER BY\n          COL0";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertTrue(sql.getSelectFields().isDistinct());
    List<SqlField> selectFields = sql.getSelectFields().getFields();
    assertEquals(4, selectFields.size());
    assertEquals("COL0", selectFields.get(0).getAlias());
    assertEquals("COL1", selectFields.get(1).getAlias());
    assertEquals("COL2", selectFields.get(2).getAlias());
    assertEquals("COL3", selectFields.get(3).getAlias());

    List<SqlField> orderFields = sql.getOrderFields().getFields();
    assertEquals(1, orderFields.size());
  }

  /**
   * Query generated by Mondrian / Analyzer.
   *
   * @throws Exception
   */
  @Test
  public void testSql07() throws Exception {
    IRowMeta rowMeta = generateServiceRowMeta();

    String sqlString =
        "select \"Service\".\"Category\" as \"c0\" from \"Service\" as \"Service\" group by \"Service\".\"Category\" "
            + "order by CASE WHEN \"Service\".\"Category\" IS NULL THEN 1 ELSE 0 END, \"Service\".\"Category\" ASC";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertFalse(sql.getSelectFields().isDistinct());
    List<SqlField> selectFields = sql.getSelectFields().getFields();
    assertEquals(1, selectFields.size());
    assertEquals("c0", selectFields.get(0).getAlias());

    List<SqlField> orderFields = sql.getOrderFields().getFields();
    assertEquals(2, orderFields.size());
  }

  //

  /**
   * Query generated by PIR
   *
   * @throws Exception
   */
  @Test
  public void testSql08() throws Exception {
    IRowMeta rowMeta = generateZipsRowMeta();

    String sqlString =
        "SELECT            BT_MONGODB_MONGODB.state AS COL0          ,SUM(BT_MONGODB_MONGODB.rows) AS COL1 FROM        "
            + "    MongoDB BT_MONGODB_MONGODB GROUP BY            BT_MONGODB_MONGODB.state ORDER BY            COL1 DESC";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("MongoDB", sql.getServiceName());
    sql.parse(rowMeta);

    assertFalse(sql.getSelectFields().isDistinct());
    List<SqlField> selectFields = sql.getSelectFields().getFields();
    assertEquals(2, selectFields.size());
    assertEquals("state", selectFields.get(0).getField());
    assertEquals("COL0", selectFields.get(0).getAlias());
    assertEquals("rows", selectFields.get(1).getField());
    assertEquals("COL1", selectFields.get(1).getAlias());

    List<SqlField> orderFields = sql.getOrderFields().getFields();
    assertEquals(1, orderFields.size());
  }

  /**
   * Tests schema.table format
   *
   * @throws Exception
   */
  @Test
  public void testSql09() throws Exception {
    IRowMeta rowMeta = generateServiceRowMeta();

    String sqlString = "SELECT Category, Country, products_sold, sales_amount FROM Kettle.Service";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("Kettle", sql.getNamespace());
    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertFalse(sql.getSelectFields().isDistinct());
    List<SqlField> selectFields = sql.getSelectFields().getFields();
    assertEquals(4, selectFields.size());
    assertEquals("Category", selectFields.get(0).getField());
    assertEquals("Country", selectFields.get(1).getField());
    assertEquals("products_sold", selectFields.get(2).getField());
    assertEquals("sales_amount", selectFields.get(3).getField());
  }

  /**
   * Tests schema."table" format
   *
   * @throws Exception
   */
  @Test
  public void testSql10() throws Exception {
    IRowMeta rowMeta = generateServiceRowMeta();

    String sqlString =
        "SELECT Category, Country, products_sold, sales_amount FROM Kettle.\"Service\"";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("Kettle", sql.getNamespace());
    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertFalse(sql.getSelectFields().isDistinct());
    List<SqlField> selectFields = sql.getSelectFields().getFields();
    assertEquals(4, selectFields.size());
    assertEquals("Category", selectFields.get(0).getField());
    assertEquals("Country", selectFields.get(1).getField());
    assertEquals("products_sold", selectFields.get(2).getField());
    assertEquals("sales_amount", selectFields.get(3).getField());
  }

  /**
   * Tests "schema".table format
   *
   * @throws Exception
   */
  @Test
  public void testSql11() throws Exception {
    IRowMeta rowMeta = generateServiceRowMeta();

    String sqlString =
        "SELECT Category, Country, products_sold, sales_amount FROM \"Kettle\".Service";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("Kettle", sql.getNamespace());
    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertFalse(sql.getSelectFields().isDistinct());
    List<SqlField> selectFields = sql.getSelectFields().getFields();
    assertEquals(4, selectFields.size());
    assertEquals("Category", selectFields.get(0).getField());
    assertEquals("Country", selectFields.get(1).getField());
    assertEquals("products_sold", selectFields.get(2).getField());
    assertEquals("sales_amount", selectFields.get(3).getField());
  }

  /**
   * Tests "schema"."table" format
   *
   * @throws Exception
   */
  @Test
  public void testSql12() throws Exception {
    IRowMeta rowMeta = generateServiceRowMeta();

    String sqlString =
        "SELECT Category, Country, products_sold, sales_amount FROM \"Kettle\".\"Service\"";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("Kettle", sql.getNamespace());
    assertEquals("Service", sql.getServiceName());
    sql.parse(rowMeta);

    assertFalse(sql.getSelectFields().isDistinct());
    List<SqlField> selectFields = sql.getSelectFields().getFields();
    assertEquals(4, selectFields.size());
    assertEquals("Category", selectFields.get(0).getField());
    assertEquals("Country", selectFields.get(1).getField());
    assertEquals("products_sold", selectFields.get(2).getField());
    assertEquals("sales_amount", selectFields.get(3).getField());
  }

  /**
   * Tests quoting in literal strings
   *
   * @throws Exception
   */
  @Test
  public void testSql13() throws Exception {
    IRowMeta rowMeta = generateGettingStartedRowMeta();

    String sqlString =
        "SELECT * FROM \"GETTING_STARTED\" WHERE \"GETTING_STARTED\".\"CUSTOMERNAME\" = 'ANNA''S DECORATIONS, LTD'";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("GETTING_STARTED", sql.getServiceName());
    sql.parse(rowMeta);

    assertNotNull(sql.getWhereCondition());
    assertEquals("CUSTOMERNAME", sql.getWhereCondition().getCondition().getLeftValuename());
    assertEquals(
        "ANNA'S DECORATIONS, LTD", sql.getWhereCondition().getCondition().getRightExactString());
  }

  /**
   * Tests empty literal strings
   *
   * @throws Exception
   */
  @Test
  public void testSql14() throws Exception {
    IRowMeta rowMeta = generateGettingStartedRowMeta();

    String sqlString =
        "SELECT * FROM \"GETTING_STARTED\" WHERE \"GETTING_STARTED\".\"CUSTOMERNAME\" = ''";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("GETTING_STARTED", sql.getServiceName());
    sql.parse(rowMeta);

    assertNotNull(sql.getWhereCondition());
    assertEquals("CUSTOMERNAME", sql.getWhereCondition().getCondition().getLeftValuename());
    assertEquals("", sql.getWhereCondition().getCondition().getRightExactString());
  }

  /**
   * Tests crazy quoting in literal strings
   *
   * @throws Exception
   */
  @Test
  public void testSql15() throws Exception {
    IRowMeta rowMeta = generateGettingStartedRowMeta();

    String sqlString =
        "SELECT * FROM \"GETTING_STARTED\" WHERE \"GETTING_STARTED\".\"CUSTOMERNAME\" = ''''''''''''";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("GETTING_STARTED", sql.getServiceName());
    sql.parse(rowMeta);

    assertNotNull(sql.getWhereCondition());
    assertEquals("CUSTOMERNAME", sql.getWhereCondition().getCondition().getLeftValuename());
    assertEquals("'''''", sql.getWhereCondition().getCondition().getRightExactString());
  }

  /**
   * Tests quoting in literal strings in IN clause
   *
   * @throws Exception
   */
  @Test
  public void testSql16() throws Exception {
    IRowMeta rowMeta = generateGettingStartedRowMeta();

    String sqlString =
        "SELECT * FROM \"GETTING_STARTED\" WHERE \"GETTING_STARTED\".\"CUSTOMERNAME\" IN ('ANNA''S DECORATIONS, LTD', "
            + "'MEN ''R'' US RETAILERS, Ltd.' )";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("GETTING_STARTED", sql.getServiceName());
    sql.parse(rowMeta);

    assertNotNull(sql.getWhereCondition());
    assertEquals("CUSTOMERNAME", sql.getWhereCondition().getCondition().getLeftValuename());
    assertEquals(
        "ANNA'S DECORATIONS, LTD;MEN 'R' US RETAILERS, Ltd.",
        sql.getWhereCondition().getCondition().getRightExactString());
  }

  /**
   * Tests semi-coluns and quoting in literal strings in IN clause
   *
   * @throws Exception
   */
  @Test
  public void testSql17() throws Exception {
    IRowMeta rowMeta = generateGettingStartedRowMeta();

    String sqlString =
        "SELECT * FROM \"GETTING_STARTED\" WHERE \"GETTING_STARTED\".\"CUSTOMERNAME\" IN ('ANNA''S DECORATIONS; LTD', "
            + "'MEN ''R'' US RETAILERS; Ltd.' )";

    Sql sql = new Sql(ThinUtil.stripNewlines(sqlString));

    assertEquals("GETTING_STARTED", sql.getServiceName());
    sql.parse(rowMeta);

    assertNotNull(sql.getWhereCondition());
    assertEquals("CUSTOMERNAME", sql.getWhereCondition().getCondition().getLeftValuename());
    assertEquals(
        "ANNA'S DECORATIONS\\; LTD;MEN 'R' US RETAILERS\\; Ltd.",
        sql.getWhereCondition().getCondition().getRightExactString());
  }

  @Test
  public void testSql18() throws Exception {

    String sqlString =
        "SELECT A, B, C\nFROM Service\nWHERE B > 5\nORDER BY B DESC\nLIMIT 5    OFFSET    10";
    Sql sql = new Sql(sqlString);
    IRowMeta rowMeta = generateTest4RowMeta();
    sql.parse(rowMeta);

    assertEquals(5, sql.getLimitValues().getLimit());
    assertEquals(10, sql.getLimitValues().getOffset());

    sqlString = "SELECT A, B, C\nFROM Service\nWHERE B > 5\nORDER BY B DESC\nLIMIT 10, 5";
    sql = new Sql(sqlString);
    rowMeta = generateTest4RowMeta();
    sql.parse(rowMeta);

    assertEquals(5, sql.getLimitValues().getLimit());
    assertEquals(10, sql.getLimitValues().getOffset());

    sqlString = "SELECT A, B, C\nFROM Service\nWHERE B > 5\nORDER BY B DESC\nLIMIT 5";
    sql = new Sql(sqlString);
    rowMeta = generateTest4RowMeta();
    sql.parse(rowMeta);

    assertEquals(5, sql.getLimitValues().getLimit());

    sqlString = "SELECT A, B, C\nFROM Service\nWHERE B > 5\nORDER BY B DESC\nLIMIT ERROR5";
    sql = new Sql(sqlString);
    rowMeta = generateTest4RowMeta();

    try {
      sql.parse(rowMeta);
      fail();
    } catch (Exception e) {
      // Should throw a Exception
    }
  }

  @Test
  public void testZeroEqualsOneTreatedAsFalse() throws Exception {

    String sqlString = "SELECT A, B, C\nFROM Service\nWHERE 1 = 0";
    Sql sql = new Sql(sqlString);
    IRowMeta rowMeta = generateTest4RowMeta();
    sql.parse(rowMeta);

    assertEquals(Condition.FUNC_TRUE, sql.getWhereCondition().getCondition().getFunction());
    assertTrue(sql.getWhereCondition().getCondition().isNegated());
  }

  @Test
  public void testAliasHandlingWithHavingEquals() throws Exception {

    String sqlString =
        "SELECT A, count(distinct B) as customerCount FROM service GROUP BY A HAVING count(distinct B) = 10";
    Sql sql = new Sql(sqlString);
    IRowMeta rowMeta = generateTest4RowMeta();
    sql.parse(rowMeta);

    final Condition condition = sql.getHavingCondition().getCondition();
    assertEquals("customerCount", condition.getLeftValuename());
    assertEquals(Condition.FUNC_EQUAL, condition.getFunction());
  }

  @Test
  public void testAliasHandlingWithHavingEqualsWhereClauseNotInSelect() throws Exception {
    // "count(D)" is not in the select.
    String sqlString =
        "SELECT A, count(B) as customerCount FROM service GROUP BY A HAVING count(D) = 10";
    Sql sql = new Sql(sqlString);
    IRowMeta rowMeta = generateTest4RowMeta();
    sql.parse(rowMeta);

    final Condition condition = sql.getHavingCondition().getCondition();
    assertFalse(condition.getFunction() == Condition.FUNC_TRUE);
    assertEquals("count(D)", condition.getLeftValuename());
    assertEquals(Condition.FUNC_EQUAL, condition.getFunction());
  }

  @Test
  public void testLiteralChecksWithMondrianSQL() throws Exception {
    // http://jira.pentaho.com/browse/BACKLOG-18470
    String query =
        "SELECT \"table\".\"field\" AS \"c0\" FROM \"Kettle\".\"table\" AS \"table\" GROUP BY \"table\""
            + ".\"field\" HAVING (SUM(\"table\".\"anotherField\") = 1.0) ORDER BY CASE WHEN \"table\".\"field\" IS NULL "
            + "THEN 1 ELSE 0 END, \"table\".\"field\" ASC";
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("field"));
    rowMeta.addValueMeta(new ValueMetaNumber("anotherField"));
    Sql sql = new Sql(query);
    sql.parse(rowMeta);
  }

  @Test
  public void testAliasHandlingWithHavingGt() throws Exception {

    String sqlString =
        "SELECT A, count(distinct B) as customerCount FROM service GROUP BY A HAVING count(distinct B) > 10";
    Sql sql = new Sql(sqlString);
    IRowMeta rowMeta = generateTest4RowMeta();
    sql.parse(rowMeta);

    final Condition condition = sql.getHavingCondition().getCondition();
    assertEquals("customerCount", condition.getLeftValuename());
    assertEquals(Condition.FUNC_LARGER, condition.getFunction());
  }

  @Test
  public void testSelectFromHaving() throws Exception {

    String sqlString = "SELECT SUM(A) FROM service HAVING SUM(A) IS NULL";
    Sql sql = new Sql(sqlString);
    IRowMeta rowMeta = generateTest4RowMeta();
    sql.parse(rowMeta);

    final Condition condition = sql.getHavingCondition().getCondition();
    assertEquals("SUM(A)", condition.getLeftValuename());
    assertEquals(Condition.FUNC_NULL, condition.getFunction());
  }

  public static IRowMeta generateTest2RowMeta() {
    IRowMeta rowMeta = new RowMetaBuilder().addString("A", 50).addInteger("B", 7).build();
    return rowMeta;
  }

  public static IRowMeta generateTest2RowMetaWithParans() {
    IRowMeta rowMeta = new RowMetaBuilder().addString("A", 50).addInteger("B (g)", 7).build();
    return rowMeta;
  }

  public static IRowMeta generateTest3RowMeta() {
    IRowMeta rowMeta =
        new RowMetaBuilder().addString("A", 50).addInteger("B", 7).addInteger("C", 7).build();
    return rowMeta;
  }

  public static IRowMeta generateTest4RowMeta() {
    IRowMeta rowMeta =
        new RowMetaBuilder()
            .addString("A", 50)
            .addInteger("B", 7)
            .addString("C", 50)
            .addInteger("D", 7)
            .build();
    return rowMeta;
  }

  public static IRowMeta generateServiceRowMeta() {
    IRowMeta rowMeta =
        new RowMetaBuilder()
            .addString("Category", 50)
            .addInteger("Country", 7)
            .addInteger("products_sold", 8)
            .addNumber("sales_amount", 7, 2)
            .build();
    return rowMeta;
  }

  public static IRowMeta generateZipsRowMeta() {
    IRowMeta rowMeta =
        new RowMetaBuilder()
            .addString("zip, 5")
            .addInteger("city", 50)
            .addString("state", 2)
            .addInteger("rows", 1)
            .build();
    return rowMeta;
  }

  public static IRowMeta generateGettingStartedRowMeta() {
    IRowMeta rowMeta =
        new RowMetaBuilder()
            .addString("CUSTOMERNAME", 50)
            .addInteger("MONTH_ID", 4)
            .addInteger("YEAR_ID", 2)
            .addString("STATE", 30)
            .addDate("ORDERDATE")
            .build();
    return rowMeta;
  }

  public static IRowMeta generateNumberRowMeta() {
    IRowMeta rowMeta = new RowMetaBuilder().addNumber("A", 0, -1).addBigNumber("B", 0, -1).build();
    return rowMeta;
  }

  public static IRowMeta mockRowMeta(String... fieldNames) {
    IRowMeta rowMeta = mock(IRowMeta.class);
    for (String field : fieldNames) {
      IValueMeta valueMeta = mock(IValueMeta.class);
      when(valueMeta.getName()).thenReturn(field);
      when(rowMeta.searchValueMeta(field)).thenReturn(valueMeta);
    }
    return rowMeta;
  }
}
