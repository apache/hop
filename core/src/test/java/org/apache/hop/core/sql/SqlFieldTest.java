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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hop.core.sql.SqlTest.mockRowMeta;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

public class SqlFieldTest {

  @Before
  public void before() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  public void testSqlField01() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "A as foo";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField01Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "Service.A as foo";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField01QuotedAlias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "\"Service\".A as foo";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField02() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "A as \"foo\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField02Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "Service.A as \"foo\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField03() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "\"A\" as \"foo\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField03Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "Service.\"A\" as \"foo\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField04() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "\"A\" \"foo\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField04Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "Service.\"A\" \"foo\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField05() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "A   as   foo";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField05Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "Service.A   as   foo";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField06() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "SUM(B) as total";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("B", field.getName());
    assertEquals("total", field.getAlias());
    assertEquals(SqlAggregation.SUM, field.getAggregation());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField06Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "SUM(Service.B) as total";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("B", field.getName());
    assertEquals("total", field.getAlias());
    assertEquals(SqlAggregation.SUM, field.getAggregation());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField07() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "SUM( B ) as total";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("B", field.getName());
    assertEquals("total", field.getAlias());
    assertEquals(SqlAggregation.SUM, field.getAggregation());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField07Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "SUM( Service.B ) as total";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("B", field.getName());
    assertEquals("total", field.getAlias());
    assertEquals(SqlAggregation.SUM, field.getAggregation());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField08() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "SUM( \"B\" ) as total";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("B", field.getName());
    assertEquals("total", field.getAlias());
    assertEquals(SqlAggregation.SUM, field.getAggregation());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField08Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "SUM( Service.\"B\" ) as total";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("B", field.getName());
    assertEquals("total", field.getAlias());
    assertEquals(SqlAggregation.SUM, field.getAggregation());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField09() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "SUM(\"B\") as   \"total\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("B", field.getName());
    assertEquals("total", field.getAlias());
    assertEquals(SqlAggregation.SUM, field.getAggregation());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField09Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "SUM(Service.\"B\") as   \"total\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("B", field.getName());
    assertEquals("total", field.getAlias());
    assertEquals(SqlAggregation.SUM, field.getAggregation());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
  }

  @Test
  public void testSqlField10() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "COUNT(*) as   \"Number of lines\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("*", field.getName());
    assertEquals("Number of lines", field.getAlias());
    assertEquals(SqlAggregation.COUNT, field.getAggregation());
    assertNull(field.getValueMeta());
    assertTrue(field.isCountStar());
  }

  @Test
  public void testSqlField10NoAlias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "COUNT(*)";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("*", field.getName());
    assertEquals("COUNT(*)", field.getAlias());
    assertEquals(SqlAggregation.COUNT, field.getAggregation());
    assertNull(field.getValueMeta());
    assertTrue(field.isCountStar());
  }

  @Test
  public void testSqlField10Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "COUNT(Service.*) as   \"Number of lines\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("*", field.getName());
    assertEquals("Number of lines", field.getAlias());
    assertEquals(SqlAggregation.COUNT, field.getAggregation());
    assertNull(field.getValueMeta());
    assertTrue(field.isCountStar());
  }

  @Test
  public void testSqlField11() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "COUNT(DISTINCT A) as   \"Number of customers\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("Number of customers", field.getAlias());
    assertEquals(SqlAggregation.COUNT, field.getAggregation());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertTrue(field.isCountDistinct());
  }

  @Test
  public void testSqlField11Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "COUNT(DISTINCT Service.A) as   \"Number of customers\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("A", field.getName());
    assertEquals("Number of customers", field.getAlias());
    assertEquals(SqlAggregation.COUNT, field.getAggregation());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertTrue(field.isCountDistinct());
  }

  @Test
  public void testSqlField12_Function() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "IIF( B>5000, 'Big', 'Small' ) as \"Sales size\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("IIF( B>5000, 'Big', 'Small' )", field.getName());
    assertEquals("Sales size", field.getAlias());
    assertNull("The service data type was discovered", field.getValueMeta());

    assertNotNull(field.getIif());
    Condition condition = field.getIif().getSqlCondition().getCondition();
    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals(">", condition.getFunctionDesc());
    assertEquals("5000", condition.getRightExactString());
  }

  @Test
  public void testSqlField12Alias_Function() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "IIF( Service.B>5000, 'Big', 'Small' ) as \"Sales size\"";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("IIF( Service.B>5000, 'Big', 'Small' )", field.getName());
    assertEquals("Sales size", field.getAlias());
    assertNull("The service data type was discovered", field.getValueMeta());

    assertNotNull(field.getIif());
    Condition condition = field.getIif().getSqlCondition().getCondition();
    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals(">", condition.getFunctionDesc());
    assertEquals("5000", condition.getRightExactString());
  }

  @Test
  public void testSqlField13_Function() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "IIF( B>50, 'high', 'low' ) as nrSize";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("IIF( B>50, 'high', 'low' )", field.getName());
    assertEquals("nrSize", field.getAlias());
    assertNull("The service data type was discovered", field.getValueMeta());

    assertNotNull(field.getIif());
    Condition condition = field.getIif().getSqlCondition().getCondition();
    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals(">", condition.getFunctionDesc());
    assertEquals("50", condition.getRightExactString());
  }

  @Test
  public void testSqlField13Alias_Function() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "IIF( Service.B>50, 'high', 'low' ) as nrSize";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("IIF( Service.B>50, 'high', 'low' )", field.getName());
    assertEquals("nrSize", field.getAlias());
    assertNull("The service data type was discovered", field.getValueMeta());

    assertNotNull(field.getIif());
    Condition condition = field.getIif().getSqlCondition().getCondition();
    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals(">", condition.getFunctionDesc());
    assertEquals("50", condition.getRightExactString());
  }

  @Test
  public void testAliasWithParans() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMetaWithParans();

    String fieldClause = "sum(\"Service\".\"B (g)\") as nrSize";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("B (g)", field.getName());
    assertEquals("nrSize", field.getAlias());
    assertEquals(SqlAggregation.SUM, field.getAggregation());
  }

  @Test
  public void testSqlFieldConstants01() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "1";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("1", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals(field.getValueMeta().getType(), IValueMeta.TYPE_INTEGER);
    assertEquals(1L, field.getValueData());
  }

  @Test
  public void testSqlFieldConstants02() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldClause = "COUNT(1)";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("1", field.getName());
    assertEquals("COUNT(1)", field.getAlias());
    assertEquals(SqlAggregation.COUNT, field.getAggregation());
    assertEquals(1L, field.getValueData());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals(field.getValueMeta().getType(), IValueMeta.TYPE_INTEGER);
    assertEquals(1L, field.getValueData());
  }

  /**
   * Mondrian generated CASE WHEN <condition> THEN true-value ELSE false-value END
   *
   * @throws Exception
   */
  @Test
  public void testSqlFieldCaseWhen01() throws Exception {
    IRowMeta rowMeta = SqlTest.generateServiceRowMeta();

    String fieldClause = "CASE WHEN \"Service\".\"Category\" IS NULL THEN 1 ELSE 0 END";

    SqlField field = new SqlField("Service", fieldClause, rowMeta);
    assertEquals("CASE WHEN \"Service\".\"Category\" IS NULL THEN 1 ELSE 0 END", field.getName());
    assertNull(field.getAlias());
    assertNotNull(field.getIif());
    assertEquals("\"Service\".\"Category\" IS NULL", field.getIif().getConditionClause());
    assertEquals(1L, field.getIif().getTrueValue().getValueData());
    assertEquals(0L, field.getIif().getFalseValue().getValueData());
  }

  @Test
  public void testAggFieldWithTableQualifier() throws Exception {
    IRowMeta rowMeta = mockRowMeta("noSpaceField");
    SqlField field = new SqlField("noSpaceTableAlias", "sum(noSpaceField)", rowMeta);
    assertThat(field.getField(), is("noSpaceField"));
    assertThat(field.getAggregation().getKeyWord(), is("SUM"));
    verify(rowMeta, atLeastOnce()).searchValueMeta("noSpaceField");
  }

  @Test
  public void testAggFieldWithSpaceAndWithTableQualifier() throws Exception {
    IRowMeta rowMeta = mockRowMeta("Space Field", "otherField");
    SqlField field =
        new SqlField("noSpaceTableAlias", "sum(\"noSpaceTableAlias\".\"Space Field\")", rowMeta);
    assertThat(field.getField(), is("Space Field"));
    assertThat(field.getAggregation().getKeyWord(), is("SUM"));
    verify(rowMeta, atLeastOnce()).searchValueMeta("Space Field");
  }

  @Test
  public void testAggFieldWithSpaceAndWithTableQualifierAndAlias() throws Exception {
    IRowMeta rowMeta = mockRowMeta("Space Field");
    SqlField field =
        new SqlField(
            "noSpaceTableAlias", "max( \"noSpaceTableAlias\".\"Space Field\")  as \"c0\"", rowMeta);
    assertThat(field.getField(), is("Space Field"));
    assertThat(field.getAggregation().getKeyWord(), is("MAX"));
    verify(rowMeta, atLeastOnce()).searchValueMeta("Space Field");
  }

  @Test
  public void testAggFieldDistinctWithNoSpace() throws Exception {
    IRowMeta rowMeta = mockRowMeta("NoSpaceField");
    SqlField field = new SqlField("noSpaceTableAlias", "count( DISTINCT NoSpaceField) ", rowMeta);
    assertThat(field.getField(), is("NoSpaceField"));
    assertThat(field.getAggregation().getKeyWord(), is("COUNT"));
    assertThat(field.isCountDistinct(), is(true));
    verify(rowMeta, atLeastOnce()).searchValueMeta("NoSpaceField");
  }

  @Test
  public void testAggFieldDistinctWithSpace() throws Exception {
    IRowMeta rowMeta = mockRowMeta("Space Field");
    SqlField field =
        new SqlField("noSpaceTableAlias", "count( DISTINCT \"Space Field\") ", rowMeta);
    assertThat(field.getField(), is("Space Field"));
    assertThat(field.getAggregation().getKeyWord(), is("COUNT"));
    assertThat(field.isCountDistinct(), is(true));
    verify(rowMeta, atLeastOnce()).searchValueMeta("Space Field");
  }

  @Test
  public void testAggFieldDistinctWithSpaceAndWithTableQualifierAndAlias() throws Exception {
    IRowMeta rowMeta = mockRowMeta("Space Field");
    SqlField field =
        new SqlField(
            "Space TableAlias",
            "count( DISTINCT \"Space TableAlias\".\"Space Field\")  as \"c0\"",
            rowMeta);
    assertThat(field.getField(), is("Space Field"));
    assertThat(field.getAggregation().getKeyWord(), is("COUNT"));
    assertThat(field.isCountDistinct(), is(true));
    verify(rowMeta, atLeastOnce()).searchValueMeta("Space Field");
  }
}
