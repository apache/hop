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
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.apache.hop.core.sql.SqlTest.mockRowMeta;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlConditionTest {

  @Before
  public void before() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  public void testCondition01() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "A = 'FOO'";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("A", condition.getLeftValuename());
    assertEquals("=", condition.getFunctionDesc());
    assertEquals("FOO", condition.getRightExactString());
  }

  @Test
  public void testCondition02() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "B > 123";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals(">", condition.getFunctionDesc());
    assertEquals("123", condition.getRightExactString());
  }

  @Test
  public void testCondition03() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "B < 123";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals("<", condition.getFunctionDesc());
    assertEquals("123", condition.getRightExactString());
  }

  @Test
  public void testCondition04() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "B >= 123";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals(">=", condition.getFunctionDesc());
    assertEquals("123", condition.getRightExactString());
  }

  @Test
  public void testCondition05() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "B => 123";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals(">=", condition.getFunctionDesc());
    assertEquals("123", condition.getRightExactString());
  }

  @Test
  public void testCondition06() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "B <= 123";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals("<=", condition.getFunctionDesc());
    assertEquals("123", condition.getRightExactString());
  }

  @Test
  public void testCondition07() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "B >= 123";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals(">=", condition.getFunctionDesc());
    assertEquals("123", condition.getRightExactString());
  }

  @Test
  public void testCondition08() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "B => 123";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals(">=", condition.getFunctionDesc());
    assertEquals("123", condition.getRightExactString());
  }

  @Test
  public void testCondition09() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "B <> 123";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals("<>", condition.getFunctionDesc());
    assertEquals("123", condition.getRightExactString());
  }

  @Test
  public void testCondition10() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "B IN (1, 2, 3, 4)";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("B", condition.getLeftValuename());
    assertEquals("IN LIST", condition.getFunctionDesc());
    assertEquals("1;2;3;4", condition.getRightExactString());
  }

  @Test
  public void testCondition11() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "A IN ( 'foo' , 'bar' )";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("A", condition.getLeftValuename());
    assertEquals("IN LIST", condition.getFunctionDesc());
    assertEquals("foo;bar", condition.getRightExactString());
  }

  @Test
  public void testCondition12() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "A REGEX 'foo.*bar'";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("A", condition.getLeftValuename());
    assertEquals("REGEXP", condition.getFunctionDesc());
    assertEquals("foo.*bar", condition.getRightExactString());
  }

  @Test
  public void testCondition13() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "A LIKE 'foo%'";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("A", condition.getLeftValuename());
    assertEquals("LIKE", condition.getFunctionDesc());
    assertEquals("foo%", condition.getRightExactString());
  }

  @Test
  public void testCondition14() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "A LIKE 'foo??bar'";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals("A", condition.getLeftValuename());
    assertEquals("LIKE", condition.getFunctionDesc());
    assertEquals("foo??bar", condition.getRightExactString());
  }

  // Now the more complex AND/OR/NOT situations...
  //
  @Test
  public void testCondition15() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "A='Foo' AND B>5";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertFalse("Non-atomic condition expected", condition.isAtomic());
    assertEquals(2, condition.nrConditions());

    Condition one = condition.getCondition(0);
    assertEquals("A", one.getLeftValuename());
    assertEquals("=", one.getFunctionDesc());
    assertEquals("Foo", one.getRightExactString());

    Condition two = condition.getCondition(1);
    assertEquals("B", two.getLeftValuename());
    assertEquals(">", two.getFunctionDesc());
    assertEquals("5", two.getRightExactString());

    assertEquals(Condition.OPERATOR_AND, two.getOperator());
  }

  @Test
  public void testCondition16() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "( A='Foo' ) AND ( B>5 )";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertFalse("Non-atomic condition expected", condition.isAtomic());
    assertEquals(2, condition.nrConditions());

    Condition one = condition.getCondition(0);
    assertEquals("A", one.getLeftValuename());
    assertEquals("=", one.getFunctionDesc());
    assertEquals("Foo", one.getRightExactString());

    Condition two = condition.getCondition(1);
    assertEquals("B", two.getLeftValuename());
    assertEquals(">", two.getFunctionDesc());
    assertEquals("5", two.getRightExactString());

    assertEquals(Condition.OPERATOR_AND, two.getOperator());
  }

  @Test
  public void testCondition17() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B, C, D";
    String conditionClause = "A='Foo' AND B>5 AND C='foo' AND D=123";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertFalse("Non-atomic condition expected", condition.isAtomic());
    assertEquals(4, condition.nrConditions());

    Condition one = condition.getCondition(0);
    assertEquals("A", one.getLeftValuename());
    assertEquals("=", one.getFunctionDesc());
    assertEquals("Foo", one.getRightExactString());

    Condition two = condition.getCondition(1);
    assertEquals("B", two.getLeftValuename());
    assertEquals(">", two.getFunctionDesc());
    assertEquals("5", two.getRightExactString());
    assertEquals(Condition.OPERATOR_AND, two.getOperator());

    Condition three = condition.getCondition(2);
    assertEquals("C", three.getLeftValuename());
    assertEquals("=", three.getFunctionDesc());
    assertEquals("foo", three.getRightExactString());
    assertEquals(Condition.OPERATOR_AND, three.getOperator());

    Condition four = condition.getCondition(3);
    assertEquals("D", four.getLeftValuename());
    assertEquals("=", four.getFunctionDesc());
    assertEquals("123", four.getRightExactString());
    assertEquals(Condition.OPERATOR_AND, four.getOperator());
  }

  /**
   * Test precedence.
   *
   * @throws Exception
   */
  @Test
  public void testCondition18() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B, C, D";
    String conditionClause = "A='Foo' OR B>5 AND C='foo' OR D=123";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertFalse("Non-atomic condition expected", condition.isAtomic());
    assertEquals(3, condition.nrConditions());

    Condition leftOr = condition.getCondition(0);

    assertTrue(leftOr.isAtomic());
    assertEquals("A", leftOr.getLeftValuename());
    assertEquals("=", leftOr.getFunctionDesc());
    assertEquals("Foo", leftOr.getRightExactString());

    Condition middleOr = condition.getCondition(1);
    assertEquals(2, middleOr.nrConditions());

    Condition leftAnd = middleOr.getCondition(0);
    assertTrue(leftAnd.isAtomic());
    assertEquals("B", leftAnd.getLeftValuename());
    assertEquals(">", leftAnd.getFunctionDesc());
    assertEquals("5", leftAnd.getRightExactString());
    assertEquals(Condition.OPERATOR_NONE, leftAnd.getOperator());

    Condition rightAnd = middleOr.getCondition(1);
    assertEquals(Condition.OPERATOR_AND, rightAnd.getOperator());
    assertEquals("C", rightAnd.getLeftValuename());
    assertEquals("=", rightAnd.getFunctionDesc());
    assertEquals("foo", rightAnd.getRightExactString());

    Condition rightOr = condition.getCondition(2);
    assertTrue(rightOr.isAtomic());
    assertEquals("D", rightOr.getLeftValuename());
    assertEquals("=", rightOr.getFunctionDesc());
    assertEquals("123", rightOr.getRightExactString());
    assertEquals(Condition.OPERATOR_OR, rightOr.getOperator());
  }

  @Test
  public void testCondition19() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "A LIKE '%AL%' AND ( B LIKE '15%' OR C IS NULL )";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertFalse("Non-atomic condition expected", condition.isAtomic());
    assertEquals(2, condition.nrConditions());

    Condition leftAnd = condition.getCondition(0);

    assertTrue(leftAnd.isAtomic());
    assertEquals("A", leftAnd.getLeftValuename());
    assertEquals("LIKE", leftAnd.getFunctionDesc());
    assertEquals("%AL%", leftAnd.getRightExactString());

    Condition rightBracket = condition.getCondition(1);
    assertEquals(1, rightBracket.nrConditions());
    Condition rightAnd = rightBracket.getCondition(0);
    assertEquals(2, rightAnd.nrConditions());

    Condition leftOr = rightAnd.getCondition(0);
    assertTrue(leftOr.isAtomic());
    assertEquals("B", leftOr.getLeftValuename());
    assertEquals("LIKE", leftOr.getFunctionDesc());
    assertEquals("15%", leftOr.getRightExactString());
    assertEquals(Condition.OPERATOR_NONE, leftOr.getOperator());

    Condition rightOr = rightAnd.getCondition(1);
    assertEquals(Condition.OPERATOR_OR, rightOr.getOperator());
    assertEquals("C", rightOr.getLeftValuename());
    assertEquals("IS NULL", rightOr.getFunctionDesc());
    assertNull(rightOr.getRightExactString());
  }

  @Test
  public void testCondition20() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "NOT ( A = 'FOO' )";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertTrue(condition.isNegated());
    assertEquals("A", condition.getLeftValuename());
    assertEquals("=", condition.getFunctionDesc());
    assertEquals("FOO", condition.getRightExactString());
  }

  @Test
  public void testCondition21() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "A='Foo' AND NOT ( B>5 )";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertFalse("Non-atomic condition expected", condition.isAtomic());
    assertEquals(2, condition.nrConditions());

    Condition one = condition.getCondition(0);
    assertEquals("A", one.getLeftValuename());
    assertEquals("=", one.getFunctionDesc());
    assertEquals("Foo", one.getRightExactString());

    Condition two = condition.getCondition(1);
    assertTrue(two.isNegated());
    assertEquals("B", two.getLeftValuename());
    assertEquals(">", two.getFunctionDesc());
    assertEquals("5", two.getRightExactString());

    assertEquals(Condition.OPERATOR_AND, two.getOperator());
  }

  @Test
  public void testCondition22() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B, C";
    String conditionClause = "A='Foo' AND NOT ( B>5 OR C='AAA' ) ";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertFalse("Non-atomic condition expected", condition.isAtomic());
    assertEquals(2, condition.nrConditions());

    Condition leftAnd = condition.getCondition(0);
    assertEquals("A", leftAnd.getLeftValuename());
    assertEquals("=", leftAnd.getFunctionDesc());
    assertEquals("Foo", leftAnd.getRightExactString());

    Condition rightAnd = condition.getCondition(1);
    assertEquals(1, rightAnd.nrConditions());

    Condition notBlock = rightAnd.getCondition(0);
    assertTrue(notBlock.isNegated());
    assertEquals(2, notBlock.nrConditions());

    Condition leftOr = notBlock.getCondition(0);
    assertTrue(leftOr.isAtomic());
    assertEquals("B", leftOr.getLeftValuename());
    assertEquals(">", leftOr.getFunctionDesc());
    assertEquals("5", leftOr.getRightExactString());
    assertEquals(Condition.OPERATOR_NONE, leftOr.getOperator());

    Condition rightOr = notBlock.getCondition(1);
    assertEquals(Condition.OPERATOR_OR, rightOr.getOperator());
    assertEquals("C", rightOr.getLeftValuename());
    assertEquals("=", rightOr.getFunctionDesc());
    assertEquals("AAA", rightOr.getRightExactString());
  }

  // Brackets, quotes...
  //
  @Test
  public void testCondition23() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "( A LIKE '(AND' ) AND ( B>5 )";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertFalse("Non-atomic condition expected", condition.isAtomic());
    assertEquals(2, condition.nrConditions());

    Condition one = condition.getCondition(0);
    assertEquals("A", one.getLeftValuename());
    assertEquals("LIKE", one.getFunctionDesc());
    assertEquals("(AND", one.getRightExactString());

    Condition two = condition.getCondition(1);
    assertEquals("B", two.getLeftValuename());
    assertEquals(">", two.getFunctionDesc());
    assertEquals("5", two.getRightExactString());

    assertEquals(Condition.OPERATOR_AND, two.getOperator());
  }

  // Brackets, quotes...
  //
  @Test
  public void testCondition24() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "( A LIKE '(AND' ) AND ( ( B>5 ) OR ( B=3 ) )";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertFalse("Non-atomic condition expected", condition.isAtomic());
    assertEquals(2, condition.nrConditions());

    Condition one = condition.getCondition(0);
    assertEquals("A", one.getLeftValuename());
    assertEquals("LIKE", one.getFunctionDesc());
    assertEquals("(AND", one.getRightExactString());

    Condition two = condition.getCondition(1);
    assertEquals(1, two.nrConditions());

    Condition brackets = condition.getCondition(1);
    assertEquals(1, brackets.nrConditions());

    Condition right = brackets.getCondition(0);
    assertEquals(2, right.nrConditions());

    Condition leftOr = right.getCondition(0);
    assertTrue(leftOr.isAtomic());
    assertEquals("B", leftOr.getLeftValuename());
    assertEquals(">", leftOr.getFunctionDesc());
    assertEquals("5", leftOr.getRightExactString());
    assertEquals(Condition.OPERATOR_NONE, leftOr.getOperator());

    Condition rightOr = right.getCondition(1);
    assertEquals(Condition.OPERATOR_OR, rightOr.getOperator());
    assertEquals("B", rightOr.getLeftValuename());
    assertEquals("=", rightOr.getFunctionDesc());
    assertEquals("3", rightOr.getRightExactString());
  }

  // Parameters...
  //

  @Test
  public void testCondition25() throws Exception {
    runParamTest("PARAMETER('param')='FOO'", "param", "FOO");
  }

  @Test
  public void testLowerCaseParamInConditionClause() throws Exception {
    runParamTest("parameter('param')='FOO'", "param", "FOO");
  }

  @Test
  public void testMixedCaseParamInConditionClause() throws Exception {
    runParamTest("Parameter('param')='FOO'", "param", "FOO");
  }

  @Test
  public void testSpaceInParamNameAndValueInConditionClause() throws Exception {
    runParamTest("Parameter('My Parameter')='Foo Bar Baz'", "My Parameter", "Foo Bar Baz");
  }

  @Test
  public void testUnquotedNumericParameterValueInConditionClause() throws Exception {
    runParamTest("Parameter('My Parameter') = 123", "My Parameter", "123");
  }

  @Test
  public void testExtraneousWhitespaceInParameterConditionClause() throws Exception {
    runParamTest(
        "Parameter   ( \t     'My Parameter'  \t   )  \t= \t    'My value'",
        "My Parameter",
        "My value");
  }

  @Test
  public void testParameterNameMissingThrows() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();
    SqlFields fields = new SqlFields("Service", rowMeta, "A, B");
    try {
      new SqlCondition("Service", "Parameter('') =  'My value'", rowMeta, fields);
      fail();
    } catch (Exception kse) {
      assertTrue(kse.getMessage().contains("A parameter name cannot be empty"));
    }
  }

  @Test
  public void testParameterValueMissingThrows() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();
    SqlFields fields = new SqlFields("Service", rowMeta, "A, B");
    try {
      new SqlCondition("Service", "Parameter('Foo') =  ''", rowMeta, fields);
      fail();
    } catch (Exception kse) {
      assertTrue(kse.getMessage().contains("A parameter value cannot be empty"));
    }
  }

  private void runParamTest(String conditionClause, String paramName, String paramValue)
      throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();
    SqlFields fields = new SqlFields("Service", rowMeta, "A, B");

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals(
        String.format(
            "Expected condition to be of type FUNC_TRUE, was (%s)",
            Condition.functions[condition.getFunction()]),
        Condition.FUNC_TRUE,
        condition.getFunction());

    assertEquals(paramName, condition.getLeftValuename());
    assertEquals(paramValue, condition.getRightExactString());
  }

  @Test
  public void testCondition26() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest4RowMeta();

    String fieldsClause = "A, B";
    String conditionClause = "A='Foo' AND B>5 OR PARAMETER('par')='foo'";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertFalse("Non-atomic condition expected", condition.isAtomic());
    assertEquals(2, condition.nrConditions());

    Condition one = condition.getCondition(0);
    assertEquals(2, one.nrConditions());

    Condition leftAnd = one.getCondition(0);
    assertEquals("A", leftAnd.getLeftValuename());
    assertEquals("=", leftAnd.getFunctionDesc());
    assertEquals("Foo", leftAnd.getRightExactString());
    Condition rightAnd = one.getCondition(1);
    assertEquals("B", rightAnd.getLeftValuename());
    assertEquals(">", rightAnd.getFunctionDesc());
    assertEquals("5", rightAnd.getRightExactString());
    assertEquals(Condition.OPERATOR_AND, rightAnd.getOperator());

    Condition param = condition.getCondition(1);
    assertTrue(param.isAtomic());
    assertEquals(Condition.OPERATOR_OR, param.getOperator());
    assertEquals(Condition.FUNC_TRUE, param.getFunction());
    assertEquals("par", param.getLeftValuename());
    assertEquals("foo", param.getRightExactString());
  }

  @Test
  public void testCondition27() throws Exception {

    IRowMeta rowMeta = SqlTest.generateServiceRowMeta();
    String fieldsClause = "\"Service\".\"Category\" as \"c0\", \"Service\".\"Country\" as \"c1\"";
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    String conditionClause =
        "(NOT((sum(\"Service\".\"sales_amount\") is null)) OR NOT((sum(\"Service\".\"products_sold\") is null)) )";
    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();
    assertNotNull(condition);
  }

  @Test
  public void testCondition28() throws Exception {

    IRowMeta rowMeta = SqlTest.generateServiceRowMeta();
    String fieldsClause =
        "\"Service\".\"Category\" as \"c0\", \"Service\".\"Country\" as \"c1\" from \"Service\" as \"Service\"";
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    String conditionClause =
        "((not (\"Service\".\"Country\" = 'Belgium') or (\"Service\".\"Country\" is null)))";
    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();
    assertNotNull(condition);
  }

  @Test
  public void testCondition29() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    String fieldsClause = "CUSTOMERNAME";
    String conditionClause =
        "\"GETTING_STARTED\".\"CUSTOMERNAME\" IN ('ANNA''S DECORATIONS, LTD', 'MEN ''R'' US RETAILERS, Ltd.' )";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals(Condition.FUNC_IN_LIST, condition.getFunction());

    assertEquals("\"GETTING_STARTED\".\"CUSTOMERNAME\"", condition.getLeftValuename());
    assertEquals(
        "ANNA'S DECORATIONS, LTD;MEN 'R' US RETAILERS, Ltd.", condition.getRightExactString());
  }

  /**
   * Test IN-clause with escaped quoting and semi-colons in them.
   *
   * @throws Exception
   */
  @Test
  public void testCondition30() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    String fieldsClause = "CUSTOMERNAME";
    String conditionClause = "CUSTOMERNAME IN (''';''', 'Toys ''R'' us' )";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals(Condition.FUNC_IN_LIST, condition.getFunction());

    assertEquals("CUSTOMERNAME", condition.getLeftValuename());
    assertEquals("'\\;';Toys 'R' us", condition.getRightExactString());
  }

  @Test
  public void testCondition31() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    String fieldsClause = "ORDERDATE";
    String conditionClause =
        "ORDERDATE IN (DATE '2004-01-15', DATE '2004-02-20', DATE '2004-05-18')"; // BACKLOG-19534

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals(Condition.FUNC_IN_LIST, condition.getFunction());

    assertEquals("ORDERDATE", condition.getLeftValuename());
    assertEquals(
        "2004/01/15 00:00:00.000;2004/02/20 00:00:00.000;2004/05/18 00:00:00.000",
        condition.getRightExactString());
  }

  @Test
  public void testNegatedTrueFuncEvaluatesAsFalse() throws Exception {
    IRowMeta rowMeta = SqlTest.generateServiceRowMeta();
    String fieldsClause = "\"Service\".\"Country\" as \"c\" from \"Service\" as \"Service\"";
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    String conditionClause = "(\"Service\".\"Country\" = null)";
    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();
    assertThat(condition.getFunctionDesc(), is("TRUE"));
    assertTrue(condition.isNegated());
  }

  @Test
  public void testLeftFieldWithTableQualifier() throws Exception {
    IRowMeta rowMeta = mockRowMeta("noSpaceField");

    SqlCondition sqlCondition =
        new SqlCondition("table", "\"table\".\"noSpaceField\" IS NULL", rowMeta);

    Condition condition = sqlCondition.getCondition();
    assertThat(condition.getFunctionDesc(), is("IS NULL"));
    assertThat(condition.getLeftValuename(), is("noSpaceField"));
    assertTrue(condition.isAtomic());
  }

  @Test
  public void testLeftFieldWithSpaceAndTableQualifier() throws Exception {
    IRowMeta rowMeta = mockRowMeta("Space Field");

    SqlCondition sqlCondition =
        new SqlCondition("table", "\"table\".\"Space Field\" IS NULL", rowMeta);
    Condition condition = sqlCondition.getCondition();
    assertThat(condition.getFunctionDesc(), is("IS NULL"));
    assertThat(condition.getLeftValuename(), is("Space Field"));
    assertTrue(condition.isAtomic());
  }

  @Test
  public void testLeftAndRightFieldWithSpaceAndTableQualifier() throws Exception {
    IRowMeta rowMeta = mockRowMeta("Left Field", "Right Field");

    SqlCondition sqlCondition =
        new SqlCondition("table", "\"table\".\"Left Field\" = \"table\".\"Right Field\"", rowMeta);
    Condition condition = sqlCondition.getCondition();
    assertThat(condition.getFunctionDesc(), is("="));
    assertThat(condition.getLeftValuename(), is("Left Field"));
    assertThat(condition.getRightValuename(), is("Right Field"));
    assertTrue(condition.isAtomic());
  }

  @Test
  public void testCompoundConditionLeftFieldWithSpaceAndTableQualifier() throws Exception {
    IRowMeta rowMeta = mockRowMeta("Space Field");

    SqlCondition sqlCondition =
        new SqlCondition(
            "table",
            "\"table\".\"Space Field\" IS NULL AND \"table\".\"Space Field\" > 1",
            rowMeta);
    Condition condition = sqlCondition.getCondition();
    assertThat(condition.getChildren().size(), is(2));

    List<Condition> children = condition.getChildren();
    assertThat(children.get(0).getFunctionDesc(), is("IS NULL"));
    assertThat(children.get(0).getLeftValuename(), is("Space Field"));
    assertThat(children.get(1).getOperator(), is(Condition.OPERATOR_AND));
    assertThat(children.get(1).getFunctionDesc(), is(">"));
    assertThat(children.get(1).getLeftValuename(), is("Space Field"));
    assertTrue(condition.isComposite());
  }

  @Test
  public void testHavingConditionWithEquals() throws Exception {
    IRowMeta rowMeta = mockRowMeta("Led Zeppelin", "Rulz!");

    SqlCondition sqlCondition = new SqlCondition("table", "SUM('Led Zeppelin') = 0", rowMeta);
    Condition condition = sqlCondition.getCondition();
    assertThat(condition.getFunctionDesc(), is("="));
    assertThat(condition.getLeftValuename(), is("SUM('Led Zeppelin')"));
    assertThat(condition.getRightExact().getValueData(), is(0l));
    assertTrue(condition.isAtomic());
  }

  @Test
  public void testSearchForWord() throws Exception {
    SqlCondition sqlCondition = mock(SqlCondition.class);
    doCallRealMethod().when(sqlCondition).searchForWord(anyString(), anyString(), anyInt());

    assertEquals(0, sqlCondition.searchForWord("AND", "AND", 0));
    assertEquals(-1, sqlCondition.searchForWord("AND", "AND", 1));
    assertEquals(1, sqlCondition.searchForWord(" AND", "AND", 0));
    assertEquals(1, sqlCondition.searchForWord(" AND", "AND", 1));
    assertEquals(-1, sqlCondition.searchForWord(" AND", "AND", 2));
    assertEquals(0, sqlCondition.searchForWord("AND ", "AND", 0));
    assertEquals(-1, sqlCondition.searchForWord("AND ", "AND", 1));
    assertEquals(1, sqlCondition.searchForWord(" AND ", "AND", 0));
    assertEquals(1, sqlCondition.searchForWord(" AND ", "AND", 1));
    assertEquals(-1, sqlCondition.searchForWord(" AND ", "AND", 2));
    assertEquals(-1, sqlCondition.searchForWord(" ANDY ", "AND", 0));
    assertEquals(-1, sqlCondition.searchForWord(" ANDY", "AND", 0));
    assertEquals(-1, sqlCondition.searchForWord(" DANDY ", "AND", 0));
    assertEquals(-1, sqlCondition.searchForWord(" DANDY ", "AND", 1));
    assertEquals(-1, sqlCondition.searchForWord(" DANDY ", "AND", 2));
    assertEquals(3, sqlCondition.searchForWord(" D AND Y ", "AND", 0));
    assertEquals(3, sqlCondition.searchForWord(" D AND Y AND ", "AND", 0));
    assertEquals(3, sqlCondition.searchForWord(" D and Y AND ", "AND", 0));
    assertEquals(3, sqlCondition.searchForWord(" D AND Y AND ", "and", 0));
    assertEquals(3, sqlCondition.searchForWord(" D\nAND\nY AND ", "and", 0));
    assertEquals(4, sqlCondition.searchForWord(" D\r\nAND\r\nY AND ", "and", 0));
  }

  // DATE_TO_STR
  //

  @Test
  public void testDateToStrConditionLeftField() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    SqlCondition sqlCondition =
        new SqlCondition("table", "DATE_TO_STR(ORDERDATE, 'yyyy-MM') = '2018-02'", rowMeta);

    Collection<DateToStrFunction> functions = sqlCondition.getDateToStrFunctions();
    assertTrue(functions.size() == 1);

    DateToStrFunction function = functions.iterator().next();
    assertThat(function.getDateMask(), is("yyyy-MM"));
    assertThat(function.getFieldName(), is("ORDERDATE"));

    Condition condition = sqlCondition.getCondition();
    assertThat(condition.getFunctionDesc(), is("="));
    assertTrue(condition.getLeftValuename() == function.getResultName());
    assertThat(condition.getRightExact().getValueData(), is("2018-02"));
    assertTrue(condition.isAtomic());
  }

  @Test
  public void testDateToStrConditionRightField() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    SqlCondition sqlCondition =
        new SqlCondition("table", "STATE = DATE_TO_STR(ORDERDATE, 'yyyy-MM')", rowMeta);

    Collection<DateToStrFunction> functions = sqlCondition.getDateToStrFunctions();
    assertTrue(functions.size() == 1);

    DateToStrFunction function = functions.iterator().next();
    assertThat(function.getDateMask(), is("yyyy-MM"));
    assertThat(function.getFieldName(), is("ORDERDATE"));

    Condition condition = sqlCondition.getCondition();
    assertThat(condition.getFunctionDesc(), is("="));
    assertTrue(condition.getRightValuename() == function.getResultName());
    assertThat(condition.getLeftValuename(), is("STATE"));
    assertTrue(condition.isAtomic());
  }

  @Test
  public void testDateToStrConditionSharedName() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    SqlCondition sqlCondition =
        new SqlCondition(
            "table",
            "DATE_TO_STR(orderdate, 'MM') >= '01' and date_to_str(ORDERDATE, 'MM') <= '03'",
            rowMeta);

    Collection<DateToStrFunction> functions = sqlCondition.getDateToStrFunctions();
    assertTrue(functions.size() == 1);

    DateToStrFunction function = functions.iterator().next();
    assertThat(function.getDateMask(), is("MM"));
    assertThat(function.getFieldName(), is("ORDERDATE"));

    Condition condition = sqlCondition.getCondition();
    Condition firstCondition = condition.getCondition(0);
    Condition secondCondition = condition.getCondition(1);

    assertThat(firstCondition.getFunctionDesc(), is(">="));
    assertThat(secondCondition.getFunctionDesc(), is("<="));
    assertTrue(firstCondition.getLeftValuename() == function.getResultName());
    assertTrue(secondCondition.getLeftValuename() == function.getResultName());
  }

  @Test
  public void testDateToStrConditionUnknownField() {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();
    try {
      SqlCondition sqlCondition =
          new SqlCondition("table", "DATE_TO_STR(potatos, 'MM') = '01'", rowMeta);
      fail();
    } catch (Exception kse) {
      assertTrue(kse.getMessage().contains("Unknown field"));
    }
  }

  @Test
  public void testDateToStrConditionInvalidFieldType() {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();
    try {
      SqlCondition sqlCondition =
          new SqlCondition("table", "DATE_TO_STR(STATE, 'MM') = '01'", rowMeta);
      fail();
    } catch (Exception kse) {
      assertTrue(kse.getMessage().contains("Invalid field type"));
    }
  }

  @Test
  public void testDateToStrEscapedQuotes() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    /* simple quote */
    SqlCondition sqlCondition =
        new SqlCondition(
            "table", "STATE = DATE_TO_STR(ORDERDATE, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX')", rowMeta);

    Collection<DateToStrFunction> functions = sqlCondition.getDateToStrFunctions();
    assertTrue(functions.size() == 1);

    DateToStrFunction function = functions.iterator().next();
    assertThat(function.getDateMask(), is("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));

    /* double quote */
    SqlCondition sqlCondition2 =
        new SqlCondition(
            "table", "STATE = DATE_TO_STR(ORDERDATE, 'hh ''o''''clock'' a, zzzz')", rowMeta);

    Collection<DateToStrFunction> functions2 = sqlCondition2.getDateToStrFunctions();
    assertTrue(functions2.size() == 1);

    DateToStrFunction function2 = functions2.iterator().next();
    assertThat(function2.getDateMask(), is("hh 'o''clock' a, zzzz"));
  }

  /**
   * Test IN-clause with decimal numbers should not truncate the number.
   *
   * @throws Exception
   */
  @Test
  public void testInNumberDecimalCondition() throws Exception {
    IRowMeta rowMeta = SqlTest.generateNumberRowMeta();

    String fieldsClause = "A";
    String conditionClause = "A IN (123.456, -123.456)";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals(Condition.FUNC_IN_LIST, condition.getFunction());

    assertEquals(fieldsClause, condition.getLeftValuename());
    assertEquals("123.456;-123.456", condition.getRightExactString());
  }

  /**
   * Test IN-clause with decimal big numbers should not truncate the number.
   *
   * @throws Exception
   */
  @Test
  public void testInBigNumberDecimalCondition() throws Exception {
    IRowMeta rowMeta = SqlTest.generateNumberRowMeta();

    String fieldsClause = "B";
    String conditionClause = "B IN (123.456, -123.456)";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals(Condition.FUNC_IN_LIST, condition.getFunction());

    assertEquals(fieldsClause, condition.getLeftValuename());
    assertEquals("123.456;-123.456", condition.getRightExactString());
  }

  /** Test Mondrian, analyzer contains hack: '%' || 'string' || '%' --> '%string%' */
  @Test
  public void testMondrianContainsCondition() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    String fieldsClause = "CUSTOMERNAME";
    String conditionClause = "CUSTOMERNAME CONTAINS '%' || 'string' || '%'";

    // Correctness of the next statement is tested in SqlFieldsTest
    //
    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition sqlCondition = new SqlCondition("Service", conditionClause, rowMeta, fields);
    Condition condition = sqlCondition.getCondition();

    assertNotNull(condition);
    assertFalse(condition.isEmpty());
    assertTrue(condition.isAtomic());
    assertEquals(Condition.FUNC_CONTAINS, condition.getFunction());

    assertEquals(fieldsClause, condition.getLeftValuename());
    assertEquals("%string%", condition.getRightExactString());
  }

  @Test
  public void testGetTableAlias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    String fieldsClause = "CUSTOMERNAME";
    String tableAlias = "Service";
    String conditionClause = "CUSTOMERNAME = 'MockConditionClause'";

    SqlFields fields = new SqlFields(tableAlias, rowMeta, fieldsClause);

    SqlCondition condition = new SqlCondition(tableAlias, conditionClause, rowMeta, fields);

    assertEquals(tableAlias, condition.getTableAlias());
  }

  @Test
  public void testGetSelectFields() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    String fieldsClause = "CUSTOMERNAME";
    String conditionClause = "CUSTOMERNAME = 'MockConditionClause'";

    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition condition = new SqlCondition("Service", conditionClause, rowMeta, fields);

    assertEquals(fields, condition.getSelectFields());
  }

  @Test
  public void testSetGetConditionClause() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();

    String fieldsClause = "CUSTOMERNAME";
    String conditionClause = "CUSTOMERNAME = 'MockConditionClause'";
    String conditionClause2 = "CUSTOMERNAME = 'MockConditionClause2'";

    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition condition = new SqlCondition("Service", conditionClause, rowMeta, fields);

    assertEquals(conditionClause, condition.getConditionClause());

    condition.setConditionClause(conditionClause2);

    assertEquals(conditionClause2, condition.getConditionClause());
  }

  @Test
  public void testSetGetServiceFields() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();
    IRowMeta rowMeta2 = SqlTest.generateServiceRowMeta();

    String fieldsClause = "CUSTOMERNAME";
    String conditionClause = "CUSTOMERNAME = 'MockConditionClause'";

    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition condition = new SqlCondition("Service", conditionClause, rowMeta, fields);

    assertEquals(rowMeta, condition.getServiceFields());

    condition.setServiceFields(rowMeta2);

    assertEquals(rowMeta2, condition.getServiceFields());
  }

  @Test
  public void testSetGetCondition() throws Exception {
    IRowMeta rowMeta = SqlTest.generateGettingStartedRowMeta();
    Condition mockCondition = mock(Condition.class);
    when(mockCondition.isEmpty()).thenReturn(true);

    String fieldsClause = "CUSTOMERNAME";
    String conditionClause = "CUSTOMERNAME = 'MockConditionClause'";

    SqlFields fields = new SqlFields("Service", rowMeta, fieldsClause);

    SqlCondition condition = new SqlCondition("Service", conditionClause, rowMeta, fields);

    assertNotNull(condition.getCondition());
    assertFalse(condition.isEmpty());

    condition.setCondition(mockCondition);

    assertEquals(mockCondition, condition.getCondition());
    assertTrue(condition.isEmpty());
  }
}
