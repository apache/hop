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

package org.apache.hop.openformula;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hop.openformula.parser.ParseException;
import org.junit.jupiter.api.Test;

public class FormulaParsingTest {

  @Test
  public void testEmptyArray() throws ParseException, EvaluationException {
    final Formula formula = new Formula("{}");
    formula.initialize(new DefaultFormulaContext());
    assertNotNull(formula.getRootReference());
  }

  @Test
  public void testEmptyArray2() throws ParseException, EvaluationException {
    final Formula formula = new Formula("COUNT({})");
    formula.initialize(new DefaultFormulaContext());
    assertNotNull(formula.getRootReference());
  }

  @Test
  public void testTermOperands() throws ParseException, EvaluationException {
    final Formula formula = new Formula("\"a\" & \"b\" & \"c\"");
    formula.initialize(new DefaultFormulaContext());
    assertNotNull(formula.getRootReference());
    assertEquals("abc", formula.evaluate());
  }

  @Test
  public void testParseWithLineBreaks() throws ParseException, EvaluationException {
    final Formula formula = new Formula("\"aaa\" \n&\n \"bb\" & \"\n\" & \"\"");
    formula.initialize(new DefaultFormulaContext());
    final Object o = formula.evaluate();
    assertEquals("aaabb\n", o, "Formula value");
  }

  @Test
  public void testParse() throws ParseException, EvaluationException {
    final Formula formula =
        new Formula("MID(UPPER([name] & \n\r   \" \" \n\r   & [firstname]);5;10)");
    final DefaultFormulaContext context = new DefaultFormulaContext();
    context.defineReference("name", "name");
    context.defineReference("firstname", "firstname");
    formula.initialize(context);
    final Object o = formula.evaluate();
    assertEquals(" FIRSTNAME", o, "Formula value");
  }

  @Test
  public void testEscapes() throws Exception {
    final Formula formula = new Formula("T(\"\\\") = \"A\"");
    final DefaultFormulaContext context = new DefaultFormulaContext();
    context.defineReference("path", "x");
    formula.initialize(context);
    final Object o = formula.evaluate();
    assertEquals(Boolean.FALSE, o, "Formula value");
  }

  @Test
  public void testQuotedReference() throws Exception {
    final Formula formula = new Formula("T([\"\\\\\"]) = \"A\"");
    final DefaultFormulaContext context = new DefaultFormulaContext();
    context.defineReference("\\\\", "Dummy");
    formula.initialize(context);
    final Object o = formula.evaluate();
    assertEquals(Boolean.FALSE, o, "Formula value");
  }

  @Test
  public void testQuotedReference2() throws Exception {
    final Formula formula = new Formula("T([\"[x]\"]) = \"x\"");
    final DefaultFormulaContext context = new DefaultFormulaContext();
    context.defineReference("[x]", "x");
    formula.initialize(context);
    final Object o = formula.evaluate();
    assertEquals(Boolean.TRUE, o, "Formula value");
  }

  @Test
  public void testParseFailure() throws Exception {
    assertThrows(ParseException.class, () -> new Formula("T([year4))"));
  }

  @Test
  public void testParseLogicalCondition() throws Exception {
    Formula formula =
        new Formula(
            "AND ( [TABLEA.COLA] = 23; [TABLEA.COLB] = 2012; ( OR ( [TABLEB.COLA] = 20932598; [TABLEA.COLC] = 20932598 ) ) )");
    assertNotNull(formula.getRootReference());
  }
}
