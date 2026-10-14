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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class FormulaCoreTest {
  @Test
  void testArithmetic() throws Exception {
    Formula f = new Formula("[one] + [two] * 3");
    DefaultFormulaContext ctx = new DefaultFormulaContext();
    ctx.defineReference("one", 10);
    ctx.defineReference("two", 5);
    f.initialize(ctx);
    Object res = f.evaluate();
    assertEquals(new BigDecimal(25), res);
  }

  @Test
  void testLogicalAndIf() throws Exception {
    Formula f = new Formula("IF([flag]; \"YES\"; \"NO\")");
    DefaultFormulaContext ctx = new DefaultFormulaContext();
    ctx.defineReference("flag", true);
    f.initialize(ctx);
    assertEquals("YES", f.evaluate());

    ctx.defineReference("flag", false);
    assertEquals("NO", f.evaluate());
  }

  @Test
  void testStringConcat() throws Exception {
    Formula f = new Formula("\"Hello \" & [name]");
    DefaultFormulaContext ctx = new DefaultFormulaContext();
    ctx.defineReference("name", "World");
    f.initialize(ctx);
    assertEquals("Hello World", f.evaluate());
  }

  @Test
  void testFunctionCatalog() {
    DefaultFormulaContext ctx = new DefaultFormulaContext();
    String[] names = ctx.getFunctionRegistry().getFunctionNames();
    assertTrue(
        names.length >= 100, "Should have registered >= 100 functions, found: " + names.length);
  }
}
