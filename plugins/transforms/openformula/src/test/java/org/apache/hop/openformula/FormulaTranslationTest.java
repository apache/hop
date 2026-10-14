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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Locale;
import org.apache.hop.openformula.common.TestFormulaContext;
import org.apache.hop.openformula.function.FunctionDescription;
import org.apache.hop.openformula.function.FunctionRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FormulaTranslationTest {
  private FormulaContext context;

  @BeforeEach
  public void setUp() throws Exception {
    context = new TestFormulaContext(TestFormulaContext.testCaseDataset);
  }

  @Test
  public void testTranslationsAvailable() {
    FunctionRegistry registry = context.getFunctionRegistry();
    String[] functions = registry.getFunctionNames();
    for (int i = 0; i < functions.length; i++) {
      String function = functions[i];
      FunctionDescription functionDesc = registry.getMetaData(function);
      assertNotNull(functionDesc, "Missing meta for " + function);
      assertFalse(functionDesc.getDisplayName(Locale.ENGLISH).isEmpty());
      assertFalse(functionDesc.getDescription(Locale.ENGLISH).isEmpty());
      int count = functionDesc.getParameterCount();
      for (int x = 0; x < count; x++) {
        assertNotNull(functionDesc.getParameterDescription(x, Locale.ENGLISH));
        assertFalse(functionDesc.getParameterDisplayName(x, Locale.ENGLISH).isEmpty());
      }
    }
  }
}
