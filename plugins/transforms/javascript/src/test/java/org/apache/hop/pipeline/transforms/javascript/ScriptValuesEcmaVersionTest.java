/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.javascript;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Scriptable;

class ScriptValuesEcmaVersionTest {

  @Test
  void emptyDefaultsToEs6() throws Exception {
    assertEquals(ScriptValuesEcmaVersion.ES6, ScriptValuesEcmaVersion.fromCode(null));
    assertEquals(ScriptValuesEcmaVersion.ES6, ScriptValuesEcmaVersion.fromCode(""));
    assertEquals(ScriptValuesEcmaVersion.ES6, ScriptValuesEcmaVersion.fromCode("  "));
  }

  @Test
  void resolvesCodesCaseInsensitively() throws Exception {
    assertEquals(ScriptValuesEcmaVersion.ES6, ScriptValuesEcmaVersion.fromCode("es6"));
    assertEquals(
        ScriptValuesEcmaVersion.ECMASCRIPT, ScriptValuesEcmaVersion.fromCode("ECMASCRIPT"));
    assertEquals(ScriptValuesEcmaVersion.DEFAULT, ScriptValuesEcmaVersion.fromCode("DEFAULT"));
    assertEquals(ScriptValuesEcmaVersion.JS_1_7, ScriptValuesEcmaVersion.fromCode("1.7"));
  }

  @Test
  void resolvesNumericRhinoVersions() throws Exception {
    assertEquals(ScriptValuesEcmaVersion.ES6, ScriptValuesEcmaVersion.fromCode("200"));
    assertEquals(ScriptValuesEcmaVersion.ECMASCRIPT, ScriptValuesEcmaVersion.fromCode("250"));
    assertEquals(ScriptValuesEcmaVersion.DEFAULT, ScriptValuesEcmaVersion.fromCode("0"));
  }

  @Test
  void rejectsUnknownValues() {
    assertThrows(HopException.class, () -> ScriptValuesEcmaVersion.fromCode("ES2024"));
    assertThrows(HopException.class, () -> ScriptValuesEcmaVersion.fromCode("999"));
  }

  @Test
  void codeFromDescriptionRoundTrips() {
    for (ScriptValuesEcmaVersion version : ScriptValuesEcmaVersion.values()) {
      assertEquals(
          version.getCode(), ScriptValuesEcmaVersion.codeFromDescription(version.getCode()));
    }
  }

  @Test
  void es6AllowsLetAndConst() throws Exception {
    Context cx = ContextFactory.getGlobal().enterContext();
    try {
      ScriptValuesEcmaVersion.ES6.applyTo(cx);
      Scriptable scope = cx.initStandardObjects();
      Object result = cx.evaluateString(scope, "let a = 1; const b = 2; a + b", "t", 1, null);
      assertEquals(3.0, Context.toNumber(result), 0.0001);
    } finally {
      Context.exit();
    }
  }

  @Test
  void legacyDefaultRejectsLet() throws Exception {
    Context cx = ContextFactory.getGlobal().enterContext();
    try {
      ScriptValuesEcmaVersion.DEFAULT.applyTo(cx);
      Scriptable scope = cx.initStandardObjects();
      assertThrows(Exception.class, () -> cx.evaluateString(scope, "let a = 1; a", "t", 1, null));
    } finally {
      Context.exit();
    }
  }

  @Test
  void descriptionsAreNonEmpty() {
    String[] descriptions = ScriptValuesEcmaVersion.getDescriptions();
    assertEquals(ScriptValuesEcmaVersion.values().length, descriptions.length);
    for (String description : descriptions) {
      assertTrue(description != null && !description.isBlank());
    }
  }
}
