/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.hop.pipeline.transforms.script;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.junit.jupiter.api.Test;

class ScriptUtilsTest {

  @Test
  void getInstance_returnsSingleton() {
    ScriptUtils a = ScriptUtils.getInstance();
    ScriptUtils b = ScriptUtils.getInstance();
    assertNotNull(a);
    assertSame(a, b);
  }

  @Test
  void getScriptLanguageNames_returnsNonEmptyList() {
    assertNotNull(ScriptUtils.getInstance().getScriptLanguageNames());
    assertTrue(ScriptUtils.getInstance().getScriptLanguageNames().size() > 0);
  }

  @Test
  void getScriptLanguageNames_containsGroovy() {
    assertTrue(
        ScriptUtils.getInstance().getScriptLanguageNames().contains("Groovy"),
        "Expected Groovy in script language names");
  }

  @Test
  void getScriptLanguageNames_containsECMAScript() {
    assertTrue(
        ScriptUtils.getInstance().getScriptLanguageNames().contains("ECMAScript"),
        "Expected ECMAScript in script language names (GraalVM JS)");
  }

  @Test
  void getScriptEngineByName_groovy_returnsEngine() {
    ScriptEngine engine = ScriptUtils.getInstance().getScriptEngineByName("Groovy");
    assertNotNull(engine);
  }

  @Test
  void getScriptEngineByName_groovy_evalSucceeds() throws ScriptException {
    ScriptEngine engine = ScriptUtils.getInstance().getScriptEngineByName("Groovy");
    assertNotNull(engine);
    Object result = engine.eval("1 + 2");
    assertEquals(3, result);
  }

  @Test
  void getScriptEngineByName_ecmascript_returnsEngine() {
    ScriptEngine engine = ScriptUtils.getInstance().getScriptEngineByName("ECMAScript");
    assertNotNull(engine);
  }

  @Test
  void getScriptEngineByName_ecmascript_evalSucceeds() throws ScriptException {
    ScriptEngine engine = ScriptUtils.getInstance().getScriptEngineByName("ECMAScript");
    assertNotNull(engine);
    Object result = engine.eval("1 + 2");
    assertNotNull(result);
    assertTrue(result instanceof Number, "Expected number, got " + result.getClass().getName());
    assertEquals(3, ((Number) result).intValue());
  }

  @Test
  void getScriptEngineByName_ecmascript_returnsGraalEngineWithHostAccess() throws ScriptException {
    ScriptEngine engine = ScriptUtils.getInstance().getScriptEngineByName("ECMAScript");
    assertNotNull(engine);
    assertTrue(
        engine instanceof GraalJSScriptEngine, "ECMAScript engine should be GraalJSScriptEngine");

    // Verify host access: script can call a method on a bound Java object (e.g. transform.logBasic)
    engine.put("holder", new StringBuilder());
    engine.eval("holder.append('ok')");
    assertEquals("ok", engine.get("holder").toString());
  }

  @Test
  void getScriptEngineByName_unknown_returnsNull() {
    ScriptEngine engine = ScriptUtils.getInstance().getScriptEngineByName("NonExistentLanguageXYZ");
    assertNull(engine);
  }

  @Test
  void getScriptEngineByName_python_returnsEngine() {
    ScriptEngine engine = ScriptUtils.getInstance().getScriptEngineByName("python");
    assertNotNull(engine);
  }

  @Test
  void getScriptEngineByName_python_evalSucceeds() throws ScriptException {
    ScriptEngine engine = ScriptUtils.getInstance().getScriptEngineByName("python");
    assertNotNull(engine);
    Object result = engine.eval("1 + 2");
    assertEquals(3, result);
  }
}
