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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ScriptValuesMetaTest {
  @BeforeAll
  static void beforeAll() throws HopException {
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        ValueMetaString.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaInteger.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaNumber.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaDate.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
  }

  @Test
  void testXmlRoundTrip() throws Exception {
    // Test XML->Meta->XML->Meta
    ScriptValuesMeta meta =
        TransformSerializationTestUtil.testSerialization("/javascript.xml", ScriptValuesMeta.class);

    // See if after all that we still have the correct data
    //
    assertEquals("2", meta.getOptimizationLevel());
    assertEquals("ES6", meta.getLanguageVersion());

    // The scripts
    assertEquals(3, meta.getJsScripts().size());
    ScriptValuesScript s = meta.getJsScripts().getFirst();
    assertEquals("startScript", s.getName());
    assertEquals("startScript", s.getScript());
    assertEquals(1, s.getType());
    s = meta.getJsScripts().get(1);
    assertEquals("transformScript", s.getName());
    assertEquals("transformScript", s.getScript());
    assertEquals(0, s.getType());
    s = meta.getJsScripts().getLast();
    assertEquals("endScript", s.getName());
    assertEquals("endScript", s.getScript());
    assertEquals(2, s.getType());

    // The fields
    assertEquals(3, meta.getScriptFields().size());
    ScriptValuesMeta.ScriptField f = meta.getScriptFields().getFirst();
    assertEquals("f1", f.getName());
    assertEquals("renamedF1", f.getRename());
    assertEquals(100, f.getLength());
    assertEquals(-1, f.getPrecision());
    assertTrue(f.isReplace());

    f = meta.getScriptFields().get(1);
    assertEquals("f2", f.getName());
    assertEquals("renamedF2", f.getRename());
    assertEquals(7, f.getLength());
    assertEquals(-1, f.getPrecision());
    assertTrue(f.isReplace());

    f = meta.getScriptFields().getLast();
    assertEquals("f3", f.getName());
    assertEquals("renamedF3", f.getRename());
    assertEquals(9, f.getLength());
    assertEquals(2, f.getPrecision());
    assertTrue(f.isReplace());
  }

  /**
   * The copy constructor used to call the no-arg one, which seeds a default "Script 1" for brand
   * new transforms. Every clone therefore added another phantom script.
   */
  @Test
  void cloningDoesNotAddPhantomScripts() {
    ScriptValuesMeta meta = new ScriptValuesMeta();
    meta.getJsScripts().clear();
    meta.getJsScripts()
        .add(new ScriptValuesScript(ScriptValuesScript.TRANSFORM_SCRIPT, "Script 1", "a=1;"));

    ScriptValuesMeta copy = (ScriptValuesMeta) meta.clone();
    for (int i = 0; i < 5; i++) {
      copy = (ScriptValuesMeta) copy.clone();
    }

    assertEquals(1, copy.getJsScripts().size());
    ScriptValuesScript script = copy.getJsScripts().getFirst();
    assertEquals("Script 1", script.getName());
    assertEquals("a=1;", script.getScript());
    assertEquals(ScriptValuesScript.TRANSFORM_SCRIPT, script.getType());
  }

  /** A clone has to be a deep copy: editing the copy may not touch the original. */
  @Test
  void cloningCopiesScriptsAndFields() {
    ScriptValuesMeta meta = new ScriptValuesMeta();
    meta.getJsScripts().clear();
    meta.getJsScripts()
        .add(new ScriptValuesScript(ScriptValuesScript.TRANSFORM_SCRIPT, "Script 1", "a=1;"));
    ScriptValuesMeta.ScriptField field = new ScriptValuesMeta.ScriptField();
    field.setName("a");
    meta.getScriptFields().add(field);
    meta.setOptimizationLevel("3");
    meta.setLanguageVersion("1.8");

    ScriptValuesMeta copy = (ScriptValuesMeta) meta.clone();
    assertEquals("3", copy.getOptimizationLevel());
    assertEquals("1.8", copy.getLanguageVersion());
    assertEquals(1, copy.getScriptFields().size());
    assertEquals("a", copy.getScriptFields().getFirst().getName());

    copy.getJsScripts().getFirst().setScript("b=2;");
    copy.getScriptFields().getFirst().setName("b");
    assertEquals("a=1;", meta.getJsScripts().getFirst().getScript());
    assertEquals("a", meta.getScriptFields().getFirst().getName());
  }
}
