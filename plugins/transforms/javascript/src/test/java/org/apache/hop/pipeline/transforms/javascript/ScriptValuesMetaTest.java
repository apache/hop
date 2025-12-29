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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.BooleanLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveBooleanArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ScriptValuesMetaTest implements IInitializer<ITransformMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  LoadSaveTester loadSaveTester;
  Class<ScriptValuesMeta> testMetaClass = ScriptValuesMeta.class;

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
    List<String> attributes =
        Arrays.asList(
            "fieldname",
            "rename",
            "type",
            "length",
            "precision",
            "replace",
            "jsScripts",
            "optimizationLevel");

    Map<String, String> getterMap =
        new HashMap<>() {
          {
            put("fieldname", "getFieldname");
            put("rename", "getRename");
            put("type", "getType");
            put("length", "getLength");
            put("precision", "getPrecision");
            put("replace", "getReplace");
            //        put( "compatible", "isCompatible" );
            put("optimizationLevel", "getOptimizationLevel");
            put("jsScripts", "getJSScripts");
          }
        };
    Map<String, String> setterMap =
        new HashMap<>() {
          {
            put("fieldname", "setFieldname");
            put("rename", "setRename");
            put("type", "setType");
            put("length", "setLength");
            put("precision", "setPrecision");
            put("replace", "setReplace");
            //        put( "compatible", "setCompatible" );
            put("optimizationLevel", "setOptimizationLevel");
            put("jsScripts", "setJSScripts");
          }
        };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
        new ArrayLoadSaveValidator<>(new StringLoadSaveValidator(), 5);

    IFieldLoadSaveValidator<ScriptValuesScript[]> svsArrayLoadSaveValidator =
        new ArrayLoadSaveValidator<>(new ScriptValuesScriptLoadSaveValidator(), 5);

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put("fieldname", stringArrayLoadSaveValidator);
    attrValidatorMap.put("rename", stringArrayLoadSaveValidator);
    attrValidatorMap.put(
        "type", new PrimitiveIntArrayLoadSaveValidator(new IntLoadSaveValidator(9), 5));
    attrValidatorMap.put(
        "length", new PrimitiveIntArrayLoadSaveValidator(new IntLoadSaveValidator(100), 5));
    attrValidatorMap.put(
        "precision", new PrimitiveIntArrayLoadSaveValidator(new IntLoadSaveValidator(6), 5));
    attrValidatorMap.put(
        "replace", new PrimitiveBooleanArrayLoadSaveValidator(new BooleanLoadSaveValidator(), 5));
    attrValidatorMap.put("jsScripts", svsArrayLoadSaveValidator);

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester(
            testMetaClass,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(ITransformMeta someMeta) {
    if (someMeta instanceof ScriptValuesMeta) {
      ((ScriptValuesMeta) someMeta).allocate(5);
    }
  }

  public class ScriptValuesScriptLoadSaveValidator
      implements IFieldLoadSaveValidator<ScriptValuesScript> {
    final Random rand = new Random();

    @Override
    public ScriptValuesScript getTestObject() {
      int scriptType = rand.nextInt(4);
      if (scriptType == 3) {
        scriptType = -1;
      }
      return new ScriptValuesScript(
          scriptType, UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(ScriptValuesScript testObject, Object actual) {
      if (!(actual instanceof ScriptValuesScript)) {
        return false;
      }
      return (actual.toString().equals(testObject.toString()));
    }
  }

  @Test
  void testExtend() {
    ScriptValuesMeta meta = new ScriptValuesMeta();
    int size = 1;
    meta.extend(size);

    assertEquals(size, meta.getFieldname().length);
    assertNull(meta.getFieldname()[0]);
    assertEquals(size, meta.getRename().length);
    assertNull(meta.getRename()[0]);
    assertEquals(size, meta.getType().length);
    assertEquals(-1, meta.getType()[0]);
    assertEquals(size, meta.getLength().length);
    assertEquals(-1, meta.getLength()[0]);
    assertEquals(size, meta.getPrecision().length);
    assertEquals(-1, meta.getPrecision()[0]);
    assertEquals(size, meta.getReplace().length);
    assertFalse(meta.getReplace()[0]);

    meta = new ScriptValuesMeta();

    meta.extend(3);
    validateExtended(meta);
  }

  private void validateExtended(final ScriptValuesMeta meta) {

    assertEquals(3, meta.getFieldname().length);
    //    assertEquals("Field 1", meta.getFieldname()[0]);
    //    assertEquals("Field 2", meta.getFieldname()[1]);
    //    assertEquals("Field 3", meta.getFieldname()[2]);
    //    assertEquals(3, meta.getRename().length);
    //    assertEquals("Field 1 - new", meta.getRename()[0]);
    //    assertNull(meta.getRename()[1]);
    //    assertNull(meta.getRename()[2]);
    //    assertEquals(3, meta.getType().length);
    //    assertEquals(IValueMeta.TYPE_STRING, meta.getType()[0]);
    //    assertEquals(IValueMeta.TYPE_INTEGER, meta.getType()[1]);
    //    assertEquals(IValueMeta.TYPE_NUMBER, meta.getType()[2]);
    //    assertEquals(3, meta.getLength().length);
    //    assertEquals(-1, meta.getLength()[0]);
    //    assertEquals(-1, meta.getLength()[1]);
    //    assertEquals(-1, meta.getLength()[2]);
    //    assertEquals(3, meta.getPrecision().length);
    //    assertEquals(-1, meta.getPrecision()[0]);
    //    assertEquals(-1, meta.getPrecision()[1]);
    //    assertEquals(-1, meta.getPrecision()[2]);
    //    assertEquals(3, meta.getReplace().length);
    //    assertFalse(meta.getReplace()[0]);
    //    assertFalse(meta.getReplace()[1]);
    //    assertFalse(meta.getReplace()[2]);
  }
}
