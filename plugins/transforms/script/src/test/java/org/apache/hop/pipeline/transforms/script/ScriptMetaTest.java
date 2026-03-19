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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ScriptMetaTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private ScriptMeta meta;
  private IRowMeta rowMeta;
  private IVariables variables;
  private IHopMetadataProvider metadataProvider;

  @BeforeEach
  void setUp() {
    meta = new ScriptMeta();
    rowMeta = new org.apache.hop.core.row.RowMeta();
    variables = mock(IVariables.class);
    metadataProvider = mock(IHopMetadataProvider.class);
  }

  @Test
  void getLanguageName_defaultIsNull() {
    assertNull(meta.getLanguageName());
  }

  @Test
  void setLanguageName_getLanguageName_roundTrip() {
    meta.setLanguageName("Groovy");
    assertEquals("Groovy", meta.getLanguageName());
    meta.setLanguageName("ECMAScript");
    assertEquals("ECMAScript", meta.getLanguageName());
  }

  @Test
  void getScripts_defaultIsEmptyList() {
    assertNotNull(meta.getScripts());
    assertTrue(meta.getScripts().isEmpty());
  }

  @Test
  void setScripts_getScripts_roundTrip() {
    List<ScriptMeta.SScript> scripts = new ArrayList<>();
    ScriptMeta.SScript s = new ScriptMeta.SScript();
    s.setScriptName("s1");
    s.setScript("x=1");
    scripts.add(s);
    meta.setScripts(scripts);
    assertEquals(1, meta.getScripts().size());
    assertEquals("s1", meta.getScripts().get(0).getScriptName());
    assertEquals("x=1", meta.getScripts().get(0).getScript());
  }

  @Test
  void getFields_defaultIsEmptyList() {
    assertNotNull(meta.getFields());
    assertTrue(meta.getFields().isEmpty());
  }

  @Test
  void setFields_getFields_roundTrip() {
    List<ScriptMeta.SField> fields = new ArrayList<>();
    ScriptMeta.SField f = new ScriptMeta.SField();
    f.setName("out1");
    f.setType("String");
    fields.add(f);
    meta.setFields(fields);
    assertEquals(1, meta.getFields().size());
    assertEquals("out1", meta.getFields().get(0).getName());
    assertEquals("String", meta.getFields().get(0).getType());
  }

  @Test
  void clone_returnsNewInstanceWithSameContent() {
    meta.setLanguageName("Groovy");
    meta.getScripts()
        .add(new ScriptMeta.SScript(ScriptMeta.ScriptType.TRANSFORM_SCRIPT, "s1", "1+1"));
    meta.getFields().add(createField("f1", "Integer"));

    ScriptMeta cloned = meta.clone();
    assertNotNull(cloned);
    assertFalse(cloned == meta);
    assertEquals("Groovy", cloned.getLanguageName());
    assertEquals(1, cloned.getScripts().size());
    assertEquals("s1", cloned.getScripts().get(0).getScriptName());
    assertEquals(1, cloned.getFields().size());
    assertEquals("f1", cloned.getFields().get(0).getName());
  }

  private static ScriptMeta.SField createField(String name, String type) {
    ScriptMeta.SField f = new ScriptMeta.SField();
    f.setName(name);
    f.setType(type);
    return f;
  }

  @Test
  void getFields_addsNewFieldWhenNotReplace() throws HopTransformException {
    ScriptMeta.SField field = createField("newField", "String");
    field.setRename("newField"); // when rename is set, getFields uses getName() for the new field
    meta.getFields().add(field);
    meta.getFields(rowMeta, "Script", null, null, variables, metadataProvider);
    assertEquals(1, rowMeta.size());
    assertEquals("newField", rowMeta.getValueMeta(0).getName());
  }

  @Test
  void getFields_skipsFieldWithEmptyName() throws HopTransformException {
    meta.getFields().add(createField("", "String"));
    ScriptMeta.SField valid = createField("valid", "Integer");
    valid.setRename("valid");
    meta.getFields().add(valid);
    meta.getFields(rowMeta, "Script", null, null, variables, metadataProvider);
    assertEquals(1, rowMeta.size());
    assertEquals("valid", rowMeta.getValueMeta(0).getName());
  }

  @Test
  void getFields_replacesExistingFieldWhenReplaceTrue() throws HopTransformException {
    rowMeta.addValueMeta(new ValueMetaString("existing"));
    ScriptMeta.SField field = createField("existing", "Integer");
    field.setReplace(true);
    field.setRename("existing");
    meta.getFields().add(field);
    meta.getFields(rowMeta, "Script", null, null, variables, metadataProvider);
    assertEquals(1, rowMeta.size());
    assertEquals("existing", rowMeta.getValueMeta(0).getName());
    assertEquals(
        org.apache.hop.core.row.IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(0).getType());
  }

  @Test
  void getFields_throwsWhenReplaceTrueAndFieldNotFound() {
    ScriptMeta.SField field = createField("missing", "String");
    field.setReplace(true);
    field.setRename("");
    meta.getFields().add(field);
    assertThrows(
        HopTransformException.class,
        () -> meta.getFields(rowMeta, "Script", null, null, variables, metadataProvider));
  }

  @Test
  void sField_getHopType_returnsCorrectType() {
    ScriptMeta.SField f = new ScriptMeta.SField();
    f.setType("String");
    assertEquals(IValueMeta.TYPE_STRING, f.getHopType());
    f.setType("Integer");
    assertEquals(IValueMeta.TYPE_INTEGER, f.getHopType());
  }

  @Test
  void sField_copyConstructor() {
    ScriptMeta.SField orig = new ScriptMeta.SField();
    orig.setName("a");
    orig.setRename("b");
    orig.setType("Number");
    orig.setLength(10);
    orig.setPrecision(2);
    orig.setReplace(true);
    orig.setScriptResult(true);
    ScriptMeta.SField copy = new ScriptMeta.SField(orig);
    assertEquals("a", copy.getName());
    assertEquals("b", copy.getRename());
    assertEquals("Number", copy.getType());
    assertEquals(10, copy.getLength());
    assertEquals(2, copy.getPrecision());
    assertTrue(copy.isReplace());
    assertTrue(copy.isScriptResult());
  }

  @Test
  void sScript_scriptTypeHelpers() {
    ScriptMeta.SScript s = new ScriptMeta.SScript();
    s.setScriptType(ScriptMeta.ScriptType.TRANSFORM_SCRIPT);
    assertTrue(s.isTransformScript());
    assertFalse(s.isStartScript());
    assertFalse(s.isEndScript());
    s.setScriptType(ScriptMeta.ScriptType.START_SCRIPT);
    assertTrue(s.isStartScript());
    s.setScriptType(ScriptMeta.ScriptType.END_SCRIPT);
    assertTrue(s.isEndScript());
  }

  @Test
  void sScript_copyConstructor() {
    ScriptMeta.SScript orig =
        new ScriptMeta.SScript(ScriptMeta.ScriptType.TRANSFORM_SCRIPT, "sn", "body");
    ScriptMeta.SScript copy = new ScriptMeta.SScript(orig);
    assertEquals(ScriptMeta.ScriptType.TRANSFORM_SCRIPT, copy.getScriptType());
    assertEquals("sn", copy.getScriptName());
    assertEquals("body", copy.getScript());
  }

  @Test
  void scriptType_enumValues() {
    assertEquals("-1", ScriptMeta.ScriptType.NORMAL_SCRIPT.getCode());
    assertEquals("0", ScriptMeta.ScriptType.TRANSFORM_SCRIPT.getCode());
    assertEquals("1", ScriptMeta.ScriptType.START_SCRIPT.getCode());
    assertEquals("2", ScriptMeta.ScriptType.END_SCRIPT.getCode());
    assertNotNull(ScriptMeta.ScriptType.TRANSFORM_SCRIPT.getDescription());
  }
}
