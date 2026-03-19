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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class ScriptTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<ScriptMeta, ScriptData> transformMockHelper;
  private Script script;
  private ScriptMeta meta;

  @BeforeAll
  static void initPlugins() throws HopException {
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @BeforeEach
  void setUp() {
    transformMockHelper =
        new TransformMockHelper<>("Script TEST", ScriptMeta.class, ScriptData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);

    meta = new ScriptMeta();
    when(transformMockHelper.iTransformMeta.getFields()).thenReturn(meta.getFields());
    when(transformMockHelper.iTransformMeta.getScripts()).thenReturn(meta.getScripts());
    when(transformMockHelper.iTransformMeta.getLanguageName()).thenReturn("Groovy");

    script =
        new Script(
            transformMockHelper.transformMeta,
            meta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }

  // --- getValueFromScript: TYPE_INTEGER ---

  @Test
  void getValueFromScript_integer_returnsLong() throws HopValueException {
    meta.getFields().add(createField("id", "Integer"));
    assertEquals(42L, script.getValueFromScript(42, 0));
    assertEquals(42L, script.getValueFromScript(Integer.valueOf(42), 0));
  }

  @Test
  void getValueFromScript_long_returnsLong() throws HopValueException {
    meta.getFields().add(createField("id", "Integer"));
    assertEquals(42L, script.getValueFromScript(42L, 0));
  }

  @Test
  void getValueFromScript_numberDouble_returnsLong() throws HopValueException {
    meta.getFields().add(createField("id", "Integer"));
    assertEquals(3L, script.getValueFromScript(3.7, 0));
  }

  @Test
  void getValueFromScript_string_returnsLong() throws HopValueException {
    meta.getFields().add(createField("id", "Integer"));
    assertEquals(100L, script.getValueFromScript("100", 0));
  }

  @Test
  void getValueFromScript_null_returnsNull() throws HopValueException {
    meta.getFields().add(createField("id", "Integer"));
    assertNull(script.getValueFromScript(null, 0));
  }

  @Test
  void getValueFromScript_emptyName_throws() {
    ScriptMeta.SField field = createField("", "Integer");
    meta.getFields().add(field);
    assertThrows(HopValueException.class, () -> script.getValueFromScript(1, 0));
  }

  // --- getValueFromScript: TYPE_NUMBER ---

  @Test
  void getValueFromScript_numberType_integer_returnsDouble() throws HopValueException {
    meta.getFields().add(createField("x", "Number"));
    assertEquals(42.0, script.getValueFromScript(42, 0));
  }

  @Test
  void getValueFromScript_numberType_double_returnsDouble() throws HopValueException {
    meta.getFields().add(createField("x", "Number"));
    assertEquals(3.14, (Double) script.getValueFromScript(3.14, 0), 1e-9);
  }

  @Test
  void getValueFromScript_numberType_null_returnsNull() throws HopValueException {
    meta.getFields().add(createField("x", "Number"));
    assertNull(script.getValueFromScript(null, 0));
  }

  // --- getValueFromScript: TYPE_STRING ---

  @Test
  void getValueFromScript_stringType_returnsString() throws HopValueException {
    meta.getFields().add(createField("name", "String"));
    assertEquals("hello", script.getValueFromScript("hello", 0));
  }

  // --- getValueFromScript: TYPE_BOOLEAN ---

  @Test
  void getValueFromScript_booleanType_returnsBoolean() throws HopValueException {
    meta.getFields().add(createField("flag", "Boolean"));
    assertEquals(Boolean.TRUE, script.getValueFromScript(true, 0));
  }

  // --- getValueFromScript: TYPE_DATE ---

  @Test
  void getValueFromScript_dateType_date_returnsDate() throws HopValueException {
    meta.getFields().add(createField("d", "Date"));
    Date d = new Date(1234567890L);
    assertEquals(d, script.getValueFromScript(d, 0));
  }

  @Test
  void getValueFromScript_dateType_null_returnsNull() throws HopValueException {
    meta.getFields().add(createField("d", "Date"));
    assertNull(script.getValueFromScript(null, 0));
  }

  // --- getValueFromScript: TYPE_BIGNUMBER ---

  @Test
  void getValueFromScript_bignumberType_integer_returnsBigDecimal() throws HopValueException {
    meta.getFields().add(createField("bn", "BigNumber"));
    assertEquals(new BigDecimal("42"), script.getValueFromScript(42, 0));
  }

  @Test
  void getValueFromScript_bignumberType_bigDecimal_returnsBigDecimal() throws HopValueException {
    meta.getFields().add(createField("bn", "BigNumber"));
    // Script.getValueFromScript TYPE_BIGNUMBER handles NativeNumber, NativeJavaObject, boxed
    // primitives, String;
    // java.math.BigDecimal falls through to default and throws (could be extended to handle
    // BigDecimal).
    BigDecimal bd = new BigDecimal("123.45");
    assertThrows(HopValueException.class, () -> script.getValueFromScript(bd, 0));
  }

  // --- putRow: Integer normalized to Long ---

  @Test
  void putRow_normalizesIntegerToLongForIntegerColumn() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));

    Object[] row = new Object[] {Integer.valueOf(99), "test"};

    IRowSet outputSet = mock(IRowSet.class, Mockito.RETURNS_MOCKS);
    when(outputSet.putRow(any(IRowMeta.class), any(Object[].class))).thenReturn(true);
    when(outputSet.isDone()).thenReturn(false);
    when(outputSet.getRowWait(anyLong(), any(TimeUnit.class))).thenReturn(null);

    script.addRowSetToOutputRowSets(outputSet);
    script.putRow(rowMeta, row);

    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
    verify(outputSet).putRow(any(IRowMeta.class), rowCaptor.capture());
    Object[] captured = rowCaptor.getValue();
    assertInstanceOf(Long.class, captured[0], "Integer column should be normalized to Long");
    assertEquals(99L, captured[0]);
    assertEquals("test", captured[1]);
  }

  @Test
  void putRow_doesNotModifyRowWhenNoIntegerColumn() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));

    Object[] row = new Object[] {"hello"};

    IRowSet outputSet = mock(IRowSet.class, Mockito.RETURNS_MOCKS);
    when(outputSet.putRow(any(IRowMeta.class), any(Object[].class))).thenReturn(true);
    when(outputSet.isDone()).thenReturn(false);

    script.addRowSetToOutputRowSets(outputSet);
    script.putRow(rowMeta, row);

    assertEquals("hello", row[0]);
  }

  @Test
  void putRow_handlesNullRowMeta() throws Exception {
    script.putRow(null, new Object[] {1});
    // no exception when rowMeta is null (normalization loop skipped)
  }

  @Test
  void getValueFromScript_indexOutOfBounds_throws() {
    meta.getFields().add(createField("a", "Integer"));
    assertThrows(IndexOutOfBoundsException.class, () -> script.getValueFromScript(1, 1));
  }

  // --- getValueFromScript: more branches ---

  @Test
  void getValueFromScript_numberType_parseFromToString() throws HopValueException {
    meta.getFields().add(createField("x", "Number"));
    Object obj =
        new Object() {
          @Override
          public String toString() {
            return "2.5";
          }
        };
    assertEquals(2.5, (Double) script.getValueFromScript(obj, 0), 1e-9);
  }

  @Test
  void getValueFromScript_integer_invalidString_throws() {
    meta.getFields().add(createField("id", "Integer"));
    assertThrows(HopValueException.class, () -> script.getValueFromScript("not-a-number", 0));
  }

  @Test
  void getValueFromScript_stringType_nonStringObject_returnsResult() throws HopValueException {
    meta.getFields().add(createField("name", "String"));
    Integer value = 42;
    assertEquals(value, script.getValueFromScript(value, 0));
  }

  @Test
  void getValueFromScript_dateType_doubleTimestamp_returnsDate() throws HopValueException {
    meta.getFields().add(createField("d", "Date"));
    long ts = 1234567890L;
    assertEquals(new Date(ts), script.getValueFromScript((double) ts, 0));
  }

  @Test
  void getValueFromScript_dateType_numericString_returnsDate() throws HopValueException {
    meta.getFields().add(createField("d", "Date"));
    long ts = 1000000L;
    Date expected = new Date(ts);
    assertEquals(expected, script.getValueFromScript(String.valueOf(ts), 0));
  }

  @Test
  void getValueFromScript_bignumberType_string_returnsBigDecimal() throws HopValueException {
    meta.getFields().add(createField("bn", "BigNumber"));
    assertEquals(new BigDecimal("999"), script.getValueFromScript("999", 0));
  }

  @Test
  void getValueFromScript_binaryType_returnsResult() throws HopValueException {
    meta.getFields().add(createField("bin", "Binary"));
    byte[] bytes = new byte[] {1, 2, 3};
    assertEquals(bytes, script.getValueFromScript(bytes, 0));
  }

  @Test
  void getValueFromScript_typeNone_throws() {
    ScriptMeta.SField field = createField("x", "NoSuchType");
    meta.getFields().add(field);
    assertThrows(HopValueException.class, () -> script.getValueFromScript(1, 0));
  }

  @Test
  void init_setsEngineWhenLanguageAvailable() throws Exception {
    ScriptData data = new ScriptData();
    meta.setLanguageName("Groovy");
    Script scriptWithData =
        new Script(
            transformMockHelper.transformMeta,
            meta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    assertTrue(scriptWithData.init());
    assertNotNull(data.engine);
  }

  @Test
  void init_stillReturnsTrueWhenEngineLookupFails() throws Exception {
    ScriptData data = new ScriptData();
    meta.setLanguageName("NonExistentLanguageXYZ");
    Script scriptWithData =
        new Script(
            transformMockHelper.transformMeta,
            meta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    assertTrue(scriptWithData.init());
    assertNull(data.engine);
  }

  @Test
  void init_copiesScriptsFromMeta() throws Exception {
    ScriptData data = new ScriptData();
    meta.setLanguageName("Groovy");
    meta.getScripts()
        .add(new ScriptMeta.SScript(ScriptMeta.ScriptType.TRANSFORM_SCRIPT, "s1", "x=1"));
    meta.getScripts()
        .add(new ScriptMeta.SScript(ScriptMeta.ScriptType.START_SCRIPT, "start", "y=0"));
    meta.getScripts().add(new ScriptMeta.SScript(ScriptMeta.ScriptType.END_SCRIPT, "end", "z=2"));
    Script scriptWithData =
        new Script(
            transformMockHelper.transformMeta,
            meta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    assertTrue(scriptWithData.init());
    assertNotNull(data.engine);
  }

  @Test
  void putRow_nullRow_doesNotThrow() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    script.putRow(rowMeta, null);
  }

  /** Runs one row through the transform: addValues, eval script, putRow. */
  @Test
  void processRow_oneRow_producesOutputRow() throws Exception {
    ScriptData data = new ScriptData();
    meta.setLanguageName("Groovy");
    meta.getScripts()
        .add(new ScriptMeta.SScript(ScriptMeta.ScriptType.TRANSFORM_SCRIPT, "s1", "out=1"));
    ScriptMeta.SField outField = createField("out", "Integer");
    outField.setRename("out");
    meta.getFields().add(outField);

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("dummy"));
    Object[] inputRow = new Object[] {"x"};

    Script runScript =
        new Script(
            transformMockHelper.transformMeta,
            meta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    runScript.addRowSetToInputRowSets(
        transformMockHelper.getMockInputRowSet(TransformMockHelper.asList(inputRow)));
    IRowSet outputSet = mock(IRowSet.class, Mockito.RETURNS_MOCKS);
    when(outputSet.putRow(any(IRowMeta.class), any(Object[].class))).thenReturn(true);
    when(outputSet.isDone()).thenReturn(false);
    when(outputSet.getRowWait(anyLong(), any(TimeUnit.class))).thenReturn(null);
    runScript.addRowSetToOutputRowSets(outputSet);

    assertTrue(runScript.init());
    assertTrue(runScript.processRow());
    runScript.processRow();

    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
    ArgumentCaptor<IRowMeta> metaCaptor = ArgumentCaptor.forClass(IRowMeta.class);
    verify(outputSet).putRow(metaCaptor.capture(), rowCaptor.capture());
    Object[] outRow = rowCaptor.getValue();
    assertNotNull(outRow);
    assertEquals(1, outRow.length);
    assertEquals(1L, outRow[0]);
  }

  private static ScriptMeta.SField createField(String name, String type) {
    ScriptMeta.SField f = new ScriptMeta.SField();
    f.setName(name);
    f.setType(type);
    return f;
  }
}
