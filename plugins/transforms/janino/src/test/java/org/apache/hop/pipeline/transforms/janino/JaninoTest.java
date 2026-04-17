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
package org.apache.hop.pipeline.transforms.janino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.Plugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link Janino#processRow()} covering the data-transform path, the {@link
 * JaninoMeta#javaTargetVersion} compilation gate, and error-handling routing.
 */
class JaninoTest {

  private TransformMockHelper<JaninoMeta, JaninoData> helper;

  @BeforeAll
  static void initPlugins() throws Exception {
    HopLogStore.init();
    // Register value meta types needed by the row meta machinery.
    PluginRegistry registry = PluginRegistry.getInstance();
    String[] valueMetaClasses = {
      ValueMetaString.class.getName(),
      ValueMetaInteger.class.getName(),
      ValueMetaDate.class.getName(),
      ValueMetaNumber.class.getName()
    };
    for (String cls : valueMetaClasses) {
      registry.registerPluginClass(cls, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
    // Register the Janino transform as a native plugin so getClassLoader() returns a usable
    // loader without triggering the full classpath scan that HopEnvironment.init() does.
    Plugin janinoPlugin =
        new Plugin(
            new String[] {"Janino"},
            TransformPluginType.class,
            Janino.class,
            "Scripting",
            "User Defined Java Expression",
            "",
            "janino.svg",
            false,
            true, // native plugin → getClassLoader() returns registry's own loader
            Map.of(Janino.class, Janino.class.getName()),
            Collections.emptyList(),
            null,
            new String[0],
            null,
            false);
    registry.registerPlugin(TransformPluginType.class, janinoPlugin);
  }

  @BeforeEach
  void setUp() {
    helper = new TransformMockHelper<>("Janino TEST", JaninoMeta.class, JaninoData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  // ------------------------------------------------------------------ helpers

  private Janino buildSpy(JaninoMeta meta) {
    return Mockito.spy(
        new Janino(
            helper.transformMeta, meta, new JaninoData(), 0, helper.pipelineMeta, helper.pipeline));
  }

  private static JaninoMetaFunction intFn(String name, String formula) {
    JaninoMetaFunction fn = new JaninoMetaFunction();
    fn.setFieldName(name);
    fn.setFormula(formula);
    fn.setValueType(IValueMeta.TYPE_INTEGER);
    fn.setValueLength(-1);
    fn.setValuePrecision(-1);
    return fn;
  }

  private static JaninoMetaFunction strFn(String name, String formula) {
    JaninoMetaFunction fn = new JaninoMetaFunction();
    fn.setFieldName(name);
    fn.setFormula(formula);
    fn.setValueType(IValueMeta.TYPE_STRING);
    fn.setValueLength(-1);
    fn.setValuePrecision(-1);
    return fn;
  }

  /** Wire a single output IRowSet and return it (for result capture). */
  private static IRowSet attachOutputRowSet(Janino janino) {
    IRowSet output = mock(IRowSet.class, Mockito.RETURNS_MOCKS);
    when(output.putRow(any(IRowMeta.class), any(Object[].class))).thenReturn(true);
    when(output.isDone()).thenReturn(false);
    when(output.getRowWait(any(Long.class), any(TimeUnit.class))).thenReturn(null);
    janino.addRowSetToOutputRowSets(output);
    return output;
  }

  // ------------------------------------------------------------------ processRow: no input

  @Test
  void processRow_noInput_returnsFalse() throws HopException {
    JaninoMeta meta = new JaninoMeta();
    Janino janino = buildSpy(meta);
    doReturn(null).when(janino).getRow();

    assertFalse(janino.processRow());
  }

  // ------------------------------------------------------------------ processRow: arithmetic

  @Test
  void processRow_integerArithmetic_appendsLongResult() throws HopException {
    JaninoMeta meta = new JaninoMeta();
    meta.getFunctions().add(intFn("result", "1 + 2"));

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("dummy"));

    Janino janino = buildSpy(meta);
    doReturn(new Object[] {"x"}).doReturn(null).when(janino).getRow();
    doReturn(inputMeta).when(janino).getInputRowMeta();

    IRowSet output = attachOutputRowSet(janino);
    janino.init();
    assertTrue(janino.processRow());
    janino.processRow(); // drain (null row)

    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
    verify(output).putRow(any(IRowMeta.class), rowCaptor.capture());
    Object[] row = rowCaptor.getValue();
    assertNotNull(row);
    assertEquals("x", row[0]);
    assertEquals(3L, row[1]); // Integer 3 promoted to Long
  }

  // ------------------------------------------------------------------ processRow: String result

  @Test
  void processRow_stringResult_appendsString() throws HopException {
    JaninoMeta meta = new JaninoMeta();
    meta.getFunctions().add(strFn("greeting", "\"hello\""));

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("id"));

    Janino janino = buildSpy(meta);
    doReturn(new Object[] {1L}).doReturn(null).when(janino).getRow();
    doReturn(inputMeta).when(janino).getInputRowMeta();

    IRowSet output = attachOutputRowSet(janino);
    janino.init();
    assertTrue(janino.processRow());

    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
    verify(output).putRow(any(IRowMeta.class), rowCaptor.capture());
    assertEquals("hello", rowCaptor.getValue()[1]);
  }

  // ------------------------------------------------------------------ processRow: input field
  // reference

  @Test
  void processRow_referencesInputField_computesCorrectly() throws HopException {
    JaninoMeta meta = new JaninoMeta();
    meta.getFunctions().add(intFn("doubled", "n * 2"));

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("n"));

    Janino janino = buildSpy(meta);
    doReturn(new Object[] {5L}).doReturn(null).when(janino).getRow();
    doReturn(inputMeta).when(janino).getInputRowMeta();

    IRowSet output = attachOutputRowSet(janino);
    janino.init();
    assertTrue(janino.processRow());

    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
    verify(output).putRow(any(IRowMeta.class), rowCaptor.capture());
    assertEquals(10L, rowCaptor.getValue()[1]); // 5 * 2 = 10
  }

  // ------------------------------------------------------------------ processRow: null formula
  // result

  @Test
  void processRow_nullResult_appendsNull() throws HopException {
    JaninoMeta meta = new JaninoMeta();
    JaninoMetaFunction fn = intFn("x", "null");
    fn.setValueType(IValueMeta.TYPE_STRING);
    meta.getFunctions().add(fn);

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("id"));

    Janino janino = buildSpy(meta);
    doReturn(new Object[] {1L}).doReturn(null).when(janino).getRow();
    doReturn(inputMeta).when(janino).getInputRowMeta();

    IRowSet output = attachOutputRowSet(janino);
    janino.init();
    assertTrue(janino.processRow());

    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
    verify(output).putRow(any(IRowMeta.class), rowCaptor.capture());
    // null result at index 1
    assertFalse(rowCaptor.getValue().length == 0);
  }

  // ------------------------------------------------------------------ target version ≥ 8: static
  // interface method

  @Test
  void processRow_target8_staticInterfaceMethod_succeeds() throws HopException {
    JaninoMeta meta = new JaninoMeta();
    meta.setJavaTargetVersion(8);
    // Comparator.naturalOrder() calls a static interface method (Java 8+)
    meta.getFunctions()
        .add(
            intFn(
                "cmp",
                "java.util.Comparator.naturalOrder().compare(Integer.valueOf(5), Integer.valueOf(3))"));

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("id"));

    Janino janino = buildSpy(meta);
    doReturn(new Object[] {1L}).doReturn(null).when(janino).getRow();
    doReturn(inputMeta).when(janino).getInputRowMeta();

    IRowSet output = attachOutputRowSet(janino);
    janino.init();
    assertTrue(janino.processRow());

    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
    verify(output).putRow(any(IRowMeta.class), rowCaptor.capture());
    // naturalOrder().compare(5, 3) > 0
    assertTrue(((Long) rowCaptor.getValue()[1]) > 0);
  }

  // ------------------------------------------------------------------ target version 6: static
  // interface method fails

  @Test
  void processRow_target6_staticInterfaceMethod_throwsHopException() throws HopException {
    JaninoMeta meta = new JaninoMeta();
    meta.setJavaTargetVersion(6); // below 8 — static interface calls blocked by Janino
    meta.getFunctions()
        .add(
            intFn(
                "cmp",
                "java.util.Comparator.naturalOrder().compare(Integer.valueOf(5), Integer.valueOf(3))"));

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("id"));

    Janino janino = buildSpy(meta);
    doReturn(new Object[] {1L}).when(janino).getRow();
    doReturn(inputMeta).when(janino).getInputRowMeta();
    attachOutputRowSet(janino);
    janino.init();

    assertThrows(HopException.class, janino::processRow);
  }

  // ------------------------------------------------------------------ error-handling route

  @Test
  void processRow_badFormula_withErrorHandling_sendsToErrorStream() throws HopException {
    JaninoMeta meta = new JaninoMeta();
    // Empty field name triggers the "Unable to find field name" exception during cook
    JaninoMetaFunction fn = new JaninoMetaFunction();
    fn.setFieldName(""); // blank — triggers HopException in calcFields
    fn.setFormula("1");
    fn.setValueType(IValueMeta.TYPE_INTEGER);
    fn.setValueLength(-1);
    fn.setValuePrecision(-1);
    meta.getFunctions().add(fn);

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("id"));

    Janino janino = buildSpy(meta);
    doReturn(new Object[] {1L}).when(janino).getRow();
    doReturn(inputMeta).when(janino).getInputRowMeta();

    // Enable error handling so the transform routes errors instead of re-throwing.
    // Set up the minimal TransformErrorMeta the BaseTransform.handlePutError path needs.
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
    TransformErrorMeta errorMeta = mock(TransformErrorMeta.class);
    when(helper.transformMeta.getTransformErrorMeta()).thenReturn(errorMeta);
    when(errorMeta.getErrorRowMeta(any())).thenReturn(new RowMeta());

    attachOutputRowSet(janino);
    janino.init();

    // Should return true (row consumed by error routing) rather than throwing HopException
    assertTrue(janino.processRow());
  }

  // ------------------------------------------------------------------ multiple rows

  @Test
  void processRow_multipleRows_computesEachCorrectly() throws HopException {
    JaninoMeta meta = new JaninoMeta();
    meta.getFunctions().add(intFn("sq", "n * n"));

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("n"));

    Janino janino = buildSpy(meta);
    doReturn(new Object[] {2L}).doReturn(new Object[] {3L}).doReturn(null).when(janino).getRow();
    doReturn(inputMeta).when(janino).getInputRowMeta();

    IRowSet output = attachOutputRowSet(janino);
    janino.init();

    assertTrue(janino.processRow()); // row 1
    assertTrue(janino.processRow()); // row 2
    assertFalse(janino.processRow()); // end

    ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
    verify(output, Mockito.times(2)).putRow(any(IRowMeta.class), rowCaptor.capture());

    assertEquals(4L, rowCaptor.getAllValues().get(0)[1]); // 2*2
    assertEquals(9L, rowCaptor.getAllValues().get(1)[1]); // 3*3
  }
}
