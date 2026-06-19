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
package org.apache.hop.pipeline.transforms.javafilter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link JavaFilter#processRow()} covering filter-true, filter-false, field
 * references, non-boolean results and compile failures.
 */
class JavaFilterTest {

  private TransformMockHelper<JavaFilterMeta, JavaFilterData> helper;

  @BeforeAll
  static void initPlugins() throws Exception {
    HopLogStore.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    String[] valueMetaClasses = {
      org.apache.hop.core.row.value.ValueMetaString.class.getName(),
      org.apache.hop.core.row.value.ValueMetaInteger.class.getName(),
      org.apache.hop.core.row.value.ValueMetaDate.class.getName(),
      org.apache.hop.core.row.value.ValueMetaNumber.class.getName()
    };
    for (String cls : valueMetaClasses) {
      registry.registerPluginClass(cls, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>("JavaFilter TEST", JavaFilterMeta.class, JavaFilterData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  // ------------------------------------------------------------------ helpers

  private JavaFilter buildSpy(JavaFilterMeta meta) {
    return Mockito.spy(
        new JavaFilter(
            helper.transformMeta,
            meta,
            new JavaFilterData(),
            0,
            helper.pipelineMeta,
            helper.pipeline));
  }

  private static IRowSet attachOutputRowSet(JavaFilter jf) {
    IRowSet output = mock(IRowSet.class, Mockito.RETURNS_MOCKS);
    when(output.putRow(any(IRowMeta.class), any(Object[].class))).thenReturn(true);
    when(output.isDone()).thenReturn(false);
    when(output.getRowWait(any(Long.class), any(TimeUnit.class))).thenReturn(null);
    jf.addRowSetToOutputRowSets(output);
    return output;
  }

  // ------------------------------------------------------------------ no input

  @Test
  void processRow_noInput_returnsFalse() throws HopException {
    JavaFilterMeta meta = new JavaFilterMeta();
    JavaFilter jf = buildSpy(meta);
    doReturn(null).when(jf).getRow();

    assertFalse(jf.processRow());
  }

  // ------------------------------------------------------------------ constant true / false

  @Test
  void processRow_conditionTrue_rowPassesThrough() throws HopException {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("true");

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("name"));

    JavaFilter jf = buildSpy(meta);
    doReturn(new Object[] {"alice"}).doReturn(null).when(jf).getRow();
    doReturn(inputMeta).when(jf).getInputRowMeta();

    IRowSet output = attachOutputRowSet(jf);
    jf.init();
    assertTrue(jf.processRow());
    jf.processRow(); // drain null

    ArgumentCaptor<Object[]> captor = ArgumentCaptor.forClass(Object[].class);
    verify(output).putRow(any(IRowMeta.class), captor.capture());
    org.junit.jupiter.api.Assertions.assertArrayEquals(new Object[] {"alice"}, captor.getValue());
  }

  @Test
  void processRow_conditionFalse_rowDropped() throws HopException {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("false");

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("name"));

    JavaFilter jf = buildSpy(meta);
    doReturn(new Object[] {"alice"}).doReturn(null).when(jf).getRow();
    doReturn(inputMeta).when(jf).getInputRowMeta();

    IRowSet output = attachOutputRowSet(jf);
    jf.init();
    assertTrue(jf.processRow());
    jf.processRow();

    // No row should reach the output
    verify(output, Mockito.never()).putRow(any(IRowMeta.class), any(Object[].class));
  }

  // ------------------------------------------------------------------ input-field reference

  @Test
  void processRow_conditionUsesInputField_filtersCorrectly() throws HopException {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("amount > 0");

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("amount"));

    JavaFilter jf = buildSpy(meta);
    // Row with amount = 5 → passes; amount = -3 → dropped
    doReturn(new Object[] {5L}).doReturn(new Object[] {-3L}).doReturn(null).when(jf).getRow();
    doReturn(inputMeta).when(jf).getInputRowMeta();

    IRowSet output = attachOutputRowSet(jf);
    jf.init();
    jf.processRow(); // amount=5  → true, sent to output
    jf.processRow(); // amount=-3 → false, dropped
    jf.processRow(); // null      → done

    verify(output, Mockito.times(1)).putRow(any(IRowMeta.class), any(Object[].class));
  }

  // ------------------------------------------------------------------ non-boolean result throws

  @Test
  void processRow_nonBooleanResult_throwsHopException() throws HopException {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("\"not a boolean\"");

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("id"));

    JavaFilter jf = buildSpy(meta);
    doReturn(new Object[] {1L}).when(jf).getRow();
    doReturn(inputMeta).when(jf).getInputRowMeta();

    attachOutputRowSet(jf);
    jf.init();

    assertThrows(HopException.class, jf::processRow);
  }

  // ------------------------------------------------------------------ compile error throws

  @Test
  void processRow_invalidConditionSyntax_throwsHopException() throws HopException {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("this is not valid java at all !!!");

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("id"));

    JavaFilter jf = buildSpy(meta);
    doReturn(new Object[] {1L}).when(jf).getRow();
    doReturn(inputMeta).when(jf).getInputRowMeta();

    attachOutputRowSet(jf);
    jf.init();

    assertThrows(HopException.class, jf::processRow);
  }

  // ------------------------------------------------------------------ multiple rows

  @Test
  void processRow_multipleRows_returnsCorrectly() throws HopException {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("n >= 0");

    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("n"));

    JavaFilter jf = buildSpy(meta);
    doReturn(new Object[] {1L})
        .doReturn(new Object[] {-1L})
        .doReturn(new Object[] {0L})
        .doReturn(null)
        .when(jf)
        .getRow();
    doReturn(inputMeta).when(jf).getInputRowMeta();

    IRowSet output = attachOutputRowSet(jf);
    jf.init();

    assertTrue(jf.processRow()); // n=1  → true
    assertTrue(jf.processRow()); // n=-1 → false (dropped)
    assertTrue(jf.processRow()); // n=0  → true
    assertFalse(jf.processRow()); // null → done

    // Only 2 rows should pass (n=1 and n=0)
    verify(output, Mockito.times(2)).putRow(any(IRowMeta.class), any(Object[].class));
  }
}
