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
package org.apache.hop.pipeline.transforms.valuemapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@MockitoSettings(strictness = Strictness.LENIENT)
class ValueMapperTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<ValueMapperMeta, ValueMapperData> helper;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>("ValueMapperTest", ValueMapperMeta.class, ValueMapperData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    when(helper.transformMeta.isPartitioned()).thenReturn(false);
    helper.pipeline.setVariables(new Variables());
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  private ValueMapper newSpyTransform(ValueMapperMeta meta, ValueMapperData data) {
    ValueMapper transform =
        spy(
            new ValueMapper(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    transform.setMetadataProvider(new MemoryMetadataProvider());
    data.mapValues = new HashMap<>();
    return transform;
  }

  @Test
  void inPlaceEmptyTargetWithDefaultFlagEmitsNull() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType(null);
    Values mapRow = new Values();
    mapRow.setSource("X");
    mapRow.setTarget("");
    meta.getValues().add(mapRow);

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"X"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals(1, outputs.size());
    assertNull(outputs.get(0)[0]);
    assertFalse(vm.processRow());
  }

  @Test
  void inPlaceEmptyTargetWithFlagNoEmitsEmptyString() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType(null);
    Values mapRow = new Values();
    mapRow.setSource("X");
    mapRow.setTarget("");
    mapRow.setEmptyStringEqualsNull(false);
    meta.getValues().add(mapRow);

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"X"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals("", outputs.get(0)[0]);
  }

  @Test
  void emptySourceWithEmptyTargetAndFlagNEmitsEmptyString() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    Values emptyKey = new Values();
    emptyKey.setSource(null);
    emptyKey.setTarget("");
    emptyKey.setEmptyStringEqualsNull(false);
    meta.getValues().add(emptyKey);

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {null}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals("", outputs.get(0)[0]);
  }

  @Test
  void emptySourceMappingWithEmptyTargetAndFlagYEmitsNull() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    Values emptyKey = new Values();
    emptyKey.setSource(null);
    emptyKey.setTarget("");
    meta.getValues().add(emptyKey);

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {null}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertNull(outputs.get(0)[0]);
  }

  @Test
  void inPlaceNoMatchUnsetCompatKeepsSourceWhenNoDefault() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType(null);
    meta.getValues().add(mapping("A", "B"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"Z"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals("Z", outputs.get(0)[0]);
  }

  @Test
  void inPlaceNoMatchExplicitNoEmitsNullWhenNoDefault() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType(null);
    meta.setKeepOriginalValueOnNonMatch(false);
    meta.getValues().add(mapping("A", "B"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"Z"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertNull(outputs.get(0)[0]);
  }

  @Test
  void newFieldNoMatchWithoutDefaultEmitsNull() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("mapped");
    meta.setTargetType(null);
    meta.getValues().add(mapping("A", "B"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"Z"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals("Z", outputs.get(0)[0]);
    assertNull(outputs.get(0)[1]);
  }

  @Test
  void keepOriginalOnNonMatchInPlacePreservesSource() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType(null);
    meta.setKeepOriginalValueOnNonMatch(true);
    meta.getValues().add(mapping("A", "B"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"Z"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals("Z", outputs.get(0)[0]);
  }

  @Test
  void keepOriginalOnNonMatchNewFieldCopiesSource() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("mapped");
    meta.setTargetType(null);
    meta.setKeepOriginalValueOnNonMatch(true);
    meta.getValues().add(mapping("A", "B"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"Z"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals("Z", outputs.get(0)[0]);
    assertEquals("Z", outputs.get(0)[1]);
  }

  @Test
  void keepOriginalIgnoresNonMatchDefaultAtRuntime() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType(null);
    meta.setKeepOriginalValueOnNonMatch(true);
    meta.setNonMatchDefault("SHOULD_NOT_USE");
    meta.getValues().add(mapping("A", "B"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"Z"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals("Z", outputs.get(0)[0]);
  }

  @Test
  void nonMatchDefaultUsedWhenNoMapping() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setNonMatchDefault("DEFAULT");
    Values mapRow = new Values();
    mapRow.setSource("A");
    mapRow.setTarget("B");
    meta.getValues().add(mapRow);

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"Z"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals("DEFAULT", outputs.get(0)[0]);
  }

  @Test
  void newTargetFieldAppendsMappedValue() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("mapped");
    meta.setTargetType(null);
    Values mapRow = new Values();
    mapRow.setSource("A");
    mapRow.setTarget("B");
    meta.getValues().add(mapRow);

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"A"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    Object[] out = outputs.get(0);
    assertEquals("A", out[0]);
    assertEquals("B", out[1]);
  }

  @Test
  void fieldToUseMissingReturnsFalseAndStops() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("missing");
    meta.setTargetField("");
    meta.getValues().add(mapping("A", "B"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"A"}).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    assertFalse(vm.processRow());
    verify(helper.pipeline).stopAll();
  }

  @Test
  void duplicateEmptySourceRowsThrowHopException() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    Values v1 = new Values();
    v1.setSource(null);
    v1.setTarget("a");
    Values v2 = new Values();
    v2.setSource(null);
    v2.setTarget("b");
    meta.getValues().add(v1);
    meta.getValues().add(v2);

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {null}).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    assertThrows(HopException.class, vm::processRow);
  }

  @Test
  void disposeDoesNotThrow() {
    ValueMapperMeta meta = new ValueMapperMeta();
    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    vm.dispose();
  }

  @Test
  void initPreparesMap() {
    ValueMapperMeta meta = new ValueMapperMeta();
    ValueMapperData data = new ValueMapperData();
    ValueMapper vm =
        new ValueMapper(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    assertTrue(vm.init());
    assertTrue(data.mapValues.isEmpty());
  }

  @Test
  void inPlaceIntegerSourceMappedToStringValue() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType(null);
    meta.getValues().add(mapping("1", "one"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("code"));
    doReturn(new Object[] {1L}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals("one", outputs.get(0)[0]);
  }

  @Test
  void invalidNonMatchDefaultForIntegerTargetThrowsHopValueException() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType("Integer");
    meta.setNonMatchDefault("not_an_int");
    meta.getValues().add(mapping("1", "1"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"1"}).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    assertThrows(HopValueException.class, vm::processRow);
  }

  @Test
  void invalidMappedSourceForIntegerFieldThrowsHopValueException() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType("Integer");
    meta.getValues().add(mapping("not_an_int", "1"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaInteger("code"));
    doReturn(new Object[] {1L}).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    assertThrows(HopValueException.class, vm::processRow);
  }

  @Test
  void invalidMappedTargetForIntegerTypeThrowsHopValueException() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType("Integer");
    meta.getValues().add(mapping("A", "not_an_int"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"A"}).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    assertThrows(HopValueException.class, vm::processRow);
  }

  @Test
  void unknownTargetTypeFallsBackToStringMeta() throws Exception {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setFieldToUse("code");
    meta.setTargetField("");
    meta.setTargetType("NotARealHopTypeName");
    meta.getValues().add(mapping("A", "B"));

    ValueMapperData data = new ValueMapperData();
    ValueMapper vm = newSpyTransform(meta, data);
    RowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("code"));
    doReturn(new Object[] {"A"}).doReturn(null).when(vm).getRow();
    doReturn(inputMeta).when(vm).getInputRowMeta();

    List<Object[]> outputs = new ArrayList<>();
    doAnswer(
            inv -> {
              outputs.add(((Object[]) inv.getArgument(1)).clone());
              return null;
            })
        .when(vm)
        .putRow(any(IRowMeta.class), any(Object[].class));

    assertTrue(vm.processRow());
    assertEquals("B", outputs.get(0)[0]);
  }

  private static Values mapping(String source, String target) {
    Values v = new Values();
    v.setSource(source);
    v.setTarget(target);
    return v;
  }
}
