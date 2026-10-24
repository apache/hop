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

package org.apache.hop.pipeline.transforms.metainject;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.inject.HopMetadataInjector;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

class MetaInjectTest {

  private static final String INJECTOR_TRANSFORM_NAME = "TEST_TRANSFORM_FOR_INJECTION";
  private static final String TEST_VALUE = "TEST_VALUE";
  private static final String TEST_TARGET_TRANSFORM_NAME = "TEST_TARGET_TRANSFORM_NAME";
  private static final String TEST_SOURCE_TRANSFORM_NAME = "TEST_SOURCE_TRANSFORM_NAME";
  private static final String TEST_FIELD = "TEST_FIELD";
  private MetaInject metaInject;
  private PipelineMeta pipelineMeta;

  @BeforeEach
  void before() throws Exception {
    pipelineMeta = Mockito.spy(new PipelineMeta());
    MetaInjectData data = new MetaInjectData();
    data.pipelineMeta = pipelineMeta;
    metaInject =
        TransformMockUtil.getTransform(
            MetaInject.class, MetaInjectMeta.class, MetaInjectData.class, "MetaInjectTest");
    metaInject = Mockito.spy(metaInject);
    metaInject.init();
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);
    metaInject.setMetadataProvider(metadataProvider);
    doReturn(pipelineMeta).when(metaInject).getPipelineMeta();

    PipelineMeta internalPipelineMeta = mock(PipelineMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);
    Pipeline pipeline = new LocalPipelineEngine();
    pipeline.setLogChannel(LogChannel.GENERAL);
    pipeline = Mockito.spy(pipeline);
    doReturn(pipeline).when(metaInject).getPipeline();
    doReturn(INJECTOR_TRANSFORM_NAME).when(transformMeta).getName();
    doReturn(Collections.singletonList(transformMeta))
        .when(internalPipelineMeta)
        .getUsedTransforms();
    ITransformMeta iTransformMeta = mock(ITransformMeta.class);
    doReturn(iTransformMeta).when(transformMeta).getTransform();
    doReturn(internalPipelineMeta).when(metaInject).loadPipelineMeta();
  }

  @Test
  void convertToUpperCaseSet_null_array() {
    Set<String> actualResult = MetaInject.convertToUpperCaseSet(null);
    assertNotNull(actualResult);
    assertTrue(actualResult.isEmpty());
  }

  @Test
  void convertToUpperCaseSet() {
    String[] input = new String[] {"Test_Transform", "test_transform1"};
    Set<String> actualResult = MetaInject.convertToUpperCaseSet(input);
    Set<String> expectedResult = new HashSet<>();
    expectedResult.add("TEST_TRANSFORM");
    expectedResult.add("TEST_TRANSFORM1");
    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testTransformChangeListener() {
    MetaInjectMeta mim = new MetaInjectMeta();
    TransformMeta sm = new TransformMeta("testTransform", mim);
    try {
      pipelineMeta.addOrReplaceTransform(sm);
    } catch (Exception ex) {
      fail();
    }
  }

  /**
   * Regression test for <a href="https://github.com/apache/hop/issues/7246">#7246</a>: a constant
   * value (a mapping without a source transform) must be injected into a legacy
   * {@code @InjectionSupported} template transform.
   */
  @Test
  void newInjectionConstants_injectsConstantWithoutSourceTransform() throws HopException {
    InjectableTestTransformMeta targetMeta = new InjectableTestTransformMeta();

    // A "constant" mapping has no source transform; the literal value to inject is held in the
    // source field of the mapping.
    MetaInjectMapping constantMapping = new MetaInjectMapping();
    constantMapping.setTargetTransformName(TEST_TARGET_TRANSFORM_NAME);
    constantMapping.setTargetAttributeKey("THERE");
    constantMapping.setSourceTransformName(null);
    constantMapping.setSourceField(TEST_VALUE);

    when(metaInject.getMeta().getMappings()).thenReturn(Collections.singletonList(constantMapping));

    metaInject.newInjectionConstants(metaInject, TEST_TARGET_TRANSFORM_NAME, targetMeta);

    assertEquals(TEST_VALUE, targetMeta.there);
  }

  /**
   * A mapping that streams from a source transform is handled by {@code newInjection()} and must
   * not be treated as a constant here. Otherwise the source field <em>name</em> would be injected
   * as a literal value, corrupting the streamed injection.
   */
  @Test
  void newInjectionConstants_ignoresMappingWithSourceTransform() throws Exception {
    InjectableTestTransformMeta targetMeta = new InjectableTestTransformMeta();

    MetaInjectMapping streamedMapping = new MetaInjectMapping();
    streamedMapping.setTargetTransformName(TEST_TARGET_TRANSFORM_NAME);
    streamedMapping.setTargetAttributeKey("THERE");
    streamedMapping.setSourceTransformName(TEST_SOURCE_TRANSFORM_NAME);
    streamedMapping.setSourceField(TEST_FIELD);

    when(metaInject.getMeta().getMappings()).thenReturn(Collections.singletonList(streamedMapping));

    metaInject.newInjectionConstants(metaInject, TEST_TARGET_TRANSFORM_NAME, targetMeta);

    assertNull(targetMeta.there);
  }

  /**
   * Regression test: a constant value containing variables must be resolved before it is injected
   * into a {@code @HopMetadataProperty} template transform. The legacy {@code @InjectionSupported}
   * path ({@link MetaInject#newInjectionConstants}) always resolved them; the rewritten
   * scalar-constant path did not, so variables leaked through unresolved (worked in 2.17).
   */
  @Test
  void collectInjectionKeyValue_resolvesVariablesInConstant() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("MY_VAR", "resolved-value");

    // A "constant" mapping: no source transform, the literal value sits in the source field.
    MetaInjectMapping mapping = new MetaInjectMapping();
    mapping.setTargetAttributeKey("SOME_KEY");
    mapping.setSourceTransformName(null);
    mapping.setSourceField("${MY_VAR}");

    Map<String, Object> injectionKeyData = new HashMap<>();
    MetaInject.collectInjectionKeyValue(variables, mapping, null, injectionKeyData);

    assertEquals("resolved-value", injectionKeyData.get("SOME_KEY"));
  }

  /**
   * A metadata-injection mapping written before {@link ConditionStreamBehavior} existed targets the
   * legacy {@code ALLOW_EMPTY_STREAM_ON_EXECUTION} injection key and feeds it a boolean-ish value.
   * Such a mapping must still work: the key is remapped to {@code CONDITION_STREAM_BEHAVIOR} and
   * the boolean is translated to the equivalent enum code (as seen by {@link
   * ConditionStreamBehavior#fromLegacyBoolean}).
   *
   * <p>This verifies the constant-injection path (no source transform).
   */
  @ParameterizedTest
  @CsvSource({
    // legacy boolean value -> expected enum code under CONDITION_STREAM_BEHAVIOR
    "Y, at-least-one-non-empty",
    "true, at-least-one-non-empty",
    "TRUE, at-least-one-non-empty",
    "yes, at-least-one-non-empty",
    "N, all-non-empty",
    "false, all-non-empty"
  })
  void legacyInjectionKeyConstantRemappedToEnumCode(String legacyValue, String expectedCode)
      throws Exception {
    Variables variables = new Variables();

    MetaInjectMapping mapping = new MetaInjectMapping();
    mapping.setTargetAttributeKey("ALLOW_EMPTY_STREAM_ON_EXECUTION");
    mapping.setSourceTransformName(null);
    mapping.setSourceField(legacyValue);

    Map<String, Object> injectionKeyData = new HashMap<>();
    MetaInject.collectInjectionKeyValue(variables, mapping, null, injectionKeyData);

    // The legacy key is remapped to the current one, carrying the enum code.
    assertNull(injectionKeyData.get("ALLOW_EMPTY_STREAM_ON_EXECUTION"));
    assertEquals(expectedCode, injectionKeyData.get("CONDITION_STREAM_BEHAVIOR"));
  }

  /**
   * Same backward-compatibility remapping as {@link
   * #legacyInjectionKeyConstantRemappedToEnumCode(String, String)} but for the streamed path, where
   * the value arrives as a typed boolean in a source row.
   */
  @ParameterizedTest
  @CsvSource({"true, at-least-one-non-empty", "false, all-non-empty"})
  void legacyInjectionKeyStreamedRemappedToEnumCode(String legacyValue, String expectedCode)
      throws Exception {
    Variables variables = new Variables();

    MetaInjectMapping mapping = new MetaInjectMapping();
    mapping.setTargetAttributeKey("ALLOW_EMPTY_STREAM_ON_EXECUTION");
    mapping.setSourceTransformName(TEST_SOURCE_TRANSFORM_NAME);
    mapping.setSourceField(TEST_FIELD);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString(TEST_FIELD));
    RowMetaAndData sourceRow = new RowMetaAndData(rowMeta, legacyValue);
    List<RowMetaAndData> sourceRows = Collections.singletonList(sourceRow);

    Map<String, Object> injectionKeyData = new HashMap<>();
    MetaInject.collectInjectionKeyValue(variables, mapping, sourceRows, injectionKeyData);

    assertNull(injectionKeyData.get("ALLOW_EMPTY_STREAM_ON_EXECUTION"));
    assertEquals(expectedCode, injectionKeyData.get("CONDITION_STREAM_BEHAVIOR"));
  }

  /**
   * End-to-end backward-compatibility check: a mapping that injects the legacy {@code
   * ALLOW_EMPTY_STREAM_ON_EXECUTION} key into a real {@link MetaInjectMeta} target produces the
   * expected {@link ConditionStreamBehavior}. This mirrors what {@code
   * injectMetadataIntoTemplateTransform} does via {@link HopMetadataInjector#inject}.
   */
  @ParameterizedTest
  @CsvSource({
    "Y, AT_LEAST_ONE_NON_EMPTY",
    "true, AT_LEAST_ONE_NON_EMPTY",
    "N, ALL_NON_EMPTY",
    "false, ALL_NON_EMPTY"
  })
  void legacyInjectionKeyIntoMetaInjectMetaSetsEnum(String legacyValue, String expectedEnum)
      throws Exception {
    MetaInjectMeta target = new MetaInjectMeta();

    MetaInjectMapping mapping = new MetaInjectMapping();
    mapping.setTargetAttributeKey("ALLOW_EMPTY_STREAM_ON_EXECUTION");
    mapping.setSourceTransformName(null);
    mapping.setSourceField(legacyValue);

    Map<String, Object> injectionKeyData = new HashMap<>();
    MetaInject.collectInjectionKeyValue(new Variables(), mapping, null, injectionKeyData);

    HopMetadataInjector.inject(null, target, injectionKeyData, new HashMap<>());

    assertEquals(
        ConditionStreamBehavior.valueOf(expectedEnum), target.getConditionStreamBehavior());
  }

  /**
   * The reviewer's second legacy scenario: the mapping key was already renamed to the new {@code
   * CONDITION_STREAM_BEHAVIOR} but the upstream source still flows a legacy boolean value ({@code
   * Y}/{@code N}/{@code true}/{@code false}). Such a value used to be rejected by the generic
   * {@code convertCodeToEnumeration}; it must now be translated to the enum code so the injection
   * still lands on the right enum. This is the constant (no source transform) variant.
   */
  @ParameterizedTest
  @CsvSource({
    "Y, AT_LEAST_ONE_NON_EMPTY",
    "true, AT_LEAST_ONE_NON_EMPTY",
    "N, ALL_NON_EMPTY",
    "false, ALL_NON_EMPTY"
  })
  void newKeyWithLegacyBooleanValueStillInjectsEnum(String legacyValue, String expectedEnum)
      throws Exception {
    MetaInjectMeta target = new MetaInjectMeta();

    MetaInjectMapping mapping = new MetaInjectMapping();
    mapping.setTargetAttributeKey("CONDITION_STREAM_BEHAVIOR");
    mapping.setSourceTransformName(null);
    mapping.setSourceField(legacyValue);

    Map<String, Object> injectionKeyData = new HashMap<>();
    MetaInject.collectInjectionKeyValue(new Variables(), mapping, null, injectionKeyData);

    HopMetadataInjector.inject(null, target, injectionKeyData, new HashMap<>());

    assertEquals(
        ConditionStreamBehavior.valueOf(expectedEnum), target.getConditionStreamBehavior());
  }

  /**
   * Full runtime data-flow backward-compatibility check: a running pipeline whose MetaInject source
   * transforms produce rows, and whose mapping targets the legacy {@code
   * ALLOW_EMPTY_STREAM_ON_EXECUTION} injection key, must still inject the value into the template's
   * MetaInjectMeta. The flowing source rows sit in {@code data.rowMap} and are picked up by {@code
   * collectDataForOneMapping} exactly like {@code injectMetadataIntoTemplateTransform} does; the
   * legacy key is remapped so {@code HopMetadataInjector.inject} lands on the current enum.
   */
  @ParameterizedTest
  @CsvSource({
    // streamedLegacyValue, expectedEnum
    "Y, AT_LEAST_ONE_NON_EMPTY",
    "true, AT_LEAST_ONE_NON_EMPTY",
    "N, ALL_NON_EMPTY",
    "false, ALL_NON_EMPTY"
  })
  void legacyInjectionKeyFlowsThroughPipelineData(String streamedLegacyValue, String expectedEnum)
      throws Exception {
    // A real source transform feeds one flowing row: the legacy boolean value in "legacyFlag".
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("legacyFlag"));
    RowMetaAndData sourceRow = new RowMetaAndData(rowMeta, streamedLegacyValue);
    metaInject.getData().rowMap = new HashMap<>();
    metaInject
        .getData()
        .rowMap
        .put(TEST_SOURCE_TRANSFORM_NAME, Collections.singletonList(sourceRow));

    // The pipeline mapping still targets the legacy injection key that existed before the enum.
    MetaInjectMapping mapping = new MetaInjectMapping();
    mapping.setTargetTransformName(TEST_TARGET_TRANSFORM_NAME);
    mapping.setTargetAttributeKey("ALLOW_EMPTY_STREAM_ON_EXECUTION");
    mapping.setSourceTransformName(TEST_SOURCE_TRANSFORM_NAME);
    mapping.setSourceField("legacyFlag");
    when(metaInject.getMeta().getMappings()).thenReturn(Collections.singletonList(mapping));

    // This mirrors the head of injectMetadataIntoTemplateTransform: collect then inject.
    MetaInjectMeta target = new MetaInjectMeta();
    Map<String, Set<String>> injectionKeyGroupMap =
        HopMetadataInjector.findInjectionGroupKeys(MetaInjectMeta.class);
    Map<String, Object> injectionKeyData = new HashMap<>();
    metaInject.collectDataForOneMapping(
        TEST_TARGET_TRANSFORM_NAME,
        mapping,
        injectionKeyGroupMap,
        new HashMap<>(),
        new HashMap<>(),
        injectionKeyData,
        MetaInjectMeta.class);

    assertNull(injectionKeyData.get("ALLOW_EMPTY_STREAM_ON_EXECUTION"));
    assertEquals("CONDITION_STREAM_BEHAVIOR", injectionKeyData.keySet().iterator().next());

    HopMetadataInjector.inject(null, target, injectionKeyData, new HashMap<>());

    assertEquals(
        ConditionStreamBehavior.valueOf(expectedEnum), target.getConditionStreamBehavior());
  }

  /**
   * A saved mapping that still targets the legacy {@code ALLOW_EMPTY_STREAM_ON_EXECUTION} injection
   * key must NOT be reported as missing by {@link MetaInject#getUnavailableTargetKeys}. That method
   * consults the {@code BeanInjectionInfo} property map for {@code CONDITION_STREAM_BEHAVIOR}; the
   * legacy key is resolved to it, so the dialog neither hides the mapping nor offers to delete it.
   */
  @Test
  void getUnavailableTargetKeys_legacyKeyNotReportedMissing() {
    // The injected pipeline holds a single MetaInject transform, so the legacy injection key is
    // looked up against its current CONDITION_STREAM_BEHAVIOR property.
    MetaInjectMeta targetMeta = new MetaInjectMeta();
    TransformMeta targetTransform = new TransformMeta(TEST_TARGET_TRANSFORM_NAME, targetMeta);
    PipelineMeta injectedPipeline = new PipelineMeta();
    injectedPipeline.addTransform(targetTransform);

    MetaInjectMapping legacy = new MetaInjectMapping();
    legacy.setTargetTransformName(TEST_TARGET_TRANSFORM_NAME);
    legacy.setTargetAttributeKey("ALLOW_EMPTY_STREAM_ON_EXECUTION"); // pre-enum key
    legacy.setSourceTransformName(null);
    legacy.setSourceField("Y");

    Set<MetaInjectMapping> missing =
        MetaInject.getUnavailableTargetKeys(
            Collections.singletonList(legacy), injectedPipeline, Collections.emptySet());

    // The legacy key resolves to the current property, so nothing is reported missing.
    assertTrue(missing.isEmpty());
  }

  /**
   * Older pipelines may store a scalar key with {@code target_detail=Y}. {@link
   * MetaInject#collectDataForOneMapping} must resolve the legacy {@code
   * ALLOW_EMPTY_STREAM_ON_EXECUTION} key before {@code isTopLevelInjectionKey}, otherwise the
   * target_detail branch logs {@code TargetKeyNotFound} and leaves the template on {@code
   * ALL_NON_EMPTY}. With the fix the value is collected under {@code CONDITION_STREAM_BEHAVIOR}.
   */
  @ParameterizedTest
  @CsvSource({
    // streamedLegacyValue, expectedEnum
    "Y, AT_LEAST_ONE_NON_EMPTY",
    "N, ALL_NON_EMPTY"
  })
  void collectDataForOneMapping_legacyKeyTargetDetailY_collects(
      String streamedLegacyValue, String expectedEnum) throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("legacyFlag"));
    RowMetaAndData sourceRow = new RowMetaAndData(rowMeta, streamedLegacyValue);
    metaInject.getData().rowMap = new HashMap<>();
    metaInject
        .getData()
        .rowMap
        .put(TEST_SOURCE_TRANSFORM_NAME, Collections.singletonList(sourceRow));

    MetaInjectMapping mapping = new MetaInjectMapping();
    mapping.setTargetTransformName(TEST_TARGET_TRANSFORM_NAME);
    mapping.setTargetAttributeKey("ALLOW_EMPTY_STREAM_ON_EXECUTION");
    mapping.setTargetDetail(true); // older pipeline stores a scalar key with target_detail=Y
    mapping.setSourceTransformName(TEST_SOURCE_TRANSFORM_NAME);
    mapping.setSourceField("legacyFlag");

    Map<String, Set<String>> injectionKeyGroupMap =
        HopMetadataInjector.findInjectionGroupKeys(MetaInjectMeta.class);
    Map<String, Object> injectionKeyData = new HashMap<>();
    metaInject.collectDataForOneMapping(
        TEST_TARGET_TRANSFORM_NAME,
        mapping,
        injectionKeyGroupMap,
        new HashMap<>(),
        new HashMap<>(),
        injectionKeyData,
        MetaInjectMeta.class);

    // The target_detail=Y scalar path must land on CONDITION_STREAM_BEHAVIOR, not be warned away.
    assertNull(injectionKeyData.get("ALLOW_EMPTY_STREAM_ON_EXECUTION"));
    assertEquals("CONDITION_STREAM_BEHAVIOR", injectionKeyData.keySet().iterator().next());
    assertEquals(
        ConditionStreamBehavior.valueOf(expectedEnum).getCode(),
        injectionKeyData.get("CONDITION_STREAM_BEHAVIOR"));
  }

  /**
   * Same regression as {@link #collectInjectionKeyValue_resolvesVariablesInConstant()} but for a
   * constant that belongs to an injection group (list) key.
   */
  @Test
  void addConstantToGroupData_resolvesVariablesInConstant() {
    Variables variables = new Variables();
    variables.setVariable("MY_VAR", "resolved-value");

    MetaInjectMapping mapping = new MetaInjectMapping();
    mapping.setTargetAttributeKey("SOME_KEY");
    mapping.setSourceField("${MY_VAR}");

    Map<String, RowBuffer> injectionGroupData = new HashMap<>();
    MetaInject.addConstantToGroupData(variables, mapping, injectionGroupData, "GROUP");

    RowBuffer buffer = injectionGroupData.get("GROUP");
    assertNotNull(buffer);
    assertEquals("resolved-value", buffer.getBuffer().getFirst()[0]);
  }

  /**
   * Regression test: a mapping to a target key that no longer exists in the template transform
   * (e.g. an option removed in a newer Hop version, such as CSV Input's {@code INPUT_IF_NULL}) must
   * NOT fail the whole injection. The mapping is logged as a warning and skipped, so a pipeline
   * built against an older Hop version keeps running. Before the fix this threw a
   * HopTransformException and the transform aborted.
   */
  @Test
  void collectDataForOneMapping_unknownTargetKey_warnsInsteadOfThrowing() {
    MetaInjectMapping mapping = new MetaInjectMapping();
    mapping.setTargetTransformName(TEST_TARGET_TRANSFORM_NAME);
    mapping.setTargetAttributeKey("INPUT_IF_NULL"); // a key that no longer exists in the target
    mapping.setTargetDetail(true);
    mapping.setSourceTransformName(null);
    mapping.setSourceField("anything");

    Map<String, Set<String>> injectionKeyGroupMap = new HashMap<>(); // key is in no group
    Map<String, List<MetaInject.GroupColumn>> groupColumnsMap = new HashMap<>();
    Map<String, RowBuffer> injectionGroupData = new HashMap<>();
    Map<String, Object> injectionKeyData = new HashMap<>();

    metaInject.getData().rowMap = new HashMap<>();

    assertDoesNotThrow(
        () ->
            metaInject.collectDataForOneMapping(
                TEST_TARGET_TRANSFORM_NAME,
                mapping,
                injectionKeyGroupMap,
                groupColumnsMap,
                injectionGroupData,
                injectionKeyData,
                InjectableTestTransformMeta.class));

    // The unknown key is skipped: nothing is collected for injection.
    assertTrue(injectionKeyData.isEmpty());
    assertTrue(injectionGroupData.isEmpty());
  }

  /**
   * A mapping saved before the {@code ALLOW_EMPTY_STREAM_ON_EXECUTION} injection key was renamed
   * still stores the legacy key. When {@code enterMapping} pre-selects it in the target rows it
   * must normalize that key (via {@link MetaInjectDialog#resolveTargetAttributeKey}) to {@code
   * CONDITION_STREAM_BEHAVIOR}, the key {@code BeanInjectionInfo} reports for the current target
   * row; otherwise {@code targetIndex == -1} and confirming the dialog drops a mapping execution
   * would have honored.
   */
  @Test
  void resolveTargetAttributeKey_legacyKeyNormalizedToCurrentRowKey() {
    MetaInjectMapping legacy = new MetaInjectMapping();
    legacy.setTargetAttributeKey("ALLOW_EMPTY_STREAM_ON_EXECUTION");

    // The normalized key is the one the target rows are built from, so the lookup matches.
    assertEquals("CONDITION_STREAM_BEHAVIOR", MetaInjectDialog.resolveTargetAttributeKey(legacy));
  }

  /**
   * Same normalization check but the other direction: a mapping that already stores the current
   * key, or any ordinary target key, is left untouched so non-legacy keys still resolve exactly.
   */
  @Test
  void resolveTargetAttributeKey_currentOrOtherKeysUnchanged() {
    MetaInjectMapping current = new MetaInjectMapping();
    current.setTargetAttributeKey("CONDITION_STREAM_BEHAVIOR");
    assertEquals("CONDITION_STREAM_BEHAVIOR", MetaInjectDialog.resolveTargetAttributeKey(current));

    MetaInjectMapping ordinary = new MetaInjectMapping();
    ordinary.setTargetAttributeKey("SOME_KEY");
    assertEquals("SOME_KEY", MetaInjectDialog.resolveTargetAttributeKey(ordinary));
  }

  /**
   * Regression test for <a href="https://github.com/apache/hop/issues/7246">#7246</a>: when an
   * injection group mixes a streamed key and a constant key, the constant value must be merged into
   * every streamed row instead of being dropped when the streamed buffer is stored.
   */
  @Test
  void mergeConstantsIntoGroupBuffer_addsConstantToEveryStreamedRow() {
    RowMeta streamedMeta = new RowMeta();
    streamedMeta.addValueMeta(new ValueMetaString("FIELD_NAME"));
    List<Object[]> streamedRows = new ArrayList<>();
    streamedRows.add(new Object[] {"x"});
    streamedRows.add(new Object[] {"y"});
    RowBuffer streamed = new RowBuffer(streamedMeta, streamedRows);

    RowMeta constantMeta = new RowMeta();
    constantMeta.addValueMeta(new ValueMetaString("REPLACE_VALUE"));
    List<Object[]> constantRows = new ArrayList<>();
    constantRows.add(new Object[] {"INJECTED"});
    RowBuffer constant = new RowBuffer(constantMeta, constantRows);

    MetaInject.mergeConstantsIntoGroupBuffer(streamed, constant);

    // The constant column is appended to the metadata...
    assertEquals(2, streamed.getRowMeta().size());
    assertEquals("FIELD_NAME", streamed.getRowMeta().getValueMeta(0).getName());
    assertEquals("REPLACE_VALUE", streamed.getRowMeta().getValueMeta(1).getName());
    // ...and the constant value is present in every streamed row. (Hop over-allocates the row
    // array, so we check the cells by index rather than comparing the whole array.)
    assertEquals(2, streamed.getBuffer().size());
    assertEquals("x", streamed.getBuffer().get(0)[0]);
    assertEquals("INJECTED", streamed.getBuffer().get(0)[1]);
    assertEquals("y", streamed.getBuffer().get(1)[0]);
    assertEquals("INJECTED", streamed.getBuffer().get(1)[1]);
  }

  /**
   * Regression test for the multi-data-grid scenario reported on top of <a
   * href="https://github.com/apache/hop/issues/7246">#7246</a>: a single injection group of a
   * {@code @HopMetadataProperty} template transform (e.g. the Select Values "fields" group) can be
   * fed from two <em>different</em> source transforms - FIELD_NAME from one data grid, FIELD_RENAME
   * from another. Every mapping must resolve its own source field and the columns must be zipped
   * together by row index. The rewrite used to cache the group row layout from the first source
   * only, so the second source's field was looked up in the wrong layout and injection failed with
   * "... to inject could not be found". This worked in 2.17 (legacy BeanInjector path).
   *
   * <p>This mirrors {@code injectMetadataIntoTemplateTransform}, which calls {@code
   * collectDataForOneMappingGroup} once per mapping and then {@code buildGroupRowBuffer} to
   * materialize the group.
   */
  @Test
  void groupFedFromTwoSourcesZipsBothColumns() {
    String groupKey = "FIELDS";

    // FIELD_NAME is injected from the "field names" data grid (column "name").
    IRowMeta namesMeta = new RowMeta();
    namesMeta.addValueMeta(new ValueMetaString("name"));
    List<RowMetaAndData> nameRows = new ArrayList<>();
    nameRows.add(new RowMetaAndData(namesMeta, "field1"));
    nameRows.add(new RowMetaAndData(namesMeta, "field2"));

    MetaInjectMapping nameMapping = new MetaInjectMapping();
    nameMapping.setTargetTransformName("Select values");
    nameMapping.setTargetAttributeKey("FIELD_NAME");
    nameMapping.setTargetDetail(true);
    nameMapping.setSourceTransformName("field names");
    nameMapping.setSourceField("name");

    // FIELD_RENAME is injected from a *different* data grid ("field renames", column "newname").
    IRowMeta renamesMeta = new RowMeta();
    renamesMeta.addValueMeta(new ValueMetaString("newname"));
    List<RowMetaAndData> renameRows = new ArrayList<>();
    renameRows.add(new RowMetaAndData(renamesMeta, "renamed1"));
    renameRows.add(new RowMetaAndData(renamesMeta, "renamed2"));

    MetaInjectMapping renameMapping = new MetaInjectMapping();
    renameMapping.setTargetTransformName("Select values");
    renameMapping.setTargetAttributeKey("FIELD_RENAME");
    renameMapping.setTargetDetail(true);
    renameMapping.setSourceTransformName("field renames");
    renameMapping.setSourceField("newname");

    Map<String, List<RowMetaAndData>> rowMap = new HashMap<>();
    rowMap.put("field names", nameRows);
    rowMap.put("field renames", renameRows);

    // Both keys belong to the same injection group, so they accumulate into the same column list.
    Map<String, List<MetaInject.GroupColumn>> groupColumnsMap = new HashMap<>();

    // Each mapping must resolve its own source field; neither call may throw "could not be found".
    assertDoesNotThrow(
        () ->
            MetaInject.collectDataForOneMappingGroup(
                nameMapping, groupColumnsMap, groupKey, nameRows));
    assertDoesNotThrow(
        () ->
            MetaInject.collectDataForOneMappingGroup(
                renameMapping, groupColumnsMap, groupKey, renameRows),
        "Injecting a group from a second data grid must not fail with "
            + "'The field ... to inject could not be found'");

    // The two columns, from two different grids, must zip together into the group rows.
    RowBuffer groupBuffer = MetaInject.buildGroupRowBuffer(groupColumnsMap.get(groupKey), rowMap);

    assertEquals(2, groupBuffer.getRowMeta().size());
    assertEquals("FIELD_NAME", groupBuffer.getRowMeta().getValueMeta(0).getName());
    assertEquals("FIELD_RENAME", groupBuffer.getRowMeta().getValueMeta(1).getName());
    assertEquals(2, groupBuffer.getBuffer().size());
    assertEquals("field1", groupBuffer.getBuffer().get(0)[0]);
    assertEquals("renamed1", groupBuffer.getBuffer().get(0)[1]);
    assertEquals("field2", groupBuffer.getBuffer().get(1)[0]);
    assertEquals("renamed2", groupBuffer.getBuffer().get(1)[1]);
  }

  @Test
  void shouldSkipExecution_allNonEmpty() {
    // ALL_NON_EMPTY: run only when every stream has data.
    assertFalse(MetaInject.shouldSkipExecution(true, false, ConditionStreamBehavior.ALL_NON_EMPTY));
    assertTrue(MetaInject.shouldSkipExecution(false, true, ConditionStreamBehavior.ALL_NON_EMPTY));
    assertTrue(MetaInject.shouldSkipExecution(true, true, ConditionStreamBehavior.ALL_NON_EMPTY));
  }

  @Test
  void shouldSkipExecution_atLeastOneNonEmpty() {
    // AT_LEAST_ONE_NON_EMPTY: run when at least one stream has data.
    assertFalse(
        MetaInject.shouldSkipExecution(
            true, false, ConditionStreamBehavior.AT_LEAST_ONE_NON_EMPTY));
    assertFalse(
        MetaInject.shouldSkipExecution(true, true, ConditionStreamBehavior.AT_LEAST_ONE_NON_EMPTY));
    assertTrue(
        MetaInject.shouldSkipExecution(
            false, true, ConditionStreamBehavior.AT_LEAST_ONE_NON_EMPTY));
  }

  @Test
  void shouldSkipExecution_always() {
    // ALWAYS: run even when every stream is empty.
    assertFalse(MetaInject.shouldSkipExecution(false, true, ConditionStreamBehavior.ALWAYS));
    assertFalse(MetaInject.shouldSkipExecution(true, true, ConditionStreamBehavior.ALWAYS));
    assertFalse(MetaInject.shouldSkipExecution(true, false, ConditionStreamBehavior.ALWAYS));
  }

  @InjectionSupported(localizationPrefix = "", groups = "groups")
  private static class InjectableTestTransformMeta
      extends BaseTransformMeta<MetaInject, MetaInjectData> {

    @Injection(name = "THERE")
    private String there;
  }
}
