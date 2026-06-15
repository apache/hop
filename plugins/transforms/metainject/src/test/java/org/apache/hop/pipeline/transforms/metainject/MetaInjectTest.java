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
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

  @InjectionSupported(localizationPrefix = "", groups = "groups")
  private static class InjectableTestTransformMeta
      extends BaseTransformMeta<MetaInject, MetaInjectData> {

    @Injection(name = "THERE")
    private String there;
  }
}
