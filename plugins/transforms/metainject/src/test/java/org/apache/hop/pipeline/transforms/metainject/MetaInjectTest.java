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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
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

  private static final String TEST_VARIABLE = "TEST_VARIABLE";

  private static final String TEST_PARAMETER = "TEST_PARAMETER";

  private static final String TEST_TARGET_TRANSFORM_NAME = "TEST_TARGET_TRANSFORM_NAME";

  private static final String TEST_SOURCE_TRANSFORM_NAME = "TEST_SOURCE_TRANSFORM_NAME";

  private static final String TEST_ATTR_VALUE = "TEST_ATTR_VALUE";

  private static final String TEST_FIELD = "TEST_FIELD";

  private static final String UNAVAILABLE_TRANSFORM = "UNAVAILABLE_TRANSFORM";

  private MetaInject metaInject;

  private MetaInjectMeta meta;

  private MetaInjectData data;

  private PipelineMeta pipelineMeta;

  private Pipeline pipeline;

  private IHopMetadataProvider metadataProvider;

  @BeforeEach
  void before() throws Exception {
    pipelineMeta = Mockito.spy(new PipelineMeta());
    meta = new MetaInjectMeta();
    data = new MetaInjectData();
    data.pipelineMeta = pipelineMeta;
    metaInject =
        TransformMockUtil.getTransform(
            MetaInject.class, MetaInjectMeta.class, MetaInjectData.class, "MetaInjectTest");
    metaInject = Mockito.spy(metaInject);
    metaInject.init();
    metadataProvider = mock(IHopMetadataProvider.class);
    metaInject.setMetadataProvider(metadataProvider);
    doReturn(pipelineMeta).when(metaInject).getPipelineMeta();

    PipelineMeta internalPipelineMeta = mock(PipelineMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);
    pipeline = new LocalPipelineEngine();
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
  void testTransformChangeListener() throws Exception {
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
  void newInjectionConstants_injectsConstantWithoutSourceTransform() throws Exception {
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

  private PipelineMeta mockSingleTransformPipelineMeta(
      final String targetTransformName, ITransformMeta smi) {
    TransformMeta transformMeta = mock(TransformMeta.class);
    when(transformMeta.getTransform()).thenReturn(smi);
    when(transformMeta.getName()).thenReturn(targetTransformName);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    when(pipelineMeta.getUsedTransforms()).thenReturn(Collections.singletonList(transformMeta));
    return pipelineMeta;
  }

  @InjectionSupported(localizationPrefix = "", groups = "groups")
  private static class InjectableTestTransformMeta
      extends BaseTransformMeta<MetaInject, MetaInjectData> {

    @Injection(name = "THERE")
    private String there;
  }
}
