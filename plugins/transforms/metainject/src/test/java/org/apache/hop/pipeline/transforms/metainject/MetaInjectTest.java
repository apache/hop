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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.logging.LogChannel;
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
