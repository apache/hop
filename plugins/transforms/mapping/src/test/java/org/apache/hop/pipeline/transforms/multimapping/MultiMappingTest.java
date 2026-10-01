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
package org.apache.hop.pipeline.transforms.multimapping;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MultiMappingTest {

  private TransformMockHelper<MultiMappingMeta, MultiMappingData> transformMockHelper;

  @BeforeEach
  void setup() {
    transformMockHelper =
        new TransformMockHelper<>(
            "MULTI_MAPPING_TEST", MultiMappingMeta.class, MultiMappingData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  void disposeDoesNotNpeWhenInitFailed() {
    MultiMappingData data = new MultiMappingData();
    MultiMapping mapping =
        new MultiMapping(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    assertDoesNotThrow(mapping::dispose);
  }

  @Test
  void stopAllIsSafeWhenChildMissing() {
    MultiMappingData data = new MultiMappingData();
    MultiMapping mapping =
        new MultiMapping(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    assertDoesNotThrow(mapping::stopAll);
    assertDoesNotThrow(mapping::stopRunning);
  }

  @Test
  void initFailsWithoutFilename() {
    MultiMappingMeta meta = new MultiMappingMeta();
    MultiMappingData data = new MultiMappingData();
    when(transformMockHelper.iTransformMeta.getFilename()).thenReturn(null);
    MultiMapping mapping =
        new MultiMapping(
            transformMockHelper.transformMeta,
            meta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    mapping.setMetadataProvider(transformMockHelper.pipeline.getMetadataProvider());
    assertFalse(mapping.init());
  }

  @Test
  void disposeFlagsErrorWhenChildHadErrors() {
    MultiMappingData data = new MultiMappingData();
    data.wasStarted = true;
    LocalPipelineEngine child = org.mockito.Mockito.mock(LocalPipelineEngine.class);
    when(child.isFinished()).thenReturn(true);
    when(child.getErrors()).thenReturn(1);
    data.mappingPipeline = child;
    MultiMapping mapping =
        new MultiMapping(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    mapping.dispose();
  }
}
