/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.engines.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.execution.sampler.plugins.first.FirstRowsExecutionDataSampler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LocalPipelineEngineSamplersTest {

  @BeforeAll
  static void initLogging() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  /**
   * Issue #9036: attaching the samplers asked every transform for its output fields. A Table Input
   * on SQL Server can only answer that by running its SQL, so inserts and procedure calls in that
   * SQL ran twice.
   */
  @Test
  void attachingSamplersDoesNotAskTransformsForTheirFields() throws Exception {
    ITransformMeta transformMeta = mock(ITransformMeta.class);
    when(transformMeta.getTransformIOMeta())
        .thenReturn(new TransformIOMeta(false, true, false, false, false, false));
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("samplers");
    TransformMeta input = new TransformMeta("input", transformMeta);
    pipelineMeta.addTransform(input);

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(ExecutionDataProfile.class)
        .save(
            new ExecutionDataProfile(
                "first-rows",
                null,
                List.<IExecutionDataSampler>of(new FirstRowsExecutionDataSampler("10"))));

    LocalPipelineEngine engine =
        new LocalPipelineEngine(pipelineMeta) {
          {
            setTransforms(new ArrayList<>());
          }
        };
    engine.setLogChannel(new LogChannel("samplers"));
    engine.setMetadataProvider(metadataProvider);
    PipelineRunConfiguration runConfiguration = new PipelineRunConfiguration();
    runConfiguration.setExecutionDataProfileName("first-rows");
    engine.setPipelineRunConfiguration(runConfiguration);

    ExecutionInfoLocation location = new ExecutionInfoLocation();
    location.setExecutionInfoLocation(mock(IExecutionInfoLocation.class));
    engine.setExecutionInfoLocation(location);

    ITransform transform = mock(ITransform.class);
    TransformMetaDataCombi combi = new TransformMetaDataCombi();
    combi.transformName = "input";
    combi.copy = 0;
    combi.transformMeta = input;
    combi.meta = transformMeta;
    combi.transform = transform;
    engine.getTransforms().add(combi);

    engine.addTransformExecutionSamplers();

    verify(transform).addRowListener(any(IRowListener.class));
    verify(transformMeta, never()).getFields(any(), any(), any(), any(), any(), any(), any());
    verify(transformMeta, never()).getFields(any(), any(), any(), any(), any(), any());
    verify(transformMeta, never()).getTableFields(any());
  }

  /** Without row metadata at init time the sampler takes the layout from the first sampled row. */
  @Test
  void samplerStoreTakesTheLayoutFromTheRows() throws Exception {
    FirstRowsExecutionDataSampler sampler = new FirstRowsExecutionDataSampler("10");
    IExecutionDataSamplerStore store =
        sampler.createSamplerStore(new ExecutionDataSamplerMeta("input", "0", "id", true, true));
    store.init(new Variables(), null, null);
    IRowListener listener = store.createRowListener(sampler);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("runs"));
    listener.rowWrittenEvent(rowMeta, new Object[] {1L});

    RowBuffer buffer = store.getSamples().values().iterator().next();
    assertSame(rowMeta, buffer.getRowMeta());
    assertEquals(1, buffer.getBuffer().size());
  }
}
