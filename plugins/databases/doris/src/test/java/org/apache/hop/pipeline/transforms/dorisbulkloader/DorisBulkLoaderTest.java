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

package org.apache.hop.pipeline.transforms.dorisbulkloader;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the batch-flush behavior of {@link DorisBulkLoader#processStreamLoad(String, boolean)}.
 *
 * <p>{@code initStreamLoad} (which would open a real HTTP connection to Doris) is private so it
 * can't be stubbed; tests skip the {@code first=true} branch by pre-populating {@link
 * DorisBulkLoaderData#dorisStreamLoad} with a mock and only exercising the post-init path.
 */
class DorisBulkLoaderTest {

  private DorisBulkLoaderMeta meta;
  private DorisBulkLoaderData data;
  private DorisStreamLoad streamLoad;
  private DorisBulkLoader transform;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    meta = mock(DorisBulkLoaderMeta.class);
    doReturn(40).when(meta).getBufferSize();
    doReturn(2).when(meta).getBufferCount();
    doReturn("json").when(meta).getFormat();

    data = new DorisBulkLoaderData();
    streamLoad = mock(DorisStreamLoad.class);
    data.dorisStreamLoad = streamLoad;

    // Build a real (minimal) PipelineMeta / Pipeline so BaseTransform.<init> can call
    // pipelineMeta.findTransform() and getTargetTransformPartitioningMeta() without NPEs.
    TransformMeta transformMeta = new TransformMeta("Doris", meta);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transformMeta);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);

    DorisBulkLoader real =
        new DorisBulkLoader(transformMeta, meta, data, 0, pipelineMeta, pipeline);
    transform = spy(real);
    doReturn("xxx").when(transform).resolve(anyString());
    doReturn(false).when(transform).isDetailed();

    ResponseContent success = mock(ResponseContent.class);
    doReturn("Success").when(success).getStatus();
    doReturn(1L).when(success).getNumberLoadedRows();
    doReturn(1L).when(success).getNumberTotalRows();
    doReturn(success).when(streamLoad).executeDorisStreamLoad();
  }

  @Test
  void testCallProcessStreamLoadWithOneBatch() throws Exception {
    when(streamLoad.canWrite(anyLong())).thenReturn(true);

    // first row already started; canWrite=true so it just writes.
    transform.processStreamLoad("{\"no\":1, \"name\":\"tom\", \"sex\":\"m\"}", false);
    // null + first=false flushes the batch.
    transform.processStreamLoad(null, false);

    verify(streamLoad, times(1)).executeDorisStreamLoad();
  }

  @Test
  void testCallProcessStreamLoadWithTwoBatch() throws Exception {
    // canWrite=false on the second row forces an intermediate flush.
    when(streamLoad.canWrite(anyLong())).thenReturn(true, false, true);

    transform.processStreamLoad("{\"no\":1, \"name\":\"tom\", \"sex\":\"m\"}", false);
    transform.processStreamLoad("{\"no\":2, \"name\":\"jack\", \"sex\":\"m\"}", false);
    transform.processStreamLoad(null, false);

    verify(streamLoad, times(2)).executeDorisStreamLoad();
  }
}
