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

package org.apache.hop.pipeline.transforms.joinrows;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JoinRowsTest {

  private JoinRowsMeta meta;
  private JoinRowsData data;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    meta = new JoinRowsMeta();
    data = new JoinRowsData();

    ILogChannelFactory logChannelFactory = mock(ILogChannelFactory.class);
    ILogChannel logChannelInterface = mock(ILogChannel.class);
    HopLogStore.setLogChannelFactory(logChannelFactory);
    when(logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(logChannelInterface);
  }

  @AfterEach
  void tearDown() {
    meta = null;
    data = null;
  }

  /** BACKLOG-8520 Check that method call does't throw an error NullPointerException. */
  @Test
  void checkThatMethodPerformedWithoutError() throws Exception {
    getJoinRows().dispose();
  }

  @Test
  void disposeDataFiles() throws Exception {
    File mockFile1 = mock(File.class);
    File mockFile2 = mock(File.class);
    data.file = new File[] {null, mockFile1, mockFile2};
    getJoinRows().dispose();
    verify(mockFile1, times(1)).delete();
    verify(mockFile2, times(1)).delete();
  }

  private JoinRows getJoinRows() throws Exception {
    TransformMeta transformMeta = new TransformMeta("test", meta);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transformMeta);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setLogChannel(new LogChannel("junit"));
    // Note: we don't call prepareExecution()/startThreads() here — the tests below either
    // exercise the transform directly on the calling thread (processRow loop) or only verify
    // dispose() behavior. Spawning the pipeline's own runner thread would create a second
    // JoinRows instance with no input rowsets and race with the test.
    return new JoinRows(transformMeta, meta, data, 0, pipelineMeta, pipeline);
  }
}
