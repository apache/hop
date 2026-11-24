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

package org.apache.hop.neo4j.logging.xp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Result;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphTransaction;
import org.apache.hop.core.graph.IGraphTransactionWork;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Neo4j execution logging at the end of a run on an engine other than the local one: the Beam and
 * native Spark engines run no transform copies.
 */
class PipelineLoggingExtensionPointTest {

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @Test
  @SuppressWarnings("unchecked")
  void theEndOfARunOnAnotherEngineLogsThePipelineOnly() throws Exception {
    List<String> statements = new ArrayList<>();
    IGraphTransaction transaction = mock(IGraphTransaction.class);
    when(transaction.execute(anyString(), anyMap()))
        .thenAnswer(
            invocation -> {
              statements.add(invocation.getArgument(0));
              return List.of();
            });
    IGraphConnection connection = mock(IGraphConnection.class);
    when(connection.executeWrite(any()))
        .thenAnswer(
            invocation ->
                ((IGraphTransactionWork<Object>) invocation.getArgument(0)).execute(transaction));

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("on-beam");
    ILogChannel channel = mock(ILogChannel.class);
    when(channel.getLogChannelId()).thenReturn("on-beam-id");
    Map<String, Object> extensionData = new HashMap<>();
    extensionData.put(PipelineLoggingExtensionPoint.PIPELINE_START_DATE, new Date());
    // Not a local Pipeline: an engine such as Beam or native Spark
    IPipelineEngine<PipelineMeta> engine = mock(IPipelineEngine.class);
    when(engine.getPipelineMeta()).thenReturn(pipelineMeta);
    when(engine.getLogChannel()).thenReturn(channel);
    when(engine.getLogChannelId()).thenReturn("on-beam-id");
    when(engine.getResult()).thenReturn(new Result());
    when(engine.getExtensionDataMap()).thenReturn(extensionData);
    when(engine.getStatusDescription()).thenReturn("Finished");

    ILogChannel log = mock(ILogChannel.class);
    new PipelineLoggingExtensionPoint().logEndOfPipeline(log, connection, "graph", engine);

    verify(log, never()).logError(anyString(), any(Throwable.class));
    assertEquals(1, statements.size(), "only the pipeline is logged: " + statements);
    assertTrue(statements.get(0).contains("EXECUTION_OF_PIPELINE"), statements.get(0));
  }
}
