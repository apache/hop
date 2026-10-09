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

package org.apache.hop.neo4j.transforms.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.CypherGraphDialect;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.neo4j.shared.FakeGraphDatabase;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Graph output resolves variables in the name of its connection, like the other graph transforms.
 */
class GraphOutputConnectionNameTest {

  private TransformMockHelper<GraphOutputMeta, GraphOutputData> helper;

  @BeforeAll
  static void beforeAll() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>("Graph output", GraphOutputMeta.class, GraphOutputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void connectionNameWithAVariable() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(GraphDatabaseMeta.class)
        .save(
            new GraphDatabaseMeta("demo", new FakeGraphDatabase(CypherGraphDialect.DEFAULT, null)));

    GraphOutputMeta meta = new GraphOutputMeta();
    meta.setConnectionName("${GRAPH_CONNECTION}");
    GraphOutputData data = new GraphOutputData();
    GraphOutput graphOutput =
        new GraphOutput(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    graphOutput.setMetadataProvider(metadataProvider);
    graphOutput.setVariable("GRAPH_CONNECTION", "demo");

    // Init stops later on, because there is no graph model
    graphOutput.init();

    assertNotNull(data.graphConnection, "The connection was not found");
    assertEquals("demo", data.graphConnection.name());
  }
}
