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

package org.apache.hop.neo4j.transforms.cypherbuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Statements which change data aren't retried on connections without transactions. */
class CypherBuilderAttemptsTest {

  private TransformMockHelper<CypherBuilderMeta, CypherBuilderData> helper;
  private CypherBuilderData data;
  private CypherBuilder cypherBuilder;

  @BeforeAll
  static void beforeAll() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "Cypher builder", CypherBuilderMeta.class, CypherBuilderData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    data = new CypherBuilderData();
    cypherBuilder =
        new CypherBuilder(
            helper.transformMeta,
            new CypherBuilderMeta(),
            data,
            0,
            helper.pipelineMeta,
            helper.pipeline);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void writesAreNotRetriedWithoutTransactions() {
    data.graphConnection = connection(false);
    data.needsWriteTransaction = true;

    assertEquals(1, cypherBuilder.getAttempts(2));
    verify(helper.iLogChannel).logBasic(contains("doesn't support transactions"));
  }

  @Test
  void readsAreRetriedWithoutTransactions() {
    data.graphConnection = connection(false);
    data.needsWriteTransaction = false;

    assertEquals(3, cypherBuilder.getAttempts(2));
    verify(helper.iLogChannel, never()).logBasic(contains("doesn't support transactions"));
  }

  @Test
  void writesAreRetriedWithTransactions() {
    data.graphConnection = connection(true);
    data.needsWriteTransaction = true;

    assertEquals(3, cypherBuilder.getAttempts(2));
  }

  @Test
  void boltWritesAreRetried() {
    data.needsWriteTransaction = true;

    assertEquals(3, cypherBuilder.getAttempts(2));
  }

  private static IGraphConnection connection(boolean supportingTransactions) {
    IGraphConnection connection = mock(IGraphConnection.class);
    when(connection.isSupportingTransactions()).thenReturn(supportingTransactions);
    return connection;
  }
}
