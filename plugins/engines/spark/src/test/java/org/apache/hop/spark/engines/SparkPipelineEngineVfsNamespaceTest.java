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

package org.apache.hop.spark.engines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfsNamespaces;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Like the local engine, a native Spark run against metadata of its own (a project's, an export on
 * a Hop Server) resolves its named VFS connections in a namespace of its own, and lets go of it
 * again.
 */
class SparkPipelineEngineVfsNamespaceTest {

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @AfterEach
  void clearNamespaces() {
    HopVfsNamespaces.reset();
  }

  @Test
  void preparingTakesTheNamespaceAndAFailureLetsGoOfIt() {
    SparkPipelineEngine engine = spy(new SparkPipelineEngine());
    engine.setPipelineMeta(new PipelineMeta());
    engine.setLogChannel(mock(ILogChannel.class));
    engine.setMetadataProvider(mock(IHopMetadataProvider.class));

    // Fail at the start of the preparation, after noting whether the namespace was there
    AtomicInteger namespacesWhilePreparing = new AtomicInteger(-1);
    doAnswer(
            invocation -> {
              if (Boolean.TRUE.equals(invocation.getArgument(0))) {
                namespacesWhilePreparing.set(HopVfsNamespaces.size());
                throw new IllegalStateException("preparation fails");
              }
              return invocation.callRealMethod();
            })
        .when(engine)
        .setPreparing(anyBoolean());

    assertThrows(HopException.class, engine::prepareExecution);

    assertEquals(
        1,
        namespacesWhilePreparing.get(),
        "Without a namespace, minio-connection://bucket/file resolves to a local folder");
    assertEquals(
        0,
        HopVfsNamespaces.size(),
        "The run never starts and never finishes, so nothing else would ever close this");

    // Cleaning up afterwards does not let go of it a second time
    engine.cleanup();
    assertEquals(0, HopVfsNamespaces.size());
  }
}
