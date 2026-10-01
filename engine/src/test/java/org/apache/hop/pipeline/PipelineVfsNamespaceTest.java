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

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfsNamespaces;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A pipeline running against metadata of its own - an export on a Hop Server - takes a VFS
 * namespace to resolve its named connections in, and has to let go of it again. Nothing else will:
 * the namespace holds a file system manager, and one that is never released is never closed.
 */
class PipelineVfsNamespaceTest {

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @AfterEach
  void clearNamespaces() {
    HopVfsNamespaces.reset();
  }

  @Test
  @DisplayName("A pipeline that fails to prepare does not leave its VFS namespace behind")
  void aFailedPreparationLetsGoOfTheNamespace() {
    IHopMetadataProvider ownMetadata = mock(IHopMetadataProvider.class);

    Pipeline pipeline = spy(new LocalPipelineEngine(new PipelineMeta()));
    pipeline.setLogChannel(mock(ILogChannel.class));
    pipeline.setMetadataProvider(ownMetadata);

    // Anything at all going wrong while preparing: a transform that will not initialise, a run
    // configuration that cannot be loaded, a file that is not there.
    doThrow(new HopRuntimeExceptionForTest()).when(pipeline).setPreparing(anyBoolean());

    assertThrows(HopRuntimeExceptionForTest.class, pipeline::prepareExecution);

    assertEquals(
        0,
        HopVfsNamespaces.size(),
        "The pipeline never runs and never finishes, so nothing else would ever close this");
  }

  /** Distinct type so the assertion cannot pass on some other failure. */
  private static class HopRuntimeExceptionForTest extends RuntimeException {}
}
