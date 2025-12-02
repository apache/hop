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

package org.apache.hop.pipeline.debug;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Unit test for {@link PipelineDebugMeta} */
@ExtendWith(MockitoExtension.class)
class PipelineDebugMetaTests {
  @Mock private IPipelineEngine<PipelineMeta> pipelineEngine;
  @Mock private TransformMeta transformMeta;

  private PipelineDebugMeta pipelineDebugMeta;
  private TransformDebugMeta transformDebugMeta;

  @BeforeEach
  void setUp() {
    pipelineDebugMeta = new PipelineDebugMeta(null);
    lenient().when(transformMeta.getName()).thenReturn("TestTransform");

    transformDebugMeta = new TransformDebugMeta();
    transformDebugMeta.setRowCount(3);
    transformDebugMeta.setReadingFirstRows(true);

    transformDebugMeta = spy(transformDebugMeta);
    pipelineDebugMeta.getTransformDebugMetaMap().put(transformMeta, transformDebugMeta);
  }

  @Test
  void testRowBufferStoresRowsCorrectly() {
    ITransform transform = mock(ITransform.class);
    when(pipelineEngine.getComponentCopies("TestTransform")).thenReturn(List.of(transform));

    pipelineDebugMeta.addRowListenersToPipeline(pipelineEngine);

    // capture RowListener
    verify(transform).addRowListener(any(RowAdapter.class));
  }

  @Test
  void testBreakPointListenerTriggeredOnce() {
    ITransform transform = mock(ITransform.class);
    when(pipelineEngine.getComponentCopies("TestTransform")).thenReturn(List.of(transform));

    // add break point listener
    final boolean[] called = {false};
    transformDebugMeta.addBreakPointListener(
        (pipelineDebug, debugMeta, rowMeta, buffer) -> {
          if (!called[0]) {
            called[0] = true;
          } else {
            fail("Break point listener should be triggered only once!");
          }
        });

    pipelineDebugMeta.addRowListenersToPipeline(pipelineEngine);

    // pipeline finish
    pipelineEngine.addExecutionFinishedListener(
        mockPipeline -> {
          pipelineDebugMeta
              .getTransformDebugMetaMap()
              .get(transformMeta)
              .fireBreakPointListeners(pipelineDebugMeta);
        });

    // first call
    transformDebugMeta.fireBreakPointListeners(pipelineDebugMeta);

    // second call, should not trigger due to dataShown
    pipelineDebugMeta.setStopClosePressed(true);
    if (!pipelineDebugMeta.isStopClosePressed()) {
      transformDebugMeta.fireBreakPointListeners(pipelineDebugMeta);
    }

    assertTrue(called[0]);
    verify(transformDebugMeta).fireBreakPointListeners(pipelineDebugMeta);
  }

  @Test
  void testRowBufferMultiplePages() {
    ITransform transform = mock(ITransform.class);
    when(pipelineEngine.getComponentCopies("TestTransform")).thenReturn(List.of(transform));

    pipelineDebugMeta.addRowListenersToPipeline(pipelineEngine);

    // add row data
    for (int i = 0; i < 5; i++) {
      Object[] row = new Object[] {i};
      transformDebugMeta.getRowBuffer().add(row);
    }

    assertEquals(5, transformDebugMeta.getRowBuffer().size());
  }

  @Test
  void testNrOfUsedTransforms() {
    // nr++
    TransformDebugMeta oneDebugMeta = new TransformDebugMeta();
    oneDebugMeta.setRowCount(2);
    oneDebugMeta.setPausingOnBreakPoint(true);
    oneDebugMeta = spy(oneDebugMeta);

    TransformMeta oneTransMeta = mock(TransformMeta.class);
    lenient().when(oneTransMeta.getName()).thenReturn("OneTransform");
    pipelineDebugMeta.getTransformDebugMetaMap().put(oneTransMeta, oneDebugMeta);

    // The condition nr++ does not get executed.
    TransformDebugMeta twoDebugMeta = new TransformDebugMeta();
    twoDebugMeta.setPausingOnBreakPoint(true);

    TransformMeta twoTransMeta = mock(TransformMeta.class);
    lenient().when(twoTransMeta.getName()).thenReturn("TwoTransform");
    pipelineDebugMeta.getTransformDebugMetaMap().put(twoTransMeta, twoDebugMeta);

    int nr = pipelineDebugMeta.getNrOfUsedTransforms();
    assertEquals(2, nr);
  }
}
