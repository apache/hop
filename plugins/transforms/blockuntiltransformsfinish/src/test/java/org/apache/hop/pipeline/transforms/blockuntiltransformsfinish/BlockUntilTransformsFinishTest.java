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

package org.apache.hop.pipeline.transforms.blockuntiltransformsfinish;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockUntilTransformsFinishTest {
  private static final String WAITED_FOR = "write to table";

  private TransformMockHelper<BlockUntilTransformsFinishMeta, BlockUntilTransformsFinishData>
      helper;

  @BeforeEach
  void setup() {
    helper =
        new TransformMockHelper<>(
            "BLOCK UNTIL TRANSFORMS FINISH TEST",
            BlockUntilTransformsFinishMeta.class,
            BlockUntilTransformsFinishData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  /**
   * #9096: the waited-for transform switches from idle to running while its status is checked. The
   * first status read sees IDLE and every later read sees RUNNING, so a check that reads the status
   * more than once matches none of the "still busy" states and stops waiting.
   */
  @Test
  void waitsWhenTransformStartsRunningDuringStatusCheck() throws Exception {
    AtomicInteger statusReads = new AtomicInteger();
    AtomicBoolean finished = new AtomicBoolean();
    IEngineComponent waitedFor = mock(IEngineComponent.class);
    when(waitedFor.getName()).thenReturn(WAITED_FOR);
    when(waitedFor.getStatus())
        .thenAnswer(
            invocation -> {
              int read = statusReads.incrementAndGet();
              if (read == 1) {
                return ComponentExecutionStatus.STATUS_IDLE;
              }
              if (read < 10) {
                return ComponentExecutionStatus.STATUS_RUNNING;
              }
              finished.set(true);
              return ComponentExecutionStatus.STATUS_FINISHED;
            });
    when(helper.pipeline.findComponent(WAITED_FOR, 0)).thenReturn(waitedFor);

    BlockUntilTransformsFinishMeta meta = new BlockUntilTransformsFinishMeta();
    meta.setBlockingTransforms(List.of(new BlockingTransform(WAITED_FOR, "0")));

    BlockUntilTransformsFinish transform =
        spy(
            new BlockUntilTransformsFinish(
                helper.transformMeta,
                meta,
                new BlockUntilTransformsFinishData(),
                0,
                helper.pipelineMeta,
                helper.pipeline));

    AtomicBoolean rowPassedBeforeFinish = new AtomicBoolean();
    doReturn(new Object[] {1L}).when(transform).getRow();
    doAnswer(
            invocation -> {
              rowPassedBeforeFinish.set(!finished.get());
              return null;
            })
        .when(transform)
        .putRow(any(), any());

    transform.processRow();

    assertFalse(rowPassedBeforeFinish.get(), "The row was passed on before the transform finished");
    assertTrue(finished.get());
  }
}
