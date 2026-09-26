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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hop.pipeline.TransformCopyCompletion.Badge;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.junit.jupiter.api.Test;

class TransformCopyCompletionTest {

  @Test
  void singleCopyShowsTheCheckOnlyWhenThatCopyIsFinished() {
    assertEquals(Badge.NONE, TransformCopyCompletion.of(List.of(copy(0, null))).badge());
    assertEquals(
        Badge.NONE,
        TransformCopyCompletion.of(List.of(copy(0, ComponentExecutionStatus.STATUS_RUNNING)))
            .badge());
    TransformCopyCompletion.Summary finished =
        TransformCopyCompletion.of(List.of(copy(0, ComponentExecutionStatus.STATUS_FINISHED)));
    assertEquals(Badge.FINISHED, finished.badge());
    assertEquals(1, finished.finished());
    assertEquals(1, finished.total());
  }

  @Test
  void partialCountUntilEveryCopyIsFinished() {
    TransformCopyCompletion.Summary oneOfFour =
        TransformCopyCompletion.of(
            List.of(
                copy(0, ComponentExecutionStatus.STATUS_FINISHED),
                copy(1, ComponentExecutionStatus.STATUS_RUNNING),
                copy(2, ComponentExecutionStatus.STATUS_RUNNING),
                copy(3, ComponentExecutionStatus.STATUS_INIT)));
    assertEquals(Badge.PARTIAL, oneOfFour.badge());
    assertEquals(1, oneOfFour.finished());
    assertEquals(4, oneOfFour.total());

    TransformCopyCompletion.Summary threeOfFour =
        TransformCopyCompletion.of(
            List.of(
                copy(0, ComponentExecutionStatus.STATUS_FINISHED),
                copy(1, ComponentExecutionStatus.STATUS_FINISHED),
                copy(2, ComponentExecutionStatus.STATUS_FINISHED),
                copy(3, ComponentExecutionStatus.STATUS_RUNNING)));
    assertEquals(Badge.PARTIAL, threeOfFour.badge());
    assertEquals(3, threeOfFour.finished());

    TransformCopyCompletion.Summary allFour =
        TransformCopyCompletion.of(
            List.of(
                copy(0, ComponentExecutionStatus.STATUS_FINISHED),
                copy(1, ComponentExecutionStatus.STATUS_FINISHED),
                copy(2, ComponentExecutionStatus.STATUS_FINISHED),
                copy(3, ComponentExecutionStatus.STATUS_FINISHED)));
    assertEquals(Badge.FINISHED, allFour.badge());
    assertEquals(4, allFour.finished());
    assertEquals(4, allFour.total());
  }

  @Test
  void stoppedCopyDoesNotCountAsFinished() {
    TransformCopyCompletion.Summary summary =
        TransformCopyCompletion.of(
            List.of(
                copy(0, ComponentExecutionStatus.STATUS_FINISHED),
                copy(1, ComponentExecutionStatus.STATUS_STOPPED)));
    assertEquals(Badge.PARTIAL, summary.badge());
    assertEquals(1, summary.finished());
    assertEquals(2, summary.total());

    assertEquals(
        Badge.NONE,
        TransformCopyCompletion.of(
                List.of(
                    copy(0, ComponentExecutionStatus.STATUS_STOPPED),
                    copy(1, ComponentExecutionStatus.STATUS_DISPOSED)))
            .badge());
  }

  @Test
  void pausedOnlyWhenNothingIsStillActive() {
    assertEquals(
        Badge.PAUSED,
        TransformCopyCompletion.of(
                List.of(
                    copy(0, ComponentExecutionStatus.STATUS_PAUSED),
                    copy(1, ComponentExecutionStatus.STATUS_PAUSED)))
            .badge());

    assertEquals(
        Badge.NONE,
        TransformCopyCompletion.of(
                List.of(
                    copy(0, ComponentExecutionStatus.STATUS_PAUSED),
                    copy(1, ComponentExecutionStatus.STATUS_RUNNING)))
            .badge());

    TransformCopyCompletion.Summary finishedAndPaused =
        TransformCopyCompletion.of(
            List.of(
                copy(0, ComponentExecutionStatus.STATUS_FINISHED),
                copy(1, ComponentExecutionStatus.STATUS_PAUSED)));
    assertEquals(Badge.PARTIAL, finishedAndPaused.badge());
    assertEquals(1, finishedAndPaused.finished());
  }

  @Test
  void emptyListAndNullStatusDoNotShowABadge() {
    assertEquals(Badge.NONE, TransformCopyCompletion.of(null).badge());
    assertEquals(Badge.NONE, TransformCopyCompletion.of(Collections.emptyList()).badge());
    assertEquals(0, TransformCopyCompletion.of(List.of()).total());

    TransformCopyCompletion.Summary nullStatus =
        TransformCopyCompletion.of(Arrays.asList(copy(0, null), null));
    assertEquals(Badge.NONE, nullStatus.badge());
    assertEquals(0, nullStatus.finished());
    assertEquals(2, nullStatus.total());
  }

  private static IEngineComponent copy(int copyNr, ComponentExecutionStatus status) {
    EngineComponent component = new EngineComponent("Load", copyNr);
    component.setStatus(status);
    return component;
  }
}
