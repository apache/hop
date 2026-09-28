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

import java.util.List;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IEngineComponent;

/**
 * Canvas badge for one transform, decided from every copy rather than from the first copy that
 * finishes.
 */
public final class TransformCopyCompletion {

  /** What to paint in the top-right corner of the transform icon. */
  public enum Badge {
    /** No status badge. */
    NONE,
    /** Waiting icon. Nothing is finished and every remaining copy is paused. */
    PAUSED,
    /** Azure count of finished copies. At least one copy is still not finished. */
    PARTIAL,
    /** Success check. Every copy is finished. */
    FINISHED
  }

  /**
   * @param badge which badge to draw
   * @param finished number of copies whose status is finished
   * @param total number of copies considered
   */
  public record Summary(Badge badge, int finished, int total) {}

  private TransformCopyCompletion() {}

  /**
   * @param copies the copies of one transform, or null when the engine has not reported any
   * @return the single badge for those copies
   */
  public static Summary of(List<IEngineComponent> copies) {
    if (copies == null || copies.isEmpty()) {
      return new Summary(Badge.NONE, 0, 0);
    }

    int finished = 0;
    int paused = 0;
    int active = 0;
    for (IEngineComponent copy : copies) {
      ComponentExecutionStatus status = copy == null ? null : copy.getStatus();
      if (status == ComponentExecutionStatus.STATUS_FINISHED) {
        finished++;
      } else if (status == ComponentExecutionStatus.STATUS_PAUSED) {
        paused++;
      } else if (isStillActive(status)) {
        active++;
      }
    }

    int total = copies.size();
    Badge badge;
    if (finished == total) {
      badge = Badge.FINISHED;
    } else if (finished > 0) {
      badge = Badge.PARTIAL;
    } else if (paused > 0 && active == 0) {
      badge = Badge.PAUSED;
    } else {
      badge = Badge.NONE;
    }
    return new Summary(badge, finished, total);
  }

  /**
   * Statuses that mean the copy has not reached a terminal paused or finished state. Stopped and
   * disposed copies are neither finished nor still active.
   */
  private static boolean isStillActive(ComponentExecutionStatus status) {
    return status == null
        || status == ComponentExecutionStatus.STATUS_RUNNING
        || status == ComponentExecutionStatus.STATUS_INIT
        || status == ComponentExecutionStatus.STATUS_IDLE
        || status == ComponentExecutionStatus.STATUS_HALTING
        || status == ComponentExecutionStatus.STATUS_EMPTY
        || status == ComponentExecutionStatus.STATUS_HALTED;
  }
}
