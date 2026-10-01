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

package org.apache.hop.ui.hopgui.perspective.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Date;
import org.apache.hop.execution.ExecutionState;
import org.junit.jupiter.api.Test;

class ExecutionStatusIconTest {

  private static final long LOGGING_INTERVAL = 20_000L;

  @Test
  void nullStateIsDefault() {
    assertEquals(ExecutionStatusIcon.DEFAULT, ExecutionStatusIcon.from(null, LOGGING_INTERVAL));
  }

  @Test
  void failedWinsOverStale() {
    ExecutionState state = new ExecutionState();
    state.setFailed(true);
    state.setUpdateTime(new Date(System.currentTimeMillis() - 60_000L));

    assertEquals(ExecutionStatusIcon.ERROR, ExecutionStatusIcon.from(state, LOGGING_INTERVAL));
  }

  @Test
  void unfinishedWithoutRecentUpdateIsStalled() {
    ExecutionState state = new ExecutionState();
    state.setFailed(false);
    state.setExecutionEndDate(null);
    state.setUpdateTime(new Date(System.currentTimeMillis() - 60_000L));

    assertEquals(ExecutionStatusIcon.STALLED, ExecutionStatusIcon.from(state, LOGGING_INTERVAL));
  }

  @Test
  void finishedIsNeverStalled() {
    ExecutionState state = new ExecutionState();
    state.setFailed(false);
    state.setExecutionEndDate(new Date(System.currentTimeMillis() - 60_000L));
    state.setUpdateTime(new Date(System.currentTimeMillis() - 60_000L));

    assertEquals(ExecutionStatusIcon.DEFAULT, ExecutionStatusIcon.from(state, LOGGING_INTERVAL));
  }

  @Test
  void freshRunningIsDefault() {
    ExecutionState state = new ExecutionState();
    state.setFailed(false);
    state.setStatusDescription("Running");
    state.setUpdateTime(new Date());

    assertEquals(ExecutionStatusIcon.DEFAULT, ExecutionStatusIcon.from(state, LOGGING_INTERVAL));
  }

  @Test
  void missingUpdateTimeIsNotStalled() {
    ExecutionState state = new ExecutionState();
    state.setFailed(false);
    state.setUpdateTime(null);

    assertEquals(ExecutionStatusIcon.DEFAULT, ExecutionStatusIcon.from(state, LOGGING_INTERVAL));
  }
}
