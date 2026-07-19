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

package org.apache.hop.databricks.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class DatabricksRunWaiterTest {

  @Test
  void pollsUntilTerminalSuccess() throws Exception {
    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.getRun(9L))
        .thenReturn(
            new DatabricksRunStatus(9L, 1L, DatabricksRunLifeCycleState.RUNNING, null, null, null))
        .thenReturn(
            new DatabricksRunStatus(
                9L, 1L, DatabricksRunLifeCycleState.TERMINATED, "SUCCESS", "ok", null));

    int[] polls = {0};
    DatabricksRunStatus status =
        DatabricksRunWaiter.waitFor(
            client,
            9L,
            60,
            1,
            true,
            new DatabricksRunWaiter.Hooks() {
              @Override
              public boolean isStopped() {
                return false;
              }

              @Override
              public void onStatus(DatabricksRunStatus s) {
                polls[0]++;
              }

              @Override
              public void logDetailed(String message) {}

              @Override
              public void logError(String message) {}
            });

    assertTrue(status.isSuccess());
    assertEquals(2, polls[0]);
    verify(client, times(2)).getRun(9L);
  }

  @Test
  void timeoutFails() throws Exception {
    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.getRun(1L))
        .thenReturn(
            new DatabricksRunStatus(
                1L, null, DatabricksRunLifeCycleState.RUNNING, null, null, null));

    assertThrows(
        HopException.class,
        () ->
            DatabricksRunWaiter.waitFor(
                client,
                1L,
                1,
                1,
                false,
                new DatabricksRunWaiter.Hooks() {
                  @Override
                  public boolean isStopped() {
                    return false;
                  }

                  @Override
                  public void onStatus(DatabricksRunStatus status) {}

                  @Override
                  public void logDetailed(String message) {}

                  @Override
                  public void logError(String message) {}
                }));
  }
}
