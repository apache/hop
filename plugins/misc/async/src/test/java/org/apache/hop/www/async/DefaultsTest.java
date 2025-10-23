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

package org.apache.hop.www.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Test class for Defaults */
public class DefaultsTest {

  @Test
  public void testAsyncStatusGroup() {
    assertNotNull(Defaults.ASYNC_STATUS_GROUP);
    assertEquals("ASYNC_STATUS_GROUP", Defaults.ASYNC_STATUS_GROUP);
  }

  @Test
  public void testAsyncActionPipelineServiceName() {
    assertNotNull(Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME);
    assertEquals(
        "enable-asynchronous-pipeline-service-name", Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME);
  }

  @Test
  public void testConstantsAreNotBlank() {
    assertNotNull(Defaults.ASYNC_STATUS_GROUP);
    assertNotNull(Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME);

    assertTrue(!Defaults.ASYNC_STATUS_GROUP.trim().isEmpty());
    assertTrue(!Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME.trim().isEmpty());
  }

  @Test
  public void testConstantsAreDifferent() {
    assertTrue(!Defaults.ASYNC_STATUS_GROUP.equals(Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME));
  }

  @Test
  public void testConstantsFormat() {
    assertTrue(Defaults.ASYNC_STATUS_GROUP.contains("ASYNC"));
    assertTrue(Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME.contains("async"));

    assertTrue(Defaults.ASYNC_STATUS_GROUP.contains("STATUS"));
    assertTrue(Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME.contains("pipeline"));
  }
}
