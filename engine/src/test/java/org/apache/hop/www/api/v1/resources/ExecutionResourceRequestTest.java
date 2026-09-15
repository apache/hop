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

package org.apache.hop.www.api.v1.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hop.www.api.v1.model.SyncRequest;
import org.junit.jupiter.api.Test;

class ExecutionResourceRequestTest {

  @Test
  void variablesDefaultToAnEmptyMap() {
    assertNotNull(new SyncRequest().getVariables());
    assertTrue(new SyncRequest().getVariables().isEmpty());
  }

  @Test
  void anExplicitNullForVariablesDoesNotBecomeANullField() {
    // A client sending "variables": null used to produce an NPE while applying parameters.
    SyncRequest request = new SyncRequest();
    request.setVariables(null);

    assertNotNull(request.getVariables());
    assertTrue(request.getVariables().isEmpty());
  }

  @Test
  void variablesArePreservedWhenGiven() {
    SyncRequest request = new SyncRequest();
    request.setVariables(Map.of("VAR1", "value1"));

    assertEquals("value1", request.getVariables().get("VAR1"));
  }

  @Test
  void theRemainingFieldsRoundTrip() {
    SyncRequest request = new SyncRequest();
    request.setService("test");
    request.setRunConfig("local");
    request.setBodyContent("payload");

    assertEquals("test", request.getService());
    assertEquals("local", request.getRunConfig());
    assertEquals("payload", request.getBodyContent());
  }
}
