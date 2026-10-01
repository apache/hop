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

package org.apache.hop.workflow.actions.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ActionWorkflowWaitTimeoutTest {

  @Test
  void waitTimeoutIsSerialized() {
    ActionWorkflow action = new ActionWorkflow("run-child");
    action.setWaitingToFinish(true);
    action.setWaitTimeout("2500");
    String xml = action.getXml();
    assertTrue(xml.contains("<wait_timeout>2500</wait_timeout>"));
    assertEquals("2500", action.getWaitTimeout());
  }
}
