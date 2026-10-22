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

package org.apache.hop.ui.hopgui.file.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.junit.jupiter.api.Test;

class HopWorkflowFileTypeTest {

  @Test
  void addDefaultStartActionAddsOneStartAction() {
    WorkflowMeta workflowMeta = new WorkflowMeta();

    HopWorkflowFileType.addDefaultStartAction(workflowMeta);

    assertEquals(1, workflowMeta.nrActions());
    ActionMeta start = workflowMeta.getAction(0);
    assertTrue(start.isStart());
    assertEquals("Start", start.getName());
    assertEquals(50, start.getLocation().x);
    assertEquals(50, start.getLocation().y);
  }
}
