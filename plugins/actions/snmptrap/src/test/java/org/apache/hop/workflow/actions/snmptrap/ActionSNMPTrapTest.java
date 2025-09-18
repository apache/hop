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

package org.apache.hop.workflow.actions.snmptrap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActionSNMPTrapTest {

  private IWorkflowEngine<WorkflowMeta> workflow;
  private ActionSNMPTrap action;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionSNMPTrap();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    workflow.setStopped(false);
  }

  @Test
  void testSerialization() throws Exception {
    HopClientEnvironment.init();

    ActionSNMPTrap action =
        ActionSerializationTestUtil.testSerialization(
            "/snmp-trap-action.xml", ActionSNMPTrap.class);

    assertEquals("awesomeserver", action.getServerName());
    assertEquals("5000", action.getTimeout());
    assertEquals("162", action.getPort());
    assertEquals("2", action.getNrretry());
    assertEquals("13131", action.getOid());
    assertEquals("averynicemessage", action.getMessage());
    assertEquals("community", action.getTargettype());
  }
}
