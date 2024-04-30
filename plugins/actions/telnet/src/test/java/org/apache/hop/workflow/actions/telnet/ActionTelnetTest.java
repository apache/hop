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

package org.apache.hop.workflow.actions.telnet;

import static org.junit.Assert.assertEquals;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ActionTelnetTest {

  private IWorkflowEngine<WorkflowMeta> workflow;
  private ActionTelnet action;

  @BeforeClass
  public static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @AfterClass
  public static void tearDownAfterClass() {}

  @Before
  public void setUp() throws Exception {
    workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionTelnet();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    workflow.setStopped(false);
  }

  @Test
  public void testSerialization() throws Exception {
    HopClientEnvironment.init();
    ActionTelnet action =
        ActionSerializationTestUtil.testSerialization("/telnet-action.xml", ActionTelnet.class);
    assertEquals("24", action.getPort());
    assertEquals("2023", action.getTimeout());
    assertEquals("Hop", action.getHostname());
  }
}
