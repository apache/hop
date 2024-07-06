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
package org.apache.hop.workflow.actions.mailvalidator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ActionMailValidatorTest {

  private IWorkflowEngine<WorkflowMeta> workflow;
  private ActionMailValidator action;

  @BeforeClass
  public static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @Before
  public void setUp() {
    workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionMailValidator();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    workflow.setStopped(false);
  }

  @Test
  public void testExecute() {
    HopLogStore.init();
    Result previousResult = new Result();
    ActionMailValidator validator = new ActionMailValidator();
    Result result = validator.execute(previousResult, 0);
    assertNotNull(result);
  }

  @Test
  public void testSerialization() throws Exception {
    HopClientEnvironment.init();
    MemoryMetadataProvider provider = new MemoryMetadataProvider();

    ActionMailValidator action =
        ActionSerializationTestUtil.testSerialization(
            "/mailvalidator-action.xml", ActionMailValidator.class, provider);
    assertEquals(true, action.isSmtpCheck());
    assertEquals("123", action.getTimeout());
    assertEquals("smtp.googlle.com", action.getDefaultSMTP());
    assertEquals("noreply@domain.com", action.getEmailSender());
    assertEquals("test@test.com", action.getEmailAddress());
  }
}
