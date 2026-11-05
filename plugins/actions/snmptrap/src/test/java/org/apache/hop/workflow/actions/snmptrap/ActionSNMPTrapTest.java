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
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/** Unit test for {@link ActionSNMPTrap} */
class ActionSNMPTrapTest {

  private ActionSNMPTrap action;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionSNMPTrap();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    workflow.setStopped(false);
  }

  @Test
  void testSerialization() throws Exception {
    HopClientEnvironment.init();

    ActionSNMPTrap trap =
        ActionSerializationTestUtil.testSerialization(
            "/snmp-trap-action.xml", ActionSNMPTrap.class);

    assertEquals("awesomeserver", trap.getServerName());
    assertEquals("5000", trap.getTimeout());
    assertEquals("162", trap.getPort());
    assertEquals("2", trap.getNrretry());
    assertEquals("13131", trap.getOid());
    assertEquals("averynicemessage", trap.getMessage());
    assertEquals("community", trap.getTargettype());
  }

  @Test
  void userTargetTypeCodeIsKeptWhenLoaded() throws Exception {
    HopClientEnvironment.init();

    action.setTargettype("user");
    assertEquals("user", action.getTargettype());
    action.setTargettype("USER");
    assertEquals("user", action.getTargettype());
    action.setTargettype(ActionSNMPTrap.targetTypeDesc[1]);
    assertEquals("user", action.getTargettype());

    String xml =
        XmlHandler.openTag(ActionMeta.XML_TAG)
            + "<name>Send v3 trap</name><targettype>user</targettype>"
            + XmlHandler.closeTag(ActionMeta.XML_TAG);
    Document document = XmlHandler.loadXmlString(xml);
    Node node = XmlHandler.getSubNode(document, ActionMeta.XML_TAG);
    ActionSNMPTrap loaded =
        XmlMetadataUtil.deSerializeFromXml(
            node, ActionSNMPTrap.class, new MemoryMetadataProvider());

    assertEquals("user", loaded.getTargettype());
  }
}
