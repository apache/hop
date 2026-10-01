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

package org.apache.hop.ai.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.gui.Point;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AiProposalXmlSupportTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void wrapsTransformWithoutMatchingTransformsTag() throws Exception {
    String xml = dummyTransformXml("Check");
    String wrapped = AiProposalXmlSupport.wrapPipelineClipboard(xml);
    assertTrue(wrapped.contains("<pipeline-transforms>"));
    assertTrue(wrapped.contains("<transforms>"));
    assertEquals(1, count(wrapped, "<transforms>"));
    assertEquals(1, AiProposalXmlSupport.transformPluginIds(xml).size());
    assertEquals("Dummy", AiProposalXmlSupport.transformPluginIds(xml).get(0));
    assertNull(AiProposalXmlSupport.validatePipelineXml(xml));
  }

  @Test
  void wrapsActionWithoutMatchingActionsTag() throws Exception {
    String xml = dummyActionXml("Check");
    String wrapped = AiProposalXmlSupport.wrapWorkflowClipboard(xml);
    assertTrue(wrapped.contains("<workflow-actions>"));
    assertEquals(1, count(wrapped, "<actions>"));
    assertEquals("DUMMY", AiProposalXmlSupport.actionPluginIds(xml).get(0));
    assertNull(AiProposalXmlSupport.validateWorkflowXml(xml));
  }

  @Test
  void unknownPluginIsInvalid() {
    String xml = "<transform><name>X</name><type>NoSuchPlugin</type></transform>";
    assertTrue(AiProposalXmlSupport.validatePipelineXml(xml).contains("Unknown plugin"));
  }

  @Test
  void emptyXmlIsInvalid() {
    assertEquals("xml parameter is required", AiProposalXmlSupport.validatePipelineXml(""));
  }

  @Test
  void hasOpenTagDoesNotMatchLongerName() {
    assertFalse(AiProposalXmlSupport.hasOpenTag("<transforms>", "transform"));
    assertTrue(AiProposalXmlSupport.hasOpenTag("<transform>", "transform"));
    assertFalse(AiProposalXmlSupport.hasOpenTag("<actions>", "action"));
    assertTrue(AiProposalXmlSupport.hasOpenTag("<action>", "action"));
  }

  public static String dummyTransformXml(String name) throws Exception {
    DummyMeta meta = new DummyMeta();
    meta.setDefault();
    TransformMeta transform = new TransformMeta("Dummy", name, meta);
    transform.setLocation(new Point(100, 80));
    return transform.getXml();
  }

  public static String dummyActionXml(String name) {
    ActionMeta action = new ActionMeta(new ActionDummy(name));
    action.setLocation(new Point(120, 90));
    return action.getXml();
  }

  private static int count(String haystack, String needle) {
    int count = 0;
    int from = 0;
    while (true) {
      int at = haystack.indexOf(needle, from);
      if (at < 0) {
        return count;
      }
      count++;
      from = at + needle.length();
    }
  }
}
