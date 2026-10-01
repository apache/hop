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

package org.apache.hop.workflow.actions.shell;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

class WorkflowActionShellLoadSaveTest {
  @Test
  void testSerialization() throws Exception {
    ActionShell meta =
        ActionSerializationTestUtil.testSerialization("/shell-action.xml", ActionShell.class);

    assertEquals("${PROJECT_HOME}/0002-shell-test.sh", meta.getFilename());
    assertEquals(Const.VAR_PROJECT_HOME, meta.getWorkDirectory());
    assertEquals(1, meta.getArguments().size());
    assertEquals("argument", meta.getArguments().get(0).getValue());
    assertFalse(meta.getArguments().get(0).isHidden());
  }

  @Test
  void testClone() throws Exception {
    ActionShell meta =
        ActionSerializationTestUtil.testSerialization("/shell-action.xml", ActionShell.class);

    ActionShell clone = (ActionShell) meta.clone();
    assertEquals(clone.getFilename(), meta.getFilename());
    assertEquals(clone.getWorkDirectory(), meta.getWorkDirectory());
    assertEquals(clone.getArguments().size(), meta.getArguments().size());
    assertEquals(clone.getArguments().get(0).getValue(), meta.getArguments().get(0).getValue());
    assertEquals(clone.getArguments().get(0).isHidden(), meta.getArguments().get(0).isHidden());
  }

  @Test
  void testHiddenArguments() {
    ActionShell meta = new ActionShell();
    meta.setArguments(
        List.of(
            new ActionShell.ShellArgument("regularArg", false),
            new ActionShell.ShellArgument("secretPassword", true)));

    assertEquals(2, meta.getArguments().size());
    assertEquals("regularArg", meta.getArguments().get(0).getValue());
    assertFalse(meta.getArguments().get(0).isHidden());
    assertEquals("secretPassword", meta.getArguments().get(1).getValue());
    assertTrue(meta.getArguments().get(1).isHidden());
  }

  @Test
  void testLegacyNumberedArgumentsBackwardCompatibility() throws Exception {
    String xml =
        "<action>\n"
            + "  <name>shell-legacy</name>\n"
            + "  <type>SHELL</type>\n"
            + "  <filename>/tmp/test.sh</filename>\n"
            + "  <argument0>firstArg</argument0>\n"
            + "  <argument1>secondArg</argument1>\n"
            + "</action>";

    Document doc = XmlHandler.loadXmlString(xml);
    Node node = XmlHandler.getSubNode(doc, ActionMeta.XML_TAG);
    ActionShell meta =
        XmlMetadataUtil.deSerializeFromXml(node, ActionShell.class, new MemoryMetadataProvider());

    assertNotNull(meta.getArguments());
    assertEquals(2, meta.getArguments().size());
    assertEquals("firstArg", meta.getArguments().get(0).getValue());
    assertFalse(meta.getArguments().get(0).isHidden());
    assertEquals("secondArg", meta.getArguments().get(1).getValue());
    assertFalse(meta.getArguments().get(1).isHidden());
  }

  @Test
  void testLegacyGroupArgumentsBackwardCompatibility() throws Exception {
    String xml =
        "<action>\n"
            + "  <name>shell-legacy-group</name>\n"
            + "  <type>SHELL</type>\n"
            + "  <filename>/tmp/test.sh</filename>\n"
            + "  <arguments>\n"
            + "    <argument>param1</argument>\n"
            + "    <argument>param2</argument>\n"
            + "  </arguments>\n"
            + "</action>";

    Document doc = XmlHandler.loadXmlString(xml);
    Node node = XmlHandler.getSubNode(doc, ActionMeta.XML_TAG);
    ActionShell meta =
        XmlMetadataUtil.deSerializeFromXml(node, ActionShell.class, new MemoryMetadataProvider());

    assertNotNull(meta.getArguments());
    assertEquals(2, meta.getArguments().size());
    assertEquals("param1", meta.getArguments().get(0).getValue());
    assertFalse(meta.getArguments().get(0).isHidden());
    assertEquals("param2", meta.getArguments().get(1).getValue());
    assertFalse(meta.getArguments().get(1).isHidden());
  }

  @Test
  void testNewFormatSerializationRoundTrip() throws Exception {
    String xml =
        "<action>\n"
            + "  <name>shell-new-format</name>\n"
            + "  <type>SHELL</type>\n"
            + "  <filename>/tmp/test.sh</filename>\n"
            + "  <arguments>\n"
            + "    <argument>\n"
            + "      <value>publicUser</value>\n"
            + "      <hidden>N</hidden>\n"
            + "    </argument>\n"
            + "    <argument>\n"
            + "      <value>secretPassword</value>\n"
            + "      <hidden>Y</hidden>\n"
            + "    </argument>\n"
            + "  </arguments>\n"
            + "</action>";

    Document doc = XmlHandler.loadXmlString(xml);
    Node node = XmlHandler.getSubNode(doc, ActionMeta.XML_TAG);
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    ActionShell meta = XmlMetadataUtil.deSerializeFromXml(node, ActionShell.class, provider);

    assertNotNull(meta.getArguments());
    assertEquals(2, meta.getArguments().size());
    assertEquals("publicUser", meta.getArguments().get(0).getValue());
    assertFalse(meta.getArguments().get(0).isHidden());
    assertEquals("secretPassword", meta.getArguments().get(1).getValue());
    assertTrue(meta.getArguments().get(1).isHidden());

    String serializedXml = ActionSerializationTestUtil.getXml(meta);
    ActionSerializationTestUtil.testXmlStringSerialization(
        ActionShell.class, ActionMeta.XML_TAG, provider, serializedXml, meta);
  }

  @Test
  void testBuildLogCommandMasksHiddenArgs() throws Exception {
    ActionShell action = new ActionShell();
    FileObject fileObject = HopVfs.getFileObject("/tmp/script.sh");
    String[] args = new String[] {"user", "mySecret123", "--flag"};
    boolean[] hidden = new boolean[] {false, true, false};
    List<String> cmds = List.of("/tmp/script.sh", "user", "mySecret123", "--flag");

    String logCommand = action.buildLogCommand(cmds, fileObject, args, hidden);
    assertFalse(logCommand.contains("mySecret123"));
    assertTrue(logCommand.contains("***"));
    assertTrue(logCommand.contains("user"));
    assertTrue(logCommand.contains("--flag"));
  }
}
