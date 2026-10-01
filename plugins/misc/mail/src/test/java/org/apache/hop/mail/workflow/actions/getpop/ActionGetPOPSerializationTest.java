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
package org.apache.hop.mail.workflow.actions.getpop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/** Unit test for {@link ActionGetPOP} */
class ActionGetPOPSerializationTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void deserializesFromXmlSnippetAndRoundTrips() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    ActionGetPOP action =
        ActionSerializationTestUtil.testSerialization(
            "/getpop-action.xml", ActionGetPOP.class, provider);

    assertEquals("imap.example.com", action.getServerName());
    assertEquals("alice", action.getUserName());
    assertTrue(action.isUseSsl());
    assertEquals("993", action.getSslPort());
    assertEquals("IMAP", action.getProtocol());
    assertEquals("INBOX", action.getImapFolder());
    assertEquals("/tmp/mail", action.getOutputDirectory());
    assertTrue(action.isSaveMessage());
    assertTrue(action.isSaveAttachment());
    assertFalse(action.isUseDifferentFolderForAttachment());
    assertEquals("boss@example.com", action.getSenderSearch());
    assertEquals("invoice", action.getSubjectSearch());
    assertEquals("urgent", action.getBodySearch());
    assertEquals(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD, action.getValueIMAPList());
    assertEquals(MailConnectionMeta.CONDITION_DATE_IGNORE, action.getConditionReceivedDate());
    assertEquals(MailConnectionMeta.AFTER_GET_IMAP_NOTHING, action.getAfterGetIMAP());
    assertEquals(MailConnectionMeta.ACTION_TYPE_GET, action.getActionType());
  }

  @Test
  void imapRetrieveUnreadSurvivesRoundTrip() throws Exception {
    ActionGetPOP original = new ActionGetPOP("Get mails (POP3/IMAP)");
    original.setProtocol("IMAP");
    original.setValueIMAPList(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD);
    original.setActionType(MailConnectionMeta.ACTION_TYPE_MOVE);
    original.setAfterGetIMAP(MailConnectionMeta.AFTER_GET_IMAP_DELETE);
    original.setConditionReceivedDate(MailConnectionMeta.CONDITION_DATE_GREATER);

    String xml = ActionSerializationTestUtil.getXml(original);
    assertTrue(xml.contains("<valueimaplist>imaplistunread</valueimaplist>"), xml);
    assertTrue(xml.contains("<actiontype>move</actiontype>"), xml);
    assertTrue(xml.contains("<aftergetimap>delete</aftergetimap>"), xml);
    assertTrue(xml.contains("<conditionreceiveddate>greater</conditionreceiveddate>"), xml);

    Document copyDocument = XmlHandler.loadXmlString(xml);
    Node copyNode = XmlHandler.getSubNode(copyDocument, "action");
    ActionGetPOP copy =
        XmlMetadataUtil.deSerializeFromXml(
            copyNode, ActionGetPOP.class, new MemoryMetadataProvider());

    assertEquals(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD, copy.getValueIMAPList());
    assertEquals(MailConnectionMeta.ACTION_TYPE_MOVE, copy.getActionType());
    assertEquals(MailConnectionMeta.AFTER_GET_IMAP_DELETE, copy.getAfterGetIMAP());
    assertEquals(MailConnectionMeta.CONDITION_DATE_GREATER, copy.getConditionReceivedDate());
  }

  @Test
  void hop213LegacyIgnoreAndUnreadCodesLoad() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    ActionGetPOP action =
        ActionSerializationTestUtil.testSerialization(
            "/getpop-action-legacy-2.13.xml", ActionGetPOP.class, provider);

    assertEquals("IMAP", action.getProtocol());
    assertEquals(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD, action.getValueIMAPList());
    assertEquals(MailConnectionMeta.CONDITION_DATE_IGNORE, action.getConditionReceivedDate());
    assertEquals(MailConnectionMeta.AFTER_GET_IMAP_NOTHING, action.getAfterGetIMAP());
    assertEquals(MailConnectionMeta.ACTION_TYPE_GET, action.getActionType());
  }

  @Test
  void hop219IntegerCodesStillLoad() throws Exception {
    String xml =
        """
        <action>
          <name>Get mails (POP3/IMAP)</name>
          <type>GET_POP</type>
          <protocol>IMAP</protocol>
          <valueimaplist>4</valueimaplist>
          <conditionreceiveddate>0</conditionreceiveddate>
          <aftergetimap>1</aftergetimap>
          <actiontype>2</actiontype>
        </action>
        """;

    Document document = XmlHandler.loadXmlString(xml);
    Node node = XmlHandler.getSubNode(document, "action");
    ActionGetPOP action =
        XmlMetadataUtil.deSerializeFromXml(node, ActionGetPOP.class, new MemoryMetadataProvider());

    assertEquals(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD, action.getValueIMAPList());
    assertEquals(MailConnectionMeta.CONDITION_DATE_IGNORE, action.getConditionReceivedDate());
    assertEquals(MailConnectionMeta.AFTER_GET_IMAP_DELETE, action.getAfterGetIMAP());
    assertEquals(MailConnectionMeta.ACTION_TYPE_DELETE, action.getActionType());
  }
}
