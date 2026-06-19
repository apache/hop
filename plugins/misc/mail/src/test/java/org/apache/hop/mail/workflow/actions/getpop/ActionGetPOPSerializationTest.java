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
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
  }
}
