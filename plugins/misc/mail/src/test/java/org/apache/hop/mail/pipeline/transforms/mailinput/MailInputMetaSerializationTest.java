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
package org.apache.hop.mail.pipeline.transforms.mailinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MailInputMetaSerializationTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void deserializesFromXmlSnippetAndRoundTrips() throws Exception {
    MailInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/mailinput-transform.xml", MailInputMeta.class);

    assertEquals("imap.example.com", meta.getServerName());
    assertEquals("alice", meta.getUsername());
    assertTrue(meta.isUseSsl());
    assertEquals("993", meta.getSslPort());
    assertEquals("IMAP", meta.getProtocol());
    assertEquals("INBOX", meta.getImapFolder());
    assertEquals("10", meta.getImapFirstMails());
    assertFalse(meta.isDelete());

    assertEquals("boss@example.com", meta.getSenderSearch());
    assertEquals("me@example.com", meta.getRecipientSearch());
    assertEquals("invoice", meta.getSubjectSearch());

    assertEquals(2, meta.getInputFields().size());
    assertEquals("messageNumber", meta.getInputFields().get(0).getName());
    assertEquals("subject", meta.getInputFields().get(1).getName());
  }
}
