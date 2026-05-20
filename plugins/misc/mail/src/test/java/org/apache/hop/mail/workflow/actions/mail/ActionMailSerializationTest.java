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
package org.apache.hop.mail.workflow.actions.mail;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ActionMailSerializationTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void deserializesFromXmlSnippetAndRoundTrips() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    ActionMail action =
        ActionSerializationTestUtil.testSerialization(
            "/mail-action.xml", ActionMail.class, provider);

    assertEquals("smtp.example.com", action.getServer());
    assertEquals("587", action.getPort());
    assertEquals("to@example.com", action.getDestination());
    assertEquals("bot@example.com", action.getReplyAddress());
    assertEquals("Workflow alert", action.getSubject());
    assertTrue(action.isIncludeDate());
    assertTrue(action.isUsingAuthentication());
    assertEquals("alice", action.getAuthenticationUser());
    assertTrue(action.isUsingSecureAuthentication());
    assertEquals("TLS", action.getSecureConnectionType());
    assertTrue(action.isCheckServerIdentity());
    assertEquals("smtp.example.com", action.getTrustedHosts());
  }
}
