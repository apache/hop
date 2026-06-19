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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ChatMessageType;
import java.util.List;
import java.util.Optional;
import org.apache.hop.core.exception.HopValueException;
import org.junit.jupiter.api.Test;

class MessageTest {

  @Test
  void toChatMessages_systemAndAssistant() throws HopValueException {
    Message sys = new Message();
    sys.setRole("system");
    sys.setContent("s");
    Message asst = new Message();
    asst.setRole("assistant");
    asst.setContent("a");
    assertEquals(1, Message.toChatMessages(List.of(sys)).size());
    assertEquals(1, Message.toChatMessages(List.of(asst)).size());
  }

  @Test
  void toChatMessages_userText() throws HopValueException {
    Message m = new Message();
    m.setRole("user");
    m.setContent("hello");
    List<ChatMessage> out = Message.toChatMessages(List.of(m));
    assertEquals(1, out.size());
    assertEquals(ChatMessageType.USER, out.get(0).type());
  }

  @Test
  void toChatMessages_missingRole_throws() {
    Message m = new Message();
    m.setContent("orphan content");
    NullPointerException ex =
        assertThrows(NullPointerException.class, () -> Message.toChatMessages(List.of(m)));
    assertTrue(ex.getMessage().contains("role"));
  }

  @Test
  void toChatMessages_invalidRole() {
    Message m = new Message();
    m.setRole("unknown-role");
    m.setContent("x");
    assertThrows(HopValueException.class, () -> Message.toChatMessages(List.of(m)));
  }

  @Test
  void detectRoleNames() {
    Message u = new Message();
    u.setRole("user");
    Message s = new Message();
    s.setRole("system");
    Message a = new Message();
    a.setRole("assistant");
    assertEquals(Optional.of("user"), Message.detectUserRoleName(List.of(u, s)));
    assertEquals(Optional.of("system"), Message.detectSystemRoleName(List.of(u, s)));
    assertEquals(Optional.of("assistant"), Message.detectAssistantRoleName(List.of(u, a)));
  }

  @Test
  void detectRoleNames_empty() {
    assertTrue(Message.detectUserRoleName(List.of()).isEmpty());
  }
}
