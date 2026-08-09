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

package org.apache.hop.pipeline.transforms.jms.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.jms.BytesMessage;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import java.util.Collections;
import java.util.Enumeration;
import org.junit.jupiter.api.Test;

/** Covers the JMS-to-Hop mapping, which is the part that can go wrong without a broker. */
class JmsProviderTest {

  @Test
  void mapsTextMessageOntoRecord() throws Exception {
    Destination destination = mock(Destination.class);
    when(destination.toString()).thenReturn("orders");

    TextMessage message = mock(TextMessage.class);
    when(message.getJMSMessageID()).thenReturn("ID:1");
    when(message.getJMSCorrelationID()).thenReturn("corr-7");
    when(message.getJMSDestination()).thenReturn(destination);
    when(message.getJMSTimestamp()).thenReturn(1_700_000_000_000L);
    when(message.getText()).thenReturn("{\"id\":1}");
    when(message.getPropertyNames()).thenReturn(enumerationOf("region"));
    when(message.getObjectProperty("region")).thenReturn("EU");

    MessageQueueRecord record = JmsProvider.toRecord(message);

    assertEquals("ID:1", record.getMessageId());
    assertEquals("corr-7", record.getKey());
    assertEquals("orders", record.getDestination());
    assertEquals("{\"id\":1}", record.getBody());
    assertEquals(1_700_000_000_000L, record.getTimestamp().getTime());
    assertEquals("EU", record.getProperties().get("region"));
    assertSame(message, record.getHandle(), "the handle must allow acknowledging the message");
  }

  @Test
  void leavesBodyNullForNonTextMessagesRatherThanGuessingAnEncoding() throws Exception {
    BytesMessage message = mock(BytesMessage.class);
    when(message.getJMSMessageID()).thenReturn("ID:2");
    when(message.getPropertyNames()).thenReturn(enumerationOf());

    MessageQueueRecord record = JmsProvider.toRecord(message);

    assertNull(record.getBody());
    assertEquals("ID:2", record.getMessageId(), "identifiers still come through");
  }

  @Test
  void omitsTimestampWhenTheBrokerDidNotSetOne() throws Exception {
    Message message = mock(Message.class);
    when(message.getJMSTimestamp()).thenReturn(0L);
    when(message.getPropertyNames()).thenReturn(enumerationOf());

    assertNull(JmsProvider.toRecord(message).getTimestamp());
  }

  @Test
  void toleratesNullPropertyValues() throws Exception {
    Message message = mock(Message.class);
    when(message.getPropertyNames()).thenReturn(enumerationOf("empty"));
    when(message.getObjectProperty("empty")).thenReturn(null);

    MessageQueueRecord record = JmsProvider.toRecord(message);

    assertTrue(record.getProperties().containsKey("empty"));
    assertNull(record.getProperties().get("empty"));
  }

  private static Enumeration<String> enumerationOf(String... names) {
    return Collections.enumeration(java.util.Arrays.asList(names));
  }
}
