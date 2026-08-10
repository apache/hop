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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link JmsProvider} against a real embedded Apache ActiveMQ Artemis broker: connect,
 * send, receive, acknowledge and redelivery. These are the behaviours that mocks cannot prove.
 *
 * <p>The broker runs in-process on a random-free port, so the test needs no Docker and no external
 * service.
 */
class JmsProviderBrokerTest {

  private static final int PORT = 61618;
  private static final String BROKER_URL = "tcp://localhost:" + PORT;

  private static EmbeddedActiveMQ broker;
  private static Path dataDir;

  @BeforeAll
  static void startBroker() throws Exception {
    dataDir = Files.createTempDirectory("hop-jms-it");
    ConfigurationImpl configuration = new ConfigurationImpl();
    configuration.setPersistenceEnabled(false);
    configuration.setSecurityEnabled(false);
    configuration.setJournalDirectory(dataDir.resolve("journal").toString());
    configuration.setBindingsDirectory(dataDir.resolve("bindings").toString());
    configuration.setLargeMessagesDirectory(dataDir.resolve("large").toString());
    configuration.setPagingDirectory(dataDir.resolve("paging").toString());
    configuration.addAcceptorConfiguration("tcp", BROKER_URL);

    broker = new EmbeddedActiveMQ();
    broker.setConfiguration(configuration);
    broker.start();
  }

  @AfterAll
  static void stopBroker() throws Exception {
    if (broker != null) {
      broker.stop();
    }
    if (dataDir != null) {
      try (var paths = Files.walk(dataDir)) {
        paths.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
      }
    }
  }

  private static JmsConnection connection() {
    JmsConnection connection = new JmsConnection("embedded");
    connection.setMode(JmsConnection.MODE_DIRECT);
    connection.setBrokerUrl(BROKER_URL);
    return connection;
  }

  private static JmsProvider provider(String destination, boolean topic, boolean transacted) {
    return new JmsProvider(connection(), destination, topic, transacted, null, null, null);
  }

  @Test
  void roundTripsAMessageThroughAQueue() throws Exception {
    IVariables variables = new Variables();
    MessageQueueRecord sent = new MessageQueueRecord("ignored", "{\"id\":42}");
    sent.setKey("corr-42");
    sent.property("region", "EU");

    try (JmsProvider producer = provider("hop.it.roundtrip", false, false)) {
      producer.connect(variables);
      producer.send(sent);
    }

    try (JmsProvider consumer = provider("hop.it.roundtrip", false, false)) {
      consumer.connect(variables);
      MessageQueueRecord received = consumer.receive(5000);

      assertNotNull(received, "the message should have been delivered");
      assertEquals("{\"id\":42}", received.getBody());
      assertEquals("corr-42", received.getKey());
      assertEquals("EU", received.getProperties().get("region"));
      assertNotNull(received.getMessageId(), "the broker assigns a message id");
      assertTrue(received.getDestination().contains("hop.it.roundtrip"));
      consumer.acknowledge(received);
    }
  }

  @Test
  void receiveReturnsNullOnAnEmptyQueueRatherThanFailing() throws Exception {
    try (JmsProvider consumer = provider("hop.it.empty", false, false)) {
      consumer.connect(new Variables());
      assertNull(consumer.receive(300), "a timeout is normal, not an error");
    }
  }

  @Test
  void unacknowledgedMessageIsRedelivered() throws Exception {
    IVariables variables = new Variables();
    try (JmsProvider producer = provider("hop.it.redelivery", false, false)) {
      producer.connect(variables);
      producer.send(new MessageQueueRecord(null, "keep-me"));
    }

    // Receive without acknowledging, then drop the connection: the broker must give it back.
    try (JmsProvider consumer = provider("hop.it.redelivery", false, false)) {
      consumer.connect(variables);
      assertEquals("keep-me", consumer.receive(5000).getBody());
    }

    try (JmsProvider consumer = provider("hop.it.redelivery", false, false)) {
      consumer.connect(variables);
      MessageQueueRecord redelivered = consumer.receive(5000);
      assertNotNull(redelivered, "an unacknowledged message must not be lost");
      assertEquals("keep-me", redelivered.getBody());
      consumer.acknowledge(redelivered);
    }

    // Now that it was acknowledged, it is gone.
    try (JmsProvider consumer = provider("hop.it.redelivery", false, false)) {
      consumer.connect(variables);
      assertNull(consumer.receive(500), "an acknowledged message must not be redelivered");
    }
  }

  @Test
  void publishesAndSubscribesOnATopic() throws Exception {
    IVariables variables = new Variables();
    try (JmsProvider subscriber = provider("hop.it.topic", true, false);
        JmsProvider publisher = provider("hop.it.topic", true, false)) {
      subscriber.connect(variables);
      // Force the subscription to exist before publishing; a topic drops messages with no
      // subscriber, so this ordering is the point of the test.
      assertNull(subscriber.receive(200));

      publisher.connect(variables);
      publisher.send(new MessageQueueRecord(null, "broadcast"));

      MessageQueueRecord received = subscriber.receive(5000);
      assertNotNull(received, "the subscriber should see the published message");
      assertEquals("broadcast", received.getBody());
    }
  }

  @Test
  void transactedSendIsVisibleAfterCommit() throws Exception {
    IVariables variables = new Variables();
    try (JmsProvider producer = provider("hop.it.tx", false, true)) {
      producer.connect(variables);
      producer.send(new MessageQueueRecord(null, "committed"));
    }
    try (JmsProvider consumer = provider("hop.it.tx", false, false)) {
      consumer.connect(variables);
      MessageQueueRecord received = consumer.receive(5000);
      assertNotNull(received);
      assertEquals("committed", received.getBody());
      consumer.acknowledge(received);
    }
  }

  @Test
  void variablesAreResolvedInTheDestinationName() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable("QUEUE_NAME", "hop.it.from.variable");

    try (JmsProvider producer = provider("${QUEUE_NAME}", false, false)) {
      producer.connect(variables);
      producer.send(new MessageQueueRecord(null, "resolved"));
    }
    try (JmsProvider consumer = provider("hop.it.from.variable", false, false)) {
      consumer.connect(variables);
      assertEquals("resolved", consumer.receive(5000).getBody());
    }
  }

  @Test
  void aBlankDestinationIsRejectedBeforeConnecting() {
    try (JmsProvider producer = provider("   ", false, false)) {
      HopException e = assertThrows(HopException.class, () -> producer.connect(new Variables()));
      assertTrue(e.getMessage().contains("destination"), e.getMessage());
    }
  }

  @Test
  void anUnreachableBrokerFailsWithTheDestinationInTheMessage() {
    JmsConnection unreachable = connection();
    unreachable.setBrokerUrl("tcp://localhost:1");
    try (JmsProvider producer =
        new JmsProvider(unreachable, "hop.it.unreachable", false, false, null, null, null)) {
      HopException e = assertThrows(HopException.class, () -> producer.connect(new Variables()));
      assertTrue(e.getMessage().contains("hop.it.unreachable"), e.getMessage());
    }
  }

  @Test
  void closeIsIdempotent() throws Exception {
    JmsProvider provider = provider("hop.it.close", false, false);
    provider.connect(new Variables());
    provider.close();
    provider.close(); // must not throw
  }
}
