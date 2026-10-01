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

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.InitialContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;

/**
 * {@link IMessageQueueProvider} backed by JMS 3.0.
 *
 * <p>Not thread safe: a JMS {@link Session} may only be used by one thread, so one provider belongs
 * to one transform copy.
 */
public class JmsProvider implements IMessageQueueProvider {

  private final JmsConnection connectionMeta;
  private final String destinationName;
  private final boolean topic;
  private final boolean transacted;
  private final String messageSelector;
  private final String durableSubscriptionName;
  private final ILogChannel log;

  private Connection connection;
  private Session session;
  private MessageConsumer consumer;
  private MessageProducer producer;
  private Destination destination;

  public JmsProvider(
      JmsConnection connectionMeta,
      String destinationName,
      boolean topic,
      boolean transacted,
      String messageSelector,
      String durableSubscriptionName,
      ILogChannel log) {
    this.connectionMeta = connectionMeta;
    this.destinationName = destinationName;
    this.topic = topic;
    this.transacted = transacted;
    this.messageSelector = messageSelector;
    this.durableSubscriptionName = durableSubscriptionName;
    this.log = log;
  }

  @Override
  public void connect(IVariables variables) throws HopException {
    if (connectionMeta == null) {
      throw new HopException("No JMS connection was configured");
    }
    String resolvedDestination = variables.resolve(destinationName);
    if (StringUtils.isBlank(resolvedDestination)) {
      throw new HopException("No JMS destination (queue or topic) was configured");
    }

    try {
      ConnectionFactory factory = createConnectionFactory(variables);

      String user = variables.resolve(connectionMeta.getUsername());
      String password = variables.resolve(connectionMeta.getPassword());
      connection =
          StringUtils.isEmpty(user)
              ? factory.createConnection()
              : factory.createConnection(user, password);

      String clientId = variables.resolve(connectionMeta.getClientId());
      if (StringUtils.isNotEmpty(clientId)) {
        connection.setClientID(clientId);
      }

      // AUTO_ACKNOWLEDGE would confirm the message before the pipeline has processed the row,
      // so a crash mid-pipeline would lose it. CLIENT_ACKNOWLEDGE lets the consumer confirm
      // only once the row is on its way.
      session =
          connection.createSession(
              transacted, transacted ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);

      destination =
          topic
              ? session.createTopic(resolvedDestination)
              : session.createQueue(resolvedDestination);

      connection.start();

      if (log != null) {
        log.logBasic(
            "Connected to JMS "
                + (topic ? "topic" : "queue")
                + " '"
                + resolvedDestination
                + "'"
                + (transacted ? " (transacted)" : ""));
      }
    } catch (Exception e) {
      close();
      throw new HopException(
          "Unable to connect to the JMS broker for destination '" + resolvedDestination + "'", e);
    }
  }

  private ConnectionFactory createConnectionFactory(IVariables variables) throws Exception {
    if (connectionMeta.isJndi()) {
      Hashtable<String, String> environment = new Hashtable<>();
      environment.put(
          Context.INITIAL_CONTEXT_FACTORY,
          variables.resolve(connectionMeta.getInitialContextFactory()));
      String providerUrl = variables.resolve(connectionMeta.getProviderUrl());
      if (StringUtils.isNotEmpty(providerUrl)) {
        environment.put(Context.PROVIDER_URL, providerUrl);
      }
      InitialContext context = new InitialContext(environment);
      try {
        String factoryName = variables.resolve(connectionMeta.getConnectionFactoryName());
        return (ConnectionFactory) context.lookup(factoryName);
      } finally {
        context.close();
      }
    }
    // Direct mode: the bundled Artemis client, reached reflectively so the plugin still loads
    // when the connection is JNDI-only and the Artemis jar has been removed.
    String brokerUrl = variables.resolve(connectionMeta.getBrokerUrl());
    if (StringUtils.isBlank(brokerUrl)) {
      throw new HopException(
          "No broker URL was configured on JMS connection '" + connectionMeta.getName() + "'");
    }
    Class<?> factoryClass =
        Class.forName("org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory");
    return (ConnectionFactory) factoryClass.getConstructor(String.class).newInstance(brokerUrl);
  }

  @Override
  public MessageQueueRecord receive(long timeoutMs) throws HopException {
    try {
      if (consumer == null) {
        consumer = createConsumer();
      }
      Message message = consumer.receive(timeoutMs);
      if (message == null) {
        return null; // timeout: the caller decides whether to keep waiting
      }
      return toRecord(message);
    } catch (JMSException e) {
      throw new HopException("Error receiving a message from the JMS destination", e);
    }
  }

  private MessageConsumer createConsumer() throws JMSException {
    String selector = StringUtils.trimToNull(messageSelector);
    if (topic && StringUtils.isNotEmpty(durableSubscriptionName)) {
      return session.createDurableSubscriber(
          (jakarta.jms.Topic) destination, durableSubscriptionName, selector, false);
    }
    return selector == null
        ? session.createConsumer(destination)
        : session.createConsumer(destination, selector);
  }

  /** Maps a JMS message onto the broker-independent record. Visible for testing. */
  static MessageQueueRecord toRecord(Message message) throws JMSException {
    MessageQueueRecord record = new MessageQueueRecord();
    record.setMessageId(message.getJMSMessageID());
    record.setKey(message.getJMSCorrelationID());
    record.setHandle(message);
    if (message.getJMSDestination() != null) {
      record.setDestination(message.getJMSDestination().toString());
    }
    if (message.getJMSTimestamp() > 0) {
      record.setTimestamp(new Date(message.getJMSTimestamp()));
    }
    if (message instanceof TextMessage textMessage) {
      record.setBody(textMessage.getText());
    } else {
      // Anything that is not a TextMessage still yields its properties and identifiers; the body
      // is left null rather than guessing at an encoding.
      record.setBody(null);
    }

    Enumeration<?> names = message.getPropertyNames();
    if (names != null) {
      for (String name : Collections.list((Enumeration<String>) names)) {
        Object value = message.getObjectProperty(name);
        record.property(name, value == null ? null : String.valueOf(value));
      }
    }
    return record;
  }

  @Override
  public void acknowledge(MessageQueueRecord record) throws HopException {
    try {
      if (transacted) {
        session.commit();
        return;
      }
      if (record != null && record.getHandle() instanceof Message message) {
        message.acknowledge();
      }
    } catch (JMSException e) {
      throw new HopException("Error acknowledging a JMS message", e);
    }
  }

  @Override
  public void send(MessageQueueRecord record) throws HopException {
    try {
      if (producer == null) {
        producer = session.createProducer(destination);
      }
      TextMessage message = session.createTextMessage(record.getBody());
      if (StringUtils.isNotEmpty(record.getKey())) {
        message.setJMSCorrelationID(record.getKey());
      }
      for (var property : record.getProperties().entrySet()) {
        if (property.getValue() != null) {
          message.setStringProperty(property.getKey(), property.getValue());
        }
      }
      producer.send(message);
      if (transacted) {
        session.commit();
      }
    } catch (JMSException e) {
      throw new HopException("Error sending a message to the JMS destination", e);
    }
  }

  @Override
  public void close() {
    // Closing the connection closes its sessions, consumers and producers, but each is closed
    // explicitly so a failure in one does not leave the others open.
    closeQuietly(consumer);
    closeQuietly(producer);
    closeQuietly(session);
    closeQuietly(connection);
    consumer = null;
    producer = null;
    session = null;
    connection = null;
  }

  private void closeQuietly(AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception e) {
      if (log != null) {
        log.logDebug("Ignoring error while closing a JMS resource: " + e.getMessage());
      }
    }
  }
}
