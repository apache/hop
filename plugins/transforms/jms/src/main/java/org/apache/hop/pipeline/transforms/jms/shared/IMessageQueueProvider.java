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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;

/**
 * A message broker, seen from a Hop transform.
 *
 * <p>The operations are the ones a transform actually needs, independent of broker: connect and
 * disconnect, get the next message, read information about it, acknowledge it, and send one. See <a
 * href="https://github.com/apache/hop/issues/2653">issue #2653</a>, where this shape was proposed.
 *
 * <p>The interface deliberately lives inside this plugin for now rather than being a Hop plugin
 * type. It is exercised here by a single JMS implementation; promoting it to a plugin type is worth
 * doing once a second, structurally different provider (Kafka, native AMQP 1.0) has been written
 * against it and the shape has been confirmed by more than one caller.
 */
public interface IMessageQueueProvider extends AutoCloseable {

  /**
   * Opens the connection and prepares the destination. Called once when the transform starts.
   *
   * @param variables used to resolve any configuration that supports variables
   */
  void connect(IVariables variables) throws HopException;

  /**
   * Returns the next message, or null when {@code timeoutMs} elapses without one arriving.
   *
   * <p>Returning null is normal, not an error: it is how a consumer notices it should check whether
   * the pipeline is stopping.
   *
   * @param timeoutMs how long to wait, in milliseconds
   */
  MessageQueueRecord receive(long timeoutMs) throws HopException;

  /**
   * Confirms that {@code record} has been processed, so the broker can drop it.
   *
   * <p>A no-op for providers that acknowledge automatically.
   */
  void acknowledge(MessageQueueRecord record) throws HopException;

  /** Publishes {@code record}. The destination on the record wins when it is set. */
  void send(MessageQueueRecord record) throws HopException;

  /** Closes the connection. Must be safe to call more than once, and must not throw. */
  @Override
  void close();
}
