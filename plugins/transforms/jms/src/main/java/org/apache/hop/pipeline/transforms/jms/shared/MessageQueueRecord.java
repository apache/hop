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

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * One message, independent of the broker it came from.
 *
 * <p>This is the "information about the message" half of the provider contract: a key, the body,
 * the destination it belongs to, timestamps, and free-form properties. Broker-specific concepts
 * that do not generalise stay behind {@link IMessageQueueProvider}.
 */
@Getter
@Setter
public class MessageQueueRecord {

  /** Provider-assigned identifier, e.g. the JMS message id. Null when the provider has none. */
  private String messageId;

  /**
   * Application-level key used to relate messages, e.g. the JMS correlation id. This is the field a
   * Kafka-style key maps onto.
   */
  private String key;

  /** Queue or topic name. */
  private String destination;

  /** Message body as text. */
  private String body;

  /** Broker timestamp, null when not supplied. */
  private Date timestamp;

  /** Provider-specific handle used by {@link IMessageQueueProvider#acknowledge}. */
  private Object handle;

  /** Free-form message properties/headers, in encounter order. */
  private final Map<String, String> properties = new LinkedHashMap<>();

  public MessageQueueRecord() {}

  public MessageQueueRecord(String destination, String body) {
    this.destination = destination;
    this.body = body;
  }

  public MessageQueueRecord property(String name, String value) {
    properties.put(name, value);
    return this;
  }
}
