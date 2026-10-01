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

package org.apache.hop.pipeline.transforms.jms.consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class JmsConsumerMetaTest {

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void onlyNamedFieldsAppearInTheOutputRow() throws Exception {
    JmsConsumerMeta meta = new JmsConsumerMeta();
    meta.setBodyField("message");
    meta.setKeyField(""); // left out
    meta.setDestinationField("queue");
    meta.setMessageIdField("");
    meta.setTimestampField("sent_at");

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("discarded"));
    meta.getFields(rowMeta, "jms", null, null, new Variables(), null);

    assertArrayEquals(new String[] {"message", "queue", "sent_at"}, rowMeta.getFieldNames());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(0).getType());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(1).getType());
    assertEquals(
        IValueMeta.TYPE_DATE, rowMeta.getValueMeta(2).getType(), "the timestamp must be a Date");
  }

  @Test
  void fieldNamesResolveVariables() throws Exception {
    JmsConsumerMeta meta = new JmsConsumerMeta();
    meta.setBodyField("${BODY_FIELD}");
    meta.setKeyField("");
    meta.setDestinationField("");
    meta.setMessageIdField("");
    meta.setTimestampField("");

    IVariables variables = new Variables();
    variables.setVariable("BODY_FIELD", "payload");

    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "jms", null, null, variables, null);

    assertArrayEquals(new String[] {"payload"}, rowMeta.getFieldNames());
  }

  @Test
  void outputRowIsEmptyWhenNoFieldIsNamed() throws Exception {
    JmsConsumerMeta meta = new JmsConsumerMeta();
    meta.setBodyField("");
    meta.setKeyField("");
    meta.setDestinationField("");
    meta.setMessageIdField("");
    meta.setTimestampField("");

    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "jms", null, null, new Variables(), null);

    assertEquals(0, rowMeta.size());
  }

  @Test
  void destinationTypeDrivesTopicMode() {
    JmsConsumerMeta meta = new JmsConsumerMeta();
    assertFalse(meta.isTopic(), "queue is the default");

    meta.setDestinationType("TOPIC");
    assertTrue(meta.isTopic());

    meta.setDestinationType("topic");
    assertTrue(meta.isTopic(), "the comparison must be case insensitive");
  }

  @Test
  void destinationTypeParsingFallsBackToQueue() {
    assertEquals(JmsDestinationType.QUEUE, JmsDestinationType.of(null));
    assertEquals(JmsDestinationType.QUEUE, JmsDestinationType.of("nonsense"));
    assertEquals(JmsDestinationType.TOPIC, JmsDestinationType.of("  topic  "));
    assertEquals(JmsDestinationType.QUEUE, JmsDestinationType.of("QUEUE"));
  }
}
