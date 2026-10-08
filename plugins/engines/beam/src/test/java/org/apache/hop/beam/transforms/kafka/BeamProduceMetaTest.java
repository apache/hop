/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.transforms.kafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.avro.Schema;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Issue #2675 (producer side): an Avro message field with no schema must fail with a message that
 * names the problem.
 *
 * <p>Without the check, {@code AvroCoder.of(null)} throws a bare NullPointerException from deep
 * inside Beam's determinism checker, before the pipeline is even submitted.
 */
class BeamProduceMetaTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  private static BeamProduceMeta meta(String messageField) {
    BeamProduceMeta meta = new BeamProduceMeta();
    meta.setMessageField(messageField);
    meta.setKeyField("key");
    return meta;
  }

  /** key String, avro Avro Record with no schema attached. */
  private static IRowMeta rowMetaWithoutSchema() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("key"));
    rowMeta.addValueMeta(new ValueMetaAvroRecord("avro"));
    return rowMeta;
  }

  /** key String, avro Avro Record with a real schema. */
  private static IRowMeta rowMetaWithSchema() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("key"));
    ValueMetaAvroRecord avro = new ValueMetaAvroRecord("avro");
    avro.setSchema(
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Row\",\"fields\":[{\"name\":\"id\","
                    + "\"type\":\"long\"}]}"));
    rowMeta.addValueMeta(avro);
    return rowMeta;
  }

  @Test
  void anAvroFieldWithoutASchemaFailsWithAClearMessage() {
    BeamProduceMeta meta = meta("avro");

    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                meta.checkAvroMessageSchema(
                    rowMetaWithoutSchema(), Variables.getADefaultVariableSpace()));

    assertTrue(
        exception.getMessage().toLowerCase().contains("schema"),
        "the message should name the missing schema, was: " + exception.getMessage());
    assertTrue(
        exception.getMessage().contains("avro"),
        "the message should name the field, was: " + exception.getMessage());
  }

  @Test
  void anAvroFieldWithASchemaIsAccepted() throws Exception {
    BeamProduceMeta meta = meta("avro");

    assertDoesNotThrow(
        () ->
            meta.checkAvroMessageSchema(rowMetaWithSchema(), Variables.getADefaultVariableSpace()));
  }

  @Test
  void aPlainStringMessageFieldIsAccepted() {
    // Not every Kafka produce transform sends Avro; a String message has no schema to check.
    BeamProduceMeta meta = meta("message");

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("key"));
    rowMeta.addValueMeta(new ValueMetaString("message"));

    assertDoesNotThrow(
        () -> meta.checkAvroMessageSchema(rowMeta, Variables.getADefaultVariableSpace()));
  }

  @Test
  void anUnknownMessageFieldFailsWithAClearMessage() {
    BeamProduceMeta meta = meta("nope");

    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                meta.checkAvroMessageSchema(
                    rowMetaWithoutSchema(), Variables.getADefaultVariableSpace()));

    assertTrue(
        exception.getMessage().contains("nope"),
        "the message should name the missing field, was: " + exception.getMessage());
  }
}
