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

package org.apache.hop.beam.core.coder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Issue #2358: can an Avro record survive a Hop row on Beam?
 *
 * <p>This is the question that decides whether an Avro transform is worth building on Beam. The
 * Avro input transform puts a {@code GenericRecord}, with a schema only known once the file has
 * been read, into a single Hop row field. Hop rows cross worker boundaries through {@link
 * HopRowCoder}, which is plain Java serialization, so the record has to survive that.
 *
 * <p>If this does not hold, no handler helps: the row itself cannot be shipped.
 */
class HopRowCoderAvroTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  private static GenericRecord sampleRecord() {
    Schema schema =
        SchemaBuilder.record("customer")
            .fields()
            .requiredString("name")
            .requiredInt("age")
            .endRecord();

    GenericRecord record = new GenericData.Record(schema);
    record.put("name", "Alice");
    record.put("age", 42);
    return record;
  }

  @Test
  void aGenericRecordSurvivesTheHopRowCoder() throws Exception {
    HopRowCoder coder = new HopRowCoder();

    // Long, not Integer: HopRowCoder is type-tagged and only handles the canonical Java
    // types, so an Integer would be rejected before the Avro field is even reached.
    //
    HopRow row = new HopRow(new Object[] {1L, "first", sampleRecord()});

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(row, out);

    HopRow decoded = coder.decode(new ByteArrayInputStream(out.toByteArray()));

    assertEquals(3, decoded.getRow().length);
    assertEquals(1L, decoded.getRow()[0]);
    assertEquals("first", decoded.getRow()[1]);

    Object third = decoded.getRow()[2];
    assertNotNull(third, "the Avro record should survive the round trip");

    GenericRecord back = (GenericRecord) third;
    assertEquals("Alice", back.get("name").toString());
    assertEquals(42, back.get("age"));
    assertEquals("customer", back.getSchema().getName(), "the schema has to come along too");
  }

  @Test
  void aNestedAvroRecordAlsoSurvives() throws Exception {
    // Avro schemas nest, and the decode transform reads nested fields, so a nested record is the
    // case that matters in practice.
    //
    HopRowCoder coder = new HopRowCoder();

    Schema addressSchema =
        SchemaBuilder.record("address").fields().requiredString("city").endRecord();
    Schema customerSchema =
        SchemaBuilder.record("customer")
            .fields()
            .name("name")
            .type()
            .stringType()
            .noDefault()
            .name("address")
            .type(addressSchema)
            .noDefault()
            .endRecord();

    GenericRecord address = new GenericData.Record(addressSchema);
    address.put("city", "Ghent");
    GenericRecord customer = new GenericData.Record(customerSchema);
    customer.put("name", "Bob");
    customer.put("address", address);

    HopRow row = new HopRow(new Object[] {customer});
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(row, out);

    HopRow decoded = coder.decode(new ByteArrayInputStream(out.toByteArray()));
    GenericRecord back = (GenericRecord) decoded.getRow()[0];
    GenericRecord backAddress = (GenericRecord) back.get("address");

    assertEquals("Bob", back.get("name").toString());
    assertEquals("Ghent", backAddress.get("city").toString());
  }

  @Test
  void aNullAvroFieldSurvivesTheCoder() throws Exception {
    HopRowCoder coder = new HopRowCoder();

    HopRow row = new HopRow(new Object[] {1L, null});
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(row, out);

    HopRow decoded = coder.decode(new ByteArrayInputStream(out.toByteArray()));
    assertEquals(2, decoded.getRow().length);
    assertEquals(null, decoded.getRow()[1]);
  }
}
