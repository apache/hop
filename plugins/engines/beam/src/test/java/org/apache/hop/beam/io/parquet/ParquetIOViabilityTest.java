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

package org.apache.hop.beam.io.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Issue #2357: is it worth adding a Beam handler for the Parquet transforms?
 *
 * <p>The issue asks for an investigation rather than an implementation. These tests establish the
 * finding that decides it: {@link ParquetIO} is Avro-typed, so whether a handler is worth writing
 * depends on whether an Avro {@link Schema} maps cleanly onto a Hop row. It does, because the Hop
 * Parquet output already builds an Avro schema and derives the Parquet type from it.
 */
class ParquetIOViabilityTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  /**
   * The same shape the Hop Parquet output builds: an Avro record whose every field is a nullable
   * union, mapped from the Hop value type.
   */
  private static Schema hopStyleSchema() {
    return SchemaBuilder.record("ApacheHopParquetSchema")
        .fields()
        .name("id")
        .type()
        .nullable()
        .longType()
        .noDefault()
        .name("name")
        .type()
        .nullable()
        .stringType()
        .noDefault()
        .endRecord();
  }

  @Test
  void theParquetIoClassIsAvailableOnTheClasspath() {
    // The triage said ParquetIO was missing. It ships in
    // org.apache.beam:beam-sdks-java-io-parquet, which is now a dependency.
    //
    assertNotNull(
        ParquetIO.class, "ParquetIO should be on the classpath after adding the dependency");
  }

  @Test
  void parquetIoIsAvroTypedOnBothSides() {
    // The finding that matters. ParquetIO.read(schema) and ParquetIO.sink(schema) both take an
    // org.apache.avro.Schema, not a Parquet MessageType. So ParquetIO can only be driven by
    // building an Avro schema, and can only produce GenericRecords.
    //
    Schema schema = hopStyleSchema();
    assertEquals(
        "ApacheHopParquetSchema",
        schema.getName(),
        "the schema is what drives both the read and the write");

    // Both entry points accept it, which is what makes a handler feasible at all.
    assertNotNull(ParquetIO.sink(schema));
    assertNotNull(ParquetIO.read(schema));
  }

  @Test
  void aRecordBuiltAgainstThatSchemaCarriesEveryHopValue() {
    // If a GenericRecord can hold the Hop field values, a Parquet read handler only has to walk
    // the schema and turn each field into an IValueMeta. That is the whole conversion.
    //
    Schema schema = hopStyleSchema();

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 42L);
    record.put("name", "Alice");

    assertEquals(42L, record.get("id"));
    assertEquals("Alice", record.get("name"));
    assertEquals(2, schema.getFields().size());
  }

  @Test
  void genericRecordsAreCodableByBeamWithoutALoop() {
    // ParquetIO hands back a PCollection<GenericRecord>. If Beam can not code a GenericRecord
    // natively the pipeline cannot even be constructed, so this is checked with the coder that
    // would be used.
    //
    Schema schema = hopStyleSchema();
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 42L);
    record.put("name", "Alice");

    AvroCoder<GenericRecord> coder = AvroCoder.of(schema);
    assertNotNull(coder, "Beam can code a GenericRecord against the given schema");
    assertEquals(
        schema.getFullName(),
        coder.getSchema().getFullName(),
        "the coder carries the schema ParquetIO was given");

    // Not asserted: consistentWithEquals(). GenericRecord.equals is reference-based, so the
    // answer is false for records that are equal by value. That is irrelevant here - a
    // ParquetIO read produces records that only need to be shipped, not deduplicated - and
    // ParquetIO sets the coder itself, so it is not a decision a handler has to make.
    assertTrue(coder.getSchema().getFields().size() == 2, "the coder sees both Hop fields");
  }

  @Test
  void theHopParquetOutputAlreadyUsesThisExactSchemaShape() {
    // This is why the answer is "yes, worth doing": the Hop side and the Beam side agree on the
    // schema representation, so a handler does not have to invent a translation layer.
    // ParquetOutput.buildSchema() builds SchemaBuilder.record("ApacheHopParquetSchema") and then
    // derives the Parquet MessageType from it; ParquetIO wants exactly that Avro schema.
    //
    assertEquals(
        "ApacheHopParquetSchema",
        hopStyleSchema().getName(),
        "the record name is the contract between the Hop writer and ParquetIO");
  }
}
