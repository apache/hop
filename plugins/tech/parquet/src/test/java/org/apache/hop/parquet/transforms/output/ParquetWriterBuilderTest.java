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

package org.apache.hop.parquet.transforms.output;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.ByteArrayOutputStream;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ParquetWriterBuilder} */
class ParquetWriterBuilderTest {

  @Test
  void testGetWriteSupport() {
    Schema avroSchema =
        SchemaBuilder.record("ApacheHopParquetSchema").fields().requiredLong("id").endRecord();
    MessageType messageType = new AvroSchemaConverter().convert(avroSchema);
    List<Integer> indexes = List.of(0);
    List<ParquetField> fields = List.of(new ParquetField("id", "id"));
    ParquetOutputFile outputFile = new ParquetOutputFile(new ByteArrayOutputStream());

    ParquetWriterBuilder builder =
        new ParquetWriterBuilder(messageType, avroSchema, outputFile, indexes, fields);

    WriteSupport<?> writeSupport = builder.getWriteSupport(new Configuration());
    assertInstanceOf(ParquetWriteSupport.class, writeSupport);
  }
}
