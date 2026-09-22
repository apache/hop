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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ParquetWriteSupport} */
class ParquetWriteSupportTest {

  @Test
  void testInitReturnsWriteContext() {
    Schema avroSchema =
        SchemaBuilder.record("ApacheHopParquetSchema")
            .fields()
            .requiredLong("id")
            .requiredString("name")
            .endRecord();
    MessageType messageType = new AvroSchemaConverter().convert(avroSchema);
    List<Integer> indexes = List.of(0, 1);
    List<ParquetField> fields =
        List.of(new ParquetField("id", "id"), new ParquetField("name", "name"));

    ParquetWriteSupport support = new ParquetWriteSupport(messageType, indexes, fields);

    WriteSupport.WriteContext context = support.init(new Configuration());
    org.junit.jupiter.api.Assertions.assertEquals(messageType, context.getSchema());
  }

  @Test
  void testWriteIntegerAndString() throws Exception {
    Schema avroSchema =
        SchemaBuilder.record("ApacheHopParquetSchema")
            .fields()
            .requiredLong("id")
            .requiredString("name")
            .endRecord();
    MessageType messageType = new AvroSchemaConverter().convert(avroSchema);
    List<Integer> indexes = List.of(0, 1);
    List<ParquetField> fields =
        List.of(new ParquetField("id", "id"), new ParquetField("name", "name"));

    ParquetWriteSupport support = new ParquetWriteSupport(messageType, indexes, fields);
    RecordConsumer consumer = mock(RecordConsumer.class);
    support.prepareForWrite(consumer);

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    support.write(new RowMetaAndData(rowMeta, new Object[] {42L, "Alice"}));

    verify(consumer).startMessage();
    verify(consumer).startField("id", 0);
    verify(consumer).addLong(42L);
    verify(consumer).endField("id", 0);
    verify(consumer).startField("name", 1);
    verify(consumer).endField("name", 1);
    verify(consumer).endMessage();
  }

  @Test
  void testWriteDateAndTimestampAsEpochMillis() throws Exception {
    Schema avroSchema =
        SchemaBuilder.record("ApacheHopParquetSchema")
            .fields()
            .requiredLong("d")
            .requiredLong("ts")
            .requiredLong("lazy")
            .endRecord();
    MessageType messageType = new AvroSchemaConverter().convert(avroSchema);
    List<Integer> indexes = List.of(0, 1, 2);
    List<ParquetField> fields =
        List.of(
            new ParquetField("d", "d"),
            new ParquetField("ts", "ts"),
            new ParquetField("lazy", "lazy"));

    ParquetWriteSupport support = new ParquetWriteSupport(messageType, indexes, fields);
    RecordConsumer consumer = mock(RecordConsumer.class);
    support.prepareForWrite(consumer);

    // A timestamp still stored as the text it was read from, as a lazy-conversion input leaves it.
    ValueMetaTimestamp lazy = new ValueMetaTimestamp("lazy");
    lazy.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    ValueMetaString lazyStorage = new ValueMetaString("lazy");
    lazyStorage.setConversionMask("yyyy-MM-dd HH:mm:ss.SSS");
    lazyStorage.setDateFormatTimeZone(TimeZone.getTimeZone("UTC"));
    lazy.setStorageMetadata(lazyStorage);

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("d"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("ts"));
    rowMeta.addValueMeta(lazy);
    support.write(
        new RowMetaAndData(
            rowMeta,
            new Object[] {
              new Date(1_000L),
              new Timestamp(2_000L),
              "1970-01-01 00:00:03.000".getBytes(StandardCharsets.UTF_8)
            }));

    verify(consumer).addLong(1_000L);
    verify(consumer).addLong(2_000L);
    verify(consumer).addLong(3_000L);
  }
}
