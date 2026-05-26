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

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
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

    ParquetWriteSupport support = new ParquetWriteSupport(messageType, avroSchema, indexes, fields);

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

    ParquetWriteSupport support = new ParquetWriteSupport(messageType, avroSchema, indexes, fields);
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
}
