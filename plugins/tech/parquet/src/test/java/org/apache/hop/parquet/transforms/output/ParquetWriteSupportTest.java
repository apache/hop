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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
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

  private static final DecimalLogicalTypeAnnotation DECIMAL_10_2 =
      (DecimalLogicalTypeAnnotation) LogicalTypeAnnotation.decimalType(2, 10);

  private static BigDecimal decimal(String fieldValue, ValueMetaBigNumber valueMeta) {
    byte[] bytes =
        ParquetWriteSupport.decimalBytes("n", valueMeta, new BigDecimal(fieldValue), DECIMAL_10_2)
            .getBytes();
    return new BigDecimal(new BigInteger(bytes), DECIMAL_10_2.getScale());
  }

  @Test
  void testDecimalIsRoundedTheWayTheFieldRounds() {
    ValueMetaBigNumber halfEven = new ValueMetaBigNumber("n");
    assertEquals(new BigDecimal("1234.56"), decimal("1234.565", halfEven));
    assertEquals(new BigDecimal("1234.58"), decimal("1234.575", halfEven));

    ValueMetaBigNumber halfUp = new ValueMetaBigNumber("n");
    halfUp.setRoundingType("half_up");
    assertEquals(new BigDecimal("1234.57"), decimal("1234.565", halfUp));

    ValueMetaBigNumber unknown = new ValueMetaBigNumber("n");
    unknown.setRoundingType("sideways");
    assertEquals(new BigDecimal("1234.56"), decimal("1234.565", unknown));
  }

  @Test
  void testDecimalWhichDoesNotFitFailsWithTheFieldName() {
    // DECIMAL(10,2) leaves 8 digits before the point.
    assertEquals(
        new BigDecimal("99999999.99"), decimal("99999999.99", new ValueMetaBigNumber("n")));

    HopRuntimeException e =
        assertThrows(
            HopRuntimeException.class,
            () ->
                ParquetWriteSupport.decimalBytes(
                    "amount",
                    new ValueMetaBigNumber("amount"),
                    new BigDecimal("123456789.00"),
                    DECIMAL_10_2));
    assertTrue(e.getMessage().contains("'amount'"), e.getMessage());
    assertTrue(e.getMessage().contains("DECIMAL(10,2)"), e.getMessage());
  }

  @Test
  void testTimestampMicrosKeepTheFractionAlsoBeforeTheEpoch() throws Exception {
    LogicalTypeAnnotation micros =
        LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
    ValueMetaTimestamp valueMeta = new ValueMetaTimestamp("ts");

    Timestamp after = new Timestamp(1_000L);
    after.setNanos(123_456_789);
    assertEquals(1_123_456L, ParquetWriteSupport.epochValue(valueMeta, after, micros));

    // One microsecond before 1970: -1, not -1001 or +999999.
    Timestamp before = new Timestamp(-1_000L);
    before.setNanos(999_999_000);
    assertEquals(-1L, ParquetWriteSupport.epochValue(valueMeta, before, micros));

    // A Date in a TIMESTAMP(MICROS) column: its milliseconds, in microseconds.
    assertEquals(
        2_000_000L,
        ParquetWriteSupport.epochValue(new ValueMetaDate("d"), new Date(2_000L), micros));

    // Any other column: milliseconds.
    assertEquals(1_123L, ParquetWriteSupport.epochValue(valueMeta, after, null));
  }

  @Test
  void testUuidBytesAreBigEndian() {
    byte[] bytes = ParquetWriteSupport.uuidBytes("00112233-4455-6677-8899-aabbccddeeff").getBytes();
    assertEquals(16, bytes.length);
    assertEquals(0x00, bytes[0]);
    assertEquals(0x11, bytes[1]);
    assertEquals((byte) 0xff, bytes[15]);
  }
}
