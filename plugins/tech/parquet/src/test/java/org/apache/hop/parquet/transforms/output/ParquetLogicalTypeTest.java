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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/** A Parquet type selected on a field is written as that logical type and read back. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ParquetLogicalTypeTest {

  @TempDir private Path tempDir;

  @Test
  void columnSchemas() throws Exception {
    ParquetField field = new ParquetField("amount", "amount");
    ValueMetaString text = new ValueMetaString("text");
    assertEquals(
        "optional binary text (STRING)",
        ParquetFieldType.Utf8.column("text", field, text).toString());
    assertEquals(
        "optional boolean flag", ParquetFieldType.Boolean.column("flag", field, text).toString());
    assertEquals(
        "optional int32 small", ParquetFieldType.Int32.column("small", field, text).toString());
    assertEquals(
        "optional int64 big", ParquetFieldType.Int64.column("big", field, text).toString());
    assertEquals(
        "optional float ratio", ParquetFieldType.Float.column("ratio", field, text).toString());
    assertEquals(
        "optional double measure",
        ParquetFieldType.Double.column("measure", field, text).toString());
    assertEquals(
        "optional binary raw", ParquetFieldType.Binary.column("raw", field, text).toString());
    assertEquals(
        "optional int32 birthday (DATE)",
        ParquetFieldType.Date.column("birthday", field, text).toString());
    assertEquals(
        "optional int32 clock (TIME(MILLIS,false))",
        ParquetFieldType.TimeMillis.column("clock", field, text).toString());
    assertEquals(
        "optional int64 clockus (TIME(MICROS,false))",
        ParquetFieldType.TimeMicros.column("clockus", field, text).toString());
    assertEquals(
        "optional int64 instant (TIMESTAMP(MILLIS,true))",
        ParquetFieldType.TimestampMillis.column("instant", field, text).toString());
    assertEquals(
        "optional int64 precise (TIMESTAMP(MICROS,true))",
        ParquetFieldType.TimestampMicros.column("precise", field, text).toString());
    assertEquals(
        "optional binary payload (JSON)",
        ParquetFieldType.Json.column("payload", field, text).toString());
    assertEquals(
        "optional fixed_len_byte_array(16) id (UUID)",
        ParquetFieldType.Uuid.column("id", field, text).toString());

    field.setPrecision("10");
    field.setScale("2");
    assertEquals(
        "optional binary amount (DECIMAL(10,2))",
        ParquetFieldType.Decimal.column("amount", field, new ValueMetaBigNumber("amount"))
            .toString());
  }

  @Test
  void decimalUsesExplicitPrecisionOverTheSourceField() throws Exception {
    ParquetField field = new ParquetField("amount", "amount");
    field.setPrecision("10");
    field.setScale("2");
    ValueMetaBigNumber valueMeta = new ValueMetaBigNumber("amount");
    valueMeta.setLength(20, 5);

    DecimalLogicalTypeAnnotation decimal = ParquetFieldType.Decimal.decimal(field, valueMeta);
    assertEquals(10, decimal.getPrecision());
    assertEquals(2, decimal.getScale());

    ParquetField fallback = new ParquetField("amount", "amount");
    DecimalLogicalTypeAnnotation fromField = ParquetFieldType.Decimal.decimal(fallback, valueMeta);
    assertEquals(20, fromField.getPrecision());
    assertEquals(5, fromField.getScale());
  }

  @Test
  void decimalWithoutPrecisionIsRejected() {
    ParquetField field = new ParquetField("amount", "amount");
    HopException e =
        assertThrows(
            HopException.class,
            () -> ParquetFieldType.Decimal.decimal(field, new ValueMetaBigNumber("amount")));
    assertTrue(e.getMessage().contains("'amount'"), e.getMessage());
    assertTrue(e.getMessage().contains("precision"), e.getMessage());
  }

  @Test
  void dateIsTheCalendarDateInTheJvmZone() throws Exception {
    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Brussels"));
    try {
      RecordConsumer consumer = mock(RecordConsumer.class);
      ValueMetaDate valueMeta = new ValueMetaDate("birthday");
      // 23:30 UTC is still 1 January in Brussels, and 31 December in UTC.
      Date date = Date.from(Instant.parse("2023-12-31T23:30:00Z"));
      ParquetFieldType.Date.write(
          consumer, new ParquetField("birthday", "birthday"), valueMeta, date);
      verify(consumer).addInteger(19723);

      ParquetFieldType.TimeMillis.write(
          consumer, new ParquetField("clock", "clock"), valueMeta, date);
      verify(consumer).addInteger(1_800_000);
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void timestampMillisIsTheUtcInstant() throws Exception {
    RecordConsumer consumer = mock(RecordConsumer.class);
    Date date = Date.from(Instant.parse("2024-01-01T00:00:00Z"));
    ParquetFieldType.TimestampMillis.write(
        consumer, new ParquetField("instant", "instant"), new ValueMetaDate("instant"), date);
    verify(consumer).addLong(1_704_067_200_000L);
  }

  @Test
  void int32RejectsAValueThatDoesNotFit() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                ParquetFieldType.Int32.write(
                    mock(RecordConsumer.class),
                    new ParquetField("code", "code"),
                    new ValueMetaInteger("code"),
                    5_000_000_000L));
    assertTrue(e.getMessage().contains("'code'"), e.getMessage());
    assertTrue(e.getMessage().contains("Int32"), e.getMessage());
  }

  @Test
  void decimalThatDoesNotFitIsRejected() {
    ParquetField field = new ParquetField("amount", "amount");
    field.setPrecision("4");
    field.setScale("2");
    assertThrows(
        HopRuntimeException.class,
        () ->
            ParquetFieldType.Decimal.write(
                mock(RecordConsumer.class),
                field,
                new ValueMetaBigNumber("amount"),
                new BigDecimal("123.45")));
  }

  @Test
  void getFieldsProposesDateForADateAndTheDefaultForTheOthers() {
    assertEquals(ParquetFieldType.Date, ParquetFieldType.forValueMeta(new ValueMetaDate("d")));
    assertEquals(
        ParquetFieldType.TimestampMicros,
        ParquetFieldType.forValueMeta(new ValueMetaTimestamp("ts")));
    assertEquals(ParquetFieldType.Utf8, ParquetFieldType.forValueMeta(new ValueMetaString("s")));
    assertEquals(ParquetFieldType.Int64, ParquetFieldType.forValueMeta(new ValueMetaInteger("i")));
    assertEquals(ParquetFieldType.Double, ParquetFieldType.forValueMeta(new ValueMetaNumber("n")));
    assertEquals(
        ParquetFieldType.Boolean, ParquetFieldType.forValueMeta(new ValueMetaBoolean("b")));
    assertEquals(ParquetFieldType.Binary, ParquetFieldType.forValueMeta(new ValueMetaBinary("b")));

    ValueMetaBigNumber wide = new ValueMetaBigNumber("wide");
    wide.setLength(50, 2);
    assertEquals(ParquetFieldType.Utf8, ParquetFieldType.forValueMeta(wide));

    ValueMetaBigNumber decimal = new ValueMetaBigNumber("amount");
    decimal.setLength(10, 2);
    assertEquals(ParquetFieldType.Decimal, ParquetFieldType.forValueMeta(decimal));
    assertNull(ParquetFieldType.forValueMeta(null));
  }

  @Test
  void selectedTypesAreWrittenAndReadBack() throws Exception {
    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Brussels"));
    try {
      Date birthday =
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2024-01-01 12:34:56.789");
      Timestamp precise = Timestamp.valueOf("2024-01-01 12:34:56.123456789");
      JsonNode payload = new ObjectMapper().readTree("{\"a\":1}");

      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaString("text"));
      rowMeta.addValueMeta(new ValueMetaBoolean("flag"));
      rowMeta.addValueMeta(new ValueMetaInteger("small"));
      rowMeta.addValueMeta(new ValueMetaInteger("big"));
      rowMeta.addValueMeta(new ValueMetaNumber("ratio"));
      rowMeta.addValueMeta(new ValueMetaNumber("measure"));
      rowMeta.addValueMeta(new ValueMetaBinary("raw"));
      rowMeta.addValueMeta(new ValueMetaDate("birthday"));
      rowMeta.addValueMeta(new ValueMetaDate("clock"));
      rowMeta.addValueMeta(new ValueMetaTimestamp("clockus"));
      rowMeta.addValueMeta(new ValueMetaDate("event"));
      rowMeta.addValueMeta(new ValueMetaDate("instant"));
      rowMeta.addValueMeta(new ValueMetaTimestamp("precise"));
      rowMeta.addValueMeta(new ValueMetaBigNumber("amount"));
      rowMeta.addValueMeta(new ValueMetaString("payload"));
      rowMeta.addValueMeta(new ValueMetaString("id"));

      ParquetOutputMeta meta = new ParquetOutputMeta();
      meta.getFields().add(typed("text", "UTF8"));
      meta.getFields().add(typed("flag", "Boolean"));
      meta.getFields().add(typed("small", "Int32"));
      meta.getFields().add(typed("big", "Int64"));
      meta.getFields().add(typed("ratio", "Float"));
      meta.getFields().add(typed("measure", "Double"));
      meta.getFields().add(typed("raw", "Binary"));
      meta.getFields().add(typed("birthday", "Date"));
      meta.getFields().add(typed("clock", "TimeMillis"));
      meta.getFields().add(typed("clockus", "TimeMicros"));
      // No type: a Date keeps the previous TIMESTAMP(MILLIS) mapping.
      meta.getFields().add(new ParquetField("event", "event"));
      meta.getFields().add(typed("instant", "TimestampMillis"));
      meta.getFields().add(typed("precise", "TimestampMicros"));
      ParquetField amount = typed("amount", "Decimal");
      amount.setPrecision("10");
      amount.setScale("2");
      meta.getFields().add(amount);
      meta.getFields().add(typed("payload", "JSON"));
      meta.getFields().add(typed("id", "UUID"));

      Object[] row =
          new Object[] {
            "h\u00e9llo",
            true,
            7L,
            42L,
            1.5D,
            2.5D,
            new byte[] {1, 2, 3},
            birthday,
            birthday,
            precise,
            birthday,
            birthday,
            precise,
            new BigDecimal("1234.565"),
            payload.toString(),
            "00112233-4455-6677-8899-aabbccddeeff"
          };
      Path file = write(meta, rowMeta, row, new Object[rowMeta.size()]);

      MessageType schema;
      try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
        schema = reader.getFooter().getFileMetaData().getSchema();
      }
      assertEquals("optional binary text (STRING)", schema.getType("text").toString());
      assertEquals("optional boolean flag", schema.getType("flag").toString());
      assertEquals("optional int32 small", schema.getType("small").toString());
      assertEquals("optional int64 big", schema.getType("big").toString());
      assertEquals("optional float ratio", schema.getType("ratio").toString());
      assertEquals("optional double measure", schema.getType("measure").toString());
      assertEquals("optional binary raw", schema.getType("raw").toString());
      assertEquals("optional int32 birthday (DATE)", schema.getType("birthday").toString());
      assertEquals("optional int32 clock (TIME(MILLIS,false))", schema.getType("clock").toString());
      assertEquals(
          "optional int64 clockus (TIME(MICROS,false))", schema.getType("clockus").toString());
      assertEquals(
          "optional int64 event (TIMESTAMP(MILLIS,true))", schema.getType("event").toString());
      assertEquals(
          "optional int64 instant (TIMESTAMP(MILLIS,true))", schema.getType("instant").toString());
      assertEquals(
          "optional int64 precise (TIMESTAMP(MICROS,true))", schema.getType("precise").toString());
      assertEquals("optional binary amount (DECIMAL(10,2))", schema.getType("amount").toString());
      assertEquals("optional binary payload (JSON)", schema.getType("payload").toString());
      assertEquals("optional fixed_len_byte_array(16) id (UUID)", schema.getType("id").toString());

      IRowMeta readAs = ParquetTestUtil.readSchema(file.toString());
      List<RowMetaAndData> rows =
          ParquetTestUtil.readAllRows(file.toString(), ParquetTestUtil.fieldsFromRowMeta(readAs));
      RowMetaAndData read = rows.get(0);
      assertEquals("String h\u00e9llo", render(value(read, "text")));
      assertEquals("Boolean true", render(value(read, "flag")));
      assertEquals("Long 7", render(value(read, "small")));
      assertEquals("Long 42", render(value(read, "big")));
      assertEquals("Double 1.5", render(value(read, "ratio")));
      assertEquals("Double 2.5", render(value(read, "measure")));
      assertEquals("bytes 010203", render(value(read, "raw")));
      assertEquals("Date 2024-01-01 00:00:00.000", render(value(read, "birthday")));
      assertEquals("Timestamp 1970-01-01 12:34:56.789", render(value(read, "clock")));
      assertEquals("Timestamp 1970-01-01 12:34:56.123456", render(value(read, "clockus")));
      assertEquals(birthday.getTime(), ((Date) value(read, "event")).getTime());
      assertEquals(birthday.getTime(), ((Date) value(read, "instant")).getTime());
      assertEquals("Timestamp 2024-01-01 12:34:56.123456", render(value(read, "precise")));
      assertEquals("BigDecimal 1234.56", render(value(read, "amount")));
      assertEquals("JSON {\"a\":1}", render(value(read, "payload")));
      assertEquals("String 00112233-4455-6677-8899-aabbccddeeff", render(value(read, "id")));

      RowMetaAndData nulls = rows.get(1);
      for (int i = 0; i < nulls.getRowMeta().size(); i++) {
        assertNull(nulls.getData()[i], nulls.getValueMeta(i).getName());
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void unknownTypeAndMissingDecimalPrecisionFailBeforeAFileIsWritten() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("birthday"));
    ParquetOutputMeta meta = new ParquetOutputMeta();
    meta.getFields().add(typed("birthday", "Geography"));

    HopException unknown =
        assertThrows(HopException.class, () -> write(meta, rowMeta, new Object[] {new Date()}));
    assertTrue(causes(unknown).contains("Geography"), causes(unknown));

    RowMeta amountRow = new RowMeta();
    amountRow.addValueMeta(new ValueMetaBigNumber("amount"));
    ParquetOutputMeta amountMeta = new ParquetOutputMeta();
    amountMeta.getFields().add(typed("amount", "Decimal"));
    HopException precision =
        assertThrows(
            HopException.class, () -> write(amountMeta, amountRow, new Object[] {BigDecimal.ONE}));
    assertTrue(causes(precision).contains("'amount'"), causes(precision));
    assertTrue(causes(precision).contains("precision"), causes(precision));
  }

  private static ParquetField typed(String name, String parquetType) {
    ParquetField field = new ParquetField(name, name);
    field.setParquetType(parquetType);
    return field;
  }

  private Path write(ParquetOutputMeta meta, IRowMeta rowMeta, Object[]... rowsToWrite)
      throws Exception {
    TransformMockHelper<ParquetOutputMeta, ParquetOutputData> mockHelper =
        new TransformMockHelper<>(
            "Parquet Output", ParquetOutputMeta.class, ParquetOutputData.class);
    try {
      when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
          .thenReturn(mockHelper.iLogChannel);
      when(mockHelper.pipeline.isRunning()).thenReturn(true);

      meta.setCompressionCodec(CompressionCodecName.UNCOMPRESSED);
      meta.setFilenameIncludingCopyNr(false);
      meta.setFilenameIncludingSplitNr(false);
      meta.setFilenameBase(tempDir.resolve("logical").toString());

      PipelineMeta pipelineMeta = new PipelineMeta();
      TransformMeta transformMeta = new TransformMeta("Parquet Output", meta);
      pipelineMeta.addTransform(transformMeta);
      Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
      ParquetOutput output =
          spy(
              new ParquetOutput(
                  transformMeta, meta, new ParquetOutputData(), 0, pipelineMeta, pipeline));
      output.setInputRowMeta(rowMeta);
      assertTrue(output.init());

      List<Object[]> remaining = new ArrayList<>(List.of(rowsToWrite));
      doNothing().when(output).putRow(any(), any());
      doAnswer(invocation -> remaining.isEmpty() ? null : remaining.remove(0))
          .when(output)
          .getRow();
      while (output.processRow()) {
        // keep going until the null row closes the file
      }
    } finally {
      mockHelper.cleanUp();
    }

    try (Stream<Path> files = Files.list(tempDir)) {
      return files
          .filter(path -> path.getFileName().toString().endsWith(".parquet"))
          .findFirst()
          .orElseThrow();
    }
  }

  private static Object value(RowMetaAndData row, String name) {
    return row.getData()[row.getRowMeta().indexOfValue(name)];
  }

  private static String render(Object value) {
    if (value instanceof Timestamp timestamp) {
      return "Timestamp " + timestamp;
    }
    if (value instanceof Date date) {
      return "Date " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
    }
    if (value instanceof byte[] bytes) {
      return "bytes " + java.util.HexFormat.of().formatHex(bytes);
    }
    if (value instanceof JsonNode json) {
      return "JSON " + json;
    }
    if (value instanceof BigDecimal decimal) {
      return "BigDecimal " + decimal.toPlainString();
    }
    return value.getClass().getSimpleName() + " " + value;
  }

  private static String causes(Throwable throwable) {
    StringBuilder message = new StringBuilder();
    while (throwable != null) {
      message.append(throwable.getMessage()).append('\n');
      throwable = throwable.getCause();
    }
    return message.toString();
  }
}
