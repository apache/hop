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

package org.apache.hop.parquet.transforms.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reads a file holding annotated Parquet columns back through the real read path, to guard the
 * logical type handling in {@link ParquetValueConverter}.
 */
class ParquetInputLogicalTypeTest {

  /** 2024-01-31, the day the schema mismatch behind issue #3598 was reported. */
  private static final LocalDate DATE = LocalDate.of(2024, 1, 31);

  private static final long EPOCH_MILLIS = 1706716800123L;

  private static final MessageType SCHEMA =
      Types.buildMessage()
          .optional(PrimitiveTypeName.INT32)
          .as(LogicalTypeAnnotation.dateType())
          .named("date_field")
          .optional(PrimitiveTypeName.INT64)
          .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
          .named("timestamp_millis_field")
          .optional(PrimitiveTypeName.INT64)
          .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
          .named("timestamp_micros_field")
          .optional(PrimitiveTypeName.INT64)
          .as(LogicalTypeAnnotation.decimalType(2, 18))
          .named("decimal_field")
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.jsonType())
          .named("json_field")
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named("string_field")
          .optional(PrimitiveTypeName.INT64)
          .named("long_field")
          .optional(PrimitiveTypeName.DOUBLE)
          .named("double_field")
          .named("LogicalTypes");

  @BeforeAll
  static void setUpBeforeAll() throws Exception {
    HopClientEnvironment.init();
  }

  private static ParquetField field(String name, String type) {
    return new ParquetField(name, name, type, null, "-1", "-1");
  }

  private static Path writeFile(Path folder) throws Exception {
    Path file = folder.resolve("logical-types.parquet");
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);

    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(SCHEMA).build()) {
      writer.write(
          groupFactory
              .newGroup()
              .append("date_field", (int) DATE.toEpochDay())
              .append("timestamp_millis_field", EPOCH_MILLIS)
              .append("timestamp_micros_field", EPOCH_MILLIS * 1000L)
              .append("decimal_field", 123456L)
              .append("json_field", Binary.fromString("{\"a\":1}"))
              .append("string_field", Binary.fromString("hop"))
              .append("long_field", 9081496L)
              .append("double_field", 9081496.6d));
    }
    return file;
  }

  private static RowMetaAndData read(Path file, List<ParquetField> fields) throws Exception {
    FileObject fileObject = HopVfs.getFileObject(file.toString());
    try (ParquetStream stream = new ParquetStream(fileObject, file.toString());
        ParquetReader<RowMetaAndData> reader =
            new ParquetReaderBuilder<>(new ParquetReadSupport(fields), stream).build()) {
      return reader.read();
    }
  }

  /** Every annotated column still has to be converted using its logical type. */
  @Test
  void testLogicalTypesAreHonoured(@TempDir Path folder) throws Exception {
    Path file = writeFile(folder);

    List<ParquetField> fields = new ArrayList<>();
    fields.add(field("date_field", "Date"));
    fields.add(field("timestamp_millis_field", "Timestamp"));
    fields.add(field("timestamp_micros_field", "Timestamp"));
    fields.add(field("decimal_field", "BigNumber"));
    fields.add(field("json_field", "JSON"));
    fields.add(field("string_field", "String"));

    RowMetaAndData row = read(file, fields);
    assertNotNull(row);

    // The DATE annotation makes this an epoch day rather than a number of millis.
    Date expectedDate = Date.from(DATE.atStartOfDay(ZoneId.systemDefault()).toInstant());
    assertEquals(expectedDate, row.getData()[0]);

    // Both timestamps describe the same instant, in a different unit.
    assertEquals(new Timestamp(EPOCH_MILLIS), row.getData()[1]);
    assertEquals(new Timestamp(EPOCH_MILLIS), row.getData()[2]);

    // The DECIMAL annotation carries the scale: 123456 with scale 2 is 1234.56
    assertEquals(0, new BigDecimal("1234.56").compareTo((BigDecimal) row.getData()[3]));

    assertEquals("{\"a\":1}", row.getData()[4].toString());
    assertEquals("hop", row.getData()[5]);
  }

  /**
   * A DATE column read without its logical type would come out as an instant near the epoch. This
   * pins the annotation actually reaching the converter.
   */
  @Test
  void testDateColumnIsNotReadAsMillis(@TempDir Path folder) throws Exception {
    Path file = writeFile(folder);

    RowMetaAndData row = read(file, List.of(field("date_field", "Date")));

    assertEquals(
        DATE, ((Date) row.getData()[0]).toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
  }

  /** The widening conversions added for issue #3598, through the real read path. */
  @Test
  void testNumericTypeMismatchIsWidened(@TempDir Path folder) throws Exception {
    Path file = writeFile(folder);

    // An int64 column read into a Number field and a double column read into an Integer field.
    RowMetaAndData row =
        read(file, List.of(field("long_field", "Number"), field("double_field", "Integer")));

    assertEquals(9081496.0, (Double) row.getData()[0], 0.0);
    assertEquals(9081497L, row.getData()[1]);
  }

  /** Columns which aren't requested stay out of the row. */
  @Test
  void testUnrequestedColumnsAreNotRead(@TempDir Path folder) throws Exception {
    Path file = writeFile(folder);

    RowMetaAndData row = read(file, List.of(field("string_field", "String")));

    assertEquals(1, row.getRowMeta().size());
    assertEquals("hop", row.getData()[0]);
  }

  /** Nulls stay null rather than being converted. */
  @Test
  void testNullValues(@TempDir Path folder) throws Exception {
    Path file = folder.resolve("nulls.parquet");
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(SCHEMA).build()) {
      writer.write(groupFactory.newGroup().append("string_field", Binary.fromString("hop")));
    }

    RowMetaAndData row =
        read(file, List.of(field("date_field", "Date"), field("string_field", "String")));

    assertNull(row.getData()[0]);
    assertEquals("hop", row.getData()[1]);
  }
}
