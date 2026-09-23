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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HexFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaJson;
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
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * What every Hop type is written as, and what it reads back as: the matrix of the Hop to Parquet
 * conversion.
 *
 * <p>One row holding a value of every Hop type goes through Parquet Output. For every field the
 * matrix shows the Parquet column it became, the Hop type Get Fields proposes for that column and
 * the value read back into that type. A second row of nulls must read back as nulls. The JVM time
 * zone is Europe/Brussels (UTC+1 in January) so that UTC and local time can't be confused.
 *
 * @see <a href="https://parquet.apache.org/docs/file-format/types/logicaltypes/">Parquet logical
 *     types</a>
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class HopToParquetTypeMatrixTest {

  private static TimeZone defaultTimeZone;

  @TempDir private static Path tempDir;

  private static MessageType schema;
  private static IRowMeta getFields;
  private static List<RowMetaAndData> rows;

  private record Field(
      IValueMeta valueMeta,
      Object value,
      String parquetColumn,
      String getFieldsType,
      String readsBack) {}

  private static List<Field> matrix() throws Exception {
    return List.of(
        new Field(
            new ValueMetaInteger("integer"), 42L, "optional int64 integer", "Integer", "Long 42"),
        new Field(
            new ValueMetaNumber("number"), 2.5D, "optional double number", "Number", "Double 2.5"),
        // Without a length there is no precision to declare: written as a string.
        new Field(
            bigNumber("bignumber", -1, -1),
            new BigDecimal("12345.6789"),
            "optional binary bignumber (STRING)",
            "String",
            "String 12345.6789"),
        // With a length: a DECIMAL, rounded half even to the precision (scale) of the field.
        new Field(
            bigNumber("decimal", 10, 2),
            new BigDecimal("1234.565"),
            "optional binary decimal (DECIMAL(10,2))",
            "BigNumber",
            "BigDecimal 1234.56"),
        // Longer than the 38 digits Spark, Hive and Trino accept: written as a string.
        new Field(
            bigNumber("widedecimal", 50, 2),
            new BigDecimal("1234.56"),
            "optional binary widedecimal (STRING)",
            "String",
            "String 1234.56"),
        new Field(
            new ValueMetaString("string"),
            "h\u00e9llo",
            "optional binary string (STRING)",
            "String",
            "String h\u00e9llo"),
        new Field(
            new ValueMetaBoolean("boolean"),
            true,
            "optional boolean boolean",
            "Boolean",
            "Boolean true"),
        // A Date is an instant with millisecond precision.
        new Field(
            new ValueMetaDate("date"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2024-01-01 12:34:56.789"),
            "optional int64 date (TIMESTAMP(MILLIS,true))",
            "Timestamp",
            "Timestamp 2024-01-01 12:34:56.789"),
        // A Timestamp keeps microseconds: the nanoseconds beyond are dropped.
        new Field(
            new ValueMetaTimestamp("timestamp"),
            Timestamp.valueOf("2024-01-01 12:34:56.123456789"),
            "optional int64 timestamp (TIMESTAMP(MICROS,true))",
            "Timestamp",
            "Timestamp 2024-01-01 12:34:56.123456"),
        new Field(
            new ValueMetaBinary("binary"),
            new byte[] {1, 2, 3},
            "optional binary binary",
            "Binary",
            "bytes 010203"),
        new Field(
            new ValueMetaJson("json"),
            new ObjectMapper().readTree("{\"a\":1}"),
            "optional binary json (JSON)",
            "JSON",
            "JSON {\"a\":1}"),
        // Get Fields proposes a String here because the UUID value type plugin isn't on the class
        // path of this test. With the plugin installed it proposes a UUID.
        new Field(
            new UuidValueMeta("uuid"),
            UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"),
            "optional fixed_len_byte_array(16) uuid (UUID)",
            "String",
            "String 00112233-4455-6677-8899-aabbccddeeff"));
  }

  @BeforeAll
  static void writeAndReadBack() throws Exception {
    defaultTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Brussels"));

    List<Field> fields = matrix();
    RowMeta rowMeta = new RowMeta();
    Object[] values = new Object[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      rowMeta.addValueMeta(fields.get(i).valueMeta);
      values[i] = fields.get(i).value;
    }
    Path file = write(rowMeta, values, new Object[fields.size()]);

    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
      schema = reader.getFooter().getFileMetaData().getSchema();
    }
    getFields = ParquetTestUtil.readSchema(file.toString());
    rows =
        ParquetTestUtil.readAllRows(file.toString(), ParquetTestUtil.fieldsFromRowMeta(getFields));
  }

  @AfterAll
  static void tearDownAfterAll() {
    TimeZone.setDefault(defaultTimeZone);
  }

  @TestFactory
  Stream<DynamicNode> hopToParquet() throws Exception {
    return matrix().stream()
        .map(
            field -> {
              String name = field.valueMeta.getName();
              return dynamicContainer(
                  field.valueMeta.getTypeDesc() + " " + name,
                  List.of(
                      dynamicTest(
                          "written as: " + field.parquetColumn,
                          () -> assertEquals(field.parquetColumn, schema.getType(name).toString())),
                      dynamicTest(
                          "Get Fields -> " + field.getFieldsType,
                          () ->
                              assertEquals(
                                  field.getFieldsType,
                                  getFields.searchValueMeta(name).getTypeDesc())),
                      dynamicTest(
                          "reads back: " + field.readsBack,
                          () -> {
                            RowMetaAndData row = rows.get(0);
                            assertEquals(
                                field.readsBack,
                                render(row.getData()[row.getRowMeta().indexOfValue(name)]));
                          }),
                      dynamicTest(
                          "null reads back as null",
                          () -> {
                            RowMetaAndData row = rows.get(1);
                            assertNull(row.getData()[row.getRowMeta().indexOfValue(name)]);
                          })));
            });
  }

  private static Path write(IRowMeta rowMeta, Object[]... rowsToWrite) throws Exception {
    TransformMockHelper<ParquetOutputMeta, ParquetOutputData> mockHelper =
        new TransformMockHelper<>(
            "Parquet Output", ParquetOutputMeta.class, ParquetOutputData.class);
    try {
      when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
          .thenReturn(mockHelper.iLogChannel);
      when(mockHelper.pipeline.isRunning()).thenReturn(true);

      ParquetOutputMeta meta = new ParquetOutputMeta();
      meta.setCompressionCodec(CompressionCodecName.UNCOMPRESSED);
      meta.setFilenameIncludingSplitNr(false);
      meta.setFilenameBase(tempDir.resolve("matrix").toString());

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
      return files.filter(Files::isRegularFile).findFirst().orElseThrow();
    }
  }

  private static ValueMetaBigNumber bigNumber(String name, int length, int precision) {
    ValueMetaBigNumber valueMeta = new ValueMetaBigNumber(name);
    valueMeta.setLength(length, precision);
    return valueMeta;
  }

  /** A value as its Java class and value, so the matrix shows the type as well. */
  private static String render(Object value) {
    if (value instanceof Timestamp timestamp) {
      return "Timestamp " + timestamp;
    }
    if (value instanceof Date date) {
      return "Date " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
    }
    if (value instanceof byte[] bytes) {
      return "bytes " + HexFormat.of().formatHex(bytes);
    }
    if (value instanceof JsonNode json) {
      return "JSON " + json;
    }
    if (value instanceof BigDecimal decimal) {
      return "BigDecimal " + decimal.toPlainString();
    }
    return value.getClass().getSimpleName() + " " + value;
  }

  /** Stands in for the UUID value type plugin, which holds java.util.UUID values. */
  private static final class UuidValueMeta extends ValueMetaBase {
    UuidValueMeta(String name) {
      super(name, IValueMeta.TYPE_UUID);
    }

    @Override
    public String getString(Object object) {
      return object == null ? null : object.toString();
    }
  }
}
