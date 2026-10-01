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

package org.apache.hop.parquet.explorer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.parquet.transforms.input.ParquetStream;
import org.apache.hop.parquet.transforms.output.ParquetOutputFile;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ParquetFileInspectorTest {

  private static final MessageType SCHEMA =
      Types.buildMessage()
          .required(PrimitiveTypeName.INT64)
          .named("id")
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named("name")
          .optional(PrimitiveTypeName.INT64)
          .as(LogicalTypeAnnotation.decimalType(2, 10))
          .named("price")
          .named("row");

  @TempDir private Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void readsFileDetailsSchemaAndRows() throws Exception {
    Path path = tempDir.resolve("sample.parquet");
    write(
        path,
        SCHEMA,
        CompressionCodecName.UNCOMPRESSED,
        WriterVersion.PARQUET_1_0,
        (factory, writer) -> {
          writer.write(row(factory, 1L, "alpha"));
          writer.write(row(factory, 2L, "beta"));
        });

    ParquetFileInspection inspection =
        ParquetFileInspector.inspect(path.toString(), new Variables());

    assertEquals("sample.parquet", inspection.getFileName());
    assertTrue(
        inspection.getFolder().contains(tempDir.getFileName().toString()), inspection.getFolder());
    assertEquals(Files.size(path), inspection.getSizeBytes());
    assertEquals("UNCOMPRESSED", inspection.getCompression());
    assertEquals("Parquet 1.0", inspection.getVersion());
    assertEquals(2L, inspection.getRowCount());
    assertEquals(1, inspection.getRowGroupCount());
    assertFalse(inspection.getCreatedBy().isBlank());
    assertNotNull(inspection.getDataPageSize());
    assertTrue(inspection.getDataPageSize() > 0);
    if (inspection.getDictionaryPageSize() != null) {
      assertTrue(inspection.getDictionaryPageSize() > 0);
    }
    assertNull(inspection.getPreviewError());

    try (FileObject fileObject = HopVfs.getFileObject(path.toString());
        ParquetStream stream = new ParquetStream(fileObject, path.toString());
        ParquetFileReader reader = ParquetFileReader.open(stream)) {
      assertEquals(
          reader.getFooter().getBlocks().get(0).getTotalByteSize(), inspection.getRowGroupSize());
    }

    JsonNode schema = new ObjectMapper().readTree(inspection.getSchemaJson());
    assertEquals("row", schema.get("name").asText());
    JsonNode price = field(schema, "price");
    assertEquals("INT64", price.get("type").asText());
    assertEquals("OPTIONAL", price.get("repetition").asText());
    assertEquals("DECIMAL(10,2)", price.get("logicalType").asText());

    ParquetColumnView name = column(inspection, "name");
    assertEquals("String", name.getHopType());
    assertEquals(-1, name.getLength());
    assertEquals(-1, name.getPrecision());
    assertEquals("BINARY (STRING)", name.getParquetType());

    ParquetColumnView priceColumn = column(inspection, "price");
    assertEquals("BigNumber", priceColumn.getHopType());
    assertEquals(10, priceColumn.getLength());
    assertEquals(2, priceColumn.getPrecision());
    assertEquals("INT64 (DECIMAL(10,2))", priceColumn.getParquetType());

    assertEquals(2, inspection.getPreviewRows().size());
    assertEquals(1L, inspection.getPreviewRows().get(0)[0]);
    assertEquals("alpha", inspection.getPreviewRows().get(0)[1]);
    assertEquals("beta", inspection.getPreviewRows().get(1)[1]);
    assertNull(inspection.getPreviewRows().get(0)[2]);
    assertEquals(List.of("id", "name", "price"), List.of(inspection.getRowMeta().getFieldNames()));
  }

  @Test
  void reportsParquet2WhenTheDataPageIsV2() throws Exception {
    Path path = tempDir.resolve("v2.parquet");
    MessageType schema =
        Types.buildMessage().required(PrimitiveTypeName.INT64).named("id").named("row");
    write(
        path,
        schema,
        CompressionCodecName.UNCOMPRESSED,
        WriterVersion.PARQUET_2_0,
        (factory, writer) -> writer.write(factory.newGroup().append("id", 1L)));

    ParquetFileInspection inspection =
        ParquetFileInspector.inspect(path.toString(), new Variables());

    assertEquals("Parquet 2.0", inspection.getVersion());
  }

  @Test
  void reportsTheCompressionCodec() throws Exception {
    Path path = tempDir.resolve("snappy.parquet");
    MessageType schema =
        Types.buildMessage().required(PrimitiveTypeName.INT64).named("id").named("row");
    write(
        path,
        schema,
        CompressionCodecName.SNAPPY,
        WriterVersion.PARQUET_1_0,
        (factory, writer) -> writer.write(factory.newGroup().append("id", 5L)));

    ParquetFileInspection inspection =
        ParquetFileInspector.inspect(path.toString(), new Variables());

    assertEquals("SNAPPY", inspection.getCompression());
  }

  @Test
  void previewStopsAtOneThousandRows() throws Exception {
    Path path = tempDir.resolve("many.parquet");
    MessageType schema =
        Types.buildMessage().required(PrimitiveTypeName.INT64).named("id").named("row");
    write(
        path,
        schema,
        CompressionCodecName.UNCOMPRESSED,
        WriterVersion.PARQUET_1_0,
        (factory, writer) -> {
          for (int i = 0; i < ParquetFileInspector.PREVIEW_ROW_LIMIT + 5; i++) {
            writer.write(factory.newGroup().append("id", (long) i));
          }
        });

    ParquetFileInspection inspection =
        ParquetFileInspector.inspect(path.toString(), new Variables());

    assertEquals(ParquetFileInspector.PREVIEW_ROW_LIMIT, inspection.getPreviewRows().size());
    assertEquals(0L, inspection.getPreviewRows().get(0)[0]);
    assertEquals(
        (long) ParquetFileInspector.PREVIEW_ROW_LIMIT - 1,
        inspection.getPreviewRows().get(ParquetFileInspector.PREVIEW_ROW_LIMIT - 1)[0]);
  }

  @Test
  void rejectsAFileThatIsNotParquet() throws Exception {
    Path path = tempDir.resolve("not.parquet");
    try (OutputStream out = HopVfs.getOutputStream(path.toString(), false)) {
      out.write("not parquet".getBytes(StandardCharsets.UTF_8));
    }

    HopException exception =
        assertThrows(
            HopException.class,
            () -> ParquetFileInspector.inspect(path.toString(), new Variables()));

    assertTrue(exception.getMessage().contains("not.parquet"), exception.getMessage());
  }

  @Test
  void schemaJsonKeepsGroupsAndTheTableListsLeaves() throws Exception {
    MessageType schema =
        Types.buildMessage()
            .required(PrimitiveTypeName.INT64)
            .named("id")
            .optionalGroup()
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("city")
            .named("address")
            .named("row");

    JsonNode root = new ObjectMapper().readTree(ParquetFileInspector.schemaJson(schema));
    assertEquals("group", field(root, "address").get("type").asText());
    assertEquals("city", field(root, "address").get("fields").get(0).get("name").asText());

    List<ParquetColumnView> leaves = ParquetFileInspector.leafColumns(schema);
    assertEquals(
        List.of("id", "address.city"), leaves.stream().map(ParquetColumnView::getName).toList());
    assertEquals("String", leaves.get(1).getHopType());
    assertEquals("Integer", leaves.get(0).getHopType());
  }

  private static JsonNode field(JsonNode schema, String name) {
    for (JsonNode field : schema.get("fields")) {
      if (name.equals(field.get("name").asText())) {
        return field;
      }
    }
    throw new AssertionError("missing field " + name);
  }

  private static ParquetColumnView column(ParquetFileInspection inspection, String name) {
    return inspection.getColumns().stream()
        .filter(column -> name.equals(column.getName()))
        .findFirst()
        .orElseThrow();
  }

  private static Group row(SimpleGroupFactory factory, long id, String name) {
    return factory.newGroup().append("id", id).append("name", Binary.fromString(name));
  }

  private static void write(
      Path path,
      MessageType schema,
      CompressionCodecName codec,
      WriterVersion version,
      WriterBody body)
      throws Exception {
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    try (OutputStream outputStream = HopVfs.getOutputStream(path.toString(), false);
        ParquetWriter<Group> writer =
            ExampleParquetWriter.builder(new ParquetOutputFile(outputStream))
                .withType(schema)
                .withCompressionCodec(codec)
                .withWriterVersion(version)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
      body.write(factory, writer);
    }
  }

  @FunctionalInterface
  private interface WriterBody {
    void write(SimpleGroupFactory factory, ParquetWriter<Group> writer) throws Exception;
  }
}
