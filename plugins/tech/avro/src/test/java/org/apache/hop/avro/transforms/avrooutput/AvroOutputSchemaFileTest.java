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

package org.apache.hop.avro.transforms.avrooutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Issue #3901: a schema filename containing variables (e.g. {@code
 * ${PROJECT_HOME}/schemas/customer.avsc}) was handed to {@code java.io.File} unresolved, so only a
 * bare filename relative to the Hop installation folder could be used.
 */
class AvroOutputSchemaFileTest {

  private static final String SCHEMA =
      "{\"type\":\"record\",\"name\":\"customer\",\"namespace\":\"org.apache.hop.test\","
          + "\"fields\":["
          + "{\"name\":\"id\",\"type\":[\"null\",\"long\"]},"
          + "{\"name\":\"name\",\"type\":[\"null\",\"string\"]}]}";

  @TempDir Path tempDir;

  private TransformMockHelper<AvroOutputMeta, AvroOutputData> mockHelper;
  private AvroOutputData data;

  @BeforeAll
  static void initEnv() throws Exception {
    HopEnvironment.init();
  }

  @AfterAll
  static void resetEnv() {
    HopEnvironment.reset();
  }

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("AvroOutput", AvroOutputMeta.class, AvroOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    data = new AvroOutputData();
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void schemaFilenameWithVariablesIsResolved() throws Exception {
    Files.writeString(tempDir.resolve("customer.avsc"), SCHEMA);
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setSchemaFileName("${SCHEMA_FOLDER}/customer.avsc");
    AvroOutput transform = createTransform(meta);
    transform.setVariable("SCHEMA_FOLDER", tempDir.toString());

    Schema schema = transform.readSchemaFile();

    assertEquals("org.apache.hop.test.customer", schema.getFullName());
  }

  @Test
  void schemaFileIsReadThroughVfs() throws Exception {
    String schemaFileName = "ram:///avro-3901/customer.avsc";
    HopVfs.getFileObject("ram:///avro-3901").createFolder();
    try (OutputStream outputStream = HopVfs.getOutputStream(schemaFileName, false)) {
      outputStream.write(SCHEMA.getBytes(StandardCharsets.UTF_8));
    }
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setSchemaFileName(schemaFileName);

    Schema schema = createTransform(meta).readSchemaFile();

    assertEquals(2, schema.getFields().size());
  }

  @Test
  void missingSchemaFileFailsWithTheResolvedFilename() {
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setSchemaFileName("${SCHEMA_FOLDER}/does-not-exist.avsc");
    AvroOutput transform = createTransform(meta);
    transform.setVariable("SCHEMA_FOLDER", tempDir.toString());

    HopException e = assertThrows(HopException.class, transform::readSchemaFile);

    assertTrue(
        e.getMessage().contains(tempDir.resolve("does-not-exist.avsc").toString()), e.getMessage());
  }

  @Test
  void emptySchemaFilenameFails() {
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setSchemaFileName("");

    assertThrows(HopException.class, () -> createTransform(meta).readSchemaFile());
  }

  @Test
  void invalidSchemaFileFails() throws Exception {
    Files.writeString(tempDir.resolve("broken.avsc"), "{ this is not a schema");
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setSchemaFileName(tempDir.resolve("broken.avsc").toString());

    assertThrows(HopException.class, () -> createTransform(meta).readSchemaFile());
  }

  /** The whole transform: rows written to a binary file with the schema from a variable path. */
  @Test
  void writesBinaryFileUsingSchemaFromVariablePath() throws Exception {
    Files.writeString(tempDir.resolve("customer.avsc"), SCHEMA);
    AvroOutputMeta meta = binaryFileMeta("${OUT}/customer.avsc", "${OUT}/customers.avro");
    AvroOutput transform =
        spyWithRows(meta, new Object[] {1L, "Alice"}, new Object[] {2L, "Bob"}, null);
    transform.setVariable("OUT", tempDir.toString());

    assertTrue(transform.processRow());
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    List<GenericRecord> records = readAvroFile(tempDir.resolve("customers.avro"));
    assertEquals(2, records.size());
    assertEquals(1L, records.get(0).get("id"));
    assertEquals("Bob", records.get(1).get("name").toString());
  }

  /** Before #3901 an unreadable schema was only logged and the transform carried on. */
  @Test
  void unreadableSchemaStopsTheTransform() throws Exception {
    AvroOutputMeta meta = binaryFileMeta("${OUT}/does-not-exist.avsc", "${OUT}/customers.avro");
    AvroOutput transform = spyWithRows(meta, new Object[] {1L, "Alice"});
    transform.setVariable("OUT", tempDir.toString());

    assertThrows(HopException.class, transform::processRow);
    assertFalse(Files.exists(tempDir.resolve("customers.avro")));
  }

  /** Beam calls startBundle() for every bundle; the schema file is only parsed once. */
  @Test
  void schemaIsReadOnlyOnceAcrossBundles() throws Exception {
    Path schemaFile = tempDir.resolve("customer.avsc");
    Files.writeString(schemaFile, SCHEMA);
    AvroOutputMeta meta = binaryFileMeta(schemaFile.toString(), "${OUT}/customers.avro");
    meta.setOutputType(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_FIELD]);
    meta.setOutputFieldName("avro");
    AvroOutput transform = spyWithRows(meta, new Object[] {1L, "Alice"});
    transform.setVariable("OUT", tempDir.toString());

    assertTrue(transform.processRow());
    Schema first = data.avroSchema;
    Files.delete(schemaFile);
    transform.startBundle();

    assertEquals(first, data.avroSchema);
  }

  private AvroOutputMeta binaryFileMeta(String schemaFileName, String fileName) {
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setDefault();
    meta.setOutputType(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE]);
    meta.setCreateSchemaFile(false);
    meta.setWriteSchemaFile(false);
    meta.setSchemaFileName(schemaFileName);
    meta.setFileName(fileName);
    List<AvroOutputField> fields = new ArrayList<>();
    fields.add(new AvroOutputField("id", "id", AvroOutputField.AVRO_TYPE_LONG, true));
    fields.add(new AvroOutputField("name", "name", AvroOutputField.AVRO_TYPE_STRING, true));
    meta.setOutputFields(fields);
    return meta;
  }

  private AvroOutput spyWithRows(AvroOutputMeta meta, Object[] row, Object[]... moreRows)
      throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));

    AvroOutput transform = spy(createTransform(meta));
    doReturn(rowMeta).when(transform).getInputRowMeta();
    doReturn(row, (Object[]) moreRows).when(transform).getRow();
    doAnswer(inv -> null).when(transform).putRow(any(), any());
    return transform;
  }

  private AvroOutput createTransform(AvroOutputMeta meta) {
    return new AvroOutput(
        mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  private static List<GenericRecord> readAvroFile(Path file) throws Exception {
    List<GenericRecord> records = new ArrayList<>();
    try (InputStream inputStream = Files.newInputStream(file);
        DataFileStream<GenericRecord> reader =
            new DataFileStream<>(inputStream, new GenericDatumReader<>())) {
      reader.forEach(records::add);
    }
    return records;
  }
}
