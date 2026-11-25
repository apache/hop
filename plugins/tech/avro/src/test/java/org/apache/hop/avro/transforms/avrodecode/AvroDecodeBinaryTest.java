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

package org.apache.hop.avro.transforms.avrodecode;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.hop.avro.transforms.avrodecode.AvroDecodeFieldFinder.FieldRow;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Bare Avro binary decoded with a schema file. Issue #9695. */
class AvroDecodeBinaryTest {

  private static final Schema EVENT =
      SchemaBuilder.record("event")
          .namespace("org.apache.hop.avro.test")
          .fields()
          .requiredLong("id")
          .name("name")
          .type()
          .nullable()
          .stringType()
          .noDefault()
          .requiredBoolean("active")
          .requiredBytes("payload")
          .requiredInt("count")
          .endRecord();

  @TempDir Path tempDir;

  private TransformMockHelper<AvroDecodeMeta, AvroDecodeData> tmh;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    tmh = new TransformMockHelper<>("Avro decode", AvroDecodeMeta.class, AvroDecodeData.class);
    when(tmh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(tmh.iLogChannel);
    when(tmh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    tmh.cleanUp();
  }

  @Test
  void roundTripThroughGenericDatumWriterReusesTheDecoder() throws Exception {
    Path schemaFile = tempDir.resolve("event.avsc");
    Files.writeString(schemaFile, EVENT.toString());
    byte[] first = encode(event(1L, "alice", true, new byte[] {1, 2, 3}, 7));
    byte[] second = encode(event(99L, null, false, new byte[] {9}, 1_000_000));

    AvroDecodeData data = new AvroDecodeData();
    AvroDecode transform = transform(data, "${SCHEMA}/event.avsc", binaryTargets());
    transform.setVariable("SCHEMA", tempDir.toString());
    List<Object[]> output = new ArrayList<>();
    feed(transform, output, new Object[] {first}, new Object[] {second});

    assertTrue(transform.processRow());
    assertTrue(Files.deleteIfExists(schemaFile));
    Object decoder = data.decoder;
    Object reader = data.reader;
    Schema schema = data.schema;
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    assertSame(decoder, data.decoder);
    assertSame(reader, data.reader);
    assertSame(schema, data.schema);
    assertEquals("org.apache.hop.avro.test.event", schema.getFullName());
    assertEquals(2, output.size());
    assertRow(output.get(0), 1L, "alice", true, new byte[] {1, 2, 3}, 7L);
    assertRow(output.get(1), 99L, null, false, new byte[] {9}, 1_000_000L);
  }

  @Test
  void javaStringPropertyDecodesAsAString() throws Exception {
    String schemaJson =
        """
        {
          "type": "record",
          "name": "event",
          "namespace": "org.apache.hop.avro.test",
          "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": {"type": "string", "avro.java.string": "String"}},
            {"name": "active", "type": "boolean"},
            {"name": "payload", "type": "bytes"},
            {"name": "count", "type": "int"}
          ]
        }
        """;
    Path schemaFile = tempDir.resolve("java-string.avsc");
    Files.writeString(schemaFile, schemaJson);
    Schema schema = new Schema.Parser().parse(schemaJson);
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 1L);
    record.put("name", "alice");
    record.put("active", true);
    record.put("payload", ByteBuffer.wrap(new byte[] {1, 2}));
    record.put("count", 4);
    byte[] encoded = encode(schema, record);

    GenericRecord readBack =
        new GenericDatumReader<GenericRecord>(schema)
            .read(null, DecoderFactory.get().binaryDecoder(encoded, null));
    assertInstanceOf(String.class, readBack.get("name"));

    AvroDecode transform = transform(new AvroDecodeData(), schemaFile.toString(), binaryTargets());
    List<Object[]> output = new ArrayList<>();
    feed(transform, output, new Object[] {encoded});

    assertTrue(transform.processRow());
    assertFalse(transform.processRow());
    assertRow(output.get(0), 1L, "alice", true, new byte[] {1, 2}, 4L);
  }

  @Test
  void schemaFileOnAVfsLocationDecodesOneRow() throws Exception {
    String folder = "ram:///avro-decode-9695";
    String schemaFileName = folder + "/event.avsc";
    HopVfs.getFileObject(folder).createFolder();
    try (OutputStream outputStream = HopVfs.getOutputStream(schemaFileName, false)) {
      outputStream.write(EVENT.toString().getBytes(StandardCharsets.UTF_8));
    }

    AvroDecodeData data = new AvroDecodeData();
    AvroDecode transform = transform(data, schemaFileName, binaryTargets());
    List<Object[]> output = new ArrayList<>();
    feed(transform, output, new Object[] {encode(event(5L, "bob", true, new byte[] {4}, 3))});

    assertTrue(transform.processRow());
    assertFalse(transform.processRow());
    assertRow(output.get(0), 5L, "bob", true, new byte[] {4}, 3L);
  }

  @Test
  void nullBinaryValueWritesNullFields() throws Exception {
    Files.writeString(tempDir.resolve("event.avsc"), EVENT.toString());
    AvroDecode transform =
        transform(new AvroDecodeData(), tempDir.resolve("event.avsc").toString(), binaryTargets());
    List<Object[]> output = new ArrayList<>();
    feed(transform, output, new Object[] {null});

    assertTrue(transform.processRow());
    assertEquals(1, output.size());
    assertNull(output.get(0)[0]);
    assertNull(output.get(0)[1]);
    assertNull(output.get(0)[2]);
    assertNull(output.get(0)[3]);
    assertNull(output.get(0)[4]);
    assertNull(output.get(0)[5]);
  }

  @Test
  void trailingBytesAreASchemaMismatch() throws Exception {
    byte[] encoded = encode(event(1L, "alice", true, new byte[] {1}, 1));
    byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
    trailing[trailing.length - 1] = 1;

    HopException e = assertThrows(HopException.class, () -> decodeSingle(trailing));

    assertTrue(e.getMessage().contains("schema mismatch"), e.getMessage());
    assertTrue(e.getMessage().contains("bytes remain"), e.getMessage());
  }

  @Test
  void missingBytesAreASchemaMismatch() throws Exception {
    byte[] encoded = encode(event(1L, "alice", true, new byte[] {1, 2, 3}, 7));
    byte[] truncated = Arrays.copyOf(encoded, encoded.length - 1);

    HopException e = assertThrows(HopException.class, () -> decodeSingle(truncated));

    assertTrue(e.getMessage().contains("schema mismatch"), e.getMessage());
    assertFalse(e.getMessage().contains("bytes remain"), e.getMessage());
    assertInstanceOf(Exception.class, e.getCause());
  }

  @Test
  void schemaMismatchGoesToTheErrorHopAndTheNextRowIsDecoded() throws Exception {
    Files.writeString(tempDir.resolve("event.avsc"), EVENT.toString());
    byte[] good = encode(event(1L, "alice", true, new byte[] {1, 2}, 4));
    byte[] encoded = encode(event(2L, "bob", false, new byte[] {3}, 5));
    byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
    byte[] after = encode(event(3L, "cara", true, new byte[] {6}, 8));

    AvroDecodeData data = new AvroDecodeData();
    AvroDecode transform =
        spy(transform(data, tempDir.resolve("event.avsc").toString(), binaryTargets()));
    when(tmh.transformMeta.isDoingErrorHandling()).thenReturn(true);
    List<String> errors = new ArrayList<>();
    List<String> codes = new ArrayList<>();
    doAnswer(
            invocation -> {
              errors.add(invocation.getArgument(3));
              codes.add(invocation.getArgument(5));
              return null;
            })
        .when(transform)
        .putError(any(), any(), anyLong(), any(), any(), any());

    List<Object[]> output = new ArrayList<>();
    feed(transform, output, new Object[] {good}, new Object[] {trailing}, new Object[] {after});

    assertTrue(transform.processRow());
    Object decoder = data.decoder;
    assertTrue(transform.processRow());
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("schema mismatch"), errors.get(0));
    assertEquals(List.of("AvroDecode001"), codes);
    assertSame(decoder, data.decoder);
    assertEquals(2, output.size());
    assertRow(output.get(0), 1L, "alice", true, new byte[] {1, 2}, 4L);
    assertRow(output.get(1), 3L, "cara", true, new byte[] {6}, 8L);
  }

  @Test
  void avroRecordIgnoresTheSchemaFile() throws Exception {
    GenericRecord record = event(4L, "dora", true, new byte[] {1}, 2);
    AvroDecodeMeta meta = new AvroDecodeMeta();
    meta.setSourceFieldName("avro");
    meta.setSchemaFileName("ram:///avro-decode-9695/does-not-exist.avsc");
    meta.setTargetFields(List.of(new TargetField("id", null, "id", "Integer", null, null, null)));
    AvroDecode transform =
        new AvroDecode(
            tmh.transformMeta, meta, new AvroDecodeData(), 0, tmh.pipelineMeta, tmh.pipeline);
    transform.init();
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaAvroRecord("avro"));
    List<Object[]> inputRows = new ArrayList<>();
    inputRows.add(new Object[] {record});
    IRowSet rowSet = tmh.getMockInputRowSet(inputRows);
    when(rowSet.getRowMeta()).thenReturn(inputRowMeta);
    transform.addRowSetToInputRowSets(rowSet);
    List<Object[]> output = new ArrayList<>();
    listen(transform, output);

    assertTrue(transform.processRow());
    assertFalse(transform.processRow());
    assertEquals(4L, output.get(0)[1]);
  }

  @Test
  void binaryWithoutASchemaFileIsRejected() throws Exception {
    AvroDecode transform = transform(new AvroDecodeData(), "", binaryTargets());
    feed(
        transform,
        new ArrayList<>(),
        new Object[] {encode(event(1L, "a", true, new byte[] {1}, 1))});

    HopException e = assertThrows(HopException.class, transform::processRow);

    assertTrue(e.getMessage().contains("We can only decode Avro data types"), e.getMessage());
    assertTrue(e.getMessage().contains("Binary"), e.getMessage());
  }

  @Test
  void missingSchemaFileNamesTheResolvedPath() throws Exception {
    AvroDecode transform =
        transform(new AvroDecodeData(), "${SCHEMA}/missing.avsc", binaryTargets());
    transform.setVariable("SCHEMA", tempDir.toString());
    feed(transform, new ArrayList<>(), new Object[] {new byte[] {1}});

    HopException e = assertThrows(HopException.class, transform::processRow);

    assertTrue(e.getMessage().contains(tempDir.resolve("missing.avsc").toString()), e.getMessage());
  }

  @Test
  void schemaFileMustBeARecord() throws Exception {
    Path schemaFile = tempDir.resolve("string.avsc");
    Files.writeString(schemaFile, "\"string\"");
    AvroDecode transform = transform(new AvroDecodeData(), schemaFile.toString(), binaryTargets());
    feed(transform, new ArrayList<>(), new Object[] {new byte[] {1}});

    HopException e = assertThrows(HopException.class, transform::processRow);

    assertTrue(e.getMessage().contains("does not describe a record"), e.getMessage());
  }

  @Test
  void unknownTargetFieldFailsBeforeDecoding() throws Exception {
    Files.writeString(tempDir.resolve("event.avsc"), EVENT.toString());
    AvroDecode transform =
        spy(
            transform(
                new AvroDecodeData(),
                tempDir.resolve("event.avsc").toString(),
                List.of(new TargetField("missing", null, "missing", "String", null, null, null))));
    when(tmh.transformMeta.isDoingErrorHandling()).thenReturn(true);
    feed(
        transform,
        new ArrayList<>(),
        new Object[] {encode(event(1L, "a", true, new byte[] {1}, 1))});

    HopException e = assertThrows(HopException.class, transform::processRow);

    assertTrue(e.getMessage().contains("missing"), e.getMessage());
  }

  @Test
  void schemaFileListsFieldsForGetFields() throws Exception {
    Files.writeString(tempDir.resolve("event.avsc"), EVENT.toString());
    Variables variables = new Variables();
    variables.setVariable("SCHEMA", tempDir.toString());

    Schema schema = AvroDecode.readSchemaFile(variables, "${SCHEMA}/event.avsc");
    List<String> names =
        AvroDecodeFieldFinder.rowsForSchema(schema).stream().map(FieldRow::sourceField).toList();

    assertEquals(List.of("active", "count", "id", "name", "payload"), names);
  }

  @Test
  void schemaFileNameRoundTripsThroughXml() throws Exception {
    AvroDecodeMeta meta = new AvroDecodeMeta();
    meta.setSchemaFileName("${SCHEMA}/event.avsc");
    String xml = "<transform>" + meta.getXml() + "</transform>";

    AvroDecodeMeta loaded = new AvroDecodeMeta();
    loaded.loadXml(
        XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform"),
        new MemoryMetadataProvider());

    assertEquals("${SCHEMA}/event.avsc", loaded.getSchemaFileName());
  }

  @Test
  void supportsErrorHandling() {
    assertTrue(new AvroDecodeMeta().supportsErrorHandling());
  }

  private void decodeSingle(byte[] bytes) throws Exception {
    Files.writeString(tempDir.resolve("event.avsc"), EVENT.toString());
    AvroDecode transform =
        transform(new AvroDecodeData(), tempDir.resolve("event.avsc").toString(), binaryTargets());
    feed(transform, new ArrayList<>(), new Object[] {bytes});
    transform.processRow();
  }

  private AvroDecode transform(
      AvroDecodeData data, String schemaFileName, List<TargetField> targets) throws Exception {
    AvroDecodeMeta meta = new AvroDecodeMeta();
    meta.setSourceFieldName("message");
    meta.setSchemaFileName(schemaFileName);
    meta.setTargetFields(targets);
    AvroDecode transform =
        new AvroDecode(tmh.transformMeta, meta, data, 0, tmh.pipelineMeta, tmh.pipeline);
    transform.init();
    return transform;
  }

  private void feed(AvroDecode transform, List<Object[]> output, Object[]... rows)
      throws Exception {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaBinary("message"));
    List<Object[]> inputRows = new ArrayList<>();
    Collections.addAll(inputRows, rows);
    IRowSet rowSet = tmh.getMockInputRowSet(inputRows);
    when(rowSet.getRowMeta()).thenReturn(inputRowMeta);
    transform.addRowSetToInputRowSets(rowSet);
    listen(transform, output);
  }

  private static void listen(AvroDecode transform, List<Object[]> output) {
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            output.add(Arrays.copyOf(row, row.length));
          }
        });
  }

  private static List<TargetField> binaryTargets() {
    return List.of(
        new TargetField("id", null, "id", "Integer", null, null, null),
        new TargetField("name", null, "name", "String", null, null, null),
        new TargetField("active", null, "active", "Boolean", null, null, null),
        new TargetField("payload", null, "payload", "Binary", null, null, null),
        new TargetField("count", null, "count", "Integer", null, null, null));
  }

  private static void assertRow(
      Object[] row, long id, String name, boolean active, byte[] payload, long count) {
    assertEquals(id, row[1]);
    assertEquals(name, row[2]);
    assertEquals(active, row[3]);
    assertArrayEquals(payload, (byte[]) row[4]);
    assertEquals(count, row[5]);
  }

  private static GenericRecord event(
      long id, String name, boolean active, byte[] payload, int count) {
    GenericRecord record = new GenericData.Record(EVENT);
    record.put("id", id);
    record.put("name", name);
    record.put("active", active);
    record.put("payload", ByteBuffer.wrap(payload));
    record.put("count", count);
    return record;
  }

  private static byte[] encode(GenericRecord record) throws Exception {
    return encode(EVENT, record);
  }

  private static byte[] encode(Schema schema, GenericRecord record) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }
}
