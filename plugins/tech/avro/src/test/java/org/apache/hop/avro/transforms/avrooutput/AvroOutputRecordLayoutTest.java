/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Where each value goes in the Avro record is worked out once, on the first row. Every row after
 * that has to end up with its own values in the right fields, nested records included.
 */
class AvroOutputRecordLayoutTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  private TransformMockHelper<AvroOutputMeta, AvroOutputData> mockHelper;

  @BeforeAll
  static void initEnv() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("AvroOutput", AvroOutputMeta.class, AvroOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  private static IRowMeta rowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("city"));
    rowMeta.addValueMeta(new ValueMetaString("zip"));
    rowMeta.addValueMeta(new ValueMetaNumber("lat"));
    rowMeta.addValueMeta(new ValueMetaNumber("amount"));
    return rowMeta;
  }

  private List<GenericRecord> write(List<AvroOutputField> fields, Object[]... rows)
      throws Exception {
    Path file = tempDir.resolve("out.avro");
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setDefault();
    meta.setOutputType(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE]);
    meta.setCreateSchemaFile(true);
    meta.setWriteSchemaFile(false);
    meta.setRecordName("customer");
    meta.setNamespace("org.apache.hop.test");
    meta.setFileName(file.toString());
    meta.setOutputFields(fields);

    AvroOutput transform =
        spy(
            new AvroOutput(
                mockHelper.transformMeta,
                meta,
                new AvroOutputData(),
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));
    doReturn(rowMeta()).when(transform).getInputRowMeta();
    Object[][] rest = new Object[rows.length][];
    System.arraycopy(rows, 1, rest, 0, rows.length - 1);
    doReturn(rows[0], (Object[]) rest).when(transform).getRow();
    doAnswer(inv -> null).when(transform).putRow(any(), any());

    assertTrue(transform.init());
    for (int i = 0; i < rows.length; i++) {
      assertTrue(transform.processRow());
    }
    assertFalse(transform.processRow());
    transform.dispose();

    List<GenericRecord> records = new ArrayList<>();
    try (InputStream inputStream = Files.newInputStream(file);
        DataFileStream<GenericRecord> reader =
            new DataFileStream<>(inputStream, new GenericDatumReader<>())) {
      reader.forEach(records::add);
    }
    return records;
  }

  @Test
  void flatFieldsKeepTheirValuesRowAfterRow() throws Exception {
    List<AvroOutputField> fields = new ArrayList<>();
    fields.add(new AvroOutputField("id", "id", AvroOutputField.AVRO_TYPE_LONG, true));
    fields.add(new AvroOutputField("name", "full_name", AvroOutputField.AVRO_TYPE_STRING, true));
    fields.add(new AvroOutputField("amount", "amount", AvroOutputField.AVRO_TYPE_DOUBLE, true));

    List<GenericRecord> records =
        write(
            fields,
            new Object[] {1L, "Alice", "Ghent", "9000", 51.05, 10.5},
            new Object[] {2L, null, "Lier", "2500", 51.13, null},
            new Object[] {3L, "Carol", "Mol", "2400", 51.19, 30.25});

    assertEquals(3, records.size());
    assertEquals(1L, records.get(0).get("id"));
    assertEquals("Alice", records.get(0).get("full_name").toString());
    assertEquals(10.5, records.get(0).get("amount"));
    // a null value leaves the field empty
    assertEquals(2L, records.get(1).get("id"));
    assertNull(records.get(1).get("full_name"));
    assertNull(records.get(1).get("amount"));
    assertEquals("Carol", records.get(2).get("full_name").toString());
    assertEquals(30.25, records.get(2).get("amount"));
  }

  @Test
  void nestedRecordsGetTheirOwnValues() throws Exception {
    List<AvroOutputField> fields = new ArrayList<>();
    fields.add(new AvroOutputField("id", "id", AvroOutputField.AVRO_TYPE_LONG, true));
    fields.add(new AvroOutputField("city", "address.city", AvroOutputField.AVRO_TYPE_STRING, true));
    fields.add(new AvroOutputField("zip", "address.zip", AvroOutputField.AVRO_TYPE_STRING, true));
    fields.add(
        new AvroOutputField("lat", "address.geo.lat", AvroOutputField.AVRO_TYPE_DOUBLE, true));
    fields.add(new AvroOutputField("name", "$.name", AvroOutputField.AVRO_TYPE_STRING, true));

    List<GenericRecord> records =
        write(
            fields,
            new Object[] {1L, "Alice", "Ghent", "9000", 51.05, 10.5},
            new Object[] {2L, "Bob", "Lier", null, 51.13, 20.0});

    assertEquals(2, records.size());
    for (int i = 0; i < 2; i++) {
      GenericRecord record = records.get(i);
      GenericRecord address = (GenericRecord) record.get("address");
      GenericRecord geo = (GenericRecord) address.get("geo");
      assertEquals((long) i + 1, record.get("id"));
      assertEquals(i == 0 ? "Alice" : "Bob", record.get("name").toString());
      assertEquals(i == 0 ? "Ghent" : "Lier", address.get("city").toString());
      assertEquals(i == 0 ? 51.05 : 51.13, geo.get("lat"));
    }
    assertEquals("9000", ((GenericRecord) records.get(0).get("address")).get("zip").toString());
    assertNull(((GenericRecord) records.get(1).get("address")).get("zip"));
  }
}
