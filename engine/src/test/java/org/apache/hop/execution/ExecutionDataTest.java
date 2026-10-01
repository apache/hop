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
 *
 */

package org.apache.hop.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.TestUtil;
import org.apache.hop.execution.caching.CacheEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExecutionDataTest {

  @BeforeEach
  void before() throws Exception {
    // Load data type plugins
    HopClientEnvironment.init();
    TestUtil.registerTestPluginTypes();
  }

  @Test
  void testSerialization() throws Exception {
    IRowMeta rowMeta =
        new RowMetaBuilder()
            .addInteger("id", 9)
            .addString("firstName", 35)
            .addString("lastName", 35)
            .addBoolean("enabled")
            .addNumber("someNumber", 7, 3)
            .addBigNumber("bigNumber", 25, 10)
            .addDate("logDate")
            .build();
    List<Object[]> rows =
        Arrays.asList(
            new Object[] {
              1L,
              "Apache",
              "Hop",
              true,
              987.654,
              new BigDecimal("132384738943236.2345678901"),
              new Date()
            },
            new Object[] {
              2L,
              "Apache",
              "Beam",
              false,
              876.543,
              new BigDecimal("132344728963226.2325679902"),
              new Date()
            },
            new Object[] {
              3L,
              "Apache",
              "Spark",
              true,
              765.432,
              new BigDecimal("231385738943236.1325658801"),
              new Date()
            },
            new Object[] {
              4L,
              "Apache",
              "Flink",
              false,
              654.321,
              new BigDecimal("290375731946235.9325653811"),
              new Date()
            },
            new Object[] {
              5L,
              "GCP",
              "Dataflow",
              true,
              543.210,
              new BigDecimal("910365731946235.5322603719"),
              new Date()
            },
            new Object[] {6L, "Nulls", null, null, null, null, null});

    ExecutionDataSetMeta setMeta =
        new ExecutionDataSetMeta(
            "firstRows", "12345-logchannel-id", "transformName", "0", "First rows of transform");
    ExecutionData data =
        ExecutionDataBuilder.of()
            .addDataSets(Map.of("firstRows", new RowBuffer(rowMeta, rows)))
            .addSetMeta(Map.of("firstRows", setMeta))
            .withParentId("parentId")
            .withOwnerId("ownerId")
            .build();

    // Serialize to JSON and back
    //
    ObjectMapper objectMapper = HopJson.newMapper();
    String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(data);

    assertNotNull(json);

    ExecutionData copy = objectMapper.readValue(json, ExecutionData.class);

    assertNotNull(copy);

    assertEquals(data, copy);
  }

  @Test
  void testAvroRecordRoundTripWithoutSchemaOnMetadata() throws Exception {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"msg\",\"fields\":[{\"name\":\"body\",\"type\":\"string\"}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("body", new Utf8("hello"));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaAvroRecord("record"));
    rowMeta.addValueMeta(new ValueMetaInteger("n"));
    List<Object[]> rows = List.<Object[]>of(new Object[] {"hop", record, 5L});

    ExecutionData data =
        ExecutionDataBuilder.of()
            .addDataSets(Map.of("rows", new RowBuffer(rowMeta, rows)))
            .withParentId("parentId")
            .withOwnerId(ExecutionDataBuilder.ALL_TRANSFORMS)
            .build();

    ObjectMapper objectMapper = HopJson.newMapper();
    ExecutionData copy =
        objectMapper.readValue(objectMapper.writeValueAsString(data), ExecutionData.class);

    assertEquals(data, copy);
    GenericRecord loaded = (GenericRecord) copy.getDataSets().get("rows").getBuffer().get(0)[1];
    assertEquals(new Utf8("hello"), loaded.get("body"));
  }

  @Test
  void testOldAvroPayloadDoesNotRejectCacheEntry() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaAvroRecord("record"));

    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"msg\",\"fields\":[{\"name\":\"body\",\"type\":\"string\"}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("body", "stored-without-schema");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (GZIPOutputStream gzos = new GZIPOutputStream(baos);
        DataOutputStream dos = new DataOutputStream(gzos)) {
      dos.writeInt(1);
      dos.writeUTF("samples");
      rowMeta.writeMeta(dos);
      dos.writeInt(2);
      // A null Avro value has the same layout as before: a single true flag.
      rowMeta.writeData(dos, new Object[] {"before", null});
      // The old writer then streamed a raw Avro encoder with no length and no schema.
      new ValueMetaString("name").writeData(dos, "after");
      dos.writeBoolean(false);
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
      writer.write(record, EncoderFactory.get().directBinaryEncoder(dos, null));
    }
    String oldBlob = Base64.getEncoder().encodeToString(baos.toByteArray());

    IRowMeta keptMeta = new RowMetaBuilder().addString("name").build();
    ExecutionData sibling =
        ExecutionDataBuilder.of()
            .withOwnerId("transform-copy")
            .withParentId("parent-id")
            .addDataSets(
                Map.of("rows", new RowBuffer(keptMeta, List.<Object[]>of(new Object[] {"kept"}))))
            .build();

    CacheEntry entry = new CacheEntry();
    entry.setId("parent-id");
    entry.setName("kafka-pipeline");
    entry.getChildExecutionData().put("transform-copy", sibling);

    ObjectMapper objectMapper = HopJson.newMapper();
    ObjectNode node = (ObjectNode) objectMapper.readTree(objectMapper.writeValueAsString(entry));
    ObjectNode allTransforms = objectMapper.createObjectNode();
    allTransforms.put("ownerId", ExecutionDataBuilder.ALL_TRANSFORMS);
    allTransforms.put("parentId", "parent-id");
    allTransforms.put("rowsBinaryGzipBase64Encoded", oldBlob);
    ((ObjectNode) node.get("childExecutionData"))
        .set(ExecutionDataBuilder.ALL_TRANSFORMS, allTransforms);

    CacheEntry loaded =
        objectMapper.readValue(objectMapper.writeValueAsString(node), CacheEntry.class);

    assertEquals("parent-id", loaded.getId());
    assertEquals("kafka-pipeline", loaded.getName());
    ExecutionData loadedSibling = loaded.getChildExecutionData().get("transform-copy");
    assertEquals("kept", loadedSibling.getDataSets().get("rows").getBuffer().get(0)[0]);

    ExecutionData samples = loaded.getChildExecutionData().get(ExecutionDataBuilder.ALL_TRANSFORMS);
    assertNotNull(samples);
    List<Object[]> keptRows = samples.getDataSets().get("samples").getBuffer();
    assertEquals(1, keptRows.size());
    assertEquals("before", keptRows.get(0)[0]);
    assertNull(keptRows.get(0)[1]);
  }
}
