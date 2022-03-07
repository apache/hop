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

package org.apache.hop.core.row.value;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hop.core.HopClientEnvironment;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.*;

public class ValueMetaAvroRecordTest {

  private static final String schemaJson =
      "{\n"
          + "  \"doc\": \"No documentation URL for now\",\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"name\": \"id\",\n"
          + "      \"type\": [\n"
          + "        \"long\",\n"
          + "        \"null\"\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"sysdate\",\n"
          + "      \"type\": [\n"
          + "        \"string\",\n"
          + "        \"null\"\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"num\",\n"
          + "      \"type\": [\n"
          + "        \"double\",\n"
          + "        \"null\"\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"int\",\n"
          + "      \"type\": [\n"
          + "        \"long\",\n"
          + "        \"null\"\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"str\",\n"
          + "      \"type\": [\n"
          + "        \"string\",\n"
          + "        \"null\"\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"uuid\",\n"
          + "      \"type\": [\n"
          + "        \"string\",\n"
          + "        \"null\"\n"
          + "      ]\n"
          + "    }\n"
          + "  ],\n"
          + "  \"name\": \"all_values\",\n"
          + "  \"namespace\": \"hop.apache.org\",\n"
          + "  \"type\": \"record\"\n"
          + "}";

  @BeforeClass
  public static void before() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  public void testCloneMeta() {
    Schema schema = new Schema.Parser().parse(schemaJson);
    String schemaString1 = schema.toString(true);
    ValueMetaAvroRecord valueMeta = new ValueMetaAvroRecord("test");
    valueMeta.setSchema(schema);

    ValueMetaAvroRecord cloned = valueMeta.clone();
    String schemaString2 = cloned.getSchema().toString(true);

    assertEquals(schemaString1, schemaString2);
    assertEquals(valueMeta.getName(), cloned.getName());
  }

  @Test
  public void testCloneData() throws Exception {
    GenericRecord genericRecord = generateGenericRecord();
    ValueMetaAvroRecord valueMeta = new ValueMetaAvroRecord("test", genericRecord.getSchema());

    GenericRecord cloned = (GenericRecord) valueMeta.cloneValueData(genericRecord);

    assertFalse(genericRecord==cloned);
    verifyGenericRecords(genericRecord, cloned);
  }

  @Test
  public void testToStringMeta() {
    ValueMetaAvroRecord valueMeta = new ValueMetaAvroRecord("test");

    assertEquals("Avro Generic Record", valueMeta.toStringMeta());

    Schema schema = new Schema.Parser().parse(schemaJson);
    valueMeta.setSchema(schema);

    assertEquals(
        "Avro Generic Record {\"type\":\"record\",\"name\":\"all_values\",\"namespace\":\"hop.apache.org\"" +
                ",\"doc\":\"No documentation URL for now\",\"fields\":[{\"name\":\"id\",\"type\":" +
                "[\"long\",\"null\"]},{\"name\":\"sysdate\",\"type\":[\"string\",\"null\"]}," +
                "{\"name\":\"num\",\"type\":[\"double\",\"null\"]},{\"name\":\"int\",\"type\":" +
                "[\"long\",\"null\"]},{\"name\":\"str\",\"type\":[\"string\",\"null\"]}," +
                "{\"name\":\"uuid\",\"type\":[\"string\",\"null\"]}]}",
        valueMeta.toStringMeta());
  }

  @Test
  public void testWriteReadMeta() throws Exception {
    Schema schema = new Schema.Parser().parse(schemaJson);
    ValueMetaAvroRecord valueMeta = new ValueMetaAvroRecord("test", schema);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    valueMeta.writeMeta(dataOutputStream);
    dataOutputStream.flush();
    dataOutputStream.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(inputStream);
    ValueMetaAvroRecord loaded = (ValueMetaAvroRecord) ValueMetaFactory.loadValueMeta(dataInputStream);

    assertEquals(valueMeta.getName(), loaded.getName());
    assertEquals(valueMeta.getSchema().toString(true), loaded.getSchema().toString(true));
  }

  @Test
  public void testStoreLoadMetaInJson() throws Exception {
    Schema schema = new Schema.Parser().parse(schemaJson);
    ValueMetaAvroRecord valueMeta = new ValueMetaAvroRecord("test", schema);

    JSONObject jValue = new JSONObject();
    valueMeta.storeMetaInJson(jValue);

    String valueJson = jValue.toJSONString();
    assertEquals(
        "{\"schema\":{\"name\":\"all_values\",\"namespace\":\"hop.apache.org\",\"doc\":" +
                "\"No documentation URL for now\",\"type\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":" +
                "[\"long\",\"null\"]},{\"name\":\"sysdate\",\"type\":[\"string\",\"null\"]}," +
                "{\"name\":\"num\",\"type\":[\"double\",\"null\"]},{\"name\":\"int\",\"type\":" +
                "[\"long\",\"null\"]},{\"name\":\"str\",\"type\":[\"string\",\"null\"]}," +
                "{\"name\":\"uuid\",\"type\":[\"string\",\"null\"]}]},\"precision\":-1,\"name\":" +
                "\"test\",\"length\":-1,\"conversionMask\":null,\"type\":20}",
        valueJson);

    // Read it back...
    //
    JSONObject jLoaded = (JSONObject) new JSONParser().parse(valueJson);

    ValueMetaAvroRecord loaded = (ValueMetaAvroRecord) ValueMetaFactory.loadValueMetaFromJson(jLoaded);

    assertEquals(valueMeta.getName(), loaded.getName());
    assertEquals(valueMeta.getSchema().toString(true), loaded.getSchema().toString(true));
  }

  @Test
  public void testWriteReadData() throws Exception {
    GenericRecord genericRecord = generateGenericRecord();
    Schema schema = genericRecord.getSchema();
    ValueMetaAvroRecord valueMeta = new ValueMetaAvroRecord("test", schema);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    valueMeta.writeData(outputStream, genericRecord);
    outputStream.close();

    // Read it back...
    //
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    DataInputStream inputStream = new DataInputStream(byteArrayInputStream);
    GenericRecord verify = (GenericRecord) valueMeta.readData(inputStream);

    verifyGenericRecords(genericRecord, verify);
  }

  private GenericRecord generateGenericRecord() {
    Schema schema = new Schema.Parser().parse(schemaJson);
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("id", 1234567L);
    genericRecord.put("sysdate", new Utf8("2021/02/04 15:28:45.999"));
    genericRecord.put("num", 1234.5678);
    genericRecord.put("int", 987654321L);
    genericRecord.put("str", new Utf8("Apache Hop"));
    genericRecord.put("uuid", new Utf8("193343413af2349123"));
    return genericRecord;
  }

  private void verifyGenericRecords(GenericRecord genericRecord, GenericRecord verify) {
    for (String key : new String[] {"id", "sysdate", "num", "int", "str", "uuid"}) {
      assertTrue(genericRecord.hasField(key));
      assertTrue(verify.hasField(key));
      assertEquals(genericRecord.get(key), verify.get(key));
    }
  }
}
