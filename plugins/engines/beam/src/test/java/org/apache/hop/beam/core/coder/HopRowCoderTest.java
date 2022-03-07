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

package org.apache.hop.beam.core.coder;

import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class HopRowCoderTest extends TestCase {

  ByteArrayOutputStream outputStream;
  private HopRowCoder hopRowCoder;

  @Override
  protected void setUp() throws Exception {

    outputStream = new ByteArrayOutputStream(1000000);
    hopRowCoder = new HopRowCoder();
  }

  @Test
  public void testEncodeDecodeBasic() throws IOException {

    HopRow row1 =
        new HopRow(
            new Object[] {
              "AAA", "BBB", Long.valueOf(100), Double.valueOf(1.234), new Date(876876868), new Timestamp(810311)
            });

    hopRowCoder.encode(row1, outputStream);
    outputStream.flush();
    outputStream.close();
    byte[] bytes = outputStream.toByteArray();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    HopRow row1d = hopRowCoder.decode(inputStream);

    assertEquals(row1, row1d);
  }

  @Test
  public void testEncodeDecodeAvro() throws Exception {
    String schemaJson =
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
    Schema schema = new Schema.Parser().parse(schemaJson);

    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("id", 1234567L);
    genericRecord.put("sysdate", new Utf8("2021/02/04 15:28:45.999"));
    genericRecord.put("num", 1234.5678);
    genericRecord.put("int", 987654321L);
    genericRecord.put("str", new Utf8("Apache Hop"));
    genericRecord.put("uuid", new Utf8("193343413af2349123"));

    // Create a row with an ID, a name and an avro record...
    //
    Object[] row = new Object[] {
            123L,
            "Apache Hop",
            genericRecord
    };
    HopRow hopRow = new HopRow(row);

    HopRowCoder hopRowCoder = new HopRowCoder();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    hopRowCoder.encode(hopRow, out);
    out.flush();
    out.close();

    // Re-inflate
    //
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    HopRow decoded = hopRowCoder.decode(in);

    assertEquals(row[0], decoded.getRow()[0]);
    assertEquals(row[1], decoded.getRow()[1]);

    GenericRecord verify = (GenericRecord) decoded.getRow()[2];
    assertEquals(schema.toString(true), verify.getSchema().toString(true));
    for (String key : new String[] {"id", "sysdate", "num", "int", "str", "uuid"}) {
      assertTrue(genericRecord.hasField(key));
      assertTrue(verify.hasField(key));
      assertEquals(genericRecord.get(key), verify.get(key));
    }
  }
}
