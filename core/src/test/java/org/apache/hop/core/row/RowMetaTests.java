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

package org.apache.hop.core.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.util.Date;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit test for {@link RowMeta} */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class RowMetaTests {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void testNullRow() {
    assertNull(RowMeta.getRowSizeEstimateFromRow(null));
  }

  @Test
  void testAllTypesCoverage() throws Exception {
    // String,3 bytes (UTF-8)
    String str = "abc";
    // byte[]
    byte[] bytes = new byte[] {1, 2, 3, 4};
    // BigDecimal
    BigDecimal bd = new BigDecimal("123.45");
    // Number
    int num = 100;
    // Date
    Date date = new Date();
    // Boolean
    boolean bool = true;
    // UUID
    UUID uuid = UUID.randomUUID();
    // JsonNode
    JsonNode jsonNode = MAPPER.readTree("{\"a\":1}");
    // GenericRecord
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"f\",\"type\":\"string\"}]}");
    GenericRecord rec = new GenericData.Record(schema);
    rec.put("f", "v");
    // InetAddress
    InetAddress ip = InetAddress.getByName("127.0.0.1");
    // Unknown type
    Object unknown = new Object();

    Object[] row =
        new Object[] {null, str, bytes, bd, num, date, bool, uuid, jsonNode, rec, ip, unknown};

    Long result = RowMeta.getRowSizeEstimateFromRow(row);
    long expected = 0;
    // String
    expected += str.getBytes().length;
    // byte[]
    expected += bytes.length;
    // BigDecimal
    expected += 32;
    // Number
    expected += 8;
    // Date
    expected += 8;
    // Boolean
    expected += 1;
    // UUID
    expected += 36;
    // JsonNode
    expected += jsonNode.toString().length() * 2L;
    // GenericRecord
    expected += rec.toString().length() * 2L;
    // InetAddress
    expected += ip.getHostAddress().length() * 2L;
    // default
    expected += 64;
    assertEquals(expected, result);
  }

  @Test
  void testOnlyNullElements() {
    Object[] row = new Object[] {null, null, null};
    Long result = RowMeta.getRowSizeEstimateFromRow(row);
    assertEquals(0L, result);
  }
}
