/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.beam.transforms.elasticsearch;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.ElasticsearchJsonFn;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class BeamElasticsearchJsonFnTest {
  @BeforeAll
  static void init() throws Exception {
    BeamHop.init();
  }

  @Test
  void convertsLazyHopStringAndCompactsMultilineJsonForBulkNdjson() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("ignored"));
    ValueMetaString payload = new ValueMetaString("payload");
    payload.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    payload.setStorageMetadata(new ValueMetaString("payload"));
    rowMeta.addValueMeta(payload);
    ElasticsearchJsonFn fn = new ElasticsearchJsonFn("write", "payload", rowMeta.getMetaXml());
    fn.setup();
    assertEquals(
        "{\"name\":\"é\"}",
        extract(
            fn,
            new HopRow(
                new Object[] {
                  "ignored",
                  "{\n  \"name\": \"é\"\n}".getBytes(java.nio.charset.StandardCharsets.UTF_8)
                })));
  }

  @Test
  void compactionKeepsNumericMeaningOfPreciseAndExtremeValues() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("payload"));
    ElasticsearchJsonFn fn = new ElasticsearchJsonFn("write", "payload", rowMeta.getMetaXml());
    fn.setup();
    String compact =
        extract(
            fn,
            new HopRow(
                new Object[] {
                  "{\n"
                      + "  \"decimal\": 0.123456789012345678901234567890,\n"
                      + "  \"id\": 9007199254740993,\n"
                      + "  \"tiny\": 1.23e-400,\n"
                      + "  \"huge\": 1.23e+309,\n"
                      + "  \"wide\": 123456789012345678901234567890,\n"
                      + "  \"nested\": { \"n\": 0.100000000000000000000000000001 },\n"
                      + "  \"list\": [ 9007199254740993 ]\n"
                      + "}"
                }));
    assertFalse(compact.contains("\n"));
    assertFalse(compact.contains("Infinity"));
    assertNumber(compact, "decimal", "0.123456789012345678901234567890");
    assertNumber(compact, "id", "9007199254740993");
    assertNumber(compact, "tiny", "1.23e-400");
    assertNumber(compact, "huge", "1.23e+309");
    assertNumber(compact, "wide", "123456789012345678901234567890");
    assertNumber(compact, "n", "0.100000000000000000000000000001");
    assertNumber(compact, "list", "9007199254740993");
  }

  static void assertNumber(String json, String field, String expected) {
    String marker = "\"" + field + "\":";
    int at = json.indexOf(marker);
    assertTrue(at >= 0, field);
    int start = at + marker.length();
    while (start < json.length() && (json.charAt(start) == '[' || json.charAt(start) == ' ')) {
      start++;
    }
    int end = start;
    while (end < json.length()) {
      char c = json.charAt(end);
      if ((c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.' || c == 'e' || c == 'E') {
        end++;
      } else {
        break;
      }
    }
    String token = json.substring(start, end);
    assertFalse(token.isEmpty(), json);
    assertEquals(0, new BigDecimal(token).compareTo(new BigDecimal(expected)), token);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "invalid secret document", "[]", "1", "{} trailing"})
  void rejectsNullEmptyOrNonObjectJsonWithoutLeakingDocument(String document) throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("payload"));
    ElasticsearchJsonFn fn = new ElasticsearchJsonFn("write", "payload", rowMeta.getMetaXml());
    fn.setup();
    HopException error =
        assertThrows(HopException.class, () -> extract(fn, new HopRow(new Object[] {document})));
    assertFalse(error.toString().contains("secret document"));
    assertNull(error.getCause(), "parser errors contain document data");
  }

  @Test
  void rejectsShortRowsWithFieldContext() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("payload"));
    ElasticsearchJsonFn fn = new ElasticsearchJsonFn("write", "payload", rowMeta.getMetaXml());
    fn.setup();
    assertThrows(HopException.class, () -> extract(fn, new HopRow(new Object[0])));
  }

  static String extract(ElasticsearchJsonFn fn, HopRow row) throws Exception {
    try {
      return (String)
          ElasticsearchJsonFn.class.getMethod("extractJson", HopRow.class).invoke(fn, row);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof HopException cause) {
        throw cause;
      }
      throw e;
    }
  }
}
