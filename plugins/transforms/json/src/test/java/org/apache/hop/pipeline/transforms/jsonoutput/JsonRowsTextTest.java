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

package org.apache.hop.pipeline.transforms.jsonoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.List;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * {@link JsonRowsText} has to give, character for character, the text json-simple gives for the
 * objects the transform used to build.
 */
class JsonRowsTextTest {

  // not as unicode escapes: the compiler would read those as line ends inside the string
  private static final char LINE_SEPARATOR = (char) 0x2028;
  private static final char PARAGRAPH_SEPARATOR = (char) 0x2029;

  /** The text the transform used to produce: an object holding an array of one object per row. */
  @SuppressWarnings("unchecked")
  private static String withJsonSimple(String blockName, List<String> names, Object[]... rows) {
    JSONArray array = new JSONArray();
    for (Object[] row : rows) {
      JSONObject object = new JSONObject();
      for (int i = 0; i < names.size(); i++) {
        object.put(names.get(i), row[i]);
      }
      array.add(object);
    }
    JSONObject document = new JSONObject();
    document.put(blockName, array);
    return document.toJSONString();
  }

  private static String withRowsText(String blockName, List<String> names, Object[]... rows) {
    JsonRowsText rowsText = new JsonRowsText(names);
    StringBuilder text = new StringBuilder();
    JsonRowsText.appendStart(text, blockName);
    boolean first = true;
    for (Object[] row : rows) {
      rowsText.appendRow(text, first, row);
      first = false;
    }
    JsonRowsText.appendEnd(text);
    return text.toString();
  }

  private static void assertSameText(String blockName, List<String> names, Object[]... rows) {
    assertEquals(withJsonSimple(blockName, names, rows), withRowsText(blockName, names, rows));
  }

  @Test
  void everyValueTypeAndNull() {
    assertSameText(
        "data",
        List.of("id", "price", "total", "active", "comment"),
        new Object[] {1L, 12.5d, new BigDecimal("12345.6789"), true, "plain"},
        new Object[] {-7L, 0.0d, new BigDecimal("0.10"), false, ""},
        new Object[] {null, null, null, null, null});
  }

  @Test
  void numbersThatJavaWritesWithAnExponentOrNotAtAll() {
    assertSameText(
        "data",
        List.of("number", "bignumber"),
        new Object[] {1.0E10d, new BigDecimal("1E+3")},
        new Object[] {1.0E-5d, new BigDecimal("1E-10")},
        new Object[] {Double.NaN, new BigDecimal("123456789012345678901234567890.5")},
        new Object[] {Double.POSITIVE_INFINITY, BigDecimal.ZERO},
        new Object[] {Double.MIN_VALUE, BigDecimal.ONE});
  }

  @Test
  void textThatNeedsEscaping() {
    assertSameText(
        "data",
        List.of("text"),
        new Object[] {"quote \" backslash \\ slash / tab \t newline \n return \r"},
        new Object[] {"control \u0001 \u001f delete \u007f \u009f"},
        new Object[] {
          "accents é ü ñ, snowman ☃, separators "
              + LINE_SEPARATOR
              + PARAGRAPH_SEPARATOR
              + ", emoji 😀"
        },
        new Object[] {"</script>"});
  }

  @Test
  void namesComeOutInTheOrderJsonSimpleUses() {
    // json-simple keeps an object in a hash map: not the order of the fields
    assertSameText(
        "data",
        List.of(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "status",
            "quantity",
            "unit_price",
            "discount",
            "country",
            "channel",
            "comment"),
        new Object[] {1L, 2L, 3L, "2026-01-01", "NEW", 4L, 5.5d, 0.1d, "BE", "web", "x"},
        new Object[] {6L, 7L, 8L, "2026-01-02", "DONE", 9L, 1.5d, 0.0d, "NL", "shop", null});
  }

  @Test
  void aNameUsedTwiceKeepsItsLastValue() {
    assertSameText(
        "data",
        List.of("id", "name", "id"),
        new Object[] {1L, "first", 99L},
        new Object[] {2L, "second", null});
  }

  @Test
  void namesAndTheBlockNameAreEscapedToo() {
    assertSameText(
        "block \"name\" / with \\ things",
        List.of("a/b", "quote\"d", "é", ""),
        new Object[] {1L, 2L, 3L, 4L});
  }

  @Test
  void noRowsAndOneRowAndNoFields() {
    assertSameText("data", List.of("id"));
    assertSameText("", List.of("id"), new Object[] {1L});
    assertSameText("data", List.of(), new Object[] {}, new Object[] {});
  }
}
