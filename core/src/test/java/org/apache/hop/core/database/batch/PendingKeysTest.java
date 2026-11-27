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

package org.apache.hop.core.database.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/**
 * The keys of waiting rows compare the way a database might, never stricter: two keys a database
 * could find equal must be equal here; two keys that are equal here only cost an extra flush.
 */
class PendingKeysTest {

  private static void assertSameKey(Object a, Object b) {
    assertEquals(PendingKeys.normalize(a), PendingKeys.normalize(b), a + " and " + b);
  }

  private static void assertOtherKey(Object a, Object b) {
    assertNotEquals(PendingKeys.normalize(a), PendingKeys.normalize(b), a + " and " + b);
  }

  @Test
  void numbersCompareByValueWhateverTheirType() {
    assertSameKey(1, 1L);
    assertSameKey(1L, new BigDecimal("1.00"));
    assertSameKey((short) 1, BigInteger.ONE);
    assertSameKey((byte) 1, 1.0d);
    assertSameKey(1.5f, new BigDecimal("1.50"));
    assertSameKey(new BigDecimal("0.000"), 0L);
    assertSameKey(-0.0d, 0L);
    assertOtherKey(1L, 2L);
    assertOtherKey(new BigDecimal("1.01"), 1L);
  }

  @Test
  void textThatHoldsANumberIsThatNumber() {
    assertSameKey("07", 7L);
    assertSameKey(" 7 ", "7.0");
    assertSameKey("-1", -1);
    assertOtherKey("7a", 7L);
  }

  @Test
  void textComparesWithoutCaseAccentsAndSurroundingSpaces() {
    assertSameKey("Abc", "aBC");
    assertSameKey("abc  ", "  abc");
    assertSameKey("Café", "CAFE");
    assertSameKey("Straße", "STRASSE");
    assertSameKey("ﬁle", "file");
    assertOtherKey("a b", "ab");
    assertOtherKey("abc", "abd");
  }

  @Test
  void datesCompareByTheirInstant() {
    long now = 1_700_000_000_123L;
    assertSameKey(new Date(now), new Timestamp(now));
    assertOtherKey(new Date(now), new Date(now + 1));
    // A date is not the number of its milliseconds
    assertOtherKey(new Date(now), now);
  }

  @Test
  void bytesCompareByContent() {
    assertSameKey("ab".getBytes(StandardCharsets.UTF_8), new byte[] {'a', 'b'});
    assertOtherKey(new byte[] {1}, new byte[] {2});
  }

  /** Empty text is null, as Oracle stores it. */
  @Test
  void nullEqualsNullAndEmptyText() {
    assertNull(PendingKeys.normalize(null));
    assertSameKey(null, "");
    assertSameKey(null, "  ");
    assertOtherKey(null, 0L);
    assertOtherKey(null, "0");
  }

  @Test
  void aKeyIsTheNormalizedValuesOfItsFields() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("code"));
    rowMeta.addValueMeta(new ValueMetaString("name"));

    List<Object> key = PendingKeys.key(rowMeta, new Object[] {5L, " Ab ", "x"}, new int[] {1, 0});

    assertEquals(List.of("ab", BigDecimal.valueOf(5)), key);
  }

  /** A value in lazy (binary string) storage is converted before it is compared. */
  @Test
  void aLazyValueIsConvertedFirst() throws Exception {
    ValueMetaString code = new ValueMetaString("code");
    code.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    code.setStorageMetadata(new ValueMetaString("code"));
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(code);

    List<Object> key =
        PendingKeys.key(
            rowMeta, new Object[] {"ABC".getBytes(StandardCharsets.UTF_8)}, new int[] {0});

    assertEquals(List.of("abc"), key);
  }
}
