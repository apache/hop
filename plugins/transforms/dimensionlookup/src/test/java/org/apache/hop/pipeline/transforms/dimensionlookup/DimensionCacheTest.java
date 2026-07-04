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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.util.Assert;
import org.junit.jupiter.api.Test;

class DimensionCacheTest {

  @Test
  void testCompareDateInterval() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaTimestamp("DATE_FROM"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("DATE_TO"));
    int[] keyIndexes = new int[] {};
    int fromDateIndex = 0;
    int toDateIndex = 1;
    DimensionCache dc = new DimensionCache(rowMeta, keyIndexes, fromDateIndex, toDateIndex);

    long t0 = 1425300000000L; // (3/2/15 4:40 PM)
    final Date D1 = new Timestamp(t0);
    final Date D2 = new Timestamp(t0 + 3600000L);
    final Date D3 = new Timestamp(t0 + 3600000L * 2);
    final Date D4 = new Timestamp(t0 + 3600000L * 3);
    final Date D5 = new Timestamp(t0 + 3600000L * 4);

    // NPE in DimensionCache class after update to Java 1.7u76
    // fix prevents NullPointerException in the combinations marked "NPE"

    assertCompareDateInterval(dc, null, null, null, null, 0);
    assertCompareDateInterval(dc, null, null, D1, null, -1);

    assertCompareDateInterval(dc, D2, null, null, null, 1);
    assertCompareDateInterval(dc, D2, null, D1, null, 1);
    assertCompareDateInterval(dc, D2, null, D2, null, 0);
    assertCompareDateInterval(dc, D2, null, D3, null, -1);

    assertCompareDateInterval(dc, D2, D4, null, null, 1); // NPE
    assertCompareDateInterval(dc, D2, D4, D1, null, 1);
    assertCompareDateInterval(dc, D2, D4, D2, null, 0);
    assertCompareDateInterval(dc, D2, D4, D3, null, 0);
    assertCompareDateInterval(dc, D2, D4, D4, null, -1);
    assertCompareDateInterval(dc, D2, D4, D5, null, -1);

    assertCompareDateInterval(dc, null, D4, null, null, 0); // NPE
    assertCompareDateInterval(dc, null, D4, D3, null, 0);
    assertCompareDateInterval(dc, null, D4, D4, null, -1); // NPE
    assertCompareDateInterval(dc, null, D4, D5, null, -1); // NPE
  }

  @Test
  void testValidateRowsForSortAcceptsValidAdjacentRanges() {
    DimensionCache cache = createKeyedCache();
    Date d1 = timestamp(0);
    Date d2 = timestamp(1);
    Date d3 = timestamp(2);

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"K1", d1, d2});
    rows.add(new Object[] {"K1", d2, d3});
    cache.setRowCache(rows);

    assertDoesNotThrow(() -> cache.validateRowsForSort("dim_customer"));
    assertDoesNotThrow(cache::sortRows);
  }

  @Test
  void testValidateRowsForSortRejectsOverlappingRanges() {
    DimensionCache cache = createKeyedCache();
    Date d1 = timestamp(0);
    Date d2 = timestamp(1);
    Date d3 = timestamp(2);
    Date d4 = timestamp(3);

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"K1", d1, d4});
    rows.add(new Object[] {"K1", d2, d3});
    cache.setRowCache(rows);

    HopTransformException exception =
        assertThrows(HopTransformException.class, () -> cache.validateRowsForSort("dim_customer"));
    assertTrue(exception.getMessage().contains("overlapping validity periods"));
    assertTrue(exception.getMessage().contains("K1"));
  }

  @Test
  void testValidateRowsForSortRejectsInvalidDateRange() {
    DimensionCache cache = createKeyedCache();
    Date d1 = timestamp(0);
    Date d2 = timestamp(1);

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"K1", d2, d1});
    cache.setRowCache(rows);

    HopTransformException exception =
        assertThrows(HopTransformException.class, () -> cache.validateRowsForSort("dim_customer"));
    assertTrue(exception.getMessage().contains("invalid date range"));
    assertTrue(exception.getMessage().contains("K1"));
  }

  @Test
  void testValidateRowsForSortRejectsNullOpenEndedOverlap() {
    DimensionCache cache = createKeyedCache();
    Date d2 = timestamp(1);
    Date d4 = timestamp(3);

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"K1", null, d4});
    rows.add(new Object[] {"K1", d2, null});
    cache.setRowCache(rows);

    HopTransformException exception =
        assertThrows(HopTransformException.class, () -> cache.validateRowsForSort("dim_customer"));
    assertTrue(exception.getMessage().contains("overlapping validity periods"));
  }

  @Test
  void testIsZeroLengthValidityDetectsEqualDatesOnly() {
    Date d1 = timestamp(0);
    Date d2 = timestamp(1);

    assertTrue(DimensionCache.isZeroLengthValidity(d1, d1));
    assertFalse(DimensionCache.isZeroLengthValidity(d1, d2));
    assertFalse(DimensionCache.isZeroLengthValidity(d2, d1));
    assertFalse(DimensionCache.isZeroLengthValidity(null, d1));
    assertFalse(DimensionCache.isZeroLengthValidity(d1, null));
  }

  @Test
  void testExcludeZeroLengthValidityRowsRemovesOnlyEqualDateRows() {
    IRowMeta rowMeta = createKeyedCache().getRowMeta();
    Date d1 = timestamp(0);
    Date d2 = timestamp(1);
    Date d3 = timestamp(2);

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"K1", d1, d2});
    rows.add(new Object[] {"K1", d2, d2});
    rows.add(new Object[] {"K2", d3, d3});

    List<Object[]> filtered = DimensionCache.excludeZeroLengthValidityRows(rowMeta, rows, 1, 2);

    assertEquals(1, filtered.size());
    assertEquals("K1", filtered.get(0)[0]);
    assertEquals(d1, filtered.get(0)[1]);
    assertEquals(d2, filtered.get(0)[2]);
  }

  @Test
  void testFilterZeroLengthRowsAllowsSortAfterRemovingPointRecords() {
    DimensionCache cache = createKeyedCache();
    IRowMeta rowMeta = cache.getRowMeta();
    Date d1 = timestamp(0);
    Date d2 = timestamp(1);
    Date d3 = timestamp(2);
    Date d4 = timestamp(3);

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"K1", d1, d4});
    rows.add(new Object[] {"K1", d2, d2});

    List<Object[]> filtered = DimensionCache.excludeZeroLengthValidityRows(rowMeta, rows, 1, 2);
    cache.setRowCache(filtered);

    assertDoesNotThrow(() -> cache.validateRowsForSort("dim_customer"));
    assertDoesNotThrow(cache::sortRows);
  }

  @Test
  void testValidateRowsForSortAllowsDifferentNaturalKeys() {
    DimensionCache cache = createKeyedCache();
    Date d1 = timestamp(0);
    Date d2 = timestamp(1);
    Date d3 = timestamp(2);
    Date d4 = timestamp(3);

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"K1", d1, d4});
    rows.add(new Object[] {"K2", d2, d3});
    cache.setRowCache(rows);

    assertDoesNotThrow(() -> cache.validateRowsForSort("dim_customer"));
  }

  private static DimensionCache createKeyedCache() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("KEY"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("DATE_FROM"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("DATE_TO"));
    return new DimensionCache(rowMeta, new int[] {0}, 1, 2);
  }

  private static Date timestamp(int hourOffset) {
    long t0 = 1425300000000L;
    return new Timestamp(t0 + 3600000L * hourOffset);
  }

  private static void assertCompareDateInterval(
      DimensionCache dc, Object from1, Object to1, Object from2, Object to2, int expectedValue) {

    final int actualValue = dc.compare(new Object[] {from1, to1}, new Object[] {from2, to2});

    boolean success =
        (expectedValue == 0 && actualValue == 0) //
            || (expectedValue < 0 && actualValue < 0) //
            || (expectedValue > 0 && actualValue > 0);
    Assert.assertTrue(
        success,
        "{0} expected, {1} actual. compare( [({2}), ({3})], [({4}), ({5})] )", //
        expectedValue,
        actualValue,
        from1,
        to1,
        from2,
        to2);
  }
}
