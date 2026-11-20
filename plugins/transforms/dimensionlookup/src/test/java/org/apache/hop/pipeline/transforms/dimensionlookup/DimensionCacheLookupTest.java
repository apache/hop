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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.Test;

/**
 * Pins what a lookup in the pre-load cache returns, including the odd corners: these results are
 * the contract any faster lookup has to keep. Each cache row is laid out the way the pre-load
 * SELECT reads it: technical key, natural key column(s), date_from, date_to.
 */
class DimensionCacheLookupTest {

  private static final long T0 = 1_425_300_000_000L;
  private static final long HOUR = 3_600_000L;

  private static Timestamp d(int hours) {
    return new Timestamp(T0 + HOUR * hours);
  }

  /** tk, key, from, to: Integer key, timestamp range. */
  private static RowMeta integerKeyMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("tk"));
    rowMeta.addValueMeta(new ValueMetaInteger("customer_id"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
    return rowMeta;
  }

  /** Builds, validates and sorts a cache the way DimensionLookup.preloadCache does. */
  static DimensionCache preload(IRowMeta rowMeta, int[] keyIndexes, List<Object[]> rows)
      throws HopException {
    int from = rowMeta.size() - 2;
    int to = rowMeta.size() - 1;
    DimensionCache cache = new DimensionCache(rowMeta, keyIndexes, from, to);
    cache.setRowCache(new ArrayList<>(rows));
    cache.validateRowsForSort("dim");
    cache.sortRows();
    assertTrue(cache.buildIndex(), "the cache should be indexed");
    return cache;
  }

  /** The technical key found, or null when lookupRow reports a miss. */
  static Object lookupTk(DimensionCache cache, Date date, Object... key) throws HopException {
    Object[] lookup = new Object[cache.getRowMeta().size()];
    for (int i = 0; i < key.length; i++) {
      lookup[cache.getKeyIndexes()[i]] = key[i];
    }
    lookup[cache.getFromDateIndex()] = date;
    int index = cache.lookupRow(lookup);
    return index >= 0 ? cache.getRow(index)[0] : null;
  }

  /**
   * Key 1: three adjacent versions, the first open at the start. Key 2: one version open at the
   * end. Key 3: one closed version, the last key in the cache.
   */
  private static DimensionCache versionedCache() throws HopException {
    List<Object[]> rows = new ArrayList<>();
    // deliberately unsorted
    rows.add(new Object[] {5L, 3L, d(1), d(3)});
    rows.add(new Object[] {3L, 1L, d(4), d(6)});
    rows.add(new Object[] {1L, 1L, null, d(2)});
    rows.add(new Object[] {4L, 2L, d(1), null});
    rows.add(new Object[] {2L, 1L, d(2), d(4)});
    return preload(integerKeyMeta(), new int[] {1}, rows);
  }

  @Test
  void dateFromIsInclusiveAndDateToExclusive() throws HopException {
    DimensionCache cache = versionedCache();

    assertEquals(1L, lookupTk(cache, d(-100), 1L)); // null date_from: -infinity
    assertEquals(1L, lookupTk(cache, d(1), 1L));
    assertEquals(2L, lookupTk(cache, d(2), 1L)); // date_from inclusive
    assertEquals(2L, lookupTk(cache, d(3), 1L));
    assertEquals(3L, lookupTk(cache, d(4), 1L)); // date_to exclusive
    assertEquals(3L, lookupTk(cache, new Timestamp(T0 + HOUR * 6 - 1), 1L));
    assertNull(lookupTk(cache, d(6), 1L)); // past the last version
    assertNull(lookupTk(cache, d(100), 1L));
  }

  @Test
  void aClosedVersionMatchesOnlyInsideItsRange() throws HopException {
    DimensionCache cache = versionedCache();

    assertNull(lookupTk(cache, d(0), 3L));
    assertEquals(5L, lookupTk(cache, d(1), 3L));
    assertEquals(5L, lookupTk(cache, d(2), 3L));
    assertNull(lookupTk(cache, d(3), 3L));
  }

  /**
   * Current behaviour, pinned: a version whose date_to is null only matches a lookup on exactly its
   * date_from. Later dates compare below the row, so the search moves past it.
   */
  @Test
  void anOpenEndedVersionOnlyMatchesItsExactStart() throws HopException {
    DimensionCache cache = versionedCache();

    assertEquals(4L, lookupTk(cache, d(1), 2L));
    assertNull(lookupTk(cache, d(2), 2L));
    assertNull(lookupTk(cache, d(100), 2L));
  }

  /**
   * Current behaviour, pinned: a lookup before an open-ended first version lands on it as the
   * insertion point and is reported as a sorting problem.
   */
  @Test
  void aLookupBeforeAnOpenEndedFirstVersionThrows() throws HopException {
    DimensionCache cache = versionedCache();

    assertThrows(HopException.class, () -> lookupTk(cache, d(0), 2L));
  }

  @Test
  void anUnknownKeyIsAMiss() throws HopException {
    DimensionCache cache = versionedCache();

    assertNull(lookupTk(cache, d(1), 0L));
    assertNull(lookupTk(cache, d(1), 99L));
    assertNull(lookupTk(cache, d(1), (Object) null));
  }

  /** A null lookup date matches only a version that starts at -infinity. */
  @Test
  void aNullLookupDate() throws HopException {
    DimensionCache cache = versionedCache();

    assertEquals(1L, lookupTk(cache, null, 1L));
    assertNull(lookupTk(cache, null, 3L));
  }

  @Test
  void aNullKeyMatchesANullKeyRow() throws HopException {
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {7L, null, d(0), d(10)});
    rows.add(new Object[] {8L, 1L, d(0), d(10)});
    DimensionCache cache = preload(integerKeyMeta(), new int[] {1}, rows);

    assertEquals(7L, lookupTk(cache, d(1), (Object) null));
    assertEquals(8L, lookupTk(cache, d(1), 1L));
    assertNull(lookupTk(cache, d(1), 2L));
  }

  /** The year range the dialog defaults to: 1900-01-01 to 2199-12-31 23:59:59. */
  @Test
  void theMinimumAndMaximumDates() throws HopException {
    Timestamp min = Timestamp.valueOf("1900-01-01 00:00:00");
    Timestamp max = Timestamp.valueOf("2199-12-31 23:59:59");
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, 1L, min, max});
    rows.add(new Object[] {2L, 2L, min, max});
    DimensionCache cache = preload(integerKeyMeta(), new int[] {1}, rows);

    assertEquals(1L, lookupTk(cache, min, 1L));
    assertEquals(1L, lookupTk(cache, Timestamp.valueOf("2026-10-22 12:00:00"), 1L));
    assertEquals(1L, lookupTk(cache, new Timestamp(max.getTime() - 1), 1L));
    assertNull(lookupTk(cache, max, 1L));
    assertNull(lookupTk(cache, new Timestamp(min.getTime() - 1), 1L));
    assertEquals(2L, lookupTk(cache, min, 2L));
  }

  /** A timestamp column compares nanoseconds, a date column only milliseconds. */
  @Test
  void nanosecondsCountOnlyForATimestampColumn() throws HopException {
    Timestamp from = new Timestamp(T0);
    from.setNanos(from.getNanos() + 500);
    Timestamp to = d(1);
    Timestamp lookup = new Timestamp(T0); // 500 ns before date_from

    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, 1L, from, to});
    rows.add(new Object[] {2L, 2L, from, to});
    DimensionCache timestampCache = preload(integerKeyMeta(), new int[] {1}, rows);
    assertNull(lookupTk(timestampCache, lookup, 1L));

    RowMeta dateMeta = new RowMeta();
    dateMeta.addValueMeta(new ValueMetaInteger("tk"));
    dateMeta.addValueMeta(new ValueMetaInteger("customer_id"));
    dateMeta.addValueMeta(new ValueMetaDate("date_from"));
    dateMeta.addValueMeta(new ValueMetaDate("date_to"));
    DimensionCache dateCache = preload(dateMeta, new int[] {1}, rows);
    assertEquals(1L, lookupTk(dateCache, lookup, 1L));
  }

  @Test
  void stringKeysAreCaseAndWhitespaceSensitiveAndEmptyIsNull() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("tk"));
    rowMeta.addValueMeta(new ValueMetaString("code"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, "A", d(0), d(10)});
    rows.add(new Object[] {2L, "a", d(0), d(10)});
    rows.add(new Object[] {3L, " A", d(0), d(10)});
    rows.add(new Object[] {4L, "", d(0), d(10)});
    rows.add(new Object[] {5L, "B", d(0), d(5)});
    rows.add(new Object[] {6L, "B", d(5), d(10)});
    DimensionCache cache = preload(rowMeta, new int[] {1}, rows);

    assertEquals(1L, lookupTk(cache, d(1), "A"));
    assertEquals(2L, lookupTk(cache, d(1), "a"));
    assertEquals(3L, lookupTk(cache, d(1), " A"));
    assertNull(lookupTk(cache, d(1), "A "));
    assertEquals(4L, lookupTk(cache, d(1), ""));
    assertEquals(4L, lookupTk(cache, d(1), (Object) null)); // empty and null are the same
    assertEquals(5L, lookupTk(cache, d(4), "B"));
    assertEquals(6L, lookupTk(cache, d(5), "B"));
    assertNull(lookupTk(cache, d(1), "C"));
  }

  @Test
  void aCompositeKey() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("tk"));
    rowMeta.addValueMeta(new ValueMetaString("country"));
    rowMeta.addValueMeta(new ValueMetaInteger("customer_id"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, "BE", 1L, d(0), d(5)});
    rows.add(new Object[] {2L, "BE", 1L, d(5), d(10)});
    rows.add(new Object[] {3L, "BE", 2L, d(0), d(10)});
    rows.add(new Object[] {4L, "NL", 1L, d(0), d(10)});
    rows.add(new Object[] {5L, "NL", null, d(0), d(10)});
    DimensionCache cache = preload(rowMeta, new int[] {1, 2}, rows);

    assertEquals(1L, lookupTk(cache, d(1), "BE", 1L));
    assertEquals(2L, lookupTk(cache, d(6), "BE", 1L));
    assertEquals(3L, lookupTk(cache, d(6), "BE", 2L));
    assertEquals(4L, lookupTk(cache, d(6), "NL", 1L));
    assertEquals(5L, lookupTk(cache, d(6), "NL", null));
    assertNull(lookupTk(cache, d(6), "NL", 2L));
    assertNull(lookupTk(cache, d(6), "FR", 1L));
    assertNull(lookupTk(cache, d(6), "BE", null));
  }

  /** BigNumber keys compare by value: 42 and 42.00 are the same key. */
  @Test
  void bigNumberKeysCompareByValue() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("tk"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("id"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, new BigDecimal("42"), d(0), d(10)});
    rows.add(new Object[] {2L, new BigDecimal("0"), d(0), d(10)});
    DimensionCache cache = preload(rowMeta, new int[] {1}, rows);

    assertEquals(1L, lookupTk(cache, d(1), new BigDecimal("42.00")));
    assertEquals(1L, lookupTk(cache, d(1), new BigDecimal("4.2E1")));
    assertEquals(2L, lookupTk(cache, d(1), new BigDecimal("0.000")));
    assertNull(lookupTk(cache, d(1), new BigDecimal("42.01")));
  }

  @Test
  void numberKeys() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("tk"));
    rowMeta.addValueMeta(new ValueMetaNumber("id"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, 42.0, d(0), d(10)});
    rows.add(new Object[] {2L, 0.5, d(0), d(10)});
    DimensionCache cache = preload(rowMeta, new int[] {1}, rows);

    assertEquals(1L, lookupTk(cache, d(1), 42.0));
    assertEquals(2L, lookupTk(cache, d(1), 0.5));
    assertNull(lookupTk(cache, d(1), 42.000001));
  }

  // ---- The stream's key type differs from the table's key column --------------------------------

  /**
   * The stream key converted the way DimensionLookup does it, then looked up: the cache works in
   * the table's types whatever the stream carries.
   */
  private static Object streamLookupTk(
      DimensionCache cache, IValueMeta streamKeyMeta, Object streamKey, Date date)
      throws HopException {
    RowMeta streamMeta = new RowMeta();
    streamMeta.addValueMeta(streamKeyMeta);
    streamMeta.addValueMeta(new ValueMetaDate("order_date"));
    Object[] lookup =
        DimensionLookup.assemblePreloadLookupRow(
            streamMeta,
            new Object[] {streamKey, date},
            List.of(0),
            cache,
            new ValueMetaDate("lookupDate"),
            date);
    int index = cache.lookupRow(lookup);
    return index >= 0 ? cache.getRow(index)[0] : null;
  }

  @Test
  void aStringStreamKeyAgainstAnIntegerColumn() throws HopException {
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, 42L, d(0), d(10)});
    rows.add(new Object[] {2L, 7L, d(0), d(10)});
    DimensionCache cache = preload(integerKeyMeta(), new int[] {1}, rows);
    ValueMetaString stream = new ValueMetaString("customer_id");

    assertEquals(1L, streamLookupTk(cache, stream, "42", d(1)));
    assertEquals(1L, streamLookupTk(cache, stream, "042", d(1)));
    assertEquals(2L, streamLookupTk(cache, stream, "7", d(1)));
    assertNull(streamLookupTk(cache, stream, "43", d(1)));
    assertNull(streamLookupTk(cache, stream, null, d(1)));
  }

  @Test
  void anIntegerStreamKeyAgainstAStringColumn() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("tk"));
    rowMeta.addValueMeta(new ValueMetaString("customer_id"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, "42", d(0), d(10)});
    rows.add(new Object[] {2L, "042", d(0), d(10)});
    DimensionCache cache = preload(rowMeta, new int[] {1}, rows);
    ValueMetaInteger stream = new ValueMetaInteger("customer_id");

    assertEquals(1L, streamLookupTk(cache, stream, 42L, d(1)));
    assertNull(streamLookupTk(cache, stream, 43L, d(1)));
  }

  @Test
  void aNumberStreamKeyAgainstAnIntegerColumn() throws HopException {
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, 42L, d(0), d(10)});
    rows.add(new Object[] {2L, 43L, d(0), d(10)});
    DimensionCache cache = preload(integerKeyMeta(), new int[] {1}, rows);
    ValueMetaNumber stream = new ValueMetaNumber("customer_id");

    assertEquals(1L, streamLookupTk(cache, stream, 42.0, d(1)));
    assertEquals(1L, streamLookupTk(cache, stream, 42.4, d(1))); // rounded
    assertEquals(2L, streamLookupTk(cache, stream, 42.5, d(1)));
  }

  @Test
  void anIntegerStreamKeyAgainstABigNumberColumn() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("tk"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("customer_id", 38, 0));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, new BigDecimal("42"), d(0), d(10)});
    rows.add(new Object[] {2L, new BigDecimal("43"), d(0), d(10)});
    DimensionCache cache = preload(rowMeta, new int[] {1}, rows);

    assertEquals(1L, streamLookupTk(cache, new ValueMetaInteger("customer_id"), 42L, d(1)));
    assertNull(streamLookupTk(cache, new ValueMetaInteger("customer_id"), 44L, d(1)));
  }

  /** The order date is a Date in the stream and a Timestamp in the table. */
  @Test
  void aDateStreamLookupAgainstATimestampRange() throws HopException {
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, 42L, d(0), d(5)});
    rows.add(new Object[] {2L, 42L, d(5), d(10)});
    rows.add(new Object[] {3L, 43L, d(0), d(10)});
    DimensionCache cache = preload(integerKeyMeta(), new int[] {1}, rows);
    ValueMetaInteger stream = new ValueMetaInteger("customer_id");

    assertEquals(1L, streamLookupTk(cache, stream, 42L, new Date(d(4).getTime())));
    assertEquals(2L, streamLookupTk(cache, stream, 42L, new Date(d(5).getTime())));
    assertNull(streamLookupTk(cache, stream, 42L, new Date(d(10).getTime())));
  }
}
