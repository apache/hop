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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.Function;
import org.apache.hop.core.exception.HopException;
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
 * The indexed lookup must return exactly what the binary search over the whole cache returns: the
 * same row index for a hit, a miss for a miss, and the same exception. Random caches of several key
 * types, with gaps, open-ended versions and null keys, are looked up both ways.
 */
class DimensionCacheIndexEquivalenceTest {

  private static final long T0 = 1_425_300_000_000L;
  private static final long MINUTE = 60_000L;

  @Test
  void integerKeys() throws HopException {
    compare(new ValueMetaInteger("k"), r -> r.nextInt(12) == 0 ? null : (long) r.nextInt(40), 1);
  }

  @Test
  void stringKeys() throws HopException {
    String[] values = {"a", "A", " a", "a ", "", null, "b", "bb", "ab", "é"};
    compare(new ValueMetaString("k"), r -> values[r.nextInt(values.length)], 2);
  }

  @Test
  void stringKeysIgnoringWhitespace() throws HopException {
    String[] values = {"a", " a", "a ", "", " ", null, "b"};
    ValueMetaString meta = new ValueMetaString("k");
    meta.setIgnoreWhitespace(true);
    compare(meta, r -> values[r.nextInt(values.length)], 3);
  }

  @Test
  void bigNumberKeys() throws HopException {
    String[] values = {"1", "1.0", "1.00", "2", "0", "0.000", "-1", "10", "1E1"};
    compare(
        new ValueMetaBigNumber("k"),
        r -> r.nextInt(10) == 0 ? null : new BigDecimal(values[r.nextInt(values.length)]),
        4);
  }

  @Test
  void numberKeys() throws HopException {
    double[] values = {0.0, -0.0, 1.0, 1.5, Double.NaN, -3.25};
    compare(new ValueMetaNumber("k"), r -> values[r.nextInt(values.length)], 5);
  }

  @Test
  void compositeKeys() throws HopException {
    for (long seed = 6; seed < 30; seed++) {
      Random random = new Random(seed);
      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaInteger("tk"));
      rowMeta.addValueMeta(new ValueMetaString("k1"));
      rowMeta.addValueMeta(new ValueMetaInteger("k2"));
      rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
      rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
      String[] k1 = {"BE", "NL", "", null};
      Function<Random, Object[]> keys =
          r ->
              new Object[] {
                k1[r.nextInt(k1.length)], r.nextInt(6) == 0 ? null : (long) r.nextInt(5)
              };
      compareOne(rowMeta, new int[] {1, 2}, keys, random, true);
    }
  }

  @Test
  void plainDateColumns() throws HopException {
    for (long seed = 30; seed < 50; seed++) {
      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaInteger("tk"));
      rowMeta.addValueMeta(new ValueMetaInteger("k"));
      rowMeta.addValueMeta(new ValueMetaDate("date_from"));
      rowMeta.addValueMeta(new ValueMetaDate("date_to"));
      compareOne(
          rowMeta, new int[] {1}, r -> new Object[] {(long) r.nextInt(20)}, new Random(seed), true);
    }
  }

  /** A key type without an index still answers, with the binary search. */
  @Test
  void anUnindexableKeyFallsBackToTheBinarySearch() throws HopException {
    ValueMetaString meta = new ValueMetaString("k");
    meta.setCaseInsensitive(true);
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("tk"));
    rowMeta.addValueMeta(meta);
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, "a", new Timestamp(T0), new Timestamp(T0 + MINUTE)});
    DimensionCache cache = new DimensionCache(rowMeta, new int[] {1}, 2, 3);
    cache.setRowCache(rows);
    cache.sortRows();

    assertFalse(cache.buildIndex());
    assertEquals(
        0, cache.lookupRow(new Object[] {null, "A", new Timestamp(T0 + MINUTE / 2), null}));
  }

  /** Changing the cache drops the index. */
  @Test
  void changingTheCacheDropsTheIndex() throws HopException {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("tk"));
    rowMeta.addValueMeta(new ValueMetaInteger("k"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
    DimensionCache cache = new DimensionCache(rowMeta, new int[] {1}, 2, 3);
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, 1L, new Timestamp(T0), new Timestamp(T0 + MINUTE)});
    cache.setRowCache(rows);
    cache.sortRows();
    assertTrue(cache.buildIndex());

    cache.addRow(new Object[] {2L, 2L, new Timestamp(T0), new Timestamp(T0 + MINUTE)});

    assertFalse(cache.isIndexed());
    assertEquals(1, cache.lookupRow(new Object[] {null, 2L, new Timestamp(T0), null}));
  }

  private static void compare(IValueMeta keyMeta, Function<Random, Object> key, long seed)
      throws HopException {
    for (long s = seed * 1000; s < seed * 1000 + 25; s++) {
      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaInteger("tk"));
      rowMeta.addValueMeta(keyMeta.clone());
      rowMeta.addValueMeta(new ValueMetaTimestamp("date_from"));
      rowMeta.addValueMeta(new ValueMetaTimestamp("date_to"));
      compareOne(rowMeta, new int[] {1}, r -> new Object[] {key.apply(r)}, new Random(s), true);
    }
  }

  private static void compareOne(
      RowMeta rowMeta,
      int[] keyIndexes,
      Function<Random, Object[]> keys,
      Random random,
      boolean expectIndexed)
      throws HopException {
    int from = rowMeta.size() - 2;
    int to = rowMeta.size() - 1;
    boolean timestamps = rowMeta.getValueMeta(from) instanceof ValueMetaTimestamp;

    // Versions: for every distinct key a few non-overlapping ranges, sometimes with gaps, a null
    // start on the first and a null end on the last.
    List<Object[]> rows = new ArrayList<>();
    List<Object[]> keySamples = new ArrayList<>();
    long tk = 1;
    for (int k = 0; k < 15; k++) {
      Object[] key = keys.apply(random);
      keySamples.add(key);
      boolean duplicate = false;
      for (Object[] row : rows) {
        Object[] probe = new Object[rowMeta.size()];
        for (int i = 0; i < keyIndexes.length; i++) {
          probe[keyIndexes[i]] = key[i];
        }
        if (rowMeta.compare(row, probe, keyIndexes) == 0) {
          duplicate = true;
          break;
        }
      }
      if (duplicate) {
        continue;
      }
      TreeSet<Long> points = new TreeSet<>();
      int versions = 1 + random.nextInt(4);
      while (points.size() < versions * 2) {
        points.add((long) random.nextInt(40));
      }
      Long[] p = points.toArray(new Long[0]);
      for (int v = 0; v < versions; v++) {
        boolean gap = random.nextBoolean();
        long start = p[v * 2];
        long end = gap ? p[v * 2 + 1] : (v + 1 < versions ? p[v * 2 + 2] : p[v * 2 + 1]);
        Object fromDate = v == 0 && random.nextInt(4) == 0 ? null : date(start, timestamps);
        Object toDate = v == versions - 1 && random.nextInt(4) == 0 ? null : date(end, timestamps);
        Object[] row = new Object[rowMeta.size()];
        row[0] = tk++;
        for (int i = 0; i < keyIndexes.length; i++) {
          row[keyIndexes[i]] = key[i];
        }
        row[from] = fromDate;
        row[to] = toDate;
        rows.add(row);
      }
    }

    DimensionCache plain = new DimensionCache(rowMeta, keyIndexes, from, to);
    plain.setRowCache(new ArrayList<>(rows));
    plain.validateRowsForSort("dim");
    plain.sortRows();

    DimensionCache indexed = new DimensionCache(rowMeta, keyIndexes, from, to);
    indexed.setRowCache(new ArrayList<>(plain.getRowCache()));
    assertEquals(expectIndexed, indexed.buildIndex());

    for (int l = 0; l < 400; l++) {
      Object[] key =
          random.nextInt(3) == 0
              ? keys.apply(random)
              : keySamples.get(random.nextInt(keySamples.size()));
      Object[] lookup = new Object[rowMeta.size()];
      for (int i = 0; i < keyIndexes.length; i++) {
        lookup[keyIndexes[i]] = key[i];
      }
      lookup[from] = random.nextInt(30) == 0 ? null : lookupDate(random, timestamps);

      String expected = outcome(plain, lookup);
      String actual = outcome(indexed, lookup);
      if (!expected.equals(actual)) {
        fail("lookup " + rowMeta.getString(lookup) + ": expected " + expected + ", got " + actual);
      }
    }
  }

  private static Object date(long minutes, boolean timestamp) {
    return timestamp ? new Timestamp(T0 + minutes * MINUTE) : new Date(T0 + minutes * MINUTE);
  }

  /** Mostly on a range boundary, sometimes in between, sometimes a few nanoseconds off. */
  private static Object lookupDate(Random random, boolean timestamp) {
    long minutes = random.nextInt(44) - 2L;
    long millis = T0 + minutes * MINUTE;
    if (random.nextInt(3) == 0) {
      millis += random.nextInt((int) MINUTE);
    }
    if (!timestamp) {
      return new Date(millis);
    }
    Timestamp ts = new Timestamp(millis);
    if (random.nextInt(5) == 0) {
      ts.setNanos(ts.getNanos() + random.nextInt(999));
    }
    return ts;
  }

  private static String outcome(DimensionCache cache, Object[] lookup) {
    try {
      int index = cache.lookupRow(lookup);
      return index >= 0 ? "row " + index : "miss";
    } catch (HopException | RuntimeException e) {
      // a null lookup date can reach Date.compareTo(null) at the insertion point: also pinned
      return "exception " + rootMessage(e);
    }
  }

  private static String rootMessage(Throwable e) {
    Throwable cause = e;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    // A HopException carries the message; the JVM may drop an NPE's message once the code is hot.
    return cause.getClass().getSimpleName()
        + (cause instanceof HopException ? ": " + cause.getMessage() : "");
  }
}
