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

package org.apache.hop.pipeline.transforms.databaselookup;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the behaviour of Database Lookup end to end against H2: load all data in the cache, the
 * on-demand cache with a size limit, and the uncached path, with the different key types, null
 * keys, duplicates, trimming, default values, eating rows and the error stream.
 */
class DatabaseLookupH2Test {

  private DatabaseLookupH2Harness h2;

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    DatabaseLookupH2Harness.registerH2();
  }

  @BeforeEach
  void setUp() throws Exception {
    h2 = new DatabaseLookupH2Harness();
    h2.execute(
        "CREATE TABLE CUSTOMER (ID BIGINT, CODE VARCHAR(20), BIRTH DATE, SEGMENT VARCHAR(20),"
            + " CITY VARCHAR(30), SCORE DOUBLE)",
        "INSERT INTO CUSTOMER VALUES (1, 'ABC', DATE '2020-01-15', 'Gold', '  Paris  ', 1.5)",
        "INSERT INTO CUSTOMER VALUES (2, 'def', DATE '2021-06-30', 'Silver', 'Berlin', 2.5)",
        "INSERT INTO CUSTOMER VALUES (3, 'Ghi', NULL, 'Bronze', ' Rome', 3.5)",
        "INSERT INTO CUSTOMER VALUES (NULL, 'NULLID', NULL, 'NullSeg', 'Nowhere', 0)",
        "INSERT INTO CUSTOMER VALUES (5, 'DUP1', NULL, 'First', 'A', 5)",
        "INSERT INTO CUSTOMER VALUES (5, 'DUP2', NULL, 'Second', 'B', 5)");
  }

  private static DatabaseLookupMeta meta(boolean cached, boolean loadAll, KeyField... keys) {
    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.setCached(cached);
    meta.setLoadingAllDataInCache(loadAll);
    meta.getLookup().setTableName("CUSTOMER");
    meta.getLookup().getKeyFields().addAll(Arrays.asList(keys));
    meta.getLookup()
        .getReturnValues()
        .add(
            new ReturnValue(
                "SEGMENT",
                "segment",
                "UNKNOWN",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));
    meta.getLookup()
        .getReturnValues()
        .add(
            new ReturnValue(
                "CITY",
                "city",
                "",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_BOTH)));
    return meta;
  }

  private static IRowMeta input(IValueMeta... valueMetas) {
    RowMeta rowMeta = new RowMeta();
    for (IValueMeta valueMeta : valueMetas) {
      rowMeta.addValueMeta(valueMeta);
    }
    return rowMeta;
  }

  private static List<Object[]> rows(Object... keys) {
    List<Object[]> rows = new ArrayList<>();
    for (Object key : keys) {
      rows.add(new Object[] {key});
    }
    return rows;
  }

  /** Output rows without the slack the row arrays may carry. */
  private static List<List<Object>> values(List<Object[]> output, int width) {
    List<List<Object>> result = new ArrayList<>();
    for (Object[] row : output) {
      result.add(Arrays.asList(Arrays.copyOf(row, width)));
    }
    return result;
  }

  private static List<Object> row(Object... values) {
    return Arrays.asList(values);
  }

  private static Date date(int year, int month, int day) {
    Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set(year, month - 1, day);
    return calendar.getTime();
  }

  @Test
  void loadAllIntegerKeyHitsMissesDefaultsAndTrim() throws Exception {
    List<Object[]> out =
        h2.run(
            meta(true, true, new KeyField("id", "", "=", "ID")),
            input(new ValueMetaInteger("id")),
            rows(1L, 2L, 99L, 3L, 1L, 1L),
            false);

    assertEquals(
        List.of(
            row(1L, "Gold", "Paris"),
            row(2L, "Silver", "Berlin"),
            row(99L, "UNKNOWN", null),
            row(3L, "Bronze", "Rome"),
            row(1L, "Gold", "Paris"),
            row(1L, "Gold", "Paris")),
        values(out, 3));
    // load all: the table is read once, never queried per row
    verify(h2.mainDatabase(), never()).getLookup(anyBoolean());
  }

  @Test
  void loadAllConvertsStreamKeyTypeToTableKeyType() throws Exception {
    List<Object[]> out =
        h2.run(
            meta(true, true, new KeyField("id", "", "=", "ID")),
            input(new ValueMetaString("id")),
            rows("2", "3", "42"),
            false);
    assertEquals(
        List.of(
            row("2", "Silver", "Berlin"), row("3", "Bronze", "Rome"), row("42", "UNKNOWN", null)),
        values(out, 3));

    h2.output.clear();
    out =
        h2.run(
            meta(true, true, new KeyField("id", "", "=", "ID")),
            input(new ValueMetaNumber("id")),
            rows(1.0, 2.0),
            false);
    assertEquals(List.of(row(1.0, "Gold", "Paris"), row(2.0, "Silver", "Berlin")), values(out, 3));
  }

  @Test
  void loadAllStringKeyIsCaseSensitive() throws Exception {
    List<Object[]> out =
        h2.run(
            meta(true, true, new KeyField("code", "", "=", "CODE")),
            input(new ValueMetaString("code")),
            rows("ABC", "abc", "def", "Ghi"),
            false);
    assertEquals(
        List.of(
            row("ABC", "Gold", "Paris"),
            row("abc", "UNKNOWN", null),
            row("def", "Silver", "Berlin"),
            row("Ghi", "Bronze", "Rome")),
        values(out, 3));
  }

  @Test
  void loadAllDateKey() throws Exception {
    List<Object[]> out =
        h2.run(
            meta(true, true, new KeyField("birth", "", "=", "BIRTH")),
            input(new ValueMetaDate("birth")),
            rows(date(2021, 6, 30), date(2020, 1, 15), date(1999, 1, 1)),
            false);
    assertEquals(
        List.of(
            row(date(2021, 6, 30), "Silver", "Berlin"),
            row(date(2020, 1, 15), "Gold", "Paris"),
            row(date(1999, 1, 1), "UNKNOWN", null)),
        values(out, 3));
  }

  /** In the load-all cache a null key matches the table row whose key is null. */
  @Test
  void loadAllNullStreamKeyMatchesNullTableKey() throws Exception {
    List<Object[]> out =
        h2.run(
            meta(true, true, new KeyField("id", "", "=", "ID")),
            input(new ValueMetaInteger("id")),
            rows(new Object[] {null}),
            false);
    assertEquals(List.of(row(null, "NullSeg", "Nowhere")), values(out, 3));
  }

  /** With duplicate keys in the table, the last row read wins. */
  @Test
  void loadAllDuplicateKeysLastRowWins() throws Exception {
    DatabaseLookupMeta meta = meta(true, true, new KeyField("id", "", "=", "ID"));
    meta.getLookup().setOrderByClause("CODE");
    List<Object[]> out = h2.run(meta, input(new ValueMetaInteger("id")), rows(5L), false);
    assertEquals(List.of(row(5L, "Second", "B")), values(out, 3));

    h2.output.clear();
    meta = meta(true, true, new KeyField("id", "", "=", "ID"));
    meta.getLookup().setOrderByClause("CODE DESC");
    out = h2.run(meta, input(new ValueMetaInteger("id")), rows(5L), false);
    assertEquals(List.of(row(5L, "First", "A")), values(out, 3));
  }

  @Test
  void loadAllEatsRowsOnLookupFailure() throws Exception {
    DatabaseLookupMeta meta = meta(true, true, new KeyField("id", "", "=", "ID"));
    meta.getLookup().setEatingRowOnLookupFailure(true);
    List<Object[]> out =
        h2.run(meta, input(new ValueMetaInteger("id")), rows(99L, 1L, 99L, 2L), false);
    assertEquals(List.of(row(1L, "Gold", "Paris"), row(2L, "Silver", "Berlin")), values(out, 3));
    assertTrue(h2.errors.isEmpty());
  }

  @Test
  void loadAllSendsMissesToTheErrorStream() throws Exception {
    List<Object[]> out =
        h2.run(
            meta(true, true, new KeyField("id", "", "=", "ID")),
            input(new ValueMetaInteger("id")),
            rows(1L, 99L, 98L, 99L),
            true);
    assertEquals(List.of(row(1L, "Gold", "Paris")), values(out, 3));
    assertEquals(3, h2.errors.size());
    assertEquals("No lookup found in CUSTOMER for id=99", h2.errors.get(0).description());
    assertEquals("No lookup found in CUSTOMER for id=98", h2.errors.get(1).description());
    assertEquals("id", h2.errors.get(0).fields());
    assertEquals("DBL001", h2.errors.get(0).code());
    assertArrayEquals(new Object[] {99L}, h2.errors.get(2).row());
  }

  /** Not all conditions are "=": the load-all data goes to the read-only (index) cache. */
  @Test
  void loadAllWithBetweenUsesReadOnlyCache() throws Exception {
    DatabaseLookupMeta meta = meta(true, true, new KeyField("lo", "hi", "BETWEEN", "ID"));
    List<Object[]> out =
        h2.run(
            meta,
            input(new ValueMetaInteger("lo"), new ValueMetaInteger("hi")),
            List.of(new Object[] {2L, 2L}, new Object[] {3L, 3L}, new Object[] {10L, 20L}),
            false);
    assertEquals(
        List.of(
            row(2L, 2L, "Silver", "Berlin"),
            row(3L, 3L, "Bronze", "Rome"),
            row(10L, 20L, "UNKNOWN", null)),
        values(out, 4));
  }

  @Test
  void onDemandCacheQueriesEachKeyOnce() throws Exception {
    List<Object[]> out =
        h2.run(
            meta(true, false, new KeyField("id", "", "=", "ID")),
            input(new ValueMetaInteger("id")),
            rows(1L, 2L, 1L, 2L, 1L, 99L, 99L),
            false);
    assertEquals(
        List.of(
            row(1L, "Gold", "Paris"),
            row(2L, "Silver", "Berlin"),
            row(1L, "Gold", "Paris"),
            row(2L, "Silver", "Berlin"),
            row(1L, "Gold", "Paris"),
            row(99L, "UNKNOWN", null),
            row(99L, "UNKNOWN", null)),
        values(out, 3));
    // 1, 2 and 99: a miss that falls back to the default values is cached too
    verify(h2.mainDatabase(), times(3)).getLookup(anyBoolean());
  }

  @Test
  void onDemandCacheWithSizeLimitStillReturnsCorrectValues() throws Exception {
    DatabaseLookupMeta meta = meta(true, false, new KeyField("id", "", "=", "ID"));
    meta.setCacheSize(2);
    List<Object> keys = new ArrayList<>();
    for (int round = 0; round < 50; round++) {
      keys.addAll(List.of(1L, 2L, 3L, 99L));
    }
    List<Object[]> out =
        h2.run(meta, input(new ValueMetaInteger("id")), rows(keys.toArray()), false);
    assertEquals(200, out.size());
    for (int i = 0; i < out.size(); i += 4) {
      assertEquals(
          List.of(
              row(1L, "Gold", "Paris"),
              row(2L, "Silver", "Berlin"),
              row(3L, "Bronze", "Rome"),
              row(99L, "UNKNOWN", null)),
          values(out.subList(i, i + 4), 3));
    }
  }

  @Test
  void onDemandCacheEatsRowsOnLookupFailure() throws Exception {
    DatabaseLookupMeta meta = meta(true, false, new KeyField("id", "", "=", "ID"));
    meta.getLookup().setEatingRowOnLookupFailure(true);
    List<Object[]> out =
        h2.run(meta, input(new ValueMetaInteger("id")), rows(99L, 1L, 99L, 1L), false);
    assertEquals(List.of(row(1L, "Gold", "Paris"), row(1L, "Gold", "Paris")), values(out, 3));
  }

  @Test
  void onDemandCacheSendsMissesToTheErrorStream() throws Exception {
    List<Object[]> out =
        h2.run(
            meta(true, false, new KeyField("id", "", "=", "ID")),
            input(new ValueMetaInteger("id")),
            rows(99L, 1L, 99L),
            true);
    assertEquals(List.of(row(1L, "Gold", "Paris")), values(out, 3));
    assertEquals(2, h2.errors.size());
    assertEquals("No lookup found in CUSTOMER for id=99", h2.errors.get(1).description());
  }

  /**
   * The uncached lookup converts what the table returns to the configured return type, using the
   * layout the database reports for the lookup ({@code getReturnRowMeta()}).
   */
  @Test
  void uncachedLookupConvertsEachReturnedTypeToTheConfiguredType() throws Exception {
    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.getLookup().setTableName("CUSTOMER");
    meta.getLookup().getKeyFields().add(new KeyField("id", "", "=", "ID"));
    String none = ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE);
    meta.getLookup().getReturnValues().add(new ReturnValue("CODE", "code", "", "String", none));
    meta.getLookup().getReturnValues().add(new ReturnValue("BIRTH", "birth", "", "Date", none));
    meta.getLookup().getReturnValues().add(new ReturnValue("SCORE", "score", "", "Number", none));
    meta.getLookup().getReturnValues().add(new ReturnValue("SCORE", "text", "", "String", none));

    List<Object[]> out =
        h2.run(meta, input(new ValueMetaInteger("id")), rows(1L, 2L, 1L, 3L), false);

    assertEquals(
        List.of(
            row(1L, "ABC", date(2020, 1, 15), 1.5, "1.5"),
            row(2L, "def", date(2021, 6, 30), 2.5, "2.5"),
            row(1L, "ABC", date(2020, 1, 15), 1.5, "1.5"),
            row(3L, "Ghi", null, 3.5, "3.5")),
        values(out, 5));
    verify(h2.mainDatabase(), times(4)).getLookup(anyBoolean());
  }

  @Test
  void uncachedLookupFailingOnMultipleResultsSendsTheRowToTheErrorStream() throws Exception {
    DatabaseLookupMeta meta = meta(false, false, new KeyField("id", "", "=", "ID"));
    meta.getLookup().setFailingOnMultipleResults(true);

    List<Object[]> out = h2.run(meta, input(new ValueMetaInteger("id")), rows(1L, 5L, 2L), true);

    assertEquals(List.of(row(1L, "Gold", "Paris"), row(2L, "Silver", "Berlin")), values(out, 3));
    assertEquals(1, h2.errors.size());
    assertEquals(5L, h2.errors.get(0).row()[0]);
    assertTrue(
        h2.errors.get(0).description().contains("Only 1 row was expected"),
        h2.errors.get(0).description());
  }

  @Test
  void uncachedLookupWithOrderByReturnsTheFirstOrderedRow() throws Exception {
    DatabaseLookupMeta meta = meta(false, false, new KeyField("id", "", "=", "ID"));
    meta.getLookup().setOrderByClause("CODE DESC");
    List<Object[]> out = h2.run(meta, input(new ValueMetaInteger("id")), rows(5L), false);
    assertEquals(List.of(row(5L, "Second", "B")), values(out, 3));

    h2.output.clear();
    meta.getLookup().setOrderByClause("CODE ASC");
    out = h2.run(meta, input(new ValueMetaInteger("id")), rows(5L), false);
    assertEquals(List.of(row(5L, "First", "A")), values(out, 3));
  }

  @Test
  void uncachedLookupConvertsKeyTypeAndAppliesDefaults() throws Exception {
    List<Object[]> out =
        h2.run(
            meta(false, false, new KeyField("id", "", "=", "ID")),
            input(new ValueMetaString("id")),
            rows("1", "1", "77"),
            false);
    assertEquals(
        List.of(row("1", "Gold", "Paris"), row("1", "Gold", "Paris"), row("77", "UNKNOWN", null)),
        values(out, 3));
    verify(h2.mainDatabase(), times(3)).getLookup(anyBoolean());
  }

  /** A key the table does not have is asked once: later rows with it are eaten from the cache. */
  @Test
  void onDemandCacheRemembersMissesOfEatenRows() throws Exception {
    DatabaseLookupMeta meta = meta(true, false, new KeyField("id", "", "=", "ID"));
    meta.getLookup().setEatingRowOnLookupFailure(true);
    List<Object[]> out =
        h2.run(meta, input(new ValueMetaInteger("id")), rows(99L, 1L, 99L, 99L, 1L), false);
    assertEquals(List.of(row(1L, "Gold", "Paris"), row(1L, "Gold", "Paris")), values(out, 3));
    verify(h2.mainDatabase(), times(2)).getLookup(anyBoolean());
  }

  /** The same for rows sent to the error stream: every one is still sent, with its own key. */
  @Test
  void onDemandCacheRemembersMissesOfErrorRows() throws Exception {
    List<Object[]> out =
        h2.run(
            meta(true, false, new KeyField("id", "", "=", "ID")),
            input(new ValueMetaInteger("id")),
            rows(99L, 98L, 99L, 98L),
            true);
    assertTrue(out.isEmpty());
    assertEquals(4, h2.errors.size());
    assertEquals("No lookup found in CUSTOMER for id=98", h2.errors.get(3).description());
    verify(h2.mainDatabase(), times(2)).getLookup(anyBoolean());
  }

  /** A default value that has to be trimmed is returned the same on a miss and on a cache hit. */
  @Test
  void onDemandCacheTrimsCachedDefaultValuesLikeBefore() throws Exception {
    DatabaseLookupMeta meta = meta(true, false, new KeyField("id", "", "=", "ID"));
    meta.getLookup().getReturnValues().get(1).setDefaultValue("  none  ");
    List<Object[]> out = h2.run(meta, input(new ValueMetaInteger("id")), rows(99L, 99L), false);
    // first: the default as configured; afterwards: the trimmed cached copy
    assertEquals(
        List.of(row(99L, "UNKNOWN", "  none  "), row(99L, "UNKNOWN", "none")), values(out, 3));
  }
}
