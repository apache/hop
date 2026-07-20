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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultCacheTest {

  private DatabaseLookupData data;
  private IRowMeta lookupMeta;
  private DatabaseLookupMeta meta;

  @BeforeEach
  void setUp() {
    data = new DatabaseLookupData();
    data.lookupMeta = new RowMeta();
    data.lookupMeta.addValueMeta(new ValueMetaInteger("id"));
    lookupMeta = data.lookupMeta.clone();

    meta = new DatabaseLookupMeta();
    meta.setCached(true);
    meta.setCacheSize(0);
    meta.setLoadingAllDataInCache(false);
  }

  @Test
  void newCache_UsesDefaultCapacityWhenSizeNonPositive() {
    DefaultCache cache = DefaultCache.newCache(data, 0);
    assertNotNull(cache);
    DefaultCache negative = DefaultCache.newCache(data, -5);
    assertNotNull(negative);
  }

  @Test
  void newCache_AcceptsPositiveCacheSize() {
    assertNotNull(DefaultCache.newCache(data, 10));
  }

  @Test
  void getRowFromCache_AllEquals_ReturnsExactMapHit() throws Exception {
    data.allEquals = true;
    data.conditions = new int[] {DatabaseLookupMeta.CONDITION_EQ};
    DefaultCache cache = DefaultCache.newCache(data, 16);

    Object[] key = new Object[] {1L};
    Object[] value = new Object[] {"one"};
    cache.storeRowInCache(meta, lookupMeta, key, value);

    assertArrayEquals(value, cache.getRowFromCache(lookupMeta, new Object[] {1L}));
    assertNull(cache.getRowFromCache(lookupMeta, new Object[] {2L}));
  }

  @Test
  void getRowFromCache_ConditionEq_ScansWhenNotAllEquals() throws Exception {
    assertConditionMatch(DatabaseLookupMeta.CONDITION_EQ, 5L, 5L, true);
    assertConditionMatch(DatabaseLookupMeta.CONDITION_EQ, 5L, 6L, false);
  }

  @Test
  void getRowFromCache_ConditionNe() throws Exception {
    assertConditionMatch(DatabaseLookupMeta.CONDITION_NE, 5L, 6L, true);
    assertConditionMatch(DatabaseLookupMeta.CONDITION_NE, 5L, 5L, false);
  }

  @Test
  void getRowFromCache_ConditionLt() throws Exception {
    // cached key < lookup → match when lookup > key
    assertConditionMatch(DatabaseLookupMeta.CONDITION_LT, 5L, 6L, true);
    assertConditionMatch(DatabaseLookupMeta.CONDITION_LT, 5L, 4L, false);
  }

  @Test
  void getRowFromCache_ConditionLe() throws Exception {
    assertConditionMatch(DatabaseLookupMeta.CONDITION_LE, 5L, 5L, true);
    assertConditionMatch(DatabaseLookupMeta.CONDITION_LE, 5L, 6L, true);
    assertConditionMatch(DatabaseLookupMeta.CONDITION_LE, 5L, 4L, false);
  }

  @Test
  void getRowFromCache_ConditionGt() throws Exception {
    assertConditionMatch(DatabaseLookupMeta.CONDITION_GT, 5L, 4L, true);
    assertConditionMatch(DatabaseLookupMeta.CONDITION_GT, 5L, 5L, false);
  }

  @Test
  void getRowFromCache_ConditionGe() throws Exception {
    assertConditionMatch(DatabaseLookupMeta.CONDITION_GE, 5L, 5L, true);
    assertConditionMatch(DatabaseLookupMeta.CONDITION_GE, 5L, 4L, true);
    assertConditionMatch(DatabaseLookupMeta.CONDITION_GE, 5L, 6L, false);
  }

  @Test
  void getRowFromCache_ConditionIsNull() throws Exception {
    data.allEquals = false;
    data.hasDBCondition = false;
    data.conditions = new int[] {DatabaseLookupMeta.CONDITION_IS_NULL};
    DefaultCache cache = DefaultCache.newCache(data, 16);

    Object[] nullKey = new Object[] {null};
    Object[] value = new Object[] {"null-row"};
    cache.storeRowInCache(meta, lookupMeta, nullKey, value);
    cache.storeRowInCache(meta, lookupMeta, new Object[] {1L}, new Object[] {"one"});

    assertArrayEquals(value, cache.getRowFromCache(lookupMeta, new Object[] {999L}));
  }

  @Test
  void getRowFromCache_ConditionIsNotNull() throws Exception {
    data.allEquals = false;
    data.hasDBCondition = false;
    data.conditions = new int[] {DatabaseLookupMeta.CONDITION_IS_NOT_NULL};
    DefaultCache cache = DefaultCache.newCache(data, 16);

    cache.storeRowInCache(meta, lookupMeta, new Object[] {null}, new Object[] {"null-row"});
    Object[] value = new Object[] {"one"};
    cache.storeRowInCache(meta, lookupMeta, new Object[] {1L}, value);

    assertArrayEquals(value, cache.getRowFromCache(lookupMeta, new Object[] {999L}));
  }

  @Test
  void getRowFromCache_ConditionBetween() throws Exception {
    data.allEquals = false;
    data.hasDBCondition = false;
    data.conditions = new int[] {DatabaseLookupMeta.CONDITION_BETWEEN};
    data.lookupMeta = new RowMeta();
    data.lookupMeta.addValueMeta(new ValueMetaInteger("from"));
    data.lookupMeta.addValueMeta(new ValueMetaInteger("to"));
    lookupMeta = data.lookupMeta.clone();

    // BETWEEN stores a single key column in the map key
    IRowMeta storeMeta = new RowMeta();
    storeMeta.addValueMeta(new ValueMetaInteger("id"));

    DefaultCache cache = DefaultCache.newCache(data, 16);
    Object[] value = new Object[] {"mid"};
    cache.storeRowInCache(meta, storeMeta, new Object[] {5L}, value);

    IRowMeta lookupWithRange = new RowMeta();
    lookupWithRange.addValueMeta(new ValueMetaInteger("from"));
    lookupWithRange.addValueMeta(new ValueMetaInteger("to"));

    assertArrayEquals(value, cache.getRowFromCache(lookupWithRange, new Object[] {1L, 10L}));
    assertNull(cache.getRowFromCache(lookupWithRange, new Object[] {6L, 10L}));
  }

  @Test
  void getRowFromCache_UnknownCondition_SetsHasDbConditionAndStopsMatching() throws Exception {
    data.allEquals = false;
    data.hasDBCondition = false;
    data.conditions = new int[] {DatabaseLookupMeta.CONDITION_LIKE};
    DefaultCache cache = DefaultCache.newCache(data, 16);
    cache.storeRowInCache(meta, lookupMeta, new Object[] {1L}, new Object[] {"one"});

    assertNull(cache.getRowFromCache(lookupMeta, new Object[] {1L}));
    assertTrue(data.hasDBCondition);
  }

  @Test
  void getRowFromCache_HasDbCondition_SkipsScanAndReturnsNull() throws Exception {
    data.allEquals = false;
    data.hasDBCondition = true;
    data.conditions = new int[] {DatabaseLookupMeta.CONDITION_LT};
    DefaultCache cache = DefaultCache.newCache(data, 16);
    cache.storeRowInCache(meta, lookupMeta, new Object[] {1L}, new Object[] {"one"});

    assertNull(cache.getRowFromCache(lookupMeta, new Object[] {10L}));
  }

  @Test
  void storeRowInCache_DoesNotEvictWhenLoadingAllDataInCache() throws Exception {
    meta.setLoadingAllDataInCache(true);
    meta.setCacheSize(2);
    data.allEquals = true;
    data.conditions = new int[] {DatabaseLookupMeta.CONDITION_EQ};
    DefaultCache cache = DefaultCache.newCache(data, 16);

    for (long i = 0; i < 5; i++) {
      cache.storeRowInCache(meta, lookupMeta, new Object[] {i}, new Object[] {"v" + i});
    }

    // All entries remain when load-all is enabled
    for (long i = 0; i < 5; i++) {
      assertNotNull(cache.getRowFromCache(lookupMeta, new Object[] {i}));
    }
  }

  @Test
  void storeRowInCache_EvictsWhenOverCacheSize() throws Exception {
    meta.setLoadingAllDataInCache(false);
    meta.setCacheSize(5);
    data.allEquals = true;
    data.conditions = new int[] {DatabaseLookupMeta.CONDITION_EQ};
    DefaultCache cache = DefaultCache.newCache(data, 16);

    for (long i = 0; i < 20; i++) {
      cache.storeRowInCache(meta, lookupMeta, new Object[] {i}, new Object[] {"v" + i});
      // small delay so TimedRow timestamps differ enough for eviction sampling
      Thread.sleep(2);
    }

    int hits = 0;
    for (long i = 0; i < 20; i++) {
      if (cache.getRowFromCache(lookupMeta, new Object[] {i}) != null) {
        hits++;
      }
    }
    // Eviction is approximate (samples ~10%); still fewer than all 20 should remain
    assertTrue(hits < 20, "Expected eviction to remove some entries, hits=" + hits);
    assertTrue(hits > 0, "Expected some entries to remain");
  }

  private void assertConditionMatch(int condition, long cachedKey, long lookupValue, boolean expect)
      throws Exception {
    data.allEquals = false;
    data.hasDBCondition = false;
    data.conditions = new int[] {condition};
    DefaultCache cache = DefaultCache.newCache(data, 16);

    Object[] value = new Object[] {"hit"};
    cache.storeRowInCache(meta, lookupMeta, new Object[] {cachedKey}, value);

    Object[] found = cache.getRowFromCache(lookupMeta, new Object[] {lookupValue});
    if (expect) {
      assertArrayEquals(value, found);
    } else {
      assertNull(found);
    }
    assertFalse(data.hasDBCondition);
  }
}
