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

package org.apache.hop.pipeline.transforms.databasejoin.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit test for {@link DatabaseCache} */
class DatabaseCacheTest {

  private IRowMeta lookupMeta;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    lookupMeta = new RowMeta();
    lookupMeta.addValueMeta(new ValueMetaInteger("id"));
  }

  @Test
  void newCacheIsEmpty() {
    DatabaseCache cache = new DatabaseCache(10);
    assertTrue(cache.isEmpty());
    assertNull(cache.getRowsFromCache(lookupMeta, new Object[] {1L}));
  }

  @Test
  void putAndGetByLookupRow() {
    DatabaseCache cache = new DatabaseCache(10);
    List<Object[]> rows = rows("one");
    cache.putRowsIntoCache(lookupMeta, new Object[] {1L}, rows);

    assertFalse(cache.isEmpty());
    assertSame(rows, cache.getRowsFromCache(lookupMeta, new Object[] {1L}));
    assertNull(cache.getRowsFromCache(lookupMeta, new Object[] {2L}));
  }

  @Test
  void putAndGetByRowMetaAndData() {
    DatabaseCache cache = new DatabaseCache(10);
    List<Object[]> rows = rows("one");
    cache.putRowsIntoCache(new RowMetaAndData(lookupMeta, 1L), rows);

    assertSame(rows, cache.getRowsFromCache(new RowMetaAndData(lookupMeta, 1L)));
  }

  @Test
  void maxSizeZeroDoesNotEvict() {
    DatabaseCache cache = new DatabaseCache(0);
    cache.putRowsIntoCache(lookupMeta, new Object[] {1L}, rows("a"));
    cache.putRowsIntoCache(lookupMeta, new Object[] {2L}, rows("b"));
    cache.putRowsIntoCache(lookupMeta, new Object[] {3L}, rows("c"));

    assertEquals("a", cache.getRowsFromCache(lookupMeta, new Object[] {1L}).getFirst()[0]);
    assertEquals("b", cache.getRowsFromCache(lookupMeta, new Object[] {2L}).getFirst()[0]);
    assertEquals("c", cache.getRowsFromCache(lookupMeta, new Object[] {3L}).getFirst()[0]);
  }

  @Test
  void maxSizeEvictsEldestEntry() {
    DatabaseCache cache = new DatabaseCache(1);
    cache.putRowsIntoCache(lookupMeta, new Object[] {1L}, rows("a"));
    cache.putRowsIntoCache(lookupMeta, new Object[] {2L}, rows("b"));

    assertNull(cache.getRowsFromCache(lookupMeta, new Object[] {1L}));
    assertEquals("b", cache.getRowsFromCache(lookupMeta, new Object[] {2L}).getFirst()[0]);
    assertFalse(cache.isEmpty());
  }

  private static List<Object[]> rows(Object value) {
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {value});
    return result;
  }
}
