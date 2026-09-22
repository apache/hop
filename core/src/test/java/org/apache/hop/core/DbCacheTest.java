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
package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

class DbCacheTest {

  @Test
  void entriesAreRemovedPerConnection() {
    DbCache cache = DbCache.getInstance();
    cache.clear(null);

    cache.put(new DbCacheEntry("one", "select * from t"), rowMeta());
    cache.put(new DbCacheEntry("two", "select * from t"), rowMeta());

    cache.clear("ONE");

    assertNull(cache.get(new DbCacheEntry("one", "select * from t")));
    assertNotNull(cache.get(new DbCacheEntry("two", "select * from t")));
  }

  @Test
  void clearingBumpsTheGeneration() {
    DbCache cache = DbCache.getInstance();

    int before = cache.getGeneration();
    cache.put(new DbCacheEntry("one", "select * from t"), rowMeta());
    assertEquals(before, cache.getGeneration(), "storing an entry is not a change of generation");

    cache.clear(null);
    int afterClearAll = cache.getGeneration();
    assertNotEquals(before, afterClearAll);

    cache.clear("one");
    assertNotEquals(afterClearAll, cache.getGeneration(), "clearing one connection counts as well");
  }

  private RowMeta rowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("a"));
    return rowMeta;
  }
}
