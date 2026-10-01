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

package org.apache.hop.neo4j.execution.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.execution.caching.CacheEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NeoLocationCacheTest {

  @BeforeEach
  @AfterEach
  void reset() {
    NeoLocationCache.clear();
    NeoLocationCache cache = NeoLocationCache.getInstance();
    cache.setMaximumSize(1000);
    cache.setEvictionBatchSize(50);
  }

  @Test
  void evictionRemovesABatchNotTheWholeCache() {
    NeoLocationCache cache = NeoLocationCache.getInstance();
    cache.setMaximumSize(5);
    cache.setEvictionBatchSize(50);

    for (int i = 0; i < 55; i++) {
      CacheEntry entry = new CacheEntry();
      entry.setId("id-" + i);
      NeoLocationCache.add(entry);
    }

    assertEquals(5, cache.getCache().size());
  }

  @Test
  void removeDropsASingleEntry() {
    CacheEntry entry = new CacheEntry();
    entry.setId("keep-me");
    NeoLocationCache.add(entry);

    NeoLocationCache.remove("keep-me");

    assertNull(NeoLocationCache.get("keep-me"));
  }
}
