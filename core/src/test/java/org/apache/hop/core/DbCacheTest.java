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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

  @Test
  void concurrentClearsAreAllCounted() throws Exception {
    DbCache cache = DbCache.getInstance();
    int threads = 8;
    int clearsPerThread = 1000;
    int before = cache.getGeneration();

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Void>> futures = new ArrayList<>();
    try {
      for (int t = 0; t < threads; t++) {
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  assertSame(cache, DbCache.getInstance());
                  for (int i = 0; i < clearsPerThread; i++) {
                    cache.clear("one");
                  }
                  return null;
                }));
      }
      start.countDown();
    } finally {
      executor.shutdown();
    }
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    for (Future<Void> future : futures) {
      future.get(); // rethrows any assertion failure from the worker threads
    }

    assertEquals(before + threads * clearsPerThread, cache.getGeneration());
  }

  private RowMeta rowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("a"));
    return rowMeta;
  }
}
