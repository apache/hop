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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.caching.CacheEntry;
import org.apache.hop.execution.caching.DatedId;

@Getter
@Setter
public class NeoLocationCache {
  private static NeoLocationCache instance;

  private final Map<String, CacheEntry> cache;

  private int maximumSize;
  private int evictionBatchSize;

  private NeoLocationCache() {
    this.cache = new HashMap<>();
    this.maximumSize = 1000;
    this.evictionBatchSize = 50;
  }

  public static synchronized NeoLocationCache getInstance() {
    if (instance == null) {
      instance = new NeoLocationCache();
    }
    return instance;
  }

  public static void add(CacheEntry entry) {
    NeoLocationCache lc = getInstance();
    synchronized (lc.cache) {
      lc.cache.put(entry.getId(), entry);
      manageCacheSize();
    }
  }

  public static void store(Execution execution) {
    CacheEntry cacheEntry = new CacheEntry();
    cacheEntry.setId(execution.getId());
    cacheEntry.setExecution(execution);
    cacheEntry.setLastWritten(new Date());
    add(cacheEntry);
  }

  public static void store(ExecutionState executionState) {
    CacheEntry cacheEntry = get(executionState.getId());
    if (cacheEntry != null) {
      cacheEntry.setExecutionState(executionState);
      cacheEntry.setLastWritten(new Date());
    }
  }

  public static void store(String executionId, ExecutionData executionData) {
    CacheEntry cacheEntry = get(executionId);
    if (cacheEntry != null) {
      cacheEntry.addExecutionData(executionData);
      cacheEntry.setLastWritten(new Date());
    }
  }

  public static CacheEntry get(String id) {
    NeoLocationCache lc = getInstance();
    synchronized (lc.cache) {
      CacheEntry cacheEntry = lc.cache.get(id);
      if (cacheEntry != null) {
        cacheEntry.setLastRead(new Date());
      }
      return cacheEntry;
    }
  }

  public static Execution getExecution(String executionId) {
    CacheEntry cacheEntry = get(executionId);
    if (cacheEntry != null) {
      return cacheEntry.getExecution();
    }
    return null;
  }

  public static ExecutionState getExecutionState(String executionId) {
    CacheEntry cacheEntry = get(executionId);
    if (cacheEntry != null) {
      return cacheEntry.getExecutionState();
    }
    return null;
  }

  public static void remove(String executionId) {
    NeoLocationCache lc = getInstance();
    synchronized (lc.cache) {
      lc.cache.remove(executionId);
    }
  }

  private static void manageCacheSize() {
    NeoLocationCache lc = getInstance();
    Map<String, CacheEntry> c = lc.cache;
    if (c.size() < lc.maximumSize + lc.evictionBatchSize) {
      return;
    }
    List<DatedId> datedIds = new ArrayList<>();
    for (CacheEntry ce : c.values()) {
      Date date = ce.getLastRead();
      if (date == null) {
        date = Const.MIN_DATE;
      }
      datedIds.add(new DatedId(ce.getId(), date));
    }
    datedIds.sort(Comparator.comparing(DatedId::getDate));
    int toRemove = Math.min(lc.evictionBatchSize, datedIds.size());
    for (int i = 0; i < toRemove; i++) {
      c.remove(datedIds.get(i).getId());
    }
  }

  public static void clear() {
    NeoLocationCache lc = getInstance();
    synchronized (lc.cache) {
      lc.cache.clear();
    }
  }
}
