/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicBoolean;
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

  private Map<String, CacheEntry> cache;

  private final AtomicBoolean locked;

  private int maximumSize;

  private NeoLocationCache() {
    this.locked = new AtomicBoolean(false);
    this.cache = new HashMap<>();
    this.maximumSize = 1000;
  }

  public static NeoLocationCache getInstance() {
    if (instance == null) {
      instance = new NeoLocationCache();
    }
    return instance;
  }

  public static void add(CacheEntry entry) {
    synchronized (getInstance().locked) {
      getInstance().locked.set(true);
      getInstance().cache.put(entry.getId(), entry);
      getInstance().locked.set(false);
    }
    manageCacheSize();
  }

  public static void store(Execution execution) {
    // Add a new Cache Entry
    //
    CacheEntry cacheEntry = new CacheEntry();
    cacheEntry.setId(execution.getId());
    cacheEntry.setExecution(execution);
    cacheEntry.setLastWritten(new Date());
    add(cacheEntry);
  }

  public static void store(ExecutionState executionState) {
    // Update the cache entry
    //
    CacheEntry cacheEntry = get(executionState.getId());
    if (cacheEntry != null) {
      cacheEntry.setExecutionState(executionState);
      cacheEntry.setLastWritten(new Date());
    }
  }

  public static void store(String executionId, ExecutionData executionData) {
    // Update the cache entry
    //
    CacheEntry cacheEntry = get(executionId);
    if (cacheEntry != null) {
      cacheEntry.addExecutionData(executionData);
      cacheEntry.setLastWritten(new Date());
    }
  }

  public static CacheEntry get(String id) {
    synchronized (getInstance().locked) {
      CacheEntry cacheEntry = getInstance().cache.get(id);
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
    synchronized (getInstance().locked) {
      getInstance().cache.remove(executionId);
    }
  }

  private static synchronized void manageCacheSize() {
    NeoLocationCache lc = getInstance();
    Map<String, CacheEntry> c = lc.cache;
    try {
      if (lc.locked.get()) {
        // Let the buffer overrun happen for a bit
        // We'll sweep it clean on the next one.
        return;
      }
      lc.locked.set(true);
      // The maximum size is by default 1000 entries
      if (c.size() >= lc.maximumSize + 50) {
        // Remove the last 50
        //
        List<DatedId> datedIds = new ArrayList<>();
        for (CacheEntry ce : c.values()) {
          Date date = ce.getLastRead();
          if (date == null) {
            // Never read?  Perhaps it's time to get rid of it.
            //
            date = Const.MIN_DATE;
          }
          datedIds.add(new DatedId(ce.getId(), date));
        }
        // reverse sort by creation date of the cache entry
        //
        datedIds.sort(Comparator.comparing(DatedId::getDate).reversed());

        // Now delete the first 50 records in the cache
        //
        for (DatedId datedId : datedIds) {
          c.remove(datedId.getId());
        }
      }
    } finally {
      instance.locked.set(false);
    }
  }

  public static void clear() {
    synchronized (getInstance().locked) {
      getInstance().cache.clear();
    }
  }
}
