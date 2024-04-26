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

package org.apache.hop.pipeline.transforms.databasejoin.cache;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;

public class DatabaseCache {

  private Map<RowMetaAndData, List<Object[]>> cache;

  public DatabaseCache(int maxSize) {
    cache = new EvictableCacheMap(maxSize);
  }

  public List<Object[]> getRowsFromCache(IRowMeta lookupMeta, Object[] lookupRow) {
    return getRowsFromCache(new RowMetaAndData(lookupMeta, lookupRow));
  }

  public List<Object[]> getRowsFromCache(RowMetaAndData key) {
    return cache.getOrDefault(key, null);
  }

  public void putRowsIntoCache(RowMetaAndData key, List<Object[]> values) {
    cache.put(key, values);
  }

  public void putRowsIntoCache(IRowMeta lookupMeta, Object[] lookupRow, List<Object[]> values) {
    putRowsIntoCache(new RowMetaAndData(lookupMeta, lookupRow), values);
  }

  private static class EvictableCacheMap extends LinkedHashMap<RowMetaAndData, List<Object[]>> {

    private final int maxSize;

    public EvictableCacheMap(int maxSize) {
      super();
      this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<RowMetaAndData, List<Object[]>> eldest) {
      return maxSize > 0
          && size() > maxSize; // do not remove entries if maxSize is 0 (inherit from LinkedHashMap)
    }
  }

  public boolean isEmpty() {
    return cache.isEmpty();
  }
}
