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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;

/** Old code, copied from {@linkplain DatabaseLookup} */
public class DefaultCache implements DatabaseLookupData.ICache {

  public static DefaultCache newCache(DatabaseLookupData data, int cacheSize) {
    if (cacheSize > 0) {
      cacheSize = (int) (cacheSize * 1.5);
    } else {
      cacheSize = 16;
    }
    return new DefaultCache(data, cacheSize);
  }

  private final DatabaseLookupData data;

  /** Keys in insertion order: the oldest entry is evicted first when a cache size is set. */
  private final LinkedHashMap<RowMetaAndData, Object[]> map;

  /**
   * Reused to probe the map, so a lookup allocates no key. It is never stored: hashCode() and
   * equals() of {@link RowMetaAndData} only read the row meta and data it holds at that moment. The
   * cache belongs to one transform copy, whose lookups are serialized.
   */
  private final RowMetaAndData probe = new RowMetaAndData();

  DefaultCache(DatabaseLookupData data, int capacity) {
    this.data = data;
    map = LinkedHashMap.newLinkedHashMap(capacity);
  }

  @Override
  public Object[] getRowFromCache(IRowMeta lookupMeta, Object[] lookupRow) throws HopException {
    if (data.allEquals) {
      // only do the map lookup when all equals otherwise conditions >, <, <> will give wrong
      // results
      probe.setRowMeta(data.lookupMeta);
      probe.setData(lookupRow);
      return map.get(probe);
    } else { // special handling of conditions <,>, <> etc.
      if (!data.hasDBCondition) { // e.g. LIKE not handled by this routine, yet
        //  find an alternative way to look up the data based on the condition.
        // Not all conditions are "=" so we are going to have to evaluate row by row
        // A sorted list or index might be a good solution here...
        //
        for (Map.Entry<RowMetaAndData, Object[]> entry : map.entrySet()) {
          RowMetaAndData key = entry.getKey();
          // Now verify that the key is matching our conditions...
          //
          boolean match = true;
          int lookupIndex = 0;
          for (int i = 0; i < data.conditions.length && match; i++) {
            IValueMeta cmpMeta = lookupMeta.getValueMeta(lookupIndex);
            Object cmpData = lookupRow[lookupIndex];
            IValueMeta keyMeta = key.getValueMeta(i);
            Object keyData = key.getData()[i];

            switch (data.conditions[i]) {
              case DatabaseLookupMeta.CONDITION_EQ:
                match = (cmpMeta.compare(cmpData, keyMeta, keyData) == 0);
                break;
              case DatabaseLookupMeta.CONDITION_NE:
                match = (cmpMeta.compare(cmpData, keyMeta, keyData) != 0);
                break;
              case DatabaseLookupMeta.CONDITION_LT:
                match = (cmpMeta.compare(cmpData, keyMeta, keyData) > 0);
                break;
              case DatabaseLookupMeta.CONDITION_LE:
                match = (cmpMeta.compare(cmpData, keyMeta, keyData) >= 0);
                break;
              case DatabaseLookupMeta.CONDITION_GT:
                match = (cmpMeta.compare(cmpData, keyMeta, keyData) < 0);
                break;
              case DatabaseLookupMeta.CONDITION_GE:
                match = (cmpMeta.compare(cmpData, keyMeta, keyData) <= 0);
                break;
              case DatabaseLookupMeta.CONDITION_IS_NULL:
                match = keyMeta.isNull(keyData);
                break;
              case DatabaseLookupMeta.CONDITION_IS_NOT_NULL:
                match = !keyMeta.isNull(keyData);
                break;
              case DatabaseLookupMeta.CONDITION_BETWEEN:
                // Between key >= cmp && key <= cmp2
                IValueMeta cmpMeta2 = lookupMeta.getValueMeta(lookupIndex + 1);
                Object cmpData2 = lookupRow[lookupIndex + 1];
                match = (keyMeta.compare(keyData, cmpMeta, cmpData) >= 0);
                if (match) {
                  match = (keyMeta.compare(keyData, cmpMeta2, cmpData2) <= 0);
                }
                lookupIndex++;
                break;
                //  add LIKE operator (think of changing the hasDBCondition logic then)
              default:
                match = false;
                data.hasDBCondition =
                    true; // avoid looping in here the next time, also safety when a new condition
                // will be introduced
                break;
            }
            lookupIndex++;
          }
          if (match) {
            return entry.getValue();
          }
        }
      }
    }
    return null;
  }

  @Override
  public void storeRowInCache(
      DatabaseLookupMeta meta, IRowMeta lookupMeta, Object[] lookupRow, Object[] add) {
    RowMetaAndData rowMetaAndData = new RowMetaAndData(lookupMeta, lookupRow);
    //  2009-02-01 XXX: I want to write a test case to prove this point before checking
    // in.
    // /* Don't insert a row with a duplicate key into the cache. It doesn't seem
    // * to serve a useful purpose and can potentially cause the transform to return
    // * different values over the life of the pipeline (if the source DB rows change)
    // * Additionally, if using the load all data feature, re-inserting would reverse the order
    // * specified in the transform.
    // */
    map.put(rowMetaAndData, add);

    // Limit the cache to the cache size by evicting the oldest entries. When all data is loaded
    // into the cache there is no limit: a cache size set before load-all was switched on must not
    // throw out rows that will never be read again.
    int cacheSize = meta.getCacheSize();
    if (!meta.isLoadingAllDataInCache() && cacheSize > 0 && map.size() > cacheSize) {
      Iterator<RowMetaAndData> eldest = map.keySet().iterator();
      while (map.size() > cacheSize && eldest.hasNext()) {
        eldest.next();
        eldest.remove();
      }
    }
  }
}
