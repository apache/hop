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

package org.apache.hop.ui.hopgui.file.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.DbCacheEntry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.Test;

/** The Problems tab force refresh drops cached database metadata before it checks again. */
class ProblemsForceRefreshTest {

  @Test
  void oneClickClearsTheCachesThenRefreshesTheOtherEditorsThenRechecks() {
    DbCache cache = DbCache.getInstance();
    cache.clear(null);
    int generation = cache.getGeneration();
    cache.put(new DbCacheEntry("conn", "select * from t"), row("old"));

    PipelineMeta openPipeline = spy(new PipelineMeta());
    List<String> order = new ArrayList<>();
    doAnswer(
            invocation -> {
              order.add("caches");
              return null;
            })
        .when(openPipeline)
        .clearCaches();

    ProblemsForceRefresh.refresh(
        List.of(openPipeline),
        () -> {
          order.add("others");
          assertNull(cache.get(new DbCacheEntry("conn", "select * from t")));
        },
        () -> order.add("recheck"));

    assertNotEquals(generation, cache.getGeneration());
    assertEquals(List.of("caches", "others", "recheck"), order);
  }

  private static RowMeta row(String name) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString(name));
    return rowMeta;
  }
}
