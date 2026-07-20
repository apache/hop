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

package org.apache.hop.vfs.databricks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.apache.commons.vfs2.FileType;
import org.junit.jupiter.api.Test;

class DatabricksListCacheTest {

  @Test
  void putAndGetByChildPath() {
    DatabricksListCache cache = new DatabricksListCache(60_000L);
    cache.put(
        "/Volumes/c/s/v",
        Map.of(
            "/Volumes/c/s/v/a.json", new DatabricksListCache.ChildInfo(FileType.FILE, 10L, 1234L)));

    DatabricksListCache.ChildInfo info = cache.get("/Volumes/c/s/v/a.json");
    assertNotNull(info);
    assertEquals(FileType.FILE, info.type);
    assertEquals(10L, info.size);
    assertEquals(1234L, info.lastModifiedEpochMs);
  }

  @Test
  void expiredEntriesAreDropped() throws Exception {
    DatabricksListCache cache = new DatabricksListCache(1L);
    cache.put(
        "/Volumes/c/s/v",
        Map.of(
            "/Volumes/c/s/v/a.json", new DatabricksListCache.ChildInfo(FileType.FILE, 10L, 1234L)));
    Thread.sleep(5L);
    assertNull(cache.get("/Volumes/c/s/v/a.json"));
  }

  @Test
  void parentPathHelpers() {
    assertEquals("/Volumes/c/s/v", DatabricksListCache.parentPath("/Volumes/c/s/v/a.json"));
    assertEquals("/", DatabricksListCache.parentPath("/Volumes"));
    assertEquals("/Volumes/c/s/v", DatabricksListCache.normalizePath("/Volumes/c/s/v/"));
  }
}
