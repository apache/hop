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

package org.apache.hop.vfs.azure;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.vfs2.FileType;

/**
 * Cache for Azure list results. When a directory is listed via listPaths, we store the child paths
 * and their types/sizes so that later doAttach() calls for those children can be answered without
 * extra API calls.
 */
public class AzureListCache {

  public static final long DEFAULT_TTL_MS = 10_000L;

  public static final class ChildInfo {
    public final FileType type;
    public final long size;
    public final Instant lastModified;

    public ChildInfo(FileType type, long size, Instant lastModified) {
      this.type = type;
      this.size = size;
      this.lastModified = lastModified;
    }
  }

  private final long ttlMs;
  private final ConcurrentHashMap<String, CachedList> cache = new ConcurrentHashMap<>();

  public AzureListCache() {
    this(DEFAULT_TTL_MS);
  }

  public AzureListCache(long ttlMs) {
    this.ttlMs = ttlMs;
  }

  private static String cacheKey(String container, String prefix) {
    return container + "|" + prefix;
  }

  private static class CachedList {
    final Map<String, ChildInfo> entries;
    final long expiryMillis;

    CachedList(Map<String, ChildInfo> entries, long expiryMillis) {
      this.entries = new ConcurrentHashMap<>(entries);
      this.expiryMillis = expiryMillis;
    }
  }

  /** Store list result for the given container and prefix. */
  public void put(String container, String prefix, Map<String, ChildInfo> childEntries) {
    if (childEntries == null || childEntries.isEmpty()) {
      return;
    }
    String key = cacheKey(container, prefix);
    cache.put(key, new CachedList(childEntries, System.currentTimeMillis() + ttlMs));
  }

  /**
   * Look up cached type/size/lastModified for a child. Returns null if not in cache or cache
   * expired.
   */
  public ChildInfo get(String container, String parentPrefix, String childPath) {
    String key = cacheKey(container, parentPrefix);
    CachedList cached = cache.get(key);
    if (cached == null) {
      return null;
    }
    if (System.currentTimeMillis() > cached.expiryMillis) {
      cache.remove(key);
      return null;
    }
    return cached.entries.get(childPath);
  }

  /** Invalidate the list cache for the given container and prefix. */
  public void invalidate(String container, String prefix) {
    cache.remove(cacheKey(container, prefix));
  }

  /**
   * Invalidate the list cache for the parent directory of the given path. Call after put/delete so
   * the next list reflects the change.
   */
  public void invalidateParentOf(String container, String path) {
    String parentPrefix = parentPrefix(path);
    invalidate(container, parentPrefix);
  }

  /** Compute parent prefix: "path/to/" for path "path/to/file", "" for "file" or "". */
  public static String parentPrefix(String path) {
    if (path == null || path.isEmpty()) {
      return "";
    }
    String stripped = path.startsWith("/") ? path.substring(1) : path;
    stripped = stripped.endsWith("/") ? stripped.substring(0, stripped.length() - 1) : stripped;
    int last = stripped.lastIndexOf('/');
    return last >= 0 ? stripped.substring(0, last + 1) : "";
  }
}
