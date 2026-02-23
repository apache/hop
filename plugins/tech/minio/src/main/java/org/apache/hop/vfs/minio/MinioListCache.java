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

package org.apache.hop.vfs.minio;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.vfs2.FileType;

/**
 * Cache for MinIO list results. When a directory is listed via listObjects, we store the child keys
 * and their types/sizes so that later doAttach() calls for those children can skip statObject
 * calls.
 */
public class MinioListCache {

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

  public MinioListCache() {
    this(DEFAULT_TTL_MS);
  }

  public MinioListCache(long ttlMs) {
    this.ttlMs = ttlMs;
  }

  private static String cacheKey(String bucket, String prefix) {
    return bucket + "|" + prefix;
  }

  private static class CachedList {
    final Map<String, ChildInfo> entries;
    final long expiryMillis;

    CachedList(Map<String, ChildInfo> entries, long expiryMillis) {
      this.entries = new ConcurrentHashMap<>(entries);
      this.expiryMillis = expiryMillis;
    }
  }

  /** Store list result for the given bucket and prefix. */
  public void put(String bucket, String prefix, Map<String, ChildInfo> childEntries) {
    if (childEntries == null || childEntries.isEmpty()) {
      return;
    }
    String key = cacheKey(bucket, prefix);
    cache.put(key, new CachedList(childEntries, System.currentTimeMillis() + ttlMs));
  }

  /**
   * Look up cached type/size/lastModified for a child. Returns null if not in cache or cache
   * expired.
   */
  public ChildInfo get(String bucket, String parentPrefix, String childFullKey) {
    String key = cacheKey(bucket, parentPrefix);
    CachedList cached = cache.get(key);
    if (cached == null) {
      return null;
    }
    if (System.currentTimeMillis() > cached.expiryMillis) {
      cache.remove(key);
      return null;
    }
    return cached.entries.get(childFullKey);
  }

  /** Invalidate the list cache for the given bucket and prefix. */
  public void invalidate(String bucket, String prefix) {
    cache.remove(cacheKey(bucket, prefix));
  }

  /**
   * Invalidate the list cache for the parent directory of the given key. Call after put/delete so
   * the next list reflects the change.
   */
  public void invalidateParentOf(String bucket, String key) {
    String parentPrefix = parentPrefix(key);
    invalidate(bucket, parentPrefix);
  }

  /** Compute parent prefix: "path/to/" for key "path/to/file", "" for "file" or "". */
  public static String parentPrefix(String key) {
    if (key == null || key.isEmpty()) {
      return "";
    }
    String stripped = key.endsWith("/") ? key.substring(0, key.length() - 1) : key;
    int last = stripped.lastIndexOf('/');
    return last >= 0 ? stripped.substring(0, last + 1) : "";
  }
}
