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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.databricks.client.DirectoryEntry;

/**
 * Short-lived cache of directory list results. When a folder is listed via the Files API, child
 * paths and their type/size/lastModified are stored so later {@code doAttach()} calls for those
 * children can use metadata without an extra HEAD (and so last-modified is available for
 * execution-info date filtering).
 */
public class DatabricksListCache {

  public static final long DEFAULT_TTL_MS = 10_000L;

  public static final class ChildInfo {
    public final FileType type;
    public final long size;
    public final long lastModifiedEpochMs;

    public ChildInfo(FileType type, long size, long lastModifiedEpochMs) {
      this.type = type;
      this.size = size;
      this.lastModifiedEpochMs = lastModifiedEpochMs;
    }

    static ChildInfo fromEntry(DirectoryEntry entry) {
      FileType type = entry.directory() ? FileType.FOLDER : FileType.FILE;
      long size = entry.directory() ? -1L : entry.sizeBytes();
      return new ChildInfo(type, size, entry.lastModifiedEpochMs());
    }
  }

  private final long ttlMs;
  private final ConcurrentHashMap<String, CachedList> byParentPath = new ConcurrentHashMap<>();

  public DatabricksListCache() {
    this(DEFAULT_TTL_MS);
  }

  public DatabricksListCache(long ttlMs) {
    this.ttlMs = ttlMs;
  }

  private static class CachedList {
    final Map<String, ChildInfo> byChildPath;
    final long expiryMillis;

    CachedList(Map<String, ChildInfo> byChildPath, long expiryMillis) {
      this.byChildPath = new ConcurrentHashMap<>(byChildPath);
      this.expiryMillis = expiryMillis;
    }
  }

  /** Store list results for a parent workspace path (absolute Files API path). */
  public void put(String parentWorkspacePath, Map<String, ChildInfo> childrenByPath) {
    if (childrenByPath == null || childrenByPath.isEmpty()) {
      return;
    }
    String key = normalizePath(parentWorkspacePath);
    byParentPath.put(key, new CachedList(childrenByPath, System.currentTimeMillis() + ttlMs));
  }

  /**
   * Look up cached metadata for a child workspace path. Returns null if missing or the parent list
   * cache expired.
   */
  public ChildInfo get(String childWorkspacePath) {
    String child = normalizePath(childWorkspacePath);
    String parent = parentPath(child);
    CachedList cached = byParentPath.get(parent);
    if (cached == null) {
      return null;
    }
    if (System.currentTimeMillis() > cached.expiryMillis) {
      byParentPath.remove(parent, cached);
      return null;
    }
    ChildInfo info = cached.byChildPath.get(child);
    if (info != null) {
      return info;
    }
    // Directories may be listed with or without trailing slash
    if (!child.endsWith("/")) {
      return cached.byChildPath.get(child + "/");
    }
    return cached.byChildPath.get(child.substring(0, child.length() - 1));
  }

  public void invalidateParentOf(String workspacePath) {
    byParentPath.remove(parentPath(normalizePath(workspacePath)));
  }

  public void invalidate(String parentWorkspacePath) {
    byParentPath.remove(normalizePath(parentWorkspacePath));
  }

  static String parentPath(String path) {
    if (path == null || path.isEmpty() || "/".equals(path)) {
      return "/";
    }
    String stripped =
        path.endsWith("/") && path.length() > 1 ? path.substring(0, path.length() - 1) : path;
    int last = stripped.lastIndexOf('/');
    if (last <= 0) {
      return "/";
    }
    return stripped.substring(0, last);
  }

  static String normalizePath(String path) {
    if (path == null || path.isEmpty()) {
      return "/";
    }
    if (path.length() > 1 && path.endsWith("/")) {
      return path.substring(0, path.length() - 1);
    }
    return path;
  }
}
