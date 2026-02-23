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
package org.apache.hop.vfs.minio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.vfs2.FileType;
import org.junit.jupiter.api.Test;

class MinioListCacheTest {

  @Test
  void testDefaultTtl() {
    assertEquals(10_000L, MinioListCache.DEFAULT_TTL_MS);
  }

  @Test
  void testDefaultConstructorUsesTtl() {
    MinioListCache cache = new MinioListCache();
    Map<String, MinioListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new MinioListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("bucket", "prefix/", entries);
    assertNotNull(cache.get("bucket", "prefix/", "key"));
  }

  @Test
  void testPutAndGet() {
    MinioListCache cache = new MinioListCache(60_000L);
    Map<String, MinioListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put(
        "path/to/file.txt", new MinioListCache.ChildInfo(FileType.FILE, 1024, Instant.now()));
    entries.put("path/to/sub/", new MinioListCache.ChildInfo(FileType.FOLDER, 0, Instant.EPOCH));

    cache.put("mybucket", "path/to/", entries);

    MinioListCache.ChildInfo fileInfo = cache.get("mybucket", "path/to/", "path/to/file.txt");
    assertNotNull(fileInfo);
    assertEquals(FileType.FILE, fileInfo.type);
    assertEquals(1024, fileInfo.size);

    MinioListCache.ChildInfo folderInfo = cache.get("mybucket", "path/to/", "path/to/sub/");
    assertNotNull(folderInfo);
    assertEquals(FileType.FOLDER, folderInfo.type);
    assertEquals(0, folderInfo.size);
  }

  @Test
  void testGetMissingKey() {
    MinioListCache cache = new MinioListCache(60_000L);
    Map<String, MinioListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put(
        "path/to/file.txt", new MinioListCache.ChildInfo(FileType.FILE, 100, Instant.now()));
    cache.put("mybucket", "path/to/", entries);

    assertNull(cache.get("mybucket", "path/to/", "path/to/other.txt"));
  }

  @Test
  void testGetMissingBucket() {
    MinioListCache cache = new MinioListCache(60_000L);
    assertNull(cache.get("nonexistent", "path/", "path/file"));
  }

  @Test
  void testExpiry() throws InterruptedException {
    MinioListCache cache = new MinioListCache(50L);
    Map<String, MinioListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new MinioListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("bucket", "prefix/", entries);

    assertNotNull(cache.get("bucket", "prefix/", "key"));

    Thread.sleep(100);

    assertNull(cache.get("bucket", "prefix/", "key"));
  }

  @Test
  void testInvalidate() {
    MinioListCache cache = new MinioListCache(60_000L);
    Map<String, MinioListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new MinioListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("bucket", "prefix/", entries);

    assertNotNull(cache.get("bucket", "prefix/", "key"));
    cache.invalidate("bucket", "prefix/");
    assertNull(cache.get("bucket", "prefix/", "key"));
  }

  @Test
  void testInvalidateParentOf() {
    MinioListCache cache = new MinioListCache(60_000L);
    Map<String, MinioListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("dir/file.txt", new MinioListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("bucket", "dir/", entries);

    assertNotNull(cache.get("bucket", "dir/", "dir/file.txt"));
    cache.invalidateParentOf("bucket", "dir/file.txt");
    assertNull(cache.get("bucket", "dir/", "dir/file.txt"));
  }

  @Test
  void testPutEmptyMap() {
    MinioListCache cache = new MinioListCache(60_000L);
    cache.put("bucket", "prefix/", new LinkedHashMap<>());
    assertNull(cache.get("bucket", "prefix/", "anything"));
  }

  @Test
  void testPutNull() {
    MinioListCache cache = new MinioListCache(60_000L);
    cache.put("bucket", "prefix/", null);
    assertNull(cache.get("bucket", "prefix/", "anything"));
  }

  @Test
  void testParentPrefix() {
    assertEquals("path/to/", MinioListCache.parentPrefix("path/to/file"));
    assertEquals("", MinioListCache.parentPrefix("file"));
    assertEquals("", MinioListCache.parentPrefix(""));
    assertEquals("", MinioListCache.parentPrefix(null));
    assertEquals("a/b/", MinioListCache.parentPrefix("a/b/c"));
  }

  @Test
  void testParentPrefixWithTrailingSlash() {
    assertEquals("path/to/", MinioListCache.parentPrefix("path/to/dir/"));
    assertEquals("", MinioListCache.parentPrefix("topdir/"));
  }

  @Test
  void testCustomTtl() {
    MinioListCache cache = new MinioListCache(5_000L);
    Map<String, MinioListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new MinioListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("bucket", "prefix/", entries);
    assertNotNull(cache.get("bucket", "prefix/", "key"));
  }

  @Test
  void testDifferentBucketsSamePrefix() {
    MinioListCache cache = new MinioListCache(60_000L);
    Map<String, MinioListCache.ChildInfo> entries1 = new LinkedHashMap<>();
    entries1.put("prefix/a", new MinioListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    Map<String, MinioListCache.ChildInfo> entries2 = new LinkedHashMap<>();
    entries2.put("prefix/b", new MinioListCache.ChildInfo(FileType.FILE, 20, Instant.now()));

    cache.put("bucket1", "prefix/", entries1);
    cache.put("bucket2", "prefix/", entries2);

    assertNotNull(cache.get("bucket1", "prefix/", "prefix/a"));
    assertNull(cache.get("bucket1", "prefix/", "prefix/b"));
    assertNotNull(cache.get("bucket2", "prefix/", "prefix/b"));
    assertNull(cache.get("bucket2", "prefix/", "prefix/a"));
  }

  @Test
  void testInvalidateNonExistentEntry() {
    MinioListCache cache = new MinioListCache(60_000L);
    cache.invalidate("nonexistent", "prefix/");
    cache.invalidateParentOf("nonexistent", "some/key");
  }

  @Test
  void testChildInfoFields() {
    Instant now = Instant.now();
    MinioListCache.ChildInfo info = new MinioListCache.ChildInfo(FileType.FILE, 42, now);
    assertEquals(FileType.FILE, info.type);
    assertEquals(42, info.size);
    assertEquals(now, info.lastModified);
  }

  @Test
  void testChildInfoFolder() {
    MinioListCache.ChildInfo info = new MinioListCache.ChildInfo(FileType.FOLDER, 0, Instant.EPOCH);
    assertEquals(FileType.FOLDER, info.type);
    assertEquals(0, info.size);
    assertEquals(Instant.EPOCH, info.lastModified);
  }

  @Test
  void testPutOverwritesPreviousEntry() {
    MinioListCache cache = new MinioListCache(60_000L);
    Map<String, MinioListCache.ChildInfo> entries1 = new LinkedHashMap<>();
    entries1.put("key", new MinioListCache.ChildInfo(FileType.FILE, 100, Instant.now()));
    cache.put("bucket", "prefix/", entries1);

    assertEquals(100, cache.get("bucket", "prefix/", "key").size);

    Map<String, MinioListCache.ChildInfo> entries2 = new LinkedHashMap<>();
    entries2.put("key", new MinioListCache.ChildInfo(FileType.FILE, 200, Instant.now()));
    cache.put("bucket", "prefix/", entries2);

    assertEquals(200, cache.get("bucket", "prefix/", "key").size);
  }

  @Test
  void testInvalidateOnlyAffectsTargetPrefix() {
    MinioListCache cache = new MinioListCache(60_000L);
    Map<String, MinioListCache.ChildInfo> entries1 = new LinkedHashMap<>();
    entries1.put("a/file", new MinioListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    Map<String, MinioListCache.ChildInfo> entries2 = new LinkedHashMap<>();
    entries2.put("b/file", new MinioListCache.ChildInfo(FileType.FILE, 20, Instant.now()));

    cache.put("bucket", "a/", entries1);
    cache.put("bucket", "b/", entries2);

    cache.invalidate("bucket", "a/");
    assertNull(cache.get("bucket", "a/", "a/file"));
    assertNotNull(cache.get("bucket", "b/", "b/file"));
  }

  @Test
  void testMultipleChildrenInSamePrefix() {
    MinioListCache cache = new MinioListCache(60_000L);
    Map<String, MinioListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("dir/file1.txt", new MinioListCache.ChildInfo(FileType.FILE, 100, Instant.now()));
    entries.put("dir/file2.txt", new MinioListCache.ChildInfo(FileType.FILE, 200, Instant.now()));
    entries.put("dir/sub/", new MinioListCache.ChildInfo(FileType.FOLDER, 0, Instant.EPOCH));

    cache.put("bucket", "dir/", entries);

    assertNotNull(cache.get("bucket", "dir/", "dir/file1.txt"));
    assertNotNull(cache.get("bucket", "dir/", "dir/file2.txt"));
    assertNotNull(cache.get("bucket", "dir/", "dir/sub/"));
    assertEquals(100, cache.get("bucket", "dir/", "dir/file1.txt").size);
    assertEquals(200, cache.get("bucket", "dir/", "dir/file2.txt").size);
    assertEquals(FileType.FOLDER, cache.get("bucket", "dir/", "dir/sub/").type);
  }

  @Test
  void testRootPrefixEmpty() {
    MinioListCache cache = new MinioListCache(60_000L);
    Map<String, MinioListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("topfile.txt", new MinioListCache.ChildInfo(FileType.FILE, 50, Instant.now()));
    cache.put("bucket", "", entries);

    assertNotNull(cache.get("bucket", "", "topfile.txt"));
  }
}
