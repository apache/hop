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
package org.apache.hop.vfs.s3.s3common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.vfs2.FileType;
import org.junit.jupiter.api.Test;

class S3ListCacheTest {

  @Test
  void testDefaultTtl() {
    assertEquals(10_000L, S3ListCache.DEFAULT_TTL_MS);
  }

  @Test
  void testPutAndGet() {
    S3ListCache cache = new S3ListCache(60_000L);
    Map<String, S3ListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("path/to/file.txt", new S3ListCache.ChildInfo(FileType.FILE, 1024, Instant.now()));
    entries.put("path/to/sub/", new S3ListCache.ChildInfo(FileType.FOLDER, 0, Instant.EPOCH));

    cache.put("mybucket", "path/to/", entries);

    S3ListCache.ChildInfo fileInfo = cache.get("mybucket", "path/to/", "path/to/file.txt");
    assertNotNull(fileInfo);
    assertEquals(FileType.FILE, fileInfo.type);
    assertEquals(1024, fileInfo.size);

    S3ListCache.ChildInfo folderInfo = cache.get("mybucket", "path/to/", "path/to/sub/");
    assertNotNull(folderInfo);
    assertEquals(FileType.FOLDER, folderInfo.type);
    assertEquals(0, folderInfo.size);
  }

  @Test
  void testGetMissingKey() {
    S3ListCache cache = new S3ListCache(60_000L);
    Map<String, S3ListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("path/to/file.txt", new S3ListCache.ChildInfo(FileType.FILE, 100, Instant.now()));
    cache.put("mybucket", "path/to/", entries);

    assertNull(cache.get("mybucket", "path/to/", "path/to/other.txt"));
  }

  @Test
  void testGetMissingBucket() {
    S3ListCache cache = new S3ListCache(60_000L);
    assertNull(cache.get("nonexistent", "path/", "path/file"));
  }

  @Test
  void testExpiry() throws InterruptedException {
    S3ListCache cache = new S3ListCache(50L);
    Map<String, S3ListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new S3ListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("bucket", "prefix/", entries);

    assertNotNull(cache.get("bucket", "prefix/", "key"));

    Thread.sleep(100);

    assertNull(cache.get("bucket", "prefix/", "key"));
  }

  @Test
  void testInvalidate() {
    S3ListCache cache = new S3ListCache(60_000L);
    Map<String, S3ListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new S3ListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("bucket", "prefix/", entries);

    assertNotNull(cache.get("bucket", "prefix/", "key"));
    cache.invalidate("bucket", "prefix/");
    assertNull(cache.get("bucket", "prefix/", "key"));
  }

  @Test
  void testInvalidateParentOf() {
    S3ListCache cache = new S3ListCache(60_000L);
    Map<String, S3ListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("dir/file.txt", new S3ListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("bucket", "dir/", entries);

    assertNotNull(cache.get("bucket", "dir/", "dir/file.txt"));
    cache.invalidateParentOf("bucket", "dir/file.txt");
    assertNull(cache.get("bucket", "dir/", "dir/file.txt"));
  }

  @Test
  void testPutEmptyMap() {
    S3ListCache cache = new S3ListCache(60_000L);
    cache.put("bucket", "prefix/", new LinkedHashMap<>());
    assertNull(cache.get("bucket", "prefix/", "anything"));
  }

  @Test
  void testPutNull() {
    S3ListCache cache = new S3ListCache(60_000L);
    cache.put("bucket", "prefix/", null);
    assertNull(cache.get("bucket", "prefix/", "anything"));
  }

  @Test
  void testParentPrefix() {
    assertEquals("path/to/", S3ListCache.parentPrefix("path/to/file"));
    assertEquals("", S3ListCache.parentPrefix("file"));
    assertEquals("", S3ListCache.parentPrefix(""));
    assertEquals("", S3ListCache.parentPrefix(null));
    assertEquals("a/b/", S3ListCache.parentPrefix("a/b/c"));
  }

  @Test
  void testCustomTtl() {
    S3ListCache cache = new S3ListCache(5_000L);
    Map<String, S3ListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new S3ListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("bucket", "prefix/", entries);
    assertNotNull(cache.get("bucket", "prefix/", "key"));
  }

  @Test
  void testDifferentBucketsSamePrefix() {
    S3ListCache cache = new S3ListCache(60_000L);
    Map<String, S3ListCache.ChildInfo> entries1 = new LinkedHashMap<>();
    entries1.put("prefix/a", new S3ListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    Map<String, S3ListCache.ChildInfo> entries2 = new LinkedHashMap<>();
    entries2.put("prefix/b", new S3ListCache.ChildInfo(FileType.FILE, 20, Instant.now()));

    cache.put("bucket1", "prefix/", entries1);
    cache.put("bucket2", "prefix/", entries2);

    assertNotNull(cache.get("bucket1", "prefix/", "prefix/a"));
    assertNull(cache.get("bucket1", "prefix/", "prefix/b"));
    assertNotNull(cache.get("bucket2", "prefix/", "prefix/b"));
    assertNull(cache.get("bucket2", "prefix/", "prefix/a"));
  }

  @Test
  void testInvalidateNonExistentEntry() {
    S3ListCache cache = new S3ListCache(60_000L);
    cache.invalidate("nonexistent", "prefix/");
    cache.invalidateParentOf("nonexistent", "some/key");
  }

  @Test
  void testChildInfoFields() {
    Instant now = Instant.now();
    S3ListCache.ChildInfo info = new S3ListCache.ChildInfo(FileType.FILE, 42, now);
    assertEquals(FileType.FILE, info.type);
    assertEquals(42, info.size);
    assertEquals(now, info.lastModified);
  }
}
