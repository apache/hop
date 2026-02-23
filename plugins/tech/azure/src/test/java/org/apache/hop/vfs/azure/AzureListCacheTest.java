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
package org.apache.hop.vfs.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.vfs2.FileType;
import org.junit.jupiter.api.Test;

class AzureListCacheTest {

  @Test
  void testDefaultTtl() {
    assertEquals(10_000L, AzureListCache.DEFAULT_TTL_MS);
  }

  @Test
  void testDefaultConstructorUsesTtl() {
    AzureListCache cache = new AzureListCache();
    Map<String, AzureListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new AzureListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("container", "prefix/", entries);
    assertNotNull(cache.get("container", "prefix/", "key"));
  }

  @Test
  void testPutAndGet() {
    AzureListCache cache = new AzureListCache(60_000L);
    Map<String, AzureListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put(
        "path/to/file.txt", new AzureListCache.ChildInfo(FileType.FILE, 1024, Instant.now()));
    entries.put("path/to/sub/", new AzureListCache.ChildInfo(FileType.FOLDER, 0, Instant.EPOCH));

    cache.put("mycontainer", "path/to/", entries);

    AzureListCache.ChildInfo fileInfo = cache.get("mycontainer", "path/to/", "path/to/file.txt");
    assertNotNull(fileInfo);
    assertEquals(FileType.FILE, fileInfo.type);
    assertEquals(1024, fileInfo.size);

    AzureListCache.ChildInfo folderInfo = cache.get("mycontainer", "path/to/", "path/to/sub/");
    assertNotNull(folderInfo);
    assertEquals(FileType.FOLDER, folderInfo.type);
    assertEquals(0, folderInfo.size);
  }

  @Test
  void testGetMissingKey() {
    AzureListCache cache = new AzureListCache(60_000L);
    Map<String, AzureListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put(
        "path/to/file.txt", new AzureListCache.ChildInfo(FileType.FILE, 100, Instant.now()));
    cache.put("mycontainer", "path/to/", entries);

    assertNull(cache.get("mycontainer", "path/to/", "path/to/other.txt"));
  }

  @Test
  void testGetMissingContainer() {
    AzureListCache cache = new AzureListCache(60_000L);
    assertNull(cache.get("nonexistent", "path/", "path/file"));
  }

  @Test
  void testExpiry() throws InterruptedException {
    AzureListCache cache = new AzureListCache(50L);
    Map<String, AzureListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new AzureListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("container", "prefix/", entries);

    assertNotNull(cache.get("container", "prefix/", "key"));

    Thread.sleep(100);

    assertNull(cache.get("container", "prefix/", "key"));
  }

  @Test
  void testInvalidate() {
    AzureListCache cache = new AzureListCache(60_000L);
    Map<String, AzureListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new AzureListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("container", "prefix/", entries);

    assertNotNull(cache.get("container", "prefix/", "key"));
    cache.invalidate("container", "prefix/");
    assertNull(cache.get("container", "prefix/", "key"));
  }

  @Test
  void testInvalidateParentOf() {
    AzureListCache cache = new AzureListCache(60_000L);
    Map<String, AzureListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("dir/file.txt", new AzureListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("container", "dir/", entries);

    assertNotNull(cache.get("container", "dir/", "dir/file.txt"));
    cache.invalidateParentOf("container", "dir/file.txt");
    assertNull(cache.get("container", "dir/", "dir/file.txt"));
  }

  @Test
  void testPutEmptyMap() {
    AzureListCache cache = new AzureListCache(60_000L);
    cache.put("container", "prefix/", new LinkedHashMap<>());
    assertNull(cache.get("container", "prefix/", "anything"));
  }

  @Test
  void testPutNull() {
    AzureListCache cache = new AzureListCache(60_000L);
    cache.put("container", "prefix/", null);
    assertNull(cache.get("container", "prefix/", "anything"));
  }

  @Test
  void testParentPrefix() {
    assertEquals("path/to/", AzureListCache.parentPrefix("path/to/file"));
    assertEquals("", AzureListCache.parentPrefix("file"));
    assertEquals("", AzureListCache.parentPrefix(""));
    assertEquals("", AzureListCache.parentPrefix(null));
    assertEquals("a/b/", AzureListCache.parentPrefix("a/b/c"));
  }

  @Test
  void testParentPrefixWithLeadingSlash() {
    assertEquals("", AzureListCache.parentPrefix("/topdir"));
    assertEquals("path/to/", AzureListCache.parentPrefix("/path/to/file"));
  }

  @Test
  void testParentPrefixWithTrailingSlash() {
    assertEquals("path/to/", AzureListCache.parentPrefix("path/to/dir/"));
    assertEquals("", AzureListCache.parentPrefix("topdir/"));
  }

  @Test
  void testParentPrefixWithLeadingAndTrailingSlash() {
    assertEquals("path/", AzureListCache.parentPrefix("/path/dir/"));
  }

  @Test
  void testCustomTtl() {
    AzureListCache cache = new AzureListCache(5_000L);
    Map<String, AzureListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("key", new AzureListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    cache.put("container", "prefix/", entries);
    assertNotNull(cache.get("container", "prefix/", "key"));
  }

  @Test
  void testDifferentContainersSamePrefix() {
    AzureListCache cache = new AzureListCache(60_000L);
    Map<String, AzureListCache.ChildInfo> entries1 = new LinkedHashMap<>();
    entries1.put("prefix/a", new AzureListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    Map<String, AzureListCache.ChildInfo> entries2 = new LinkedHashMap<>();
    entries2.put("prefix/b", new AzureListCache.ChildInfo(FileType.FILE, 20, Instant.now()));

    cache.put("container1", "prefix/", entries1);
    cache.put("container2", "prefix/", entries2);

    assertNotNull(cache.get("container1", "prefix/", "prefix/a"));
    assertNull(cache.get("container1", "prefix/", "prefix/b"));
    assertNotNull(cache.get("container2", "prefix/", "prefix/b"));
    assertNull(cache.get("container2", "prefix/", "prefix/a"));
  }

  @Test
  void testInvalidateNonExistentEntry() {
    AzureListCache cache = new AzureListCache(60_000L);
    cache.invalidate("nonexistent", "prefix/");
    cache.invalidateParentOf("nonexistent", "some/key");
  }

  @Test
  void testChildInfoFields() {
    Instant now = Instant.now();
    AzureListCache.ChildInfo info = new AzureListCache.ChildInfo(FileType.FILE, 42, now);
    assertEquals(FileType.FILE, info.type);
    assertEquals(42, info.size);
    assertEquals(now, info.lastModified);
  }

  @Test
  void testChildInfoFolder() {
    AzureListCache.ChildInfo info = new AzureListCache.ChildInfo(FileType.FOLDER, 0, Instant.EPOCH);
    assertEquals(FileType.FOLDER, info.type);
    assertEquals(0, info.size);
    assertEquals(Instant.EPOCH, info.lastModified);
  }

  @Test
  void testPutOverwritesPreviousEntry() {
    AzureListCache cache = new AzureListCache(60_000L);
    Map<String, AzureListCache.ChildInfo> entries1 = new LinkedHashMap<>();
    entries1.put("key", new AzureListCache.ChildInfo(FileType.FILE, 100, Instant.now()));
    cache.put("container", "prefix/", entries1);

    assertEquals(100, cache.get("container", "prefix/", "key").size);

    Map<String, AzureListCache.ChildInfo> entries2 = new LinkedHashMap<>();
    entries2.put("key", new AzureListCache.ChildInfo(FileType.FILE, 200, Instant.now()));
    cache.put("container", "prefix/", entries2);

    assertEquals(200, cache.get("container", "prefix/", "key").size);
  }

  @Test
  void testInvalidateOnlyAffectsTargetPrefix() {
    AzureListCache cache = new AzureListCache(60_000L);
    Map<String, AzureListCache.ChildInfo> entries1 = new LinkedHashMap<>();
    entries1.put("a/file", new AzureListCache.ChildInfo(FileType.FILE, 10, Instant.now()));
    Map<String, AzureListCache.ChildInfo> entries2 = new LinkedHashMap<>();
    entries2.put("b/file", new AzureListCache.ChildInfo(FileType.FILE, 20, Instant.now()));

    cache.put("container", "a/", entries1);
    cache.put("container", "b/", entries2);

    cache.invalidate("container", "a/");
    assertNull(cache.get("container", "a/", "a/file"));
    assertNotNull(cache.get("container", "b/", "b/file"));
  }

  @Test
  void testMultipleChildrenInSamePrefix() {
    AzureListCache cache = new AzureListCache(60_000L);
    Map<String, AzureListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("dir/file1.txt", new AzureListCache.ChildInfo(FileType.FILE, 100, Instant.now()));
    entries.put("dir/file2.txt", new AzureListCache.ChildInfo(FileType.FILE, 200, Instant.now()));
    entries.put("dir/sub/", new AzureListCache.ChildInfo(FileType.FOLDER, 0, Instant.EPOCH));

    cache.put("container", "dir/", entries);

    assertNotNull(cache.get("container", "dir/", "dir/file1.txt"));
    assertNotNull(cache.get("container", "dir/", "dir/file2.txt"));
    assertNotNull(cache.get("container", "dir/", "dir/sub/"));
    assertEquals(100, cache.get("container", "dir/", "dir/file1.txt").size);
    assertEquals(200, cache.get("container", "dir/", "dir/file2.txt").size);
    assertEquals(FileType.FOLDER, cache.get("container", "dir/", "dir/sub/").type);
  }

  @Test
  void testRootPrefixEmpty() {
    AzureListCache cache = new AzureListCache(60_000L);
    Map<String, AzureListCache.ChildInfo> entries = new LinkedHashMap<>();
    entries.put("topfile.txt", new AzureListCache.ChildInfo(FileType.FILE, 50, Instant.now()));
    cache.put("container", "", entries);

    assertNotNull(cache.get("container", "", "topfile.txt"));
  }
}
