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
 *
 */

package org.apache.hop.vfs.minio;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Deleting a folder reads and removes nothing outside that folder. It used to list the top level of
 * the bucket to remove it, which only a lazy, never-sent request kept from happening. See issue
 * #9762.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class MinioFolderDeleteTest {

  private static final String SCHEME = "store";
  private static final String BUCKET = "data";
  private static final byte[] CONTENT = "content".getBytes(StandardCharsets.UTF_8);
  private static final byte[] FOLDER_MARKER = new byte[0];

  private FakeS3Server s3;
  private DefaultFileSystemManager manager;

  @BeforeEach
  void setUp() throws Exception {
    s3 = new FakeS3Server(BUCKET);
    s3.objects.put("keep-1.txt", CONTENT);
    s3.objects.put("keep-2.txt", CONTENT);
    s3.objects.put("other/", FOLDER_MARKER);
    manager = s3.fileSystemManager(SCHEME);
  }

  @AfterEach
  void tearDown() {
    if (manager != null) {
      manager.close();
    }
    if (s3 != null) {
      s3.stop();
    }
  }

  @Test
  void deletingAnEmptyFolderLeavesTheRestOfTheBucket() throws Exception {
    s3.objects.put("empty/", FOLDER_MARKER);

    assertTrue(manager.resolveFile(SCHEME + ":///" + BUCKET + "/empty").delete());

    assertTheRestOfTheBucketIsLeft("empty/");
  }

  @Test
  void deletingANestedFolderLeavesTheRestOfTheBucket() throws Exception {
    s3.objects.put("docs/", FOLDER_MARKER);
    s3.objects.put("docs/report.txt", CONTENT);
    s3.objects.put("docs/sub/", FOLDER_MARKER);
    s3.objects.put("docs/sub/a.txt", CONTENT);

    FileObject docs = manager.resolveFile(SCHEME + ":///" + BUCKET + "/docs");
    docs.deleteAll();

    assertFalse(s3.objects.containsKey("docs/report.txt"), "docs/report.txt deleted");
    assertFalse(s3.objects.containsKey("docs/sub/a.txt"), "docs/sub/a.txt deleted");
    assertTheRestOfTheBucketIsLeft("docs/");
  }

  private void assertTheRestOfTheBucketIsLeft(String folderPrefix) {
    for (String key : new String[] {"keep-1.txt", "keep-2.txt", "other/"}) {
      assertTrue(s3.objects.containsKey(key), key + " is still there");
    }
    for (String prefix : s3.listedPrefixes) {
      assertTrue(
          prefix.startsWith(folderPrefix),
          "deleting " + folderPrefix + " listed '" + prefix + "': " + s3.listedPrefixes);
    }
    assertTrue(s3.unexpected.isEmpty(), "unexpected requests: " + s3.unexpected);
  }
}
