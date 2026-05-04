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
package org.apache.hop.vfs.webdav;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Test;

class HopWebDavConnectionFileNameParserTest {

  @Test
  void buildSyntheticUri_appendsPathUnderRoot() throws Exception {
    assertEquals(
        "webdav4://localhost/remote.php/dav/files/admin/Documents/x.txt",
        HopWebDavConnectionFileNameParser.buildSyntheticUri(
            "myconn:///Documents/x.txt", "webdav4://localhost/remote.php/dav/files/admin/"));
  }

  @Test
  void buildSyntheticUri_rootOnly() throws Exception {
    assertEquals(
        "webdav4s://cloud.example/remote.php/dav/files/user/",
        HopWebDavConnectionFileNameParser.buildSyntheticUri(
            "myconn:///", "webdav4s://cloud.example/remote.php/dav/files/user"));
  }

  @Test
  void buildSyntheticUri_rejectsBadRoot() {
    assertThrows(
        FileSystemException.class,
        () ->
            HopWebDavConnectionFileNameParser.buildSyntheticUri(
                "myconn:///a", "http://localhost/remote.php/dav/files/admin/"));
  }

  @Test
  void wirePathPrefixFromRootUrl_normalizesSlash() throws Exception {
    assertEquals(
        "/remote.php/dav/files/admin/",
        HopWebDavLogicalUris.wirePathPrefixFromRootUrl(
            "webdav4://localhost/remote.php/dav/files/admin"));
  }

  @Test
  void relativePathFromWirePaths_stripsRootPrefix() {
    assertEquals(
        "Documents/x.txt",
        HopWebDavLogicalUris.relativePathFromWirePaths(
            "/remote.php/dav/files/admin/", "/remote.php/dav/files/admin/Documents/x.txt"));
  }
}
