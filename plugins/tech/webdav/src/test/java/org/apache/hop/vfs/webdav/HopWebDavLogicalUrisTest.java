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

import org.junit.jupiter.api.Test;

class HopWebDavLogicalUrisTest {

  @Test
  void rawPathFromUri_acceptsSpacesAndExtraSlashesFromVfsResolveName() throws Exception {
    assertEquals(
        "/remote.php/dav/files/admin/New document.docx",
        HopWebDavLogicalUris.rawPathFromUri(
            "nextcloud:////remote.php/dav/files/admin/New document.docx"));
    assertEquals(
        "/Documents/New document.docx",
        HopWebDavLogicalUris.rawPathFromUri("myconn:///Documents/New document.docx"));
  }

  @Test
  void wirePathPrefixFromRootUrl_normalizesSlash() throws Exception {
    assertEquals(
        "/remote.php/dav/files/admin/",
        HopWebDavLogicalUris.wirePathPrefixFromRootUrl(
            "webdav4://localhost/remote.php/dav/files/admin"));
  }

  @Test
  void wirePathCombine_appendsRelativeUnderPrefix() {
    assertEquals(
        "/remote.php/dav/files/admin/Documents/x.txt",
        HopWebDavFileSystem.wirePathCombine("/remote.php/dav/files/admin/", "Documents/x.txt"));
  }

  @Test
  void effectiveRelativeSegment_stripsDuplicateOfRootPrefix() {
    assertEquals(
        "",
        HopWebDavFileSystem.effectiveRelativeSegment(
            "/remote.php/dav/files/admin/", "/remote.php/dav/files/admin"));
    assertEquals(
        "",
        HopWebDavFileSystem.effectiveRelativeSegment(
            "/remote.php/dav/files/admin/", "/remote.php/dav/files/admin/"));
  }

  @Test
  void effectiveRelativeSegment_keepsSuffixBeyondRootPrefix() {
    assertEquals(
        "Documents",
        HopWebDavFileSystem.effectiveRelativeSegment(
            "/remote.php/dav/files/admin/", "/remote.php/dav/files/admin/Documents"));
    assertEquals(
        "Documents/x.txt",
        HopWebDavFileSystem.effectiveRelativeSegment(
            "/remote.php/dav/files/admin/", "/remote.php/dav/files/admin/Documents/x.txt"));
  }

  @Test
  void effectiveRelativeSegment_forHostRootPrefix_passesThroughLogicalPath() {
    assertEquals(
        "remote.php/dav/files/admin",
        HopWebDavFileSystem.effectiveRelativeSegment("/", "/remote.php/dav/files/admin/"));
  }
}
