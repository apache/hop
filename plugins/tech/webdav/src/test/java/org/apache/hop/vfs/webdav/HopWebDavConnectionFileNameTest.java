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

import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileName;
import org.junit.jupiter.api.Test;

class HopWebDavConnectionFileNameTest {

  @Test
  void getUri_usesLogicalConnectionScheme() {
    Webdav4FileName wire =
        new Webdav4FileName(
            "webdav4",
            "localhost",
            80,
            80,
            "u",
            "p",
            "/remote.php/dav/files/admin/Documents/x.txt",
            FileType.FILE,
            null,
            false);
    HopWebDavConnectionFileName n =
        new HopWebDavConnectionFileName(
            wire, "nextcloud", "Documents/x.txt", "/remote.php/dav/files/admin/");
    assertEquals("nextcloud:///Documents/x.txt", n.getURI());
    assertEquals("nextcloud:///", n.getRootURI());
  }

  @Test
  void createName_childCarriesLogicalUri() {
    Webdav4FileName wire =
        new Webdav4FileName(
            "webdav4",
            "localhost",
            80,
            80,
            "u",
            "p",
            "/remote.php/dav/files/admin/Documents",
            FileType.FOLDER,
            null,
            true);
    HopWebDavConnectionFileName parent =
        new HopWebDavConnectionFileName(
            wire, "nextcloud", "Documents", "/remote.php/dav/files/admin/");
    HopWebDavConnectionFileName child =
        (HopWebDavConnectionFileName)
            parent.createName("/remote.php/dav/files/admin/Documents/y.bin", FileType.FILE);
    assertEquals("nextcloud:///Documents/y.bin", child.getURI());
  }
}
