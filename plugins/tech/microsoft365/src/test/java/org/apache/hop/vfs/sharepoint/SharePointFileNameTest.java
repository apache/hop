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

package org.apache.hop.vfs.sharepoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.junit.jupiter.api.Test;

class SharePointFileNameTest {

  private static SharePointFileName parse(String uri) throws FileSystemException {
    return (SharePointFileName) SharePointFileNameParser.getInstance().parseUri(null, null, uri);
  }

  @Test
  void pluginHasNoFixedScheme() {
    SharePointVfsPlugin plugin = new SharePointVfsPlugin();
    assertEquals(0, plugin.getUrlSchemes().length);
    assertNull(plugin.getProvider());
  }

  @Test
  void parsesNamedConnectionUri() throws Exception {
    SharePointFileName name = parse("m365:///Finance/Documents/reports/2026/daily.csv");
    assertEquals("m365", name.getScheme());
    assertEquals("/Finance/Documents/reports/2026/daily.csv", name.getPath());
    assertEquals(FileType.FILE, name.getType());
    assertEquals("m365:///", name.getRootURI());
    assertEquals("Finance", name.getSiteName());
    assertEquals("Documents", name.getDriveName());
    assertEquals("reports/2026/daily.csv", name.getEncodedItemPath());
    assertTrue(name.isItem());
  }

  @Test
  void parsesRoot() throws Exception {
    for (String uri : new String[] {"m365://", "m365:///", "m365:/"}) {
      SharePointFileName name = parse(uri);
      assertEquals("/", name.getPath());
      assertEquals(FileType.FOLDER, name.getType());
      assertEquals(0, name.getLevel());
      assertNull(name.getSiteName());
      assertNull(name.getDriveName());
      assertFalse(name.isItem());
    }
  }

  @Test
  void sitesAndLibrariesAreNotItems() throws Exception {
    SharePointFileName site = parse("m365:///Finance");
    assertEquals("Finance", site.getSiteName());
    assertNull(site.getDriveName());
    assertFalse(site.isItem());

    SharePointFileName library = parse("m365:///Finance/Shared Documents/");
    assertEquals("/Finance/Shared Documents", library.getPath());
    assertEquals("Shared Documents", library.getDriveName());
    assertEquals("", library.getEncodedItemPath());
    assertFalse(library.isItem());
  }

  @Test
  void encodesItemPathForGraph() throws Exception {
    SharePointFileName name = parse("m365:///Finance/Documents/Q1 report/50%25 d'été #2+.csv");
    // VFS keeps a literal percent sign escaped in the path
    assertEquals("/Finance/Documents/Q1 report/50%25 d'été #2+.csv", name.getPath());
    assertEquals("50% d'été #2+.csv", name.getDecodedBaseName());
    assertEquals(4, name.getLevel());
    assertEquals("Q1%20report/50%25%20d%27%C3%A9t%C3%A9%20%232%2B.csv", name.getEncodedItemPath());
  }

  @Test
  void resolvesAgainstBase() throws Exception {
    SharePointFileName base = parse("m365:///Finance/Documents");
    SharePointFileNameParser parser =
        (SharePointFileNameParser) SharePointFileNameParser.getInstance();

    SharePointFileName relative = (SharePointFileName) parser.parseUri(null, base, "a/b.txt");
    assertEquals("m365", relative.getScheme());
    assertEquals("/Finance/Documents/a/b.txt", relative.getPath());

    SharePointFileName absolute =
        (SharePointFileName) parser.parseUri(null, base, "/Other/Lib/c.txt");
    assertEquals("/Other/Lib/c.txt", absolute.getPath());
  }

  @Test
  void relativeUriNeedsBase() {
    assertThrows(FileSystemException.class, () -> parse("no/scheme.txt"));
  }
}
