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

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.vfs.webdav.metadata.WebDavConnection;
import org.junit.jupiter.api.Test;

class HopWebDavFileNameParserTest {

  @Test
  void parseRoot() throws Exception {
    WebDavConnection c = new WebDavConnection();
    c.setName("mywebdav");
    HopWebDavFileNameParser p = new HopWebDavFileNameParser(c);
    FileName n = p.parseUri(null, null, "mywebdav:///");
    assertEquals("mywebdav:///", n.getURI());
    assertEquals(FileType.FOLDER, n.getType());
  }

  @Test
  void parseFile_withSpaceInName() throws Exception {
    WebDavConnection c = new WebDavConnection();
    c.setName("myconn");
    HopWebDavFileNameParser p = new HopWebDavFileNameParser(c);
    FileName n = p.parseUri(null, null, "myconn:///Documents/New document.docx");
    assertEquals("myconn:///Documents/New document.docx", n.getURI());
    assertEquals(FileType.FILE, n.getType());
  }

  @Test
  void parseFile() throws Exception {
    WebDavConnection c = new WebDavConnection();
    c.setName("myconn");
    HopWebDavFileNameParser p = new HopWebDavFileNameParser(c);
    FileName n = p.parseUri(null, null, "myconn:///Documents/x.txt");
    assertEquals("myconn:///Documents/x.txt", n.getURI());
    assertEquals(FileType.FILE, n.getType());
  }

  @Test
  void wrongSchemeThrows() {
    WebDavConnection c = new WebDavConnection();
    c.setName("a");
    HopWebDavFileNameParser p = new HopWebDavFileNameParser(c);
    assertThrows(FileSystemException.class, () -> p.parseUri(null, null, "b:///x"));
  }
}
