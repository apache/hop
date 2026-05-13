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
import org.junit.jupiter.api.Test;

class HopWebDavFileNameTest {

  @Test
  void getUri_usesConnectionScheme() {
    HopWebDavFileName n = new HopWebDavFileName("mywebdav", "/Documents/x.txt", FileType.FILE);
    assertEquals("mywebdav", n.getScheme());
    assertEquals("mywebdav:///Documents/x.txt", n.getURI());
    assertEquals("mywebdav:///", n.getRootURI());
  }

  @Test
  void createName_preservesScheme() {
    HopWebDavFileName parent = new HopWebDavFileName("mywebdav", "/Documents", FileType.FOLDER);
    HopWebDavFileName child =
        (HopWebDavFileName) parent.createName("/Documents/y.bin", FileType.FILE);
    assertEquals("mywebdav:///Documents/y.bin", child.getURI());
  }

  @Test
  void buildChild_underRoot() {
    HopWebDavFileName parent = new HopWebDavFileName("mywebdav", "/", FileType.FOLDER);
    HopWebDavFileName child = HopWebDavFileName.buildChild(parent, "Readme.md", FileType.FILE);
    assertEquals("mywebdav:///Readme.md", child.getURI());
  }

  @Test
  void buildChild_underFolder() {
    HopWebDavFileName parent = new HopWebDavFileName("mywebdav", "/Documents/", FileType.FOLDER);
    HopWebDavFileName child = HopWebDavFileName.buildChild(parent, "y.bin", FileType.FILE);
    assertEquals("mywebdav:///Documents/y.bin", child.getURI());
  }
}
