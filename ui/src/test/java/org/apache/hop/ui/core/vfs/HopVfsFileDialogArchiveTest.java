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

package org.apache.hop.ui.core.vfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.junit.jupiter.api.Test;

class HopVfsFileDialogArchiveTest {

  @Test
  void getArchiveSchemeRecognizesSupportedExtensions() {
    assertEquals("zip", HopVfsFileDialog.getArchiveScheme("archive.zip"));
    assertEquals("zip", HopVfsFileDialog.getArchiveScheme("/home/matt/Downloads/archive.ZIP"));
    assertEquals("jar", HopVfsFileDialog.getArchiveScheme("lib.jar"));
    assertEquals("jar", HopVfsFileDialog.getArchiveScheme("app.war"));
    assertEquals("jar", HopVfsFileDialog.getArchiveScheme("module.ear"));
    assertEquals("tar", HopVfsFileDialog.getArchiveScheme("data.tar"));
    assertEquals("tgz", HopVfsFileDialog.getArchiveScheme("data.tgz"));
    assertEquals("tgz", HopVfsFileDialog.getArchiveScheme("data.tar.gz"));
    assertEquals("tbz2", HopVfsFileDialog.getArchiveScheme("data.tbz2"));
    assertEquals("tbz2", HopVfsFileDialog.getArchiveScheme("data.tar.bz2"));
  }

  @Test
  void getArchiveSchemeUsesBasenameFromPathAndNestedUri() {
    assertEquals("zip", HopVfsFileDialog.getArchiveScheme("/tmp/folder/file.zip"));
    assertEquals("jar", HopVfsFileDialog.getArchiveScheme("zip:file:///tmp/outer.zip!/nested.jar"));
  }

  @Test
  void getArchiveSchemeRejectsNonDrillableFiles() {
    assertNull(HopVfsFileDialog.getArchiveScheme(null));
    assertNull(HopVfsFileDialog.getArchiveScheme(""));
    assertNull(HopVfsFileDialog.getArchiveScheme("readme.txt"));
    assertNull(HopVfsFileDialog.getArchiveScheme("data.gz"));
    assertNull(HopVfsFileDialog.getArchiveScheme("data.bz2"));
    assertNull(HopVfsFileDialog.getArchiveScheme("archive.7z"));
    assertNull(HopVfsFileDialog.getArchiveScheme("archive.rar"));
  }

  @Test
  void buildArchiveBrowseUriFromSchemeAndPath() {
    assertEquals(
        "zip:/home/matt/Downloads/archive.zip!/",
        HopVfsFileDialog.buildArchiveBrowseUri("zip", "/home/matt/Downloads/archive.zip"));
    assertEquals(
        "jar:zip:file:///tmp/outer.zip!/nested.jar!/",
        HopVfsFileDialog.buildArchiveBrowseUri("jar", "zip:file:///tmp/outer.zip!/nested.jar"));
    assertNull(HopVfsFileDialog.buildArchiveBrowseUri(null, "/tmp/a.zip"));
    assertNull(HopVfsFileDialog.buildArchiveBrowseUri("zip", null));
    assertNull(HopVfsFileDialog.buildArchiveBrowseUri("", "/tmp/a.zip"));
  }

  @Test
  void buildArchiveBrowseUriFromNestedFileObject() {
    FileObject nestedJar = mock(FileObject.class);
    FileName name = mock(FileName.class);
    when(nestedJar.getName()).thenReturn(name);
    when(name.getBaseName()).thenReturn("nested.jar");
    when(name.getRootURI()).thenReturn("zip:file:///tmp/outer.zip!/");
    when(name.getURI()).thenReturn("zip:file:///tmp/outer.zip!/nested.jar");

    assertEquals(
        "jar:zip:file:///tmp/outer.zip!/nested.jar!/",
        HopVfsFileDialog.buildArchiveBrowseUri(nestedJar));
  }

  @Test
  void buildArchiveBrowseUriRejectsNonArchives() {
    assertNull(HopVfsFileDialog.buildArchiveBrowseUri(null));

    FileObject textFile = mock(FileObject.class);
    FileName name = mock(FileName.class);
    when(textFile.getName()).thenReturn(name);
    when(name.getBaseName()).thenReturn("readme.txt");
    when(name.getRootURI()).thenReturn("file:///");

    assertNull(HopVfsFileDialog.buildArchiveBrowseUri(textFile));
  }
}
