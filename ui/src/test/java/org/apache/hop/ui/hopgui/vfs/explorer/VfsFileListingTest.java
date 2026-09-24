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

package org.apache.hop.ui.hopgui.vfs.explorer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.core.vfs.HopVfsFileDialog;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.GenericFileType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class VfsFileListingTest {

  @AfterEach
  void tearDown() {
    HopVfs.reset();
  }

  @Test
  void ramFileHasNameExtensionAndSizeButNoOwner() throws Exception {
    FileObject folder = HopVfs.getFileObject("ram:///vfs-explorer/");
    folder.createFolder();
    FileObject notes = folder.resolveFile("notes.txt");
    try (OutputStream out = notes.getContent().getOutputStream()) {
      out.write(new byte[] {1, 2, 3, 4, 5});
    }
    folder.resolveFile(".secret").createFile();
    folder.resolveFile("incoming").createFolder();
    folder.refresh();

    List<VfsFileRow> rows = VfsFileListing.childrenOf(folder);
    VfsFileRow notesRow = find(rows, "notes.txt");
    assertEquals("txt", notesRow.getExtension());
    assertEquals(5L, notesRow.getSize());
    assertEquals("5", notesRow.getSizeText());
    assertEquals("", notesRow.getOwner());
    assertEquals("", notesRow.getPermissions());
    assertFalse(notesRow.isFolder());
    assertTrue(find(rows, "incoming").isFolder());
    assertEquals("", find(rows, "incoming").getSizeText());

    List<VfsFileRow> visible =
        VfsFileListing.visible(rows, false, false, "note", VfsFileColumn.NAME, true);
    assertEquals(List.of("notes.txt"), visible.stream().map(VfsFileRow::getName).toList());
    assertTrue(
        VfsFileListing.visible(rows, false, false, "", VfsFileColumn.NAME, true).stream()
            .noneMatch(row -> row.getName().startsWith(".")));
    assertTrue(
        VfsFileListing.visible(rows, true, true, "", VfsFileColumn.NAME, true).stream()
            .anyMatch(row -> ".secret".equals(row.getName())));
  }

  @Test
  void hiddenFoldersAndHiddenFilesAreIndependent() {
    List<VfsFileRow> rows =
        List.of(
            row(".git", "ram:///.git", true, VfsFileRow.UNKNOWN),
            row(".secret", "ram:///.secret", false, 3),
            row("notes.txt", "ram:///notes.txt", false, 5));

    List<String> filesOnly =
        VfsFileListing.visible(rows, false, true, "", VfsFileColumn.NAME, true).stream()
            .map(VfsFileRow::getName)
            .toList();
    assertEquals(List.of(".secret", "notes.txt"), filesOnly);

    List<String> foldersOnly =
        VfsFileListing.visible(rows, true, false, "", VfsFileColumn.NAME, true).stream()
            .map(VfsFileRow::getName)
            .toList();
    assertEquals(List.of(".git", "notes.txt"), foldersOnly);
  }

  @Test
  void treeFoldersAreAlphabeticalIgnoringCase() {
    List<VfsFileRow> rows =
        List.of(
            row("Zebra", "ram:///Zebra", true, 0),
            row("notes.txt", "ram:///notes.txt", false, 1),
            row("apple", "ram:///apple", true, 0),
            row(".git", "ram:///.git", true, 0),
            row("Banana", "ram:///Banana", true, 0));

    assertEquals(
        List.of("apple", "Banana", "Zebra"),
        VfsFileListing.foldersForTree(rows, false).stream().map(VfsFileRow::getName).toList());
    assertEquals(
        List.of(".git", "apple", "Banana", "Zebra"),
        VfsFileListing.foldersForTree(rows, true).stream().map(VfsFileRow::getName).toList());
  }

  @Test
  void sortIsTotalWhenSizeAndDateAreMissing() {
    VfsFileRow first = row("b.txt", "ram:///b.txt", false, VfsFileRow.UNKNOWN);
    VfsFileRow second = row("a.txt", "ram:///a.txt", false, VfsFileRow.UNKNOWN);
    assertNotEquals(0, VfsFileListing.compare(first, second, VfsFileColumn.SIZE, true));
    assertNotEquals(0, VfsFileListing.compare(first, second, VfsFileColumn.MODIFIED, true));
    assertEquals(
        -VfsFileListing.compare(first, second, VfsFileColumn.SIZE, true),
        VfsFileListing.compare(second, first, VfsFileColumn.SIZE, true));

    VfsFileRow sameName = row("a.txt", "ram:///a.txt", false, VfsFileRow.UNKNOWN);
    VfsFileRow sameNameAgain = row("a.txt", "ram:///a.txt", false, VfsFileRow.UNKNOWN);
    assertNotEquals(0, VfsFileListing.compare(sameName, sameNameAgain, VfsFileColumn.NAME, true));
    assertEquals(0, VfsFileListing.compare(sameName, sameName, VfsFileColumn.NAME, false));

    VfsFileRow folder = row("z", "ram:///z", true, VfsFileRow.UNKNOWN);
    VfsFileRow file = row("a.txt", "ram:///a.txt", false, 10);
    assertTrue(VfsFileListing.compare(folder, file, VfsFileColumn.SIZE, false) < 0);
    assertTrue(VfsFileListing.compare(file, folder, VfsFileColumn.NAME, true) > 0);
  }

  @Test
  void staleGenerationDoesNotReplaceRows() {
    VfsListingGeneration generation = new VfsListingGeneration();
    int first = generation.next();
    List<VfsFileRow> current = List.of(row("current", "ram:///current", false, 1));
    int second = generation.next();
    assertFalse(generation.isCurrent(first));
    assertSame(
        current,
        VfsFileListing.publish(
            generation, first, current, List.of(row("late", "ram:///late", false, 1))));
    List<VfsFileRow> incoming = List.of(row("fresh", "ram:///fresh", false, 1));
    assertSame(incoming, VfsFileListing.publish(generation, second, current, incoming));
  }

  @Test
  void fileTypeMatchDoesNotReadContent() throws Exception {
    IHopFileType match = mock(IHopFileType.class);
    when(match.isHandledBy("sales.hpl", false)).thenReturn(true);
    when(match.isHandledBy(anyString(), eq(true))).thenThrow(new HopException("content"));
    IHopFileType miss = mock(IHopFileType.class);
    when(miss.isHandledBy(anyString(), eq(false))).thenReturn(false);
    when(miss.isHandledBy(anyString(), eq(true))).thenThrow(new HopException("content"));

    assertSame(
        match,
        VfsHopFileTypes.findByExtension(
            List.of(new GenericFileType(), miss, match), "sales.hpl", false));
    verify(match, never()).isHandledBy(anyString(), eq(true));
    verify(miss, never()).isHandledBy(anyString(), eq(true));
  }

  @Test
  void archiveBrowseUrisStayWithTheFileDialogHelpers() {
    assertEquals(
        "zip:/tmp/data.zip!/", HopVfsFileDialog.buildArchiveBrowseUri("zip", "/tmp/data.zip"));
    assertEquals(
        "jar:zip:file:///tmp/outer.zip!/nested.jar!/",
        HopVfsFileDialog.buildArchiveBrowseUri("jar", "zip:file:///tmp/outer.zip!/nested.jar"));
  }

  private static VfsFileRow find(List<VfsFileRow> rows, String name) {
    return rows.stream().filter(row -> name.equals(row.getName())).findFirst().orElseThrow();
  }

  private static VfsFileRow row(String name, String uri, boolean folder, long size) {
    return new VfsFileRow(name, uri, folder, "", size, "", VfsFileRow.UNKNOWN, "", "", "");
  }
}
