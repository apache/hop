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
package org.apache.hop.core.fileinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileInputListTest {

  /**
   * Builds a deterministic tree used by the filter/option tests:
   *
   * <pre>
   *   root/
   *     file-a.txt
   *     file-b.txt
   *     data.csv
   *     folder-one/nested-1.txt
   *     folder-two/nested-2.txt
   * </pre>
   */
  private static void buildTree(Path root) throws IOException {
    Files.createDirectories(root.resolve("folder-one"));
    Files.createDirectories(root.resolve("folder-two"));
    Files.writeString(root.resolve("file-a.txt"), "alpha");
    Files.writeString(root.resolve("file-b.txt"), "beta");
    Files.writeString(root.resolve("data.csv"), "c,d");
    Files.writeString(root.resolve("folder-one/nested-1.txt"), "keep");
    Files.writeString(root.resolve("folder-two/nested-2.txt"), "keep");
  }

  private static FileInputList list(
      IVariables variables,
      Path root,
      FileTypeFilter filter,
      String mask,
      String excludeMask,
      boolean subFolders) {
    return FileInputList.createFileList(
        variables,
        new String[] {root.toAbsolutePath().toString()},
        new String[] {mask},
        new String[] {excludeMask},
        new String[] {"N"},
        new boolean[] {subFolders},
        new FileTypeFilter[] {filter});
  }

  private static List<String> baseNames(FileInputList list) {
    List<String> baseNames = new ArrayList<>();
    for (FileObject file : list.getFiles()) {
      baseNames.add(file.getName().getBaseName());
    }
    return baseNames;
  }

  @Test
  void testGetUrlStrings() {

    String sFileA = "hdfs://myfolderA/myfileA.txt";
    String sFileB = "file:///myfolderB/myfileB.txt";

    FileObject fileA = mock(FileObject.class);
    FileObject fileB = mock(FileObject.class);

    when(fileA.getPublicURIString()).thenReturn(sFileA);
    when(fileB.getPublicURIString()).thenReturn(sFileB);

    FileInputList fileInputList = new FileInputList();
    fileInputList.addFile(fileA);
    fileInputList.addFile(fileB);
    String[] result = fileInputList.getUrlStrings();
    assertEquals(2, result.length);
    assertEquals(sFileA, result[0]);
    assertEquals(sFileB, result[1]);
  }

  /**
   * Regression test for the "only folders" file type filter being silently downgraded to "only
   * files". The filter is carried on each {@link InputFile} (it is dropped from the {@code
   * FileTypeFilter[]} argument when the list is built from the String[] overloads used by Get File
   * Names), so it has to be honored even when that array is null. Before the fix only the plain
   * file was returned instead of the sub-folder.
   */
  @Test
  void onlyFoldersFilterIsHonoredThroughStringArrayOverload(@TempDir Path tempDir)
      throws IOException {
    buildTree(tempDir);
    List<String> names =
        baseNames(list(new Variables(), tempDir, FileTypeFilter.ONLY_FOLDERS, "", "", false));
    assertEquals(2, names.size(), "Expected only the sub-folders, got: " + names);
    assertTrue(names.contains("folder-one") && names.contains("folder-two"));
    assertFalse(names.contains("file-a.txt"), "A file leaked through the only-folders filter");
  }

  @Test
  void onlyFilesFilterIsHonoredThroughStringArrayOverload(@TempDir Path tempDir)
      throws IOException {
    buildTree(tempDir);
    List<String> names =
        baseNames(list(new Variables(), tempDir, FileTypeFilter.ONLY_FILES, "", "", false));
    assertEquals(3, names.size(), "Expected only the top level files, got: " + names);
    assertTrue(
        names.contains("file-a.txt") && names.contains("file-b.txt") && names.contains("data.csv"));
    assertFalse(names.contains("folder-one"));
  }

  @Test
  void filesAndFoldersFilterReturnsBothThroughStringArrayOverload(@TempDir Path tempDir)
      throws IOException {
    buildTree(tempDir);
    List<String> names =
        baseNames(list(new Variables(), tempDir, FileTypeFilter.FILES_AND_FOLDERS, "", "", false));
    assertEquals(5, names.size(), "Expected the files and the folders, got: " + names);
  }

  @Test
  void includeMaskMatchesOnlyTxtFiles(@TempDir Path tempDir) throws IOException {
    buildTree(tempDir);
    List<String> names =
        baseNames(list(new Variables(), tempDir, FileTypeFilter.ONLY_FILES, ".*\\.txt", "", false));
    assertEquals(2, names.size(), "Expected the two .txt files, got: " + names);
    assertTrue(names.contains("file-a.txt") && names.contains("file-b.txt"));
    assertFalse(names.contains("data.csv"));
  }

  @Test
  void excludeMaskRemovesMatchingFiles(@TempDir Path tempDir) throws IOException {
    buildTree(tempDir);
    List<String> names =
        baseNames(list(new Variables(), tempDir, FileTypeFilter.ONLY_FILES, "", ".*\\.csv", false));
    assertEquals(2, names.size(), "Expected the csv to be excluded, got: " + names);
    assertFalse(names.contains("data.csv"));
  }

  @Test
  void includeSubFoldersWalksNestedFiles(@TempDir Path tempDir) throws IOException {
    buildTree(tempDir);
    List<String> names =
        baseNames(list(new Variables(), tempDir, FileTypeFilter.ONLY_FILES, "", "", true));
    assertEquals(5, names.size(), "Expected top level and nested files, got: " + names);
    assertTrue(names.contains("nested-1.txt") && names.contains("nested-2.txt"));
  }

  @Test
  void requiredMissingFolderIsReportedAsNonExistent(@TempDir Path tempDir) {
    IVariables variables = new Variables();
    FileInputList list =
        FileInputList.createFileList(
            variables,
            new String[] {tempDir.resolve("does-not-exist").toAbsolutePath().toString()},
            new String[] {""},
            new String[] {""},
            new String[] {"Y"},
            new boolean[] {false},
            new FileTypeFilter[] {FileTypeFilter.ONLY_FILES});
    assertEquals(0, list.nrOfFiles());
    assertEquals(1, list.getNonExistentFiles().size());
    assertEquals(1, list.nrOfMissingFiles());
  }

  @Test
  void blankFileNameIsSkipped() {
    IVariables variables = new Variables();
    FileInputList list =
        FileInputList.createFileList(
            variables,
            new String[] {""},
            new String[] {""},
            new String[] {""},
            new String[] {"N"},
            new boolean[] {false},
            new FileTypeFilter[] {FileTypeFilter.ONLY_FILES});
    assertEquals(0, list.nrOfFiles());
    assertEquals(0, list.nrOfMissingFiles());
  }

  @Test
  void nullFilterArrayDefaultsToOnlyFiles(@TempDir Path tempDir) throws IOException {
    buildTree(tempDir);
    // No filter information at all (array null, InputFile filter null) must default to files only.
    IVariables variables = new Variables();
    FileInputList list =
        FileInputList.createFileList(
            variables,
            new String[] {tempDir.toAbsolutePath().toString()},
            new String[] {""},
            new String[] {""},
            new String[] {"N"},
            new boolean[] {false});
    List<String> names = baseNames(list);
    assertEquals(3, names.size(), "Default (no filter) should return files only, got: " + names);
    assertFalse(names.contains("folder-one"));
  }

  @Test
  void createFolderListReturnsOnlySubFolders(@TempDir Path tempDir) throws IOException {
    buildTree(tempDir);
    IVariables variables = new Variables();
    FileInputList list =
        FileInputList.createFolderList(
            variables, new String[] {tempDir.toAbsolutePath().toString()}, new String[] {"N"});
    List<String> names = baseNames(list);
    assertEquals(2, names.size(), "createFolderList should return only folders, got: " + names);
    assertTrue(names.contains("folder-one") && names.contains("folder-two"));
  }
}
