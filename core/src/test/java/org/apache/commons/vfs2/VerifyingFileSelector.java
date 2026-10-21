/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.vfs2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.util.FileObjectUtils;
import org.junit.jupiter.api.Assertions;

/** A file selector that asserts that all files are visited, in the correct order. */
public class VerifyingFileSelector extends Assertions implements FileSelector {

  private final FileInfo rootFile;
  private final List<FileObject> files = new ArrayList<>();

  private FileInfo currentFolderInfo;
  private FileObject currentFolder;
  private Set<String> children;
  private final List<Set<String>> stack = new ArrayList<>();

  public VerifyingFileSelector(final FileInfo fileInfo) {
    rootFile = fileInfo;
    children = new HashSet<>();
    children.add(rootFile.baseName);
  }

  /**
   * Asserts that the selector has seen all the files.
   *
   * @return The files in the order they were visited.
   */
  public List<FileObject> finish() {
    assertEquals(0, children.size());
    return files;
  }

  /** Finds a child of the current folder. */
  private FileInfo getChild(final String baseName) {
    if (currentFolderInfo == null) {
      assertEquals(rootFile.baseName, baseName);
      return rootFile;
    }
    final FileInfo child = currentFolderInfo.children.get(baseName);
    assertNotNull(child, "Could not locate child " + baseName);
    return child;
  }

  /** Determines if a file or folder should be selected. */
  @Override
  public boolean includeFile(final FileSelectInfo fileInfo) throws FileSystemException {
    final FileObject file = fileInfo.getFile();
    if (file == currentFolder) {
      // Pop current folder
      assertEquals(0, children.size());
      currentFolder = currentFolder.getParent();
      currentFolderInfo = currentFolderInfo.getParent();
      children = stack.remove(0);
    }

    final String baseName = file.getName().getBaseName();

    final FileInfo childInfo = getChild(baseName);
    assertSame(childInfo.type, file.getType());

    final boolean isChild = children.remove(baseName);
    assertTrue(isChild);

    files.add(file);
    return true;
  }

  /** Determines whether a folder should be traversed. */
  @Override
  public boolean traverseDescendents(final FileSelectInfo fileInfo) throws FileSystemException {
    // Check that the given file is a folder
    final FileObject folder = FileObjectUtils.getAbstractFileObject(fileInfo.getFile());
    assertSame(FileType.FOLDER, folder.getType());
    assertTrue(folder.isFolder());

    // Locate the info for the folder
    final String baseName = folder.getName().getBaseName();
    if (currentFolder == null) {
      assertEquals(rootFile.baseName, baseName);
      currentFolderInfo = rootFile;
    } else {
      final AbstractFileObject parent = FileObjectUtils.getAbstractFileObject(folder.getParent());
      assertSame(currentFolder, parent);

      // Locate the info for the child, and make sure it is folder
      currentFolderInfo = getChild(baseName);
      assertSame(FileType.FOLDER, currentFolderInfo.type);
    }

    // Push the folder
    stack.add(0, children);
    children = new HashSet<>(currentFolderInfo.children.keySet());
    currentFolder = folder;

    return true;
  }
}
