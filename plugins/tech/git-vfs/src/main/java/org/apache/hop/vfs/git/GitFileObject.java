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
package org.apache.hop.vfs.git;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;

/**
 * A file in the working copy of a named git connection.
 *
 * <p>Everything here is a thin translation: a name of this file system is a path relative to the
 * base path of the connection, and {@link GitFileSystem} turns that into a real file on disk. Once
 * there, this class reads and writes that file, so the behaviour of a file behind a git connection
 * is the behaviour of a local file - which is the whole point of the driver.
 *
 * <p>The one thing which is not local is the meaning of a write. A write only changes the working
 * copy: nothing here commits it, and nothing here pushes it. Committing as each file object closes
 * would make every write its own commit, and VFS has no transaction to hang one on. A connection
 * which is not read only therefore lets a caller change the checkout, and pushing is a separate,
 * deliberate step.
 */
public class GitFileObject extends AbstractFileObject<GitFileSystem> {

  public GitFileObject(AbstractFileName name, GitFileSystem fileSystem) {
    super(name, fileSystem);
  }

  private Path file() throws FileSystemException {
    return getAbstractFileSystem().resolveToFile((GitFileName) getName());
  }

  private FileType typeOf(Path file) throws IOException {
    if (Files.isDirectory(file)) {
      return FileType.FOLDER;
    }
    if (Files.isRegularFile(file)) {
      return FileType.FILE;
    }
    // A broken symlink and a file which is not there at all both land here, and IMAGINARY is what
    // keeps exists() honest about there being nothing to read.
    return FileType.IMAGINARY;
  }

  @Override
  protected FileType doGetType() throws IOException {
    return typeOf(file());
  }

  @Override
  protected long doGetContentSize() throws IOException {
    Path file = file();
    return Files.isRegularFile(file) ? Files.size(file) : 0L;
  }

  @Override
  protected long doGetLastModifiedTime() throws IOException {
    return Files.getLastModifiedTime(file()).toMillis();
  }

  @Override
  protected boolean doIsReadable() throws IOException {
    Path file = file();
    return Files.isRegularFile(file) && Files.isReadable(file);
  }

  @Override
  protected boolean doIsWriteable() throws IOException {
    if (isReadOnly()) {
      return false;
    }
    Path file = file();
    if (Files.exists(file)) {
      return Files.isWritable(file);
    }
    // A file which is not there yet is writable when the folder holding it is: the parent is what
    // decides. AbstractFileObject.isWriteable() asks the parent for a missing file, but only after
    // this has already said what it thinks.
    Path parent = file.getParent();
    return parent != null && Files.isDirectory(parent) && Files.isWritable(parent);
  }

  @Override
  protected boolean doIsHidden() throws FileSystemException {
    // The working copy holds a .git folder. Hide that folder only: .gitignore and .gitattributes
    // are files of the project, and a name check of ".git*" would hide those too.
    Path file = file();
    Path name = file.getFileName();
    return name != null && ".git".equals(name.toString());
  }

  @Override
  protected InputStream doGetInputStream(int bufferSize) throws IOException {
    return Files.newInputStream(file());
  }

  @Override
  protected String[] doListChildren() throws IOException {
    Path folder = file();
    if (!Files.isDirectory(folder)) {
      return null;
    }
    List<String> names = new ArrayList<>();
    try (Stream<Path> children = Files.list(folder)) {
      children.forEach(
          child -> {
            String name = child.getFileName().toString();
            // .git is the repository itself, and the ready marker lives in it. Neither is a file
            // of the project a browse dialog should offer.
            if (".git".equals(name) || "hop-git-vfs-ready".equals(name)) {
              return;
            }
            names.add(name);
          });
    }
    return names.toArray(new String[0]);
  }

  @Override
  protected void doCreateFolder() throws Exception {
    checkWritable();
    Path folder = file();
    if (Files.isDirectory(folder)) {
      return;
    }
    Path parent = folder.getParent();
    if (parent != null && !Files.isDirectory(parent)) {
      Files.createDirectories(parent);
    }
    Files.createDirectory(folder);
  }

  @Override
  protected void doDelete() throws Exception {
    checkWritable();
    Path file = file();
    if (!Files.isDirectory(file)) {
      Files.deleteIfExists(file);
      return;
    }
    try (Stream<Path> walk = Files.walk(file)) {
      List<Path> entries = new ArrayList<>(walk.toList());
      // Deepest first: a folder can only go once everything under it has.
      entries.sort((a, b) -> b.getNameCount() - a.getNameCount());
      for (Path entry : entries) {
        Files.deleteIfExists(entry);
      }
    }
  }

  @Override
  protected void doRename(FileObject newFile) throws Exception {
    checkWritable();
    Path source = file();
    Path destination = ((GitFileObject) newFile).file();
    Path parent = destination.getParent();
    if (parent != null && !Files.isDirectory(parent)) {
      Files.createDirectories(parent);
    }
    Files.move(source, destination);
  }

  @Override
  protected OutputStream doGetOutputStream(boolean bAppend) throws IOException {
    checkWritable();
    Path file = file();
    Path parent = file.getParent();
    if (parent != null && !Files.isDirectory(parent)) {
      throw new FileSystemException("vfs.provider/write.error", file.toString());
    }
    if (bAppend) {
      return Files.newOutputStream(
          file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }
    return Files.newOutputStream(
        file,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING);
  }

  /**
   * Refuse the write on a read only connection.
   *
   * <p>Reported as "this file is read-only" rather than as a git error: from the caller's point of
   * view the file it asked to write is not writable, and the connection being read only is exactly
   * why.
   */
  private void checkWritable() throws FileSystemException {
    if (isReadOnly()) {
      throw new FileSystemException(
          "vfs.provider/write-read-only.error", getName().getFriendlyURI());
    }
  }

  private boolean isReadOnly() {
    return ((GitFileName) getName()).isReadOnly();
  }
}
