/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.commons.vfs2.provider.local.LocalFileName;
import org.apache.commons.vfs2.util.URIUtils;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/** A failed or cancelled traversal never returns a partial snapshot. */
public class VfsFileScanner {
  private final FileObject root;
  private final boolean recursive;
  private final Pattern include;
  private final Pattern exclude;
  private final int maximumEntries;
  private final BooleanSupplier stopped;

  public VfsFileScanner(
      FileObject root,
      boolean recursive,
      String include,
      String exclude,
      int maximumEntries,
      BooleanSupplier stopped) {
    this.root = root;
    this.recursive = recursive;
    this.include = include == null || include.isEmpty() ? null : Pattern.compile(include);
    this.exclude = exclude == null || exclude.isEmpty() ? null : Pattern.compile(exclude);
    this.maximumEntries = maximumEntries;
    this.stopped = stopped;
  }

  public Map<String, FileState> snapshot() throws IOException {
    root.refresh();
    if (!root.exists() || !root.isFolder() || !root.isReadable()) {
      throw new IOException(
          "Watch root is missing, unreadable or not a directory: " + HopVfs.getFriendlyURI(root));
    }
    Map<String, FileState> files = new HashMap<>();
    Deque<String> directories = new ArrayDeque<>();
    directories.add(".");
    Set<String> visited = new HashSet<>();
    while (!directories.isEmpty()) {
      try (FileObject directory = resolveRelative(directories.removeFirst())) {
        scan(directory, files, visited, directories);
      }
    }
    return files;
  }

  private void scan(
      FileObject directory,
      Map<String, FileState> files,
      Set<String> visited,
      Deque<String> directories)
      throws IOException {
    checkStopped();
    if (!visited.add(directory.getName().getURI())) {
      return;
    }
    if (visited.size() > maximumEntries) {
      throw new WatchLimitException("Directory entry limit exceeded. Increase maximum entries.");
    }
    directory.refresh();
    if (!directory.exists()
        || !directory.isFolder()
        || !directory.isReadable()
        || isLink(directory)) {
      throw new IOException("Directory changed or became unreadable during reconciliation.");
    }
    FileObject[] children = children(directory);
    if (children == null) {
      throw new IOException("VFS provider returned no directory listing.");
    }
    // Close every handle, including children after the one that failed.
    Throwable traversalFailure = null;
    try {
      for (FileObject child : children) {
        checkStopped();
        child.refresh();
        if (isLink(child)) {
          continue;
        }
        FileType type = child.getType();
        if (type == FileType.FOLDER && recursive) {
          if (!child.isReadable()) {
            throw new IOException("Unreadable child directory.");
          }
          if (visited.size() + directories.size() >= maximumEntries) {
            throw new WatchLimitException(
                "Directory entry limit exceeded. Increase maximum entries.");
          }
          directories.add(root.getName().getRelativeName(child.getName()));
        } else if (type == FileType.FILE
            && matches(UriParser.decode(child.getName().getBaseName()))) {
          FileState file = readState(child);
          if (files.size() >= maximumEntries) {
            throw new WatchLimitException("File entry limit exceeded. Increase maximum entries.");
          }
          files.put(file.getUri(), file);
        } else if (type == FileType.IMAGINARY) {
          // Concurrent removal: restart on the next iteration rather than infer mass deletion.
          throw new IOException("Directory changed during listing; retry reconciliation.");
        }
      }
    } catch (IOException | RuntimeException e) {
      traversalFailure = e;
      throw e;
    } finally {
      IOException failure = null;
      for (FileObject child : children) {
        try {
          child.close();
        } catch (IOException e) {
          if (traversalFailure != null) {
            traversalFailure.addSuppressed(e);
          } else if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  public FileState readRelative(String relative) throws IOException {
    checkStopped();
    try (FileObject file = resolveRelative(relative)) {
      file.refresh();
      if (!file.exists()
          || file.getType() != FileType.FILE
          || isLink(file)
          || !matches(UriParser.decode(file.getName().getBaseName()))) {
        return null;
      }
      return readState(file);
    }
  }

  public String relativeUri(String uri) throws IOException {
    if (root instanceof LocalFile) {
      String prefix = root.getName().getURI();
      if (!prefix.endsWith("/")) prefix += "/";
      if (!uri.startsWith(prefix)) throw new IOException("File is outside the watched root");
      return uri.substring(prefix.length());
    }
    try (FileObject resolved = root.resolveFile(uri)) {
      return root.getName().getRelativeName(resolved.getName());
    }
  }

  public String uri(String relative) throws IOException {
    try (FileObject file = resolveRelative(relative)) {
      return file.getName().getURI();
    }
  }

  public boolean matches(String name) {
    return (include == null || include.matcher(name).matches())
        && (exclude == null || !exclude.matcher(name).matches());
  }

  private FileObject resolveRelative(String relative) throws IOException {
    if (!(root instanceof LocalFile)) return root.resolveFile(relative);
    LocalFileName name = (LocalFileName) root.getName();
    String prefix = name.getPath();
    if (!prefix.endsWith("/")) prefix += "/";
    return root.getFileSystem()
        .resolveFile(
            relative.equals(".") ? name : name.createName(prefix + relative, FileType.FILE));
  }

  private FileObject[] children(FileObject directory) throws IOException {
    if (!(directory instanceof LocalFile)) return directory.getChildren();
    // VFS 2.10 getChildren() treats a literal Unix backslash as a separator and rejects the
    // entire listing. Enumerate local names with NIO, then keep metadata/content access in VFS.
    ArrayList<FileObject> children = new ArrayList<>();
    LocalFileName name = (LocalFileName) directory.getName();
    String prefix = name.getPath();
    if (!prefix.endsWith("/")) prefix += "/";
    try (var entries = Files.newDirectoryStream(localPath(directory))) {
      for (Path entry : entries) {
        checkStopped();
        children.add(
            directory
                .getFileSystem()
                .resolveFile(
                    name.createName(
                        prefix + UriParser.encode(entry.getFileName().toString()), FileType.FILE)));
      }
    } catch (IOException | RuntimeException e) {
      for (FileObject child : children) {
        try {
          child.close();
        } catch (IOException closeFailure) {
          e.addSuppressed(closeFailure);
        }
      }
      if (e instanceof DirectoryIteratorException iteration) {
        IOException cause = iteration.getCause();
        for (Throwable suppressed : e.getSuppressed()) cause.addSuppressed(suppressed);
        throw cause;
      }
      throw e;
    }
    return children.toArray(FileObject[]::new);
  }

  private FileState readState(FileObject file) throws IOException {
    long size = file.getContent().getSize();
    long modified = file.getContent().getLastModifiedTime();
    if (size < 0 || modified < 0) {
      throw new IOException("VFS provider must supply a non-negative size and modification time.");
    }
    String filename = HopVfs.getFilename(file);
    if (file instanceof LocalFile && filename.contains("%")) {
      // A literal percent in a raw local filename is otherwise interpreted as a VFS escape.
      filename = file.getName().getURI();
    }
    return new FileState(
        file.getName().getURI(),
        filename,
        UriParser.decode(file.getName().getBaseName()),
        file.getName().getParent().getURI(),
        file.getName().getScheme(),
        size,
        modified);
  }

  private boolean isLink(FileObject file) throws IOException {
    return file.isSymbolicLink()
        || (file instanceof LocalFile && Files.isSymbolicLink(localPath(file)));
  }

  public static Path localPath(FileObject file) throws IOException {
    LocalFileName name = (LocalFileName) file.getName();
    // Match LocalFile.doAttach(): the provider root carries the Windows drive/UNC share,
    // while getPathDecoded() preserves literal filename characters without URI parsing.
    return Path.of(name.getRootFile() + name.getPathDecoded()).toAbsolutePath().normalize();
  }

  public static FileObject resolveFile(String filename, IVariables variables)
      throws HopFileException {
    filename = variables.resolve(filename);
    // Standard file URIs use UTF-8 percent encoding. Some VFS local parsers decode each escaped
    // byte as a character instead, turning an escaped accented filename into a different path.
    // Decode explicit file names once without interpreting filename characters as URI syntax.
    // URLDecoder has form semantics, so protect literal '+' before UTF-8 percent decoding.
    // Remote providers retain their own parsing and Hop's VFS execution context.
    if (filename.regionMatches(true, 0, "file:", 0, 5)) {
      try {
        filename = URLDecoder.decode(filename.replace("+", "%2B"), StandardCharsets.UTF_8);
      } catch (IllegalArgumentException e) {
        throw new HopFileException("Invalid percent encoding in local file name", e);
      }
      // Encode the entire decoded path with VFS's URI codec, not a selected-character list.
      // NIO then interprets the platform drive/UNC root at this explicit URI boundary only.
      return resolveLocalPath(Path.of(URI.create(URIUtils.encodePath(filename))), variables);
    } else {
      String scheme = UriParser.extractScheme(filename);
      if (scheme == null || (scheme.length() == 1 && Character.isLetter(scheme.charAt(0)))) {
        return resolveLocalPath(Path.of(filename), variables);
      }
    }
    return HopVfs.getFileObject(filename, variables);
  }

  private static FileObject resolveLocalPath(Path path, IVariables variables)
      throws HopFileException {
    Path absolute = path.toAbsolutePath().normalize();
    Path prefix = absolute.getRoot();
    try (FileObject rootFile =
        HopVfs.getFileObject(UriParser.encode(prefix.toString()), variables)) {
      LocalFileName name = (LocalFileName) rootFile.getName();
      String relative =
          prefix.relativize(absolute).toString().replace(path.getFileSystem().getSeparator(), "/");
      return rootFile
          .getFileSystem()
          .resolveFile(name.createName("/" + UriParser.encode(relative), FileType.FILE));
    } catch (IOException e) {
      throw new HopFileException("Unable to resolve local filesystem path", e);
    }
  }

  private void checkStopped() throws IOException {
    if (stopped.getAsBoolean()) {
      throw new IOException("Watch Files was stopped.");
    }
  }
}
