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
package org.apache.hop.vfs.sftp.provider;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.URLFileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;

/** Implementation for SFTP. Sets the default port to 22. */
public class SftpFileNameParser extends URLFileNameParser {

  private static final int DEFAULT_PORT = 22;

  private static final SftpFileNameParser INSTANCE = new SftpFileNameParser();

  /**
   * Gets the singleton instance.
   *
   * @return the singleton instance.
   */
  public static FileNameParser getInstance() {
    return INSTANCE;
  }

  /** Constructs a new instance with the default port 22. */
  public SftpFileNameParser() {
    super(DEFAULT_PORT);
  }

  /**
   * As {@link URLFileNameParser#parseUri}, but a path rooted at a second slash keeps it.
   *
   * <p>The prefix is taken off before the path is canonicalized and normalised, because
   * normalisation is what would drop it, and it is put in the root of a {@link SftpFileName} rather
   * than left in the path so that it survives every child and every parent derived from this name.
   */
  @Override
  public FileName parseUri(VfsComponentContext context, FileName base, String fileName)
      throws FileSystemException {
    StringBuilder name = new StringBuilder();

    Authority auth = extractToPath(context, fileName, name);
    String queryString = UriParser.extractQueryString(name);
    String rootPrefix = resolveRootPrefix(name, fileName, base);

    UriParser.canonicalizePath(name, 0, name.length(), this);
    UriParser.fixSeparators(name);
    FileType fileType = UriParser.normalisePath(name);

    return new SftpFileName(
        auth.getScheme(),
        auth.getHostName(),
        auth.getPort(),
        getDefaultPort(),
        auth.getUserName(),
        auth.getPassword(),
        name.toString(),
        fileType,
        queryString,
        rootPrefix);
  }

  /**
   * The prefix for a path, taken from the base name whenever this URI was built out of it.
   *
   * <p>The textual rule cannot be trusted on a derived name. {@link
   * org.apache.commons.vfs2.impl.DefaultFileSystemManager#resolveName} rebuilds every child, every
   * parent and every root lookup as {@code base.getRootURI() + path}, and since the root URI ends
   * in a separator and the path begins with one, that concatenation manufactures a {@code //} at
   * the boundary which is indistinguishable from one a user typed. It is also the reason Commons
   * VFS collapses {@code //} at all. A URI built that way starts with the root URI of the base it
   * came from, and the base already knows what it is rooted at, so ask it.
   *
   * <p>The one thing this cannot tell apart is a full URI handed to {@code
   * FileObject.resolveFile(String)} on a file of the same file system, which looks exactly like a
   * derived name. Resolving from a base is for relative paths; an absolute URI belongs to {@code
   * HopVfs.getFileObject}, which has no base and is read as written.
   *
   * @param path the path, with the scheme and the authority already taken off
   * @param uri the whole URI being parsed
   * @param base the name this one is being derived from, null if there is none
   */
  public static String resolveRootPrefix(StringBuilder path, String uri, FileName base) {
    if (base instanceof IRootPrefixFileName prefixed && uri.startsWith(base.getRootURI())) {
      return prefixed.getRootPrefix();
    }
    return extractRootPrefix(path);
  }

  /**
   * Takes the second slash off a path which starts with exactly two, leaving an ordinary absolute
   * path behind.
   *
   * <p>Exactly two is what POSIX gives a meaning of its own and what z/OS reads as an MVS data set;
   * three or more mean the same as one, so those are left for the normaliser to collapse. Only a
   * literal slash counts - a {@code %2F} is an escaped slash, which is to say not a separator at
   * all.
   *
   * @param path the path, with the scheme and the authority already taken off
   * @return {@code "/"} if a second slash was taken off, {@code ""} otherwise
   */
  public static String extractRootPrefix(StringBuilder path) {
    if (path.length() >= 2
        && path.charAt(0) == '/'
        && path.charAt(1) == '/'
        && (path.length() == 2 || path.charAt(2) != '/')) {
      path.deleteCharAt(0);
      return "/";
    }
    return "";
  }

  /**
   * Gets a data set name of a single level, such as {@code //HLQ.NAME}, to the server intact.
   *
   * <p>jsch runs almost every path through its glob handling, which splits the path at the last
   * slash and so turns {@code //HLQ.NAME} into {@code /HLQ.NAME} - the USS path. With a trailing
   * slash the last slash comes after the data set name, and {@code //HLQ.NAME/} reaches the server
   * as is. Paths of more than one level, and every path not starting with exactly two slashes, are
   * left alone.
   *
   * <p>Verified on z/OS for a data set name prefix used as a folder. A single-level sequential data
   * set read or written as a file gets the trailing slash too, which hasn't been verified yet.
   *
   * @param path a path as it goes to jsch
   * @return the path, with a trailing slash if it's a data set name of a single level
   */
  public static String keepDataSetPrefix(String path) {
    return path != null
            && path.length() > 2
            && path.startsWith("//")
            && path.charAt(2) != '/'
            && path.indexOf('/', 2) < 0
        ? path + "/"
        : path;
  }
}
