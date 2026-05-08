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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileNameParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.hop.core.Const;
import org.apache.hop.vfs.webdav.metadata.WebDavConnection;

/**
 * Parses logical {@code <connection-name>:///relative/path} like {@link
 * org.apache.hop.vfs.s3.s3.vfs.S3FileNameParser} — scheme comes from the URI, not from wire {@code
 * webdav4}.
 */
final class HopWebDavFileNameParser extends AbstractFileNameParser {

  private final WebDavConnection meta;

  HopWebDavFileNameParser(WebDavConnection meta) {
    this.meta = meta;
  }

  @Override
  public FileName parseUri(VfsComponentContext context, FileName base, String uri)
      throws FileSystemException {
    String conn = Const.NVL(meta.getName(), "").trim();
    if (StringUtils.isEmpty(conn)) {
      throw new FileSystemException("WebDAV connection has no name");
    }
    int c = uri.indexOf(':');
    if (c < 0) {
      throw new FileSystemException("Missing URI scheme: " + uri);
    }
    String scheme = uri.substring(0, c);
    if (!scheme.equals(conn)) {
      throw new FileSystemException(
          "URI scheme must match WebDAV connection name \"" + conn + "\", got: " + scheme);
    }
    String pathPart = HopWebDavLogicalUris.rawPathFromUri(uri);
    String path = pathPart.isEmpty() || "/".equals(pathPart) ? "/" : pathPart;
    FileType type = inferType(uri, path);
    return new HopWebDavFileName(scheme, path, type);
  }

  private static FileType inferType(String uri, String path) {
    if (uri.endsWith("/")) {
      return FileType.FOLDER;
    }
    if ("/".equals(path) || (path.length() > 1 && path.endsWith("/"))) {
      return FileType.FOLDER;
    }
    return FileType.FILE;
  }
}
