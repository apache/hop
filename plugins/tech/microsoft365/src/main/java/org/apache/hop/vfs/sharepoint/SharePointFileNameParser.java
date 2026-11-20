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

package org.apache.hop.vfs.sharepoint;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileNameParser;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Parses {@code scheme:///site/library/path}. The scheme is the named connection. Credentials are
 * not in the URI.
 */
@NullMarked
public class SharePointFileNameParser extends AbstractFileNameParser {
  private static final SharePointFileNameParser INSTANCE = new SharePointFileNameParser();

  public static FileNameParser getInstance() {
    return INSTANCE;
  }

  @Override
  public FileName parseUri(
      @Nullable VfsComponentContext context, @Nullable FileName base, String uri)
      throws FileSystemException {
    StringBuilder name = new StringBuilder();
    String scheme = UriParser.extractScheme(uri, name);
    if (scheme == null) {
      if (base == null) {
        throw new FileSystemException("vfs.provider/absolute-uri.error", uri);
      }
      scheme = base.getScheme();
      if (name.isEmpty() || name.charAt(0) != '/') {
        name.insert(0, base.getPath().endsWith("/") ? base.getPath() : base.getPath() + "/");
      }
    }
    UriParser.canonicalizePath(name, 0, name.length(), this);
    UriParser.fixSeparators(name);
    while (!name.isEmpty() && name.charAt(0) == '/') {
      name.deleteCharAt(0);
    }
    name.insert(0, '/');
    FileType fileType = UriParser.normalisePath(name);
    String path = name.toString();
    if (path.isEmpty() || "/".equals(path)) {
      path = "/";
      fileType = FileType.FOLDER;
    }
    return new SharePointFileName(scheme, path, fileType);
  }
}
