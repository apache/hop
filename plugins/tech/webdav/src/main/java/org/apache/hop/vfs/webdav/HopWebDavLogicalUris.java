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

import java.net.URI;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;

/** Helpers for logical {@code <connection-name>:///…} URIs vs internal WebDAV wire paths. */
final class HopWebDavLogicalUris {

  private HopWebDavLogicalUris() {}

  /**
   * Path prefix under the WebDAV host for {@link
   * org.apache.hop.vfs.webdav.metadata.WebDavConnection#getRootUrl()}, always starting with {@code
   * /} and ending with {@code /} when non-empty.
   */
  static String wirePathPrefixFromRootUrl(String resolvedRootUrl) throws FileSystemException {
    String rootUrl = Const.NVL(resolvedRootUrl, "").trim();
    if (StringUtils.isEmpty(rootUrl)) {
      throw new FileSystemException("WebDAV connection has no rootUrl");
    }
    URI u;
    try {
      u = URI.create(rootUrl);
    } catch (IllegalArgumentException e) {
      throw new FileSystemException(e);
    }
    String p = u.getPath();
    if (p == null || p.isEmpty()) {
      return "/";
    }
    if (!p.endsWith("/")) {
      p = p + "/";
    }
    if (!p.startsWith("/")) {
      p = "/" + p;
    }
    return p;
  }

  /**
   * Relative path after the connection URI authority (no leading slash), e.g. {@code myconn:///a/b}
   * → {@code a/b}.
   */
  /**
   * Normalized path from a connection URI (leading {@code /}, collapsed duplicate slashes after
   * scheme), used when matching {@link #wirePathPrefixFromRootUrl} for VFS {@code resolveName}
   * output.
   */
  static String rawPathFromUri(String uri) throws FileSystemException {
    URI parsed;
    try {
      parsed = URI.create(uri);
    } catch (IllegalArgumentException e) {
      throw new FileSystemException(e);
    }
    String path = parsed.getRawPath();
    if (path == null || path.isEmpty()) {
      return "";
    }
    while (path.startsWith("//")) {
      path = path.substring(1);
    }
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    return path;
  }

  static String extractPathSuffixAfterScheme(String uri) throws FileSystemException {
    URI parsed;
    try {
      parsed = URI.create(uri);
    } catch (IllegalArgumentException e) {
      throw new FileSystemException(e);
    }
    String path = parsed.getRawPath();
    if (path == null) {
      path = "";
    }
    if (path.isEmpty() || "/".equals(path)) {
      return "";
    }
    if (path.startsWith("/")) {
      return path.substring(1);
    }
    return path;
  }

  /** Strip {@code rootWirePathPrefix} from {@code childWirePath} to get the logical suffix. */
  static String relativePathFromWirePaths(String rootWirePathPrefix, String childWirePath) {
    String root = rootWirePathPrefix == null ? "/" : rootWirePathPrefix;
    String child = childWirePath == null ? "" : childWirePath;
    if (!child.startsWith("/")) {
      child = "/" + child;
    }
    if (!root.endsWith("/")) {
      root = root + "/";
    }
    if (child.startsWith(root)) {
      String rel = child.substring(root.length());
      if (rel.startsWith("/")) {
        rel = rel.substring(1);
      }
      return rel;
    }
    if (child.startsWith("/")) {
      return child.substring(1);
    }
    return child;
  }
}
