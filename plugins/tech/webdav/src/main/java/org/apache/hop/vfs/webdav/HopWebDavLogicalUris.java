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
   * Normalized path from a connection URI (leading {@code /}, collapsed duplicate slashes after
   * scheme). Used when matching {@link #wirePathPrefixFromRootUrl} and when VFS {@code resolveName}
   * builds child display names.
   *
   * <p>We do not use {@link URI#create(String)} because logical paths may contain spaces and other
   * characters that are valid on WebDAV but illegal in strict URIs.
   */
  static String rawPathFromUri(String uri) throws FileSystemException {
    if (StringUtils.isEmpty(uri)) {
      return "";
    }
    int colon = uri.indexOf(':');
    if (colon < 0) {
      throw new FileSystemException("Missing URI scheme: " + uri);
    }
    String rest = uri.substring(colon + 1);
    int hash = rest.indexOf('#');
    if (hash >= 0) {
      rest = rest.substring(0, hash);
    }
    if (!rest.startsWith("//")) {
      throw new FileSystemException("Expected // after scheme in connection URI: " + uri);
    }
    rest = rest.substring(2);
    int q = rest.indexOf('?');
    if (q >= 0) {
      rest = rest.substring(0, q);
    }
    while (rest.startsWith("/")) {
      rest = rest.substring(1);
    }
    String path = rest.isEmpty() ? "" : "/" + rest;
    while (path.startsWith("//")) {
      path = path.substring(1);
    }
    if (!path.isEmpty() && !path.startsWith("/")) {
      path = "/" + path;
    }
    return path;
  }

  /**
   * Relative path after the connection URI authority (no leading slash), e.g. {@code myconn:///a/b}
   * → {@code a/b}.
   */
  static String extractPathSuffixAfterScheme(String uri) throws FileSystemException {
    String path = rawPathFromUri(uri);
    if (path.isEmpty() || "/".equals(path)) {
      return "";
    }
    return path.startsWith("/") ? path.substring(1) : path;
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
