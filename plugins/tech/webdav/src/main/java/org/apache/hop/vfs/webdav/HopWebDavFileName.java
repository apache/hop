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
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;

/**
 * Logical WebDAV connection URI like {@code myconn:///folder/file.txt}, same pattern as Hop's S3
 * and Google Storage file names (connection name as {@code AbstractFileName} scheme).
 */
public final class HopWebDavFileName extends AbstractFileName {

  public HopWebDavFileName(String scheme, String path, FileType type) {
    super(scheme, normalizeStoredPath(path, type), type);
  }

  /**
   * Child path under {@code parent} using wire {@code baseName} (last segment). Avoids {@link
   * org.apache.commons.vfs2.FileSystemManager#resolveName} per row when building directory
   * listings.
   */
  public static HopWebDavFileName buildChild(
      HopWebDavFileName parent, String baseName, FileType childType) {
    String p = parent.getPath();
    String childPath;
    if (p == null || p.isEmpty() || "/".equals(p)) {
      childPath = "/" + baseName;
    } else if (p.endsWith("/")) {
      childPath = p + baseName;
    } else {
      childPath = p + "/" + baseName;
    }
    return new HopWebDavFileName(parent.getScheme(), childPath, childType);
  }

  private static String normalizeStoredPath(String path, FileType type) {
    String p = StringUtils.defaultString(path);
    if (p.isEmpty() || "/".equals(p)) {
      return "/";
    }
    if (!p.startsWith("/")) {
      p = "/" + p;
    }
    if (type == FileType.FOLDER && p.length() > 1 && !p.endsWith("/")) {
      p = p + "/";
    }
    return p;
  }

  @Override
  public FileName createName(String absPath, FileType fileType) {
    return new HopWebDavFileName(getScheme(), absPath, fileType);
  }

  @Override
  public String getURI() {
    StringBuilder sb = new StringBuilder();
    sb.append(getScheme()).append(":///");
    String p = getPath();
    if (p == null || p.isEmpty() || "/".equals(p)) {
      return sb.toString();
    }
    String suffix = p.startsWith("/") ? p.substring(1) : p;
    sb.append(suffix);
    return sb.toString();
  }

  @Override
  public String getRootURI() {
    return getScheme() + ":///";
  }

  @Override
  public String getFriendlyURI() {
    return getURI();
  }

  @Override
  protected void appendRootUri(StringBuilder buffer, boolean addPassword) {
    buffer.append(getScheme());
    buffer.append(":///");
  }
}
