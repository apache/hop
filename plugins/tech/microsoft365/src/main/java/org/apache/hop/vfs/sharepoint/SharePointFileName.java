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
 *
 */

package org.apache.hop.vfs.sharepoint;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.UriParser;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Named Microsoft 365 connection URI: {@code connectionName:///site/library/path/in/the/library}.
 */
@NullMarked
public class SharePointFileName extends AbstractFileName {

  /** Depth of a site: {@code connection:///site}. */
  public static final int SITE_DEPTH = 1;

  /** Depth of a document library (drive): {@code connection:///site/library}. */
  public static final int DRIVE_DEPTH = 2;

  /** The decoded names of the path: the site, the library, then the folders and the file. */
  private final String[] segments;

  public SharePointFileName(final String scheme, final String path, final FileType type) {
    super(scheme, path, type);
    this.segments = split(getPath());
  }

  /**
   * URL encodes a name for a Microsoft Graph path-based address, where a space is {@code %20} and
   * not a plus sign.
   */
  static String encodeSegment(String name) {
    return URLEncoder.encode(name, StandardCharsets.UTF_8).replace("+", "%20");
  }

  /**
   * @return the name of the site or null for the root of the file system
   */
  public @Nullable String getSiteName() {
    return segments.length >= SITE_DEPTH ? segments[SITE_DEPTH - 1] : null;
  }

  /**
   * @return the name of the document library or null above the libraries
   */
  public @Nullable String getDriveName() {
    return segments.length >= DRIVE_DEPTH ? segments[DRIVE_DEPTH - 1] : null;
  }

  /**
   * The path of the item relative to the root of its document library, every segment URL encoded so
   * it can be used in a Microsoft Graph path-based address.
   *
   * @return the encoded path without leading slash, empty for the library itself and above
   */
  public String getEncodedItemPath() {
    StringJoiner joiner = new StringJoiner("/");
    for (int i = DRIVE_DEPTH; i < segments.length; i++) {
      joiner.add(encodeSegment(segments[i]));
    }
    return joiner.toString();
  }

  /**
   * @return true if this name is an item (file or folder) inside a document library
   */
  public boolean isItem() {
    return getLevel() > DRIVE_DEPTH;
  }

  /**
   * @return the number of names in the path: 0 for the root, 1 for a site, 2 for a library
   */
  public int getLevel() {
    return segments.length;
  }

  /**
   * @return the base name as Microsoft Graph knows it, without the VFS escaping of the path
   */
  public String getDecodedBaseName() {
    return segments.length == 0 ? "" : segments[segments.length - 1];
  }

  private static String[] split(String path) {
    if (path.length() <= 1) {
      return new String[0];
    }
    // The path keeps a literal percent sign escaped
    String[] segments = path.substring(1).split("/");
    for (int i = 0; i < segments.length; i++) {
      try {
        segments[i] = UriParser.decode(segments[i]);
      } catch (FileSystemException e) {
        // Not an escape sequence, the name is used as it is
      }
    }
    return segments;
  }

  @Override
  public FileName createName(String absPath, FileType type) {
    return new SharePointFileName(getScheme(), absPath, type);
  }

  @Override
  protected void appendRootUri(StringBuilder buffer, boolean addPassword) {
    buffer.append(getScheme());
    buffer.append("://");
  }
}
