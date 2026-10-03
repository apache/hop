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
package org.apache.hop.vfs.smb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.i18n.BaseMessages;

/** Turns a VFS path into a share-relative SMB path and keeps it under the connection's folder. */
public final class SmbPaths {
  private static final Class<?> PKG = SmbPaths.class;

  private SmbPaths() {}

  /**
   * Share-relative path using {@code \} separators and no leading separator. Empty means the share
   * root (or the base folder, when one is set).
   */
  public static String sharePath(String basePath, String vfsPath) throws IOException {
    String base = normalize(basePath);
    String relative = normalize(vfsPath);
    if (base.isEmpty()) {
      return relative;
    }
    if (relative.isEmpty()) {
      return base;
    }
    return base + "\\" + relative;
  }

  static String normalize(String path) throws IOException {
    if (path == null || path.isBlank()) {
      return "";
    }
    String unified = path.replace('\\', '/').trim();
    while (unified.startsWith("/")) {
      unified = unified.substring(1);
    }
    while (unified.endsWith("/") && !unified.isEmpty()) {
      unified = unified.substring(0, unified.length() - 1);
    }
    if (unified.isEmpty()) {
      return "";
    }
    List<String> parts = new ArrayList<>();
    for (String part : unified.split("/")) {
      if (part.isEmpty() || ".".equals(part)) {
        continue;
      }
      if ("..".equals(part)) {
        if (parts.isEmpty()) {
          throw new IOException(BaseMessages.getString(PKG, "Smb.Error.PathEscapes", path));
        }
        parts.remove(parts.size() - 1);
        continue;
      }
      if (part.indexOf('\0') >= 0) {
        throw new IOException(BaseMessages.getString(PKG, "Smb.Error.PathEscapes", path));
      }
      parts.add(part);
    }
    return String.join("\\", parts);
  }
}
