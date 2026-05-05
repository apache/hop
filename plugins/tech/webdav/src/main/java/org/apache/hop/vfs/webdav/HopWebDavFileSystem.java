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

import java.util.Collection;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileProvider;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileSystem;

/** Logical WebDAV file system: {@link HopWebDavFileName} + delegate {@link Webdav4FileSystem}. */
class HopWebDavFileSystem extends AbstractFileSystem {

  private final Webdav4FileSystem wireFs;
  private final Webdav4FileName wireRoot;
  private final String rootWirePathPrefix;

  HopWebDavFileSystem(
      HopWebDavFileName rootName,
      FileSystemOptions fileSystemOptions,
      Webdav4FileSystem wireFs,
      Webdav4FileName wireRoot,
      String rootWirePathPrefix) {
    super(rootName, null, fileSystemOptions);
    this.wireFs = wireFs;
    this.wireRoot = wireRoot;
    this.rootWirePathPrefix = rootWirePathPrefix;
  }

  @Override
  protected FileObject createFile(AbstractFileName name) throws Exception {
    // HopRawWebDavFileSystem is built outside DefaultFileSystemManager; it never gets a context
    // unless we copy it from this logical FS (needed for resolveFile → files cache).
    wireFs.setContext(getContext());
    HopWebDavFileName logical = (HopWebDavFileName) name;
    FileObject delegate = wireFs.resolveFile(toWire(logical));
    return new HopWebDavFileObject(logical, this, delegate);
  }

  @Override
  protected void addCapabilities(Collection<Capability> caps) {
    caps.addAll(new Webdav4FileProvider().getCapabilities());
  }

  Webdav4FileName toWire(HopWebDavFileName logical) throws Exception {
    if (isLogicalRoot(logical)) {
      return wireRoot;
    }
    String rel = effectiveRelativeSegment(rootWirePathPrefix, logical.getPath());
    String wirePath = wirePathCombine(rootWirePathPrefix, rel);
    FileName child = wireRoot.createName(wirePath, logical.getType());
    return HopWebDavWireNames.asWebdav4(child);
  }

  private static boolean isLogicalRoot(HopWebDavFileName logical) {
    String p = logical.getPath();
    return p == null || p.isEmpty() || "/".equals(p);
  }

  /**
   * When {@code rootUrl} already ends at e.g. {@code /remote.php/dav/files/admin}, a logical URI
   * that repeats that path ({@code myconn:///remote.php/dav/files/admin/}) must not be appended
   * again under the wire prefix.
   */
  static String effectiveRelativeSegment(String rootWirePathPrefix, String logicalPath) {
    String rel = normalizeLogicalPathToRelativeSegment(logicalPath);
    if (rel.isEmpty()) {
      return "";
    }
    String prefix = rootWirePathPrefix == null ? "/" : rootWirePathPrefix;
    if (!prefix.startsWith("/")) {
      prefix = "/" + prefix;
    }
    String rootNoTrail = prefix;
    while (rootNoTrail.length() > 1 && rootNoTrail.endsWith("/")) {
      rootNoTrail = rootNoTrail.substring(0, rootNoTrail.length() - 1);
    }
    if ("/".equals(rootNoTrail)) {
      return rel;
    }
    String logicalAbs = "/" + rel;
    if (logicalAbs.equals(rootNoTrail)) {
      return "";
    }
    if (logicalAbs.startsWith(rootNoTrail + "/")) {
      return logicalAbs.substring(rootNoTrail.length() + 1);
    }
    return rel;
  }

  /** Logical path {@code /a/b} → {@code a/b} for appending under DAV root. */
  private static String normalizeLogicalPathToRelativeSegment(String path) {
    String p = path == null ? "" : path;
    if (p.startsWith("/")) {
      p = p.substring(1);
    }
    while (p.length() > 1 && p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    return p;
  }

  static String wirePathCombine(String rootPrefix, String relativeNoLeadingSlash) {
    String prefix = rootPrefix == null ? "/" : rootPrefix;
    if (!prefix.endsWith("/")) {
      prefix = prefix + "/";
    }
    if (relativeNoLeadingSlash == null || relativeNoLeadingSlash.isEmpty()) {
      String p = prefix;
      if (p.endsWith("/") && p.length() > 1) {
        p = p.substring(0, p.length() - 1);
      }
      return p;
    }
    return prefix + relativeNoLeadingSlash;
  }
}
