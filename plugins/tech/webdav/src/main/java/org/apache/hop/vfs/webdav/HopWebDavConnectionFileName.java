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
import org.apache.commons.vfs2.provider.GenericURLFileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileName;

/**
 * Keeps internal {@link Webdav4FileName} wire identity (host, path, credentials) for HTTP/WebDAV
 * while exposing {@link #getURI()} / {@link #getRootURI()} as {@code
 * <metadata-name>:///relative/path} like Azure/S3 named VFS schemes.
 */
public final class HopWebDavConnectionFileName extends Webdav4FileName {

  private final String logicalConnectionName;
  private final String logicalRelativePath;
  private final String rootWirePathPrefix;

  public HopWebDavConnectionFileName(
      Webdav4FileName wire,
      String logicalConnectionName,
      String logicalRelativePath,
      String rootWirePathPrefix) {
    super(
        wire.getScheme(),
        wire.getHostName(),
        wire.getPort(),
        wire.getDefaultPort(),
        wire.getUserName(),
        wire.getPassword(),
        wire.getPath(),
        wire.getType(),
        wire.getQueryString(),
        readAppendTrailingSlash(wire));
    this.logicalConnectionName = logicalConnectionName;
    this.logicalRelativePath = logicalRelativePath == null ? "" : logicalRelativePath;
    this.rootWirePathPrefix = rootWirePathPrefix;
  }

  private static boolean readAppendTrailingSlash(Webdav4FileName wire) {
    try {
      java.lang.reflect.Field f = Webdav4FileName.class.getDeclaredField("appendTrailingSlash");
      f.setAccessible(true);
      return f.getBoolean(wire);
    } catch (ReflectiveOperationException e) {
      return wire.getType() == FileType.FOLDER;
    }
  }

  @Override
  protected String createURI() {
    StringBuilder sb = new StringBuilder();
    sb.append(logicalConnectionName).append(":///");
    if (StringUtils.isNotEmpty(logicalRelativePath)) {
      sb.append(logicalRelativePath);
    }
    if (getType() == FileType.FOLDER
        && StringUtils.isNotEmpty(logicalRelativePath)
        && !logicalRelativePath.endsWith("/")) {
      sb.append('/');
    }
    String q = getQueryString();
    if (StringUtils.isNotEmpty(q)) {
      sb.append('?').append(q);
    }
    return sb.toString();
  }

  @Override
  public String getRootURI() {
    String q = getQueryString();
    if (StringUtils.isNotEmpty(q)) {
      return logicalConnectionName + ":///?" + q;
    }
    return logicalConnectionName + ":///";
  }

  @Override
  public String getFriendlyURI() {
    return getURI();
  }

  @Override
  public FileName createName(String absPath, FileType fileType) {
    GenericURLFileName g = (GenericURLFileName) super.createName(absPath, fileType);
    boolean trailing = fileType == FileType.FOLDER;
    Webdav4FileName wire =
        new Webdav4FileName(
            g.getScheme(),
            g.getHostName(),
            g.getPort(),
            g.getDefaultPort(),
            g.getUserName(),
            g.getPassword(),
            g.getPath(),
            fileType,
            g.getQueryString(),
            trailing);
    String childRel =
        HopWebDavLogicalUris.relativePathFromWirePaths(rootWirePathPrefix, g.getPath());
    return new HopWebDavConnectionFileName(
        wire, logicalConnectionName, childRel, rootWirePathPrefix);
  }
}
