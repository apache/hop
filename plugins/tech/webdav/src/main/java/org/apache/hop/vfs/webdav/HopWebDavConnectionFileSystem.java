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

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileSystem;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;

/**
 * Ensures every {@link Webdav4FileObject} gets a {@link HopWebDavConnectionFileName} so {@link
 * FileObject#getName()}{@code .getURI()} stays on the metadata connection scheme (e.g. {@code
 * nextcloud:///…}) after {@code PROPFIND} / internal resolves, not only on the initial parse.
 */
class HopWebDavConnectionFileSystem extends Webdav4FileSystem {

  private final String logicalConnectionName;
  private final String rootWirePathPrefix;

  HopWebDavConnectionFileSystem(
      FileName rootName,
      FileSystemOptions fileSystemOptions,
      HttpClient httpClient,
      HttpClientContext httpClientContext,
      String logicalConnectionName,
      String rootWirePathPrefix) {
    super(rootName, fileSystemOptions, httpClient, httpClientContext);
    this.logicalConnectionName = logicalConnectionName;
    this.rootWirePathPrefix = rootWirePathPrefix;
  }

  @Override
  protected FileObject createFile(AbstractFileName name) throws Exception {
    return super.createFile(wrap(name));
  }

  private AbstractFileName wrap(AbstractFileName name) {
    if (name instanceof HopWebDavConnectionFileName) {
      return name;
    }
    if (name instanceof Webdav4FileName) {
      Webdav4FileName w = (Webdav4FileName) name;
      String rel = HopWebDavLogicalUris.relativePathFromWirePaths(rootWirePathPrefix, w.getPath());
      return new HopWebDavConnectionFileName(w, logicalConnectionName, rel, rootWirePathPrefix);
    }
    return name;
  }
}
