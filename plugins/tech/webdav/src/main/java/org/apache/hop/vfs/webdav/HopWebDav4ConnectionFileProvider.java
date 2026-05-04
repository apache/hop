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
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.webdav.metadata.WebDavConnection;

/** WebDAV over HTTP with credentials from {@link WebDavConnection} metadata. */
public class HopWebDav4ConnectionFileProvider extends Webdav4FileProvider {

  private final IVariables variables;
  private final WebDavConnection meta;

  public HopWebDav4ConnectionFileProvider(IVariables variables, WebDavConnection meta) {
    this.variables = variables;
    this.meta = meta;
    setFileNameParser(new HopWebDavConnectionFileNameParser(variables, meta));
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName name, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    FileSystemOptions opts =
        fileSystemOptions != null
            ? (FileSystemOptions) fileSystemOptions.clone()
            : new FileSystemOptions();
    HopWebDavConnectionAuth.apply(opts, variables, meta);
    return super.doCreateFileSystem(name, opts);
  }
}
