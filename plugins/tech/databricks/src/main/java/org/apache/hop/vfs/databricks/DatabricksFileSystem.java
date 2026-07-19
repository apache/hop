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

package org.apache.hop.vfs.databricks;

import java.util.Collection;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.hop.databricks.client.DatabricksFilesClient;

public class DatabricksFileSystem extends AbstractFileSystem {

  private final DatabricksFilesClient client;

  /** Normalized scheme root, or empty when absolute workspace paths are required. */
  private final String rootPath;

  private final DatabricksListCache listCache = new DatabricksListCache();

  protected DatabricksFileSystem(
      final FileName rootName,
      final DatabricksFilesClient client,
      final FileSystemOptions fileSystemOptions) {
    this(rootName, client, fileSystemOptions, "");
  }

  protected DatabricksFileSystem(
      final FileName rootName,
      final DatabricksFilesClient client,
      final FileSystemOptions fileSystemOptions,
      final String rootPath) {
    super(rootName, null, fileSystemOptions);
    this.client = client;
    this.rootPath = rootPath == null ? "" : rootPath;
  }

  DatabricksListCache getListCache() {
    return listCache;
  }

  @Override
  protected void addCapabilities(final Collection<Capability> caps) {
    caps.addAll(DatabricksFileProvider.CAPABILITIES);
  }

  @Override
  protected FileObject createFile(final AbstractFileName name) throws FileSystemException {
    return new DatabricksFileObject(name, this);
  }

  DatabricksFilesClient getClient() throws FileSystemException {
    if (client == null) {
      throw new FileSystemException(
          "Databricks VFS is not bound to a connection. Use a named Databricks VFS Connection as the URI scheme.");
    }
    return client;
  }

  /** Scheme root (e.g. {@code /Volumes/cat/schema/vol}), or blank. */
  String getRootPath() {
    return rootPath;
  }

  boolean hasRootPath() {
    return StringUtils.isNotBlank(rootPath);
  }
}
