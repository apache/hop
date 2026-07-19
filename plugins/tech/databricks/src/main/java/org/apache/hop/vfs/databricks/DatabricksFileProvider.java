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
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databricks.client.RestDatabricksFilesClient;
import org.apache.hop.databricks.metadata.DatabricksConnection;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.vfs.databricks.metadata.DatabricksVfsConnection;

/**
 * Originating provider for Databricks Files API paths. Bound to a named {@link
 * DatabricksVfsConnection} whose name is the VFS scheme; auth is resolved via the referenced {@link
 * DatabricksConnection}. Optional root path shortens URIs relative to a volume or workspace folder.
 */
public class DatabricksFileProvider extends AbstractOriginatingFileProvider {

  protected static final Collection<Capability> CAPABILITIES =
      Set.of(
          Capability.CREATE,
          Capability.DELETE,
          Capability.RENAME,
          Capability.GET_TYPE,
          Capability.LIST_CHILDREN,
          Capability.READ_CONTENT,
          Capability.URI,
          Capability.WRITE_CONTENT,
          Capability.GET_LAST_MODIFIED);

  private static final FileSystemOptions DEFAULT_OPTIONS = new FileSystemOptions();

  private final IVariables variables;
  private final DatabricksVfsConnection vfsConnection;

  public DatabricksFileProvider() {
    this(null, null);
  }

  public DatabricksFileProvider(IVariables variables, DatabricksVfsConnection vfsConnection) {
    super();
    this.variables = variables;
    this.vfsConnection = vfsConnection;
    setFileNameParser(DatabricksFileNameParser.getInstance());
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return CAPABILITIES;
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    FileSystemOptions opts = fileSystemOptions != null ? fileSystemOptions : DEFAULT_OPTIONS;
    if (vfsConnection == null || variables == null) {
      // Default FSM registration path — no live client until a named provider is used.
      return new DatabricksFileSystem(rootName, null, opts, "");
    }
    try {
      DatabricksConnection connection = resolveDatabricksConnection();
      RestDatabricksFilesClient client = RestDatabricksFilesClient.create(connection, variables);
      String rootPath = resolveRootPath();
      return new DatabricksFileSystem(rootName, client, opts, rootPath);
    } catch (HopException e) {
      throw new FileSystemException(
          "Unable to create Databricks Files client for VFS connection '"
              + vfsConnection.getName()
              + "'",
          e);
    }
  }

  private String resolveRootPath() throws HopException {
    String raw = vfsConnection.getDefaultBasePath();
    if (StringUtils.isBlank(raw)) {
      return "";
    }
    return DatabricksPathResolver.normalizeRootPath(variables.resolve(raw));
  }

  private DatabricksConnection resolveDatabricksConnection() throws HopException {
    String connName = vfsConnection.getDatabricksConnectionName();
    if (StringUtils.isBlank(connName)) {
      throw new HopException(
          "Databricks VFS connection '"
              + vfsConnection.getName()
              + "' has no Databricks Connection selected");
    }
    connName = variables.resolve(connName);
    IHopMetadataProvider metadataProvider =
        HopMetadataUtil.getStandardHopMetadataProvider(variables);
    DatabricksConnection connection =
        metadataProvider.getSerializer(DatabricksConnection.class).load(connName);
    if (connection == null) {
      throw new HopException(
          "Databricks Connection '"
              + connName
              + "' not found (referenced by VFS connection '"
              + vfsConnection.getName()
              + "')");
    }
    return connection;
  }
}
