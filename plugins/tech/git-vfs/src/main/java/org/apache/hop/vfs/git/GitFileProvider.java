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
package org.apache.hop.vfs.git;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.git.metadata.GitConnection;

/**
 * The provider of one named git connection: the scheme is the name of the connection and everything
 * behind it is a path in the working copy of the repository that connection points at.
 *
 * <p>Extends {@link AbstractOriginatingFileProvider} rather than {@code AbstractFileProvider}: a
 * connection is a root which owns its own file system rather than a layer over somebody else's, and
 * this is the base class for that.
 */
public class GitFileProvider extends AbstractOriginatingFileProvider {

  private final IVariables variables;
  private final GitConnection connection;

  public GitFileProvider(IVariables variables, GitConnection connection) {
    this.variables = variables;
    this.connection = connection;
    setFileNameParser(new GitFileNameParser(variables, connection));
  }

  @Override
  protected FileSystem doCreateFileSystem(
      FileName rootFileName, FileSystemOptions fileSystemOptions) throws FileSystemException {
    AbstractFileName rootName = (AbstractFileName) rootFileName;
    // The name carries the repository: the parser puts one there, so a file system is never left
    // without one however it was reached.
    GitCheckout checkout =
        rootName instanceof GitFileName gitName
            ? gitName.getCheckout()
            : new GitCheckout(variables, connection);
    return new GitFileSystem(rootName, null, checkout);
  }

  /**
   * What a connection of this plugin can do.
   *
   * <p>Per instance rather than a constant, because it depends on the connection: a read only one
   * does not write. Reporting the writable set to the file system manager is what lets a caller ask
   * "can this connection be written to" before it tries, without a failed write to find out.
   */
  @Override
  public Collection<Capability> getCapabilities() {
    List<Capability> capabilities =
        new ArrayList<>(
            Arrays.asList(
                Capability.GET_TYPE,
                Capability.LIST_CHILDREN,
                Capability.URI,
                Capability.GET_LAST_MODIFIED));
    if (!connection.isReadOnly()) {
      capabilities.addAll(
          Arrays.asList(
              Capability.WRITE_CONTENT,
              Capability.APPEND_CONTENT,
              Capability.CREATE,
              Capability.DELETE,
              Capability.RENAME));
    }
    return Collections.unmodifiableCollection(capabilities);
  }

  public GitConnection getConnection() {
    return connection;
  }
}
