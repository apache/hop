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
package org.apache.hop.vfs.sftp;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.provider.sftp.SftpFileProvider;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;

/**
 * The plain SFTP provider of commons-vfs, driven by a named {@link SftpConnection} instead of by
 * the URI: the scheme is the name of the connection and everything behind it is a path on the
 * server the connection points at.
 */
public class SftpConnectionFileProvider extends SftpFileProvider {

  private final IVariables variables;
  private final SftpConnection connection;

  /** The session settings of the connection, the same instance for every file we resolve. */
  private FileSystemOptions connectionOptions;

  public SftpConnectionFileProvider(IVariables variables, SftpConnection connection) {
    this.variables = variables;
    this.connection = connection;
    setFileNameParser(new SftpConnectionFileNameParser(variables, connection));
  }

  /**
   * VFS caches a file system under the options it was created with, and looks it up under the
   * options of the caller. Handing it a freshly built set of options for every file would make
   * those two differ every single time: a new file system, and with it a new SSH session, for every
   * file. Build them once and use them for both.
   */
  private synchronized FileSystemOptions connectionOptions(FileSystemOptions base)
      throws FileSystemException {
    if (connectionOptions == null) {
      connectionOptions = SftpConnectionOptions.build(variables, connection, base);
    }
    return connectionOptions;
  }

  @Override
  public FileObject findFile(FileObject baseFile, String uri, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    return super.findFile(baseFile, uri, connectionOptions(fileSystemOptions));
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    if (LogChannel.GENERAL.isDebug()) {
      // Never the password: this ends up in the log of whoever uses the connection.
      GenericFileName name = (GenericFileName) rootName;
      LogChannel.GENERAL.logDebug(
          "Opening SFTP connection \""
              + connection.getName()
              + "\" : "
              + Const.NVL(name.getUserName(), "")
              + "@"
              + name.getHostName()
              + ":"
              + name.getPort());
    }

    return super.doCreateFileSystem(rootName, connectionOptions(fileSystemOptions));
  }
}
