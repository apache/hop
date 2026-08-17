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
package org.apache.hop.vfs.ftp;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftps.FtpsFileProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;

/**
 * The FTPS provider of commons-vfs, driven by a named {@link FtpConnection} - the FTPS twin of
 * {@link FtpConnectionFileProvider}, which is what a connection in one of the FTPS security modes
 * is served by.
 */
public class FtpsConnectionFileProvider extends FtpsFileProvider {

  private final FtpConnectionSupport support;

  public FtpsConnectionFileProvider(IVariables variables, FtpConnection connection) {
    this.support = new FtpConnectionSupport(variables, connection);
    setFileNameParser(new FtpConnectionFileNameParser(variables, connection));
  }

  @Override
  public FileObject findFile(FileObject baseFile, String uri, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    return support.withSocksCredentials(
        () -> super.findFile(baseFile, uri, support.options(fileSystemOptions)));
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    return support.withSocksCredentials(
        () -> super.doCreateFileSystem(rootName, support.options(fileSystemOptions)));
  }
}
