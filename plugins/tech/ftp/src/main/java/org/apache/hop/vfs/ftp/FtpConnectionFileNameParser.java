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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;

/**
 * Turns {@code <connection name>://path} (any number of slashes after the scheme) into a {@link
 * FtpConnectionFileName} holding the server and credentials of the connection.
 */
public class FtpConnectionFileNameParser extends AbstractFileNameParser {

  private final IVariables variables;
  private final FtpConnection connection;

  public FtpConnectionFileNameParser(IVariables variables, FtpConnection connection) {
    this.variables = variables;
    this.connection = connection;
  }

  @Override
  public FileName parseUri(VfsComponentContext context, FileName base, String uri)
      throws FileSystemException {
    String connectionName = Const.NVL(connection.getName(), "").trim();
    if (StringUtils.isEmpty(connectionName)) {
      throw new FileSystemException("The FTP connection has no name");
    }

    StringBuilder buffer = new StringBuilder();
    String scheme =
        UriParser.extractScheme(context.getFileSystemManager().getSchemes(), uri, buffer);
    if (scheme == null) {
      // A name relative to the base file: the base already tells us which connection we're on.
      //
      if (base == null) {
        throw new FileSystemException("vfs.provider/absolute-uri.error", uri);
      }
      buffer.insert(0, base.getPath().endsWith("/") ? base.getPath() : base.getPath() + "/");
    } else if (!scheme.equals(connectionName)) {
      throw new FileSystemException(
          "The URI scheme must be the name of the FTP connection \""
              + connectionName
              + "\", not \""
              + scheme
              + "\"");
    } else {
      // Everything after the scheme is a path on the server: prod://tmp/x, prod:///tmp/x and
      // prod:////tmp/x all point at /tmp/x.
      //
      int slashes = 0;
      while (slashes < buffer.length() && buffer.charAt(slashes) == '/') {
        slashes++;
      }
      buffer.delete(0, slashes);
      buffer.insert(0, '/');
    }

    UriParser.canonicalizePath(buffer, 0, buffer.length(), this);
    UriParser.fixSeparators(buffer);
    FileType fileType = UriParser.normalisePath(buffer);
    String path = buffer.toString();

    String host = Const.NVL(variables.resolve(connection.getServerName()), "").trim();
    if (StringUtils.isEmpty(host)) {
      throw new FileSystemException(
          "The FTP connection \"" + connectionName + "\" has no server name");
    }
    int defaultPort = connection.getSecurityMode().getDefaultPort();
    int port = Const.toInt(variables.resolve(connection.getServerPort()), defaultPort);

    // The provider connects to the FTP proxy rather than to the server when there is one, and
    // names the real server in the user name - the same convention the actions follow.
    //
    String proxyHost = variables.resolve(connection.getProxyHost());
    if (StringUtils.isNotEmpty(proxyHost)) {
      port = Const.toInt(variables.resolve(connection.getProxyPort()), port);
      host = proxyHost;
    }

    return new FtpConnectionFileName(
        connectionName,
        host,
        port,
        defaultPort,
        FtpClientFactory.loginUserName(
            variables, connection, Const.NVL(variables.resolve(connection.getServerName()), "")),
        FtpClientFactory.loginPassword(variables, connection),
        path,
        fileType);
  }
}
