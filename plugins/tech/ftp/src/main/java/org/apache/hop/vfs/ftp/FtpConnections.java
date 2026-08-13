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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;

/** Looking up named FTP connections. */
public final class FtpConnections {
  private static final Class<?> PKG = FtpConnections.class;

  private FtpConnections() {}

  /**
   * @param metadataProvider where the connections live
   * @param name the name of the connection, variables already resolved
   * @return the connection, never null
   * @throws HopException if there's no connection with that name
   */
  public static FtpConnection load(IHopMetadataProvider metadataProvider, String name)
      throws HopException {
    if (StringUtils.isEmpty(name)) {
      throw new HopException(BaseMessages.getString(PKG, "FtpConnection.Error.NoConnection"));
    }
    FtpConnection connection =
        metadataProvider.getSerializer(FtpConnection.class).load(name.trim());
    if (connection == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "FtpConnection.Error.ConnectionNotFound", name));
    }
    return connection;
  }

  /**
   * The base URI of a connection: {@code <name>://}. Append an absolute path on the server to get
   * to a file.
   */
  public static String getBaseUri(FtpConnection connection) {
    return connection.getName() + "://";
  }

  /**
   * Build the URI of a file behind a named connection. Both the folder and the file name are
   * optional.
   */
  public static String buildUri(FtpConnection connection, String folder, String filename) {
    StringBuilder uri = new StringBuilder(getBaseUri(connection));
    String path = StringUtils.isEmpty(folder) ? "" : folder.replace('\\', '/').trim();
    if (StringUtils.isNotEmpty(filename)) {
      if (StringUtils.isNotEmpty(path) && !path.endsWith("/")) {
        path += "/";
      }
      path += filename.replace('\\', '/').trim();
    }
    while (path.startsWith("/")) {
      path = path.substring(1);
    }
    return uri.append(path).toString();
  }
}
