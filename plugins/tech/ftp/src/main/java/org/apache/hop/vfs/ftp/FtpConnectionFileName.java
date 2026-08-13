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
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.GenericFileName;

/**
 * The name of a file behind a named FTP connection.
 *
 * <p>The server, port and credentials are carried along for the FTP provider to connect with, but
 * they're deliberately kept out of the URI: {@code prod:///inbox/customers.csv} is what the user
 * typed and what they get back from {@link #getURI()}, whichever server the connection happens to
 * point at today. That also means a URI taken from a file object can be handed straight back to
 * VFS.
 */
public class FtpConnectionFileName extends GenericFileName {

  protected FtpConnectionFileName(
      String scheme,
      String hostName,
      int port,
      int defaultPort,
      String userName,
      String password,
      String path,
      FileType type) {
    super(
        scheme,
        hostName,
        port,
        defaultPort,
        userName,
        password,
        path,
        type == null ? FileType.IMAGINARY : type);
  }

  /** {@code <connection name>://}, without server or credentials. */
  @Override
  protected void appendRootUri(StringBuilder buffer, boolean addPassword) {
    buffer.append(getScheme()).append("://");
  }

  @Override
  public FileName createName(String absPath, FileType type) {
    return new FtpConnectionFileName(
        getScheme(),
        getHostName(),
        getPort(),
        getDefaultPort(),
        getUserName(),
        getPassword(),
        absPath,
        type);
  }
}
