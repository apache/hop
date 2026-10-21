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
package org.apache.hop.vfs.sftp.provider;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.URLFileName;

/**
 * The name of a file on an SFTP server, which remembers whether the user rooted it at a second
 * slash: {@code sftp://host/A.B.C} and {@code sftp://host//A.B.C} are two different files, and on
 * z/OS the second is an MVS data set.
 *
 * @see IRootPrefixFileName
 */
public class SftpFileName extends URLFileName implements IRootPrefixFileName {

  private final String rootPrefix;

  public SftpFileName(
      String scheme,
      String hostName,
      int port,
      int defaultPort,
      String userName,
      String password,
      String path,
      FileType type,
      String queryString,
      String rootPrefix) {
    super(scheme, hostName, port, defaultPort, userName, password, path, type, queryString);
    this.rootPrefix = rootPrefix == null ? "" : rootPrefix;
  }

  @Override
  public String getRootPrefix() {
    return rootPrefix;
  }

  /** Children and parents keep the root they were reached through. */
  @Override
  public FileName createName(String absPath, FileType type) {
    return new SftpFileName(
        getScheme(),
        getHostName(),
        getPort(),
        getDefaultPort(),
        getUserName(),
        getPassword(),
        absPath,
        type,
        getQueryString(),
        rootPrefix);
  }

  /**
   * The extra slash belongs to the root, so it lands between the host and the path and the URI
   * reads back as the user typed it: {@code sftp://host//A.B.C}.
   */
  @Override
  protected void appendRootUri(StringBuilder buffer, boolean addPassword) {
    super.appendRootUri(buffer, addPassword);
    buffer.append(rootPrefix);
  }
}
