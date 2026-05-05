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
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.GenericURLFileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileName;

/**
 * Some Commons VFS / webdav4 versions return {@link GenericURLFileName} from parse/create; {@link
 * Webdav4FileSystem#resolveFile} expects {@link Webdav4FileName}.
 */
final class HopWebDavWireNames {

  private HopWebDavWireNames() {}

  static Webdav4FileName asWebdav4(FileName name) throws FileSystemException {
    if (name instanceof Webdav4FileName) {
      return (Webdav4FileName) name;
    }
    if (name instanceof GenericURLFileName) {
      GenericURLFileName g = (GenericURLFileName) name;
      boolean trailing = g.getType() == FileType.FOLDER;
      return new Webdav4FileName(
          g.getScheme(),
          g.getHostName(),
          g.getPort(),
          g.getDefaultPort(),
          g.getUserName(),
          g.getPassword(),
          g.getPath(),
          g.getType(),
          g.getQueryString(),
          trailing);
    }
    throw new FileSystemException("Unexpected WebDAV wire file name type: " + name.getClass());
  }
}
