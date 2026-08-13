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

import java.util.Map;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.ftps.FtpsFileProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;

/**
 * Registers the plain {@code ftps://} scheme. Commons VFS serves FTPS with a provider of its own,
 * and a VFS plugin registers one provider for all of its fixed schemes, so this is a plugin next to
 * {@link FtpVfsPlugin} rather than a second scheme on it. The named connections all live on {@link
 * FtpVfsPlugin}, including the ones which speak FTPS.
 */
@VfsPlugin(
    type = "ftps",
    typeDescription = "FTPS VFS",
    // Same class loader as the rest of the FTP and SFTP code, see FtpVfsPlugin.
    classLoaderGroup = "sftp")
public class FtpsVfsPlugin implements IVfs {

  @Override
  public String[] getUrlSchemes() {
    return new String[] {"ftps"};
  }

  @Override
  public FileProvider getProvider() {
    return new FtpsFileProvider();
  }

  @Override
  public Map<String, FileProvider> getProviders(IVariables variables) {
    return Map.of();
  }
}
