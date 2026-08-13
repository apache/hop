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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.ftp.FtpFileProvider;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;

/**
 * Registers the plain {@code ftp://} scheme, plus every named FTP connection in the metadata under
 * its own scheme. A connection in one of the FTPS security modes gets an FTPS provider, whichever
 * scheme it is named after.
 *
 * <p>{@code ftp://} lives here rather than in Hop core so that all the FTP code stays in one place,
 * the same way the SFTP technology plugin owns {@code sftp://}. {@code ftps://} is registered by
 * {@link FtpsVfsPlugin}: Commons VFS has a separate provider for it and a VFS plugin has room for
 * only one.
 */
@VfsPlugin(
    type = "ftp-connection",
    typeDescription = "FTP VFS (named connections)",
    // Shared with the SFTP plugin: the FTP delete action speaks SFTP through its client.
    classLoaderGroup = "sftp")
public class FtpVfsPlugin implements IVfs {

  @Override
  public String[] getUrlSchemes() {
    return new String[] {"ftp"};
  }

  @Override
  public FileProvider getProvider() {
    return new FtpFileProvider();
  }

  @Override
  public Map<String, FileProvider> getProviders(IVariables variables) {
    Map<String, FileProvider> providers = new HashMap<>();
    try {
      IHopMetadataProvider metadataProvider =
          HopMetadataUtil.getStandardHopMetadataProvider(variables);
      List<FtpConnection> connections =
          metadataProvider.getSerializer(FtpConnection.class).loadAll();
      for (FtpConnection connection : connections) {
        String name = connection.getName();
        if (StringUtils.isEmpty(name)) {
          continue;
        }
        providers.put(
            name,
            connection.getSecurityMode().isSecure()
                ? new FtpsConnectionFileProvider(variables, connection)
                : new FtpConnectionFileProvider(variables, connection));
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Unable to load the FTP VFS providers", e);
    }
    return providers;
  }
}
