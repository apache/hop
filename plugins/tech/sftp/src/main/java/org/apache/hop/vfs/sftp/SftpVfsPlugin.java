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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.apache.hop.vfs.sftp.provider.SftpFileProvider;

/**
 * Registers the plain {@code sftp://} scheme, plus every named SFTP connection in the metadata
 * under its own scheme.
 *
 * <p>Both are served by {@link org.apache.hop.vfs.sftp.provider}, Hop's fork of the Commons VFS
 * SFTP provider - see that package for why the fork exists. It lives here rather than in Hop core
 * so that all the SFTP code stays in one place, which means {@code sftp://} is available wherever
 * this plugin is installed.
 */
@VfsPlugin(
    type = "sftp-connection",
    typeDescription = "SFTP VFS (named connections)",
    classLoaderGroup = "sftp")
public class SftpVfsPlugin implements IVfs {

  @Override
  public String[] getUrlSchemes() {
    return new String[] {"sftp"};
  }

  @Override
  public FileProvider getProvider() {
    return new SftpFileProvider();
  }

  @Override
  public Map<String, FileProvider> getProviders(IVariables variables) {
    return getProviders(variables, null);
  }

  @Override
  public Map<String, FileProvider> getProviders(
      IVariables variables, IHopMetadataProvider executionMetadata) {
    Map<String, FileProvider> providers = new HashMap<>();
    try {
      IHopMetadataProvider metadataProvider =
          executionMetadata != null
              ? executionMetadata
              : HopMetadataUtil.getStandardHopMetadataProvider(variables);
      List<SftpConnection> connections =
          metadataProvider.getSerializer(SftpConnection.class).loadAll();
      for (SftpConnection connection : connections) {
        String name = connection.getName();
        if (StringUtils.isEmpty(name)) {
          continue;
        }
        providers.put(name, new SftpConnectionFileProvider(variables, connection));
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Unable to load the SFTP VFS providers", e);
    }
    return providers;
  }
}
