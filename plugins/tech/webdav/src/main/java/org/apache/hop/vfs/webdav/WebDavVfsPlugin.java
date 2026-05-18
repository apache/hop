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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileProvider;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.vfs.webdav.metadata.WebDavConnection;

@VfsPlugin(
    type = "webdav-connection",
    typeDescription = "WebDAV VFS (named connections)",
    classLoaderGroup = "vfs-webdav")
public class WebDavVfsPlugin implements IVfs {

  @Override
  public String[] getUrlSchemes() {
    return new String[] {"webdav4"};
  }

  @Override
  public FileProvider getProvider() {
    return new Webdav4FileProvider();
  }

  @Override
  public Map<String, FileProvider> getProviders(IVariables variables) {
    Map<String, FileProvider> providers = new HashMap<>();
    try {
      IHopMetadataProvider metadataProvider =
          HopMetadataUtil.getStandardHopMetadataProvider(variables);
      List<WebDavConnection> connections =
          metadataProvider.getSerializer(WebDavConnection.class).loadAll();
      for (WebDavConnection connection : connections) {
        String name = connection.getName();
        if (StringUtils.isEmpty(name)) {
          continue;
        }
        providers.put(name, new HopWebDavFileProvider(variables, connection));
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Unable to load WebDAV VFS providers", e);
    }
    return providers;
  }
}
