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

import java.util.Collections;
import java.util.Map;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.webdav4s.Webdav4sFileProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;

/**
 * Registers the built-in secure WebDAV scheme through the VFS plugin mechanism, aligned with other
 * storage plugins. Named providers are loaded by {@link WebDavVfsPlugin}.
 */
@VfsPlugin(
    type = "webdav4s",
    typeDescription = "WebDAV HTTPS VFS plugin",
    classLoaderGroup = "vfs-webdav")
public class WebDavSecureVfsPlugin implements IVfs {

  @Override
  public String[] getUrlSchemes() {
    return new String[] {"webdav4s"};
  }

  @Override
  public FileProvider getProvider() {
    return new Webdav4sFileProvider();
  }

  @Override
  public Map<String, FileProvider> getProviders(IVariables variables) {
    return Collections.emptyMap();
  }
}
