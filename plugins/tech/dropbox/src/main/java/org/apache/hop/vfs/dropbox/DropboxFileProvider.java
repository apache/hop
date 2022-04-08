/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.vfs.dropbox;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hop.vfs.dropbox.config.DropboxConfig;
import org.apache.hop.vfs.dropbox.config.DropboxConfigSingleton;
import java.util.Collection;
import java.util.Set;

/** A provider for dropbox file systems. */
public class DropboxFileProvider extends AbstractOriginatingFileProvider {

  private static final String APPLICATION_NAME = "Apache-Hop-Dropbox-VFS";

  protected static final Collection<Capability> capabilities =
      Set.of(
              Capability.CREATE,
              Capability.DELETE,
              Capability.RENAME,
              Capability.GET_TYPE,
              Capability.LIST_CHILDREN,
              Capability.READ_CONTENT,
              Capability.GET_LAST_MODIFIED,
              Capability.URI,
              Capability.WRITE_CONTENT);

  private static FileSystemOptions defaultOptions = new FileSystemOptions();

  public static FileSystemOptions getDefaultFileSystemOptions() {
    return defaultOptions;
  }

  public DropboxFileProvider() {
    super();
    
    setFileNameParser(DropboxFileNameParser.getInstance());
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName fileName, FileSystemOptions fileSystemOptions)
      throws FileSystemException {

    if (fileSystemOptions == null) {
      fileSystemOptions = getDefaultFileSystemOptions();
    }

    DbxClientV2 client = createClient();

    return new DropboxFileSystem(fileName, client, fileSystemOptions);
  }

  private DbxClientV2 createClient() throws FileSystemException {
    try {
      DropboxConfig config = DropboxConfigSingleton.getConfig();
      if (StringUtils.isEmpty(config.getAccessToken())) {
        throw new FileSystemException(
            "Please configure the Dropbox access token to use in the configuration (Options dialog or with hop-conf)");
      }

      DbxRequestConfig requestConfig =
          DbxRequestConfig.newBuilder(APPLICATION_NAME).withAutoRetryEnabled().build();

      return new DbxClientV2(requestConfig, config.getAccessToken());

    } catch (Exception e) {
      throw new FileSystemException(e);
    }
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return capabilities;
  }
}
