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
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileProvider;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileSystemConfigBuilder;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.webdav.metadata.WebDavConnection;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;

/** WebDAV over HTTP with credentials from {@link WebDavConnection} metadata. */
public class HopWebDav4ConnectionFileProvider extends Webdav4FileProvider {

  private static final UserAuthenticationData.Type[] AUTH_TYPES =
      new UserAuthenticationData.Type[] {
        UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD
      };

  private final IVariables variables;
  private final WebDavConnection meta;

  public HopWebDav4ConnectionFileProvider(IVariables variables, WebDavConnection meta) {
    this.variables = variables;
    this.meta = meta;
    setFileNameParser(new HopWebDavConnectionFileNameParser(variables, meta));
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName name, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    if (!(name instanceof GenericFileName)) {
      throw new FileSystemException("Unsupported FileName type: " + name);
    }
    GenericFileName genericRoot = (GenericFileName) name;
    FileSystemOptions opts =
        fileSystemOptions != null
            ? (FileSystemOptions) fileSystemOptions.clone()
            : new FileSystemOptions();
    HopWebDavConnectionAuth.apply(opts, variables, meta);
    Webdav4FileSystemConfigBuilder builder = Webdav4FileSystemConfigBuilder.getInstance();
    UserAuthenticationData auth = null;
    try {
      auth = UserAuthenticatorUtils.authenticate(opts, AUTH_TYPES);
      HttpClientContext ctx = createHttpClientContext(builder, genericRoot, opts, auth);
      HttpClient client = createHttpClient(builder, genericRoot, opts);
      String rootPrefix =
          HopWebDavLogicalUris.wirePathPrefixFromRootUrl(
              Const.NVL(variables.resolve(meta.getRootUrl()), "").trim());
      return new HopWebDavConnectionFileSystem(name, opts, client, ctx, meta.getName(), rootPrefix);
    } finally {
      UserAuthenticatorUtils.cleanup(auth);
    }
  }
}
