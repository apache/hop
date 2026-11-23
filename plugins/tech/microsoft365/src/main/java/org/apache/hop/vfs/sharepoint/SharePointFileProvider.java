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
 *
 */

package org.apache.hop.vfs.sharepoint;

import com.azure.core.credential.TokenCredential;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import java.util.Collection;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.sharepoint.metadatatype.SharePointConnection;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

@NullMarked
public class SharePointFileProvider extends AbstractOriginatingFileProvider {

  /** The .default scope is required by app-only authentication. */
  private static final String[] SCOPES = {"https://graph.microsoft.com/.default"};

  private final IVariables variables;
  private final SharePointConnection connection;

  public SharePointFileProvider(IVariables variables, SharePointConnection connection) {
    this.variables = variables;
    this.connection = connection;
    setFileNameParser(SharePointFileNameParser.getInstance());
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return SharePointFileSystem.CAPABILITIES;
  }

  @Override
  protected FileSystem doCreateFileSystem(
      FileName name, @Nullable FileSystemOptions fileSystemOptions) throws FileSystemException {
    try {
      LogChannel.GENERAL.logDetailed(
          "Initialize Microsoft Graph client for " + connection.getName());
      TokenCredential credential = connection.credentials(LogChannel.GENERAL, variables);
      GraphServiceClient client = new GraphServiceClient(credential, SCOPES);
      return new SharePointFileSystem(
          name, client, variables.resolve(connection.getHostname()), fileSystemOptions);
    } catch (Exception e) {
      throw new FileSystemException(
          "Unable to connect to Microsoft Graph with connection " + connection.getName(), e);
    }
  }
}
