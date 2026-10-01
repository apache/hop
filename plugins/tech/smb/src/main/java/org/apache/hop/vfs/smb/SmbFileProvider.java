/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.vfs.smb;

import com.hierynomus.smbj.auth.AuthenticationContext;
import java.util.Collection;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.smb.metadata.SmbConnection;

public class SmbFileProvider extends AbstractOriginatingFileProvider {
  private final IVariables variables;
  private final SmbConnection connection;
  private final SmbShare injected;
  private final String injectedBasePath;

  public SmbFileProvider(IVariables variables, SmbConnection connection) {
    this.variables = variables;
    this.connection = connection;
    this.injected = null;
    this.injectedBasePath = null;
    setFileNameParser(SmbFileNameParser.getInstance());
  }

  /** Test constructor. The share is already open and is not built from metadata. */
  public SmbFileProvider(SmbShare share, String basePath) {
    this.variables = null;
    this.connection = null;
    this.injected = share;
    this.injectedBasePath = basePath == null ? "" : basePath;
    setFileNameParser(SmbFileNameParser.getInstance());
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return SmbFileSystem.CAPABILITIES;
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName name, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    if (injected != null) {
      return new SmbFileSystem(name, fileSystemOptions, injected, injectedBasePath);
    }
    try {
      SmbSettings settings = SmbSettings.resolve(connection, variables);
      AuthenticationContext context =
          SmbAuthenticators.forType(settings.authType()).authenticate(connection, variables);
      String domain = "";
      String username = "";
      if (!context.isGuest() && !context.isAnonymous()) {
        domain = context.getDomain() == null ? "" : context.getDomain();
        username = context.getUsername() == null ? "" : context.getUsername();
      }
      SmbShare share =
          new SmbjSmbShare(
              settings.toClientConfig(),
              settings.host(),
              settings.port(),
              settings.share(),
              domain,
              username,
              context);
      return new SmbFileSystem(name, fileSystemOptions, share, settings.basePath());
    } catch (RuntimeException e) {
      throw new FileSystemException(e.getMessage(), e);
    }
  }
}
