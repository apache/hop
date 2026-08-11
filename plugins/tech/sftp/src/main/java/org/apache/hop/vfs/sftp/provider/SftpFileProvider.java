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
package org.apache.hop.vfs.sftp.provider;

import com.jcraft.jsch.Session;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;

/** A provider for accessing files over SFTP. */
public class SftpFileProvider extends AbstractOriginatingFileProvider {

  /** User Information. */
  public static final String ATTR_USER_INFO = "UI";

  /** Authentication types. */
  public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = {
    UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD
  };

  /** The provider's capabilities. */
  protected static final Collection<Capability> capabilities =
      Collections.unmodifiableCollection(
          Arrays.asList(
              Capability.CREATE,
              Capability.DELETE,
              Capability.RENAME,
              Capability.GET_TYPE,
              Capability.LIST_CHILDREN,
              Capability.READ_CONTENT,
              Capability.URI,
              Capability.WRITE_CONTENT,
              Capability.GET_LAST_MODIFIED,
              Capability.SET_LAST_MODIFIED_FILE,
              Capability.RANDOM_ACCESS_READ,
              Capability.APPEND_CONTENT));

  /**
   * Creates a new Session.
   *
   * @return A Session, never null.
   */
  static Session createSession(
      final GenericFileName rootName, final FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    UserAuthenticationData authData = null;
    try {
      authData = UserAuthenticatorUtils.authenticate(fileSystemOptions, AUTHENTICATOR_TYPES);

      return SftpClientFactory.createConnection(
          rootName.getHostName(),
          rootName.getPort(),
          UserAuthenticatorUtils.getData(
              authData,
              UserAuthenticationData.USERNAME,
              UserAuthenticatorUtils.toChar(rootName.getUserName())),
          UserAuthenticatorUtils.getData(
              authData,
              UserAuthenticationData.PASSWORD,
              UserAuthenticatorUtils.toChar(rootName.getPassword())),
          fileSystemOptions);
    } catch (final Exception e) {
      throw new FileSystemException("vfs.provider.sftp/connect.error", rootName, e);
    } finally {
      UserAuthenticatorUtils.cleanup(authData);
    }
  }

  /** Constructs a new provider. */
  public SftpFileProvider() {
    setFileNameParser(SftpFileNameParser.getInstance());
  }

  /** Creates a {@link FileSystem}. */
  @Override
  protected FileSystem doCreateFileSystem(
      final FileName name, final FileSystemOptions fileSystemOptions) throws FileSystemException {
    // Create the file system
    return new SftpFileSystem(
        (GenericFileName) name,
        createSession((GenericFileName) name, fileSystemOptions),
        fileSystemOptions);
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return capabilities;
  }

  @Override
  public FileSystemConfigBuilder getConfigBuilder() {
    return SftpFileSystemConfigBuilder.getInstance();
  }
}
