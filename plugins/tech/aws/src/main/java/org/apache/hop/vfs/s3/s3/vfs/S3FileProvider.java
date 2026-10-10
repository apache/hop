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

package org.apache.hop.vfs.s3.s3.vfs;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.s3.metadata.S3AuthType;
import org.apache.hop.vfs.s3.metadata.S3Meta;
import org.apache.hop.vfs.s3.s3common.S3CommonFileProvider;
import org.apache.hop.vfs.s3.s3common.S3CommonFileSystemConfigBuilder;

public class S3FileProvider extends S3CommonFileProvider {

  /** The scheme this provider was designed to support */
  public static final String SCHEME = "s3";

  /** User Information. */
  public static final String ATTR_USER_INFO = "UI";

  private final IVariables variables;
  private final S3Meta s3Meta;

  /** A named connection's options, built once; see {@link #options(FileSystemOptions)}. */
  private FileSystemOptions connectionOptions;

  public S3FileProvider() {
    super();
    this.variables = null;
    this.s3Meta = null;
    setFileNameParser(S3FileNameParser.getInstance());
  }

  /** Named connection: use this provider for URLs like connectionName://bucket/key */
  public S3FileProvider(IVariables variables, S3Meta s3Meta) {
    super();
    this.variables = variables;
    this.s3Meta = s3Meta;
    setFileNameParser(S3FileNameParser.getInstance());
  }

  /**
   * Look file systems up by the options they are created with. Commons VFS caches a file system
   * under its own options but looks it up under the caller's, which are empty when resolving
   * through HopVfs; a named connection's options never matched, so every resolve built a new file
   * system and S3 client, and the provider kept all of them.
   */
  @Override
  protected synchronized FileSystem getFileSystem(
      final FileName rootFileName, final FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    return super.getFileSystem(rootFileName, options(fileSystemOptions));
  }

  @Override
  public FileSystem doCreateFileSystem(
      final FileName name, final FileSystemOptions fileSystemOptions) {
    return new S3FileSystem(name, options(fileSystemOptions));
  }

  /**
   * The options of the file system for the given request. For s3:// the caller's options, or the
   * defaults. For a named connection one options instance, built from the connection the first time
   * it is needed: dedicated, so it never changes the defaults s3:// uses, and the same every time,
   * so its file system is found again.
   */
  private synchronized FileSystemOptions options(final FileSystemOptions fileSystemOptions) {
    if (s3Meta == null || variables == null) {
      return fileSystemOptions != null ? fileSystemOptions : getDefaultFileSystemOptions();
    }
    if (connectionOptions == null) {
      connectionOptions = buildConnectionOptions();
    }
    return connectionOptions;
  }

  private FileSystemOptions buildConnectionOptions() {
    final FileSystemOptions options = new FileSystemOptions();
    S3CommonFileSystemConfigBuilder config = new S3CommonFileSystemConfigBuilder(options);
    String authType = s3Meta.getAuthenticationType();
    if (StringUtils.isEmpty(authType)) {
      if (StringUtils.isNotEmpty(s3Meta.getAccessKey())
          || StringUtils.isNotEmpty(s3Meta.getSecretKey())) {
        authType = S3AuthType.ACCESS_KEYS.name();
      } else if (StringUtils.isNotEmpty(s3Meta.getCredentialsFile())) {
        authType = S3AuthType.CREDENTIALS_FILE.name();
      } else if (S3AuthType.ANONYMOUS.name().equals(s3Meta.getAuthenticationType())) {
        authType = S3AuthType.ANONYMOUS.name();
      } else {
        authType = S3AuthType.DEFAULT.name();
      }
    }
    String region = variables.resolve(s3Meta.getRegion());
    String endpoint = variables.resolve(s3Meta.getEndpoint());

    if (StringUtils.isNotEmpty(region)) {
      config.setRegion(region);
    }
    if (StringUtils.isNotEmpty(endpoint)) {
      config.setEndpoint(endpoint);
    }
    if (S3AuthType.ACCESS_KEYS.name().equals(authType)) {
      String accessKey = variables.resolve(s3Meta.getAccessKey());
      String secretKey = variables.resolve(s3Meta.getSecretKey());
      String sessionToken = variables.resolve(s3Meta.getSessionToken());
      if (StringUtils.isNotEmpty(accessKey)) {
        config.setAccessKey(accessKey);
      }
      if (StringUtils.isNotEmpty(secretKey)) {
        config.setSecretKey(secretKey);
      }
      if (StringUtils.isNotEmpty(sessionToken)) {
        config.setSessionToken(sessionToken);
      }
    } else if (S3AuthType.CREDENTIALS_FILE.name().equals(authType)) {
      String credentialsFile = variables.resolve(s3Meta.getCredentialsFile());
      String profileName = variables.resolve(s3Meta.getProfileName());
      if (StringUtils.isNotEmpty(credentialsFile)) {
        config.setCredentialsFile(credentialsFile);
      }
      if (StringUtils.isNotEmpty(profileName)) {
        config.setProfileName(profileName);
      }
    } else if (S3AuthType.DEFAULT.name().equals(authType)) {
      String profileName = variables.resolve(s3Meta.getProfileName());
      if (StringUtils.isNotEmpty(profileName)) {
        config.setProfileName(profileName);
      }
    } else if (S3AuthType.ANONYMOUS.name().equals(authType)) {
      config.setUseAnonymousAccess(true);
    }
    config.setPathStyleAccess(String.valueOf(s3Meta.isPathStyleAccess()));
    String cacheTtl = variables.resolve(s3Meta.getCacheTtlSeconds());
    if (StringUtils.isNotEmpty(cacheTtl)) {
      config.setCacheTtlSeconds(cacheTtl);
    }
    return options;
  }
}
