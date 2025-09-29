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

package org.apache.hop.vfs.minio;

import java.util.Collection;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.minio.metadata.MinioMeta;

public class MinioFileProvider extends AbstractOriginatingFileProvider {
  public final long DEFAULT_PART_SIZE = 5 * 1024 * 1024;

  private static final FileSystemOptions defaultOptions = new FileSystemOptions();

  public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES =
      new UserAuthenticationData.Type[] {
        UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD
      };

  public static FileSystemOptions getDefaultFileSystemOptions() {
    return defaultOptions;
  }

  private IVariables variables;

  private MinioMeta minioMeta;

  public MinioFileProvider() {
    super();
    setFileNameParser(MinioFileNameParser.getInstance());
  }

  public MinioFileProvider(IVariables variables, MinioMeta minioMeta) {
    super();
    this.variables = variables;
    this.minioMeta = minioMeta;
    setFileNameParser(MinioFileNameParser.getInstance());
  }

  @Override
  public FileSystem doCreateFileSystem(
      final FileName name, final FileSystemOptions fileSystemOptions) {

    FileSystemOptions fsOptions =
        fileSystemOptions != null ? fileSystemOptions : getDefaultFileSystemOptions();
    MinioFileSystem fileSystem = new MinioFileSystem(name, fsOptions);

    UserAuthenticationData authData = null;

    try {
      authData = UserAuthenticatorUtils.authenticate(fsOptions, AUTHENTICATOR_TYPES);

      // This is the case for the default VFS initialization.
      // Beyond this, it's not used.
      if (minioMeta == null || variables == null) {
        return fileSystem;
      }

      // The end point hostname
      String endPointHostname = variables.resolve(minioMeta.getEndPointHostname());
      if (StringUtils.isEmpty(endPointHostname)) {
        LogChannel.GENERAL.logBasic(
            "MinIO VFS: end point hostname not set. "
                + "Set it in the Hop configuration or with variable HOP_MINIO_ENDPOINT_HOSTNAME");
      }
      fileSystem.setEndPointHostname(endPointHostname);

      // The end point port
      String endPointPort = variables.resolve(minioMeta.getEndPointPort());
      fileSystem.setEndPointPort(Const.toInt(endPointPort, 9000));

      // Is the end point secure? (https)
      boolean endPointSecure = minioMeta.isEndPointSecure();
      fileSystem.setEndPointSecure(endPointSecure);

      // The access key
      String accessKey = variables.resolve(minioMeta.getAccessKey());
      if (StringUtils.isEmpty(accessKey)) {
        LogChannel.GENERAL.logBasic(
            "MinIO VFS: no access key set. "
                + "Set it in the Hop configuration or with variable HOP_MINIO_ACCESS_KEY");
      }
      fileSystem.setAccessKey(accessKey);

      // The secret key
      String secretKey = variables.resolve(minioMeta.getSecretKey());
      fileSystem.setSecretKey(secretKey);
      if (StringUtils.isEmpty(secretKey)) {
        LogChannel.GENERAL.logBasic(
            "MinIO VFS: no secret key set. "
                + "Set it in the Hop configuration or with variable HOP_MINIO_SECRET_KEY");
      }
      // The region
      String region = variables.resolve(minioMeta.getRegion());
      if (StringUtils.isEmpty(region)) {
        LogChannel.GENERAL.logBasic(
            "MinIO VFS: no region set. "
                + "Set it in the Hop configuration or with variable HOP_MINIO_REGION");
      }
      fileSystem.setRegion(region);

      // The part size
      String partSize = variables.resolve(minioMeta.getRegion());
      fileSystem.setPartSize(Const.toLong(partSize, DEFAULT_PART_SIZE));

      return fileSystem;
    } finally {
      UserAuthenticatorUtils.cleanup(authData);
    }
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return MinioFileSystem.CAPABILITIES;
  }
}
