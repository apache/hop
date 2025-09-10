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
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.minio.config.MinioConfig;
import org.apache.hop.vfs.minio.config.MinioConfigSingleton;

public class MinioFileProvider extends AbstractOriginatingFileProvider {
  public final long DEFAULT_PART_SIZE = 5 * 1024 * 1024;
  public static final String HOP_MINIO_ENDPOINT_HOSTNAME = "HOP_MINIO_ENDPOINT_HOSTNAME";
  public static final String HOP_MINIO_ENDPOINT_PORT = "HOP_MINIO_ENDPOINT_PORT";
  public static final String HOP_MINIO_ENDPOINT_SECURE = "HOP_MINIO_ENDPOINT_SECURE";
  public static final String HOP_MINIO_ACCESS_KEY = "HOP_MINIO_ACCESS_KEY";
  public static final String HOP_MINIO_SECRET_KEY = "HOP_MINIO_SECRET_KEY";
  public static final String HOP_MINIO_REGION = "HOP_MINIO_REGION";
  public static final String HOP_MINIO_PART_SIZE = "HOP_MINIO_PART_SIZE";

  public MinioFileProvider() {
    super();
    setFileNameParser(MinioFileNameParser.getInstance());
  }

  @Override
  public FileSystem doCreateFileSystem(
      final FileName name, final FileSystemOptions fileSystemOptions) {
    MinioFileSystem fileSystem = new MinioFileSystem(name, fileSystemOptions);
    IVariables variables = Variables.getADefaultVariableSpace();
    MinioConfig config = MinioConfigSingleton.getConfig();

    // The end point hostname
    String endPointHostname =
        Const.NVL(
            variables.resolve(config.getEndPointHostname()),
            variables.getVariable(HOP_MINIO_ENDPOINT_HOSTNAME));
    if (StringUtils.isEmpty(endPointHostname)) {
      LogChannel.GENERAL.logBasic(
          "MinIO VFS: end point hostname not set. "
              + "Set it in the Hop configuration or with variable HOP_MINIO_ENDPOINT_HOSTNAME");
    }
    fileSystem.setEndPointHostname(endPointHostname);

    // The end point port
    String endPointPort =
        Const.NVL(
            variables.resolve(config.getEndPointPort()),
            variables.getVariable(HOP_MINIO_ENDPOINT_PORT));
    fileSystem.setEndPointPort(Const.toInt(endPointPort, 9000));

    // Is the end point secure? (https)
    boolean endPointSecure =
        Const.toBoolean(variables.getVariable(HOP_MINIO_ENDPOINT_SECURE))
            || config.isEndPointSecure();
    fileSystem.setEndPointSecure(endPointSecure);

    // The access key
    String accessKey =
        Const.NVL(
            variables.resolve(config.getAccessKey()), variables.getVariable(HOP_MINIO_ACCESS_KEY));
    if (StringUtils.isEmpty(accessKey)) {
      LogChannel.GENERAL.logBasic(
          "MinIO VFS: no access key set. "
              + "Set it in the Hop configuration or with variable HOP_MINIO_ACCESS_KEY");
    }
    fileSystem.setAccessKey(accessKey);

    // The secret key
    String secretKey =
        Const.NVL(
            variables.resolve(config.getSecretKey()), variables.getVariable(HOP_MINIO_SECRET_KEY));
    fileSystem.setSecretKey(secretKey);
    if (StringUtils.isEmpty(secretKey)) {
      LogChannel.GENERAL.logBasic(
          "MinIO VFS: no secret key set. "
              + "Set it in the Hop configuration or with variable HOP_MINIO_SECRET_KEY");
    }
    // The region
    String region =
        Const.NVL(variables.resolve(config.getRegion()), variables.getVariable(HOP_MINIO_REGION));
    if (StringUtils.isEmpty(region)) {
      LogChannel.GENERAL.logBasic(
          "MinIO VFS: no region set. "
              + "Set it in the Hop configuration or with variable HOP_MINIO_REGION");
    }
    fileSystem.setRegion(region);

    // The part size
    String partSize =
        Const.NVL(
            variables.resolve(config.getRegion()), variables.getVariable(HOP_MINIO_PART_SIZE));
    fileSystem.setPartSize(Const.toLong(partSize, DEFAULT_PART_SIZE));

    return fileSystem;
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return MinioFileSystem.CAPABILITIES;
  }
}
