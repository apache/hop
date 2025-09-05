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
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.minio.config.MinioConfig;
import org.apache.hop.vfs.minio.config.MinioConfigSingleton;

public class MinioFileProvider extends AbstractOriginatingFileProvider {
  public final long DEFAULT_PART_SIZE = 5 * 1024 * 1024;

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

    fileSystem.setEndPointHostname(variables.resolve(config.getEndPointHostname()));
    fileSystem.setEndPointPort(Const.toInt(variables.resolve(config.getEndPointHostname()), 9000));
    fileSystem.setEndPointSecure(config.isEndPointSecure());
    fileSystem.setAccessKey(variables.resolve(config.getAccessKey()));
    fileSystem.setSecretKey(variables.resolve(config.getSecretKey()));
    fileSystem.setRegion(variables.resolve(config.getRegion()));
    fileSystem.setPartSize(
        Const.toLong(variables.resolve(config.getPartSize()), DEFAULT_PART_SIZE));

    return fileSystem;
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return MinioFileSystem.CAPABILITIES;
  }
}
