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
package org.apache.hop.vfs.hdfs;

import java.util.Collection;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.hdfs.client.HdfsWebHdfsClient;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;

public class HdfsFileProvider extends AbstractOriginatingFileProvider {
  private static final FileSystemOptions DEFAULT_OPTIONS = new FileSystemOptions();

  private final IVariables variables;
  private final HdfsMeta meta;

  public HdfsFileProvider() {
    this(null, null);
  }

  public HdfsFileProvider(IVariables variables, HdfsMeta meta) {
    super();
    this.variables = variables;
    this.meta = meta;
    setFileNameParser(HdfsFileNameParser.getInstance());
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName name, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    FileSystemOptions options = fileSystemOptions != null ? fileSystemOptions : DEFAULT_OPTIONS;
    HdfsFileSystem fileSystem = new HdfsFileSystem(name, options);
    if (meta == null || variables == null) {
      return fileSystem;
    }

    String host = variables.resolve(Const.NVL(meta.getEndpointHostname(), ""));
    if (StringUtils.isEmpty(host)) {
      HdfsHttp.logMissingHost(meta);
    }
    String defaultRoot = variables.resolve(Const.NVL(meta.getDefaultRoot(), ""));
    CloseableHttpClient httpClient = HdfsHttp.createClient(variables, meta);
    HdfsWebHdfsClient client =
        HdfsHttp.createWebHdfsClient(variables, meta, httpClient, fileSystem.getExecutor());
    fileSystem.setClient(client);
    fileSystem.setDefaultRoot(defaultRoot);
    return fileSystem;
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return HdfsFileSystem.CAPABILITIES;
  }
}
