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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.vfs.minio.metadata.MinioMeta;

@VfsPlugin(type = "minio", typeDescription = "S3 VFS plugin", classLoaderGroup = "vfs-s3")
public class MinioVfsPlugin implements IVfs {
  @Override
  public String[] getUrlSchemes() {
    // Not used for Minio.  The URL schemes are derived from metadata.
    return new String[] {};
  }

  @Override
  public FileProvider getProvider() {
    return new MinioFileProvider();
  }

  @Override
  public Map<String, FileProvider> getProviders(IVariables variables) {
    Map<String, FileProvider> providers = new HashMap<>();
    try {
      IHopMetadataProvider metadataProvider =
          HopMetadataUtil.getStandardHopMetadataProvider(variables);
      List<MinioMeta> minioMetaTypes = metadataProvider.getSerializer(MinioMeta.class).loadAll();
      for (MinioMeta minioMeta : minioMetaTypes) {
        providers.put(minioMeta.getName(), new MinioFileProvider(variables, minioMeta));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return providers;
  }
}
