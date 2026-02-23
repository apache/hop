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

package org.apache.hop.vfs.azure;

import com.azure.storage.file.datalake.DataLakeServiceClient;
import java.util.Collection;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

public class AzureFileSystem extends AbstractFileSystem {

  private final DataLakeServiceClient serviceClient;
  private final String fsName;
  private final String account;
  private long listCacheTtlMs = AzureListCache.DEFAULT_TTL_MS;
  private AzureListCache listCache;

  public AzureFileSystem(
      AzureFileName fileName,
      DataLakeServiceClient serviceClient,
      String fsName,
      FileSystemOptions fileSystemOptions,
      String account)
      throws FileSystemException {
    super(fileName, null, fileSystemOptions);
    this.serviceClient = serviceClient;
    this.fsName = fsName;
    this.account = account;
  }

  public void setListCacheTtlMs(long listCacheTtlMs) {
    this.listCacheTtlMs = listCacheTtlMs;
  }

  private AzureListCache getListCache() {
    if (listCache == null) {
      listCache = new AzureListCache(listCacheTtlMs);
    }
    return listCache;
  }

  @Override
  protected void addCapabilities(Collection<Capability> capabilities) {
    capabilities.addAll(AzureFileProvider.capabilities);
  }

  @Override
  protected FileObject createFile(AbstractFileName name) throws Exception {
    return new AzureFileObject(name, this, serviceClient);
  }

  public String getFilesystemName() {
    return fsName;
  }

  public String getAccount() {
    return account;
  }

  void putListCache(
      String container, String prefix, java.util.Map<String, AzureListCache.ChildInfo> entries) {
    getListCache().put(container, prefix, entries);
  }

  AzureListCache.ChildInfo getFromListCache(
      String container, String parentPrefix, String childPath) {
    return getListCache().get(container, parentPrefix, childPath);
  }

  void invalidateListCache(String container, String prefix) {
    getListCache().invalidate(container, prefix);
  }

  void invalidateListCacheForParentOf(String container, String path) {
    getListCache().invalidateParentOf(container, path);
  }
}
