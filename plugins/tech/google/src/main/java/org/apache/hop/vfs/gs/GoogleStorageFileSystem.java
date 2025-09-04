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

package org.apache.hop.vfs.gs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.Collection;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.threeten.bp.Duration;

public class GoogleStorageFileSystem extends AbstractFileSystem {

  Storage storage = null;
  FileSystemOptions fileSystemOptions;

  protected GoogleStorageFileSystem(
      FileName rootName, FileObject parentLayer, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    super(rootName, parentLayer, fileSystemOptions);
    this.fileSystemOptions = fileSystemOptions;
  }

  @Override
  protected FileObject createFile(AbstractFileName name) throws Exception {
    return new GoogleStorageFileObject(
        GoogleStorageFileSystemConfigBuilder.getInstance().getSchema(fileSystemOptions),
        name,
        this);
  }

  @Override
  protected void addCapabilities(Collection<Capability> caps) {
    caps.addAll(GoogleStorageFileProvider.capabilities);
  }

  Storage setupStorage() {
    if (storage != null) {
      return storage;
    }
    RetrySettings retrySettings =
        StorageOptions.getDefaultRetrySettings().toBuilder()
            // Set the max number of attempts to 10 (initial attempt plus 9 retries)
            .setMaxAttempts(10)
            // Set the backoff multiplier to 3.0
            .setRetryDelayMultiplier(3.0)
            // Set the max duration of all attempts to 5 minutes
            .setTotalTimeout(Duration.ofMinutes(5))
            .build();

    StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();
    optionsBuilder.setCredentials(
        GoogleStorageFileSystemConfigBuilder.getInstance().getGoogleCredentials(fileSystemOptions));
    optionsBuilder.setRetrySettings(retrySettings);

    return storage = optionsBuilder.build().getService();
  }

  String getBucketName(FileName name) {

    String path = name.getPath();
    int idx = path.indexOf('/', 1);
    if (idx > -1) {
      return name.getPath().substring(1, idx);
    } else {
      return name.getPath().substring(1);
    }
  }

  String getBucketPath(FileName name) {
    int idx = name.getPath().indexOf('/', 1);
    if (idx > -1) {
      return name.getPath().substring(idx + 1);
    } else {
      return "";
    }
  }
}
