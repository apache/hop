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

package org.apache.hop.databricks.deploy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fatjar.FatJarCache;
import org.apache.hop.core.fatjar.IStagingStore;
import org.apache.hop.databricks.client.DatabricksJobsClient;
import org.apache.hop.databricks.client.DirectoryEntry;
import org.apache.hop.databricks.client.WorkspaceFileMetadata;

/**
 * A staging folder on a Databricks volume (or DBFS), written through the Files API of the
 * workspace. Jobs read the files at the same path.
 */
final class DatabricksVolumeStagingStore implements IStagingStore {

  private final DatabricksJobsClient client;
  private final String base;

  /**
   * @param base normalized base folder, for example {@code /Volumes/catalog/schema/volume/hop}
   */
  DatabricksVolumeStagingStore(DatabricksJobsClient client, String base) {
    this.client = client;
    this.base = base;
  }

  @Override
  public long size(String name) throws HopException {
    WorkspaceFileMetadata metadata = client.getFileMetadata(location(name));
    return metadata.exists() ? metadata.sizeBytes() : -1L;
  }

  @Override
  public void upload(Path localFile, String name) throws HopException {
    client.uploadToDbfs(localFile, location(name));
    if (name.endsWith(".jar")) {
      // Volumes expose no content checksum: the sidecar lets people check a jar with sha256sum
      client.uploadText(location(name) + ".sha256", FatJarCache.sha256Of(localFile) + "\n");
    }
  }

  /** Volumes and workspace files can be listed and deleted; classic DBFS paths are not cleaned. */
  @Override
  public boolean supportsCleanup() {
    return client.canListAndDelete(base);
  }

  @Override
  public List<StagedFile> list() throws HopException {
    List<StagedFile> files = new ArrayList<>();
    List<DirectoryEntry> entries;
    try {
      entries = client.listDirectory(base);
    } catch (HopException e) {
      if (String.valueOf(e.getMessage()).contains("HTTP 404")) {
        // Nothing staged yet
        return files;
      }
      throw e;
    }
    for (DirectoryEntry entry : entries) {
      if (!entry.directory()) {
        files.add(new StagedFile(entry.name(), entry.sizeBytes(), entry.lastModifiedEpochMs()));
      }
    }
    return files;
  }

  @Override
  public void delete(String name) throws HopException {
    client.deleteFile(location(name));
  }

  @Override
  public void writeText(String name, String text) throws HopException {
    client.uploadText(location(name), text);
  }

  @Override
  public String readText(String name) throws HopException {
    return client.downloadTextIfExists(location(name)).orElse(null);
  }

  @Override
  public String location(String name) {
    return base + "/" + name;
  }

  @Override
  public String clusterUrl(String name) {
    return location(name);
  }
}
