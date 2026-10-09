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
package org.apache.hop.vfs.git;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPlugin;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.vfs.git.metadata.GitConnection;

/**
 * Registers a provider for every named git connection in the metadata, each under its own name.
 *
 * <p>There is no fixed scheme: a git repository cannot be named by a scheme alone. Which
 * repository, which revision and which credentials all have to come from somewhere, and that
 * somewhere is the connection. So a connection called {@code ops} is what makes {@code
 * ops:///workflows/daily.hwf} work.
 *
 * <p>This is the pattern the other named VFS connections follow (Minio, Databricks, S3, ...), and
 * it is why {@link #getUrlSchemes()} is empty and {@link #getProvider()} is null: a provider the
 * file system manager has no scheme for can never be reached, and cannot be closed either.
 */
@VfsPlugin(
    type = "git-vfs",
    typeDescription = "Git VFS (named connections)",
    classLoaderGroup = "vfs-git")
public class GitVfsPlugin implements IVfs {

  @Override
  public String[] getUrlSchemes() {
    return new String[] {};
  }

  @Override
  public FileProvider getProvider() {
    return null;
  }

  @Override
  public Map<String, FileProvider> getProviders(IVariables variables) {
    return getProviders(variables, null);
  }

  @Override
  public Map<String, FileProvider> getProviders(
      IVariables variables, IHopMetadataProvider executionMetadata) {
    Map<String, FileProvider> providers = new HashMap<>();
    try {
      IHopMetadataProvider metadataProvider =
          executionMetadata != null
              ? executionMetadata
              : HopMetadataUtil.getStandardHopMetadataProvider(variables);
      List<GitConnection> connections =
          metadataProvider.getSerializer(GitConnection.class).loadAll();
      for (GitConnection connection : connections) {
        String name = connection.getName();
        if (StringUtils.isEmpty(name)) {
          continue;
        }
        providers.put(name, new GitFileProvider(variables, connection));
      }
    } catch (Exception e) {
      // Never silently: an unreadable connection here means files resolved through its scheme
      // quietly
      // fail to resolve at all, and a log line is the only trace of why.
      LogChannel.GENERAL.logError("Unable to load the git VFS providers", e);
    }
    return providers;
  }
}
