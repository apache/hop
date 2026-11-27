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

package org.apache.hop.config;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fatjar.StagingCleanup;
import org.apache.hop.core.fatjar.VfsStagingStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfsNamespace;
import org.apache.hop.core.vfs.HopVfsNamespaces;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import picocli.CommandLine;

/** Lists a cluster staging folder, or removes the files in it that no run used for a while. */
@ConfigPlugin(
    id = "StagingCleanupConfigPlugin",
    description = "List or clean up a staging folder of cluster runs")
@Getter
@Setter
public class StagingCleanupConfigPlugin implements IConfigOptions {

  @CommandLine.Option(
      names = {"-stl", "--staging-list"},
      description =
          "List the files Hop staged in this folder (fat jars, project packages, Spark runtime"
              + " archives) with their size, last use and whether a cleanup removes them")
  private String listFolder;

  @CommandLine.Option(
      names = {"-stc", "--staging-cleanup"},
      description =
          "Remove the files Hop staged in this folder that no run used within the retention."
              + " Other files in the folder are never touched.")
  private String cleanupFolder;

  @CommandLine.Option(
      names = {"-str", "--staging-retention-days"},
      description =
          "Days a staged file is kept after its last use. Default: HOP_STAGING_RETENTION_DAYS,"
              + " else 14.")
  private String retentionDays;

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    if (StringUtils.isEmpty(listFolder) && StringUtils.isEmpty(cleanupFolder)) {
      return false;
    }
    // The staging folder can be a named VFS connection (MinIO, S3, ...) of this metadata
    IHopMetadataProvider metadataProvider =
        hasHopMetadataProvider != null ? hasHopMetadataProvider.getMetadataProvider() : null;
    HopVfsNamespace namespace =
        HopVfsNamespaces.acquire(variables, metadataProvider, "hop-conf staging cleanup");
    HopVfsNamespace previous = HopVfsNamespaces.bindThread(namespace);
    try {
      handle(log, variables);
    } finally {
      HopVfsNamespaces.restoreThread(previous);
      if (namespace != null) {
        HopVfsNamespaces.release(metadataProvider);
      }
    }
    return true;
  }

  private void handle(ILogChannel log, IVariables variables) throws HopException {
    Duration retention = Duration.ofDays(retention(variables));
    if (StringUtils.isNotEmpty(listFolder)) {
      VfsStagingStore store = store(variables, listFolder);
      List<StagingCleanup.Entry> entries = StagingCleanup.inspect(store, retention, Instant.now());
      log.logBasic(
          entries.size()
              + " staged file(s) in "
              + store.getFolder()
              + ", retention "
              + retention.toDays()
              + " days:");
      for (StagingCleanup.Entry entry : entries) {
        log.logBasic(
            "  "
                + entry.name()
                + "  "
                + size(entry.size())
                + "  last used "
                + (entry.lastUsed() != null ? entry.lastUsed() : "unknown")
                + "  "
                + entry.state().getDescription());
      }
    }
    if (StringUtils.isNotEmpty(cleanupFolder)) {
      VfsStagingStore store = store(variables, cleanupFolder);
      StagingCleanup.Result result = StagingCleanup.cleanUp(log, store, retention, Instant.now());
      log.logBasic(
          "Removed "
              + result.removed().size()
              + " staged file(s) from "
              + store.getFolder()
              + " ("
              + size(result.removedBytes())
              + ")"
              + (result.failed().isEmpty()
                  ? ""
                  : ", " + result.failed().size() + " could not be removed"));
      if (!result.failed().isEmpty()) {
        throw new HopException("Unable to remove " + String.join(", ", result.failed().keySet()));
      }
    }
  }

  static String size(long bytes) {
    return bytes < 1024 * 1024
        ? (bytes + 1023) / 1024 + " kB"
        : String.format("%,.1f MB", bytes / (1024.0 * 1024.0));
  }

  private int retention(IVariables variables) throws HopException {
    if (StringUtils.isEmpty(retentionDays)) {
      int days = StagingCleanup.retentionDays(variables);
      // 0 turns off the automatic cleanup; asked for explicitly, use the default
      return days > 0 ? days : StagingCleanup.DEFAULT_RETENTION_DAYS;
    }
    try {
      int days = Integer.parseInt(variables.resolve(retentionDays).trim());
      if (days < 1) {
        throw new NumberFormatException();
      }
      return days;
    } catch (NumberFormatException e) {
      throw new HopException(
          "The staging retention must be a number of days, 1 or more: " + retentionDays);
    }
  }

  private static VfsStagingStore store(IVariables variables, String folder) throws HopException {
    return new VfsStagingStore(variables, folder, null, "Specify the staging folder");
  }
}
