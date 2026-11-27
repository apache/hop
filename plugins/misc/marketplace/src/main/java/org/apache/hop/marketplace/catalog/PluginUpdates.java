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

package org.apache.hop.marketplace.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.util.VersionCompare;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.resolve.MavenCoordinates;

/** Which marketplace-installed plugins have a newer version among the discovered plugins. */
public final class PluginUpdates {

  private PluginUpdates() {}

  /** An installed plugin and the newest version discovery found for it. */
  public record Update(InstallReceipt receipt, OptionalPluginInfo latest) {}

  /**
   * The newest discovered version of the receipt's plugin, when it is newer than the installed one.
   * Null otherwise, and for a plugin that is staged or being uninstalled. A snapshot is no update
   * for a release: it is offered only to an installed snapshot.
   */
  public static OptionalPluginInfo newerVersion(
      InstallReceipt receipt, Collection<OptionalPluginInfo> discovered) {
    if (receipt == null
        || discovered == null
        || receipt.isPendingActivation()
        || receipt.isUninstallPending()
        || StringUtils.isAnyBlank(receipt.getArtifactId(), receipt.getVersion())) {
      return null;
    }
    OptionalPluginInfo newest = null;
    for (OptionalPluginInfo info : discovered) {
      if (info == null || StringUtils.isBlank(info.getVersion()) || !sameArtifact(receipt, info)) {
        continue;
      }
      if (VersionCompare.isSnapshot(info.getVersion())
          && !VersionCompare.isSnapshot(receipt.getVersion())) {
        continue;
      }
      if (newest == null || VersionCompat.compare(info.getVersion(), newest.getVersion()) > 0) {
        newest = info;
      }
    }
    return newest != null && VersionCompat.compare(newest.getVersion(), receipt.getVersion()) > 0
        ? newest
        : null;
  }

  /**
   * Whether discovery found any version of the receipt's plugin. When it did not, there is no
   * telling whether the plugin is up to date: it came from a repository that cannot be browsed, or
   * one that did not answer.
   */
  public static boolean isDiscovered(
      InstallReceipt receipt, Collection<OptionalPluginInfo> discovered) {
    if (receipt == null || discovered == null || StringUtils.isBlank(receipt.getArtifactId())) {
      return false;
    }
    for (OptionalPluginInfo info : discovered) {
      if (info != null
          && StringUtils.isNotBlank(info.getVersion())
          && sameArtifact(receipt, info)) {
        return true;
      }
    }
    return false;
  }

  /**
   * What an update downloads. A discovered entry without a group id (Apache catalog entries,
   * definition plugins) keeps the group id of the installed plugin.
   */
  public static MavenCoordinates coordinates(Update update, MarketplaceConfig config) {
    String groupId =
        StringUtils.firstNonBlank(
            update.latest().getGroupId(),
            update.receipt().getGroupId(),
            config == null ? null : config.getGroupId());
    return new MavenCoordinates(
        StringUtils.defaultIfBlank(groupId, "org.apache.hop"),
        update.receipt().getArtifactId(),
        update.latest().getVersion());
  }

  /** The installed plugins that have a newer version, in the order of {@code receipts}. */
  public static List<Update> outdated(
      Collection<InstallReceipt> receipts, Collection<OptionalPluginInfo> discovered) {
    List<Update> updates = new ArrayList<>();
    for (InstallReceipt receipt : receipts) {
      OptionalPluginInfo latest = newerVersion(receipt, discovered);
      if (latest != null) {
        updates.add(new Update(receipt, latest));
      }
    }
    return updates;
  }

  /**
   * The repository to try first for an update: the one discovery found the new version in, else the
   * one the installed version came from, when it is still configured.
   */
  public static String preferredRepository(Update update, MarketplaceConfig config) {
    if (config == null) {
      return null;
    }
    String source = update.latest().getSource();
    if (StringUtils.isNotBlank(source)
        && !"apache".equalsIgnoreCase(source)
        && config.findRepository(source) != null) {
      return source;
    }
    String installedFrom = update.receipt().getRepositoryId();
    return StringUtils.isNotBlank(installedFrom) && config.findRepository(installedFrom) != null
        ? installedFrom
        : null;
  }

  /**
   * Same artifact id, and the same group id when both have one: Apache catalog entries have none,
   * and neither do receipts written before the group id was recorded.
   */
  static boolean sameArtifact(InstallReceipt receipt, OptionalPluginInfo info) {
    if (!receipt.getArtifactId().equalsIgnoreCase(info.getArtifactId())) {
      return false;
    }
    return StringUtils.isAnyBlank(receipt.getGroupId(), info.getGroupId())
        || receipt.getGroupId().equalsIgnoreCase(info.getGroupId());
  }
}
