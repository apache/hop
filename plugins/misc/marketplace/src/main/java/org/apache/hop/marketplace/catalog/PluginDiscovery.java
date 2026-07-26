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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.resolve.MavenCoordinates;

/**
 * Discovers marketplace plugins for query / GUI / install coordinate resolution.
 *
 * <p>Always includes the bundled Apache optional catalog, plus a <strong>live</strong> listing of
 * every enabled repository with {@code browse=true} (Nexus zip search). Optional plugin metadata
 * embedded in the repository definition enriches live results; it is not a cache.
 */
public final class PluginDiscovery {

  private PluginDiscovery() {}

  /**
   * Resolved install target: Maven coordinates plus an optional preferred repository id (from
   * discovery {@code source}) tried first in the install fallback chain.
   */
  public record InstallTarget(MavenCoordinates coordinates, String preferredRepoId) {}

  /**
   * Full marketplace discovery: bundled Apache optional catalog plus every enabled repository with
   * {@code browse=true} (live Nexus zip list or optional catalog URL).
   *
   * @param filter free-text filter (may be blank)
   * @param repoId if non-blank, only that browse repository among remotes (Apache catalog still
   *     included)
   */
  public static List<OptionalPluginInfo> query(
      String filter, String repoId, MarketplaceConfig config, ILogChannel log) {
    String hopVersion = resolveHopVersion(config);
    Map<String, OptionalPluginInfo> byKey = new LinkedHashMap<>();

    for (OptionalPluginInfo info : OptionalPluginCatalog.query(filter)) {
      if (info == null) {
        continue;
      }
      // Apache optional plugins track the running Hop line — not absolute repo <latest>.
      if (StringUtils.isBlank(info.getVersion()) && StringUtils.isNotBlank(hopVersion)) {
        info.setVersion(hopVersion);
      }
      if (StringUtils.isBlank(info.getSource())) {
        info.setSource("apache");
      }
      byKey.putIfAbsent(key(info), info);
    }

    if (config != null && config.getRepositories() != null) {
      for (MarketplaceRepository repo : config.getRepositories()) {
        if (repo == null || !repo.isEnabled() || !repo.isBrowse()) {
          continue;
        }
        if (StringUtils.isNotBlank(repoId) && !repoId.equals(repo.getId())) {
          continue;
        }
        try {
          for (OptionalPluginInfo info : discoverRepoLive(repo, filter, log)) {
            if (info == null) {
              continue;
            }
            if (!VersionCompat.isCompatibleWithHop(info, hopVersion)) {
              continue;
            }
            byKey.putIfAbsent(key(info), info);
          }
        } catch (Exception e) {
          if (log != null) {
            log.logError(
                "Marketplace discovery failed for repository '"
                    + repo.getId()
                    + "': "
                    + e.getMessage(),
                e);
          }
        }
      }
    }

    return new ArrayList<>(byKey.values());
  }

  /**
   * Resolve an install coordinate the same way users search with {@code marketplace query}.
   *
   * <ul>
   *   <li>{@code groupId:artifactId:version} — used as-is (no discovery)
   *   <li>{@code artifactId} or short name (e.g. {@code datavault}) — discovery for unique match;
   *       uses discovered artifact id, group, version, and preferred source repo
   *   <li>{@code artifactId:version} — discovery for artifact/group/source; version from the user
   *   <li>No discovery hit — falls back to literal {@link MavenCoordinates#parse} with defaults
   * </ul>
   */
  public static InstallTarget resolveInstall(
      String spec,
      String defaultGroupId,
      String defaultVersion,
      MarketplaceConfig config,
      ILogChannel log)
      throws HopException {
    if (StringUtils.isBlank(spec)) {
      throw new HopException("Plugin coordinate is empty");
    }
    String trimmed = spec.trim();
    String[] parts = trimmed.split(":");
    if (parts.length > 3) {
      throw new HopException(
          "Invalid coordinate '"
              + spec
              + "'. Use artifactId, artifactId:version, or groupId:artifactId:version");
    }
    if (parts.length == 3) {
      return new InstallTarget(new MavenCoordinates(parts[0], parts[1], parts[2]), null);
    }

    String token = parts[0];
    String userVersion = parts.length == 2 ? parts[1] : null;

    List<OptionalPluginInfo> matches = query(token, null, config, log);
    OptionalPluginInfo chosen = pickInstallMatch(token, matches);
    if (chosen == null) {
      return new InstallTarget(
          MavenCoordinates.parse(trimmed, defaultGroupId, defaultVersion), null);
    }

    String groupId =
        StringUtils.isNotBlank(chosen.getGroupId()) ? chosen.getGroupId() : defaultGroupId;
    String artifactId = chosen.getArtifactId();
    String version;
    if (StringUtils.isNotBlank(userVersion)) {
      version = userVersion;
    } else if (StringUtils.isNotBlank(chosen.getVersion())) {
      version = chosen.getVersion();
    } else {
      version = defaultVersion;
    }
    if (StringUtils.isBlank(version)) {
      throw new HopException(
          "No version for '"
              + artifactId
              + "'. Use artifactId:version or groupId:artifactId:version");
    }

    String preferredRepo = null;
    if (StringUtils.isNotBlank(chosen.getSource())
        && !"apache".equalsIgnoreCase(chosen.getSource())
        && config != null
        && config.findRepository(chosen.getSource()) != null) {
      preferredRepo = chosen.getSource();
    }

    return new InstallTarget(new MavenCoordinates(groupId, artifactId, version), preferredRepo);
  }

  /**
   * Choose a single install target from discovery matches for {@code token} (artifact id or short
   * name). Prefer exact artifact id, then unique artifact-id substring match, then unique overall
   * match. Throws when multiple candidates remain.
   */
  static OptionalPluginInfo pickInstallMatch(String token, List<OptionalPluginInfo> matches)
      throws HopException {
    if (matches == null || matches.isEmpty() || StringUtils.isBlank(token)) {
      return null;
    }
    String needle = token.trim().toLowerCase(Locale.ROOT);

    List<OptionalPluginInfo> exactArtifact = new ArrayList<>();
    List<OptionalPluginInfo> artifactContains = new ArrayList<>();
    for (OptionalPluginInfo info : matches) {
      if (info == null || StringUtils.isBlank(info.getArtifactId())) {
        continue;
      }
      String aid = info.getArtifactId().toLowerCase(Locale.ROOT);
      if (aid.equals(needle)) {
        exactArtifact.add(info);
      } else if (aid.contains(needle)) {
        artifactContains.add(info);
      }
    }

    if (exactArtifact.size() == 1) {
      return exactArtifact.get(0);
    }
    if (exactArtifact.size() > 1) {
      // Same artifact from multiple sources/versions — prefer first (query order)
      return exactArtifact.get(0);
    }
    if (artifactContains.size() == 1) {
      return artifactContains.get(0);
    }
    if (artifactContains.size() > 1) {
      throw ambiguous(token, artifactContains);
    }

    // Description/name-only hits: allow only when unique
    List<OptionalPluginInfo> withArtifact =
        matches.stream()
            .filter(i -> i != null && StringUtils.isNotBlank(i.getArtifactId()))
            .collect(Collectors.toList());
    if (withArtifact.size() == 1) {
      return withArtifact.get(0);
    }
    if (withArtifact.size() > 1) {
      throw ambiguous(token, withArtifact);
    }
    return null;
  }

  private static HopException ambiguous(String token, List<OptionalPluginInfo> candidates) {
    String list =
        candidates.stream()
            .map(
                i ->
                    "  - "
                        + nullToEmpty(i.getGroupId())
                        + ":"
                        + i.getArtifactId()
                        + ":"
                        + (StringUtils.isNotBlank(i.getVersion()) ? i.getVersion() : "?")
                        + (StringUtils.isNotBlank(i.getSource()) ? " [" + i.getSource() + "]" : ""))
            .collect(Collectors.joining("\n"));
    return new HopException(
        "Ambiguous install target '"
            + token
            + "'. Matches:\n"
            + list
            + "\nUse a full artifactId or groupId:artifactId:version.");
  }

  /**
   * Live list for one repository: Nexus zip search (or catalog URL), enriched with any plugin
   * metadata from the repository definition.
   */
  public static List<OptionalPluginInfo> discoverRepoLive(
      MarketplaceRepository repo, String filter, ILogChannel log) throws Exception {
    if (repo == null) {
      return List.of();
    }

    List<OptionalPluginInfo> live;
    if (StringUtils.isNotBlank(repo.getCatalogUrl())) {
      live = RemotePluginCatalog.load(repo);
      live = RemotePluginCatalog.filter(live, repo, filter);
    } else {
      live = NexusRepositoryBrowser.browse(repo, filter, log);
    }

    // If live listing failed empty but definition has explicit plugins, use those (not a cache —
    // author-supplied list in the YAML).
    if (live.isEmpty() && repo.getPlugins() != null && !repo.getPlugins().isEmpty()) {
      return RemotePluginCatalog.filter(repo.getPlugins(), repo, filter);
    }

    return enrichWithDefinitionMetadata(live, repo);
  }

  /**
   * Apply name/description/category/hop-range and optional version pin from definition plugins when
   * artifact ids match (groupId optional on either side).
   */
  static List<OptionalPluginInfo> enrichWithDefinitionMetadata(
      List<OptionalPluginInfo> live, MarketplaceRepository repo) {
    if (live == null
        || live.isEmpty()
        || repo.getPlugins() == null
        || repo.getPlugins().isEmpty()) {
      return live == null ? List.of() : live;
    }
    Map<String, OptionalPluginInfo> byGa = new LinkedHashMap<>();
    Map<String, OptionalPluginInfo> byArtifact = new LinkedHashMap<>();
    for (OptionalPluginInfo p : repo.getPlugins()) {
      if (p == null || StringUtils.isBlank(p.getArtifactId())) {
        continue;
      }
      byGa.putIfAbsent(artifactKey(p), p);
      byArtifact.putIfAbsent(p.getArtifactId().toLowerCase(Locale.ROOT), p);
    }
    for (OptionalPluginInfo info : live) {
      if (info == null || StringUtils.isBlank(info.getArtifactId())) {
        continue;
      }
      OptionalPluginInfo m = byGa.get(artifactKey(info));
      if (m == null) {
        m = byArtifact.get(info.getArtifactId().toLowerCase(Locale.ROOT));
      }
      if (m == null) {
        continue;
      }
      if (StringUtils.isNotBlank(m.getName())
          && (StringUtils.isBlank(info.getName()) || info.getName().equals(info.getArtifactId()))) {
        info.setName(m.getName());
      }
      if (StringUtils.isNotBlank(m.getDescription())
          && (StringUtils.isBlank(info.getDescription())
              || "auto-discovered".equals(info.getCategory())
              || looksLikeMavenPath(info.getDescription()))) {
        if (!looksLikeMavenPath(m.getDescription())) {
          info.setDescription(m.getDescription());
        }
      }
      // Prefer human category from YAML over Nexus auto-discovered
      if (StringUtils.isNotBlank(m.getCategory())
          && (StringUtils.isBlank(info.getCategory())
              || "auto-discovered".equalsIgnoreCase(info.getCategory())
              || !"auto-discovered".equalsIgnoreCase(m.getCategory()))) {
        if (!"auto-discovered".equalsIgnoreCase(m.getCategory())
            || StringUtils.isBlank(info.getCategory())) {
          info.setCategory(m.getCategory());
        }
      }
      if (StringUtils.isNotBlank(m.getMinHopVersion())) {
        info.setMinHopVersion(m.getMinHopVersion());
      }
      if (StringUtils.isNotBlank(m.getMaxHopVersion())) {
        info.setMaxHopVersion(m.getMaxHopVersion());
      }
      // Optional pin of plugin artifact version from the shareable definition
      if (StringUtils.isNotBlank(m.getVersion())) {
        info.setVersion(m.getVersion());
      }
      if (StringUtils.isBlank(info.getGroupId()) && StringUtils.isNotBlank(m.getGroupId())) {
        info.setGroupId(m.getGroupId());
      }
    }
    return live;
  }

  private static boolean looksLikeMavenPath(String s) {
    return s != null && s.contains("/") && s.contains(".");
  }

  static String artifactKey(OptionalPluginInfo info) {
    String g =
        StringUtils.isNotBlank(info.getGroupId())
            ? info.getGroupId().toLowerCase(Locale.ROOT)
            : "org.apache.hop";
    return g + ":" + info.getArtifactId().toLowerCase(Locale.ROOT);
  }

  private static String key(OptionalPluginInfo info) {
    return nullToEmpty(info.getGroupId())
        + ":"
        + nullToEmpty(info.getArtifactId())
        + ":"
        + nullToEmpty(info.getVersion())
        + ":"
        + nullToEmpty(info.getSource());
  }

  private static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }

  /**
   * Running Hop / marketplace default version — used for Apache optional plugin coordinates and
   * minHopVersion checks. Mirrors {@code MarketplaceCommand.resolveDefaultVersion} without a
   * circular dependency on the CLI class.
   */
  static String resolveHopVersion(MarketplaceConfig config) {
    if (config != null && StringUtils.isNotBlank(config.getDefaultVersion())) {
      return config.getDefaultVersion();
    }
    try {
      String[] versions = new HopVersionProvider().getVersion();
      if (versions != null && versions.length > 0 && StringUtils.isNotBlank(versions[0])) {
        return versions[0];
      }
    } catch (Exception ignored) {
      // fall through
    }
    return System.getProperty("hop.version", "2.19.0-SNAPSHOT");
  }
}
