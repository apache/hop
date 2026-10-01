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

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.marketplace.config.MarketplaceHttp;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.resolve.SnapshotVersions;

/**
 * Best-effort browse of a Forgejo (or Gitea) Maven package registry.
 *
 * <p>Forgejo has no Nexus-style search endpoint. It lists an owner's packages at {@code
 * /api/v1/packages/{owner}?type=maven}, where each entry carries a package {@code name} of the form
 * {@code groupId:artifactId} plus a {@code version}. Whether a version ships an installable plugin
 * zip is a second call to {@code .../files}, so entries without a zip (plain library jars) are
 * dropped instead of being offered as installs that would 404.
 *
 * <p>Downloads never come through here — they use plain Maven layout via {@code
 * MavenRepositoryClient}, which already works against Forgejo unchanged.
 */
public final class ForgejoRepositoryBrowser {

  /** Marker separating the host from the package registry path. */
  private static final String PACKAGES_MARKER = "/api/packages/";

  static final int PAGE_SIZE = 50;
  static final int MAX_PAGES = 50;

  /**
   * Upper bound on the per-version {@code /files} calls used to confirm a zip exists. Reaching it
   * is logged rather than silently truncating the list.
   */
  static final int MAX_ZIP_LOOKUPS = 200;

  private ForgejoRepositoryBrowser() {}

  /** One candidate package version, before the zip check. */
  record Candidate(String groupId, String artifactId, String version, String createdAt) {
    String gaKey() {
      return groupId.toLowerCase(Locale.ROOT) + ":" + artifactId.toLowerCase(Locale.ROOT);
    }

    String packageName() {
      return groupId + ":" + artifactId;
    }
  }

  public static List<OptionalPluginInfo> browse(
      MarketplaceRepository repository, String textFilter, ILogChannel log) throws HopException {
    return browse(repository, textFilter, log, MarketplaceHttp.newClient());
  }

  static List<OptionalPluginInfo> browse(
      MarketplaceRepository repository, String textFilter, ILogChannel log, HttpClient client)
      throws HopException {
    if (repository == null || StringUtils.isBlank(repository.getUrl())) {
      return List.of();
    }
    String apiBase = extractApiBase(repository.getUrl());
    String owner = extractOwner(repository.getUrl());
    if (StringUtils.isAnyBlank(apiBase, owner)) {
      throw new HopException(
          "Cannot derive Forgejo host/owner from URL: "
              + repository.getUrl()
              + " (expected .../api/packages/{owner}/maven)");
    }

    // One row per groupId:artifactId — keep the newest version, like the Nexus browser.
    Map<String, Candidate> byGa = new LinkedHashMap<>();
    Map<String, String> updatedByGa = new LinkedHashMap<>();

    String query = StringUtils.firstNonBlank(textFilter, repository.getSearchQuery());
    for (int page = 1; page <= MAX_PAGES; page++) {
      StringBuilder url = new StringBuilder();
      url.append(apiBase)
          .append("/api/v1/packages/")
          .append(MarketplaceHttp.enc(owner))
          .append("?type=maven&limit=")
          .append(PAGE_SIZE)
          .append("&page=")
          .append(page);
      if (StringUtils.isNotBlank(query)) {
        url.append("&q=").append(MarketplaceHttp.enc(query.trim()));
      }

      List<Candidate> candidates =
          parsePackagesPage(
              MarketplaceHttp.getText(
                  client, url.toString(), repository, "Forgejo package search"));
      for (Candidate candidate : candidates) {
        if (StringUtils.isNotBlank(repository.getGroupIdFilter())
            && !repository.getGroupIdFilter().equals(candidate.groupId())) {
          continue;
        }
        if (!repository.isIncludeSnapshots() && SnapshotVersions.isSnapshot(candidate.version())) {
          continue;
        }
        String key = candidate.gaKey();
        OptionalPluginInfo existing = asInfo(byGa.get(key), updatedByGa.get(key));
        if (!NexusRepositoryBrowser.isPreferable(
            candidate.version(), candidate.createdAt(), existing)) {
          continue;
        }
        byGa.put(key, candidate);
        updatedByGa.put(key, candidate.createdAt());
      }

      if (candidates.size() < PAGE_SIZE) {
        break;
      }
      if (page == MAX_PAGES && log != null) {
        log.logBasic(
            "Forgejo package listing stopped after "
                + MAX_PAGES
                + " pages for owner "
                + owner
                + "; some plugins may not be listed.");
      }
    }

    List<OptionalPluginInfo> out = new ArrayList<>();
    int lookups = 0;
    for (Candidate candidate : byGa.values()) {
      if (lookups >= MAX_ZIP_LOOKUPS) {
        if (log != null) {
          log.logBasic(
              "Forgejo browse checked only the first "
                  + MAX_ZIP_LOOKUPS
                  + " packages for plugin zips; "
                  + (byGa.size() - lookups)
                  + " more were skipped. Narrow the search or set a groupIdFilter.");
        }
        break;
      }
      lookups++;

      String filesUrl =
          apiBase
              + "/api/v1/packages/"
              + MarketplaceHttp.enc(owner)
              + "/maven/"
              + MarketplaceHttp.enc(candidate.packageName())
              + "/"
              + MarketplaceHttp.enc(candidate.version())
              + "/files";
      String zipFile;
      try {
        zipFile =
            extractZipFileName(
                MarketplaceHttp.getText(client, filesUrl, repository, "Forgejo package files"));
      } catch (HopException e) {
        if (log != null) {
          log.logDetailed(
              "Skipping "
                  + candidate.packageName()
                  + ":"
                  + candidate.version()
                  + " — file list unavailable: "
                  + e.getMessage());
        }
        continue;
      }
      if (zipFile == null) {
        // A jar-only artifact (shared library), not an installable Hop plugin.
        continue;
      }

      OptionalPluginInfo info = new OptionalPluginInfo();
      info.setGroupId(candidate.groupId());
      info.setArtifactId(candidate.artifactId());
      info.setVersion(candidate.version());
      info.setName(candidate.artifactId());
      info.setCategory("auto-discovered");
      String path =
          NexusRepositoryBrowser.mavenPath(
                  candidate.groupId(), candidate.artifactId(), candidate.version())
              + zipFile;
      info.setDescription(path);
      info.setInstallPath(path);
      info.setLastUpdated(candidate.createdAt());
      info.setSource(repository.getId());
      out.add(info);
    }

    if (StringUtils.isNotBlank(query)) {
      // Server-side q= is a partial name match only; refine over all fields.
      out = RemotePluginCatalog.filter(out, repository, query);
    }
    return out;
  }

  /** Adapt a stored candidate for {@link NexusRepositoryBrowser#isPreferable} comparison. */
  private static OptionalPluginInfo asInfo(Candidate candidate, String lastUpdated) {
    if (candidate == null) {
      return null;
    }
    OptionalPluginInfo info = new OptionalPluginInfo();
    info.setVersion(candidate.version());
    info.setLastUpdated(lastUpdated);
    return info;
  }

  /** Parse one page of {@code /api/v1/packages/{owner}} into candidates. */
  static List<Candidate> parsePackagesPage(String json) throws HopException {
    if (StringUtils.isBlank(json)) {
      return List.of();
    }
    List<?> items;
    try {
      items = HopJson.newMapper().readValue(json, List.class);
    } catch (Exception e) {
      throw new HopException("Unable to parse Forgejo package list JSON", e);
    }
    List<Candidate> out = new ArrayList<>();
    for (Object item : items) {
      if (!(item instanceof Map<?, ?> map)) {
        continue;
      }
      String type = MarketplaceHttp.str(map.get("type"));
      if (type != null && !"maven".equalsIgnoreCase(type)) {
        continue;
      }
      String[] ga = splitPackageName(MarketplaceHttp.str(map.get("name")));
      String version = MarketplaceHttp.str(map.get("version"));
      if (ga == null || version == null) {
        continue;
      }
      out.add(new Candidate(ga[0], ga[1], version, MarketplaceHttp.str(map.get("created_at"))));
    }
    return out;
  }

  /**
   * Forgejo stores a Maven package under the name {@code groupId:artifactId}. Returns {@code
   * {groupId, artifactId}}, or null when the name has no colon.
   */
  static String[] splitPackageName(String packageName) {
    if (StringUtils.isBlank(packageName)) {
      return null;
    }
    String name = packageName.trim();
    int colon = name.lastIndexOf(':');
    if (colon <= 0 || colon == name.length() - 1) {
      return null;
    }
    return new String[] {name.substring(0, colon), name.substring(colon + 1)};
  }

  /**
   * File name of the first plugin zip in a {@code .../files} response, or null when the version has
   * no zip (checksum sidecars are not files of their own in Forgejo, but are excluded defensively).
   */
  static String extractZipFileName(String json) throws HopException {
    if (StringUtils.isBlank(json)) {
      return null;
    }
    List<?> files;
    try {
      files = HopJson.newMapper().readValue(json, List.class);
    } catch (Exception e) {
      throw new HopException("Unable to parse Forgejo package file list JSON", e);
    }
    for (Object file : files) {
      if (!(file instanceof Map<?, ?> map)) {
        continue;
      }
      String name = MarketplaceHttp.str(map.get("name"));
      if (isPluginZip(name)) {
        return name;
      }
    }
    return null;
  }

  /** True when the file is a real zip artifact rather than a checksum sidecar. */
  static boolean isPluginZip(String fileName) {
    return StringUtils.isNotBlank(fileName) && fileName.toLowerCase(Locale.ROOT).endsWith(".zip");
  }

  /** {@code https://forge.example.io/api/packages/acme/maven} → {@code https://forge.example.io} */
  static String extractApiBase(String repositoryUrl) {
    if (StringUtils.isBlank(repositoryUrl)) {
      return null;
    }
    int idx = repositoryUrl.indexOf(PACKAGES_MARKER);
    return idx < 0 ? null : repositoryUrl.substring(0, idx);
  }

  /** {@code https://forge.example.io/api/packages/acme/maven} → {@code acme} */
  static String extractOwner(String repositoryUrl) {
    if (StringUtils.isBlank(repositoryUrl)) {
      return null;
    }
    int idx = repositoryUrl.indexOf(PACKAGES_MARKER);
    if (idx < 0) {
      return null;
    }
    String rest = repositoryUrl.substring(idx + PACKAGES_MARKER.length());
    while (rest.startsWith("/")) {
      rest = rest.substring(1);
    }
    int slash = rest.indexOf('/');
    String owner = slash < 0 ? rest : rest.substring(0, slash);
    return StringUtils.trimToNull(owner);
  }
}
