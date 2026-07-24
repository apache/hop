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

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.resolve.SnapshotVersions;

/**
 * Best-effort browse of a Sonatype Nexus 3 hosted/proxy Maven repository via REST search for {@code
 * maven.extension=zip} assets.
 */
public final class NexusRepositoryBrowser {

  private NexusRepositoryBrowser() {}

  public static List<OptionalPluginInfo> browse(
      MarketplaceRepository repository, String textFilter, ILogChannel log) throws HopException {
    if (repository == null || StringUtils.isBlank(repository.getUrl())) {
      return List.of();
    }
    String repoName = extractRepositoryName(repository.getUrl());
    String nexusBase = extractNexusBase(repository.getUrl());
    if (StringUtils.isBlank(repoName) || StringUtils.isBlank(nexusBase)) {
      throw new HopException(
          "Cannot derive Nexus host/repository name from URL: " + repository.getUrl());
    }

    HttpClient client =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    // One row per groupId:artifactId — keep the latest plugin version (not every unique SNAPSHOT).
    Map<String, OptionalPluginInfo> byGa = new LinkedHashMap<>();
    String continuation = null;
    int pages = 0;
    do {
      pages++;
      if (pages > 50) {
        if (log != null) {
          log.logBasic("Nexus search pagination stopped after 50 pages for " + repoName);
        }
        break;
      }
      StringBuilder q = new StringBuilder();
      q.append(nexusBase)
          .append("/service/rest/v1/search?repository=")
          .append(enc(repoName))
          .append("&format=maven2&maven.extension=zip");
      if (StringUtils.isNotBlank(repository.getGroupIdFilter())) {
        q.append("&group=").append(enc(repository.getGroupIdFilter()));
      }
      if (StringUtils.isNotBlank(textFilter)) {
        q.append("&name=").append(enc(textFilter.trim()));
      } else if (StringUtils.isNotBlank(repository.getSearchQuery())) {
        q.append("&name=").append(enc(repository.getSearchQuery().trim()));
      }
      if (StringUtils.isNotBlank(continuation)) {
        q.append("&continuationToken=").append(enc(continuation));
      }

      String body = getText(client, q.toString(), repository);
      Map<?, ?> json;
      try {
        json = HopJson.newMapper().readValue(body, Map.class);
      } catch (Exception e) {
        throw new HopException("Unable to parse Nexus search JSON from " + q, e);
      }
      Object items = json.get("items");
      if (items instanceof List<?> list) {
        for (Object item : list) {
          if (!(item instanceof Map<?, ?> m)) {
            continue;
          }
          String group = str(m.get("group"));
          String name = str(m.get("name"));
          String rawVersion = str(m.get("version"));
          if (StringUtils.isAnyBlank(group, name, rawVersion)) {
            continue;
          }
          // Nexus reports unique SNAPSHOT timestamps (1.0-20260721.105615-1); Maven layout uses
          // base folder 1.0-SNAPSHOT/. Prefer the version folder from the zip asset path.
          String zipAssetPath = extractZipAssetPath(m);
          String version = SnapshotVersions.toBaseVersion(rawVersion, zipAssetPath);
          if (!repository.isIncludeSnapshots() && SnapshotVersions.isSnapshot(version)) {
            continue;
          }
          String gaKey = group.toLowerCase(Locale.ROOT) + ":" + name.toLowerCase(Locale.ROOT);
          String lastUpdated = extractLastUpdated(m);
          OptionalPluginInfo existing = byGa.get(gaKey);
          if (existing != null && !isPreferable(version, lastUpdated, existing)) {
            continue;
          }
          String path =
              StringUtils.isNotBlank(zipAssetPath)
                  ? zipAssetPath.replaceFirst("^/+", "")
                  : mavenPath(group, name, version);
          OptionalPluginInfo info = new OptionalPluginInfo();
          info.setGroupId(group);
          info.setArtifactId(name);
          info.setVersion(version);
          info.setName(name);
          info.setCategory("auto-discovered");
          info.setDescription(path);
          info.setInstallPath(path);
          info.setLastUpdated(lastUpdated);
          info.setSource(repository.getId());
          byGa.put(gaKey, info);
        }
      }
      Object token = json.get("continuationToken");
      continuation =
          token == null || StringUtils.isBlank(String.valueOf(token))
              ? null
              : String.valueOf(token);
    } while (continuation != null);

    List<OptionalPluginInfo> out = new ArrayList<>(byGa.values());
    if (StringUtils.isNotBlank(textFilter) || StringUtils.isNotBlank(repository.getSearchQuery())) {
      // client-side refine (server name= is only a partial match)
      String needle = StringUtils.isNotBlank(textFilter) ? textFilter : repository.getSearchQuery();
      out = RemotePluginCatalog.filter(out, repository, needle);
    }
    return out;
  }

  /**
   * Prefer higher plugin version; if versions equal, prefer newer lastUpdated (unique SNAPSHOT
   * builds collapsed to the same base version).
   */
  static boolean isPreferable(
      String candidateVersion, String candidateUpdated, OptionalPluginInfo existing) {
    if (existing == null) {
      return true;
    }
    int vc = VersionCompat.compare(candidateVersion, existing.getVersion());
    if (vc > 0) {
      return true;
    }
    if (vc < 0) {
      return false;
    }
    return isNewer(candidateUpdated, existing.getLastUpdated());
  }

  static String extractRepositoryName(String repositoryUrl) {
    if (StringUtils.isBlank(repositoryUrl)) {
      return null;
    }
    String u = repositoryUrl.trim();
    while (u.endsWith("/")) {
      u = u.substring(0, u.length() - 1);
    }
    int idx = u.indexOf("/repository/");
    if (idx < 0) {
      return null;
    }
    String rest = u.substring(idx + "/repository/".length());
    int slash = rest.indexOf('/');
    return slash < 0 ? rest : rest.substring(0, slash);
  }

  static String extractNexusBase(String repositoryUrl) {
    if (StringUtils.isBlank(repositoryUrl)) {
      return null;
    }
    String u = repositoryUrl.trim();
    int idx = u.indexOf("/repository/");
    if (idx < 0) {
      // already a host root?
      while (u.endsWith("/")) {
        u = u.substring(0, u.length() - 1);
      }
      return u;
    }
    return u.substring(0, idx);
  }

  private static String getText(HttpClient client, String url, MarketplaceRepository repository)
      throws HopException {
    try {
      HttpRequest.Builder b =
          HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(60)).GET();
      if (repository.hasCredentials()) {
        String token =
            Base64.getEncoder()
                .encodeToString(
                    (repository.effectiveUsername() + ":" + repository.effectivePassword())
                        .getBytes(StandardCharsets.UTF_8));
        b.header("Authorization", "Basic " + token);
      }
      HttpResponse<String> response = client.send(b.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new HopException("Nexus search failed HTTP " + response.statusCode() + " for " + url);
      }
      return response.body();
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Nexus search failed for " + url, e);
    }
  }

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  private static String str(Object o) {
    if (o == null) {
      return null;
    }
    String s = String.valueOf(o).trim();
    return s.isEmpty() ? null : s;
  }

  static String mavenPath(String group, String artifact, String version) {
    return group.replace('.', '/') + "/" + artifact + "/" + version + "/";
  }

  /**
   * Path of the first {@code .zip} asset (not checksum sidecars), relative to the repository root.
   */
  static String extractZipAssetPath(Map<?, ?> item) {
    Object assets = item.get("assets");
    if (!(assets instanceof List<?> list)) {
      return null;
    }
    for (Object o : list) {
      if (!(o instanceof Map<?, ?> asset)) {
        continue;
      }
      String path = str(asset.get("path"));
      if (path == null) {
        continue;
      }
      String lower = path.toLowerCase(Locale.ROOT);
      if (lower.endsWith(".zip")
          && !lower.endsWith(".zip.sha1")
          && !lower.endsWith(".zip.md5")
          && !lower.endsWith(".zip.sha256")
          && !lower.endsWith(".zip.sha512")) {
        return path;
      }
    }
    return null;
  }

  static String extractLastUpdated(Map<?, ?> item) {
    Object assets = item.get("assets");
    if (assets instanceof List<?> list && !list.isEmpty()) {
      // Prefer zip asset lastModified when present
      for (Object o : list) {
        if (o instanceof Map<?, ?> asset) {
          String path = str(asset.get("path"));
          if (path != null && path.toLowerCase(Locale.ROOT).endsWith(".zip")) {
            String lm = str(asset.get("lastModified"));
            if (lm != null) {
              return lm;
            }
          }
        }
      }
      Object first = list.get(0);
      if (first instanceof Map<?, ?> asset) {
        String lm = str(asset.get("lastModified"));
        if (lm != null) {
          return lm;
        }
      }
    }
    String assetsUpdated = str(item.get("assetsUpdated"));
    if (assetsUpdated != null) {
      return assetsUpdated;
    }
    return str(item.get("lastModified"));
  }

  /** Prefer non-blank newer lastUpdated strings (ISO-8601 sorts lexicographically). */
  static boolean isNewer(String candidate, String existing) {
    if (StringUtils.isBlank(existing)) {
      return true;
    }
    if (StringUtils.isBlank(candidate)) {
      return false;
    }
    return candidate.compareTo(existing) > 0;
  }
}
