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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
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
 * Best-effort browse of a JFrog Artifactory Maven repository.
 *
 * <p>Artifactory offers three ways to list a repository and each has a catch, so this browser uses
 * two of them:
 *
 * <ul>
 *   <li><b>AQL</b> ({@code POST /api/search/aql}) filters server-side in one round trip, but
 *       requires an authenticated user. It is used whenever credentials resolve.
 *   <li><b>Folder Info</b> ({@code GET /api/storage/{repo}/{path}}) is available to any user with
 *       read access, anonymous included, at the cost of one request per folder. It is the fallback,
 *       and the only option for the anonymous-read repositories that mirror the Nexus setup Hop
 *       already supports.
 *   <li><b>File List</b> ({@code ?list&deep=1}) would be ideal but is an Artifactory Pro feature,
 *       so it is not used at all.
 * </ul>
 *
 * <p>Downloads never come through here — they use plain Maven layout via {@code
 * MavenRepositoryClient}, which already works against Artifactory unchanged.
 */
public final class JfrogRepositoryBrowser {

  /** Marker separating the host and context path from the repository key. */
  private static final String ARTIFACTORY_MARKER = "/artifactory/";

  /** AQL's own default; stated explicitly so the limit is visible in the query that is sent. */
  static final int AQL_LIMIT = 1000;

  /**
   * Upper bound on Folder Info requests for one browse. A Maven repository is a deep tree and the
   * walk is one request per folder, so an unbounded walk of a large shared repository would hammer
   * the server. Reaching it is logged rather than silently truncating the list.
   */
  static final int MAX_WALK_REQUESTS = 300;

  /** Deepest folder the walk descends to, counted from the repository root. */
  static final int MAX_WALK_DEPTH = 10;

  private JfrogRepositoryBrowser() {}

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
    String apiBase = extractArtifactoryBase(repository.getUrl());
    String repoKey = extractRepoKey(repository.getUrl());
    if (StringUtils.isAnyBlank(apiBase, repoKey)) {
      throw new HopException(
          "Cannot derive Artifactory base/repository from URL: "
              + repository.getUrl()
              + " (expected .../artifactory/{repository}/)");
    }

    String query = StringUtils.firstNonBlank(textFilter, repository.getSearchQuery());
    List<OptionalPluginInfo> found = null;

    // AQL needs an authenticated user, so it is only worth trying when credentials resolve.
    if (repository.hasCredentials()) {
      try {
        found =
            parseAqlResults(
                MarketplaceHttp.postText(
                    client,
                    apiBase + "/api/search/aql",
                    buildAqlQuery(repoKey, repository.getGroupIdFilter(), query),
                    "text/plain",
                    repository,
                    "Artifactory AQL search"),
                repository);
        if (found.size() >= AQL_LIMIT && log != null) {
          // Without a sort — which the OSS edition rejects — the rows that come back at the cap
          // are an arbitrary slice rather than the newest, so say the list is incomplete instead
          // of presenting it as the whole repository. Under-reports if rows were dropped as
          // non-zip, which is the harmless direction.
          log.logBasic(
              "Artifactory AQL search returned the maximum of "
                  + AQL_LIMIT
                  + " artifacts for repository '"
                  + repository.getId()
                  + "'; some plugins may not be listed. Set a groupIdFilter to narrow it.");
        }
      } catch (HopException e) {
        // A restricted token, or an instance with AQL disabled, still browses through storage.
        if (log != null) {
          log.logDetailed(
              "Artifactory AQL search unavailable for repository '"
                  + repository.getId()
                  + "', falling back to the storage walk: "
                  + e.getMessage());
        }
      }
    }

    if (found == null) {
      found = walkStorage(repository, apiBase, repoKey, log, client);
    }

    List<OptionalPluginInfo> out = newestPerArtifact(found, repository);
    if (StringUtils.isNotBlank(query)) {
      // AQL matches on file name only and the walk does not filter at all; refine over all fields.
      out = RemotePluginCatalog.filter(out, repository, query);
    }
    return out;
  }

  // ------------------------------------------------------------------ AQL

  /**
   * An {@code items.find} over the plugin zips of one repository.
   *
   * <p>Values are JSON string literals inside the query, so anything coming from configuration or
   * from the search box is escaped: an unescaped quote would otherwise end the literal and let the
   * rest of the text be read as query syntax.
   */
  static String buildAqlQuery(String repoKey, String groupIdFilter, String textFilter) {
    StringBuilder criteria = new StringBuilder();
    criteria.append("{\"repo\":").append(jsonString(repoKey));
    // One name criterion covering both the extension and the search text; AQL matches the file
    // name, which for Maven layout carries the artifactId and version.
    String namePattern =
        StringUtils.isBlank(textFilter) ? "*.zip" : "*" + textFilter.trim() + "*.zip";
    criteria.append(",\"name\":{\"$match\":").append(jsonString(namePattern)).append("}");
    if (StringUtils.isNotBlank(groupIdFilter)) {
      criteria
          .append(",\"path\":{\"$match\":")
          .append(jsonString(groupIdFilter.trim().replace('.', '/') + "/*"))
          .append("}");
    }
    criteria.append("}");
    // No .sort(): Artifactory OSS rejects the whole query with "Sorting is not supported by AQL
    // in the open source version". Ordering is not needed anyway — newestPerArtifact compares
    // versions and timestamps itself.
    return "items.find("
        + criteria
        + ").include(\"repo\",\"path\",\"name\",\"created\",\"modified\").limit("
        + AQL_LIMIT
        + ")";
  }

  /** Candidates from an AQL {@code results} array; rows that are not Maven layout are skipped. */
  static List<OptionalPluginInfo> parseAqlResults(String json, MarketplaceRepository repository)
      throws HopException {
    if (StringUtils.isBlank(json)) {
      return List.of();
    }
    Map<?, ?> root;
    try {
      root = HopJson.newMapper().readValue(json, Map.class);
    } catch (Exception e) {
      throw new HopException("Unable to parse Artifactory AQL response JSON", e);
    }
    if (!(root.get("results") instanceof List<?> results)) {
      return List.of();
    }
    List<OptionalPluginInfo> out = new ArrayList<>();
    for (Object item : results) {
      if (!(item instanceof Map<?, ?> row)) {
        continue;
      }
      String path = MarketplaceHttp.str(row.get("path"));
      String name = MarketplaceHttp.str(row.get("name"));
      if (!isPluginZip(name)) {
        continue;
      }
      String updated =
          StringUtils.firstNonBlank(
              MarketplaceHttp.str(row.get("modified")), MarketplaceHttp.str(row.get("created")));
      OptionalPluginInfo info = toPluginInfo(path, name, updated, repository);
      if (info != null) {
        out.add(info);
      }
    }
    return out;
  }

  // -------------------------------------------------------------- storage

  /**
   * Depth-first walk of the repository tree, one Folder Info request per folder, stopping at the
   * first folder that holds a plugin zip — in Maven layout that is the version folder, and nothing
   * below it is another artifact.
   *
   * <p>A {@code groupIdFilter} is applied as the starting path rather than as a filter, which is
   * what keeps the walk affordable on a shared repository.
   */
  static List<OptionalPluginInfo> walkStorage(
      MarketplaceRepository repository,
      String apiBase,
      String repoKey,
      ILogChannel log,
      HttpClient client)
      throws HopException {

    String root =
        StringUtils.isBlank(repository.getGroupIdFilter())
            ? ""
            : repository.getGroupIdFilter().trim().replace('.', '/');

    List<OptionalPluginInfo> out = new ArrayList<>();
    Deque<String> pending = new ArrayDeque<>();
    pending.push(root);
    int requests = 0;

    while (!pending.isEmpty()) {
      if (requests >= MAX_WALK_REQUESTS) {
        if (log != null) {
          log.logBasic(
              "Artifactory storage walk stopped after "
                  + MAX_WALK_REQUESTS
                  + " requests for repository '"
                  + repository.getId()
                  + "'; some plugins may not be listed. Set a groupIdFilter to narrow it, or"
                  + " configure credentials so the faster AQL search can be used.");
        }
        break;
      }
      String path = pending.pop();
      requests++;

      String url =
          apiBase
              + "/api/storage/"
              + encodePath(repoKey)
              + (path.isEmpty() ? "" : "/" + encodePath(path));
      Folder folder;
      try {
        folder =
            parseFolder(
                MarketplaceHttp.getText(client, url, repository, "Artifactory storage listing"));
      } catch (HopException e) {
        if (path.equals(root)) {
          // The starting point has to be readable; anything deeper may legitimately be denied.
          throw e;
        }
        if (log != null) {
          log.logDetailed("Skipping unreadable Artifactory folder " + path + ": " + e.getMessage());
        }
        continue;
      }

      String zip = firstPluginZip(folder.files());
      if (zip != null) {
        OptionalPluginInfo info = toPluginInfo(path, zip, folder.lastModified(), repository);
        if (info != null) {
          out.add(info);
        }
        continue;
      }
      if (depthOf(path) >= MAX_WALK_DEPTH) {
        continue;
      }
      for (String child : folder.folders()) {
        pending.push(path.isEmpty() ? child : path + "/" + child);
      }
    }
    return out;
  }

  /** Child names of one Folder Info response, split by kind. */
  record Folder(List<String> folders, List<String> files, String lastModified) {}

  static Folder parseFolder(String json) throws HopException {
    List<String> folders = new ArrayList<>();
    List<String> files = new ArrayList<>();
    if (StringUtils.isBlank(json)) {
      return new Folder(folders, files, null);
    }
    Map<?, ?> root;
    try {
      root = HopJson.newMapper().readValue(json, Map.class);
    } catch (Exception e) {
      throw new HopException("Unable to parse Artifactory storage listing JSON", e);
    }
    if (root.get("children") instanceof List<?> children) {
      for (Object child : children) {
        if (!(child instanceof Map<?, ?> map)) {
          continue;
        }
        String uri = MarketplaceHttp.str(map.get("uri"));
        if (uri == null) {
          continue;
        }
        String name = uri.startsWith("/") ? uri.substring(1) : uri;
        if (name.isEmpty()) {
          continue;
        }
        if (Boolean.TRUE.equals(map.get("folder"))) {
          folders.add(name);
        } else {
          files.add(name);
        }
      }
    }
    return new Folder(folders, files, MarketplaceHttp.str(root.get("lastModified")));
  }

  // --------------------------------------------------------------- shared

  /**
   * Maven layout turns a repository path into coordinates: the last segment is the version, the one
   * before it the artifactId, and everything above the groupId. Fewer than three segments is not
   * Maven layout, so there is nothing installable to report.
   *
   * <p>Taking the version from the folder rather than from the file name is what makes unique
   * SNAPSHOT builds collapse: {@code 1.0.0-SNAPSHOT/plugin-1.0.0-20260101.120000-1.zip} reports
   * {@code 1.0.0-SNAPSHOT}, the version a download can actually resolve.
   */
  static OptionalPluginInfo toPluginInfo(
      String path, String fileName, String lastUpdated, MarketplaceRepository repository) {
    if (StringUtils.isAnyBlank(path, fileName) || ".".equals(path)) {
      return null;
    }
    String[] segments = StringUtils.strip(path, "/").split("/");
    if (segments.length < 3) {
      return null;
    }
    String version = segments[segments.length - 1];
    String artifactId = segments[segments.length - 2];
    String groupId = String.join(".", List.of(segments).subList(0, segments.length - 2));
    if (StringUtils.isAnyBlank(groupId, artifactId, version)) {
      return null;
    }
    if (repository != null
        && !repository.isIncludeSnapshots()
        && SnapshotVersions.isSnapshot(version)) {
      return null;
    }
    if (repository != null
        && StringUtils.isNotBlank(repository.getGroupIdFilter())
        && !groupId.equals(repository.getGroupIdFilter().trim())) {
      return null;
    }

    OptionalPluginInfo info = new OptionalPluginInfo();
    info.setGroupId(groupId);
    info.setArtifactId(artifactId);
    info.setVersion(version);
    info.setName(artifactId);
    info.setCategory("auto-discovered");
    String installPath = StringUtils.strip(path, "/") + "/" + fileName;
    info.setDescription(installPath);
    info.setInstallPath(installPath);
    info.setLastUpdated(lastUpdated);
    info.setSource(repository == null ? null : repository.getId());
    return info;
  }

  /** One row per groupId:artifactId, keeping the newest, as the Nexus and Forgejo browsers do. */
  static List<OptionalPluginInfo> newestPerArtifact(
      List<OptionalPluginInfo> candidates, MarketplaceRepository repository) {
    Map<String, OptionalPluginInfo> byGa = new LinkedHashMap<>();
    for (OptionalPluginInfo info : candidates) {
      if (info == null || StringUtils.isBlank(info.getArtifactId())) {
        continue;
      }
      String key =
          StringUtils.lowerCase(info.getGroupId(), Locale.ROOT)
              + ":"
              + info.getArtifactId().toLowerCase(Locale.ROOT);
      OptionalPluginInfo existing = byGa.get(key);
      if (existing == null
          || NexusRepositoryBrowser.isPreferable(
              info.getVersion(), info.getLastUpdated(), existing)) {
        byGa.put(key, info);
      }
    }
    List<OptionalPluginInfo> out = new ArrayList<>(byGa.values());
    if (repository != null) {
      out.forEach(info -> info.setSource(repository.getId()));
    }
    return out;
  }

  /** True when the file is a real zip artifact rather than a checksum sidecar. */
  static boolean isPluginZip(String fileName) {
    if (StringUtils.isBlank(fileName)) {
      return false;
    }
    String lower = fileName.toLowerCase(Locale.ROOT);
    return lower.endsWith(".zip");
  }

  static String firstPluginZip(List<String> files) {
    if (files == null) {
      return null;
    }
    return files.stream().filter(JfrogRepositoryBrowser::isPluginZip).findFirst().orElse(null);
  }

  /**
   * {@code https://acme.jfrog.io/artifactory/hop-plugins/} → {@code
   * https://acme.jfrog.io/artifactory}
   */
  static String extractArtifactoryBase(String repositoryUrl) {
    if (StringUtils.isBlank(repositoryUrl)) {
      return null;
    }
    int idx = repositoryUrl.indexOf(ARTIFACTORY_MARKER);
    return idx < 0 ? null : repositoryUrl.substring(0, idx + ARTIFACTORY_MARKER.length() - 1);
  }

  /** {@code https://acme.jfrog.io/artifactory/hop-plugins/} → {@code hop-plugins} */
  static String extractRepoKey(String repositoryUrl) {
    if (StringUtils.isBlank(repositoryUrl)) {
      return null;
    }
    int idx = repositoryUrl.indexOf(ARTIFACTORY_MARKER);
    if (idx < 0) {
      return null;
    }
    String rest = repositoryUrl.substring(idx + ARTIFACTORY_MARKER.length());
    int slash = rest.indexOf('/');
    return StringUtils.trimToNull(slash < 0 ? rest : rest.substring(0, slash));
  }

  /**
   * Percent-encode each segment, leaving the separators alone. {@code MarketplaceHttp.enc} is form
   * encoding, which writes a space as {@code +}; in a path that is a literal plus, so it is
   * corrected to {@code %20} here.
   */
  static String encodePath(String path) {
    List<String> encoded = new ArrayList<>();
    for (String segment : StringUtils.strip(path, "/").split("/")) {
      encoded.add(MarketplaceHttp.enc(segment).replace("+", "%20"));
    }
    return String.join("/", encoded);
  }

  private static int depthOf(String path) {
    return path.isEmpty() ? 0 : StringUtils.countMatches(path, '/') + 1;
  }

  /** A JSON string literal, so configured text cannot break out into AQL syntax. */
  static String jsonString(String value) {
    StringBuilder sb = new StringBuilder("\"");
    for (char c : String.valueOf(value).toCharArray()) {
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    return sb.append('"').toString();
  }
}
