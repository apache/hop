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

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/** Loads a remote or local hop-marketplace-catalog YAML/JSON. */
public final class RemotePluginCatalog {

  private RemotePluginCatalog() {}

  public static List<OptionalPluginInfo> load(MarketplaceRepository repository)
      throws HopException {
    if (repository == null || StringUtils.isBlank(repository.getCatalogUrl())) {
      return List.of();
    }
    String url = repository.getCatalogUrl().trim();
    List<OptionalPluginInfo> plugins;
    if (url.startsWith("http://") || url.startsWith("https://")) {
      plugins = loadHttp(url);
    } else {
      plugins = loadFile(Path.of(url));
    }
    String source =
        StringUtils.isNotBlank(repository.getId()) ? repository.getId() : repository.displayName();
    for (OptionalPluginInfo p : plugins) {
      if (p != null && StringUtils.isBlank(p.getSource())) {
        p.setSource(source);
      }
      if (p != null && StringUtils.isBlank(p.getGroupId())) {
        p.setGroupId("org.apache.hop");
      }
    }
    return filter(plugins, repository, null);
  }

  public static List<OptionalPluginInfo> loadFile(Path file) throws HopException {
    if (file == null || !Files.isRegularFile(file)) {
      throw new HopException("Catalog file not found: " + file);
    }
    try (InputStream in = Files.newInputStream(file)) {
      return parse(in, file.toString());
    } catch (Exception e) {
      throw new HopException("Unable to read catalog: " + file, e);
    }
  }

  public static List<OptionalPluginInfo> loadHttp(String url) throws HopException {
    try {
      HttpClient client =
          HttpClient.newBuilder()
              .connectTimeout(Duration.ofSeconds(30))
              .followRedirects(HttpClient.Redirect.NORMAL)
              .build();
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(60)).GET().build();
      HttpResponse<InputStream> response =
          client.send(request, HttpResponse.BodyHandlers.ofInputStream());
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new HopException(
            "Unable to download catalog from " + url + " (HTTP " + response.statusCode() + ")");
      }
      try (InputStream in = response.body()) {
        return parse(in, url);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to download catalog from " + url, e);
    }
  }

  @SuppressWarnings("unchecked")
  static List<OptionalPluginInfo> parse(InputStream in, String source) throws HopException {
    try {
      String lower = source == null ? "" : source.toLowerCase(Locale.ROOT);
      Map<String, Object> root;
      if (lower.endsWith(".json")) {
        root = HopJson.newMapper().readValue(in, Map.class);
      } else {
        Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
        Object loaded = yaml.load(in);
        if (!(loaded instanceof Map)) {
          throw new HopException("Catalog root must be a mapping: " + source);
        }
        root = (Map<String, Object>) loaded;
      }
      Object pluginsNode = root.get("plugins");
      if (!(pluginsNode instanceof List<?> list)) {
        return List.of();
      }
      List<OptionalPluginInfo> out = new ArrayList<>();
      for (Object item : list) {
        if (!(item instanceof Map<?, ?> m)) {
          continue;
        }
        OptionalPluginInfo info = new OptionalPluginInfo();
        info.setGroupId(str(m.get("groupId")));
        info.setArtifactId(str(m.get("artifactId")));
        info.setVersion(str(m.get("version")));
        info.setName(str(m.get("name")));
        info.setCategory(str(m.get("category")));
        info.setDescription(str(m.get("description")));
        info.setInstallPath(str(m.get("installPath")));
        info.setMinHopVersion(str(m.get("minHopVersion")));
        info.setMaxHopVersion(str(m.get("maxHopVersion")));
        info.setSource(str(m.get("source")));
        if (StringUtils.isNotBlank(info.getArtifactId())) {
          out.add(info);
        }
      }
      return out;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to parse catalog: " + source, e);
    }
  }

  /** Apply repository filters and optional free-text filter. */
  public static List<OptionalPluginInfo> filter(
      List<OptionalPluginInfo> plugins, MarketplaceRepository repository, String textFilter) {
    if (plugins == null || plugins.isEmpty()) {
      return List.of();
    }
    String groupFilter =
        repository != null ? StringUtils.trimToNull(repository.getGroupIdFilter()) : null;
    boolean includeSnapshots = repository == null || repository.isIncludeSnapshots();
    String repoQuery =
        repository != null ? StringUtils.trimToNull(repository.getSearchQuery()) : null;
    String text = StringUtils.trimToNull(textFilter);
    List<OptionalPluginInfo> out = new ArrayList<>();
    for (OptionalPluginInfo p : plugins) {
      if (p == null || StringUtils.isBlank(p.getArtifactId())) {
        continue;
      }
      if (groupFilter != null
          && StringUtils.isNotBlank(p.getGroupId())
          && !groupFilter.equals(p.getGroupId())) {
        continue;
      }
      if (!includeSnapshots
          && p.getVersion() != null
          && org.apache.hop.marketplace.resolve.SnapshotVersions.isSnapshot(p.getVersion())) {
        continue;
      }
      if (repoQuery != null && !matchesText(p, repoQuery)) {
        continue;
      }
      if (text != null && !matchesText(p, text)) {
        continue;
      }
      out.add(p);
    }
    return out;
  }

  static boolean matchesText(OptionalPluginInfo p, String needle) {
    String n = needle.toLowerCase(Locale.ROOT);
    return contains(p.getArtifactId(), n)
        || contains(p.getName(), n)
        || contains(p.getCategory(), n)
        || contains(p.getDescription(), n)
        || contains(p.getGroupId(), n)
        || contains(p.getVersion(), n)
        || contains(p.getInstallPath(), n)
        || contains(p.getSource(), n);
  }

  private static boolean contains(String value, String needleLower) {
    return value != null && value.toLowerCase(Locale.ROOT).contains(needleLower);
  }

  private static String str(Object o) {
    if (o == null) {
      return null;
    }
    String s = String.valueOf(o).trim();
    return s.isEmpty() ? null : s;
  }
}
