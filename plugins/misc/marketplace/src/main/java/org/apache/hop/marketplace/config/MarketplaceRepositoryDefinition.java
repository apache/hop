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

package org.apache.hop.marketplace.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Shareable Hop marketplace repository definition ({@code hop-marketplace-repo.yaml}). Passwords
 * are never written on export.
 */
public final class MarketplaceRepositoryDefinition {

  public static final String KIND = "hop-marketplace-repository";
  public static final String SCHEMA_VERSION = "1.0";

  private MarketplaceRepositoryDefinition() {}

  public static MarketplaceRepository load(Path file) throws HopException {
    if (file == null || !Files.isRegularFile(file)) {
      throw new HopException("Repository definition not found: " + file);
    }
    try (InputStream in = Files.newInputStream(file)) {
      return parse(in, file.toString());
    } catch (IOException e) {
      throw new HopException("Unable to read repository definition: " + file, e);
    }
  }

  public static MarketplaceRepository loadFromUri(String uriOrPath) throws HopException {
    if (StringUtils.isBlank(uriOrPath)) {
      throw new HopException("Repository definition path or URL is required");
    }
    String s = uriOrPath.trim();
    if (s.startsWith("http://") || s.startsWith("https://")) {
      return loadFromHttp(s);
    }
    return load(Path.of(s));
  }

  public static MarketplaceRepository loadFromHttp(String url) throws HopException {
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
            "Unable to download repository definition from "
                + url
                + " (HTTP "
                + response.statusCode()
                + ")");
      }
      try (InputStream in = response.body()) {
        return parse(in, url);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to download repository definition from " + url, e);
    }
  }

  static MarketplaceRepository parse(InputStream in, String source) throws HopException {
    try {
      String name = source == null ? "" : source.toLowerCase(Locale.ROOT);
      if (name.endsWith(".json")) {
        Map<?, ?> map = HopJson.newMapper().readValue(in, Map.class);
        return fromMap(map);
      }
      Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
      Object loaded = yaml.load(in);
      if (!(loaded instanceof Map)) {
        throw new HopException("Repository definition root must be a mapping: " + source);
      }
      return fromMap((Map<?, ?>) loaded);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to parse repository definition: " + source, e);
    }
  }

  static MarketplaceRepository fromMap(Map<?, ?> map) throws HopException {
    if (map == null) {
      throw new HopException("Empty repository definition");
    }
    Object kind = map.get("kind");
    if (kind != null && !KIND.equals(String.valueOf(kind).trim())) {
      // allow missing kind for brevity
      if (StringUtils.isNotBlank(String.valueOf(kind))
          && !String.valueOf(kind).contains("marketplace")) {
        throw new HopException(
            "Unexpected kind '" + kind + "' (expected " + KIND + " or omit kind)");
      }
    }
    MarketplaceRepository repo = new MarketplaceRepository();
    repo.setId(stringVal(map.get("id")));
    repo.setName(stringVal(map.get("name")));
    repo.setUrl(stringVal(map.get("url")));
    if (StringUtils.isBlank(repo.getId()) || StringUtils.isBlank(repo.getUrl())) {
      throw new HopException("Repository definition requires id and url");
    }
    if (StringUtils.isBlank(repo.getName())) {
      repo.setName(repo.getId());
    }
    repo.setPrimary(boolVal(map.get("primary"), false));
    repo.setEnabled(boolVal(map.get("enabled"), true));
    repo.setUsername(stringVal(map.get("username")));
    // password intentionally ignored on import unless present (discouraged)
    String password = stringVal(map.get("password"));
    if (StringUtils.isNotBlank(password)) {
      repo.setPassword(password);
    }
    repo.setBrowse(boolVal(map.get("browse"), false));
    repo.setCatalogUrl(stringVal(map.get("catalogUrl")));
    repo.setSearchQuery(stringVal(map.get("searchQuery")));
    repo.setIncludeSnapshots(boolVal(map.get("includeSnapshots"), true));
    repo.setGroupIdFilter(stringVal(map.get("groupIdFilter")));
    repo.setHomepage(stringVal(map.get("homepage")));
    repo.setDescription(stringVal(map.get("description")));
    repo.setPlugins(parsePlugins(map.get("plugins"), repo.getId()));
    return repo;
  }

  static List<OptionalPluginInfo> parsePlugins(Object pluginsNode, String defaultSource) {
    List<OptionalPluginInfo> out = new ArrayList<>();
    if (!(pluginsNode instanceof List<?> list)) {
      return out;
    }
    for (Object item : list) {
      if (!(item instanceof Map<?, ?> m)) {
        continue;
      }
      OptionalPluginInfo info = new OptionalPluginInfo();
      info.setGroupId(stringVal(m.get("groupId")));
      info.setArtifactId(stringVal(m.get("artifactId")));
      info.setVersion(stringVal(m.get("version")));
      info.setName(stringVal(m.get("name")));
      info.setCategory(stringVal(m.get("category")));
      info.setDescription(stringVal(m.get("description")));
      info.setInstallPath(stringVal(m.get("installPath")));
      info.setLastUpdated(stringVal(m.get("lastUpdated")));
      info.setMinHopVersion(stringVal(m.get("minHopVersion")));
      info.setMaxHopVersion(stringVal(m.get("maxHopVersion")));
      info.setSource(stringVal(m.get("source")));
      if (StringUtils.isBlank(info.getSource())) {
        info.setSource(defaultSource);
      }
      if (StringUtils.isBlank(info.getCategory())) {
        info.setCategory("auto-discovered");
      }
      if (StringUtils.isBlank(info.getName())) {
        info.setName(info.getArtifactId());
      }
      if (StringUtils.isNotBlank(info.getArtifactId())) {
        out.add(info);
      }
    }
    return out;
  }

  public static void save(Path file, MarketplaceRepository repo) throws HopException {
    if (file == null) {
      throw new HopException("Output path is required");
    }
    if (repo == null) {
      throw new HopException("Repository is required");
    }
    try {
      Path parent = file.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      Map<String, Object> map = toYamlMap(repo, false);
      String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
      if (name.endsWith(".json")) {
        HopJson.newMapper().writerWithDefaultPrettyPrinter().writeValue(file.toFile(), map);
        return;
      }
      DumperOptions options = new DumperOptions();
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
      options.setPrettyFlow(true);
      options.setIndent(2);
      Yaml yaml = new Yaml(options);
      try (Writer writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
        writer.write("# Hop marketplace repository definition (shareable; no passwords)\n");
        yaml.dump(map, writer);
      }
    } catch (IOException e) {
      throw new HopException("Unable to write repository definition: " + file, e);
    }
  }

  /** Map for export; passwords omitted unless {@code includePassword} is true. */
  public static Map<String, Object> toYamlMap(MarketplaceRepository repo, boolean includePassword) {
    Map<String, Object> root = new LinkedHashMap<>();
    root.put("schemaVersion", SCHEMA_VERSION);
    root.put("kind", KIND);
    root.put("id", repo.getId());
    if (StringUtils.isNotBlank(repo.getName())) {
      root.put("name", repo.getName());
    }
    root.put("url", repo.getUrl());
    root.put("primary", repo.isPrimary());
    root.put("enabled", repo.isEnabled());
    if (StringUtils.isNotBlank(repo.getUsername())) {
      root.put("username", repo.getUsername());
    }
    if (includePassword && StringUtils.isNotBlank(repo.getPassword())) {
      root.put("password", repo.getPassword());
    }
    root.put("browse", repo.isBrowse());
    if (StringUtils.isNotBlank(repo.getCatalogUrl())) {
      root.put("catalogUrl", repo.getCatalogUrl());
    }
    if (StringUtils.isNotBlank(repo.getSearchQuery())) {
      root.put("searchQuery", repo.getSearchQuery());
    }
    root.put("includeSnapshots", repo.isIncludeSnapshots());
    if (StringUtils.isNotBlank(repo.getGroupIdFilter())) {
      root.put("groupIdFilter", repo.getGroupIdFilter());
    }
    if (StringUtils.isNotBlank(repo.getHomepage())) {
      root.put("homepage", repo.getHomepage());
    }
    if (StringUtils.isNotBlank(repo.getDescription())) {
      root.put("description", repo.getDescription());
    }
    if (repo.getPlugins() != null && !repo.getPlugins().isEmpty()) {
      List<Map<String, Object>> plugins = new ArrayList<>();
      for (OptionalPluginInfo p : repo.getPlugins()) {
        if (p == null || StringUtils.isBlank(p.getArtifactId())) {
          continue;
        }
        Map<String, Object> m = new LinkedHashMap<>();
        if (StringUtils.isNotBlank(p.getGroupId())) {
          m.put("groupId", p.getGroupId());
        }
        m.put("artifactId", p.getArtifactId());
        if (StringUtils.isNotBlank(p.getVersion())) {
          m.put("version", p.getVersion());
        }
        if (StringUtils.isNotBlank(p.getName())) {
          m.put("name", p.getName());
        }
        if (StringUtils.isNotBlank(p.getCategory())) {
          m.put("category", p.getCategory());
        }
        if (StringUtils.isNotBlank(p.getDescription())) {
          m.put("description", p.getDescription());
        }
        if (StringUtils.isNotBlank(p.getLastUpdated())) {
          m.put("lastUpdated", p.getLastUpdated());
        }
        if (StringUtils.isNotBlank(p.getInstallPath())) {
          m.put("installPath", p.getInstallPath());
        }
        if (StringUtils.isNotBlank(p.getMinHopVersion())) {
          m.put("minHopVersion", p.getMinHopVersion());
        }
        if (StringUtils.isNotBlank(p.getMaxHopVersion())) {
          m.put("maxHopVersion", p.getMaxHopVersion());
        }
        plugins.add(m);
      }
      if (!plugins.isEmpty()) {
        root.put("plugins", plugins);
      }
    }
    return root;
  }

  public static void applyToConfig(
      MarketplaceConfig config, MarketplaceRepository imported, boolean makePrimary)
      throws HopException {
    if (config == null || imported == null) {
      throw new HopException("Config and repository are required");
    }
    if (makePrimary) {
      imported.setPrimary(true);
    }
    MarketplaceRepository existing = config.findRepository(imported.getId());
    if (existing == null) {
      config.addRepository(imported);
    } else {
      // Upsert discovery + connection fields; keep password if import omitted it
      existing.setName(imported.getName());
      existing.setUrl(imported.getUrl());
      existing.setEnabled(imported.isEnabled());
      if (imported.isPrimary() || makePrimary) {
        config.setPrimary(existing.getId());
      }
      if (StringUtils.isNotBlank(imported.getUsername())) {
        existing.setUsername(imported.getUsername());
      }
      if (StringUtils.isNotBlank(imported.getPassword())) {
        existing.setPassword(imported.getPassword());
      }
      existing.setBrowse(imported.isBrowse());
      existing.setCatalogUrl(imported.getCatalogUrl());
      existing.setSearchQuery(imported.getSearchQuery());
      existing.setIncludeSnapshots(imported.isIncludeSnapshots());
      existing.setGroupIdFilter(imported.getGroupIdFilter());
      existing.setHomepage(imported.getHomepage());
      existing.setDescription(imported.getDescription());
      if (imported.getPlugins() != null) {
        existing.setPlugins(new ArrayList<>(imported.getPlugins()));
      }
      config.ensureValidPrimary();
    }
  }

  private static String stringVal(Object o) {
    if (o == null) {
      return null;
    }
    String s = String.valueOf(o).trim();
    return s.isEmpty() ? null : s;
  }

  private static boolean boolVal(Object o, boolean defaultValue) {
    if (o == null) {
      return defaultValue;
    }
    if (o instanceof Boolean b) {
      return b;
    }
    return Boolean.parseBoolean(String.valueOf(o).trim());
  }
}
