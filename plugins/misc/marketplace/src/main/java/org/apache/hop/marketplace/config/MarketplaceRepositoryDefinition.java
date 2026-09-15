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
import java.util.Objects;
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
 * are never written on export unless explicitly asked for, and then only obfuscated (see {@link
 * MarketplaceSecrets}).
 */
public final class MarketplaceRepositoryDefinition {

  public static final String KIND = "hop-marketplace-repository";
  public static final String SCHEMA_VERSION = "1.0";

  private static final String HTTPS_PREFIX = "https://";

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

  /**
   * Download and parse a definition published at a public URL, for import from an address the user
   * typed or pasted.
   *
   * <p>Differs from {@link #loadFromHttp(String)} on two points, both because the address is not
   * necessarily trusted:
   *
   * <ul>
   *   <li>The request is anonymous. Credentials are never offered to a pasted address, so a hostile
   *       URL cannot collect the {@code HOP_MARKETPLACE_*} credentials that apply to every
   *       repository.
   *   <li>Any {@code username} / {@code password} in the downloaded file is dropped. A definition
   *       fetched over the network describes <em>where</em> a repository is, never <em>who</em>
   *       connects to it; credentials are supplied locally, keyed by repository id.
   * </ul>
   *
   * <p>Plain HTTP is refused: the definition names the hosts that plugin code is downloaded from,
   * so it must not be modifiable in transit.
   *
   * <p>Only the download is anonymous. Once imported, the repository is an ordinary configuration
   * entry and is contacted with whatever credentials are configured for it.
   */
  public static MarketplaceRepository loadFromPublicUrl(String url) throws HopException {
    String trimmed = requireHttps(url);
    return withoutCredentials(download(trimmed, null));
  }

  /** Trimmed URL, or a failure explaining why only https is accepted. */
  static String requireHttps(String url) throws HopException {
    if (StringUtils.isBlank(url)) {
      throw new HopException("Repository definition URL is required");
    }
    String trimmed = url.trim();
    if (!trimmed.regionMatches(true, 0, HTTPS_PREFIX, 0, HTTPS_PREFIX.length())) {
      throw new HopException(
          "Repository definitions can only be imported over https, so that they cannot be modified"
              + " in transit. Download the file and import it from disk if the host has no TLS: "
              + trimmed);
    }
    return trimmed;
  }

  /**
   * Drop credentials from a definition that came off the network. Everything describing where the
   * repository is survives; only who connects to it is removed.
   */
  static MarketplaceRepository withoutCredentials(MarketplaceRepository imported) {
    if (imported != null) {
      imported.setUsername(null);
      imported.setPassword(null);
    }
    return imported;
  }

  /**
   * Download and parse a shared repository definition.
   *
   * <p>No repository entry exists yet at import time, so credentials can only come from the
   * environment; a bare repository resolves exactly those. Without this, a definition published in
   * a private repository could not be imported by URL at all.
   */
  public static MarketplaceRepository loadFromHttp(String url) throws HopException {
    return download(url, new MarketplaceRepository("import", url));
  }

  /**
   * Fetch and parse a definition. A null {@code credentials} repository makes the request
   * anonymous.
   */
  private static MarketplaceRepository download(String url, MarketplaceRepository credentials)
      throws HopException {
    try {
      HttpResponse<InputStream> response =
          MarketplaceHttp.send(
              MarketplaceHttp.newClient(),
              url,
              Duration.ofSeconds(60),
              credentials,
              HttpResponse.BodyHandlers.ofInputStream());
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new HopException(
            "Unable to download repository definition from "
                + url
                + " (HTTP "
                + response.statusCode()
                + ")"
                + downloadHint(response.statusCode(), credentials));
      }
      try (InputStream in = response.body()) {
        return parse(in, url);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new HopException("Unable to download repository definition from " + url, e);
    }
  }

  /**
   * Explain a failed download. The anonymous path cannot act on the environment credentials {@link
   * MarketplaceHttp#authHint} suggests, so it says what actually applies: the definition itself has
   * to be readable without credentials.
   */
  private static String downloadHint(int status, MarketplaceRepository credentials) {
    if (credentials != null) {
      return MarketplaceHttp.authHint(status, credentials);
    }
    if (MarketplaceHttp.isAuthFailure(status)) {
      return ". The definition was requested without credentials, which is deliberate for an"
          + " address that was typed or pasted. Publish the definition so it can be read without"
          + " authentication, or download the file and import it from disk.";
    }
    return "";
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
      // Obfuscated when exported by Hop, clear text when hand-written: both are accepted.
      repo.setPassword(MarketplaceSecrets.decode(password));
    }
    repo.setBrowse(boolVal(map.get("browse"), false));
    repo.setCatalogUrl(stringVal(map.get("catalogUrl")));
    repo.setUrlTemplate(stringVal(map.get("urlTemplate")));
    String browserType = stringVal(map.get("browserType"));
    if (StringUtils.isNotBlank(browserType)) {
      repo.setBrowserType(browserType);
    }
    String authType = stringVal(map.get("authType"));
    if (StringUtils.isNotBlank(authType)) {
      repo.setAuthType(authType);
    }
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
      root.put("password", MarketplaceSecrets.encode(repo.getPassword()));
    }
    root.put("browse", repo.isBrowse());
    if (StringUtils.isNotBlank(repo.getCatalogUrl())) {
      root.put("catalogUrl", repo.getCatalogUrl());
    }
    if (StringUtils.isNotBlank(repo.getUrlTemplate())) {
      root.put("urlTemplate", repo.getUrlTemplate());
    }
    // Only export an explicit choice; auto-detection is the default and stays implicit.
    if (StringUtils.isNotBlank(repo.getBrowserType())
        && !MarketplaceRepository.BROWSER_AUTO.equalsIgnoreCase(repo.getBrowserType())) {
      root.put("browserType", repo.getBrowserType());
    }
    if (StringUtils.isNotBlank(repo.getAuthType())
        && !MarketplaceRepository.AUTH_AUTO.equalsIgnoreCase(repo.getAuthType())) {
      root.put("authType", repo.getAuthType());
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

  /**
   * What importing {@code imported} would change beyond adding one repository.
   *
   * @param takesOverPrimary the definition claims {@code primary: true} while a different
   *     repository currently holds it, so every install would try the new one first
   * @param currentPrimaryName display name of the repository that would be demoted, or null
   * @param noPublicFallback neither the Apache nor the Maven Central repository is configured and
   *     enabled, so nothing public is left to fall back on
   */
  public record ImportRisk(
      boolean takesOverPrimary, String currentPrimaryName, boolean noPublicFallback) {

    public boolean isSafe() {
      return !takesOverPrimary && !noPublicFallback;
    }
  }

  /**
   * Inspect an import before applying it. Importing adds or updates a single repository, with one
   * exception: a definition may declare itself primary and demote the repository that holds that
   * role today. That is worth surfacing when the definition came from somewhere else.
   */
  public static ImportRisk assess(MarketplaceConfig config, MarketplaceRepository imported) {
    if (config == null || imported == null) {
      return new ImportRisk(false, null, false);
    }
    MarketplaceRepository currentPrimary = null;
    boolean publicFallback = false;
    for (MarketplaceRepository repo : nullSafe(config.getRepositories())) {
      if (repo == null || !repo.isEnabled()) {
        continue;
      }
      if (repo.isPrimary() && currentPrimary == null) {
        currentPrimary = repo;
      }
      publicFallback |= isPublicDefault(repo);
    }
    // Re-importing the definition of the repository that is already primary changes nothing.
    boolean takesOver =
        imported.isPrimary()
            && currentPrimary != null
            && !Objects.equals(currentPrimary.getId(), imported.getId());
    return new ImportRisk(
        takesOver, takesOver ? currentPrimary.displayName() : null, !publicFallback);
  }

  /**
   * The shipped public repositories, matched on id or URL: an id can be renamed, and a URL can be
   * pointed at a mirror, so either identifies one.
   */
  private static boolean isPublicDefault(MarketplaceRepository repo) {
    String url = repo.normalizedUrl();
    return MarketplaceConfig.DEFAULT_ASF_ID.equals(repo.getId())
        || MarketplaceConfig.DEFAULT_CENTRAL_ID.equals(repo.getId())
        || MarketplaceConfig.DEFAULT_ASF_URL.equals(url)
        || MarketplaceConfig.DEFAULT_CENTRAL_URL.equals(url);
  }

  private static <T> List<T> nullSafe(List<T> list) {
    return list == null ? List.of() : list;
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
      existing.setBrowserType(imported.getBrowserType());
      existing.setAuthType(imported.getAuthType());
      existing.setUrlTemplate(imported.getUrlTemplate());
      existing.setSearchQuery(imported.getSearchQuery());
      existing.setIncludeSnapshots(imported.isIncludeSnapshots());
      existing.setGroupIdFilter(imported.getGroupIdFilter());
      existing.setHomepage(imported.getHomepage());
      existing.setDescription(imported.getDescription());
      // Multiple per-plugin YAMLs may share one repository id (e.g. community Nexus).
      // Merge plugin metadata by G:A; empty import must not wipe existing entries.
      if (imported.getPlugins() != null && !imported.getPlugins().isEmpty()) {
        existing.setPlugins(mergePlugins(existing.getPlugins(), imported.getPlugins()));
      }
      config.ensureValidPrimary();
    }
  }

  /**
   * Merge plugin metadata lists for same-id repository import.
   *
   * <p>Existing entries not present in {@code imported} are kept. Imported entries with a matching
   * G:A (or artifactId when groupId is missing on either side) replace the prior entry so re-import
   * refreshes version and display fields. New G:A values are appended. Order: prior plugins first,
   * then newly appended imports.
   */
  static List<OptionalPluginInfo> mergePlugins(
      List<OptionalPluginInfo> existing, List<OptionalPluginInfo> imported) {
    List<OptionalPluginInfo> result = new ArrayList<>();
    if (existing != null) {
      for (OptionalPluginInfo p : existing) {
        if (p != null && StringUtils.isNotBlank(p.getArtifactId())) {
          result.add(p);
        }
      }
    }
    if (imported == null || imported.isEmpty()) {
      return result;
    }
    for (OptionalPluginInfo incoming : imported) {
      if (incoming == null || StringUtils.isBlank(incoming.getArtifactId())) {
        continue;
      }
      int idx = indexOfMatchingPlugin(result, incoming);
      if (idx >= 0) {
        result.set(idx, incoming);
      } else {
        result.add(incoming);
      }
    }
    return result;
  }

  /**
   * Find an existing plugin that should be updated by {@code candidate}: prefer groupId+artifactId,
   * fall back to artifactId alone when either side lacks groupId.
   */
  static int indexOfMatchingPlugin(List<OptionalPluginInfo> plugins, OptionalPluginInfo candidate) {
    if (plugins == null || candidate == null || StringUtils.isBlank(candidate.getArtifactId())) {
      return -1;
    }
    String candArt = candidate.getArtifactId().toLowerCase(Locale.ROOT);
    String candGa = pluginGaKey(candidate);
    boolean candHasGroup = StringUtils.isNotBlank(candidate.getGroupId());

    for (int i = 0; i < plugins.size(); i++) {
      OptionalPluginInfo p = plugins.get(i);
      if (p == null || StringUtils.isBlank(p.getArtifactId())) {
        continue;
      }
      if (!candArt.equals(p.getArtifactId().toLowerCase(Locale.ROOT))) {
        continue;
      }
      boolean pHasGroup = StringUtils.isNotBlank(p.getGroupId());
      if (candHasGroup && pHasGroup) {
        if (candGa.equals(pluginGaKey(p))) {
          return i;
        }
        // Same artifactId, different groupId → distinct plugins
        continue;
      }
      // One or both sides lack groupId: treat as the same plugin
      return i;
    }
    return -1;
  }

  /** Lowercase {@code groupId:artifactId}; blank groupId becomes empty prefix. */
  static String pluginGaKey(OptionalPluginInfo info) {
    if (info == null || StringUtils.isBlank(info.getArtifactId())) {
      return "";
    }
    String g =
        StringUtils.isNotBlank(info.getGroupId()) ? info.getGroupId().toLowerCase(Locale.ROOT) : "";
    return g + ":" + info.getArtifactId().toLowerCase(Locale.ROOT);
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
