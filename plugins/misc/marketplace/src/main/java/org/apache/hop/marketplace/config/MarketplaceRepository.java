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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.UnaryOperator;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;

@Getter
@Setter
public class MarketplaceRepository {

  /** Detect the browse API from the repository URL. */
  public static final String BROWSER_AUTO = "auto";

  /** Sonatype Nexus 3 REST search ({@code /service/rest/v1/search}). */
  public static final String BROWSER_NEXUS = "nexus";

  /** Forgejo / Gitea package registry ({@code /api/v1/packages/{owner}}). */
  public static final String BROWSER_FORGEJO = "forgejo";

  /** Marker in a Forgejo / Gitea package registry URL. */
  private static final String FORGEJO_PACKAGES_MARKER = "/api/packages/";

  private String id = MarketplaceConfig.DEFAULT_ASF_ID;
  private String name;
  private String url = MarketplaceConfig.DEFAULT_ASF_URL;

  /** When true, this repository is tried first for installs. */
  private boolean primary;

  /** When false, skipped in the install fallback chain. */
  private boolean enabled = true;

  /** Optional HTTP Basic auth username. */
  private String username;

  /**
   * Optional HTTP Basic auth password. Prefer leaving this empty in hop-config.json and setting
   * {@code HOP_MARKETPLACE_PASSWORD} instead for private repos. Do not set for anonymous ASF /
   * Central / local Nexus.
   */
  private String password;

  /**
   * When true, include this repository in {@code marketplace query} and the GUI Plugins tab (live
   * Nexus zip list, or {@link #catalogUrl} if set). Default false so ASF/Central stay install-only
   * endpoints.
   */
  private boolean browse;

  /**
   * Optional remote catalog index URL (YAML/JSON). Advanced only — prefer live Nexus zip listing
   * when {@link #browse} is true.
   */
  private String catalogUrl;

  /**
   * Which repository-manager API live browsing speaks: {@code auto} (default), {@code nexus} or
   * {@code forgejo}. Only used when {@link #browse} is true and no {@link #catalogUrl} is set;
   * downloads are plain Maven layout unless {@link #urlTemplate} says otherwise.
   */
  private String browserType = BROWSER_AUTO;

  /**
   * Optional download URL template for repositories that do not serve Maven layout — release
   * assets, static file hosts, CDNs. When set, {@link #url} is ignored for plugin zip downloads and
   * this template is expanded instead.
   *
   * <p>Placeholders: <code>${groupId}</code>, <code>${groupPath}</code> (groupId with dots replaced
   * by slashes), <code>${artifactId}</code> and <code>${version}</code>. Example for a Forgejo
   * release asset:
   *
   * <pre>
   * https://forge.example.org/acme/dist/releases/download/v${version}/${artifactId}-${version}.zip
   * </pre>
   *
   * <p>There is no Maven metadata behind such a host, so versions must be exact — pair this with a
   * {@link #catalogUrl} that pins them. SNAPSHOT resolution does not apply.
   */
  private String urlTemplate;

  /**
   * Optional plugin metadata from a shareable repository definition (import/export). Used to enrich
   * live discovery results (names, categories, descriptions) or as a fallback list if live browse
   * returns nothing. Not a discovery cache — listing always prefers a live Nexus/catalog fetch.
   */
  private List<OptionalPluginInfo> plugins = new ArrayList<>();

  /**
   * Optional search/filter string applied when browsing this repository (substring over GAV /
   * catalog fields). Empty means no extra filter beyond {@link #groupIdFilter}.
   */
  private String searchQuery;

  /** When false, SNAPSHOT versions are hidden from discovery results. Default true. */
  private boolean includeSnapshots = true;

  /** Optional Maven groupId restriction for discovery (e.g. {@code com.acme.hop}). */
  private String groupIdFilter;

  /** Optional human homepage for this marketplace (documentation only). */
  private String homepage;

  /** Optional human description (documentation / export). */
  private String description;

  public MarketplaceRepository() {
    // Jackson
  }

  public MarketplaceRepository(String id, String url) {
    this.id = id;
    this.url = url;
    this.name = id;
  }

  public MarketplaceRepository(String id, String url, boolean primary) {
    this(id, url);
    this.primary = primary;
  }

  public MarketplaceRepository(String id, String name, String url, boolean primary) {
    this.id = id;
    this.name = name;
    this.url = url;
    this.primary = primary;
  }

  public MarketplaceRepository(String id, String url, String username, String password) {
    this(id, url);
    this.username = username;
    this.password = password;
  }

  public String displayName() {
    if (StringUtils.isNotBlank(name)) {
      return name;
    }
    return StringUtils.isNotBlank(id) ? id : normalizedUrl();
  }

  /** Base URL always ending with {@code /}. */
  public String normalizedUrl() {
    if (StringUtils.isBlank(url)) {
      return MarketplaceConfig.DEFAULT_ASF_URL;
    }
    return url.endsWith("/") ? url : url + "/";
  }

  /**
   * Environment lookup, replaceable in tests. Package-private on purpose: credential resolution is
   * otherwise untestable, and it is the part most likely to go subtly wrong.
   */
  private static UnaryOperator<String> environment = System::getenv;

  static void setEnvironmentForTesting(UnaryOperator<String> lookup) {
    environment = lookup == null ? System::getenv : lookup;
  }

  private static String env(String name) {
    return name == null ? null : StringUtils.trimToNull(environment.apply(name));
  }

  /**
   * This repository's environment variable prefix, e.g. {@code ACME} for id {@code acme} or {@code
   * LOCAL_NEXUS} for {@code local-nexus}.
   */
  public String environmentIdPrefix() {
    if (StringUtils.isBlank(id)) {
      return "";
    }
    return id.trim().toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "_");
  }

  private String scopedEnv(String suffix) {
    String prefix = environmentIdPrefix();
    return StringUtils.isBlank(prefix) ? null : env("HOP_MARKETPLACE_" + prefix + "_" + suffix);
  }

  /**
   * Effective credentials, most specific first: the repository's own fields, then repository-scoped
   * environment variables ({@code HOP_MARKETPLACE_<ID>_USERNAME}), then the global {@code
   * HOP_MARKETPLACE_USERNAME}. No credentials means anonymous HTTP.
   *
   * <p>Scoped variables exist so several private repositories can each have their own token; the
   * global pair applies to every repository, which is why {@link #credentialsFromEnvironmentOnly()}
   * exists to let a rejected global credential fall back to anonymous.
   */
  public String effectiveUsername() {
    if (StringUtils.isNotBlank(username)) {
      return username;
    }
    return firstNonBlank(
        scopedEnv("USERNAME"),
        scopedEnv("USER"),
        env("HOP_MARKETPLACE_USERNAME"),
        env("HOP_MARKETPLACE_USER"));
  }

  public String effectivePassword() {
    if (StringUtils.isNotBlank(password)) {
      return password;
    }
    return firstNonBlank(scopedEnv("PASSWORD"), env("HOP_MARKETPLACE_PASSWORD"));
  }

  /**
   * True when credentials were supplied by the environment rather than by this repository entry.
   *
   * <p>Such credentials are not necessarily meant for this repository — the global variables apply
   * to all of them — so a server that rejects them is retried anonymously. Credentials configured
   * on the entry itself are deliberate and are never dropped.
   */
  public boolean credentialsFromEnvironmentOnly() {
    return StringUtils.isAllBlank(username, password) && hasCredentials();
  }

  /**
   * Resolved browse backend. When {@link #browserType} is blank or {@code auto}, a Forgejo / Gitea
   * package registry URL ({@code .../api/packages/{owner}/maven}) selects {@code forgejo}; anything
   * else falls back to {@code nexus}, which is the historical behaviour.
   */
  public String effectiveBrowserType() {
    if (StringUtils.isNotBlank(browserType) && !BROWSER_AUTO.equalsIgnoreCase(browserType.trim())) {
      return browserType.trim().toLowerCase(Locale.ROOT);
    }
    return url != null && url.contains(FORGEJO_PACKAGES_MARKER) ? BROWSER_FORGEJO : BROWSER_NEXUS;
  }

  public boolean hasCredentials() {
    return StringUtils.isNotBlank(effectiveUsername())
        && StringUtils.isNotBlank(effectivePassword());
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String v : values) {
      if (StringUtils.isNotBlank(v)) {
        return v;
      }
    }
    return null;
  }
}
