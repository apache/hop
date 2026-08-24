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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
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

  /** JFrog Artifactory ({@code /api/search/aql} and {@code /api/storage}). */
  public static final String BROWSER_JFROG = "jfrog";

  /** Marker in a Forgejo / Gitea package registry URL. */
  private static final String FORGEJO_PACKAGES_MARKER = "/api/packages/";

  /** Marker in a JFrog Artifactory repository URL. */
  private static final String ARTIFACTORY_MARKER = "/artifactory/";

  /**
   * Pick the scheme from what is configured: Basic when both parts resolve, anonymous otherwise.
   */
  public static final String AUTH_AUTO = "auto";

  /** Never send credentials, even when the global {@code HOP_MARKETPLACE_*} variables are set. */
  public static final String AUTH_NONE = "none";

  /** HTTP Basic with username and password. */
  public static final String AUTH_BASIC = "basic";

  /** Bearer token; {@link #password} holds the token and the username is unused. */
  public static final String AUTH_TOKEN = "token";

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
   * Optional secret, held in plain form in memory and obfuscated in hop-config.json (see {@link
   * MarketplaceSecrets}). This is the Basic auth password, and with {@link #AUTH_TOKEN} it is the
   * bearer token — one field so that obfuscation, variable resolution and the {@code
   * HOP_MARKETPLACE_*} fallbacks apply to both without being written twice.
   *
   * <p>Obfuscation is not encryption, so for private repos prefer a variable ({@code ${MY_TOKEN}}),
   * a variable resolver expression, or {@code HOP_MARKETPLACE_PASSWORD} — those keep the secret out
   * of the file entirely. Do not set for anonymous ASF / Central / local Nexus.
   */
  @JsonSerialize(using = MarketplaceSecrets.Serializer.class)
  @JsonDeserialize(using = MarketplaceSecrets.Deserializer.class)
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
   * Which repository-manager API live browsing speaks: {@code auto} (default), {@code nexus},
   * {@code forgejo} or {@code jfrog}. Only used when {@link #browse} is true and no {@link
   * #catalogUrl} is set; downloads are plain Maven layout unless {@link #urlTemplate} says
   * otherwise.
   */
  private String browserType = BROWSER_AUTO;

  /**
   * How this repository is authenticated: {@code auto} (default), {@code none}, {@code basic} or
   * {@code token}.
   *
   * <p>{@code auto} reproduces the historical rule — Basic when a username and password both
   * resolve, anonymous otherwise. It never selects {@code token}: the global {@code
   * HOP_MARKETPLACE_PASSWORD} applies to every repository including public ones, so inferring a
   * bearer token from a lone password would start sending that secret to hosts that today receive
   * nothing. Token authentication is opt-in.
   *
   * <p>{@code none} is not the same as leaving credentials unset: it suppresses the environment
   * credentials as well, which is how a public repository opts out of globally configured ones.
   */
  private String authType = AUTH_AUTO;

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
   *
   * <p>Credentials configured on the entry may be variables or variable resolver expressions; they
   * are resolved here, at use, so the config file only ever holds the reference.
   */
  public String effectiveUsername() {
    if (StringUtils.isNotBlank(username)) {
      return MarketplaceSecrets.resolve(username);
    }
    return firstNonBlank(
        scopedEnv("USERNAME"),
        scopedEnv("USER"),
        env("HOP_MARKETPLACE_USERNAME"),
        env("HOP_MARKETPLACE_USER"));
  }

  public String effectivePassword() {
    if (StringUtils.isNotBlank(password)) {
      return MarketplaceSecrets.resolve(password);
    }
    return firstNonBlank(
        scopedEnv("PASSWORD"), scopedEnv("TOKEN"), env("HOP_MARKETPLACE_PASSWORD"));
  }

  /**
   * The bearer token used by {@link #AUTH_TOKEN}, which is the same secret as {@link
   * #effectivePassword()}. {@code HOP_MARKETPLACE_<ID>_TOKEN} reads better than {@code _PASSWORD}
   * for a token and resolves to the same value; there is no global {@code HOP_MARKETPLACE_TOKEN},
   * because a token belongs to one repository rather than to all of them.
   */
  public String effectiveToken() {
    return effectivePassword();
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
   * Resolved browse backend. When {@link #browserType} is blank or {@code auto}, the URL decides: a
   * Forgejo / Gitea package registry ({@code .../api/packages/{owner}/maven}) selects {@code
   * forgejo} and an Artifactory repository ({@code .../artifactory/{repo}/}) selects {@code jfrog}.
   * Anything else falls back to {@code nexus}, which is the historical behaviour.
   *
   * <p>Forgejo is tested first because its marker is the more specific of the two; a host serving
   * both would otherwise be misread.
   */
  public String effectiveBrowserType() {
    if (StringUtils.isNotBlank(browserType) && !BROWSER_AUTO.equalsIgnoreCase(browserType.trim())) {
      return browserType.trim().toLowerCase(Locale.ROOT);
    }
    if (url == null) {
      return BROWSER_NEXUS;
    }
    if (url.contains(FORGEJO_PACKAGES_MARKER)) {
      return BROWSER_FORGEJO;
    }
    return url.contains(ARTIFACTORY_MARKER) ? BROWSER_JFROG : BROWSER_NEXUS;
  }

  /**
   * Resolved authentication scheme. An explicit {@link #authType} is returned as configured, even
   * when the credentials it needs are missing: reporting the requested scheme is what lets {@link
   * MarketplaceHttp#authHint} say that {@code basic} was asked for but no username resolved,
   * instead of silently falling back to anonymous and returning an unexplained 401.
   *
   * <p>{@code auto} resolves to {@link #AUTH_BASIC} when a username and password both resolve and
   * {@link #AUTH_NONE} otherwise, which is exactly how this class behaved before {@code authType}
   * existed.
   */
  public String effectiveAuthType() {
    if (StringUtils.isNotBlank(authType) && !AUTH_AUTO.equalsIgnoreCase(authType.trim())) {
      return authType.trim().toLowerCase(Locale.ROOT);
    }
    return StringUtils.isNotBlank(effectiveUsername())
            && StringUtils.isNotBlank(effectivePassword())
        ? AUTH_BASIC
        : AUTH_NONE;
  }

  /**
   * True when a request for this repository will carry an Authorization header — the scheme is one
   * that authenticates, and everything it needs resolves. An unrecognised {@link #authType} sends
   * nothing rather than guessing.
   */
  public boolean hasCredentials() {
    return switch (effectiveAuthType()) {
      case AUTH_BASIC ->
          StringUtils.isNotBlank(effectiveUsername())
              && StringUtils.isNotBlank(effectivePassword());
      case AUTH_TOKEN -> StringUtils.isNotBlank(effectiveToken());
      default -> false;
    };
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
