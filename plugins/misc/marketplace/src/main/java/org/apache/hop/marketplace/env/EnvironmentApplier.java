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

package org.apache.hop.marketplace.env;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.marketplace.catalog.OptionalPluginCatalog;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.command.MarketplaceCommand;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.install.IInstallListener;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.marketplace.install.PluginUninstaller;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.apache.hop.marketplace.resolve.MavenRepositoryClient;

/** Applies or validates a {@link HopInstallSpec} against a Hop installation. */
public class EnvironmentApplier {

  private final ILogChannel log;
  private final Path hopHome;
  private final MarketplaceConfig baseConfig;

  /** Id of a spec repository that has none; a second one gets {@code spec-2}, and so on. */
  static final String DEFAULT_SPEC_REPOSITORY_ID = "spec";

  public EnvironmentApplier(ILogChannel log, Path hopHome, MarketplaceConfig baseConfig) {
    this.log = log;
    this.hopHome = hopHome;
    this.baseConfig = baseConfig;
  }

  public EnvironmentDrift validate(HopInstallSpec env) throws HopException {
    EnvironmentDrift drift = new EnvironmentDrift();
    String defaultVersion = resolveEnvVersion(env);

    for (HopInstallSpec.PluginRef ref : nullSafe(env.getPlugins())) {
      if (StringUtils.isBlank(ref.getArtifactId())) {
        continue;
      }
      String groupId =
          StringUtils.isNotBlank(ref.getGroupId()) ? ref.getGroupId() : baseConfig.getGroupId();
      String version = StringUtils.isNotBlank(ref.getVersion()) ? ref.getVersion() : defaultVersion;
      InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, ref.getArtifactId());
      boolean onDisk = isPluginOnDisk(ref.getArtifactId());
      if ((!onDisk && receipt == null) || (receipt != null && receipt.isUninstallPending())) {
        drift.getMissingPlugins().add(groupId + ":" + ref.getArtifactId() + ":" + version);
        continue;
      }
      if (receipt != null
          && StringUtils.isNotBlank(version)
          && !version.equals(receipt.getVersion())) {
        drift
            .getVersionMismatches()
            .add(ref.getArtifactId() + " local=" + receipt.getVersion() + " required=" + version);
      }
    }

    for (HopInstallSpec.DependencyRef dep : nullSafe(env.getDependencies())) {
      if (StringUtils.isAnyBlank(dep.getGroupId(), dep.getArtifactId(), dep.getVersion())) {
        continue;
      }
      String target = StringUtils.defaultIfBlank(dep.getTarget(), "lib/jdbc");
      Path jar =
          hopHome.resolve(target).resolve(dep.getArtifactId() + "-" + dep.getVersion() + ".jar");
      if (!Files.isRegularFile(jar)) {
        // also accept any jar starting with artifactId-
        if (!anyJarPresent(hopHome.resolve(target), dep.getArtifactId())) {
          drift
              .getMissingDependencies()
              .add(dep.getGroupId() + ":" + dep.getArtifactId() + ":" + dep.getVersion());
        }
      }
    }

    return drift;
  }

  /**
   * Install missing plugins/deps; optionally prune marketplace plugins not listed in the spec file.
   */
  public void apply(HopInstallSpec env, boolean prune) throws HopException {
    apply(env, prune, IInstallListener.NONE);
  }

  /**
   * @param listener receives per-artifact and byte-level progress across the whole batch, and can
   *     cancel between chunks. Pass {@link IInstallListener#NONE} for headless callers.
   */
  public void apply(HopInstallSpec env, boolean prune, IInstallListener listener)
      throws HopException {
    IInstallListener progress = listener == null ? IInstallListener.NONE : listener;
    MarketplaceConfig config = configFromEnv(env);
    String defaultVersion = resolveEnvVersion(env);
    PluginInstaller installer = new PluginInstaller(log, hopHome, config);
    installer.activateAllPending();

    // Every reference is one batch item, satisfied ones included: they complete instantly and keep
    // the bar monotonic, which is friendlier than a total that shrinks as we discover what is
    // already present.
    int totalItems = nullSafe(env.getPlugins()).size() + nullSafe(env.getDependencies()).size();
    int itemIndex = 0;

    Set<String> desiredArtifacts = new HashSet<>();
    for (HopInstallSpec.PluginRef ref : nullSafe(env.getPlugins())) {
      if (StringUtils.isBlank(ref.getArtifactId())) {
        itemIndex++;
        continue;
      }
      if (progress.isCancelled()) {
        throw new HopException("Applying the install spec was cancelled");
      }
      progress.item(ref.getArtifactId(), itemIndex++, totalItems);
      desiredArtifacts.add(ref.getArtifactId());
      String groupId =
          StringUtils.isNotBlank(ref.getGroupId()) ? ref.getGroupId() : config.getGroupId();
      String version = StringUtils.isNotBlank(ref.getVersion()) ? ref.getVersion() : defaultVersion;
      InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, ref.getArtifactId());
      boolean onDisk = isPluginOnDisk(ref.getArtifactId());
      // Present on disk without a receipt (e.g. install-wave1-plugins.sh) counts as satisfied.
      boolean versionMismatch =
          receipt != null
              && StringUtils.isNotBlank(version)
              && !version.equals(receipt.getVersion());
      boolean needsInstall =
          (!onDisk && receipt == null)
              || versionMismatch
              || (receipt != null && receipt.isUninstallPending());
      if (needsInstall) {
        MavenCoordinates coords = new MavenCoordinates(groupId, ref.getArtifactId(), version);
        log.logBasic("Applying install spec: installing " + coords.gav());
        installer.install(coords, true, null, null, progress);
      } else {
        log.logBasic("Applying install spec: " + ref.getArtifactId() + " already satisfied");
      }
    }

    for (HopInstallSpec.DependencyRef dep : nullSafe(env.getDependencies())) {
      if (StringUtils.isAnyBlank(dep.getGroupId(), dep.getArtifactId(), dep.getVersion())) {
        itemIndex++;
        continue;
      }
      if (progress.isCancelled()) {
        throw new HopException("Applying the install spec was cancelled");
      }
      progress.item(dep.getArtifactId(), itemIndex++, totalItems);
      String target = StringUtils.defaultIfBlank(dep.getTarget(), "lib/jdbc");
      Path dir = hopHome.resolve(target);
      Path jar = dir.resolve(dep.getArtifactId() + "-" + dep.getVersion() + ".jar");
      if (Files.isRegularFile(jar)) {
        log.logBasic("Dependency already present: " + jar.getFileName());
        continue;
      }
      MavenCoordinates coords =
          new MavenCoordinates(dep.getGroupId(), dep.getArtifactId(), dep.getVersion());
      String relativePath =
          coords.groupId().replace('.', '/')
              + "/"
              + coords.artifactId()
              + "/"
              + coords.version()
              + "/"
              + coords.artifactId()
              + "-"
              + coords.version()
              + ".jar";
      log.logBasic("Downloading dependency " + coords.gav() + " → " + target);
      progress.phase(IInstallListener.Phase.DOWNLOAD, target);
      try {
        Files.createDirectories(dir);
        new MavenRepositoryClient(log)
            .downloadArtifact(
                config.primaryRepository(), relativePath, coords.gav(), jar, progress);
      } catch (IOException e) {
        throw new HopException("Failed to install dependency " + coords.gav(), e);
      }
    }

    if (prune) {
      pruneExtras(desiredArtifacts);
    }
  }

  private void pruneExtras(Set<String> desiredArtifacts) throws HopException {
    Path receiptsDir = hopHome.resolve(PluginInstaller.RECEIPTS_DIR);
    if (!Files.isDirectory(receiptsDir)) {
      return;
    }
    PluginUninstaller uninstaller = new PluginUninstaller(log, hopHome);
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(receiptsDir, "*.json")) {
      for (Path file : stream) {
        String name = file.getFileName().toString();
        String artifactId = name.substring(0, name.length() - ".json".length());
        if (!desiredArtifacts.contains(artifactId)) {
          log.logBasic("Pruning marketplace plugin not in install spec: " + artifactId);
          uninstaller.uninstall(artifactId);
        }
      }
    } catch (IOException e) {
      throw new HopException("Failed to scan marketplace receipts for prune", e);
    }
  }

  /** Package-private so the credential scoping below can be asserted without a live install. */
  MarketplaceConfig configFromEnv(HopInstallSpec env) {
    MarketplaceConfig config = new MarketplaceConfig();
    config.setEnabled(baseConfig.isEnabled());
    config.setGroupId(baseConfig.getGroupId());
    config.setDefaultVersion(
        StringUtils.isNotBlank(env.getHopVersion())
            ? env.getHopVersion()
            : MarketplaceCommand.resolveDefaultVersion(baseConfig));
    config.getRepositories().clear();
    if (env.getRepositories() != null && !env.getRepositories().isEmpty()) {
      List<String> ids = repositoryIds(env.getRepositories());
      boolean first = true;
      for (int i = 0; i < env.getRepositories().size(); i++) {
        HopInstallSpec.RepositoryRef ref = env.getRepositories().get(i);
        if (ids.get(i) != null) {
          config.getRepositories().add(repositoryFromRef(ref, ids.get(i), first));
          first = false;
        }
      }
    }
    if (config.getRepositories().isEmpty()) {
      config.getRepositories().addAll(baseConfig.getRepositories());
    }
    if (config.getRepositories().isEmpty()) {
      config.getRepositories().addAll(MarketplaceConfig.defaultRepositories());
    }
    config.ensureValidPrimary();
    return config;
  }

  /**
   * The id of each spec repository, {@code null} for one without a URL. A repository without an id
   * gets {@code spec}, then {@code spec-2}, {@code spec-3} and so on: the install fallback chain
   * drops a repository with the primary's id, so one shared default id would silently lose every
   * fallback after the first.
   */
  private List<String> repositoryIds(List<HopInstallSpec.RepositoryRef> refs) {
    Set<String> taken = new HashSet<>();
    for (HopInstallSpec.RepositoryRef ref : refs) {
      if (ref != null
          && StringUtils.isNotBlank(ref.getUrl())
          && StringUtils.isNotBlank(ref.getId())) {
        if (!taken.add(MarketplaceRepository.environmentIdPrefix(ref.getId()))) {
          log.logBasic(
              "Install spec lists repository id '"
                  + ref.getId().trim()
                  + "' more than once. Give each repository its own id: a repository with the"
                  + " primary's id is not tried as a fallback.");
        }
      }
    }
    List<String> ids = new ArrayList<>();
    int next = 1;
    for (HopInstallSpec.RepositoryRef ref : refs) {
      if (ref == null || StringUtils.isBlank(ref.getUrl())) {
        ids.add(null);
      } else if (StringUtils.isNotBlank(ref.getId())) {
        ids.add(ref.getId());
      } else {
        String id;
        do {
          id = next == 1 ? DEFAULT_SPEC_REPOSITORY_ID : DEFAULT_SPEC_REPOSITORY_ID + "-" + next;
          next++;
        } while (!taken.add(MarketplaceRepository.environmentIdPrefix(id)));
        ids.add(id);
      }
    }
    return ids;
  }

  /**
   * Turn a repository the install spec declares into a marketplace repository. The URL is the
   * project's, so the configured credentials are only reused when the project points at the same
   * repository they belong to — same scheme, host and port. A project naming a host the operator
   * never configured gets what the project itself declared, or nothing.
   *
   * <p>The spec also chooses the id, and with it the {@code HOP_MARKETPLACE_<ID>_*} variables. A
   * new id may be given credentials that way; an id that is the operator's (see {@link
   * #scopedVariablesWithheld}) only gets its variables on that repository's host.
   */
  private MarketplaceRepository repositoryFromRef(
      HopInstallSpec.RepositoryRef ref, String id, boolean first) {
    MarketplaceRepository source = configuredCredentialSource(id, ref.getUrl());
    boolean specCredentials =
        StringUtils.isNotBlank(ref.getUsername()) || StringUtils.isNotBlank(ref.getPassword());
    MarketplaceRepository repo =
        new MarketplaceRepository(
            id,
            ref.getUrl(),
            StringUtils.isNotBlank(ref.getUsername())
                ? ref.getUsername()
                : (source == null ? null : source.getUsername()),
            StringUtils.isNotBlank(ref.getPassword())
                ? ref.getPassword()
                : (source == null ? null : source.getPassword()));
    if (source != null && !specCredentials) {
      // Reusing the source's credentials includes how they are sent: a token stays a bearer token.
      repo.setAuthType(source.getAuthType());
    }

    List<String> environmentIds = new ArrayList<>();
    String withheld = scopedVariablesWithheld(id, ref.getUrl());
    if (withheld == null) {
      environmentIds.add(id);
    } else if (MarketplaceRepository.hasScopedEnvironmentVariables(id)) {
      log.logBasic(
          "Install spec repository '"
              + id
              + "' at "
              + ref.getUrl()
              + " "
              + withheld
              + ": HOP_MARKETPLACE_"
              + repo.environmentIdPrefix()
              + "_* is not sent to it");
    }
    if (source == null) {
      // The global HOP_MARKETPLACE_USERNAME / _PASSWORD pair belongs to the operator's own
      // repositories for the same reason.
      repo.setGlobalEnvironmentCredentials(false);
    } else if (!environmentIds.contains(source.getId())) {
      // Same host as a configured repository: its own scoped variables apply too.
      environmentIds.add(source.getId());
    }
    repo.setEnvironmentCredentialIds(environmentIds);
    repo.setPrimary(first);
    repo.setEnabled(true);
    return repo;
  }

  /**
   * Why the {@code HOP_MARKETPLACE_<ID>_*} variables of {@code id} must not go to {@code url}, or
   * {@code null} when they may. They are withheld when the id is the operator's for another host:
   *
   * <ul>
   *   <li>a repository in hop-config with that id is on another origin (disabled ones count: their
   *       variables are still set), including the built-in {@code asf} and {@code central} when
   *       hop-config has no repositories of its own;
   *   <li>the id is the one {@code hop marketplace install --repo-url} and the Docker image use,
   *       {@code corporate-repo} or {@code HOP_PLUGINS_REPO_ID}, which is never saved to
   *       hop-config. Its variables only go to the {@code HOP_PLUGINS_MAVEN_REPO} host;
   *   <li>hop-config could not be loaded, so which ids are the operator's is unknown.
   * </ul>
   */
  private String scopedVariablesWithheld(String id, String url) {
    if (baseConfig.isDefaultsAfterLoadError()) {
      return "cannot be checked against hop-config, whose marketplace section did not load";
    }
    String prefix = MarketplaceRepository.environmentIdPrefix(id);
    boolean configured = false;
    for (MarketplaceRepository repo : nullSafe(baseConfig.getRepositories())) {
      if (repo != null && prefix.equals(repo.environmentIdPrefix())) {
        if (!repo.sameOriginAs(url)) {
          return "reuses the id of a configured repository on another host";
        }
        configured = true;
      }
    }
    if (configured || !isAdHocRepositoryId(prefix)) {
      return null;
    }
    String adHocUrl = MarketplaceRepository.environmentVariable("HOP_PLUGINS_MAVEN_REPO");
    String adHocOrigin = MarketplaceRepository.originOf(adHocUrl);
    if (adHocOrigin != null && adHocOrigin.equals(MarketplaceRepository.originOf(url))) {
      return null;
    }
    return "uses the id of the repository given with --repo-url or HOP_PLUGINS_MAVEN_REPO";
  }

  private static boolean isAdHocRepositoryId(String prefix) {
    return prefix.equals(
            MarketplaceRepository.environmentIdPrefix(
                MarketplaceCommand.DEFAULT_AD_HOC_REPOSITORY_ID))
        || prefix.equals(
            MarketplaceRepository.environmentIdPrefix(
                MarketplaceRepository.environmentVariable("HOP_PLUGINS_REPO_ID")));
  }

  /**
   * The configured repository whose credentials are reused for {@code url}, or null when no
   * repository on that origin has any. Credentials kept only in the environment count, so the check
   * resolves them, but only for repositories on that origin. Of several, the one with the spec's id
   * wins, then the first in install order. A repository with authType {@code none} has none to
   * give, which keeps the global pair off a host the operator opted out of.
   */
  private MarketplaceRepository configuredCredentialSource(String id, String url) {
    List<MarketplaceRepository> sameOrigin = new ArrayList<>();
    boolean anyCredentials = false;
    for (MarketplaceRepository repo : baseConfig.orderedRepositories()) {
      if (repo.sameOriginAs(url)) {
        sameOrigin.add(repo);
      } else {
        anyCredentials |= repo.hasCredentialsConfigured();
      }
    }
    if (sameOrigin.isEmpty()) {
      if (anyCredentials) {
        log.logBasic(
            "Install spec repository "
                + url
                + " is not a configured marketplace repository: the configured credentials are"
                + " not sent to it");
      }
      return null;
    }
    String prefix = MarketplaceRepository.environmentIdPrefix(id);
    MarketplaceRepository idMatch = null;
    for (MarketplaceRepository repo : sameOrigin) {
      if (prefix.equals(repo.environmentIdPrefix())) {
        idMatch = repo;
        break;
      }
    }
    if (idMatch != null && idMatch.hasCredentials()) {
      return idMatch;
    }
    for (MarketplaceRepository repo : sameOrigin) {
      if (repo != idMatch && repo.hasCredentials()) {
        return repo;
      }
    }
    return null;
  }

  private String resolveEnvVersion(HopInstallSpec env) {
    if (StringUtils.isNotBlank(env.getHopVersion())) {
      return env.getHopVersion();
    }
    return MarketplaceCommand.resolveDefaultVersion(baseConfig);
  }

  private boolean isPluginOnDisk(String artifactId) {
    for (OptionalPluginInfo info : OptionalPluginCatalog.listWave1()) {
      if (artifactId.equals(info.getArtifactId())) {
        return OptionalPluginCatalog.isInstalledOnDisk(hopHome, info);
      }
    }
    // fallback: any receipt counts as installed for non-catalog plugins
    try {
      InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, artifactId);
      return receipt != null && !receipt.isUninstallPending();
    } catch (HopException e) {
      return false;
    }
  }

  private static boolean anyJarPresent(Path dir, String artifactId) throws HopException {
    if (!Files.isDirectory(dir)) {
      return false;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, artifactId + "-*.jar")) {
      return stream.iterator().hasNext();
    } catch (IOException e) {
      throw new HopException("Unable to list " + dir, e);
    }
  }

  private static <T> List<T> nullSafe(List<T> list) {
    return list == null ? List.of() : list;
  }

  /** Locate hop-env.yaml/json relative to hop home / project / properties. */
  public static Path resolveEnvironmentFile(Path hopHome, String explicitPath) {
    if (StringUtils.isNotBlank(explicitPath)) {
      return Path.of(explicitPath).toAbsolutePath().normalize();
    }
    String prop = System.getProperty("hop.env.file");
    if (StringUtils.isNotBlank(prop)) {
      return Path.of(prop).toAbsolutePath().normalize();
    }
    String env = System.getenv("HOP_ENV_FILE");
    if (StringUtils.isNotBlank(env)) {
      return Path.of(env).toAbsolutePath().normalize();
    }
    String projectHome = System.getProperty("PROJECT_HOME");
    if (StringUtils.isBlank(projectHome)) {
      projectHome = System.getenv("PROJECT_HOME");
    }
    if (StringUtils.isNotBlank(projectHome)) {
      Path p = Path.of(projectHome);
      for (String name : List.of("hop-env.yaml", "hop-env.yml", "hop-env.json")) {
        Path candidate = p.resolve(name);
        if (Files.isRegularFile(candidate)) {
          return candidate;
        }
      }
    }
    if (hopHome != null) {
      for (String name : List.of("hop-env.yaml", "hop-env.yml", "hop-env.json")) {
        Path candidate = hopHome.resolve(name);
        if (Files.isRegularFile(candidate)) {
          return candidate;
        }
      }
    }
    return null;
  }
}
