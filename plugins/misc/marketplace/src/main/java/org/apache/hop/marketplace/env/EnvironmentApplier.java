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
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.marketplace.install.PluginUninstaller;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.apache.hop.marketplace.resolve.MavenRepositoryClient;

/** Applies or validates a {@link HopEnvironmentSpec} against a Hop installation. */
public class EnvironmentApplier {

  private final ILogChannel log;
  private final Path hopHome;
  private final MarketplaceConfig baseConfig;

  public EnvironmentApplier(ILogChannel log, Path hopHome, MarketplaceConfig baseConfig) {
    this.log = log;
    this.hopHome = hopHome;
    this.baseConfig = baseConfig;
  }

  public EnvironmentDrift validate(HopEnvironmentSpec env) throws HopException {
    EnvironmentDrift drift = new EnvironmentDrift();
    String defaultVersion = resolveEnvVersion(env);

    for (HopEnvironmentSpec.PluginRef ref : nullSafe(env.getPlugins())) {
      if (StringUtils.isBlank(ref.getArtifactId())) {
        continue;
      }
      String groupId =
          StringUtils.isNotBlank(ref.getGroupId()) ? ref.getGroupId() : baseConfig.getGroupId();
      String version = StringUtils.isNotBlank(ref.getVersion()) ? ref.getVersion() : defaultVersion;
      InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, ref.getArtifactId());
      boolean onDisk = isPluginOnDisk(ref.getArtifactId());
      if (!onDisk && receipt == null) {
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

    for (HopEnvironmentSpec.DependencyRef dep : nullSafe(env.getDependencies())) {
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
   * Install missing plugins/deps; optionally prune marketplace plugins not listed in the env file.
   */
  public void apply(HopEnvironmentSpec env, boolean prune) throws HopException {
    MarketplaceConfig config = configFromEnv(env);
    String defaultVersion = resolveEnvVersion(env);
    PluginInstaller installer = new PluginInstaller(log, hopHome, config);
    installer.activateAllPending();

    Set<String> desiredArtifacts = new HashSet<>();
    for (HopEnvironmentSpec.PluginRef ref : nullSafe(env.getPlugins())) {
      if (StringUtils.isBlank(ref.getArtifactId())) {
        continue;
      }
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
      boolean needsInstall = (!onDisk && receipt == null) || versionMismatch;
      if (needsInstall) {
        MavenCoordinates coords = new MavenCoordinates(groupId, ref.getArtifactId(), version);
        log.logBasic("Applying environment: installing " + coords.gav());
        installer.install(coords, true);
      } else {
        log.logBasic("Applying environment: " + ref.getArtifactId() + " already satisfied");
      }
    }

    for (HopEnvironmentSpec.DependencyRef dep : nullSafe(env.getDependencies())) {
      if (StringUtils.isAnyBlank(dep.getGroupId(), dep.getArtifactId(), dep.getVersion())) {
        continue;
      }
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
      try {
        Files.createDirectories(dir);
        new MavenRepositoryClient(log)
            .downloadArtifact(config.primaryRepository(), relativePath, coords.gav(), jar);
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
          log.logBasic("Pruning marketplace plugin not in env file: " + artifactId);
          uninstaller.uninstall(artifactId);
        }
      }
    } catch (IOException e) {
      throw new HopException("Failed to scan marketplace receipts for prune", e);
    }
  }

  private MarketplaceConfig configFromEnv(HopEnvironmentSpec env) {
    MarketplaceConfig config = new MarketplaceConfig();
    config.setEnabled(baseConfig.isEnabled());
    config.setGroupId(baseConfig.getGroupId());
    config.setDefaultVersion(
        StringUtils.isNotBlank(env.getHopVersion())
            ? env.getHopVersion()
            : MarketplaceCommand.resolveDefaultVersion(baseConfig));
    config.getRepositories().clear();
    MarketplaceRepository baseRepo = baseConfig.primaryRepository();
    if (env.getRepositories() != null && !env.getRepositories().isEmpty()) {
      for (HopEnvironmentSpec.RepositoryRef ref : env.getRepositories()) {
        if (StringUtils.isNotBlank(ref.getUrl())) {
          MarketplaceRepository repo =
              new MarketplaceRepository(
                  StringUtils.defaultIfBlank(ref.getId(), "env"),
                  ref.getUrl(),
                  StringUtils.isNotBlank(ref.getUsername())
                      ? ref.getUsername()
                      : baseRepo.getUsername(),
                  StringUtils.isNotBlank(ref.getPassword())
                      ? ref.getPassword()
                      : baseRepo.getPassword());
          config.getRepositories().add(repo);
        }
      }
    }
    if (config.getRepositories().isEmpty()) {
      config.getRepositories().addAll(baseConfig.getRepositories());
    }
    if (config.getRepositories().isEmpty()) {
      config
          .getRepositories()
          .add(new MarketplaceRepository("central", MarketplaceConfig.DEFAULT_REPO_URL));
    }
    return config;
  }

  private String resolveEnvVersion(HopEnvironmentSpec env) {
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
      return PluginInstaller.readReceipt(hopHome, artifactId) != null;
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
