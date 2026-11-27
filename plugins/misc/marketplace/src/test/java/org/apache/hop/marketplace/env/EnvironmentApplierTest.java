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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceHttp;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EnvironmentApplierTest {

  @AfterEach
  void restoreEnvironment() {
    MarketplaceRepository.setEnvironmentForTesting(null);
  }

  @TempDir Path tempDir;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @Test
  void validateDetectsMissingPlugin() throws Exception {
    Path hopHome = tempDir.resolve("hop");
    Files.createDirectories(hopHome.resolve("plugins"));

    HopInstallSpec env = new HopInstallSpec();
    env.setHopVersion("2.19.0");
    HopInstallSpec.PluginRef ref = new HopInstallSpec.PluginRef();
    ref.setArtifactId("hop-tech-parquet");
    ref.setVersion("2.19.0");
    env.getPlugins().add(ref);

    EnvironmentDrift drift =
        new EnvironmentApplier(new LogChannel("test"), hopHome, new MarketplaceConfig())
            .validate(env);
    assertTrue(drift.hasDrift());
    assertEquals(1, drift.getMissingPlugins().size());
  }

  @Test
  void validateOkWhenPluginOnDisk() throws Exception {
    Path hopHome = tempDir.resolve("hop");
    Files.createDirectories(hopHome.resolve("plugins/tech/parquet"));

    HopInstallSpec env = new HopInstallSpec();
    HopInstallSpec.PluginRef ref = new HopInstallSpec.PluginRef();
    ref.setArtifactId("hop-tech-parquet");
    env.getPlugins().add(ref);

    EnvironmentDrift drift =
        new EnvironmentApplier(new LogChannel("test"), hopHome, new MarketplaceConfig())
            .validate(env);
    assertFalse(drift.hasDrift());
  }

  @Test
  void validateVersionMismatchFromReceipt() throws Exception {
    Path hopHome = tempDir.resolve("hop");
    Files.createDirectories(hopHome.resolve("plugins/tech/parquet"));
    Files.createDirectories(hopHome.resolve(PluginInstaller.RECEIPTS_DIR));
    InstallReceipt receipt = new InstallReceipt();
    receipt.setGroupId("org.apache.hop");
    receipt.setArtifactId("hop-tech-parquet");
    receipt.setVersion("2.18.0");
    HopJson.newMapper()
        .writeValue(
            hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-tech-parquet.json").toFile(),
            receipt);

    HopInstallSpec env = new HopInstallSpec();
    HopInstallSpec.PluginRef ref = new HopInstallSpec.PluginRef();
    ref.setArtifactId("hop-tech-parquet");
    ref.setVersion("2.19.0");
    env.getPlugins().add(ref);

    EnvironmentDrift drift =
        new EnvironmentApplier(new LogChannel("test"), hopHome, new MarketplaceConfig())
            .validate(env);
    assertEquals(1, drift.getVersionMismatches().size());
  }

  @Test
  void resolveEnvironmentFileDiscoversProjectFile() throws Exception {
    Path project = tempDir.resolve("project");
    Files.createDirectories(project);
    Path envFile = project.resolve("hop-env.yaml");
    Files.writeString(envFile, "version: \"1.0\"\n");
    System.setProperty("PROJECT_HOME", project.toString());
    try {
      Path found = EnvironmentApplier.resolveEnvironmentFile(tempDir, null);
      assertEquals(envFile.toAbsolutePath().normalize(), found);
    } finally {
      System.clearProperty("PROJECT_HOME");
    }
  }

  private static MarketplaceConfig configuredWithPrivateRepository() {
    MarketplaceConfig config = new MarketplaceConfig();
    config.getRepositories().clear();
    MarketplaceRepository repo =
        new MarketplaceRepository(
            "nexus", "https://nexus.example.org/repository/hop/", "operator", "operator-secret");
    repo.setPrimary(true);
    config.getRepositories().add(repo);
    return config;
  }

  private static HopInstallSpec specWithRepository(String id, String url) {
    HopInstallSpec env = new HopInstallSpec();
    HopInstallSpec.RepositoryRef ref = new HopInstallSpec.RepositoryRef();
    ref.setId(id);
    ref.setUrl(url);
    env.getRepositories().add(ref);
    return env;
  }

  @Test
  void projectRepositoryOnAnotherHostDoesNotGetTheConfiguredCredentials() {
    MarketplaceConfig config =
        new EnvironmentApplier(new LogChannel("test"), tempDir, configuredWithPrivateRepository())
            .configFromEnv(
                specWithRepository("project", "https://evil.example.com/repository/hop/"));

    MarketplaceRepository applied = config.primaryRepository();
    assertEquals("project", applied.getId());
    assertNull(applied.getUsername());
    assertNull(applied.getPassword());
    assertFalse(applied.isGlobalEnvironmentCredentials());
  }

  @Test
  void projectRepositoryOnTheSameHostStillInheritsThem() {
    MarketplaceConfig config =
        new EnvironmentApplier(new LogChannel("test"), tempDir, configuredWithPrivateRepository())
            .configFromEnv(
                specWithRepository("project", "https://nexus.example.org/repository/hop-extra/"));

    MarketplaceRepository applied = config.primaryRepository();
    assertEquals("operator", applied.getUsername());
    assertEquals("operator-secret", applied.getPassword());
    assertTrue(applied.isGlobalEnvironmentCredentials());
  }

  @Test
  void aDifferentPortOrSchemeIsADifferentRepository() {
    for (String url :
        new String[] {
          "https://nexus.example.org:8443/repository/hop/",
          "http://nexus.example.org/repository/hop/"
        }) {
      MarketplaceConfig config =
          new EnvironmentApplier(new LogChannel("test"), tempDir, configuredWithPrivateRepository())
              .configFromEnv(specWithRepository("project", url));
      assertNull(config.primaryRepository().getUsername(), url);
      assertNull(config.primaryRepository().getPassword(), url);
    }
  }

  @Test
  void credentialsDeclaredByTheProjectAreAlwaysUsed() {
    HopInstallSpec env = specWithRepository("project", "https://other.example.com/repository/hop/");
    env.getRepositories().get(0).setUsername("project-user");
    env.getRepositories().get(0).setPassword("project-secret");

    MarketplaceRepository applied =
        new EnvironmentApplier(new LogChannel("test"), tempDir, configuredWithPrivateRepository())
            .configFromEnv(env)
            .primaryRepository();
    assertEquals("project-user", applied.getUsername());
    assertEquals("project-secret", applied.getPassword());
  }

  @Test
  void credentialsComeFromTheMatchingRepositoryNotOnlyFromThePrimary() {
    MarketplaceConfig config = new MarketplaceConfig();
    config.getRepositories().clear();
    MarketplaceRepository primary =
        new MarketplaceRepository("asf", "https://repository.apache.org/content/groups/public/");
    primary.setPrimary(true);
    config.getRepositories().add(primary);
    config
        .getRepositories()
        .add(
            new MarketplaceRepository(
                "acme", "https://nexus.example.org/repository/hop/", "acme-user", "acme-secret"));

    MarketplaceRepository applied =
        new EnvironmentApplier(new LogChannel("test"), tempDir, config)
            .configFromEnv(
                specWithRepository("project", "https://nexus.example.org/repository/hop-extra/"))
            .primaryRepository();
    assertEquals("acme-user", applied.getUsername());
    assertEquals("acme-secret", applied.getPassword());
  }

  /** hop-config with one private repository whose secrets are only in the environment. */
  private static MarketplaceConfig configuredWithEnvironmentCredentials(Map<String, String> env) {
    MarketplaceRepository.setEnvironmentForTesting(env::get);
    MarketplaceConfig config = new MarketplaceConfig();
    config.getRepositories().clear();
    MarketplaceRepository repo =
        new MarketplaceRepository("nexus", "https://nexus.example.org/repository/hop/");
    repo.setPrimary(true);
    config.getRepositories().add(repo);
    return config;
  }

  @Test
  void aConfiguredIdOnAnotherHostDoesNotGetItsScopedVariables() {
    MarketplaceConfig base =
        configuredWithEnvironmentCredentials(
            Map.of(
                "HOP_MARKETPLACE_NEXUS_USERNAME", "nexus-user",
                "HOP_MARKETPLACE_NEXUS_PASSWORD", "nexus-secret"));

    for (String id : new String[] {"nexus", "Nexus", "NEXUS"}) {
      MarketplaceRepository applied =
          new EnvironmentApplier(new LogChannel("test"), tempDir, base)
              .configFromEnv(specWithRepository(id, "https://evil.example.com/repository/hop/"))
              .primaryRepository();
      assertNull(applied.effectiveUsername(), id);
      assertNull(applied.effectivePassword(), id);
      assertFalse(applied.hasCredentials(), id);
    }
  }

  @Test
  void aNewSpecIdCanStillBeGivenScopedVariables() {
    MarketplaceConfig base =
        configuredWithEnvironmentCredentials(
            Map.of(
                "HOP_MARKETPLACE_PROJECT_USERNAME", "project-user",
                "HOP_MARKETPLACE_PROJECT_PASSWORD", "project-secret",
                "HOP_MARKETPLACE_NEXUS_USERNAME", "nexus-user",
                "HOP_MARKETPLACE_NEXUS_PASSWORD", "nexus-secret"));

    MarketplaceRepository applied =
        new EnvironmentApplier(new LogChannel("test"), tempDir, base)
            .configFromEnv(
                specWithRepository("project", "https://other.example.com/repository/hop/"))
            .primaryRepository();
    assertEquals("project-user", applied.effectiveUsername());
    assertEquals("project-secret", applied.effectivePassword());
  }

  @Test
  void theConfiguredIdOnItsOwnHostKeepsItsScopedVariables() {
    MarketplaceConfig base =
        configuredWithEnvironmentCredentials(
            Map.of(
                "HOP_MARKETPLACE_NEXUS_USERNAME", "nexus-user",
                "HOP_MARKETPLACE_NEXUS_PASSWORD", "nexus-secret"));

    MarketplaceRepository applied =
        new EnvironmentApplier(new LogChannel("test"), tempDir, base)
            .configFromEnv(
                specWithRepository("nexus", "https://nexus.example.org/repository/hop-extra/"))
            .primaryRepository();
    assertEquals("nexus-user", applied.effectiveUsername());
    assertEquals("nexus-secret", applied.effectivePassword());
  }

  @Test
  void sameHostWithCredentialsOnlyInTheEnvironmentStillGetsThem() {
    // Global pair: the spec repository counts as the operator's own, so the pair still applies.
    MarketplaceRepository global =
        new EnvironmentApplier(
                new LogChannel("test"),
                tempDir,
                configuredWithEnvironmentCredentials(
                    Map.of(
                        "HOP_MARKETPLACE_USERNAME", "global-user",
                        "HOP_MARKETPLACE_PASSWORD", "global-secret")))
            .configFromEnv(
                specWithRepository("project", "https://nexus.example.org/repository/hop-extra/"))
            .primaryRepository();
    assertTrue(global.isGlobalEnvironmentCredentials());
    assertEquals("global-user", global.effectiveUsername());
    assertEquals("global-secret", global.effectivePassword());

    // Scoped to the configured repository: a new id on its host inherits them.
    MarketplaceRepository scoped =
        new EnvironmentApplier(
                new LogChannel("test"),
                tempDir,
                configuredWithEnvironmentCredentials(
                    Map.of(
                        "HOP_MARKETPLACE_NEXUS_USERNAME", "nexus-user",
                        "HOP_MARKETPLACE_NEXUS_PASSWORD", "nexus-secret")))
            .configFromEnv(
                specWithRepository("project", "https://nexus.example.org/repository/hop-extra/"))
            .primaryRepository();
    assertEquals("nexus-user", scoped.effectiveUsername());
    assertEquals("nexus-secret", scoped.effectivePassword());
  }

  @Test
  void anotherConfiguredRepositoryOnTheSpecHostDecidesInstead() {
    // The spec reuses the id "nexus" on the host of the configured "corp" repository: corp's
    // variables apply there, nexus's do not.
    MarketplaceConfig base =
        configuredWithEnvironmentCredentials(
            Map.of(
                "HOP_MARKETPLACE_NEXUS_USERNAME", "nexus-user",
                "HOP_MARKETPLACE_NEXUS_PASSWORD", "nexus-secret",
                "HOP_MARKETPLACE_CORP_USERNAME", "corp-user",
                "HOP_MARKETPLACE_CORP_PASSWORD", "corp-secret"));
    base.getRepositories()
        .add(new MarketplaceRepository("corp", "https://corp.example.com/maven/"));

    MarketplaceRepository applied =
        new EnvironmentApplier(new LogChannel("test"), tempDir, base)
            .configFromEnv(specWithRepository("nexus", "https://corp.example.com/maven/hop/"))
            .primaryRepository();
    assertEquals("corp-user", applied.effectiveUsername());
    assertEquals("corp-secret", applied.effectivePassword());
  }

  @Test
  void anIdMatchWithoutCredentialsDoesNotWinOverOneWithThem() {
    MarketplaceConfig base = new MarketplaceConfig();
    base.getRepositories().clear();
    MarketplaceRepository anonymous =
        new MarketplaceRepository("nexus", "https://nexus.corp/repository/maven-public/");
    anonymous.setPrimary(true);
    base.getRepositories().add(anonymous);
    base.getRepositories()
        .add(
            new MarketplaceRepository(
                "nexus-private", "https://nexus.corp/repository/hop-private/", "corp", "secret"));

    MarketplaceRepository applied =
        new EnvironmentApplier(new LogChannel("test"), tempDir, base)
            .configFromEnv(
                specWithRepository("nexus", "https://nexus.corp/repository/hop-private/"))
            .primaryRepository();
    assertEquals("corp", applied.effectiveUsername());
    assertEquals("secret", applied.effectivePassword());
  }

  @Test
  void aSourceWithAuthTypeNoneIsNoSource() {
    MarketplaceRepository.setEnvironmentForTesting(
        Map.of("HOP_MARKETPLACE_USERNAME", "op-user", "HOP_MARKETPLACE_PASSWORD", "op-pass")::get);
    MarketplaceConfig base = new MarketplaceConfig();
    base.getRepositories().clear();
    MarketplaceRepository nexus = new MarketplaceRepository("nexus", "https://nexus.corp/hop/");
    nexus.setPrimary(true);
    MarketplaceRepository central =
        new MarketplaceRepository("central", MarketplaceConfig.DEFAULT_CENTRAL_URL);
    central.setAuthType(MarketplaceRepository.AUTH_NONE);
    base.getRepositories().add(nexus);
    base.getRepositories().add(central);

    MarketplaceRepository applied =
        new EnvironmentApplier(new LogChannel("test"), tempDir, base)
            .configFromEnv(specWithRepository("mirror", MarketplaceConfig.DEFAULT_CENTRAL_URL))
            .primaryRepository();
    assertFalse(applied.isGlobalEnvironmentCredentials());
    assertFalse(applied.hasCredentials());
  }

  @Test
  void theSourcesAuthTypeIsKeptOnlyWhenItsCredentialsAreUsed() {
    MarketplaceRepository.setEnvironmentForTesting(
        Map.of("HOP_MARKETPLACE_ARTIFACTORY_TOKEN", "the-token")::get);
    MarketplaceConfig base = new MarketplaceConfig();
    base.getRepositories().clear();
    MarketplaceRepository artifactory =
        new MarketplaceRepository("artifactory", "https://acme.jfrog.io/artifactory/hop/");
    artifactory.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    artifactory.setPrimary(true);
    base.getRepositories().add(artifactory);
    EnvironmentApplier applier = new EnvironmentApplier(new LogChannel("test"), tempDir, base);

    MarketplaceRepository reused =
        applier
            .configFromEnv(
                specWithRepository("project", "https://acme.jfrog.io/artifactory/hop-extra/"))
            .primaryRepository();
    assertEquals(MarketplaceRepository.AUTH_TOKEN, reused.effectiveAuthType());
    assertEquals("the-token", reused.effectiveToken());

    HopInstallSpec own = specWithRepository("project", "https://acme.jfrog.io/artifactory/x/");
    own.getRepositories().get(0).setUsername("project-user");
    own.getRepositories().get(0).setPassword("project-secret");
    MarketplaceRepository declared = applier.configFromEnv(own).primaryRepository();
    assertEquals(MarketplaceRepository.AUTH_BASIC, declared.effectiveAuthType());
  }

  @Test
  void theAdHocRepositoryIdIsReservedForItsOwnHost() {
    Map<String, String> env =
        Map.of(
            "HOP_PLUGINS_REPO_ID", "acme-mirror",
            "HOP_PLUGINS_MAVEN_REPO", "https://nexus.corp/repository/hop/",
            "HOP_MARKETPLACE_CORPORATE_REPO_PASSWORD", "corp-secret",
            "HOP_MARKETPLACE_CORPORATE_REPO_USERNAME", "corp-user",
            "HOP_MARKETPLACE_ACME_MIRROR_USERNAME", "acme-user",
            "HOP_MARKETPLACE_ACME_MIRROR_PASSWORD", "acme-secret");
    MarketplaceRepository.setEnvironmentForTesting(env::get);
    EnvironmentApplier applier =
        new EnvironmentApplier(new LogChannel("test"), tempDir, new MarketplaceConfig());

    for (String id : new String[] {"corporate-repo", "acme-mirror"}) {
      MarketplaceRepository elsewhere =
          applier
              .configFromEnv(specWithRepository(id, "https://evil.example/repository/hop/"))
              .primaryRepository();
      assertFalse(elsewhere.hasCredentials(), id);

      MarketplaceRepository home =
          applier
              .configFromEnv(specWithRepository(id, "https://nexus.corp/repository/hop-extra/"))
              .primaryRepository();
      assertTrue(home.hasCredentials(), id);
    }
  }

  @Test
  void anUnloadableHopConfigGivesSpecIdsNoScopedVariables() {
    MarketplaceRepository.setEnvironmentForTesting(
        Map.of(
                "HOP_MARKETPLACE_PROJECT_USERNAME", "project-user",
                "HOP_MARKETPLACE_PROJECT_PASSWORD", "project-secret")
            ::get);
    MarketplaceConfig failed = new MarketplaceConfig();
    failed.setDefaultsAfterLoadError(true);

    MarketplaceRepository applied =
        new EnvironmentApplier(new LogChannel("test"), tempDir, failed)
            .configFromEnv(specWithRepository("project", "https://other.example.com/hop/"))
            .primaryRepository();
    assertFalse(applied.hasCredentials());
  }

  @Test
  void specRepositoriesWithoutIdsKeepEveryFallback() {
    HopInstallSpec spec = specWithRepository(null, "https://mirror.example.com/maven/");
    HopInstallSpec.RepositoryRef fallback = new HopInstallSpec.RepositoryRef();
    fallback.setUrl("https://fallback.example.com/maven/");
    spec.getRepositories().add(fallback);
    HopInstallSpec.RepositoryRef third = new HopInstallSpec.RepositoryRef();
    third.setUrl("https://third.example.com/maven/");
    spec.getRepositories().add(third);
    spec.getRepositories().get(0).setId(null);
    // An explicit id that the generated ones must step around.
    HopInstallSpec.RepositoryRef named = new HopInstallSpec.RepositoryRef();
    named.setId("spec-2");
    named.setUrl("https://named.example.com/maven/");
    spec.getRepositories().add(named);

    List<MarketplaceRepository> ordered =
        new EnvironmentApplier(new LogChannel("test"), tempDir, new MarketplaceConfig())
            .configFromEnv(spec)
            .orderedRepositories();
    assertEquals(
        List.of("spec", "spec-3", "spec-4", "spec-2"),
        ordered.stream().map(MarketplaceRepository::getId).toList());
  }

  @Test
  void theAuthHintNamesTheVariablesTheRepositoryReads() {
    MarketplaceConfig base =
        configuredWithEnvironmentCredentials(
            Map.of(
                "HOP_MARKETPLACE_NEXUS_USERNAME", "nexus-user",
                "HOP_MARKETPLACE_NEXUS_PASSWORD", "nexus-secret"));
    EnvironmentApplier applier = new EnvironmentApplier(new LogChannel("test"), tempDir, base);

    String reserved =
        MarketplaceHttp.authHint(
            401,
            applier
                .configFromEnv(specWithRepository("nexus", "https://other.example/hop/"))
                .primaryRepository());
    assertFalse(reserved.contains("HOP_MARKETPLACE_NEXUS_USERNAME"), reserved);
    assertFalse(reserved.contains("HOP_MARKETPLACE_USERNAME"), reserved);
    assertTrue(reserved.contains("repository entry in the install spec"), reserved);

    String ownId =
        MarketplaceHttp.authHint(
            401,
            applier
                .configFromEnv(specWithRepository("project", "https://other.example/hop/"))
                .primaryRepository());
    assertTrue(ownId.contains("HOP_MARKETPLACE_PROJECT_USERNAME"), ownId);
    assertFalse(ownId.contains("HOP_MARKETPLACE_USERNAME"), ownId);
  }

  @Test
  void specWithoutRepositoriesKeepsTheConfiguredOnes() {
    MarketplaceConfig config =
        new EnvironmentApplier(new LogChannel("test"), tempDir, configuredWithPrivateRepository())
            .configFromEnv(new HopInstallSpec());
    assertEquals("operator", config.primaryRepository().getUsername());
  }

  @Test
  void applyWithAChangedVersionRemovesThePreviousVersionsFiles() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    serveZip(server, "1.0.0", "plugins/tech/test/lib/dep-1.0.jar");
    serveZip(server, "2.0.0", "plugins/tech/test/lib/dep-2.0.jar");
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-apply");
      Files.createDirectories(hopHome.resolve("plugins"));
      MarketplaceConfig config = new MarketplaceConfig();
      config.getRepositories().clear();
      config
          .getRepositories()
          .add(
              new MarketplaceRepository(
                  "local", "http://127.0.0.1:" + server.getAddress().getPort() + "/", true));
      EnvironmentApplier applier = new EnvironmentApplier(new LogChannel("test"), hopHome, config);

      applier.apply(specWithPlugin("1.0.0"), false);
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/lib/dep-1.0.jar")));

      applier.apply(specWithPlugin("2.0.0"), false);
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/lib/dep-2.0.jar")));
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test/lib/dep-1.0.jar")));
      assertEquals("2.0.0", PluginInstaller.readReceipt(hopHome, "hop-test-plugin").getVersion());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void applyInstallsAPluginWhoseUninstallIsUnfinished() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    serveZip(server, "2.0.0", "plugins/tech/test/lib/dep-2.0.jar");
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-apply-after-uninstall");
      // What an uninstall leaves when a file of the previous version was in use.
      Files.createDirectories(hopHome.resolve("plugins/tech/test/lib"));
      Files.writeString(hopHome.resolve("plugins/tech/test/lib/dep-1.0.jar"), "1.0.0");
      Files.createDirectories(hopHome.resolve(PluginInstaller.RECEIPTS_DIR));
      Files.writeString(
          hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-test-plugin.json"),
          "{\"artifactId\":\"hop-test-plugin\",\"version\":\"2.0.0\","
              + "\"uninstallPending\":true,\"paths\":[\"plugins/tech/test\"],"
              + "\"obsoletePaths\":[\"plugins/tech/test/lib/dep-1.0.jar\"]}");
      MarketplaceConfig config = new MarketplaceConfig();
      config.getRepositories().clear();
      config
          .getRepositories()
          .add(
              new MarketplaceRepository(
                  "local", "http://127.0.0.1:" + server.getAddress().getPort() + "/", true));
      EnvironmentApplier applier = new EnvironmentApplier(new LogChannel("test"), hopHome, config);

      assertEquals(1, applier.validate(specWithPlugin("2.0.0")).getMissingPlugins().size());

      applier.apply(specWithPlugin("2.0.0"), false);
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/lib/dep-2.0.jar")));
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test/lib/dep-1.0.jar")));
      InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, "hop-test-plugin");
      assertEquals("2.0.0", receipt.getVersion());
      assertFalse(receipt.isUninstallPending());
    } finally {
      server.stop(0);
    }
  }

  private static HopInstallSpec specWithPlugin(String version) {
    HopInstallSpec env = new HopInstallSpec();
    HopInstallSpec.PluginRef ref = new HopInstallSpec.PluginRef();
    ref.setGroupId("org.apache.hop");
    ref.setArtifactId("hop-test-plugin");
    ref.setVersion(version);
    env.getPlugins().add(ref);
    return env;
  }

  /** Serves a plugin zip with plugin.jar and one versioned dependency jar. */
  private static void serveZip(HttpServer server, String version, String dependencyJar)
      throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(bos)) {
      for (String entry : new String[] {"plugins/tech/test/plugin.jar", dependencyJar}) {
        zos.putNextEntry(new ZipEntry(entry));
        zos.write(version.getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
      }
    }
    byte[] zipBytes = bos.toByteArray();
    server.createContext(
        "/org/apache/hop/hop-test-plugin/" + version + "/hop-test-plugin-" + version + ".zip",
        exchange -> {
          exchange.getResponseHeaders().add("Content-Type", "application/zip");
          exchange.sendResponseHeaders(200, zipBytes.length);
          exchange.getResponseBody().write(zipBytes);
          exchange.close();
        });
  }
}
