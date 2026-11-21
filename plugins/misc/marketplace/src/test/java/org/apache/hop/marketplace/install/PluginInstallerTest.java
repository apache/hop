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

package org.apache.hop.marketplace.install;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.marketplace.command.MarketplaceCommand;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class PluginInstallerTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @Test
  void sharedCorePaths() {
    assertFalse(PluginInstaller.isSharedCorePath("lib/beam/foo.jar"));
    assertFalse(PluginInstaller.isSharedCorePath("plugins/engines/beam/hop-engines-beam.jar"));
    assertTrue(PluginInstaller.isSharedCorePath("lib/core/bar.jar"));
    assertTrue(PluginInstaller.isSharedCorePath("lib/core"));
    assertFalse(PluginInstaller.isSharedCorePath("plugins/engines/beam/hop.jar"));
    assertFalse(PluginInstaller.isSharedCorePath("plugins/tech/parquet/lib/x.jar"));
  }

  @Test
  void skipSharedCoreCopyRules() {
    // Missing target: always install (covers Azure / first-time shared deps, #7666)
    assertFalse(PluginInstaller.skipSharedCoreCopy(false, null, false));
    assertFalse(PluginInstaller.skipSharedCoreCopy(false, null, true));

    // Identical content: always skip (Windows classpath locks + unnecessary I/O)
    assertTrue(PluginInstaller.skipSharedCoreCopy(true, -1L, false));
    assertTrue(PluginInstaller.skipSharedCoreCopy(true, -1L, true));

    // Different content: skip only on Windows
    assertFalse(PluginInstaller.skipSharedCoreCopy(true, 0L, false));
    assertTrue(PluginInstaller.skipSharedCoreCopy(true, 0L, true));
  }

  @Test
  void installAndUninstallFromLocalHttpRepo() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    String path = "/org/apache/hop/hop-test-plugin/1.0.0/hop-test-plugin-1.0.0.zip";
    server.createContext(
        path,
        exchange -> {
          exchange.getResponseHeaders().add("Content-Type", "application/zip");
          exchange.sendResponseHeaders(200, zipBytes.length);
          exchange.getResponseBody().write(zipBytes);
          exchange.close();
        });
    server.start();
    try {
      int port = server.getAddress().getPort();
      Path hopHome = tempDir.resolve("hop");
      Files.createDirectories(hopHome.resolve("plugins"));

      MarketplaceConfig config = new MarketplaceConfig();
      config.getRepositories().clear();
      MarketplaceRepository local =
          new MarketplaceRepository("local", "http://127.0.0.1:" + port + "/", true);
      config.getRepositories().add(local);

      LogChannel log = new LogChannel("test");
      PluginInstaller installer = new PluginInstaller(log, hopHome, config);
      MavenCoordinates coords = new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0");
      InstallReceipt receipt = installer.install(coords, true);
      assertEquals("local", receipt.getRepositoryId());

      Path pluginJar = hopHome.resolve("plugins/tech/test/plugin.jar");
      assertTrue(Files.isRegularFile(pluginJar));
      assertTrue(
          Files.isRegularFile(
              hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-test-plugin.json")));
      // provided-scope jars land under lib/core and must be installed (system classpath)
      Path sharedJar = hopHome.resolve("lib/core/shared.jar");
      assertTrue(Files.isRegularFile(sharedJar));
      assertEquals("shared-lib", Files.readString(sharedJar));

      new PluginUninstaller(log, hopHome).uninstall("hop-test-plugin");
      assertFalse(Files.exists(pluginJar));
      assertFalse(
          Files.exists(
              hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-test-plugin.json")));
      // lib/core is sticky: left in place so other plugins can share it
      assertTrue(Files.isRegularFile(sharedJar));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void installSkipsIdenticalPreexistingSharedCoreJar() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    String path = "/org/apache/hop/hop-test-plugin/1.0.0/hop-test-plugin-1.0.0.zip";
    server.createContext(
        path,
        exchange -> {
          exchange.getResponseHeaders().add("Content-Type", "application/zip");
          exchange.sendResponseHeaders(200, zipBytes.length);
          exchange.getResponseBody().write(zipBytes);
          exchange.close();
        });
    server.start();
    try {
      int port = server.getAddress().getPort();
      Path hopHome = tempDir.resolve("hop-identical-core");
      Files.createDirectories(hopHome.resolve("plugins"));
      // Simulate slim client already shipping the same shared jar (e.g. avro on Windows)
      Path sharedJar = hopHome.resolve("lib/core/shared.jar");
      Files.createDirectories(sharedJar.getParent());
      Files.writeString(sharedJar, "shared-lib");

      MarketplaceConfig config = new MarketplaceConfig();
      config.getRepositories().clear();
      MarketplaceRepository local =
          new MarketplaceRepository("local", "http://127.0.0.1:" + port + "/", true);
      config.getRepositories().add(local);

      new PluginInstaller(new LogChannel("test"), hopHome, config)
          .install(new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0"), true);

      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
      assertEquals("shared-lib", Files.readString(sharedJar));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void installHandlesPreexistingDifferentSharedCoreJar() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    String path = "/org/apache/hop/hop-test-plugin/1.0.0/hop-test-plugin-1.0.0.zip";
    server.createContext(
        path,
        exchange -> {
          exchange.getResponseHeaders().add("Content-Type", "application/zip");
          exchange.sendResponseHeaders(200, zipBytes.length);
          exchange.getResponseBody().write(zipBytes);
          exchange.close();
        });
    server.start();
    try {
      int port = server.getAddress().getPort();
      Path hopHome = tempDir.resolve("hop-different-core");
      Files.createDirectories(hopHome.resolve("plugins"));
      Path sharedJar = hopHome.resolve("lib/core/shared.jar");
      Files.createDirectories(sharedJar.getParent());
      Files.writeString(sharedJar, "old-shared-lib");

      MarketplaceConfig config = new MarketplaceConfig();
      config.getRepositories().clear();
      MarketplaceRepository local =
          new MarketplaceRepository("local", "http://127.0.0.1:" + port + "/", true);
      config.getRepositories().add(local);

      new PluginInstaller(new LogChannel("test"), hopHome, config)
          .install(new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0"), true);

      // Plugin tree always activates; shared core replace depends on OS (#7717).
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
      if (org.apache.hop.core.Const.isWindows()) {
        assertEquals("old-shared-lib", Files.readString(sharedJar));
      } else {
        assertEquals("shared-lib", Files.readString(sharedJar));
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void installWithBasicAuth() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    String path = "/org/apache/hop/hop-test-plugin/1.0.0/hop-test-plugin-1.0.0.zip";
    server.createContext(
        path,
        exchange -> {
          String auth = exchange.getRequestHeaders().getFirst("Authorization");
          if (auth == null || !auth.startsWith("Basic ")) {
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
            return;
          }
          String decoded =
              new String(
                  java.util.Base64.getDecoder().decode(auth.substring("Basic ".length())),
                  StandardCharsets.UTF_8);
          if (!"admin:s3cret".equals(decoded)) {
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
            return;
          }
          exchange.getResponseHeaders().add("Content-Type", "application/zip");
          exchange.sendResponseHeaders(200, zipBytes.length);
          exchange.getResponseBody().write(zipBytes);
          exchange.close();
        });
    server.start();
    try {
      int port = server.getAddress().getPort();
      Path hopHome = tempDir.resolve("hop-auth");
      Files.createDirectories(hopHome.resolve("plugins"));

      MarketplaceConfig config = new MarketplaceConfig();
      config.getRepositories().clear();
      MarketplaceRepository local =
          new MarketplaceRepository("local", "http://127.0.0.1:" + port + "/", "admin", "s3cret");
      local.setPrimary(true);
      config.getRepositories().add(local);

      new PluginInstaller(new LogChannel("test"), hopHome, config)
          .install(new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0"), true);
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void installFallsBackToSecondRepository() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    String path = "/org/apache/hop/hop-test-plugin/1.0.0/hop-test-plugin-1.0.0.zip";
    server.createContext(
        "/missing/",
        exchange -> {
          exchange.sendResponseHeaders(404, -1);
          exchange.close();
        });
    server.createContext(
        path,
        exchange -> {
          exchange.getResponseHeaders().add("Content-Type", "application/zip");
          exchange.sendResponseHeaders(200, zipBytes.length);
          exchange.getResponseBody().write(zipBytes);
          exchange.close();
        });
    server.start();
    try {
      int port = server.getAddress().getPort();
      Path hopHome = tempDir.resolve("hop-fallback");
      Files.createDirectories(hopHome.resolve("plugins"));

      MarketplaceConfig config = new MarketplaceConfig();
      config.getRepositories().clear();
      MarketplaceRepository missing =
          new MarketplaceRepository("missing", "http://127.0.0.1:" + port + "/missing/", true);
      MarketplaceRepository ok =
          new MarketplaceRepository("ok", "http://127.0.0.1:" + port + "/", false);
      config.getRepositories().add(missing);
      config.getRepositories().add(ok);

      InstallReceipt receipt =
          new PluginInstaller(new LogChannel("test"), hopHome, config)
              .install(new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0"), true);
      assertEquals("ok", receipt.getRepositoryId());
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void installReportsPhasesAndDownloadBytes() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = zipServer(zipBytes);
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-progress");
      Files.createDirectories(hopHome.resolve("plugins"));
      MarketplaceConfig config = localRepoConfig(server.getAddress().getPort());
      RecordingInstallListener listener = new RecordingInstallListener();

      new PluginInstaller(new LogChannel("test"), hopHome, config)
          .install(
              new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0"),
              true,
              null,
              null,
              listener);

      assertEquals(
          List.of(
              IInstallListener.Phase.RESOLVE,
              IInstallListener.Phase.DOWNLOAD,
              IInstallListener.Phase.UNZIP,
              IInstallListener.Phase.ACTIVATE),
          listener.phases,
          "the progress bar depends on phases arriving in install order");
      assertEquals(zipBytes.length, listener.startedTotal, "size must reach the listener");
      assertEquals(
          zipBytes.length,
          listener.lastBytes,
          "the final byte callback must equal the file size so the bar completes");
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void installCommandWithRepoUrlInstallsFromAdHocRepository() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = zipServer(zipBytes);
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-adhoc-install");
      Files.createDirectories(hopHome.resolve("plugins"));
      int port = server.getAddress().getPort();

      String originalUserDir = System.getProperty("user.dir");
      try {
        System.setProperty("user.dir", hopHome.toAbsolutePath().toString());
        MarketplaceCommand.InstallCommand cmd = new MarketplaceCommand.InstallCommand();
        CommandLine cl = new CommandLine(cmd);
        cl.parseArgs("--repo-url", localUrl(port), "org.apache.hop:hop-test-plugin:1.0.0");
        cmd.run();

        Path pluginJar = hopHome.resolve("plugins/tech/test/plugin.jar");
        assertTrue(Files.isRegularFile(pluginJar));
        assertTrue(
            Files.isRegularFile(
                hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-test-plugin.json")));
        assertEquals(
            "corporate-repo",
            PluginInstaller.readReceipt(hopHome, "hop-test-plugin").getRepositoryId());
      } finally {
        if (originalUserDir != null) {
          System.setProperty("user.dir", originalUserDir);
        }
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void cancelDuringDownloadInstallsNothingAndDoesNotTryOtherRepositories() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = zipServer(zipBytes);
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-cancel");
      Files.createDirectories(hopHome.resolve("plugins"));
      int port = server.getAddress().getPort();

      // Two working repositories: a cancel must not silently restart against the second one.
      MarketplaceConfig config = new MarketplaceConfig();
      config.getRepositories().clear();
      config.getRepositories().add(new MarketplaceRepository("one", localUrl(port), true));
      config.getRepositories().add(new MarketplaceRepository("two", localUrl(port), false));

      RecordingInstallListener listener = new RecordingInstallListener();
      listener.cancelOnceDownloading = true;

      assertThrows(
          HopException.class,
          () ->
              new PluginInstaller(new LogChannel("test"), hopHome, config)
                  .install(
                      new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0"),
                      true,
                      null,
                      null,
                      listener));

      assertFalse(
          Files.exists(hopHome.resolve("plugins/tech/test/plugin.jar")),
          "a cancelled install must not leave plugin files behind");
      assertFalse(
          Files.exists(
              hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-test-plugin.json")),
          "a cancelled install must not write a receipt");
      assertEquals(
          1,
          listener.phases.stream().filter(p -> p == IInstallListener.Phase.DOWNLOAD).count(),
          "cancel must not fall through to the second repository");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void secondInstallSkipsWhenReceiptMatchesAndFilesRemain() throws Exception {
    byte[] zipBytes = buildPluginZip();
    AtomicInteger hits = new AtomicInteger();
    HttpServer server = zipServer(zipBytes, hits);
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-skip");
      Files.createDirectories(hopHome.resolve("plugins"));
      MarketplaceConfig config = localRepoConfig(server.getAddress().getPort());
      PluginInstaller installer = new PluginInstaller(new LogChannel("test"), hopHome, config);
      MavenCoordinates coords = new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0");

      installer.install(coords, true);
      installer.install(coords, true);
      assertEquals(
          1, hits.get(), "a matching receipt with its files present must not download again");

      Files.delete(hopHome.resolve("plugins/tech/test/plugin.jar"));
      installer.install(coords, true);
      assertEquals(2, hits.get(), "a missing receipt file must be installed again");
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void preferredRepositoryIsTriedFirstAndFallsBack() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = zipServer(zipBytes);
    server.start();
    try {
      int port = server.getAddress().getPort();
      Path hopHome = tempDir.resolve("hop-preferred");
      Files.createDirectories(hopHome.resolve("plugins"));
      MarketplaceConfig config = new MarketplaceConfig();
      config.getRepositories().clear();
      config
          .getRepositories()
          .add(
              new MarketplaceRepository(
                  "corporate-repo", "http://127.0.0.1:" + port + "/missing/", false));
      config.getRepositories().add(new MarketplaceRepository("central", localUrl(port), true));

      InstallReceipt receipt =
          new PluginInstaller(new LogChannel("test"), hopHome, config)
              .install(
                  new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0"),
                  true,
                  null,
                  "corporate-repo");
      assertEquals("central", receipt.getRepositoryId());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void forcedRepositoryDoesNotFallBack() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = zipServer(zipBytes);
    server.start();
    try {
      int port = server.getAddress().getPort();
      Path hopHome = tempDir.resolve("hop-forced");
      Files.createDirectories(hopHome.resolve("plugins"));
      MarketplaceConfig config = new MarketplaceConfig();
      config.getRepositories().clear();
      config
          .getRepositories()
          .add(new MarketplaceRepository("only", "http://127.0.0.1:" + port + "/missing/", true));
      config.getRepositories().add(new MarketplaceRepository("other", localUrl(port), false));

      assertThrows(
          HopException.class,
          () ->
              new PluginInstaller(new LogChannel("test"), hopHome, config)
                  .install(
                      new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0"),
                      true,
                      "only",
                      null));
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test/plugin.jar")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void installCommandWithRepoUrlUsesBasicAuthAndRejectsAnonymous() throws Exception {
    byte[] zipBytes = buildPluginZip();
    AtomicInteger anonymous = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    String path = "/org/apache/hop/hop-test-plugin/1.0.0/hop-test-plugin-1.0.0.zip";
    String expectedBasic =
        "Basic "
            + Base64.getEncoder().encodeToString("admin:s3cret".getBytes(StandardCharsets.UTF_8));
    server.createContext(
        path,
        exchange -> {
          String auth = exchange.getRequestHeaders().getFirst("Authorization");
          if (!expectedBasic.equals(auth)) {
            anonymous.incrementAndGet();
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
            return;
          }
          exchange.getResponseHeaders().add("Content-Type", "application/zip");
          exchange.sendResponseHeaders(200, zipBytes.length);
          exchange.getResponseBody().write(zipBytes);
          exchange.close();
        });
    server.start();
    try {
      int port = server.getAddress().getPort();
      Path hopHome = tempDir.resolve("hop-basic-url");
      Files.createDirectories(hopHome.resolve("plugins"));
      String originalUserDir = System.getProperty("user.dir");
      try {
        System.setProperty("user.dir", hopHome.toAbsolutePath().toString());
        withIsolatedMarketplaceConfig(
            () -> {
              marketplaceEnv(Map.of());
              MarketplaceCommand.InstallCommand anonymousCmd =
                  new MarketplaceCommand.InstallCommand();
              CommandLine anonymousLine = new CommandLine(anonymousCmd);
              anonymousLine.parseArgs(
                  "--repo-url", localUrl(port), "org.apache.hop:hop-test-plugin:1.0.0");
              PrintStream originalErr = System.err;
              ByteArrayOutputStream err = new ByteArrayOutputStream();
              System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
              try {
                CommandLine.ExecutionException failure =
                    assertThrows(CommandLine.ExecutionException.class, anonymousCmd::run);
                assertTrue(failure.getMessage().contains("hop-test-plugin"), failure.getMessage());
              } finally {
                System.setErr(originalErr);
              }
              assertTrue(
                  err.toString(StandardCharsets.UTF_8).contains("401"),
                  err.toString(StandardCharsets.UTF_8));
              assertFalse(Files.exists(hopHome.resolve("plugins/tech/test/plugin.jar")));
              assertTrue(anonymous.get() > 0);

              marketplaceEnv(
                  Map.of(
                      "HOP_MARKETPLACE_CORPORATE_REPO_USERNAME",
                      "admin",
                      "HOP_MARKETPLACE_CORPORATE_REPO_PASSWORD",
                      "s3cret"));
              MarketplaceCommand.InstallCommand cmd = new MarketplaceCommand.InstallCommand();
              CommandLine cl = new CommandLine(cmd);
              cl.parseArgs(
                  "--repo-url",
                  localUrl(port),
                  "--repo-id",
                  "corporate-repo",
                  "--auth-type",
                  "basic",
                  "org.apache.hop:hop-test-plugin:1.0.0");
              cmd.run();
            });
        assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
        assertEquals(
            "corporate-repo",
            PluginInstaller.readReceipt(hopHome, "hop-test-plugin").getRepositoryId());
      } finally {
        MarketplaceRepository.setEnvironmentForTesting(null);
        if (originalUserDir != null) {
          System.setProperty("user.dir", originalUserDir);
        }
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void installCommandWithRepoUrlUsesBearerToken() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    String path = "/org/apache/hop/hop-test-plugin/1.0.0/hop-test-plugin-1.0.0.zip";
    server.createContext(
        path,
        exchange -> {
          String auth = exchange.getRequestHeaders().getFirst("Authorization");
          if (!"Bearer s3cret-token".equals(auth)) {
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
            return;
          }
          exchange.getResponseHeaders().add("Content-Type", "application/zip");
          exchange.sendResponseHeaders(200, zipBytes.length);
          exchange.getResponseBody().write(zipBytes);
          exchange.close();
        });
    server.start();
    try {
      int port = server.getAddress().getPort();
      Path hopHome = tempDir.resolve("hop-token-url");
      Files.createDirectories(hopHome.resolve("plugins"));
      String originalUserDir = System.getProperty("user.dir");
      try {
        System.setProperty("user.dir", hopHome.toAbsolutePath().toString());
        withIsolatedMarketplaceConfig(
            () -> {
              marketplaceEnv(Map.of("HOP_MARKETPLACE_CORPORATE_REPO_TOKEN", "s3cret-token"));
              MarketplaceCommand.InstallCommand cmd = new MarketplaceCommand.InstallCommand();
              CommandLine cl = new CommandLine(cmd);
              cl.parseArgs(
                  "--repo-url",
                  localUrl(port),
                  "--repo-id",
                  "corporate-repo",
                  "--auth-type",
                  "token",
                  "org.apache.hop:hop-test-plugin:1.0.0");
              cmd.run();
            });
        assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
      } finally {
        MarketplaceRepository.setEnvironmentForTesting(null);
        if (originalUserDir != null) {
          System.setProperty("user.dir", originalUserDir);
        }
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void installCommandRejectsShortNameWhenRepositoryCannotBeBrowsed() throws Exception {
    Path hopHome = tempDir.resolve("hop-short-name");
    Files.createDirectories(hopHome.resolve("plugins"));
    String originalUserDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", hopHome.toAbsolutePath().toString());
      withIsolatedMarketplaceConfig(
          () -> {
            marketplaceEnv(Map.of());
            MarketplaceCommand.InstallCommand cmd = new MarketplaceCommand.InstallCommand();
            CommandLine cl = new CommandLine(cmd);
            cl.parseArgs(
                "--repo-url",
                "http://127.0.0.1:9/maven/releases/",
                "--repo-type",
                "maven",
                "zz-no-such-plugin-8723:1.0.0");
            CommandLine.ExecutionException failure =
                assertThrows(CommandLine.ExecutionException.class, cmd::run);
            assertTrue(failure.getMessage().contains("cannot be browsed"), failure.getMessage());
            assertTrue(
                failure.getMessage().contains("groupId:artifactId:version"), failure.getMessage());
          });
    } finally {
      MarketplaceRepository.setEnvironmentForTesting(null);
      if (originalUserDir != null) {
        System.setProperty("user.dir", originalUserDir);
      }
    }
  }

  @Test
  void installCommandWritesIntoConfiguredPluginFolder() throws Exception {
    byte[] zipBytes = buildPluginZip();
    HttpServer server = zipServer(zipBytes);
    server.start();
    try {
      int port = server.getAddress().getPort();
      Path image = tempDir.resolve("image-home");
      Path volume = tempDir.resolve("volume-home");
      Files.createDirectories(image.resolve("plugins"));
      Files.createDirectories(volume.resolve("plugins"));
      String originalUserDir = System.getProperty("user.dir");
      String originalFolders = System.getProperty("HOP_PLUGIN_BASE_FOLDERS");
      try {
        System.setProperty("user.dir", image.toAbsolutePath().toString());
        System.setProperty(
            "HOP_PLUGIN_BASE_FOLDERS", image.resolve("plugins") + "," + volume.resolve("plugins"));
        withIsolatedMarketplaceConfig(
            () -> {
              MarketplaceCommand.InstallCommand cmd = new MarketplaceCommand.InstallCommand();
              CommandLine cl = new CommandLine(cmd);
              cl.parseArgs("--repo-url", localUrl(port), "org.apache.hop:hop-test-plugin:1.0.0");
              cmd.run();
            });
        assertTrue(Files.isRegularFile(volume.resolve("plugins/tech/test/plugin.jar")));
        assertTrue(Files.isRegularFile(volume.resolve("lib/core/shared.jar")));
        assertFalse(Files.exists(image.resolve("plugins/tech/test/plugin.jar")));
      } finally {
        if (originalUserDir != null) {
          System.setProperty("user.dir", originalUserDir);
        }
        if (originalFolders == null) {
          System.clearProperty("HOP_PLUGIN_BASE_FOLDERS");
        } else {
          System.setProperty("HOP_PLUGIN_BASE_FOLDERS", originalFolders);
        }
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void webLayoutInstallsSharedJarIntoWebInfLibAndSkipsWhenItRemains() throws Exception {
    byte[] zipBytes = buildPluginZip();
    AtomicInteger hits = new AtomicInteger();
    HttpServer server = zipServer(zipBytes, hits);
    server.start();
    try {
      Path hopHome = tempDir.resolve("tomcat");
      Files.createDirectories(hopHome.resolve("plugins"));
      Path webLib = hopHome.resolve("webapps/ROOT/WEB-INF/lib");
      Files.createDirectories(webLib);
      MarketplaceConfig config = localRepoConfig(server.getAddress().getPort());
      PluginInstaller installer = new PluginInstaller(new LogChannel("test"), hopHome, config);
      MavenCoordinates coords = new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0");

      installer.install(coords, true);
      assertTrue(Files.isRegularFile(webLib.resolve("shared.jar")));
      assertEquals("shared-lib", Files.readString(webLib.resolve("shared.jar")));
      assertFalse(Files.exists(hopHome.resolve("lib/core/shared.jar")));
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));

      installer.install(coords, true);
      assertEquals(1, hits.get(), "a shared jar in WEB-INF/lib still satisfies the receipt");
    } finally {
      server.stop(0);
    }
  }

  private static void marketplaceEnv(Map<String, String> values) {
    MarketplaceRepository.setEnvironmentForTesting(values::get);
  }

  private static void withIsolatedMarketplaceConfig(ThrowingRunnable action) throws Exception {
    HopConfig hopConfig = HopConfig.getInstance();
    Field field = ConfigFile.class.getDeclaredField("configMap");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) field.get(hopConfig);
    Object previous = map.get(MarketplaceConfig.CONFIG_KEY);
    Map<String, Object> closed = new LinkedHashMap<>();
    closed.put("id", "closed");
    closed.put("url", "http://127.0.0.1:1/");
    closed.put("enabled", true);
    closed.put("primary", true);
    closed.put("browse", false);
    Map<String, Object> marketplace = new LinkedHashMap<>();
    marketplace.put("enabled", true);
    marketplace.put("groupId", "org.apache.hop");
    marketplace.put("repositories", List.of(closed));
    map.put(MarketplaceConfig.CONFIG_KEY, marketplace);
    try {
      action.run();
    } finally {
      if (previous == null) {
        map.remove(MarketplaceConfig.CONFIG_KEY);
      } else {
        map.put(MarketplaceConfig.CONFIG_KEY, previous);
      }
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  /** Captures the phase sequence and byte counts an install reports. */
  private static class RecordingInstallListener implements IInstallListener {
    private final List<Phase> phases = new ArrayList<>();
    private long startedTotal = Long.MIN_VALUE;
    private long lastBytes = -1;
    private boolean cancelOnceDownloading;

    @Override
    public void phase(Phase phase, String detail) {
      phases.add(phase);
    }

    @Override
    public void started(String label, long totalBytes) {
      startedTotal = totalBytes;
    }

    @Override
    public void transferred(long bytesSoFar, long totalBytes) {
      lastBytes = bytesSoFar;
    }

    @Override
    public boolean isCancelled() {
      // Only once the download has actually started, so this exercises cancel mid-transfer rather
      // than the cheaper pre-flight check.
      return cancelOnceDownloading && phases.contains(Phase.DOWNLOAD);
    }
  }

  @Test
  void updateRemovesFilesOnlyThePreviousVersionHad() throws Exception {
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-update");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));

      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/lib/dep-1.0.jar")));

      InstallReceipt receipt =
          installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), true);

      Path pluginFolder = hopHome.resolve("plugins/tech/test");
      assertEquals("plugin-2", Files.readString(pluginFolder.resolve("plugin.jar")));
      assertTrue(Files.isRegularFile(pluginFolder.resolve("lib/dep-2.0.jar")));
      assertFalse(
          Files.exists(pluginFolder.resolve("lib/dep-1.0.jar")),
          "the jar only version 1 had must not stay on the plugin classpath");
      assertFalse(
          Files.exists(pluginFolder.resolve("samples")),
          "a folder only version 1 had is removed once it is empty");
      // Shared lib/core jars stay sticky on update as on uninstall (#9614)
      assertTrue(Files.isRegularFile(hopHome.resolve("lib/core/shared-1.0.jar")));
      assertTrue(Files.isRegularFile(hopHome.resolve("lib/core/shared-2.0.jar")));

      assertEquals("2.0.0", receipt.getVersion());
      assertTrue(receipt.getObsoletePaths().isEmpty());
      String receiptJson =
          Files.readString(
              hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve(TEST_PLUGIN + ".json"));
      assertFalse(receiptJson.contains("obsoletePaths"), receiptJson);

      new PluginUninstaller(new LogChannel("test"), hopHome).uninstall(TEST_PLUGIN);
      assertFalse(Files.exists(pluginFolder), "uninstall after an update leaves nothing behind");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void updateThroughTheInstallCommandRemovesTheOldJar() throws Exception {
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    Path hopHome = tempDir.resolve("hop-command-update");
    Files.createDirectories(hopHome.resolve("plugins"));
    String originalUserDir = System.getProperty("user.dir");
    String originalFolders = System.getProperty("HOP_PLUGIN_BASE_FOLDERS");
    try {
      System.setProperty("user.dir", hopHome.toAbsolutePath().toString());
      System.clearProperty("HOP_PLUGIN_BASE_FOLDERS");
      String url = localUrl(server.getAddress().getPort());
      withIsolatedMarketplaceConfig(
          () -> {
            for (String version : List.of("1.0.0", "2.0.0")) {
              MarketplaceCommand.InstallCommand cmd = new MarketplaceCommand.InstallCommand();
              new CommandLine(cmd)
                  .parseArgs("--repo-url", url, "org.apache.hop:" + TEST_PLUGIN + ":" + version);
              cmd.run();
            }
          });
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/lib/dep-2.0.jar")));
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test/lib/dep-1.0.jar")));
    } finally {
      if (originalUserDir != null) {
        System.setProperty("user.dir", originalUserDir);
      }
      if (originalFolders != null) {
        System.setProperty("HOP_PLUGIN_BASE_FOLDERS", originalFolders);
      }
      server.stop(0);
    }
  }

  @Test
  void stagedUpdateRemovesOldFilesWhenActivated() throws Exception {
    HttpServer server =
        versionServer(
            TEST_PLUGIN,
            Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip(), "3.0.0", version3Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-staged");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      Path lib = hopHome.resolve("plugins/tech/test/lib");

      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);
      // Stage two updates in a row without activating: version 1's files are the ones on disk.
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), false);
      InstallReceipt staged =
          installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "3.0.0"), false);
      assertTrue(staged.isPendingActivation());
      assertTrue(staged.getObsoletePaths().contains("plugins/tech/test/lib/dep-1.0.jar"));
      assertTrue(
          Files.isRegularFile(lib.resolve("dep-1.0.jar")), "nothing changes before activation");

      installer.activateAllPending();

      assertTrue(Files.isRegularFile(lib.resolve("dep-3.0.jar")));
      assertFalse(Files.exists(lib.resolve("dep-1.0.jar")));
      assertFalse(Files.exists(lib.resolve("dep-2.0.jar")), "version 2 was never activated");
      InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
      assertEquals("3.0.0", receipt.getVersion());
      assertFalse(receipt.isPendingActivation());
      assertTrue(receipt.getObsoletePaths().isEmpty());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void updateKeepsFilesThatAnotherReceiptLists() throws Exception {
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-shared-path");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);

      // Another marketplace artifact that ships the same file
      InstallReceipt other = new InstallReceipt();
      other.setArtifactId("hop-other-plugin");
      other.setVersion("1.0.0");
      other.setPaths(List.of("plugins/tech/test/lib/dep-1.0.jar"));
      HopJson.newMapper()
          .writeValue(
              hopHome
                  .resolve(PluginInstaller.RECEIPTS_DIR)
                  .resolve("hop-other-plugin.json")
                  .toFile(),
              other);

      // An unreadable receipt of yet another plugin does not stop the update.
      Files.writeString(
          hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-broken-plugin.json"), "{");

      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), true);
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/lib/dep-1.0.jar")));
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/lib/dep-2.0.jar")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void obsoleteFileThatCannotBeRemovedIsRetriedAndUninstalled() throws Exception {
    Path hopHome = tempDir.resolve("hop-locked");
    Files.createDirectories(hopHome.resolve("plugins"));
    assumeTrue(
        Files.getFileStore(hopHome).supportsFileAttributeView("posix"),
        "needs POSIX permissions to make a delete fail");
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    Path samples = hopHome.resolve("plugins/tech/test/samples");
    try {
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      MavenCoordinates v2 = new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0");
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);

      // Stand-in for a jar Windows keeps locked: a file in a read-only folder cannot be deleted.
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("r-xr-xr-x"));
      assumeFalse(Files.isWritable(samples), "running as a user that ignores permissions");
      InstallReceipt receipt = installer.install(v2, true);
      assertEquals(List.of("plugins/tech/test/samples/sample.hpl"), receipt.getObsoletePaths());
      assertEquals(
          List.of("plugins/tech/test/samples/sample.hpl"),
          PluginInstaller.readReceipt(hopHome, TEST_PLUGIN).getObsoletePaths());

      // Installing the same version again retries the removal without downloading.
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));
      InstallReceipt again = installer.install(v2, true);
      assertTrue(again.isAlreadyPresent());
      assertTrue(again.getObsoletePaths().isEmpty());
      assertFalse(Files.exists(samples));

      // Uninstall removes a leftover as well.
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("r-xr-xr-x"));
      installer.install(v2, true);
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));
      new PluginUninstaller(new LogChannel("test"), hopHome).uninstall(TEST_PLUGIN);
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test")));
    } finally {
      if (Files.isDirectory(samples)) {
        Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));
      }
      server.stop(0);
    }
  }

  @Test
  void uninstallKeepsAnObsoleteFileThatCannotBeRemovedInTheReceipt() throws Exception {
    Path hopHome = tempDir.resolve("hop-uninstall-locked");
    Files.createDirectories(hopHome.resolve("plugins"));
    assumeTrue(
        Files.getFileStore(hopHome).supportsFileAttributeView("posix"),
        "needs POSIX permissions to make a delete fail");
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    Path samples = hopHome.resolve("plugins/tech/test/samples");
    try {
      install(hopHome, server, "1.0.0");
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("r-xr-xr-x"));
      assumeFalse(Files.isWritable(samples), "running as a user that ignores permissions");
      install(hopHome, server, "2.0.0");

      // The leftover is still in use: the rest of the plugin goes, the leftover stays listed.
      PluginUninstaller uninstaller = new PluginUninstaller(new LogChannel("test"), hopHome);
      assertDoesNotThrow(() -> uninstaller.uninstall(TEST_PLUGIN));
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test/plugin.jar")));
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test/lib")));
      assertTrue(Files.isRegularFile(samples.resolve("sample.hpl")));
      InstallReceipt left = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
      for (String path : left.getPaths()) {
        assertTrue(Files.isDirectory(hopHome.resolve(path)), path);
      }
      assertEquals(List.of("plugins/tech/test/samples/sample.hpl"), left.getObsoletePaths());
      // It no longer counts as an install of any version.
      assertTrue(left.isUninstallPending());
      assertNull(left.getVersion());
      assertTrue(left.getChecksums().isEmpty());
      assertTrue(left.getSharedFiles().isEmpty());

      // Once the file is free, the next install or apply finishes the uninstall.
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));
      new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()))
          .activateAllPending();
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test")));
      assertNull(PluginInstaller.readReceipt(hopHome, TEST_PLUGIN));
    } finally {
      if (Files.isDirectory(samples)) {
        Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));
      }
      server.stop(0);
    }
  }

  @Test
  void reinstallAfterAnUnfinishedUninstallDownloadsAndRecordsItsOwnSharedFiles() throws Exception {
    Path hopHome = tempDir.resolve("hop-reinstall-after-uninstall");
    Files.createDirectories(hopHome.resolve("plugins"));
    assumeTrue(
        Files.getFileStore(hopHome).supportsFileAttributeView("posix"),
        "needs POSIX permissions to make a delete fail");
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    Path samples = hopHome.resolve("plugins/tech/test/samples");
    try {
      install(hopHome, server, "1.0.0");
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("r-xr-xr-x"));
      assumeFalse(Files.isWritable(samples), "running as a user that ignores permissions");
      install(hopHome, server, "2.0.0");
      new PluginUninstaller(new LogChannel("test"), hopHome).uninstall(TEST_PLUGIN);
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));

      InstallReceipt receipt =
          new PluginInstaller(
                  new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()))
              .install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), true);

      assertFalse(receipt.isAlreadyPresent());
      assertFalse(receipt.isUninstallPending());
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
      assertFalse(Files.exists(samples));
      // The jar left in lib/core matches: this install did not create or replace it.
      InstallReceipt.SharedFile shared = sharedFile(hopHome, "lib/core/shared-2.0.jar");
      assertEquals(InstallReceipt.SharedFileAction.UNCHANGED, shared.getAction());
      assertNull(shared.getBackup());
    } finally {
      if (Files.isDirectory(samples)) {
        Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));
      }
      server.stop(0);
    }
  }

  @Test
  void uninstallOfAStagedPluginRemovesTheStagedFiles() throws Exception {
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-uninstall-staged");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), false);

      new PluginUninstaller(new LogChannel("test"), hopHome).uninstall(TEST_PLUGIN);
      assertFalse(Files.exists(hopHome.resolve(PluginInstaller.STAGING_DIR).resolve(TEST_PLUGIN)));

      // The next start does not bring the plugin back.
      installer.activateAllPending();
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test")));
      assertNull(PluginInstaller.readReceipt(hopHome, TEST_PLUGIN));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void unreadableReceiptReportsTheFilesTheStagedVersionDoesNotHave() throws Exception {
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-unreadable-update");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), false);
      Files.writeString(
          hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve(TEST_PLUGIN + ".json"), "{");

      assertEquals(
          List.of("plugins/tech/test/lib/dep-1.0.jar", "plugins/tech/test/samples/sample.hpl"),
          installer.filesTheStagedVersionDoesNotHave(
              TEST_PLUGIN,
              List.of(
                  "plugins/tech/test/plugin.jar",
                  "plugins/tech/test/lib/dep-2.0.jar",
                  "lib/core/shared-2.0.jar")));

      // Reported, not removed: a plugin folder can hold files added by hand.
      installer.activateAllPending();
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/lib/dep-2.0.jar")));
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/lib/dep-1.0.jar")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void corruptReceiptOfAStagedPluginDoesNotStopActivation() throws Exception {
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-corrupt-staged");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), false);
      // Interrupted while writing the receipt: truncated JSON next to the staged files.
      Files.writeString(
          hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve(TEST_PLUGIN + ".json"), "{");

      installer.activateAllPending();

      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/lib/dep-2.0.jar")));
      assertFalse(Files.exists(hopHome.resolve(PluginInstaller.STAGING_DIR).resolve(TEST_PLUGIN)));

      // The unreadable receipt is kept aside and a new one lists the activated files.
      assertEquals(
          "{",
          Files.readString(
              hopHome
                  .resolve(PluginInstaller.RECEIPTS_DIR)
                  .resolve(TEST_PLUGIN + ".json.unreadable")));
      InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
      assertEquals(TEST_PLUGIN, receipt.getArtifactId());
      assertNull(receipt.getVersion());
      assertFalse(receipt.isPendingActivation());
      assertTrue(receipt.getPaths().contains("plugins/tech/test/plugin.jar"));
      assertEquals(sha256("plugin-2"), receipt.getChecksums().get("plugins/tech/test/plugin.jar"));

      // With a readable receipt the plugin can be updated and uninstalled again.
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);
      assertEquals("1.0.0", PluginInstaller.readReceipt(hopHome, TEST_PLUGIN).getVersion());
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test/lib/dep-2.0.jar")));
      new PluginUninstaller(new LogChannel("test"), hopHome).uninstall(TEST_PLUGIN);
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void leftoverThatAnotherPluginInstalledSinceIsKept() throws Exception {
    Path hopHome = tempDir.resolve("hop-leftover-claimed");
    Files.createDirectories(hopHome.resolve("plugins"));
    assumeTrue(
        Files.getFileStore(hopHome).supportsFileAttributeView("posix"),
        "needs POSIX permissions to make a delete fail");
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    Path samples = hopHome.resolve("plugins/tech/test/samples");
    try {
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("r-xr-xr-x"));
      assumeFalse(Files.isWritable(samples), "running as a user that ignores permissions");
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), true);
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));
      assertEquals(
          List.of("plugins/tech/test/samples/sample.hpl"),
          PluginInstaller.readReceipt(hopHome, TEST_PLUGIN).getObsoletePaths());

      // Another plugin installs the same file before the leftover is retried.
      writeOtherReceipt(hopHome, "plugins/tech/test/samples/sample.hpl");

      new PluginUninstaller(new LogChannel("test"), hopHome).uninstall(TEST_PLUGIN);
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test/plugin.jar")));
      assertTrue(
          Files.isRegularFile(samples.resolve("sample.hpl")),
          "uninstall must not delete a leftover another plugin lists");

      // The retry from activateAllPending keeps it as well, and drops it from the receipt.
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("r-xr-xr-x"));
      Files.delete(hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-other-plugin.json"));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), true);
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));
      writeOtherReceipt(hopHome, "plugins/tech/test/samples/sample.hpl");
      installer.activateAllPending();
      assertTrue(Files.isRegularFile(samples.resolve("sample.hpl")));
      assertTrue(PluginInstaller.readReceipt(hopHome, TEST_PLUGIN).getObsoletePaths().isEmpty());
    } finally {
      if (Files.isDirectory(samples)) {
        Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));
      }
      server.stop(0);
    }
  }

  @Test
  void obsoletePathThatIsTheCurrentFileUnderAnotherCaseIsKept() throws Exception {
    Map<String, String> v1 = new LinkedHashMap<>();
    v1.put("plugins/tech/Foo/a.jar", "a-1");
    Map<String, String> v2 = new LinkedHashMap<>();
    v2.put("plugins/tech/foo/a.jar", "a-2");
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", zip(v1), "2.0.0", zip(v2)));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-case");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);

      // Stand-in for a case-insensitive file system: both spellings name one folder.
      Path upper = hopHome.resolve("plugins/tech/Foo");
      Path lower = hopHome.resolve("plugins/tech/foo");
      Files.move(upper, lower);
      try {
        Files.createSymbolicLink(upper, lower.getFileName());
      } catch (UnsupportedOperationException | IOException e) {
        assumeTrue(false, "needs symbolic links: " + e.getMessage());
      }

      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), true);
      assertEquals("a-2", Files.readString(lower.resolve("a.jar")));
    } finally {
      server.stop(0);
    }
  }

  private static void writeOtherReceipt(Path hopHome, String path) throws IOException {
    InstallReceipt other = new InstallReceipt();
    other.setArtifactId("hop-other-plugin");
    other.setVersion("1.0.0");
    other.setPaths(List.of(path));
    HopJson.newMapper()
        .writeValue(
            hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-other-plugin.json").toFile(),
            other);
  }

  @Test
  void receiptRecordsChecksumsAndThePreviousVersion() throws Exception {
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-checksums");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      MavenCoordinates v2 = new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0");

      InstallReceipt first =
          installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);
      Map<String, String> expected = new LinkedHashMap<>();
      expected.put("plugins/tech/test/plugin.jar", sha256("plugin-1"));
      expected.put("plugins/tech/test/lib/dep-1.0.jar", sha256("dep-1.0"));
      expected.put("plugins/tech/test/samples/sample.hpl", sha256("<pipeline/>"));
      expected.put("lib/core/shared-1.0.jar", sha256("shared-1.0"));
      assertEquals(expected, first.getChecksums(), "one checksum per file, none for folders");
      assertNull(first.getPreviousVersion());
      String json =
          Files.readString(
              hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve(TEST_PLUGIN + ".json"));
      assertFalse(json.contains("previousVersion"), json);
      assertEquals(expected, PluginInstaller.readReceipt(hopHome, TEST_PLUGIN).getChecksums());

      assertEquals("1.0.0", installer.install(v2, true).getPreviousVersion());

      // Installing 2.0.0 again (a file went missing) still replaced 1.0.0.
      Files.delete(hopHome.resolve("plugins/tech/test/plugin.jar"));
      assertEquals("1.0.0", installer.install(v2, true).getPreviousVersion());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void receiptRecordsWhatActivationDidToSharedJars() throws Exception {
    assumeFalse(Const.isWindows(), "Windows never replaces an existing lib/core jar");
    HttpServer server = zipServer(buildPluginZip());
    server.start();
    try {
      // Created by this install
      Path fresh = tempDir.resolve("hop-shared-created");
      Files.createDirectories(fresh.resolve("plugins"));
      install(fresh, server, "1.0.0");
      InstallReceipt.SharedFile created = sharedFile(fresh, "lib/core/shared.jar");
      assertEquals(InstallReceipt.SharedFileAction.CREATED, created.getAction());
      assertTrue(created.isCreatedByMarketplace());
      assertNull(created.getBackup());

      // Shipped with the client, same content
      Path shipped = tempDir.resolve("hop-shared-unchanged");
      Files.createDirectories(shipped.resolve("lib/core"));
      Files.writeString(shipped.resolve("lib/core/shared.jar"), "shared-lib");
      install(shipped, server, "1.0.0");
      InstallReceipt.SharedFile unchanged = sharedFile(shipped, "lib/core/shared.jar");
      assertEquals(InstallReceipt.SharedFileAction.UNCHANGED, unchanged.getAction());
      assertFalse(unchanged.isCreatedByMarketplace());

      // Shipped with the client, other content: backed up, then replaced
      Path older = tempDir.resolve("hop-shared-replaced");
      Files.createDirectories(older.resolve("lib/core"));
      Files.writeString(older.resolve("lib/core/shared.jar"), "old-shared-lib");
      install(older, server, "1.0.0");
      InstallReceipt.SharedFile replaced = sharedFile(older, "lib/core/shared.jar");
      assertEquals(InstallReceipt.SharedFileAction.REPLACED, replaced.getAction());
      assertFalse(replaced.isCreatedByMarketplace());
      assertTrue(
          replaced.getBackup().startsWith("plugins/.marketplace/backup/hop-test-plugin/1.0.0/"),
          replaced.getBackup());
      assertTrue(replaced.getBackup().endsWith("/lib/core/shared.jar"), replaced.getBackup());
      assertEquals("old-shared-lib", Files.readString(older.resolve(replaced.getBackup())));
      assertEquals(replaced.getBackup(), replaced.getOriginalBackup(), "the client's own copy");
      assertEquals("shared-lib", Files.readString(older.resolve("lib/core/shared.jar")));

      // Installing the same version again finds the jar unchanged but keeps what it replaced.
      Files.delete(older.resolve("plugins/tech/test/plugin.jar"));
      install(older, server, "1.0.0");
      InstallReceipt.SharedFile again = sharedFile(older, "lib/core/shared.jar");
      assertEquals(InstallReceipt.SharedFileAction.REPLACED, again.getAction());
      assertEquals(replaced.getBackup(), again.getBackup());
      assertEquals("old-shared-lib", Files.readString(older.resolve(again.getBackup())));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void sharedJarThatCannotBeBackedUpIsKept() throws Exception {
    assumeFalse(Const.isWindows(), "Windows never replaces an existing lib/core jar");
    Path hopHome = tempDir.resolve("hop-shared-kept");
    Files.createDirectories(hopHome.resolve("lib/core"));
    assumeTrue(
        Files.getFileStore(hopHome).supportsFileAttributeView("posix"),
        "needs POSIX permissions to make the backup fail");
    Files.writeString(hopHome.resolve("lib/core/shared.jar"), "old-shared-lib");
    Path backupDir = hopHome.resolve(PluginInstaller.BACKUP_DIR);
    Files.createDirectories(backupDir);
    Files.setPosixFilePermissions(backupDir, PosixFilePermissions.fromString("r-xr-xr-x"));
    HttpServer server = zipServer(buildPluginZip());
    server.start();
    try {
      assumeFalse(Files.isWritable(backupDir), "running as a user that ignores permissions");
      install(hopHome, server, "1.0.0");
      InstallReceipt.SharedFile kept = sharedFile(hopHome, "lib/core/shared.jar");
      assertEquals(InstallReceipt.SharedFileAction.KEPT, kept.getAction());
      assertNull(kept.getBackup());
      assertEquals("old-shared-lib", Files.readString(hopHome.resolve("lib/core/shared.jar")));
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
    } finally {
      Files.setPosixFilePermissions(backupDir, PosixFilePermissions.fromString("rwxr-xr-x"));
      server.stop(0);
    }
  }

  @Test
  void unusedBackupLeavesNoEmptyFoldersBehind() throws Exception {
    assumeFalse(Const.isWindows(), "Windows never replaces an existing lib/core jar");
    Path hopHome = tempDir.resolve("hop-unused-backup");
    Path libCore = hopHome.resolve("lib/core");
    Files.createDirectories(libCore);
    assumeTrue(
        Files.getFileStore(hopHome).supportsFileAttributeView("posix"),
        "needs POSIX permissions to make the copy fail");
    Files.writeString(libCore.resolve("shared.jar"), "old-shared-lib");
    // The backup can be written, but the jar in lib/core cannot be replaced.
    Files.setPosixFilePermissions(libCore, PosixFilePermissions.fromString("r-xr-xr-x"));
    HttpServer server = zipServer(buildPluginZip());
    server.start();
    try {
      assumeFalse(Files.isWritable(libCore), "running as a user that ignores permissions");
      install(hopHome, server, "1.0.0");
      assertEquals(
          InstallReceipt.SharedFileAction.KEPT,
          sharedFile(hopHome, "lib/core/shared.jar").getAction());
      assertEquals("old-shared-lib", Files.readString(libCore.resolve("shared.jar")));
      Path backupDir = hopHome.resolve(PluginInstaller.BACKUP_DIR);
      if (Files.isDirectory(backupDir)) {
        try (var stream = Files.list(backupDir)) {
          assertEquals(List.of(), stream.toList(), "no backup folders left behind");
        }
      }
    } finally {
      Files.setPosixFilePermissions(libCore, PosixFilePermissions.fromString("rwxr-xr-x"));
      server.stop(0);
    }
  }

  @Test
  void backupPathStaysUnderTheBackupFolder() {
    assertEquals(
        PluginInstaller.BACKUP_DIR + "/hop-test-plugin/2.20.0-SNAPSHOT/id/lib/core/x.jar",
        PluginInstaller.backupPath("hop-test-plugin", "2.20.0-SNAPSHOT", "id", "lib/core/x.jar"));
    Path root = tempDir.resolve(PluginInstaller.BACKUP_DIR).normalize();
    for (String[] ids :
        new String[][] {
          {"..", ".."}, {"a/../../..", "1.0"}, {"a", "../../../../lib/core"}, {"a", "..\\..\\x"}
        }) {
      String backup = PluginInstaller.backupPath(ids[0], ids[1], "id", "lib/core/x.jar");
      Path resolved = tempDir.resolve(backup).normalize();
      assertTrue(resolved.startsWith(root), backup);
      // <artifactId>/<version>/<installId>/lib/core/x.jar: one folder each for id and version
      assertEquals(6, root.relativize(resolved).getNameCount(), backup);
    }
  }

  @Test
  void createdByMarketplaceSurvivesUpdatesAndOtherPlugins() throws Exception {
    Map<String, String> v1 = new LinkedHashMap<>();
    v1.put("plugins/tech/test/plugin.jar", "plugin-1");
    v1.put("lib/core/common.jar", "common");
    Map<String, String> v2 = new LinkedHashMap<>(v1);
    v2.put("plugins/tech/test/plugin.jar", "plugin-2");
    Map<String, String> other = new LinkedHashMap<>();
    other.put("plugins/tech/other/other.jar", "other");
    other.put("lib/core/common.jar", "common");
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", zip(v1), "2.0.0", zip(v2)));
    serve(server, "hop-other-plugin", "1.0.0", zip(other));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-created-flag");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), true);

      InstallReceipt.SharedFile updated = sharedFile(hopHome, "lib/core/common.jar");
      assertEquals(InstallReceipt.SharedFileAction.UNCHANGED, updated.getAction());
      assertTrue(updated.isCreatedByMarketplace(), "1.0.0 created it");

      installer.install(new MavenCoordinates("org.apache.hop", "hop-other-plugin", "1.0.0"), true);
      InstallReceipt.SharedFile fromOther =
          PluginInstaller.readReceipt(hopHome, "hop-other-plugin")
              .getSharedFiles()
              .get("lib/core/common.jar");
      assertEquals(InstallReceipt.SharedFileAction.UNCHANGED, fromOther.getAction());
      assertTrue(fromOther.isCreatedByMarketplace(), "another plugin created it");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void backupsAreNeverOverwrittenAndTheClientOriginalIsKept() throws Exception {
    assumeFalse(Const.isWindows(), "Windows never replaces an existing lib/core jar");
    Map<String, String> v1 = new LinkedHashMap<>();
    v1.put("plugins/tech/test/plugin.jar", "plugin-1");
    v1.put("lib/core/x.jar", "x-from-1");
    Map<String, String> v2 = new LinkedHashMap<>();
    v2.put("plugins/tech/test/plugin.jar", "plugin-2");
    v2.put("lib/core/x.jar", "x-from-2");
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", zip(v1), "2.0.0", zip(v2)));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-downgrade");
      Files.createDirectories(hopHome.resolve("lib/core"));
      Files.writeString(hopHome.resolve("lib/core/x.jar"), "x-client");

      install(hopHome, server, "1.0.0");
      InstallReceipt.SharedFile first = sharedFile(hopHome, "lib/core/x.jar");
      install(hopHome, server, "2.0.0");
      InstallReceipt.SharedFile second = sharedFile(hopHome, "lib/core/x.jar");
      install(hopHome, server, "1.0.0");
      InstallReceipt.SharedFile third = sharedFile(hopHome, "lib/core/x.jar");

      assertEquals(InstallReceipt.SharedFileAction.REPLACED, third.getAction());
      assertNotEquals(first.getBackup(), third.getBackup(), "a backup folder per install");
      assertEquals("x-client", Files.readString(hopHome.resolve(first.getBackup())));
      assertEquals("x-from-1", Files.readString(hopHome.resolve(second.getBackup())));
      assertEquals("x-from-2", Files.readString(hopHome.resolve(third.getBackup())));
      assertEquals(first.getBackup(), third.getOriginalBackup());
      assertFalse(third.isCreatedByMarketplace());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void keptJarIsRecordedUnchangedOnceItMatchesThePackage() throws Exception {
    assumeFalse(Const.isWindows(), "Windows never replaces an existing lib/core jar");
    Path hopHome = tempDir.resolve("hop-kept-then-fixed");
    Files.createDirectories(hopHome.resolve("lib/core"));
    assumeTrue(
        Files.getFileStore(hopHome).supportsFileAttributeView("posix"),
        "needs POSIX permissions to make the backup fail");
    Files.writeString(hopHome.resolve("lib/core/shared.jar"), "old-shared-lib");
    Path backupDir = hopHome.resolve(PluginInstaller.BACKUP_DIR);
    Files.createDirectories(backupDir);
    Files.setPosixFilePermissions(backupDir, PosixFilePermissions.fromString("r-xr-xr-x"));
    HttpServer server = zipServer(buildPluginZip());
    server.start();
    try {
      assumeFalse(Files.isWritable(backupDir), "running as a user that ignores permissions");
      install(hopHome, server, "1.0.0");
      assertEquals(
          InstallReceipt.SharedFileAction.KEPT,
          sharedFile(hopHome, "lib/core/shared.jar").getAction());

      // The user replaces the jar by hand, then the same version is installed again.
      Files.writeString(hopHome.resolve("lib/core/shared.jar"), "shared-lib");
      Files.delete(hopHome.resolve("plugins/tech/test/plugin.jar"));
      install(hopHome, server, "1.0.0");
      assertEquals(
          InstallReceipt.SharedFileAction.UNCHANGED,
          sharedFile(hopHome, "lib/core/shared.jar").getAction());
    } finally {
      Files.setPosixFilePermissions(backupDir, PosixFilePermissions.fromString("rwxr-xr-x"));
      server.stop(0);
    }
  }

  @Test
  void sharedJarAnUpdateNoLongerShipsStaysInTheReceipt() throws Exception {
    HttpServer server =
        versionServer(
            TEST_PLUGIN,
            Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip(), "3.0.0", version3Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-not-shipped");
      Files.createDirectories(hopHome.resolve("plugins"));
      install(hopHome, server, "1.0.0");
      install(hopHome, server, "2.0.0");

      InstallReceipt.SharedFile dropped = sharedFile(hopHome, "lib/core/shared-1.0.jar");
      assertEquals(InstallReceipt.SharedFileAction.NOT_SHIPPED, dropped.getAction());
      assertTrue(dropped.isCreatedByMarketplace());
      assertTrue(Files.isRegularFile(hopHome.resolve("lib/core/shared-1.0.jar")), "sticky");

      install(hopHome, server, "3.0.0");
      assertEquals(
          InstallReceipt.SharedFileAction.NOT_SHIPPED,
          sharedFile(hopHome, "lib/core/shared-1.0.jar").getAction());
      assertEquals(
          InstallReceipt.SharedFileAction.NOT_SHIPPED,
          sharedFile(hopHome, "lib/core/shared-2.0.jar").getAction());

      // Once the file is gone, the next install drops the entry.
      Files.delete(hopHome.resolve("lib/core/shared-1.0.jar"));
      Files.delete(hopHome.resolve("plugins/tech/test/plugin.jar"));
      install(hopHome, server, "3.0.0");
      assertNull(sharedFile(hopHome, "lib/core/shared-1.0.jar"));
      assertNotNull(sharedFile(hopHome, "lib/core/shared-2.0.jar"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void stagedPluginsSharingAJarBothRecordThatTheMarketplaceCreatedIt() throws Exception {
    Map<String, String> a = new LinkedHashMap<>();
    a.put("plugins/tech/test/plugin.jar", "plugin-1");
    a.put("lib/core/common.jar", "common");
    Map<String, String> b = new LinkedHashMap<>();
    b.put("plugins/tech/other/other.jar", "other");
    b.put("lib/core/common.jar", "common");
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", zip(a)));
    serve(server, "hop-other-plugin", "1.0.0", zip(b));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-staged-both");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), false);
      installer.install(new MavenCoordinates("org.apache.hop", "hop-other-plugin", "1.0.0"), false);

      installer.activateAllPending();

      for (String artifactId : List.of(TEST_PLUGIN, "hop-other-plugin")) {
        InstallReceipt.SharedFile common =
            PluginInstaller.readReceipt(hopHome, artifactId)
                .getSharedFiles()
                .get("lib/core/common.jar");
        assertTrue(common.isCreatedByMarketplace(), artifactId);
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void stagedInstallRecordsSharedJarsWhenActivated() throws Exception {
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-staged-shared");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      InstallReceipt staged =
          installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), false);
      assertTrue(staged.getSharedFiles().isEmpty(), "nothing is activated yet");
      assertEquals(4, staged.getChecksums().size());

      installer.activateAllPending();

      InstallReceipt.SharedFile shared = sharedFile(hopHome, "lib/core/shared-1.0.jar");
      assertEquals(InstallReceipt.SharedFileAction.CREATED, shared.getAction());
      assertTrue(shared.isCreatedByMarketplace());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void receiptWrittenByAnOlderVersionLoadsAndIsUpgradedOnUpdate() throws Exception {
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("2.0.0", version2Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-old-receipt");
      Path receipts = hopHome.resolve(PluginInstaller.RECEIPTS_DIR);
      Files.createDirectories(receipts);
      Files.createDirectories(hopHome.resolve("plugins/tech/test/lib"));
      Files.writeString(hopHome.resolve("plugins/tech/test/lib/dep-1.0.jar"), "dep-1.0");
      // As written by Hop 2.20
      Files.writeString(
          receipts.resolve(TEST_PLUGIN + ".json"),
          "{\"groupId\":\"org.apache.hop\",\"artifactId\":\"hop-test-plugin\","
              + "\"version\":\"1.0.0\",\"installedAt\":\"2026-10-01T10:00:00Z\","
              + "\"repositoryId\":\"local\",\"repositoryUrl\":\"http://localhost/\","
              + "\"paths\":[\"plugins/tech/test\",\"plugins/tech/test/lib/dep-1.0.jar\"],"
              + "\"pendingActivation\":false}");

      InstallReceipt old = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
      assertTrue(old.getChecksums().isEmpty());
      assertTrue(old.getSharedFiles().isEmpty());
      assertNull(old.getPreviousVersion());

      InstallReceipt updated =
          new PluginInstaller(
                  new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()))
              .install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), true);
      assertEquals("1.0.0", updated.getPreviousVersion());
      assertEquals(sha256("plugin-2"), updated.getChecksums().get("plugins/tech/test/plugin.jar"));
      assertEquals(
          InstallReceipt.SharedFileAction.CREATED,
          updated.getSharedFiles().get("lib/core/shared-2.0.jar").getAction());
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test/lib/dep-1.0.jar")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void previousVersionRules() {
    assertNull(PluginInstaller.previousVersion(null, "2.0.0"));
    InstallReceipt active = new InstallReceipt();
    active.setVersion("1.0.0");
    active.setPreviousVersion("0.9.0");
    assertEquals("1.0.0", PluginInstaller.previousVersion(active, "2.0.0"));
    assertEquals("0.9.0", PluginInstaller.previousVersion(active, "1.0.0"));
    active.setPendingActivation(true);
    assertEquals("0.9.0", PluginInstaller.previousVersion(active, "2.0.0"));
  }

  private static void install(Path hopHome, HttpServer server, String version) throws Exception {
    new PluginInstaller(
            new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()))
        .install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, version), true);
  }

  @Test
  void stagedPluginWhoseReceiptHasNoVersionStillActivates() throws Exception {
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    try {
      Path hopHome = tempDir.resolve("hop-staged-no-version");
      Files.createDirectories(hopHome.resolve("plugins"));
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), false);
      Files.writeString(
          hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve(TEST_PLUGIN + ".json"),
          "{\"artifactId\":\"hop-test-plugin\",\"pendingActivation\":true}");

      assertDoesNotThrow(installer::activateAllPending);
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
      assertFalse(PluginInstaller.readReceipt(hopHome, TEST_PLUGIN).isPendingActivation());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void leftoverIsRetriedWithoutAStagingFolderAndAReceiptProblemDoesNotFailTheRun()
      throws Exception {
    Path hopHome = tempDir.resolve("hop-leftover-retry");
    Files.createDirectories(hopHome.resolve("plugins"));
    assumeTrue(
        Files.getFileStore(hopHome).supportsFileAttributeView("posix"),
        "needs POSIX permissions to make a delete fail");
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    Path samples = hopHome.resolve("plugins/tech/test/samples");
    Path receiptFile = hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve(TEST_PLUGIN + ".json");
    try {
      PluginInstaller installer =
          new PluginInstaller(
              new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "1.0.0"), true);
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("r-xr-xr-x"));
      assumeFalse(Files.isWritable(samples), "running as a user that ignores permissions");
      installer.install(new MavenCoordinates("org.apache.hop", TEST_PLUGIN, "2.0.0"), true);
      Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));

      // The receipt cannot be rewritten: the retry logs it and the run carries on.
      Files.setPosixFilePermissions(receiptFile, PosixFilePermissions.fromString("r--r--r--"));
      PluginInstaller.deleteRecursive(hopHome.resolve(PluginInstaller.STAGING_DIR));
      assertDoesNotThrow(installer::activateAllPending);
      assertFalse(
          Files.exists(samples.resolve("sample.hpl")),
          "the leftover is retried even when there is no staging folder");
    } finally {
      if (Files.isDirectory(samples)) {
        Files.setPosixFilePermissions(samples, PosixFilePermissions.fromString("rwxr-xr-x"));
      }
      if (Files.exists(receiptFile)) {
        Files.setPosixFilePermissions(receiptFile, PosixFilePermissions.fromString("rw-r--r--"));
      }
      server.stop(0);
    }
  }

  @Test
  void obsoletePathsRules() {
    InstallReceipt previous = new InstallReceipt();
    previous.setPaths(
        List.of(
            "plugins/tech/test",
            "plugins/tech/test/plugin.jar",
            "plugins/tech/test/lib/dep-1.0.jar",
            "plugins/tech/test/lib/shared-with-other.jar",
            "lib/core/shared-1.0.jar"));
    previous.setObsoletePaths(List.of("plugins/tech/test/lib/dep-0.9.jar"));

    List<String> obsolete =
        PluginInstaller.obsoletePaths(
            previous,
            List.of("plugins/tech/test/", "plugins\\tech\\test\\plugin.jar"),
            Set.of("plugins/tech/test/lib/shared-with-other.jar"));

    assertEquals(
        List.of("plugins/tech/test/lib/dep-1.0.jar", "plugins/tech/test/lib/dep-0.9.jar"),
        obsolete);
    assertTrue(PluginInstaller.obsoletePaths(null, List.of("a"), Set.of()).isEmpty());
  }

  private static String localUrl(int port) {
    return "http://127.0.0.1:" + port + "/";
  }

  private static MarketplaceConfig localRepoConfig(int port) {
    MarketplaceConfig config = new MarketplaceConfig();
    config.getRepositories().clear();
    config.getRepositories().add(new MarketplaceRepository("local", localUrl(port), true));
    return config;
  }

  private static HttpServer zipServer(byte[] zipBytes) throws IOException {
    return zipServer(zipBytes, null);
  }

  private static HttpServer zipServer(byte[] zipBytes, AtomicInteger hits) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/org/apache/hop/hop-test-plugin/1.0.0/hop-test-plugin-1.0.0.zip",
        exchange -> {
          if (hits != null) {
            hits.incrementAndGet();
          }
          exchange.getResponseHeaders().add("Content-Type", "application/zip");
          exchange.sendResponseHeaders(200, zipBytes.length);
          exchange.getResponseBody().write(zipBytes);
          exchange.close();
        });
    return server;
  }

  private static byte[] buildPluginZip() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(bos)) {
      zos.putNextEntry(new ZipEntry("plugins/tech/test/plugin.jar"));
      zos.write("fake-jar".getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
      zos.putNextEntry(new ZipEntry("plugins/tech/test/version.xml"));
      zos.write("<version>1.0.0</version>".getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
      // Shared system-classpath jar (assembly provided → lib/core); must install, sticky on
      // uninstall
      zos.putNextEntry(new ZipEntry("lib/core/shared.jar"));
      zos.write("shared-lib".getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }
    return bos.toByteArray();
  }

  private static final String TEST_PLUGIN = "hop-test-plugin";

  /** Serves one zip per version of {@code org.apache.hop:<artifactId>}. */
  private static HttpServer versionServer(String artifactId, Map<String, byte[]> zipsByVersion)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    for (Map.Entry<String, byte[]> entry : zipsByVersion.entrySet()) {
      serve(server, artifactId, entry.getKey(), entry.getValue());
    }
    return server;
  }

  private static void serve(HttpServer server, String artifactId, String version, byte[] zipBytes) {
    server.createContext(
        "/org/apache/hop/" + artifactId + "/" + version + "/" + artifactId + "-" + version + ".zip",
        exchange -> {
          exchange.getResponseHeaders().add("Content-Type", "application/zip");
          exchange.sendResponseHeaders(200, zipBytes.length);
          exchange.getResponseBody().write(zipBytes);
          exchange.close();
        });
  }

  private static String sha256(String content) {
    return HexFormat.of()
        .formatHex(PluginInstaller.sha256().digest(content.getBytes(StandardCharsets.UTF_8)));
  }

  private static InstallReceipt.SharedFile sharedFile(Path hopHome, String path)
      throws HopException {
    return PluginInstaller.readReceipt(hopHome, TEST_PLUGIN).getSharedFiles().get(path);
  }

  private static byte[] version1Zip() throws IOException {
    Map<String, String> entries = new LinkedHashMap<>();
    entries.put("plugins/tech/test/", null);
    entries.put("plugins/tech/test/plugin.jar", "plugin-1");
    entries.put("plugins/tech/test/lib/dep-1.0.jar", "dep-1.0");
    entries.put("plugins/tech/test/samples/", null);
    entries.put("plugins/tech/test/samples/sample.hpl", "<pipeline/>");
    entries.put("lib/core/shared-1.0.jar", "shared-1.0");
    return zip(entries);
  }

  private static byte[] version2Zip() throws IOException {
    Map<String, String> entries = new LinkedHashMap<>();
    entries.put("plugins/tech/test/", null);
    entries.put("plugins/tech/test/plugin.jar", "plugin-2");
    entries.put("plugins/tech/test/lib/dep-2.0.jar", "dep-2.0");
    entries.put("lib/core/shared-2.0.jar", "shared-2.0");
    return zip(entries);
  }

  private static byte[] version3Zip() throws IOException {
    Map<String, String> entries = new LinkedHashMap<>();
    entries.put("plugins/tech/test/plugin.jar", "plugin-3");
    entries.put("plugins/tech/test/lib/dep-3.0.jar", "dep-3.0");
    return zip(entries);
  }

  /** Builds a zip; a null value makes a directory entry. */
  private static byte[] zip(Map<String, String> entries) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(bos)) {
      for (Map.Entry<String, String> entry : entries.entrySet()) {
        zos.putNextEntry(new ZipEntry(entry.getKey()));
        if (entry.getValue() != null) {
          zos.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
        zos.closeEntry();
      }
    }
    return bos.toByteArray();
  }
}
