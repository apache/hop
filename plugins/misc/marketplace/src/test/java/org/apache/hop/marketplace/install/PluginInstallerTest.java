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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PluginInstallerTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @Test
  void sharedCorePaths() {
    assertFalse(PluginInstaller.isSharedCorePath("lib/beam/foo.jar"));
    assertFalse(PluginInstaller.isSharedCorePath("plugins/engines/beam/lib-beam/x.jar"));
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
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/org/apache/hop/hop-test-plugin/1.0.0/hop-test-plugin-1.0.0.zip",
        exchange -> {
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
}
