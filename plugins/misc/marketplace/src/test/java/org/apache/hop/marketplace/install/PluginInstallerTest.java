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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
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
  void protectedPaths() {
    assertTrue(PluginInstaller.isProtectedPath("lib/beam/foo.jar"));
    assertTrue(PluginInstaller.isProtectedPath("lib/core/bar.jar"));
    assertFalse(PluginInstaller.isProtectedPath("plugins/engines/beam/hop.jar"));
    assertFalse(PluginInstaller.isProtectedPath("plugins/tech/parquet/lib/x.jar"));
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
      config
          .getRepositories()
          .add(new MarketplaceRepository("local", "http://127.0.0.1:" + port + "/"));

      LogChannel log = new LogChannel("test");
      PluginInstaller installer = new PluginInstaller(log, hopHome, config);
      MavenCoordinates coords = new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0");
      installer.install(coords, true);

      Path pluginJar = hopHome.resolve("plugins/tech/test/plugin.jar");
      assertTrue(Files.isRegularFile(pluginJar));
      assertTrue(
          Files.isRegularFile(
              hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-test-plugin.json")));
      // protected path from zip must not be written
      assertFalse(Files.exists(hopHome.resolve("lib/beam/evil.jar")));

      new PluginUninstaller(log, hopHome).uninstall("hop-test-plugin");
      assertFalse(Files.exists(pluginJar));
      assertFalse(
          Files.exists(
              hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-test-plugin.json")));
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
      config
          .getRepositories()
          .add(
              new MarketplaceRepository(
                  "local", "http://127.0.0.1:" + port + "/", "admin", "s3cret"));

      new PluginInstaller(new LogChannel("test"), hopHome, config)
          .install(new MavenCoordinates("org.apache.hop", "hop-test-plugin", "1.0.0"), true);
      assertTrue(Files.isRegularFile(hopHome.resolve("plugins/tech/test/plugin.jar")));
    } finally {
      server.stop(0);
    }
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
      // Should be ignored on activate
      zos.putNextEntry(new ZipEntry("lib/beam/evil.jar"));
      zos.write("nope".getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }
    return bos.toByteArray();
  }
}
