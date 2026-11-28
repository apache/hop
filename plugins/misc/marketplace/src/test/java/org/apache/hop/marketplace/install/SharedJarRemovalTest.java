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

import static org.apache.hop.marketplace.install.PluginInstallerTest.TEST_PLUGIN;
import static org.apache.hop.marketplace.install.PluginInstallerTest.assumeLauncherScript;
import static org.apache.hop.marketplace.install.PluginInstallerTest.assumePosix;
import static org.apache.hop.marketplace.install.PluginInstallerTest.localRepoConfig;
import static org.apache.hop.marketplace.install.PluginInstallerTest.lock;
import static org.apache.hop.marketplace.install.PluginInstallerTest.serve;
import static org.apache.hop.marketplace.install.PluginInstallerTest.startHop;
import static org.apache.hop.marketplace.install.PluginInstallerTest.unlock;
import static org.apache.hop.marketplace.install.PluginInstallerTest.version1Zip;
import static org.apache.hop.marketplace.install.PluginInstallerTest.versionServer;
import static org.apache.hop.marketplace.install.PluginInstallerTest.windowsInstaller;
import static org.apache.hop.marketplace.install.PluginInstallerTest.writeOtherReceipt;
import static org.apache.hop.marketplace.install.PluginInstallerTest.zip;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.sun.net.httpserver.HttpServer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A shared {@code lib/core} jar goes when the marketplace created it and no other installed plugin
 * lists it (#9614).
 */
class SharedJarRemovalTest {

  private static final String OTHER_PLUGIN = "hop-other-plugin";
  private static final String SHARED = "lib/core/shared-1.0.jar";

  @TempDir Path tempDir;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  private Path hopHome(String name) throws Exception {
    Path hopHome = tempDir.resolve(name);
    Files.createDirectories(hopHome.resolve("plugins"));
    return hopHome;
  }

  private static PluginInstaller installer(Path hopHome, HttpServer server) {
    return new PluginInstaller(
        new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
  }

  private static void install(PluginInstaller installer, String artifactId) throws Exception {
    installer.install(new MavenCoordinates("org.apache.hop", artifactId, "1.0.0"), true);
  }

  private static void uninstall(Path hopHome, String artifactId) throws Exception {
    new PluginUninstaller(new LogChannel("test"), hopHome).uninstall(artifactId);
  }

  /** Another plugin that ships the same shared jar as version 1 of the test plugin. */
  private static byte[] otherPluginZip() throws Exception {
    Map<String, String> entries = new LinkedHashMap<>();
    entries.put("plugins/tech/other/plugin.jar", "other");
    entries.put(SHARED, "shared-1.0");
    return zip(entries);
  }

  @Test
  void aJarAnotherPluginShipsStaysUntilThatOneIsUninstalledToo() throws Exception {
    Path hopHome = hopHome("hop-two-plugins");
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    serve(server, OTHER_PLUGIN, "1.0.0", otherPluginZip());
    server.start();
    try {
      PluginInstaller installer = installer(hopHome, server);
      install(installer, TEST_PLUGIN);
      install(installer, OTHER_PLUGIN);

      uninstall(hopHome, TEST_PLUGIN);
      assertTrue(Files.isRegularFile(hopHome.resolve(SHARED)), OTHER_PLUGIN + " lists it");
      assertNull(PluginInstaller.readReceipt(hopHome, TEST_PLUGIN));

      uninstall(hopHome, OTHER_PLUGIN);
      assertFalse(Files.exists(hopHome.resolve(SHARED)));
      assertTrue(Files.isDirectory(hopHome.resolve("lib/core")), "the folder stays");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void jarsTheClientShippedStay() throws Exception {
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    try {
      for (String content : List.of("shared-1.0", "the client's own version")) {
        Path hopHome = hopHome("hop-client-jar-" + content.length());
        Files.createDirectories(hopHome.resolve("lib/core"));
        Files.writeString(hopHome.resolve(SHARED), content);
        install(installer(hopHome, server), TEST_PLUGIN);

        uninstall(hopHome, TEST_PLUGIN);
        assertTrue(Files.isRegularFile(hopHome.resolve(SHARED)), content);
        assertTrue(new PendingChanges(hopHome).isEmpty());
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void aReceiptThatDoesNotRecordWhoCreatedTheJarKeepsIt() throws Exception {
    Path hopHome = hopHome("hop-old-receipt");
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    try {
      install(installer(hopHome, server), TEST_PLUGIN);
      // As Hop before 2.21 wrote it
      InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
      receipt.setSharedFiles(new LinkedHashMap<>());
      HopJson.newMapper()
          .writeValue(
              hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve(TEST_PLUGIN + ".json").toFile(),
              receipt);

      uninstall(hopHome, TEST_PLUGIN);
      assertTrue(Files.isRegularFile(hopHome.resolve(SHARED)));
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void aJarThatAnOlderReceiptListsStays() throws Exception {
    Path hopHome = hopHome("hop-listed-by-old-receipt");
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    try {
      install(installer(hopHome, server), TEST_PLUGIN);
      writeOtherReceipt(hopHome, SHARED);

      uninstall(hopHome, TEST_PLUGIN);
      assertTrue(Files.isRegularFile(hopHome.resolve(SHARED)));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void anUnreadableReceiptKeepsEverySharedJar() throws Exception {
    Path hopHome = hopHome("hop-unreadable-receipt");
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    try {
      install(installer(hopHome, server), TEST_PLUGIN);
      Files.writeString(
          hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve(OTHER_PLUGIN + ".json"), "{");

      uninstall(hopHome, TEST_PLUGIN);
      assertTrue(Files.isRegularFile(hopHome.resolve(SHARED)), "the unreadable one may list it");
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test")), "the plugin itself goes");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void webLayoutRemovesTheJarFromWebInfLib() throws Exception {
    Path hopHome = hopHome("tomcat");
    Path webLib = hopHome.resolve("webapps/ROOT/WEB-INF/lib");
    Files.createDirectories(webLib);
    Files.writeString(webLib.resolve("hop-core.jar"), "shipped with Hop Web");
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    try {
      install(installer(hopHome, server), TEST_PLUGIN);
      assertTrue(Files.isRegularFile(webLib.resolve("shared-1.0.jar")));

      uninstall(hopHome, TEST_PLUGIN);
      assertFalse(Files.exists(webLib.resolve("shared-1.0.jar")));
      assertTrue(Files.isRegularFile(webLib.resolve("hop-core.jar")));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void windowsRemovesASharedJarInUseAtTheNextStart() throws Exception {
    assumeLauncherScript();
    Path hopHome = hopHome("hop-windows-shared-uninstall");
    assumePosix(hopHome);
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    Path lib = hopHome.resolve("lib/core");
    try {
      install(windowsInstaller(hopHome, server), TEST_PLUGIN);
      lock(lib);
      assumeFalse(Files.isWritable(lib), "running as a user that ignores permissions");

      new PluginUninstaller(new LogChannel("test"), hopHome, true).uninstall(TEST_PLUGIN);

      assertTrue(Files.isRegularFile(hopHome.resolve(SHARED)));
      assertEquals(List.of("Remove " + SHARED), new PendingChanges(hopHome).describe());
      assertNull(PluginInstaller.readReceipt(hopHome, TEST_PLUGIN), "nothing left to retry");

      unlock(lib);
      startHop(hopHome);
      assertFalse(Files.exists(hopHome.resolve(SHARED)));
    } finally {
      unlock(lib);
      server.stop(0);
    }
  }

  @Test
  void aSharedJarThatCannotBeRemovedIsRetried() throws Exception {
    Path hopHome = hopHome("hop-shared-locked");
    assumePosix(hopHome);
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    Path lib = hopHome.resolve("lib/core");
    try {
      PluginInstaller installer = installer(hopHome, server);
      install(installer, TEST_PLUGIN);
      lock(lib);
      assumeFalse(Files.isWritable(lib), "running as a user that ignores permissions");

      uninstall(hopHome, TEST_PLUGIN);

      assertTrue(Files.isRegularFile(hopHome.resolve(SHARED)));
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/test")));
      InstallReceipt left = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
      assertTrue(left.isUninstallPending());
      assertEquals(
          InstallReceipt.SharedFileAction.NOT_SHIPPED,
          left.getSharedFiles().get(SHARED).getAction());
      assertTrue(new PendingChanges(hopHome).isEmpty(), "only Windows waits for the next start");

      unlock(lib);
      installer.activateAllPending();
      assertFalse(Files.exists(hopHome.resolve(SHARED)));
      assertNull(PluginInstaller.readReceipt(hopHome, TEST_PLUGIN));
    } finally {
      unlock(lib);
      server.stop(0);
    }
  }
}
