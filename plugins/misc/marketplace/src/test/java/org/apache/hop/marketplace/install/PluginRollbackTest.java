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
import static org.apache.hop.marketplace.install.PluginInstallerTest.localUrl;
import static org.apache.hop.marketplace.install.PluginInstallerTest.lock;
import static org.apache.hop.marketplace.install.PluginInstallerTest.startHop;
import static org.apache.hop.marketplace.install.PluginInstallerTest.unlock;
import static org.apache.hop.marketplace.install.PluginInstallerTest.version1Zip;
import static org.apache.hop.marketplace.install.PluginInstallerTest.version2Zip;
import static org.apache.hop.marketplace.install.PluginInstallerTest.version3Zip;
import static org.apache.hop.marketplace.install.PluginInstallerTest.versionServer;
import static org.apache.hop.marketplace.install.PluginInstallerTest.zip;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.marketplace.command.MarketplaceCommand;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/** Updates keep the version they replace, put it back when they fail, and roll back to it. */
class PluginRollbackTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  private static MavenCoordinates version(String version) {
    return new MavenCoordinates("org.apache.hop", TEST_PLUGIN, version);
  }

  private PluginInstaller installer(Path hopHome, HttpServer server) {
    return new PluginInstaller(
        new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
  }

  private Path hopHome(String name) throws Exception {
    Path hopHome = tempDir.resolve(name);
    Files.createDirectories(hopHome.resolve("plugins"));
    return hopHome;
  }

  private static Path rollbackZip(Path hopHome, String version) {
    return PluginInstaller.rollbackDir(hopHome, TEST_PLUGIN, version)
        .resolve(PluginInstaller.ROLLBACK_ZIP);
  }

  private static void assertVersion1(Path plugin) throws Exception {
    assertEquals("plugin-1", Files.readString(plugin.resolve("plugin.jar")));
    assertEquals("dep-1.0", Files.readString(plugin.resolve("lib/dep-1.0.jar")));
    assertEquals("<pipeline/>", Files.readString(plugin.resolve("samples/sample.hpl")));
    assertFalse(Files.exists(plugin.resolve("lib/dep-2.0.jar")));
  }

  @Test
  void rollbackPutsBackTheVersionAnUpdateReplacedWithoutDownloadingIt() throws Exception {
    Path hopHome = hopHome("hop-rollback");
    Path plugin = hopHome.resolve("plugins/tech/test");
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    PluginInstaller installer = installer(hopHome, server);
    try {
      installer.install(version("1.0.0"), true);
      assertNull(
          PluginInstaller.rollbackVersion(
              hopHome, PluginInstaller.readReceipt(hopHome, TEST_PLUGIN)),
          "a first install has nothing to roll back to");

      InstallReceipt updated = installer.install(version("2.0.0"), true);
      assertEquals("1.0.0", PluginInstaller.rollbackVersion(hopHome, updated));
      assertTrue(Files.isRegularFile(rollbackZip(hopHome, "2.0.0")));
      assertFalse(Files.exists(plugin.resolve("samples")));
    } finally {
      server.stop(0);
    }

    // The repository is gone: the rollback uses the backup.
    InstallReceipt rolledBack = installer.rollback(TEST_PLUGIN, IInstallListener.NONE);
    assertEquals("1.0.0", rolledBack.getVersion());
    assertEquals("2.0.0", rolledBack.getPreviousVersion());
    assertEquals("local", rolledBack.getRepositoryId(), "the receipt of version 1 is restored");
    assertVersion1(plugin);
    assertTrue(rolledBack.getObsoletePaths().isEmpty());
    assertEquals(
        rolledBack.getVersion(), PluginInstaller.readReceipt(hopHome, TEST_PLUGIN).getVersion());
    // Shared jars stay, as on every update.
    assertTrue(Files.isRegularFile(hopHome.resolve("lib/core/shared-2.0.jar")));
    assertFalse(
        Files.exists(hopHome.resolve(PluginInstaller.STAGING_DIR).resolve(TEST_PLUGIN)),
        "nothing stays staged");

    // The version the rollback replaced is kept in turn, the one it put back is not.
    assertEquals("2.0.0", PluginInstaller.rollbackVersion(hopHome, rolledBack));
    assertFalse(Files.exists(rollbackZip(hopHome, "2.0.0")));

    InstallReceipt again = installer.rollback(TEST_PLUGIN, IInstallListener.NONE);
    assertEquals("2.0.0", again.getVersion());
    assertEquals("plugin-2", Files.readString(plugin.resolve("plugin.jar")));
    assertTrue(Files.isRegularFile(plugin.resolve("lib/dep-2.0.jar")));
    assertFalse(Files.exists(plugin.resolve("lib/dep-1.0.jar")));
    assertFalse(Files.exists(plugin.resolve("samples")));
    assertEquals("1.0.0", PluginInstaller.rollbackVersion(hopHome, again));
  }

  @Test
  void onlyTheVersionTheInstalledOneReplacedIsKept() throws Exception {
    Path hopHome = hopHome("hop-rollback-prune");
    HttpServer server =
        versionServer(
            TEST_PLUGIN,
            Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip(), "3.0.0", version3Zip()));
    server.start();
    try {
      PluginInstaller installer = installer(hopHome, server);
      installer.install(version("1.0.0"), true);
      installer.install(version("2.0.0"), true);
      // A shared lib/core backup of the install of version 2 is not a rollback backup.
      Path sharedBackup =
          hopHome
              .resolve(PluginInstaller.BACKUP_DIR)
              .resolve(TEST_PLUGIN)
              .resolve("2.0.0/20260101T000000000Z/lib/core/shared.jar");
      Files.createDirectories(sharedBackup.getParent());
      Files.writeString(sharedBackup, "client copy");

      InstallReceipt receipt = installer.install(version("3.0.0"), true);

      assertEquals("2.0.0", PluginInstaller.rollbackVersion(hopHome, receipt));
      assertTrue(Files.isRegularFile(rollbackZip(hopHome, "3.0.0")));
      assertFalse(Files.exists(rollbackZip(hopHome, "2.0.0")));
      assertFalse(
          Files.exists(
              PluginInstaller.rollbackDir(hopHome, TEST_PLUGIN, "2.0.0")
                  .resolve(PluginInstaller.ROLLBACK_RECEIPT)));
      assertEquals("client copy", Files.readString(sharedBackup));

      // Installing the same version again keeps what it can roll back to.
      Files.delete(hopHome.resolve("plugins/tech/test/lib/dep-3.0.jar"));
      InstallReceipt reinstalled = installer.install(version("3.0.0"), true);
      assertEquals("2.0.0", reinstalled.getPreviousVersion());
      assertEquals("2.0.0", PluginInstaller.rollbackVersion(hopHome, reinstalled));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void aFailedUpdatePutsThePreviousVersionBack() throws Exception {
    Path hopHome = hopHome("hop-failed-update");
    Path plugin = hopHome.resolve("plugins/tech/test");
    Map<String, String> broken = new LinkedHashMap<>();
    broken.put("plugins/tech/test/plugin.jar", "plugin-2");
    broken.put("plugins/tech/test/lib/dep-2.0.jar", "dep-2.0");
    broken.put("plugins/tech/test/conf/settings.xml", "<settings/>");
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", zip(broken)));
    server.start();
    try {
      PluginInstaller installer = installer(hopHome, server);
      installer.install(version("1.0.0"), true);
      // A file where version 2 needs a folder: activation fails after plugin.jar and the new
      // jar were copied.
      Files.writeString(plugin.resolve("conf"), "not a folder");

      HopException e =
          assertThrows(HopException.class, () -> installer.install(version("2.0.0"), true));

      assertTrue(e.getMessage().contains("Hop put version 1.0.0 back"), e.getMessage());
      assertVersion1(plugin);
      InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
      assertEquals("1.0.0", receipt.getVersion());
      assertFalse(receipt.isPendingActivation());
      assertTrue(receipt.getObsoletePaths().isEmpty(), receipt.getObsoletePaths().toString());
      assertFalse(Files.exists(hopHome.resolve(PluginInstaller.STAGING_DIR).resolve(TEST_PLUGIN)));
      assertFalse(Files.exists(hopHome.resolve(PluginInstaller.STAGING_DIR).resolve(".restore")));
      assertEquals("not a folder", Files.readString(plugin.resolve("conf")));
      assertFalse(
          Files.exists(PluginInstaller.rollbackDir(hopHome, TEST_PLUGIN, "2.0.0")),
          "version 1 is installed again, so its backup for the failed install is gone");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void uninstallRemovesTheKeptVersion() throws Exception {
    Path hopHome = hopHome("hop-rollback-uninstall");
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    try {
      PluginInstaller installer = installer(hopHome, server);
      installer.install(version("1.0.0"), true);
      installer.install(version("2.0.0"), true);
      assertTrue(Files.isRegularFile(rollbackZip(hopHome, "2.0.0")));

      new PluginUninstaller(new LogChannel("test"), hopHome).uninstall(TEST_PLUGIN);

      assertFalse(
          Files.exists(hopHome.resolve(PluginInstaller.BACKUP_DIR).resolve(TEST_PLUGIN)),
          "nothing is left to roll back to");
      HopException e =
          assertThrows(
              HopException.class, () -> installer.rollback(TEST_PLUGIN, IInstallListener.NONE));
      assertTrue(e.getMessage().contains("No marketplace install"), e.getMessage());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void rollbackWithoutAnEarlierVersionChangesNothing() throws Exception {
    Path hopHome = hopHome("hop-rollback-none");
    HttpServer server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip()));
    server.start();
    try {
      PluginInstaller installer = installer(hopHome, server);
      installer.install(version("1.0.0"), true);

      HopException e =
          assertThrows(
              HopException.class, () -> installer.rollback(TEST_PLUGIN, IInstallListener.NONE));

      assertTrue(e.getMessage().contains("no earlier version"), e.getMessage());
      assertVersion1(hopHome.resolve("plugins/tech/test"));
      assertEquals("1.0.0", PluginInstaller.readReceipt(hopHome, TEST_PLUGIN).getVersion());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void windowsRollbackOfAPluginInUseFinishesAtTheNextStart() throws Exception {
    assumeLauncherScript();
    Path hopHome = hopHome("hop-windows-rollback");
    assumePosix(hopHome);
    Path plugin = hopHome.resolve("plugins/tech/test");
    Path lib = plugin.resolve("lib");
    HttpServer server =
        versionServer(
            TEST_PLUGIN,
            Map.of(
                "1.0.0",
                zip(
                    Map.of(
                        "plugins/tech/test/plugin.jar",
                        "plugin-1",
                        "plugins/tech/test/lib/dep.jar",
                        "dep-1")),
                "2.0.0",
                zip(
                    Map.of(
                        "plugins/tech/test/plugin.jar",
                        "plugin-2",
                        "plugins/tech/test/lib/dep.jar",
                        "dep-2"))));
    server.start();
    PluginInstaller installer = installer(hopHome, server);
    installer.setFilesLockedWhileInUse(true);
    try {
      installer.install(version("1.0.0"), true);
      installer.install(version("2.0.0"), true);
    } finally {
      server.stop(0);
    }
    try {
      // Hop runs version 2: its jars are in use.
      lock(plugin, lib);
      assumeFalse(Files.isWritable(plugin), "running as a user that ignores permissions");

      InstallReceipt rolledBack = installer.rollback(TEST_PLUGIN, IInstallListener.NONE);

      assertEquals("1.0.0", rolledBack.getVersion());
      assertEquals("plugin-2", Files.readString(plugin.resolve("plugin.jar")));
      assertEquals(
          List.of("Replace plugins/tech/test/lib/dep.jar", "Replace plugins/tech/test/plugin.jar"),
          new PendingChanges(hopHome).describe().stream().sorted().toList());

      unlock(plugin, lib);
      startHop(hopHome);
      assertEquals("plugin-1", Files.readString(plugin.resolve("plugin.jar")));
      assertEquals("dep-1", Files.readString(lib.resolve("dep.jar")));
      assertEquals("2.0.0", PluginInstaller.rollbackVersion(hopHome, rolledBack));
    } finally {
      unlock(plugin, lib);
    }
  }

  @Test
  void updateListAndRollbackCommands() throws Exception {
    Path hopHome = hopHome("hop-update-commands");
    Path plugin = hopHome.resolve("plugins/tech/test");
    HttpServer server =
        versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    Path catalog = tempDir.resolve("catalog.yaml");
    Files.writeString(
        catalog,
        """
        schemaVersion: "1.0"
        kind: hop-marketplace-catalog
        plugins:
          - groupId: org.apache.hop
            artifactId: hop-test-plugin
            version: 1.0.0
          - groupId: org.apache.hop
            artifactId: hop-test-plugin
            version: 2.0.0
        """,
        StandardCharsets.UTF_8);
    String userDir = System.getProperty("user.dir");
    try {
      installer(hopHome, server).install(version("1.0.0"), true);
      System.setProperty("user.dir", hopHome.toAbsolutePath().toString());
      withMarketplaceRepository(
          localUrl(server.getAddress().getPort()),
          catalog.toString(),
          () -> {
            String outdated = run(new MarketplaceCommand.ListCommand(), "--outdated");
            assertTrue(outdated.contains(TEST_PLUGIN), outdated);
            assertTrue(outdated.matches("(?s).*1\\.0\\.0\\s*\\|?\\s*2\\.0\\.0.*"), outdated);

            run(new MarketplaceCommand.UpdateCommand(), TEST_PLUGIN);
            assertEquals("2.0.0", PluginInstaller.readReceipt(hopHome, TEST_PLUGIN).getVersion());
            assertEquals("plugin-2", Files.readString(plugin.resolve("plugin.jar")));

            String upToDate = run(new MarketplaceCommand.UpdateCommand(), "--all");
            assertTrue(upToDate.contains(TEST_PLUGIN + " is up to date (2.0.0)"), upToDate);
            String none = run(new MarketplaceCommand.ListCommand(), "--outdated");
            assertTrue(none.contains("up to date"), none);

            String rolledBack = run(new MarketplaceCommand.RollbackCommand(), TEST_PLUGIN);
            assertTrue(rolledBack.contains("from 2.0.0 to 1.0.0"), rolledBack);
            assertVersion1(plugin);
          });
    } finally {
      if (userDir != null) {
        System.setProperty("user.dir", userDir);
      }
      server.stop(0);
    }
  }

  @Test
  void updateNeedsArtifactIdsOrAll() {
    MarketplaceCommand.UpdateCommand cmd = new MarketplaceCommand.UpdateCommand();
    new CommandLine(cmd).parseArgs();
    assertThrows(CommandLine.ParameterException.class, cmd::run);
    MarketplaceCommand.UpdateCommand both = new MarketplaceCommand.UpdateCommand();
    new CommandLine(both).parseArgs("--all", TEST_PLUGIN);
    assertThrows(CommandLine.ParameterException.class, both::run);
  }

  /** Run a command and return what it printed. */
  private static String run(Runnable command, String... args) {
    new CommandLine(command).parseArgs(args);
    PrintStream out = System.out;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
      command.run();
    } finally {
      System.setOut(out);
    }
    return captured.toString(StandardCharsets.UTF_8);
  }

  /** A hop-config marketplace section with one browsable repository that has a catalog. */
  private static void withMarketplaceRepository(
      String url, String catalogUrl, ThrowingRunnable action) throws Exception {
    Field field = ConfigFile.class.getDeclaredField("configMap");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) field.get(HopConfig.getInstance());
    Object previous = map.get(MarketplaceConfig.CONFIG_KEY);
    Map<String, Object> repository = new LinkedHashMap<>();
    repository.put("id", "local");
    repository.put("url", url);
    repository.put("enabled", true);
    repository.put("primary", true);
    repository.put("browse", true);
    repository.put("catalogUrl", catalogUrl);
    Map<String, Object> marketplace = new LinkedHashMap<>();
    marketplace.put("enabled", true);
    marketplace.put("groupId", "org.apache.hop");
    marketplace.put("repositories", List.of(repository));
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
}
