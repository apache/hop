/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.marketplace.install;

import static org.apache.hop.marketplace.install.PluginInstallerTest.TEST_PLUGIN;
import static org.apache.hop.marketplace.install.PluginInstallerTest.localRepoConfig;
import static org.apache.hop.marketplace.install.PluginInstallerTest.version1Zip;
import static org.apache.hop.marketplace.install.PluginInstallerTest.version2Zip;
import static org.apache.hop.marketplace.install.PluginInstallerTest.versionServer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.marketplace.command.MarketplaceCommand;
import org.apache.hop.marketplace.install.InstallReceipt.SharedFile;
import org.apache.hop.marketplace.install.InstallReceipt.SharedFileAction;
import org.apache.hop.marketplace.install.PluginVerifier.FileFinding;
import org.apache.hop.marketplace.install.PluginVerifier.Finding;
import org.apache.hop.marketplace.install.PluginVerifier.Result;
import org.apache.hop.marketplace.install.PluginVerifier.State;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/** Verify compares installs with their receipts; reinstall puts the packaged files back. */
class PluginVerifyTest {

  private static final String PLUGIN_JAR = "plugins/tech/test/plugin.jar";
  private static final String DEP_JAR = "plugins/tech/test/lib/dep-1.0.jar";
  private static final String SHARED_JAR = "lib/core/shared-1.0.jar";

  @TempDir Path tempDir;

  private Path hopHome;
  private HttpServer server;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @BeforeEach
  void installVersion1() throws Exception {
    hopHome = tempDir.resolve("hop");
    Files.createDirectories(hopHome.resolve("plugins"));
    server = versionServer(TEST_PLUGIN, Map.of("1.0.0", version1Zip(), "2.0.0", version2Zip()));
    server.start();
    installer().install(version("1.0.0"), true);
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private PluginInstaller installer() {
    return new PluginInstaller(
        new LogChannel("test"), hopHome, localRepoConfig(server.getAddress().getPort()));
  }

  private PluginVerifier verifier() {
    return new PluginVerifier(new LogChannel("test"), hopHome);
  }

  private static MavenCoordinates version(String version) {
    return new MavenCoordinates("org.apache.hop", TEST_PLUGIN, version);
  }

  @Test
  void anInstallAsItWasWrittenIsOk() throws Exception {
    Result result = verifier().verify(TEST_PLUGIN);
    assertEquals(State.OK, result.state());
    assertEquals("1.0.0", result.version());
    assertFalse(result.withoutChecksums());
    assertEquals(List.of(), result.files());
  }

  @Test
  void missingAndModifiedFilesDamageTheInstallAndReinstallRepairsIt() throws Exception {
    Files.delete(hopHome.resolve(DEP_JAR));
    Files.writeString(hopHome.resolve(PLUGIN_JAR), "patched by hand");
    // A file somebody added to the plugin folder
    Files.writeString(hopHome.resolve("plugins/tech/test/lib/driver.jar"), "driver");

    Result result = verifier().verify(TEST_PLUGIN);
    assertEquals(State.DAMAGED, result.state());
    assertTrue(result.isDamaged());
    assertEquals(List.of(DEP_JAR), result.paths(Finding.MISSING));
    assertEquals(List.of(PLUGIN_JAR), result.paths(Finding.MODIFIED));
    assertEquals(List.of("plugins/tech/test/lib/driver.jar"), result.paths(Finding.UNKNOWN));

    InstallReceipt receipt = installer().reinstall(TEST_PLUGIN, IInstallListener.NONE);
    assertEquals("1.0.0", receipt.getVersion());
    assertEquals("plugin-1", Files.readString(hopHome.resolve(PLUGIN_JAR)));
    assertEquals("dep-1.0", Files.readString(hopHome.resolve(DEP_JAR)));
    // Not a file of the plugin: left alone
    assertTrue(Files.exists(hopHome.resolve("plugins/tech/test/lib/driver.jar")));
    Result after = verifier().verify(TEST_PLUGIN);
    assertEquals(State.OK, after.state());
    assertEquals(List.of("plugins/tech/test/lib/driver.jar"), after.paths(Finding.UNKNOWN));
  }

  @Test
  void reinstallKeepsTheVersionToRollBackTo() throws Exception {
    installer().install(version("2.0.0"), true);
    Files.writeString(hopHome.resolve(PLUGIN_JAR), "patched by hand");
    // Every file is there, so installing the same version skips the download
    assertTrue(installer().install(version("2.0.0"), true).isAlreadyPresent());
    assertEquals("patched by hand", Files.readString(hopHome.resolve(PLUGIN_JAR)));

    InstallReceipt receipt = installer().reinstall(TEST_PLUGIN, IInstallListener.NONE);
    assertFalse(receipt.isAlreadyPresent());
    assertEquals("2.0.0", receipt.getVersion());
    assertEquals("1.0.0", receipt.getPreviousVersion());
    assertEquals("plugin-2", Files.readString(hopHome.resolve(PLUGIN_JAR)));
    assertEquals("1.0.0", PluginInstaller.rollbackVersion(hopHome, receipt));

    installer().rollback(TEST_PLUGIN, IInstallListener.NONE);
    assertEquals("plugin-1", Files.readString(hopHome.resolve(PLUGIN_JAR)));
  }

  @Test
  void reinstallNeedsAnActiveMarketplaceInstall() throws Exception {
    HopException notInstalled =
        assertThrows(
            HopException.class, () -> installer().reinstall("hop-other", IInstallListener.NONE));
    assertTrue(notInstalled.getMessage().contains("No marketplace install of 'hop-other'"));

    installer().install(version("2.0.0"), false);
    HopException staged =
        assertThrows(
            HopException.class, () -> installer().reinstall(TEST_PLUGIN, IInstallListener.NONE));
    assertTrue(staged.getMessage().contains("staged and not active yet"), staged.getMessage());
    assertEquals(State.PENDING_ACTIVATION, verifier().verify(TEST_PLUGIN).state());
  }

  @Test
  void aSharedJarAnotherInstallWroteIsExpected() throws Exception {
    Files.writeString(hopHome.resolve(SHARED_JAR), "shared-1.0 of another plugin");
    InstallReceipt other = new InstallReceipt();
    other.setGroupId("org.apache.hop");
    other.setArtifactId("hop-other");
    other.setVersion("3.0.0");
    other.setPaths(List.of(SHARED_JAR));
    other.setChecksums(Map.of(SHARED_JAR, sha256("shared-1.0 of another plugin")));
    PluginInstaller.writeReceipt(hopHome, other);

    Result result = verifier().verify(TEST_PLUGIN);
    assertEquals(State.OK, result.state());
    assertEquals(
        List.of(new FileFinding(SHARED_JAR, Finding.SHARED_BY_OTHER, "hop-other")), result.files());

    // Content no receipt explains
    Files.writeString(hopHome.resolve(SHARED_JAR), "patched by hand");
    Result patched = verifier().verify(TEST_PLUGIN);
    assertEquals(State.DAMAGED, patched.state());
    assertEquals(List.of(SHARED_JAR), patched.paths(Finding.MODIFIED));
  }

  @Test
  void aSharedJarTheInstallKeptIsExpected() throws Exception {
    InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
    receipt
        .getSharedFiles()
        .put(SHARED_JAR, new SharedFile(SharedFileAction.KEPT, null, false, null));
    PluginInstaller.writeReceipt(hopHome, receipt);
    Files.writeString(hopHome.resolve(SHARED_JAR), "the client's copy");

    Result result = verifier().verify(TEST_PLUGIN);
    assertEquals(State.OK, result.state());
    assertEquals(List.of(SHARED_JAR), result.paths(Finding.SHARED_KEPT));
  }

  @Test
  void aChangeForTheNextStartCountsAsMade() throws Exception {
    // Windows: the jar was in use, the next start copies the new content over it
    Path newContent = tempDir.resolve("plugin.jar");
    Files.writeString(newContent, "plugin-1");
    Files.writeString(hopHome.resolve(PLUGIN_JAR), "the jar of the version before");
    new PendingChanges(hopHome).queueCopy(newContent, hopHome.resolve(PLUGIN_JAR));
    new PendingChanges(hopHome).queueDelete(hopHome.resolve(DEP_JAR));

    Result result = verifier().verify(TEST_PLUGIN);
    assertEquals(List.of(), result.paths(Finding.MODIFIED));
    assertEquals(List.of(DEP_JAR), result.paths(Finding.MISSING));
  }

  @Test
  void aReceiptWithoutChecksumsOnlyChecksThatFilesAreThere() throws Exception {
    InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
    receipt.setChecksums(Map.of());
    PluginInstaller.writeReceipt(hopHome, receipt);
    Files.writeString(hopHome.resolve(PLUGIN_JAR), "patched by hand");

    Result result = verifier().verify(TEST_PLUGIN);
    assertEquals(State.OK, result.state());
    assertTrue(result.withoutChecksums());

    Files.delete(hopHome.resolve(DEP_JAR));
    assertEquals(List.of(DEP_JAR), verifier().verify(TEST_PLUGIN).paths(Finding.MISSING));

    // A reinstall records them
    installer().reinstall(TEST_PLUGIN, IInstallListener.NONE);
    Result after = verifier().verify(TEST_PLUGIN);
    assertEquals(State.OK, after.state());
    assertFalse(after.withoutChecksums());
  }

  @Test
  void leftoversOfTheVersionBeforeAreNotDamage() throws Exception {
    Path leftover = hopHome.resolve("plugins/tech/test/lib/dep-0.9.jar");
    Files.writeString(leftover, "dep-0.9");
    InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
    receipt.setObsoletePaths(List.of("plugins/tech/test/lib/dep-0.9.jar"));
    PluginInstaller.writeReceipt(hopHome, receipt);

    Result result = verifier().verify(TEST_PLUGIN);
    assertEquals(State.OK, result.state());
    assertEquals(
        List.of(new FileFinding("plugins/tech/test/lib/dep-0.9.jar", Finding.OBSOLETE, null)),
        result.files());
  }

  @Test
  void verifyAllReportsUnreadableReceiptsAndRefusesUnknownPlugins() throws Exception {
    Files.writeString(
        hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-broken.json"), "{ not json");

    List<Result> results = verifier().verifyAll();
    assertEquals(
        List.of("hop-broken", TEST_PLUGIN), results.stream().map(Result::artifactId).toList());
    assertEquals(State.UNREADABLE, results.get(0).state());
    assertTrue(results.get(0).error().contains("cannot be read"), results.get(0).error());
    assertEquals(State.OK, results.get(1).state());

    HopException unknown =
        assertThrows(HopException.class, () -> verifier().verify("hop-not-installed"));
    assertTrue(unknown.getMessage().contains("No marketplace install of 'hop-not-installed'"));
  }

  @Test
  void aReceiptPathOutsideTheInstallationIsIgnored() throws Exception {
    Files.writeString(tempDir.resolve("outside.jar"), "outside");
    InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, TEST_PLUGIN);
    List<String> paths = new ArrayList<>(receipt.getPaths());
    paths.add("../outside.jar");
    receipt.setPaths(paths);
    receipt.getChecksums().put("../outside.jar", sha256("something else"));
    PluginInstaller.writeReceipt(hopHome, receipt);

    assertEquals(State.OK, verifier().verify(TEST_PLUGIN).state());
  }

  @Test
  void verifyCommandFailsOnDamageAndNamesTheReinstall() throws Exception {
    String userDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", hopHome.toAbsolutePath().toString());
      String ok = run(new MarketplaceCommand.VerifyCommand());
      assertTrue(ok.contains(TEST_PLUGIN + " 1.0.0: OK"), ok);

      Files.delete(hopHome.resolve(DEP_JAR));
      String damaged = runFailing(new MarketplaceCommand.VerifyCommand(), TEST_PLUGIN);
      assertTrue(damaged.contains(TEST_PLUGIN + " 1.0.0: missing or modified files"), damaged);
      assertTrue(damaged.contains("missing  " + DEP_JAR), damaged);
      assertTrue(damaged.contains("'hop marketplace reinstall " + TEST_PLUGIN + "'"), damaged);

      // An unknown plugin fails, the others are still verified
      String unknown =
          runFailing(new MarketplaceCommand.VerifyCommand(), "hop-not-installed", TEST_PLUGIN);
      assertTrue(unknown.contains(TEST_PLUGIN + " 1.0.0: missing or modified files"), unknown);
    } finally {
      System.setProperty("user.dir", userDir);
    }
  }

  @Test
  void reinstallCommandOptionsAreParsed() {
    MarketplaceCommand.ReinstallCommand none = new MarketplaceCommand.ReinstallCommand();
    assertThrows(CommandLine.ParameterException.class, () -> new CommandLine(none).parseArgs());
  }

  private static String sha256(String content) {
    return HexFormat.of()
        .formatHex(PluginInstaller.sha256().digest(content.getBytes(StandardCharsets.UTF_8)));
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

  /** Run a command that fails, and return what it printed before it did. */
  private static String runFailing(Runnable command, String... args) {
    new CommandLine(command).parseArgs(args);
    PrintStream out = System.out;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
      assertThrows(CommandLine.ExecutionException.class, command::run);
    } finally {
      System.setOut(out);
    }
    return captured.toString(StandardCharsets.UTF_8);
  }
}
