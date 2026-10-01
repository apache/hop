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

package org.apache.hop.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IClassLoadingPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * What {@code hop driver install <id>} says when it has nothing to install. "Unknown driver id"
 * only fits an id no database type goes by; a database type that simply declares no download needs
 * to be told apart from one whose driver already ships with Hop.
 */
class DriverInstallCommandTest {

  private static final int EXIT_OK = 0;
  private static final int EXIT_UNKNOWN_DRIVER = 3;
  private static final int EXIT_NO_DOWNLOAD = 4;

  /** A database plugin the registry accepts and whose class it asks the plugin itself for. */
  interface DatabasePluginMock extends IClassLoadingPlugin, IPlugin {}

  private final List<DatabasePluginMock> registeredPlugins = new ArrayList<>();
  private ByteArrayOutputStream out;
  private ByteArrayOutputStream err;
  private PrintStream originalOut;
  private PrintStream originalErr;

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void capture() {
    originalOut = System.out;
    originalErr = System.err;
    out = new ByteArrayOutputStream();
    err = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
    System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
  }

  @AfterEach
  void restore() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    // The plugin registry is process-global, so a plugin registered here would otherwise be
    // visible to every later test in this JVM.
    registeredPlugins.forEach(
        plugin -> PluginRegistry.getInstance().removePlugin(DatabasePluginType.class, plugin));
    registeredPlugins.clear();
  }

  @Test
  void anIdNoDatabaseTypeGoesByIsUnknown() throws HopException {
    registerDatabase("TESTMANUAL", "Test Manual", noDownload("com.example.NoSuchDriver"));

    assertEquals(EXIT_UNKNOWN_DRIVER, install("nosuchdatabase"));
    assertTrue(stderr().contains("Unknown driver id: 'nosuchdatabase'"), stderr());
    assertTrue(stderr().contains("hop driver list"), stderr());
  }

  @Test
  void aNearMissGetsTheIdItAlmostTyped() throws HopException {
    registerDatabase("TESTMANUAL", "Test Manual", noDownload("com.example.NoSuchDriver"));

    assertEquals(EXIT_UNKNOWN_DRIVER, install("testmanu"));
    assertTrue(stderr().contains("Did you mean: testmanual?"), stderr());
  }

  @Test
  void aDatabaseTypeWithoutADownloadSaysSoAndPointsAtTheJdbcFolder() throws HopException {
    registerDatabase("TESTMANUAL", "Test Manual", noDownload("com.example.NoSuchDriver"));

    assertEquals(EXIT_NO_DOWNLOAD, install("testmanual"));
    String message = stderr();
    assertTrue(message.contains("No driver download available for 'testmanual'"), message);
    assertTrue(message.contains("Test Manual"), message);
    // The whole point: tell the user where to put the jar they fetch from the vendor themselves.
    assertTrue(message.contains(DriverInstaller.defaultInstallFolder().getAbsolutePath()), message);
    assertFalse(message.contains("Unknown driver id"), message);
  }

  @Test
  void aDriverThatAlreadyShipsWithHopIsNothingToInstall() throws HopException {
    // Any class on this classpath stands in for a bundled JDBC driver: what the command checks is
    // whether the plugin's own classloader can load the driver class it names.
    registerDatabase("TESTBUNDLED", "Test Bundled", noDownload(DriverCatalog.class.getName()));

    assertEquals(EXIT_OK, install("testbundled"));
    assertTrue(stdout().contains("Nothing to install"), stdout());
    assertTrue(stdout().contains("Test Bundled"), stdout());
  }

  @Test
  void aDatabaseTypeWithADownloadStillTakesTheInstallPath() throws HopException {
    registerDatabase("TESTDOWNLOAD", "Test Download", withDownload());

    // --target keeps the (restricted) driver from being fetched: the license notice comes first.
    assertEquals(2, install("testdownload"));
    assertTrue(stdout().contains("--accept-license"), stdout());
  }

  // ------------------------------------------------------------------ helpers

  private int install(String... args) {
    return new CommandLine(new DriverInstallCommand()).execute(args);
  }

  private String stdout() {
    return out.toString(StandardCharsets.UTF_8);
  }

  private String stderr() {
    return err.toString(StandardCharsets.UTF_8);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void registerDatabase(String databaseType, String name, IDatabase database)
      throws HopException {
    DatabasePluginMock plugin = mock(DatabasePluginMock.class);
    when(plugin.getIds()).thenReturn(new String[] {databaseType});
    when(plugin.getName()).thenReturn(name);
    when(plugin.getMainType()).thenReturn((Class) IDatabase.class);
    when(plugin.loadClass(IDatabase.class)).thenReturn(database);
    when(plugin.matches(databaseType)).thenReturn(true);
    PluginRegistry.getInstance().registerPlugin(DatabasePluginType.class, plugin);
    registeredPlugins.add(plugin);
  }

  private static IDatabase noDownload(String driverClass) {
    IDatabase database = mock(IDatabase.class);
    when(database.getDriverClass()).thenReturn(driverClass);
    return database;
  }

  private static IDatabase withDownload() {
    IDatabase database = mock(IDatabase.class);
    when(database.getDriverClass()).thenReturn("com.example.NoSuchDriver");
    when(database.getDriverDownload())
        .thenReturn(
            DriverDownload.builder()
                .mavenCoordinate("com.example:example-jdbc")
                .defaultVersion("1.0.0")
                .licenseCategory("X")
                .licenseName("Example License")
                .build());
    return database;
  }
}
