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

package org.apache.hop.setup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class HopEnvironmentDefaultsTest {

  private static UserPaths unix(String shell) {
    Path home = Path.of("/home/alice");
    return new UserPaths(
        home,
        home.resolve(".local/share"),
        home.resolve(".local/state"),
        home.resolve(".config"),
        shell);
  }

  @Test
  void linuxUsesXdgShareAndState() {
    UserPaths paths = unix("/bin/bash");
    assertEquals(
        "/home/alice/.local/share/hop",
        HopEnvironmentDefaults.recommendedConfigFolder(OsFamily.UNIX, paths));
    assertEquals(
        "/home/alice/.local/state/hop",
        HopEnvironmentDefaults.recommendedAuditFolder(OsFamily.UNIX, paths));
    assertEquals(
        "/home/alice/.config/hop/hop-env.sh",
        HopEnvironmentDefaults.wellKnownEnvFile(OsFamily.UNIX, paths));
  }

  @Test
  void linuxHonorsCustomXdg() {
    Path home = Path.of("/home/alice");
    UserPaths paths =
        new UserPaths(home, Path.of("/data"), Path.of("/state"), Path.of("/cfg"), "/bin/bash");
    assertEquals("/data/hop", HopEnvironmentDefaults.recommendedConfigFolder(OsFamily.UNIX, paths));
    assertEquals("/state/hop", HopEnvironmentDefaults.recommendedAuditFolder(OsFamily.UNIX, paths));
    assertEquals(
        "/cfg/hop/hop-env.sh", HopEnvironmentDefaults.wellKnownEnvFile(OsFamily.UNIX, paths));
  }

  @Test
  void windowsUsesDotHop() {
    Path home = Path.of("C:\\Users\\alice");
    UserPaths paths = new UserPaths(home, home, home, home, null);
    assertEquals(
        home.resolve(".hop").resolve("config").toString(),
        HopEnvironmentDefaults.recommendedConfigFolder(OsFamily.WINDOWS, paths));
    assertEquals(
        home.resolve(".hop").resolve("audit").toString(),
        HopEnvironmentDefaults.recommendedAuditFolder(OsFamily.WINDOWS, paths));
    assertEquals(
        home.resolve(".hop").resolve("hop-env.cmd").toString(),
        HopEnvironmentDefaults.wellKnownEnvFile(OsFamily.WINDOWS, paths));
  }

  @Test
  void zshUsesZshrc() {
    assertEquals(
        "/home/alice/.zshrc", HopEnvironmentDefaults.recommendedShellRcFile(unix("/bin/zsh")));
    assertEquals(
        "/home/alice/.bashrc", HopEnvironmentDefaults.recommendedShellRcFile(unix("/bin/bash")));
    assertEquals("/home/alice/.bashrc", HopEnvironmentDefaults.recommendedShellRcFile(unix(null)));
  }

  @Test
  void defaultProjectHomeIsInDocumentsOnLinuxAndWindows() {
    UserPaths paths = unix("/bin/bash");
    assertEquals(
        "/home/alice/Documents/hop/default",
        HopEnvironmentDefaults.recommendedDefaultProjectHome(OsFamily.UNIX, paths));
    Path winHome = Path.of("C:\\Users\\alice");
    UserPaths win = new UserPaths(winHome, winHome, winHome, winHome, null);
    assertEquals(
        winHome.resolve("Documents").resolve("Hop").resolve("default").toString(),
        HopEnvironmentDefaults.recommendedDefaultProjectHome(OsFamily.WINDOWS, win));
    assertEquals(
        "/home/alice/hop/default",
        HopEnvironmentDefaults.recommendedDefaultProjectHome(OsFamily.OSX, paths));
  }

  @Test
  void installFallbacksAreRelative() {
    assertEquals("./config", HopEnvironmentDefaults.INSTALL_CONFIG_FOLDER);
    assertEquals("./audit", HopEnvironmentDefaults.INSTALL_AUDIT_FOLDER);
  }

  @Test
  void fishIsNotASupportedRcShell() {
    assertFalse(HopEnvironmentDefaults.supportsShellRc(unix("/usr/bin/fish")));
    assertTrue(HopEnvironmentDefaults.supportsShellRc(unix("/bin/zsh")));
  }
}
