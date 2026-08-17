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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.setup.persist.ConfigFolderSeeder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopEnvironmentApplierTest {

  @TempDir Path temp;

  private HopEnvironmentApplier unixApplier() {
    Path home = temp.resolve("home");
    UserPaths paths =
        new UserPaths(
            home,
            home.resolve(".local/share"),
            home.resolve(".local/state"),
            home.resolve(".config"),
            "/bin/bash");
    return new HopEnvironmentApplier(
        OsFamily.UNIX, paths, new ConfigFolderSeeder(), command -> 0, new LogChannel("test"));
  }

  @Test
  void dryRunDoesNotWriteRcFile() throws Exception {
    Path rc = temp.resolve("home/.bashrc");
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(temp.resolve("cfg").toString());
    spec.setAuditFolder(temp.resolve("audit").toString());
    spec.setWriteShellRc(true);
    spec.setShellRcFile(rc.toString());
    spec.setWriteScript(false);
    spec.setDryRun(true);
    spec.setCreateFolders(true);
    HopEnvironmentApplyResult result = unixApplier().apply(spec);
    assertFalse(Files.exists(rc));
    assertTrue(result.getPlannedFiles().containsKey(rc.toString()));
    assertTrue(result.describe().contains("Would"));
  }

  @Test
  void writesRcAndScript() throws Exception {
    Path home = temp.resolve("home");
    Files.createDirectories(home);
    Path rc = home.resolve(".bashrc");
    Files.writeString(rc, "export PATH=/usr/bin\n");
    Path script = home.resolve(".config/hop/hop-env.sh");
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(temp.resolve("cfg").toString());
    spec.setAuditFolder(temp.resolve("audit").toString());
    spec.setWriteShellRc(true);
    spec.setShellRcFile(rc.toString());
    spec.setWriteScript(true);
    spec.setScriptFile(script.toString());
    spec.setCreateFolders(true);
    unixApplier().apply(spec);
    String rcText = Files.readString(rc);
    assertTrue(rcText.contains("export PATH=/usr/bin"));
    assertTrue(rcText.contains("HOP_CONFIG_FOLDER"));
    assertTrue(Files.exists(home.resolve(".bashrc.hop-setup.bak")));
    assertTrue(Files.readString(script).contains("HOP_CONFIG_FOLDER"));
    assertTrue(Files.isDirectory(temp.resolve("cfg")));
  }

  @Test
  void rejectsQuoteInShellValue() {
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder("o'reilly");
    spec.setWriteShellRc(true);
    spec.setWriteScript(false);
    spec.setCreateFolders(false);
    assertThrows(HopSetupException.class, () -> unixApplier().apply(spec));
  }

  @Test
  void rejectsUserEnvOnUnix() {
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder("/tmp/cfg");
    spec.setWriteUserEnv(true);
    spec.setWriteScript(false);
    spec.setCreateFolders(false);
    assertThrows(HopSetupException.class, () -> unixApplier().apply(spec));
  }

  @Test
  void requiresAtLeastOneVariable() {
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setWriteScript(true);
    spec.setCreateFolders(false);
    assertThrows(HopSetupException.class, () -> unixApplier().apply(spec));
  }

  @Test
  void windowsDryRunDoesNotCallPowerShell() throws Exception {
    List<List<String>> captured = new ArrayList<>();
    Path home = temp.resolve("winhome");
    UserPaths paths = new UserPaths(home, home, home, home, null);
    HopEnvironmentApplier applier =
        new HopEnvironmentApplier(
            OsFamily.WINDOWS,
            paths,
            new ConfigFolderSeeder(),
            command -> {
              captured.add(command);
              return 0;
            },
            new LogChannel("test"));
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(temp.resolve("wincfg").toString());
    spec.setWriteUserEnv(true);
    spec.setWriteScript(false);
    spec.setCreateFolders(false);
    spec.setDryRun(true);
    applier.apply(spec);
    assertTrue(captured.isEmpty());
  }
}
