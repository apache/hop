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

package org.apache.hop.setup.persist;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.setup.HopEnvironmentApplyResult;
import org.apache.hop.setup.HopEnvironmentSpec;
import org.apache.hop.setup.HopSetupVariables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigFolderSeederTest {

  @TempDir Path temp;

  @Test
  void createsMissingFolders() throws Exception {
    Path config = temp.resolve("cfg");
    Path audit = temp.resolve("audit");
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(config.toString());
    spec.setAuditFolder(audit.toString());
    spec.setCreateFolders(true);
    new ConfigFolderSeeder().seed(spec, new HopEnvironmentApplyResult(), null);
    assertTrue(Files.isDirectory(config));
    assertTrue(Files.isDirectory(audit));
  }

  @Test
  void copiesInstallConfigWhenTargetEmpty() throws Exception {
    Path install = temp.resolve("hop");
    Files.createDirectories(install.resolve("plugins"));
    Files.createDirectories(install.resolve("config"));
    Files.writeString(install.resolve("config").resolve(HopSetupVariables.HOP_CONFIG_JSON), "{}");
    Files.writeString(install.resolve("config").resolve("extra.txt"), "keep");
    Path target = temp.resolve("user-config");
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(target.toString());
    spec.setCreateFolders(true);
    spec.setCopyExisting(true);
    new ConfigFolderSeeder().seed(spec, new HopEnvironmentApplyResult(), install);
    assertTrue(Files.exists(target.resolve(HopSetupVariables.HOP_CONFIG_JSON)));
    assertTrue(Files.exists(target.resolve("extra.txt")));
  }

  @Test
  void doesNotOverwriteExistingHopConfig() throws Exception {
    Path install = temp.resolve("hop");
    Files.createDirectories(install.resolve("plugins"));
    Files.createDirectories(install.resolve("config"));
    Files.writeString(
        install.resolve("config").resolve(HopSetupVariables.HOP_CONFIG_JSON), "install");
    Path target = temp.resolve("user-config");
    Files.createDirectories(target);
    Files.writeString(target.resolve(HopSetupVariables.HOP_CONFIG_JSON), "mine");
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(target.toString());
    spec.setCopyExisting(true);
    new ConfigFolderSeeder().seed(spec, new HopEnvironmentApplyResult(), install);
    assertTrue(
        Files.readString(target.resolve(HopSetupVariables.HOP_CONFIG_JSON)).contains("mine"));
    assertFalse(Files.exists(target.resolve("extra.txt")));
  }
}
