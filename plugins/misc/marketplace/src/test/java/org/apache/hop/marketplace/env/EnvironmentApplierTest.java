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

package org.apache.hop.marketplace.env;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EnvironmentApplierTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @Test
  void validateDetectsMissingPlugin() throws Exception {
    Path hopHome = tempDir.resolve("hop");
    Files.createDirectories(hopHome.resolve("plugins"));

    HopEnvironmentSpec env = new HopEnvironmentSpec();
    env.setHopVersion("2.19.0");
    HopEnvironmentSpec.PluginRef ref = new HopEnvironmentSpec.PluginRef();
    ref.setArtifactId("hop-tech-parquet");
    ref.setVersion("2.19.0");
    env.getPlugins().add(ref);

    EnvironmentDrift drift =
        new EnvironmentApplier(new LogChannel("test"), hopHome, new MarketplaceConfig())
            .validate(env);
    assertTrue(drift.hasDrift());
    assertEquals(1, drift.getMissingPlugins().size());
  }

  @Test
  void validateOkWhenPluginOnDisk() throws Exception {
    Path hopHome = tempDir.resolve("hop");
    Files.createDirectories(hopHome.resolve("plugins/tech/parquet"));

    HopEnvironmentSpec env = new HopEnvironmentSpec();
    HopEnvironmentSpec.PluginRef ref = new HopEnvironmentSpec.PluginRef();
    ref.setArtifactId("hop-tech-parquet");
    env.getPlugins().add(ref);

    EnvironmentDrift drift =
        new EnvironmentApplier(new LogChannel("test"), hopHome, new MarketplaceConfig())
            .validate(env);
    assertFalse(drift.hasDrift());
  }

  @Test
  void validateVersionMismatchFromReceipt() throws Exception {
    Path hopHome = tempDir.resolve("hop");
    Files.createDirectories(hopHome.resolve("plugins/tech/parquet"));
    Files.createDirectories(hopHome.resolve(PluginInstaller.RECEIPTS_DIR));
    InstallReceipt receipt = new InstallReceipt();
    receipt.setGroupId("org.apache.hop");
    receipt.setArtifactId("hop-tech-parquet");
    receipt.setVersion("2.18.0");
    HopJson.newMapper()
        .writeValue(
            hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("hop-tech-parquet.json").toFile(),
            receipt);

    HopEnvironmentSpec env = new HopEnvironmentSpec();
    HopEnvironmentSpec.PluginRef ref = new HopEnvironmentSpec.PluginRef();
    ref.setArtifactId("hop-tech-parquet");
    ref.setVersion("2.19.0");
    env.getPlugins().add(ref);

    EnvironmentDrift drift =
        new EnvironmentApplier(new LogChannel("test"), hopHome, new MarketplaceConfig())
            .validate(env);
    assertEquals(1, drift.getVersionMismatches().size());
  }

  @Test
  void resolveEnvironmentFileDiscoversProjectFile() throws Exception {
    Path project = tempDir.resolve("project");
    Files.createDirectories(project);
    Path envFile = project.resolve("hop-env.yaml");
    Files.writeString(envFile, "version: \"1.0\"\n");
    System.setProperty("PROJECT_HOME", project.toString());
    try {
      Path found = EnvironmentApplier.resolveEnvironmentFile(tempDir, null);
      assertEquals(envFile.toAbsolutePath().normalize(), found);
    } finally {
      System.clearProperty("PROJECT_HOME");
    }
  }
}
