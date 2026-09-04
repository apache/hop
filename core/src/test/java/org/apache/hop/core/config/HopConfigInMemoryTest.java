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

package org.apache.hop.core.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopConfigInMemoryTest {

  @TempDir private Path folder;

  @AfterEach
  void tearDown() {
    System.clearProperty(Const.HOP_CONFIG_IN_MEMORY);
    HopConfig.setInMemoryMode(false);
  }

  @Test
  void testSystemPropertyActivatesInMemoryMode() {
    System.setProperty(Const.HOP_CONFIG_IN_MEMORY, "Y");
    assertTrue(HopConfig.isInMemoryMode());

    System.setProperty(Const.HOP_CONFIG_IN_MEMORY, "true");
    assertTrue(HopConfig.isInMemoryMode());

    System.setProperty(Const.HOP_CONFIG_IN_MEMORY, "N");
    assertFalse(HopConfig.isInMemoryMode());
  }

  @Test
  void testConfigFileInMemoryDoesNotWriteToFile() throws Exception {
    Path targetFile = folder.resolve("in-memory-config.json");
    assertFalse(Files.exists(targetFile));

    ConfigFile configFile =
        new ConfigFile() {
          private String filename = targetFile.toString();

          @Override
          public String getConfigFilename() {
            return filename;
          }

          @Override
          public void setConfigFilename(String filename) {
            this.filename = filename;
          }
        };
    configFile.setInMemory(true);
    configFile.setConfigMap(new HashMap<>());
    configFile.getConfigMap().put("testKey", "testVal");

    // Saving to file should be a no-op and not create the file on disk
    configFile.saveToFile();
    assertFalse(Files.exists(targetFile));
  }

  @Test
  void testHopConfigInMemorySavesOptionOnlyInMemory() throws Exception {
    HopConfig.setInMemoryMode(true);
    assertTrue(HopConfig.isInMemoryMode());

    HopConfig.getInstance().saveOption("ephemeral_option", "ephemeral_value");
    assertEquals("ephemeral_value", HopConfig.readOption("ephemeral_option"));
  }
}
