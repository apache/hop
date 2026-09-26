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
package org.apache.hop.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.config.ConfigFileSerializer;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.IHopConfigSerializer;
import org.apache.hop.core.config.plugin.ConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class SetHopConfigVariablesTest {

  private static final String VARIABLE_NAME = "HOP_SET_CONFIG_VARIABLES_TEST";

  @TempDir Path tempDir;

  private Path configFile;
  private String originalConfigFilename;
  private IHopConfigSerializer originalSerializer;

  @BeforeEach
  void redirectConfigFile() throws Exception {
    // Never write to the developer's real hop-config.json
    //
    configFile = tempDir.resolve("hop-config.json");
    Files.writeString(configFile, "{}");

    HopConfig hopConfig = HopConfig.getInstance();
    originalConfigFilename = hopConfig.getConfigFilename();
    originalSerializer = hopConfig.getSerializer();
    hopConfig.setConfigFilename(configFile.toString());
    hopConfig.setSerializer(new ConfigFileSerializer());
  }

  @AfterEach
  void restoreConfigFile() {
    HopConfig hopConfig = HopConfig.getInstance();
    hopConfig.getDescribedVariables().removeIf(v -> VARIABLE_NAME.equals(v.getName()));
    hopConfig.setConfigFilename(originalConfigFilename);
    hopConfig.setSerializer(originalSerializer);
  }

  @Test
  void setVariableIsReportedAndSaved() throws Exception {
    assertTrue(handle("-sv", VARIABLE_NAME + "=value"));

    assertEquals("value", HopConfig.getInstance().findDescribedVariableValue(VARIABLE_NAME));
    Map<String, Object> saved = savedVariable();
    assertEquals("value", saved.get("value"));
  }

  @Test
  void describeVariableIsReportedAndSaved() throws Exception {
    assertTrue(handle("-dv", VARIABLE_NAME + "=description"));

    Map<String, Object> saved = savedVariable();
    assertEquals("description", saved.get("description"));
  }

  @Test
  void describeKeepsExistingValue() throws Exception {
    handle("-sv", VARIABLE_NAME + "=value");
    handle("-dv", VARIABLE_NAME + "=description");

    Map<String, Object> saved = savedVariable();
    assertEquals("value", saved.get("value"));
    assertEquals("description", saved.get("description"));
  }

  @Test
  void noOptionIsNotAnAction() throws Exception {
    assertFalse(handle());
    assertEquals("{}", Files.readString(configFile));
  }

  @Test
  void invalidFormatIsRejected() {
    assertThrows(HopException.class, () -> handle("-sv", VARIABLE_NAME));
  }

  private boolean handle(String... args) throws HopException {
    SetHopConfigVariables configVariables = new SetHopConfigVariables();
    new CommandLine(configVariables).parseArgs(args);
    return configVariables.handleOption(null, null, null);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> savedVariable() throws HopException {
    Map<String, Object> config = new ConfigFileSerializer().readFromFile(configFile.toString());
    List<Map<String, Object>> variables =
        (List<Map<String, Object>>) config.get(ConfigFile.HOP_VARIABLES_KEY);
    assertNotNull(variables, "No variables were written to " + configFile);
    return variables.stream()
        .filter(v -> VARIABLE_NAME.equals(v.get("name")))
        .findFirst()
        .orElseThrow(() -> new AssertionError(VARIABLE_NAME + " was not written"));
  }
}
