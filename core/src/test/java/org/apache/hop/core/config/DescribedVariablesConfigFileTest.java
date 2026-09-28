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

package org.apache.hop.core.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.variables.DescribedVariable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DescribedVariablesConfigFileTest {

  @TempDir Path folder;

  @Test
  void filenameAndSerializer() {
    DescribedVariablesConfigFile file = new DescribedVariablesConfigFile("/tmp/hop-config.json");
    assertEquals("/tmp/hop-config.json", file.getConfigFilename());
    assertInstanceOf(ConfigFileSerializer.class, file.getSerializer());

    file.setConfigFilename("/other/path.json");
    assertEquals("/other/path.json", file.getConfigFilename());
  }

  @Test
  void descriptionRoundTripsWithVariables() throws Exception {
    Path path = folder.resolve("env.json");
    DescribedVariablesConfigFile file = new DescribedVariablesConfigFile(path.toString());
    file.setDescription("  Warehouse connection  ");
    file.setDescribedVariable(new DescribedVariable("DB_HOSTNAME", "localhost", "Warehouse host"));
    file.saveToFile();

    DescribedVariablesConfigFile loaded = new DescribedVariablesConfigFile(path.toString());
    loaded.readFromFile();
    assertEquals("Warehouse connection", loaded.getDescription());
    assertEquals("localhost", loaded.findDescribedVariableValue("DB_HOSTNAME"));
    assertEquals("Warehouse host", loaded.findDescribedVariable("DB_HOSTNAME").getDescription());
  }

  @Test
  void readingVariablesKeepsTheFileDescription() throws Exception {
    Path path = folder.resolve("env.json");
    Files.writeString(
        path,
        """
        {
          "description" : "my environment config",
          "variables" : [ { "name" : "TEST_ENV_VAR", "value" : "test_val", "description" : "" } ]
        }
        """);

    DescribedVariablesConfigFile file = new DescribedVariablesConfigFile(path.toString());
    file.readFromFile();
    assertEquals("test_val", file.findDescribedVariableValue("TEST_ENV_VAR"));
    assertEquals(1, file.getDescribedVariables().size());
    assertEquals("my environment config", file.getDescription());

    file.saveToFile();
    DescribedVariablesConfigFile reloaded = new DescribedVariablesConfigFile(path.toString());
    reloaded.readFromFile();
    assertEquals("my environment config", reloaded.getDescription());
    assertEquals("test_val", reloaded.findDescribedVariableValue("TEST_ENV_VAR"));
  }

  @Test
  void savingVariablesDoesNotInventADescription() throws Exception {
    Path path = folder.resolve("env.json");
    Files.writeString(
        path,
        """
        { "extra" : "keep", "variables" : [ { "name" : "A", "value" : "1", "description" : "d" } ] }
        """);

    DescribedVariablesConfigFile file = new DescribedVariablesConfigFile(path.toString());
    file.readFromFile();
    file.setDescribedVariables(file.getDescribedVariables());
    file.saveToFile();

    DescribedVariablesConfigFile loaded = new DescribedVariablesConfigFile(path.toString());
    loaded.readFromFile();
    assertNull(loaded.getDescription());
    assertNull(loaded.getConfigMap().get(DescribedVariablesConfigFile.HOP_DESCRIPTION_KEY));
    assertEquals("keep", loaded.getConfigMap().get("extra"));
    assertEquals("1", loaded.findDescribedVariableValue("A"));
    assertEquals("d", loaded.findDescribedVariable("A").getDescription());
  }

  @Test
  void blankDescriptionRemovesTheKeyAndKeepsOtherEntries() throws Exception {
    Path path = folder.resolve("env.json");
    DescribedVariablesConfigFile file = new DescribedVariablesConfigFile(path.toString());
    file.setDescription("hello");
    file.setDescribedVariable(new DescribedVariable("A", "1", ""));
    file.getConfigMap().put("extra", "keep");
    file.saveToFile();

    file.setDescription("   ");
    file.saveToFile();

    DescribedVariablesConfigFile loaded = new DescribedVariablesConfigFile(path.toString());
    loaded.readFromFile();
    assertNull(loaded.getDescription());
    assertNull(loaded.getConfigMap().get(DescribedVariablesConfigFile.HOP_DESCRIPTION_KEY));
    assertEquals("keep", loaded.getConfigMap().get("extra"));
    assertEquals("1", loaded.findDescribedVariableValue("A"));
  }
}
