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

package org.apache.hop.projects.environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.environment.EmbeddedEnvironmentImporter.EnvironmentSource;
import org.apache.hop.projects.environment.EmbeddedEnvironmentImporter.VariableAssignment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EmbeddedEnvironmentImporterTest {

  private Path tempRoot;

  @BeforeEach
  void setUp() throws Exception {
    tempRoot = Files.createTempDirectory("hop-import-env");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (tempRoot != null && Files.exists(tempRoot)) {
      try (Stream<Path> walk = Files.walk(tempRoot)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
      tempRoot = null;
    }
  }

  @Test
  void readCopiesConfigurationValuesAsDefaults() throws Exception {
    String configPath = writeConfig("dev.json", variables());
    LifecycleEnvironment environment =
        new LifecycleEnvironment("dev", "Developer workstation", "warehouse", List.of(configPath));

    List<LifecycleEnvironment> environments = new ArrayList<>();
    environments.add(environment);
    environments.add(null);
    environments.add(new LifecycleEnvironment());
    List<EnvironmentSource> sources =
        EmbeddedEnvironmentImporter.read(environments, new Variables());

    assertEquals(1, sources.size());
    EnvironmentSource source = sources.get(0);
    assertEquals("dev", source.getName());
    assertEquals("Developer workstation", source.getDescription());
    assertTrue(source.getUnreadableFiles().isEmpty());
    assertEquals(List.of("LOG_LEVEL", "DB_HOST", "DB_PASSWORD", "DB_TOKEN"), names(source));
    assertFalse(secret(source, "LOG_LEVEL"));
    assertFalse(secret(source, "DB_HOST"));
    assertTrue(secret(source, "DB_PASSWORD"));
    assertTrue(secret(source, "DB_TOKEN"));
    assertEquals("Hop log level", description(source, "LOG_LEVEL"));
    assertEquals("Basic", defaultValue(source, "LOG_LEVEL"));
    assertEquals("db.example", defaultValue(source, "DB_HOST"));
    assertEquals("real-password", defaultValue(source, "DB_PASSWORD"));
    assertEquals(Encr.PASSWORD_ENCRYPTED_PREFIX + "abc", defaultValue(source, "DB_TOKEN"));

    EmbeddedEnvironment embedded =
        EmbeddedEnvironmentImporter.toEmbeddedEnvironment(
            source.getName(), source.getDescription(), source.getVariables());
    assertEquals(4, embedded.getVariables().size());
    assertEquals("Basic", embedded.getVariables().get(0).getDefaultValue());
    assertEquals("LOG_LEVEL", embedded.getVariables().get(0).getName());
    assertFalse(embedded.getVariables().get(0).isSecret());
    assertEquals("db.example", embedded.getVariables().get(1).getDefaultValue());
    assertEquals("DB_HOST", embedded.getVariables().get(1).getName());
    assertFalse(embedded.getVariables().get(1).isSecret());
    assertEquals("real-password", embedded.getVariables().get(2).getDefaultValue());
    assertEquals("DB_PASSWORD", embedded.getVariables().get(2).getName());
    assertTrue(embedded.getVariables().get(2).isSecret());
    assertEquals(
        Encr.PASSWORD_ENCRYPTED_PREFIX + "abc", embedded.getVariables().get(3).getDefaultValue());
    assertEquals("DB_TOKEN", embedded.getVariables().get(3).getName());
    assertTrue(embedded.getVariables().get(3).isSecret());
    assertFalse(embedded.getVariables().get(1).isMandatory());
    assertFalse(embedded.getVariables().get(2).isMandatory());
  }

  @Test
  void readMergesFilesAndKeepsAnUnreadablePath() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable("ENV_DIR", tempRoot.toString());
    String first =
        writeConfig(
            "first.json",
            List.of(
                new DescribedVariable("DB_HOST", "db.example", ""),
                new DescribedVariable("DATA_FOLDER", " /data ", "Data folder")));
    String second =
        writeConfig(
            "second.json",
            List.of(
                new DescribedVariable("DB_HOST", "other", "JDBC host"),
                new DescribedVariable("DATA_FOLDER", "   ", "later description"),
                new DescribedVariable(
                    "API_KEY", Encr.PASSWORD_ENCRYPTED_PREFIX + "abc", "API key")));
    LifecycleEnvironment environment =
        new LifecycleEnvironment(
            "prod", "   ", "warehouse", List.of("${ENV_DIR}/missing.json", first, second));

    EnvironmentSource source =
        EmbeddedEnvironmentImporter.read(List.of(environment), variables).get(0);

    assertNull(source.getDescription());
    assertEquals(List.of("${ENV_DIR}/missing.json"), source.getUnreadableFiles());
    assertFalse(secret(source, "DB_HOST"));
    assertEquals("JDBC host", description(source, "DB_HOST"));
    assertEquals("other", defaultValue(source, "DB_HOST"));
    assertEquals("Data folder", description(source, "DATA_FOLDER"));
    assertEquals("/data", defaultValue(source, "DATA_FOLDER"));
    assertTrue(secret(source, "API_KEY"));
    assertEquals(Encr.PASSWORD_ENCRYPTED_PREFIX + "abc", defaultValue(source, "API_KEY"));

    EmbeddedEnvironment embedded =
        EmbeddedEnvironmentImporter.toEmbeddedEnvironment(
            source.getName(),
            source.getDescription(),
            List.of(
                new VariableAssignment("DB_HOST", "JDBC host", false, true, " other "),
                new VariableAssignment("API_KEY", "API key", true, true),
                new VariableAssignment("  ", "ignored", false)));
    assertEquals(2, embedded.getVariables().size());
    assertEquals("DB_HOST", embedded.getVariables().get(0).getName());
    assertEquals("other", embedded.getVariables().get(0).getDefaultValue());
    assertTrue(embedded.getVariables().get(0).isMandatory());
    assertFalse(embedded.getVariables().get(0).isSecret());
    assertEquals("API_KEY", embedded.getVariables().get(1).getName());
    assertNull(embedded.getVariables().get(1).getDefaultValue());
    assertTrue(embedded.getVariables().get(1).isMandatory());
    assertTrue(embedded.getVariables().get(1).isSecret());
  }

  private List<DescribedVariable> variables() {
    return List.of(
        new DescribedVariable("LOG_LEVEL", "Basic", "Hop log level"),
        new DescribedVariable("DB_HOST", "db.example", "JDBC host"),
        new DescribedVariable("DB_PASSWORD", "real-password", "JDBC password"),
        new DescribedVariable("DB_TOKEN", Encr.PASSWORD_ENCRYPTED_PREFIX + "abc", "token"),
        new DescribedVariable("  ", "ignored", "ignored"));
  }

  private String writeConfig(String filename, List<DescribedVariable> variables) throws Exception {
    String path = tempRoot.resolve(filename).toString();
    DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile(path);
    configFile.setDescribedVariables(new ArrayList<>(variables));
    configFile.saveToFile();
    return path;
  }

  private static List<String> names(EnvironmentSource source) {
    List<String> names = new ArrayList<>();
    for (VariableAssignment assignment : source.getVariables()) {
      names.add(assignment.getName());
    }
    return names;
  }

  private static boolean secret(EnvironmentSource source, String name) {
    return assignment(source, name).isSecret();
  }

  private static String description(EnvironmentSource source, String name) {
    return assignment(source, name).getDescription();
  }

  private static String defaultValue(EnvironmentSource source, String name) {
    return assignment(source, name).getDefaultValue();
  }

  private static VariableAssignment assignment(EnvironmentSource source, String name) {
    for (VariableAssignment assignment : source.getVariables()) {
      if (name.equals(assignment.getName())) {
        return assignment;
      }
    }
    throw new AssertionError("Missing variable " + name);
  }
}
