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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for HopLinter class */
public class HopLinterTest {

  private HopLinter linter;

  @BeforeEach
  public void setUp() {
    linter = new HopLinter();
  }

  @Test
  public void testConstructor() {
    assertNotNull(linter);
    assertNull(linter.getConfig());
  }

  @Test
  public void testLoadDefaultConfig() {
    linter.loadDefaultConfig();

    LinterConfig config = linter.getConfig();
    assertNotNull(config);
    assertTrue(config.isEnabled());

    Map<String, RuleConfig> rules = config.getRules();
    assertNotNull(rules);
    assertTrue(rules.size() >= 10);

    assertTrue(rules.containsKey("TRANS-002"));
    assertTrue(rules.containsKey("TRANS-002"));
    assertTrue(rules.containsKey("DB-001"));

    assertTrue(rules.get("TRANS-002").isEnabled());
    assertTrue(rules.get("TRANS-002").isEnabled());
    assertTrue(rules.get("DB-001").isEnabled());
  }

  @Test
  public void testLoadConfigFromNonExistentFile() throws IOException {
    File tempFile = File.createTempFile("test-config", ".yml");
    tempFile.delete(); // Ensure it doesn't exist

    linter.loadConfig(tempFile.getAbsolutePath());

    LinterConfig config = linter.getConfig();
    assertNotNull(config);
    // Should fall back to default config
    assertTrue(config.getRules().size() >= 10);
  }

  @Test
  public void testLoadConfigFromValidFile() throws IOException {
    // Create a temporary YAML config file
    String yamlContent =
        "rules:\n"
            + "  STRUCT-001:\n"
            + "    enabled: true\n"
            + "    parameters:\n"
            + "      maxTransforms: 15\n"
            + "  TRANS-002:\n"
            + "    enabled: false\n"
            + "  DB-001:\n"
            + "    enabled: true\n"
            + "    parameters: {}";

    File tempFile = File.createTempFile("test-config", ".yml");
    try {
      java.nio.file.Files.write(tempFile.toPath(), yamlContent.getBytes());

      linter.loadConfig(tempFile.getAbsolutePath());

      LinterConfig config = linter.getConfig();
      assertNotNull(config);

      Map<String, RuleConfig> rules = config.getRules();
      assertTrue(rules.containsKey("TRANS-002"));
      assertFalse(rules.get("TRANS-002").isEnabled());
      assertTrue(rules.get("DB-001").isEnabled());

      RuleConfig struct001 = rules.get("STRUCT-001");
      assertTrue(struct001.isEnabled());
      assertEquals(15, struct001.getParameters().get("maxTransforms"));

    } finally {
      tempFile.delete();
    }
  }

  @Test
  public void testLoadConfigWithCustomRule() throws IOException {
    // Create a temporary YAML config file with custom rule
    String yamlContent =
        "rules:\n"
            + "  STRUCT-001:\n"
            + "    enabled: true\n"
            + "    parameters:\n"
            + "      maxTransforms: 10\n"
            + "  custom-rule-1:\n"
            + "    enabled: true\n"
            + "    type: custom\n"
            + "    target: PIPELINE\n"
            + "    targetField: name\n"
            + "    condition: NOT_EMPTY\n"
            + "    conditionValue: \"\"\n"
            + "    severity: ERROR\n"
            + "    name: \"Pipeline Name Required\"\n"
            + "    description: \"All pipelines must have a name\"\n"
            + "    parameters: {}";

    File tempFile = File.createTempFile("test-config", ".yml");
    try {
      java.nio.file.Files.write(tempFile.toPath(), yamlContent.getBytes());

      linter.loadConfig(tempFile.getAbsolutePath());

      LinterConfig config = linter.getConfig();
      assertNotNull(config);

      Map<String, RuleConfig> rules = config.getRules();
      assertTrue(rules.containsKey("custom-rule-1"));
      assertTrue(rules.get("custom-rule-1").isEnabled());

    } finally {
      tempFile.delete();
    }
  }

  @Test
  public void testLoadConfigWithInvalidYAML() throws IOException {
    File tempFile = File.createTempFile("test-config", ".yml");
    try {
      java.nio.file.Files.write(tempFile.toPath(), "invalid: yaml: content: [".getBytes());

      try {
        linter.loadConfig(tempFile.getAbsolutePath());
        fail("Should have thrown IOException for invalid YAML");
      } catch (IOException e) {
        // Expected
        assertTrue(e.getMessage().contains("Failed to load configuration"));
      }

    } finally {
      tempFile.delete();
    }
  }

  @Test
  public void testIsRuleEnabled() {
    linter.loadDefaultConfig();

    assertTrue(linter.isRuleEnabled("TRANS-002"));
    assertTrue(linter.isRuleEnabled("TRANS-002"));
    assertTrue(linter.isRuleEnabled("DB-001"));
    assertFalse(linter.isRuleEnabled("NONEXISTENT"));
  }

  @Test
  public void testGetRuleParameters() {
    linter.loadDefaultConfig();

    Map<String, Object> trans001Params = linter.getRuleParameters("STRUCT-001");
    assertNotNull(trans001Params);
  }

  @Test
  public void testFindHopFiles() {
    // Create a temporary directory structure
    File tempDir = createTempDirectoryStructure();
    try {
      List<String> hopFiles = linter.findHopFiles(tempDir.getAbsolutePath());

      assertNotNull(hopFiles);
      // Discovery recurses, so the nested pipeline counts too.
      assertEquals(3, hopFiles.size()); // pipeline.hpl, workflow.hwf, subdir1/nested.hpl

      assertTrue(hopFiles.stream().anyMatch(f -> f.endsWith("pipeline.hpl")));
      assertTrue(hopFiles.stream().anyMatch(f -> f.endsWith("workflow.hwf")));
      assertTrue(hopFiles.stream().anyMatch(f -> f.endsWith("nested.hpl")));

    } finally {
      deleteDirectory(tempDir);
    }
  }

  @Test
  public void testFindHopFilesWithNonExistentDirectory() {
    List<String> hopFiles = linter.findHopFiles("/non/existent/directory");

    assertNotNull(hopFiles);
    assertTrue(hopFiles.isEmpty());
  }

  @Test
  public void testFindHopFilesWithEmptyDirectory() throws IOException {
    File tempDir = File.createTempFile("empty", "");
    tempDir.delete();
    tempDir.mkdir();

    try {
      List<String> hopFiles = linter.findHopFiles(tempDir.getAbsolutePath());

      assertNotNull(hopFiles);
      assertTrue(hopFiles.isEmpty());

    } finally {
      tempDir.delete();
    }
  }

  // Helper methods for creating test directory structure
  private File createTempDirectoryStructure() {
    try {
      File tempDir = File.createTempFile("hop-test", "");
      tempDir.delete();
      tempDir.mkdir();

      // Create subdirectories
      File subDir1 = new File(tempDir, "subdir1");
      subDir1.mkdir();

      File subDir2 = new File(tempDir, "subdir2");
      subDir2.mkdir();

      // Create Hop files
      new File(tempDir, "pipeline.hpl").createNewFile();
      new File(tempDir, "workflow.hwf").createNewFile();
      new File(subDir1, "nested.hpl").createNewFile();

      // Create non-Hop files
      new File(tempDir, "readme.txt").createNewFile();
      new File(tempDir, "config.xml").createNewFile();

      return tempDir;
    } catch (IOException e) {
      throw new RuntimeException("Failed to create test directory structure", e);
    }
  }

  private void deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
      directory.delete();
    }
  }
}
