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

package org.apache.hop.projects.environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.i18n.BaseMessages;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EnvironmentConfigFileSummaryTest {

  @TempDir Path folder;

  @Test
  void tooltipShowsStoredValuesAndVariableDescriptions() throws Exception {
    Path path = folder.resolve("env.json");
    write(
        path,
        """
        {
          "description" : "Warehouse connection",
          "variables" : [ {
            "name" : "DB_HOSTNAME",
            "value" : "${OTHER}",
            "description" : "Warehouse host"
          }, {
            "name" : "DB_PORT",
            "value" : "5432",
            "description" : ""
          } ]
        }
        """);

    EnvironmentConfigFileSummary summary = EnvironmentConfigFileSummary.read(path.toString());
    assertEquals("Warehouse connection", summary.getDescription());
    String tooltip = summary.tooltip(summary.getDescription());

    assertTrue(tooltip.startsWith("Warehouse connection\n\n"));
    assertTrue(tooltip.contains("DB_HOSTNAME = ${OTHER} (Warehouse host)"));
    assertTrue(tooltip.contains("DB_PORT = 5432"));
    assertFalse(tooltip.contains("resolved-host"));
  }

  @Test
  void tooltipMasksSecretsAndEncryptedValues() throws Exception {
    Path path = folder.resolve("secrets.json");
    DescribedVariablesConfigFile file = new DescribedVariablesConfigFile(path.toString());
    file.setDescribedVariable(new DescribedVariable("DB_PASSWORD", "s3cret", ""));
    file.setDescribedVariable(new DescribedVariable("API_TOKEN", "abc", "api"));
    file.setDescribedVariable(new DescribedVariable("my_secret", "hide-me", ""));
    file.setDescribedVariable(new DescribedVariable("credential_file", "hide-too", ""));
    file.setDescribedVariable(new DescribedVariable("DB_PASSWD", "hide-passwd", ""));
    file.setDescribedVariable(
        new DescribedVariable("DB_HOSTNAME", Encr.PASSWORD_ENCRYPTED_PREFIX + "xyz", "host"));
    file.setDescribedVariable(new DescribedVariable("PLAIN", "visible", ""));
    file.saveToFile();

    String tooltip = EnvironmentConfigFileSummary.read(path.toString()).tooltip(null);

    assertTrue(tooltip.contains("DB_PASSWORD = " + EnvironmentConfigFileSummary.MASKED_VALUE));
    assertTrue(
        tooltip.contains("API_TOKEN = " + EnvironmentConfigFileSummary.MASKED_VALUE + " (api)"));
    assertTrue(tooltip.contains("my_secret = " + EnvironmentConfigFileSummary.MASKED_VALUE));
    assertTrue(tooltip.contains("credential_file = " + EnvironmentConfigFileSummary.MASKED_VALUE));
    assertTrue(tooltip.contains("DB_PASSWD = " + EnvironmentConfigFileSummary.MASKED_VALUE));
    assertTrue(
        tooltip.contains("DB_HOSTNAME = " + EnvironmentConfigFileSummary.MASKED_VALUE + " (host)"));
    assertTrue(tooltip.contains("PLAIN = visible"));
    assertFalse(tooltip.contains("s3cret"));
    assertFalse(tooltip.contains("hide-me"));
    assertFalse(tooltip.contains(Encr.PASSWORD_ENCRYPTED_PREFIX + "xyz"));
  }

  @Test
  void tooltipCapsTheVariableList() throws Exception {
    Path path = folder.resolve("many.json");
    DescribedVariablesConfigFile file = new DescribedVariablesConfigFile(path.toString());
    int count = EnvironmentConfigFileSummary.MAX_VARIABLES_IN_TOOLTIP + 1;
    for (int i = 1; i <= count; i++) {
      file.setDescribedVariable(new DescribedVariable(String.format("NAME_%02d", i), "v" + i, ""));
    }
    file.saveToFile();

    String tooltip = EnvironmentConfigFileSummary.read(path.toString()).tooltip("Many");
    assertTrue(tooltip.contains("NAME_01 = v1"));
    assertTrue(tooltip.contains("NAME_40 = v40"));
    assertFalse(tooltip.contains("NAME_41 ="));
    assertTrue(
        tooltip.contains(
            BaseMessages.getString(
                EnvironmentConfigFileSummary.class,
                "LifecycleEnvironmentDialog.ConfigFile.ToolTip.More",
                "1")));
  }

  @Test
  void tooltipForMissingUnreadableAndEmptyFiles() throws Exception {
    Path missing = folder.resolve("missing.json");
    EnvironmentConfigFileSummary missingSummary =
        EnvironmentConfigFileSummary.read(missing.toString());
    assertNull(missingSummary.getDescription());
    assertEquals(
        message("LifecycleEnvironmentDialog.ConfigFile.ToolTip.Missing"),
        missingSummary.tooltip(null));
    assertEquals(
        "Note\n\n" + message("LifecycleEnvironmentDialog.ConfigFile.ToolTip.Missing"),
        missingSummary.tooltip("  Note  "));

    Path broken = folder.resolve("broken.json");
    Files.writeString(broken, "{");
    EnvironmentConfigFileSummary brokenSummary =
        EnvironmentConfigFileSummary.read(broken.toString());
    assertEquals(
        message("LifecycleEnvironmentDialog.ConfigFile.ToolTip.Unreadable"),
        brokenSummary.tooltip(""));

    Path empty = folder.resolve("empty.json");
    Files.writeString(empty, "{ \"description\" : \"Only a note\", \"variables\" : [ ] }");
    EnvironmentConfigFileSummary emptySummary = EnvironmentConfigFileSummary.read(empty.toString());
    assertEquals(
        "Only a note\n\n" + message("LifecycleEnvironmentDialog.ConfigFile.ToolTip.NoVariables"),
        emptySummary.tooltip(emptySummary.getDescription()));
  }

  @Test
  void unchangedDescriptionDoesNotRewriteTheFile() throws Exception {
    Path path = folder.resolve("same.json");
    String original =
        """
        {
          "description" : "keep",
          "variables" : [ {
            "name" : "A",
            "value" : "1",
            "description" : ""
          } ]
        }
        """;
    Files.writeString(path, original);

    assertFalse(EnvironmentConfigFileSummary.saveDescriptionIfChanged(path.toString(), " keep "));
    assertEquals(original, Files.readString(path));
  }

  @Test
  void changedDescriptionKeepsVariablesAndOtherKeys() throws Exception {
    Path path = folder.resolve("change.json");
    Files.writeString(
        path,
        """
        { "description" : "old", "extra" : "keep", "variables" : [ { "name" : "A", "value" : "1", "description" : "d" } ] }
        """);

    assertTrue(EnvironmentConfigFileSummary.saveDescriptionIfChanged(path.toString(), " new "));

    DescribedVariablesConfigFile loaded = new DescribedVariablesConfigFile(path.toString());
    loaded.readFromFile();
    assertEquals("new", loaded.getDescription());
    assertEquals("keep", loaded.getConfigMap().get("extra"));
    assertEquals("1", loaded.findDescribedVariableValue("A"));
    assertEquals("d", loaded.findDescribedVariable("A").getDescription());
  }

  @Test
  void clearingTheDescriptionRemovesTheKey() throws Exception {
    Path path = folder.resolve("clear.json");
    DescribedVariablesConfigFile created = new DescribedVariablesConfigFile(path.toString());
    created.setDescription("gone");
    created.setDescribedVariable(new DescribedVariable("A", "1", ""));
    created.saveToFile();

    assertTrue(EnvironmentConfigFileSummary.saveDescriptionIfChanged(path.toString(), "   "));

    DescribedVariablesConfigFile loaded = new DescribedVariablesConfigFile(path.toString());
    loaded.readFromFile();
    assertNull(loaded.getDescription());
    assertNull(loaded.getConfigMap().get(DescribedVariablesConfigFile.HOP_DESCRIPTION_KEY));
    assertEquals("1", loaded.findDescribedVariableValue("A"));
  }

  @Test
  void createsAMissingFileOnlyWhenTheDescriptionIsSet() throws Exception {
    Path created = folder.resolve("created.json");
    assertFalse(EnvironmentConfigFileSummary.saveDescriptionIfChanged(created.toString(), "  "));
    assertFalse(Files.exists(created));

    assertTrue(
        EnvironmentConfigFileSummary.saveDescriptionIfChanged(created.toString(), "Created"));
    DescribedVariablesConfigFile loaded = new DescribedVariablesConfigFile(created.toString());
    loaded.readFromFile();
    assertEquals("Created", loaded.getDescription());
    assertTrue(loaded.getDescribedVariables().isEmpty());
  }

  @Test
  void refusesToOverwriteAnUnreadableFile() throws Exception {
    Path broken = folder.resolve("broken.json");
    Files.writeString(broken, "{");

    assertFalse(EnvironmentConfigFileSummary.saveDescriptionIfChanged(broken.toString(), ""));
    assertEquals("{", Files.readString(broken));

    HopException error =
        assertThrows(
            HopException.class,
            () -> EnvironmentConfigFileSummary.saveDescriptionIfChanged(broken.toString(), "new"));
    assertTrue(error.getMessage().contains(broken.toString()));
    assertEquals("{", Files.readString(broken));
  }

  private static void write(Path path, String json) throws Exception {
    Files.writeString(path, json);
  }

  private static String message(String key) {
    return BaseMessages.getString(EnvironmentConfigFileSummary.class, key);
  }
}
