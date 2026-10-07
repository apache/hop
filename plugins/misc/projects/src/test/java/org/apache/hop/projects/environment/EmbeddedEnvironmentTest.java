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

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class EmbeddedEnvironmentTest {

  @Test
  void normalizeDropsBlankNamesAndTrims() {
    EmbeddedEnvironment environment = new EmbeddedEnvironment();
    environment.setName("  dev  ");
    environment.setDescription("   ");
    environment
        .getVariables()
        .add(new EmbeddedEnvironmentVariable("  LOG_LEVEL  ", "  Basic  ", "  level  "));
    environment.getVariables().add(new EmbeddedEnvironmentVariable("  ", "ignored", "ignored"));
    environment.getVariables().add(null);

    EmbeddedEnvironmentValidator.normalize(environment);

    assertEquals("dev", environment.getName());
    assertNull(environment.getDescription());
    assertEquals(1, environment.getVariables().size());
    assertEquals("LOG_LEVEL", environment.getVariables().get(0).getName());
    assertEquals("Basic", environment.getVariables().get(0).getDefaultValue());
    assertEquals("level", environment.getVariables().get(0).getDescription());
    assertFalse(environment.getVariables().get(0).isMandatory());
    assertFalse(environment.getVariables().get(0).isSecret());
  }

  @Test
  void legacySecretWithTheSameNameIsADuplicate() {
    EmbeddedEnvironment environment = environmentNamed("dev");
    environment.getVariables().add(new EmbeddedEnvironmentVariable("DB_HOST", "localhost", null));
    environment.setSecretVariables(
        List.of(new EmbeddedEnvironmentVariable(" DB_HOST ", "change-to-your-password", null)));

    EmbeddedEnvironmentValidator.normalize(environment);

    assertEquals("DB_HOST", EmbeddedEnvironmentValidator.duplicateVariableName(environment));
    assertTrue(environment.getVariables().get(1).isSecret());
  }

  @Test
  void duplicateVariableNameInsideOneList() {
    EmbeddedEnvironment environment = environmentNamed("dev");
    environment.getVariables().add(new EmbeddedEnvironmentVariable("DB_PORT", "5432", null, true));
    environment.getVariables().add(new EmbeddedEnvironmentVariable("DB_PORT", "5433", null, true));

    assertEquals("DB_PORT", EmbeddedEnvironmentValidator.duplicateVariableName(environment));
  }

  @Test
  void distinctVariableNamesAreAccepted() {
    EmbeddedEnvironment environment = environmentNamed("dev");
    environment.getVariables().add(new EmbeddedEnvironmentVariable("LOG_LEVEL", "Basic", null));
    environment
        .getVariables()
        .add(new EmbeddedEnvironmentVariable("DB_HOST", "specify the database host", null, true));
    environment
        .getVariables()
        .add(
            new EmbeddedEnvironmentVariable(
                "DB_PASSWORD", "change-to-your-password", null, true, true));

    assertNull(EmbeddedEnvironmentValidator.duplicateVariableName(environment));
    assertFalse(EmbeddedEnvironmentValidator.missingName(environment));
    assertTrue(environment.getVariables().get(2).isSecret());
  }

  @Test
  void duplicateEnvironmentNames() {
    List<EmbeddedEnvironment> environments = new ArrayList<>();
    environments.add(environmentNamed("dev"));
    environments.add(environmentNamed("  dev "));
    environments.add(environmentNamed("prod"));

    assertEquals("dev", EmbeddedEnvironmentValidator.duplicateEnvironmentName(environments));
  }

  @Test
  void blankEnvironmentNamesAreNotDuplicates() {
    List<EmbeddedEnvironment> environments = new ArrayList<>();
    environments.add(environmentNamed("  "));
    environments.add(environmentNamed(null));

    assertNull(EmbeddedEnvironmentValidator.duplicateEnvironmentName(environments));
    assertTrue(EmbeddedEnvironmentValidator.missingName(environments.get(0)));
  }

  @Test
  void nameTakenMatchesTrimmedNames() {
    assertTrue(EmbeddedEnvironmentValidator.nameTaken(" dev ", List.of("dev", "prod")));
    assertFalse(EmbeddedEnvironmentValidator.nameTaken("qa", List.of("dev")));
    assertFalse(EmbeddedEnvironmentValidator.nameTaken("  ", List.of("dev")));
  }

  @Test
  void copyDoesNotShareVariableLists() {
    EmbeddedEnvironment environment = environmentNamed("dev");
    environment.setSecretVariables(
        List.of(
            new EmbeddedEnvironmentVariable("TOKEN", "change-to-your-token", "API token", true)));

    EmbeddedEnvironment copy = environment.copy();
    copy.getVariables().get(0).setDefaultValue("other");
    copy.getVariables().get(0).setMandatory(false);
    copy.getVariables().get(0).setSecret(false);

    assertEquals("change-to-your-token", environment.getVariables().get(0).getDefaultValue());
    assertTrue(environment.getVariables().get(0).isMandatory());
    assertTrue(environment.getVariables().get(0).isSecret());
  }

  private static EmbeddedEnvironment environmentNamed(String name) {
    EmbeddedEnvironment environment = new EmbeddedEnvironment();
    environment.setName(name);
    return environment;
  }
}
