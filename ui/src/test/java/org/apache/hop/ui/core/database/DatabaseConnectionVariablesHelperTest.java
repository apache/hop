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

package org.apache.hop.ui.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.variables.DescribedVariable;
import org.junit.jupiter.api.Test;

class DatabaseConnectionVariablesHelperTest {

  @Test
  void sanitizeConnectionNamePrefix() {
    assertEquals("EDW", DatabaseConnectionVariablesHelper.sanitizeConnectionNamePrefix("EDW"));
    assertEquals("MY_DB", DatabaseConnectionVariablesHelper.sanitizeConnectionNamePrefix("My DB"));
    assertEquals(
        "MY_DB", DatabaseConnectionVariablesHelper.sanitizeConnectionNamePrefix("  my-db  "));
    assertEquals("", DatabaseConnectionVariablesHelper.sanitizeConnectionNamePrefix(""));
    assertEquals("", DatabaseConnectionVariablesHelper.sanitizeConnectionNamePrefix("   "));
    assertEquals(
        "A_B_C", DatabaseConnectionVariablesHelper.sanitizeConnectionNamePrefix("a__b---c"));
  }

  @Test
  void expressionFor() {
    assertEquals(
        "${EDW_HOSTNAME}", DatabaseConnectionVariablesHelper.expressionFor("EDW_HOSTNAME"));
  }

  @Test
  void buildProposedVariablesIncludesStandardFields() {
    Map<String, String> descriptions = new HashMap<>();
    descriptions.put(DatabaseConnectionVariablesHelper.SUFFIX_HOSTNAME, "Host");
    descriptions.put(DatabaseConnectionVariablesHelper.SUFFIX_PORT, "Port");
    descriptions.put(DatabaseConnectionVariablesHelper.SUFFIX_DATABASE, "Database");
    descriptions.put(DatabaseConnectionVariablesHelper.SUFFIX_USERNAME, "User");
    descriptions.put(DatabaseConnectionVariablesHelper.SUFFIX_PASSWORD, "Password");

    List<DescribedVariable> variables =
        DatabaseConnectionVariablesHelper.buildProposedVariables(
            "EDW",
            "localhost",
            "5432",
            "warehouse",
            "alice",
            "secret",
            null,
            true,
            true,
            descriptions);

    assertEquals(5, variables.size());
    assertEquals("EDW_HOSTNAME", variables.get(0).getName());
    assertEquals("localhost", variables.get(0).getValue());
    assertEquals("Host", variables.get(0).getDescription());
    assertEquals("EDW_PORT", variables.get(1).getName());
    assertEquals("5432", variables.get(1).getValue());
    assertEquals("EDW_DATABASE", variables.get(2).getName());
    assertEquals("warehouse", variables.get(2).getValue());
    assertEquals("EDW_USERNAME", variables.get(3).getName());
    assertEquals("alice", variables.get(3).getValue());
    assertEquals("EDW_PASSWORD", variables.get(4).getName());
    assertEquals("secret", variables.get(4).getValue());
  }

  @Test
  void buildProposedVariablesIncludesUrlOnlyWhenFilled() {
    List<DescribedVariable> withoutUrl =
        DatabaseConnectionVariablesHelper.buildProposedVariables(
            "EDW", "h", "1", "d", "u", "p", "  ", true, true, null);
    assertFalse(
        withoutUrl.stream()
            .anyMatch(v -> v.getName().endsWith(DatabaseConnectionVariablesHelper.SUFFIX_URL)));

    List<DescribedVariable> withUrl =
        DatabaseConnectionVariablesHelper.buildProposedVariables(
            "EDW", "h", "1", "d", "u", "p", "jdbc:test://x", true, true, null);
    assertTrue(
        withUrl.stream()
            .anyMatch(v -> "EDW_URL".equals(v.getName()) && "jdbc:test://x".equals(v.getValue())));
  }

  @Test
  void buildProposedVariablesSkipsUsernamePasswordWhenExcluded() {
    List<DescribedVariable> variables =
        DatabaseConnectionVariablesHelper.buildProposedVariables(
            "EDW", "h", "1", "d", "u", "p", null, false, false, null);
    assertEquals(3, variables.size());
    assertFalse(
        variables.stream()
            .anyMatch(
                v -> v.getName().endsWith(DatabaseConnectionVariablesHelper.SUFFIX_USERNAME)));
    assertFalse(
        variables.stream()
            .anyMatch(
                v -> v.getName().endsWith(DatabaseConnectionVariablesHelper.SUFFIX_PASSWORD)));
  }

  @Test
  void findVariableNamesBySuffix() {
    List<DescribedVariable> variables =
        List.of(
            new DescribedVariable("EDW_HOSTNAME", "a", ""),
            new DescribedVariable("CUSTOM_PORT", "b", ""),
            new DescribedVariable("other", "c", ""),
            new DescribedVariable("EDW_HOSTNAME", "ignored", ""));

    Map<String, String> bySuffix =
        DatabaseConnectionVariablesHelper.findVariableNamesBySuffix(variables);
    assertEquals("EDW_HOSTNAME", bySuffix.get(DatabaseConnectionVariablesHelper.SUFFIX_HOSTNAME));
    assertEquals("CUSTOM_PORT", bySuffix.get(DatabaseConnectionVariablesHelper.SUFFIX_PORT));
    assertFalse(bySuffix.containsKey(DatabaseConnectionVariablesHelper.SUFFIX_DATABASE));
  }

  @Test
  void defaultConfigFilename() {
    assertEquals("EDW-config.json", DatabaseConnectionVariablesHelper.defaultConfigFilename("EDW"));
    assertEquals(
        "database-config.json", DatabaseConnectionVariablesHelper.defaultConfigFilename(""));
  }
}
