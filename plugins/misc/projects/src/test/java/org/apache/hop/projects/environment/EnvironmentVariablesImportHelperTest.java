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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.core.search.SearchResult;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.util.ProjectsUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EnvironmentVariablesImportHelperTest {

  @TempDir Path tempDir;

  @Test
  void extractVariableNamesUnixOnly() {
    String text =
        "host=${DB_HOST} port=%%DB_PORT%% url=#{resolver:x} path=${PROJECT_HOME}/data nested=${A}${B}";
    Set<String> names = EnvironmentVariablesImportHelper.extractVariableNames(text);
    assertEquals(Set.of("DB_HOST", "PROJECT_HOME", "A", "B"), names);
    assertFalse(names.contains("DB_PORT"));
    assertFalse(names.contains("resolver:x"));
  }

  @Test
  void extractVariableNamesEmpty() {
    assertTrue(EnvironmentVariablesImportHelper.extractVariableNames(null).isEmpty());
    assertTrue(EnvironmentVariablesImportHelper.extractVariableNames("").isEmpty());
    assertTrue(
        EnvironmentVariablesImportHelper.extractVariableNames("no variables here").isEmpty());
  }

  @Test
  void variableExpressionRegexMatchesPropertyValues() {
    Pattern pattern = Pattern.compile(EnvironmentVariablesImportHelper.VARIABLE_EXPRESSION_REGEX);
    assertTrue(pattern.matcher("jdbc:postgresql://${DB_HOST}:5432/db").find());
    assertTrue(pattern.matcher("${ONLY}").find());
    assertFalse(pattern.matcher("%%WINDOWS%%").find());
    assertFalse(pattern.matcher("#{resolver:x}").find());
    assertFalse(pattern.matcher("plain text").find());
  }

  @Test
  void isManagedVariable() {
    assertTrue(
        EnvironmentVariablesImportHelper.isManagedVariable(ProjectsUtil.VARIABLE_PROJECT_HOME));
    assertTrue(EnvironmentVariablesImportHelper.isManagedVariable(Const.HOP_METADATA_FOLDER));
    assertTrue(
        EnvironmentVariablesImportHelper.isManagedVariable(
            Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY));
    assertTrue(
        EnvironmentVariablesImportHelper.isManagedVariable(
            EnvironmentVariablesImportHelper.JAVA_IO_TMPDIR));
    assertTrue(EnvironmentVariablesImportHelper.isManagedVariable(""));
    assertFalse(EnvironmentVariablesImportHelper.isManagedVariable("DB_HOSTNAME"));
  }

  @Test
  void formatFoundLocationIncludesTypeNameAndField() {
    ISearchable<String> searchable =
        new ISearchable<>() {
          @Override
          public String getLocation() {
            return "project";
          }

          @Override
          public String getName() {
            return "load-data";
          }

          @Override
          public String getType() {
            return "Pipeline";
          }

          @Override
          public String getFilename() {
            return "/p/load-data.hpl";
          }

          @Override
          public String getSearchableObject() {
            return "meta";
          }

          @Override
          public ISearchableCallback getSearchCallback() {
            return null;
          }
        };
    ISearchResult result =
        new SearchResult(
            searchable, "${DB_HOST}", "hostname : host name", "Table input", "${DB_HOST}");
    assertEquals(
        "Pipeline 'load-data' → Table input → host name",
        EnvironmentVariablesImportHelper.formatFoundLocation(result));
  }

  @Test
  void formatLocationsDescriptionJoinsAndCaps() {
    assertEquals("", EnvironmentVariablesImportHelper.formatLocationsDescription(null));
    assertEquals(
        "a; b",
        EnvironmentVariablesImportHelper.formatLocationsDescription(
            new LinkedHashSet<>(List.of("a", "b"))));

    Set<String> many = new LinkedHashSet<>();
    for (int i = 1; i <= 12; i++) {
      many.add("loc" + i);
    }
    String description = EnvironmentVariablesImportHelper.formatLocationsDescription(many);
    assertTrue(description.startsWith("loc1; "));
    assertTrue(description.endsWith("(+2 more)"));
  }

  @Test
  void defaultConfigFilename() {
    assertEquals("dev-config.json", EnvironmentVariablesImportHelper.defaultConfigFilename("dev"));
    assertEquals(
        "my-env-config.json", EnvironmentVariablesImportHelper.defaultConfigFilename("My Env"));
    assertEquals(
        "environment-config.json", EnvironmentVariablesImportHelper.defaultConfigFilename(""));
  }

  @Test
  void loadConfiguredVariableNamesAndSaveMerge() throws Exception {
    Path configPath = tempDir.resolve("env-config.json");
    DescribedVariablesConfigFile created = new DescribedVariablesConfigFile(configPath.toString());
    created.setDescribedVariable(new DescribedVariable("EXISTING", "one", "already there"));
    created.saveToFile();

    Variables variables = new Variables();
    Set<String> names =
        EnvironmentVariablesImportHelper.loadConfiguredVariableNames(
            List.of(configPath.toString()), variables);
    assertTrue(names.contains("EXISTING"));
    assertEquals(1, names.size());

    List<DescribedVariable> toAdd = new ArrayList<>();
    toAdd.add(new DescribedVariable("NEW_VAR", "two", "imported"));
    toAdd.add(new DescribedVariable("EXISTING", "updated", "overwritten"));
    EnvironmentVariablesImportHelper.saveVariablesToConfigFile(configPath.toString(), toAdd);

    DescribedVariablesConfigFile reloaded = new DescribedVariablesConfigFile(configPath.toString());
    reloaded.readFromFile();
    assertEquals("updated", reloaded.findDescribedVariableValue("EXISTING"));
    assertEquals("two", reloaded.findDescribedVariableValue("NEW_VAR"));
    assertEquals(2, reloaded.getDescribedVariables().size());
    assertTrue(Files.size(configPath) > 0);
  }

  @Test
  void loadConfiguredVariableNamesSkipsMissingFiles() throws Exception {
    Variables variables = new Variables();
    Set<String> names =
        EnvironmentVariablesImportHelper.loadConfiguredVariableNames(
            List.of(tempDir.resolve("does-not-exist.json").toString()), variables);
    assertTrue(names.isEmpty());
  }
}
