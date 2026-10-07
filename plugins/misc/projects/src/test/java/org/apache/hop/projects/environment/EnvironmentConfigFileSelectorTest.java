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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EnvironmentConfigFileSelectorTest {

  @TempDir Path tempDir;

  @Test
  void directoryWildcardKeepsJsonFilesOnly() throws Exception {
    writeTree();

    List<String> names =
        baseNames(
            EnvironmentConfigFileSelector.resolve(
                new Variables(), tempDir.toString(), ".*\\.json", "", false));

    assertEquals(List.of("a.json", "notes.json", "secret.json"), names);
  }

  @Test
  void excludeWildcardAndSubfolders() throws Exception {
    writeTree();

    List<String> withoutSubfolders =
        baseNames(
            EnvironmentConfigFileSelector.resolve(
                new Variables(), tempDir.toString(), ".*\\.json", "secret.*", false));
    assertEquals(List.of("a.json", "notes.json"), withoutSubfolders);

    List<String> withSubfolders =
        baseNames(
            EnvironmentConfigFileSelector.resolve(
                new Variables(), tempDir.toString(), ".*\\.json", "secret.*", true));
    assertEquals(List.of("a.json", "nested.json", "notes.json"), withSubfolders);
  }

  @Test
  void singleFileAndVariableDirectory() throws Exception {
    writeTree();
    Path single = tempDir.resolve("a.json");
    List<String> oneFile =
        EnvironmentConfigFileSelector.resolve(new Variables(), single.toString(), "", "", false);
    assertEquals(1, oneFile.size());
    assertTrue(oneFile.get(0).endsWith("a.json"));

    IVariables variables = new Variables();
    variables.setVariable("CONFIG_DIR", tempDir.toString());
    List<String> fromVariable =
        baseNames(
            EnvironmentConfigFileSelector.resolve(
                variables, "${CONFIG_DIR}", ".*\\.json", "", false));
    assertEquals(List.of("a.json", "notes.json", "secret.json"), fromVariable);
  }

  @Test
  void missingFolderAndShellGlobFail() throws Exception {
    writeTree();

    HopException missing =
        assertThrows(
            HopException.class,
            () ->
                EnvironmentConfigFileSelector.resolve(
                    new Variables(), tempDir.resolve("missing").toString(), "", "", false));
    assertTrue(missing.getMessage().contains("missing"));

    HopException glob =
        assertThrows(
            HopException.class,
            () ->
                EnvironmentConfigFileSelector.resolve(
                    new Variables(), tempDir.toString(), "*.json", "", false));
    assertTrue(glob.getMessage().contains("regular expression"));
  }

  @Test
  void emptyLocationReturnsNothing() throws Exception {
    assertTrue(
        EnvironmentConfigFileSelector.resolve(new Variables(), "  ", ".*\\.json", "", true)
            .isEmpty());
  }

  @Test
  void combineKeepsExplicitFilesAndSkipsDuplicates() throws Exception {
    writeTree();
    Path extra = tempDir.resolve("sibling").resolve("extra.json");
    Files.createDirectories(extra.getParent());
    Files.writeString(extra, "{}");

    List<String> directoryFiles =
        EnvironmentConfigFileSelector.resolve(
            new Variables(), tempDir.toString(), ".*\\.json", "secret.*", false);
    assertEquals(2, directoryFiles.size());

    List<String> combined =
        EnvironmentConfigFileSelector.combine(
            new Variables(),
            new String[] {"  ", extra.toString(), directoryFiles.get(0), extra.toString()},
            tempDir.toString(),
            ".*\\.json",
            "secret.*",
            false);

    assertEquals(3, combined.size());
    assertEquals(extra.toString(), combined.get(0));
    assertEquals(directoryFiles.get(0), combined.get(1));
    assertEquals(directoryFiles.get(1), combined.get(2));
  }

  @Test
  void wildcardWithoutDirectoryIsRejected() {
    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                EnvironmentConfigFileSelector.combine(
                    new Variables(), new String[] {"/tmp/a.json"}, null, "*.json", null, false));
    assertTrue(exception.getMessage().contains("--environment-config-file-directory"));

    assertThrows(
        HopException.class,
        () -> EnvironmentConfigFileSelector.combine(new Variables(), null, " ", null, null, true));
  }

  @Test
  void explicitFilesAloneAreNotScanned() throws Exception {
    List<String> files =
        EnvironmentConfigFileSelector.combine(
            new Variables(),
            new String[] {"config/a.json", " ", "config/b.json"},
            null,
            null,
            null,
            false);
    assertEquals(List.of("config/a.json", "config/b.json"), files);
  }

  private void writeTree() throws Exception {
    Files.writeString(tempDir.resolve("a.json"), "{}");
    Files.writeString(tempDir.resolve("notes.json"), "{}");
    Files.writeString(tempDir.resolve("secret.json"), "{}");
    Files.writeString(tempDir.resolve("readme.txt"), "text");
    Files.createDirectories(tempDir.resolve("nested"));
    Files.writeString(tempDir.resolve("nested").resolve("nested.json"), "{}");
  }

  private static List<String> baseNames(List<String> paths) {
    List<String> names = new ArrayList<>();
    for (String path : paths) {
      int slash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
      names.add(slash < 0 ? path : path.substring(slash + 1));
    }
    return names;
  }
}
